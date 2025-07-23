use anyhow::{Context, Result};
use clap::Parser;
use serde::Deserialize;
use signal_hook::{consts::SIGINT, consts::SIGTERM, iterator::Signals};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use overlay_mount::{OverlayManager, config::MountConfig, rsync::SyncManager, rsync::SyncResult};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to TOML configuration file
    #[arg(long)]
    config: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Options {
    show_dmesg: Option<bool>,
    success_file: Option<PathBuf>,
    #[serde(default = "default_resync_interval")]
    resync_interval_seconds: u64,
    #[serde(default = "default_sync_timeout")]
    sync_timeout_seconds: u64,
}

fn default_resync_interval() -> u64 {
    300 // 5 minutes
}

fn default_sync_timeout() -> u64 {
    1800 // 30 minutes
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(flatten)]
    mount_config: MountConfig,

    options: Options,
}

fn main() -> Result<()> {
    let args = Args::parse();

    // Read and parse TOML config
    let config_content = fs::read_to_string(&args.config)
        .with_context(|| format!("Failed to read config file: {:?}", args.config))?;

    let config: Config = toml::from_str(&config_content)
        .with_context(|| format!("Failed to parse config file: {:?}", args.config))?;

    println!("Config: {config:#?}");

    let options = config.options;

    // Setup signal handling
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();

    let mut signals = Signals::new([SIGINT, SIGTERM])?;
    thread::spawn(move || {
        for sig in signals.forever() {
            println!("Received interrupt signal {sig:?}, shutting down...");
            r.store(false, Ordering::SeqCst);
        }
    });

    // Validate config and create manager
    let validated_config = config
        .mount_config
        .validate()
        .context("Failed to validate config")?;

    let (mut sync_manager, synced_config) = match SyncManager::new(validated_config) {
        Ok(res) => res,
        Err((path, err)) => {
            return Err(err).context(format!("failed to sync: {path:?}"));
        }
    };

    let manager = OverlayManager::new(synced_config).context("Failed to create overlay manager")?;

    // Mount the overlay
    if let Err(e) = manager.mount() {
        if options.show_dmesg.unwrap_or(false) {
            if let overlay_mount::ManagerError::MountError(_, Ok(dmesg_lines)) = &e {
                eprintln!("Recent dmesg output:");
                for line in dmesg_lines {
                    eprintln!("  {line}");
                }
            }
        }
        return Err(anyhow::Error::from(e).context("Failed to mount overlay"));
    }

    println!("Overlay mount setup complete.");
    match post_mount(running, options, &mut sync_manager) {
        Ok(_) => manager.umount().context("Error during cleanup"),
        Err(run_err) => match manager.umount() {
            Ok(_) => Err(run_err).context("Error during maintenance loop"),
            Err(umount_err) => Err(umount_err)
                .context("failed umount")
                .with_context(|| format!("after getting error: {run_err:?}")),
        },
    }
}

fn post_mount(
    running: Arc<AtomicBool>,
    options: Options,
    sync_manager: &mut SyncManager,
) -> Result<()> {
    // Create success file if specified
    if let Some(success_file) = &options.success_file {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("Failed to get current time")?
            .as_secs();

        fs::write(success_file, timestamp.to_string())
            .with_context(|| format!("Failed to write success file: {success_file:?}"))?;

        println!("Success file created: {success_file:?}");
    }

    let resync_interval = Duration::from_secs(options.resync_interval_seconds);
    let sync_timeout = Duration::from_secs(options.sync_timeout_seconds);
    let mut last_sync = SystemTime::now();

    // Keep the program running until interrupted
    while running.load(Ordering::SeqCst) {
        thread::sleep(Duration::from_millis(200));

        // Check if it's time to resync
        if last_sync.elapsed().unwrap_or(Duration::ZERO) >= resync_interval {
            for (path, res) in sync_manager.try_sync(sync_timeout) {
                match res {
                    SyncResult::Ok => {
                        println!("Successfully synced: '{path:?}'");
                    }
                    SyncResult::Transient(e) => {
                        println!("Transient sync failure for '{path:?}': {e}");
                    }
                    SyncResult::Fatal(e) => {
                        return Err(e).context(format!("failed repeatedly to sync '{path:?}'"));
                    }
                }
            }
            last_sync = SystemTime::now();
        }
    }

    Ok(())
}
