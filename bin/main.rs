use anyhow::{Context, Result};
use clap::Parser;
use overlay_mount::{OverlayManager, config::MountConfig};
use serde::Deserialize;
use signal_hook::{consts::SIGINT, consts::SIGTERM, iterator::Signals};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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

    let manager =
        OverlayManager::new(validated_config).context("Failed to create overlay manager")?;

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
    match post_mount(running, options.success_file) {
        Ok(_) => manager.umount().context("Error during cleanup"),
        Err(run_err) => match manager.umount() {
            Ok(_) => Err(run_err).context("Error during maintenance loop"),
            Err(umount_err) => Err(umount_err)
                .context("failed umount")
                .with_context(|| format!("after getting error: {run_err:?}")),
        },
    }
}

fn post_mount(running: Arc<AtomicBool>, success_file: Option<PathBuf>) -> Result<()> {
    // Create success file if specified
    if let Some(success_file) = &success_file {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("Failed to get current time")?
            .as_secs();

        fs::write(success_file, timestamp.to_string())
            .with_context(|| format!("Failed to write success file: {success_file:?}"))?;

        println!("Success file created: {success_file:?}");
    }
    // Keep the program running until interrupted
    while running.load(Ordering::SeqCst) {
        thread::sleep(Duration::from_millis(200));
    }

    Ok(())
}
