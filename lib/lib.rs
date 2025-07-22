use nix::mount::{MsFlags, mount, umount};
use std::io;
use std::process::Command;

use config::{MountConfig, ValidatedMountConfig};

pub mod config;

#[derive(thiserror::Error, Debug)]
pub enum ManagerError {
    #[error("mount error {0:}")]
    MountError(nix::errno::Errno, Result<Vec<String>, io::Error>),
    #[error("failed to unmount volume: {0}")]
    UmountError(nix::errno::Errno),
}

pub struct OverlayManager {
    config: MountConfig,
}

impl OverlayManager {
    pub fn new(config: ValidatedMountConfig) -> Result<Self, ManagerError> {
        Ok(OverlayManager {
            config: config.into(),
        })
    }

    /// Mount the overlay filesystem
    pub fn mount(&self) -> Result<(), ManagerError> {
        let lowerdir = self
            .config
            .lower_dirs
            .iter()
            .map(|lower| lower.full_path().display().to_string())
            .collect::<Vec<_>>()
            .join(":");

        let mount_options = format!(
            "lowerdir={},upperdir={},workdir={}",
            lowerdir,
            self.config.upper_dir.upper_path().display(),
            self.config.upper_dir.work_path().display()
        );

        match mount(
            Some("overlay"),
            &self.config.upper_dir.merged_path(),
            Some("overlay"),
            MsFlags::empty(),
            Some(mount_options.as_str()),
        ) {
            Ok(_) => {
                println!("Successfully mounted overlay filesystem");
                Ok(())
            }
            Err(e) => {
                // Try to get dmesg output for debugging
                let debug_logs = match Command::new("dmesg").output() {
                    Ok(dmesg_output) => {
                        let output = String::from_utf8_lossy(&dmesg_output.stdout);
                        let dmesg_lines: Vec<String> = output
                            .lines()
                            .rev()
                            .take(15)
                            .map(|c| c.to_string())
                            .collect();
                        Ok(dmesg_lines)
                    }
                    Err(e) => Err(e),
                };

                Err(ManagerError::MountError(e, debug_logs))
            }
        }
    }

    /// Setup overlay mount with the given configuration
    pub fn umount(&self) -> Result<(), ManagerError> {
        umount(&self.config.upper_dir.merged_path()).map_err(ManagerError::UmountError)
    }
}
