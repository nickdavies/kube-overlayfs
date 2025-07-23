use serde::Deserialize;

use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, Instant};
use thiserror::Error;

use crate::config::{IOErrorAtPath, LowerDir, MountConfig, ValidatedMountConfig};

pub enum SyncResult<E> {
    Ok,
    Transient(E),
    Fatal(E),
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncMode {
    #[default]
    None,
    Once(PathBuf),
    Constant(PathBuf),
}

#[derive(Error, Debug)]
pub enum SyncError {
    #[error("rsync command failed with exit code {code}: {stderr}")]
    RsyncFailed { code: i32, stderr: String },
    #[error("failed to execute rsync command: {0}")]
    CommandError(#[from] std::io::Error),

    #[error("failed to create directory: {0}")]
    DirCreateError(#[from] IOErrorAtPath),
}

pub struct SyncedConfig(MountConfig);
impl From<SyncedConfig> for MountConfig {
    fn from(other: SyncedConfig) -> Self {
        other.0
    }
}

pub struct SyncManager {
    targets: Vec<DirSyncer>,
}

impl SyncManager {
    pub fn new(config: ValidatedMountConfig) -> Result<(Self, SyncedConfig), (PathBuf, SyncError)> {
        let mut targets = Vec::new();
        for dir in &Into::<&MountConfig>::into(&config).lower_dirs {
            if let SyncMode::None = dir.sync_mode() {
                continue;
            }
            let dir_sync = DirSyncer::new(dir).map_err(|e| (dir.full_path(), e))?;
            targets.push(dir_sync);
        }

        Ok((Self { targets }, SyncedConfig(config.into())))
    }

    pub fn try_sync(&mut self, max_age: Duration) -> Vec<(PathBuf, SyncResult<SyncError>)> {
        let mut results = Vec::new();
        for target in self.targets.iter_mut() {
            if let SyncMode::Constant(_) = target.target.sync_mode() {
                results.push((target.target.full_path(), target.try_sync(max_age)));
            }
        }
        results
    }
}

struct DirSyncer {
    target: LowerDir,
    last_successful_sync: Instant,
}

impl DirSyncer {
    pub fn new(target: &LowerDir) -> Result<Self, SyncError> {
        Self::sync(target)?;
        Ok(Self {
            target: target.clone(),
            last_successful_sync: Instant::now(),
        })
    }

    pub fn try_sync(&mut self, max_age: Duration) -> SyncResult<SyncError> {
        match Self::sync(&self.target) {
            Ok(_) => {
                self.last_successful_sync = Instant::now();
                SyncResult::Ok
            }
            Err(e) => {
                if self.last_successful_sync.elapsed() <= max_age {
                    SyncResult::Transient(e)
                } else {
                    SyncResult::Fatal(e)
                }
            }
        }
    }

    fn sync(target: &LowerDir) -> Result<(), SyncError> {
        let source = target.full_path();
        let target = target.mount_path();

        // Create target directory if it doesn't exist
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent).map_err(|e| IOErrorAtPath(parent.to_path_buf(), e))?;
        }

        let output = Command::new("rsync")
            .arg("-av")
            .arg("--delete")
            .arg(format!("{}/", source.display()))
            .arg(target)
            .output()?;

        if output.status.success() {
            Ok(())
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr).to_string();
            Err(SyncError::RsyncFailed {
                code: output.status.code().unwrap_or(-1),
                stderr,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{LowerDir, MountConfig, UpperDir, ValidatedMountConfig};
    use std::fs;
    use tempfile::TempDir;

    fn create_test_file(dir: &std::path::Path, relative_path: &str, content: &str) -> PathBuf {
        let file_path = dir.join(relative_path);
        if let Some(parent) = file_path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(&file_path, content).unwrap();
        file_path
    }

    fn create_test_mount_config(temp_dir: &TempDir) -> ValidatedMountConfig {
        let volume = temp_dir.path().to_path_buf();

        let lower_dir = LowerDir::new(volume.join("lower"), None).unwrap();
        let upper_dir = UpperDir::new(
            volume.clone(),
            PathBuf::from("upper"),
            PathBuf::from("work"),
            PathBuf::from("merged"),
        )
        .unwrap();

        let mount_config = MountConfig {
            lower_dirs: vec![lower_dir],
            upper_dir,
        };

        mount_config.validate().unwrap()
    }

    #[test]
    fn test_sync_mode_default() {
        let sync_mode = SyncMode::default();
        assert!(matches!(sync_mode, SyncMode::None));
    }

    #[test]
    fn test_sync_manager_new_with_no_sync_dirs() {
        let temp_dir = TempDir::new().unwrap();
        let config = create_test_mount_config(&temp_dir);

        let (sync_manager, _synced_config) = SyncManager::new(config).unwrap();
        assert_eq!(sync_manager.targets.len(), 0);
    }

    #[test]
    fn test_sync_manager_new_with_sync_dirs() {
        let temp_dir = TempDir::new().unwrap();
        let volume = temp_dir.path().to_path_buf();

        // Create source directory with content
        let source_path = volume.join("source");
        fs::create_dir_all(&source_path).unwrap();
        create_test_file(&source_path, "test.txt", "test content");

        // Create target directory
        let target_path = volume.join("target");
        fs::create_dir_all(&target_path).unwrap();

        let lower_dir =
            LowerDir::new_with_sync(source_path, None, SyncMode::Once(target_path)).unwrap();

        let upper_dir = UpperDir::new(
            volume.clone(),
            PathBuf::from("upper"),
            PathBuf::from("work"),
            PathBuf::from("merged"),
        )
        .unwrap();

        let mount_config = MountConfig {
            lower_dirs: vec![lower_dir],
            upper_dir,
        };

        let validated_config = mount_config.validate().unwrap();
        let (sync_manager, _synced_config) = SyncManager::new(validated_config).unwrap();
        assert_eq!(sync_manager.targets.len(), 1);
    }

    #[test]
    fn test_sync_manager_try_sync_with_constant_mode() {
        let temp_dir = TempDir::new().unwrap();
        let volume = temp_dir.path().to_path_buf();

        // Create source directory with content
        let source_path = volume.join("source");
        fs::create_dir_all(&source_path).unwrap();
        create_test_file(&source_path, "test.txt", "test content");

        // Create target directory
        let target_path = volume.join("target");
        fs::create_dir_all(&target_path).unwrap();

        let lower_dir =
            LowerDir::new_with_sync(source_path, None, SyncMode::Constant(target_path.clone()))
                .unwrap();

        let upper_dir = UpperDir::new(
            volume.clone(),
            PathBuf::from("upper"),
            PathBuf::from("work"),
            PathBuf::from("merged"),
        )
        .unwrap();

        let mount_config = MountConfig {
            lower_dirs: vec![lower_dir],
            upper_dir,
        };

        let validated_config = mount_config.validate().unwrap();
        let (mut sync_manager, _synced_config) = SyncManager::new(validated_config).unwrap();

        let results = sync_manager.try_sync(Duration::from_secs(60));
        assert_eq!(results.len(), 1);

        // Verify the file was synced
        assert!(target_path.join("test.txt").exists());
        let content = fs::read_to_string(target_path.join("test.txt")).unwrap();
        assert_eq!(content, "test content");
    }

    #[test]
    fn test_sync_manager_try_sync_ignores_once_mode() {
        let temp_dir = TempDir::new().unwrap();
        let volume = temp_dir.path().to_path_buf();

        // Create source directory with content
        let source_path = volume.join("source");
        fs::create_dir_all(&source_path).unwrap();
        create_test_file(&source_path, "test.txt", "test content");

        // Create target directory
        let target_path = volume.join("target");
        fs::create_dir_all(&target_path).unwrap();

        let lower_dir =
            LowerDir::new_with_sync(source_path, None, SyncMode::Once(target_path)).unwrap();

        let upper_dir = UpperDir::new(
            volume.clone(),
            PathBuf::from("upper"),
            PathBuf::from("work"),
            PathBuf::from("merged"),
        )
        .unwrap();

        let mount_config = MountConfig {
            lower_dirs: vec![lower_dir],
            upper_dir,
        };

        let validated_config = mount_config.validate().unwrap();
        let (mut sync_manager, _synced_config) = SyncManager::new(validated_config).unwrap();

        // try_sync should ignore Once mode directories
        let results = sync_manager.try_sync(Duration::from_secs(60));
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_dir_syncer_new_performs_initial_sync() {
        let temp_dir = TempDir::new().unwrap();
        let volume = temp_dir.path().to_path_buf();

        // Create source directory with content
        let source_path = volume.join("source");
        fs::create_dir_all(&source_path).unwrap();
        create_test_file(&source_path, "test.txt", "test content");
        create_test_file(&source_path, "subdir/nested.txt", "nested content");

        // Create target directory
        let target_path = volume.join("target");
        fs::create_dir_all(&target_path).unwrap();

        let lower_dir =
            LowerDir::new_with_sync(source_path, None, SyncMode::Once(target_path.clone()))
                .unwrap();

        let _syncer = DirSyncer::new(&lower_dir).unwrap();

        // Verify files were synced
        assert!(target_path.join("test.txt").exists());
        assert!(target_path.join("subdir/nested.txt").exists());

        let content = fs::read_to_string(target_path.join("test.txt")).unwrap();
        assert_eq!(content, "test content");

        let nested_content = fs::read_to_string(target_path.join("subdir/nested.txt")).unwrap();
        assert_eq!(nested_content, "nested content");
    }

    #[test]
    fn test_dir_syncer_try_sync_success() {
        let temp_dir = TempDir::new().unwrap();
        let volume = temp_dir.path().to_path_buf();

        // Create source directory with content
        let source_path = volume.join("source");
        fs::create_dir_all(&source_path).unwrap();
        create_test_file(&source_path, "test.txt", "test content");

        // Create target directory
        let target_path = volume.join("target");
        fs::create_dir_all(&target_path).unwrap();

        let lower_dir = LowerDir::new_with_sync(
            source_path.clone(),
            None,
            SyncMode::Constant(target_path.clone()),
        )
        .unwrap();

        let mut syncer = DirSyncer::new(&lower_dir).unwrap();

        // Add a new file to source
        create_test_file(&source_path, "new_file.txt", "new content");

        let result = syncer.try_sync(Duration::from_secs(60));
        assert!(matches!(result, SyncResult::Ok));

        // Verify new file was synced
        assert!(target_path.join("new_file.txt").exists());
        let content = fs::read_to_string(target_path.join("new_file.txt")).unwrap();
        assert_eq!(content, "new content");
    }

    #[test]
    fn test_dir_syncer_try_sync_transient_error() {
        let temp_dir = TempDir::new().unwrap();
        let volume = temp_dir.path().to_path_buf();

        // Create source directory
        let source_path = volume.join("source");
        fs::create_dir_all(&source_path).unwrap();
        create_test_file(&source_path, "test.txt", "test content");

        // Create target directory
        let target_path = volume.join("target");
        fs::create_dir_all(&target_path).unwrap();

        let lower_dir =
            LowerDir::new_with_sync(source_path, None, SyncMode::Constant(target_path)).unwrap();

        let mut syncer = DirSyncer::new(&lower_dir).unwrap();

        // Create an invalid target to force rsync failure
        let invalid_lower_dir = LowerDir::new_with_sync(
            PathBuf::from("/nonexistent/source"),
            None,
            SyncMode::Constant(PathBuf::from("/nonexistent/target")),
        )
        .unwrap();

        syncer.target = invalid_lower_dir;

        let result = syncer.try_sync(Duration::from_secs(60));
        assert!(matches!(result, SyncResult::Transient(_)));
    }

    #[test]
    fn test_dir_syncer_try_sync_fatal_error_after_timeout() {
        let temp_dir = TempDir::new().unwrap();
        let volume = temp_dir.path().to_path_buf();

        // Create source directory
        let source_path = volume.join("source");
        fs::create_dir_all(&source_path).unwrap();
        create_test_file(&source_path, "test.txt", "test content");

        // Create target directory
        let target_path = volume.join("target");
        fs::create_dir_all(&target_path).unwrap();

        let lower_dir =
            LowerDir::new_with_sync(source_path, None, SyncMode::Constant(target_path)).unwrap();

        let mut syncer = DirSyncer::new(&lower_dir).unwrap();

        // Simulate an old last successful sync
        syncer.last_successful_sync = Instant::now() - Duration::from_secs(120);

        // Create an invalid target to force rsync failure
        let invalid_lower_dir = LowerDir::new_with_sync(
            PathBuf::from("/nonexistent/source"),
            None,
            SyncMode::Constant(PathBuf::from("/nonexistent/target")),
        )
        .unwrap();

        syncer.target = invalid_lower_dir;

        let result = syncer.try_sync(Duration::from_secs(60));
        assert!(matches!(result, SyncResult::Fatal(_)));
    }

    #[test]
    fn test_synced_config_conversion() {
        let temp_dir = TempDir::new().unwrap();
        let config = create_test_mount_config(&temp_dir);
        let original_config: &MountConfig = (&config).into();

        let synced_config = SyncedConfig(config.clone().into());
        let converted_config: MountConfig = synced_config.into();

        assert_eq!(
            original_config.lower_dirs.len(),
            converted_config.lower_dirs.len()
        );
    }
}
