use serde::Deserialize;
use std::collections::BTreeSet;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use crate::rsync::SyncMode;

#[derive(thiserror::Error, Debug)]
#[error("IO Error at '{0:?}': {1}")]
pub struct IOErrorAtPath(pub PathBuf, #[source] pub io::Error);

#[derive(thiserror::Error, Debug)]
pub enum ValidationError {
    #[error("provided path '{0:?}' must be relative to parent '{1:?}' ie must not start with /")]
    NonRelative(PathBuf, PathBuf),
    #[error("failed filesystem operation: {0}")]
    IOError(#[from] IOErrorAtPath),

    #[error("one or more file paths are masked by rw layer: {0:?}")]
    MaskedFiles(Vec<PathBuf>),
}

#[derive(thiserror::Error, Debug)]
pub enum ConfigError {
    #[error("Failed to create dir: {0}")]
    CreateDirError(#[from] IOErrorAtPath),
    #[error("Invalid config/environment: {0:?}")]
    ValidationError(#[from] ValidationError),
}

#[derive(Debug, Clone)]
pub struct ValidatedMountConfig(MountConfig);

impl From<ValidatedMountConfig> for MountConfig {
    fn from(config: ValidatedMountConfig) -> Self {
        config.0
    }
}

impl<'a> From<&'a ValidatedMountConfig> for &'a MountConfig {
    fn from(config: &'a ValidatedMountConfig) -> Self {
        &config.0
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct LowerDir {
    volume: PathBuf,
    subdir: Option<PathBuf>,
    #[serde(default)]
    sync_mode: SyncMode,
}

fn enforce_relative(volume: &Path, subdir: Option<&PathBuf>) -> Result<(), ValidationError> {
    if let Some(subdir) = subdir {
        if subdir.is_absolute() {
            return Err(ValidationError::NonRelative(
                subdir.to_path_buf(),
                volume.to_path_buf(),
            ));
        }
    }
    Ok(())
}

impl LowerDir {
    pub fn new(volume: PathBuf, subdir: Option<PathBuf>) -> Result<Self, ValidationError> {
        enforce_relative(&volume, subdir.as_ref())?;
        Ok(Self {
            volume,
            subdir,
            sync_mode: SyncMode::None,
        })
    }

    pub fn new_with_sync(
        volume: PathBuf,
        subdir: Option<PathBuf>,
        sync_mode: SyncMode,
    ) -> Result<Self, ValidationError> {
        enforce_relative(&volume, subdir.as_ref())?;
        Ok(Self {
            volume,
            subdir,
            sync_mode,
        })
    }

    pub fn full_path(&self) -> PathBuf {
        match &self.subdir {
            Some(subdir) => self.volume.join(subdir),
            None => self.volume.clone(),
        }
    }

    pub fn sync_mode(&self) -> &SyncMode {
        &self.sync_mode
    }

    pub fn mount_path(&self) -> PathBuf {
        match &self.sync_mode {
            SyncMode::None => self.full_path(),
            SyncMode::Once(target) | SyncMode::Constant(target) => target.clone(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct UpperDir {
    volume: PathBuf,
    upper_subdir: PathBuf,
    work_subdir: PathBuf,
    merged_subdir: PathBuf,
}

impl UpperDir {
    pub fn new(
        volume: PathBuf,
        upper_subdir: PathBuf,
        work_subdir: PathBuf,
        merged_subdir: PathBuf,
    ) -> Result<Self, ValidationError> {
        enforce_relative(&volume, Some(&upper_subdir))?;
        enforce_relative(&volume, Some(&work_subdir))?;
        enforce_relative(&volume, Some(&merged_subdir))?;
        Ok(Self {
            volume,
            upper_subdir,
            work_subdir,
            merged_subdir,
        })
    }

    pub fn upper_path(&self) -> PathBuf {
        self.volume.join(&self.upper_subdir)
    }

    pub fn work_path(&self) -> PathBuf {
        self.volume.join(&self.work_subdir)
    }

    pub fn merged_path(&self) -> PathBuf {
        self.volume.join(&self.merged_subdir)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[non_exhaustive]
pub struct MountConfig {
    pub lower_dirs: Vec<LowerDir>,
    pub upper_dir: UpperDir,
    #[serde(default)]
    pub allowed_masked_files: BTreeSet<PathBuf>,
}

impl MountConfig {
    /// We are running overlay FS but in a slightly constrained environment where we don't to allow
    /// masking of the top volume.
    ///
    /// The idea here is that all the lower values are the static (read-only) configs and then any
    /// mutations made should be in other files not already provided. So if we find any configs in
    /// the lower layers that are overwritten by the rw volume then we are not honoring that RO
    /// config layer correctly.
    pub fn validate(self) -> Result<ValidatedMountConfig, ConfigError> {
        self.create_directories()?;

        let masked_files = self.find_masked_files()?;
        if !masked_files.is_empty() {
            Err(ValidationError::MaskedFiles(masked_files).into())
        } else {
            Ok(ValidatedMountConfig(self))
        }
    }

    /// Create necessary directories for overlay filesystem
    fn create_directories(&self) -> Result<(), IOErrorAtPath> {
        println!("Creating overlay directories...");

        let upper_path = self.upper_dir.upper_path();
        fs::create_dir_all(&upper_path).map_err(|e| IOErrorAtPath(upper_path, e))?;

        let work_path = self.upper_dir.work_path();
        fs::create_dir_all(&work_path).map_err(|e| IOErrorAtPath(work_path, e))?;

        let merged_path = self.upper_dir.merged_path();
        fs::create_dir_all(&merged_path).map_err(|e| IOErrorAtPath(merged_path, e))?;

        Ok(())
    }

    /// Find files in upper layer that would mask files in lower layers
    fn find_masked_files(&self) -> Result<Vec<PathBuf>, ValidationError> {
        let mut masked_files = Vec::new();
        let upper_path = self.upper_dir.upper_path();

        if !upper_path.exists() {
            return Ok(masked_files);
        }

        // Collect all file paths from lower directories
        let mut lower_files = std::collections::HashSet::new();
        for lower_dir in &self.lower_dirs {
            let lower_path = lower_dir.full_path();
            if lower_path.exists() {
                Self::collect_file_paths(&lower_path, &lower_path, &mut lower_files)?;
            }
        }

        // Check if any of these paths exist in upper layer
        for relative_path in lower_files {
            let upper_file_path = upper_path.join(&relative_path);
            if upper_file_path.exists() && !self.allowed_masked_files.contains(&relative_path) {
                masked_files.push(upper_file_path);
            }
        }

        Ok(masked_files)
    }

    /// Recursively collect relative file paths from a directory
    fn collect_file_paths(
        dir: &Path,
        base_dir: &Path,
        file_paths: &mut std::collections::HashSet<PathBuf>,
    ) -> Result<(), IOErrorAtPath> {
        for entry in fs::read_dir(dir).map_err(|e| IOErrorAtPath(dir.to_path_buf(), e))? {
            let entry = entry.map_err(|e| IOErrorAtPath(dir.to_path_buf(), e))?;
            let path = entry.path();

            if path.is_dir() {
                Self::collect_file_paths(&path, base_dir, file_paths)?;
            } else if let Ok(relative_path) = path.strip_prefix(base_dir) {
                file_paths.insert(relative_path.to_path_buf());
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn create_test_file(dir: &Path, relative_path: &str, content: &str) -> PathBuf {
        let file_path = dir.join(relative_path);
        if let Some(parent) = file_path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(&file_path, content).unwrap();
        file_path
    }

    #[test]
    fn test_lower_dir_new_valid() {
        let temp_dir = TempDir::new().unwrap();
        let volume = temp_dir.path().join("volume");
        let subdir = Some(PathBuf::from("subdir"));

        let lower_dir = LowerDir::new(volume.clone(), subdir.clone()).unwrap();
        assert_eq!(lower_dir.volume, volume);
        assert_eq!(lower_dir.subdir, subdir);
    }

    #[test]
    fn test_lower_dir_new_absolute_subdir_fails() {
        let temp_dir = TempDir::new().unwrap();
        let volume = temp_dir.path().join("volume");
        let subdir = Some(PathBuf::from("/absolute/path"));

        let result = LowerDir::new(volume, subdir);
        assert!(matches!(result, Err(ValidationError::NonRelative(_, _))));
    }

    #[test]
    fn test_lower_dir_full_path() {
        let temp_dir = TempDir::new().unwrap();
        let volume = temp_dir.path().join("volume");
        let subdir = Some(PathBuf::from("subdir"));

        let lower_dir = LowerDir::new(volume.clone(), subdir).unwrap();
        assert_eq!(lower_dir.full_path(), volume.join("subdir"));
    }

    #[test]
    fn test_lower_dir_full_path_no_subdir() {
        let temp_dir = TempDir::new().unwrap();
        let volume = temp_dir.path().join("volume");

        let lower_dir = LowerDir::new(volume.clone(), None).unwrap();
        assert_eq!(lower_dir.full_path(), volume);
    }

    #[test]
    fn test_upper_dir_new_valid() {
        let temp_dir = TempDir::new().unwrap();
        let volume = temp_dir.path().join("volume");
        let upper_subdir = PathBuf::from("upper");
        let work_subdir = PathBuf::from("work");
        let merged_subdir = PathBuf::from("merged");

        let upper_dir = UpperDir::new(
            volume.clone(),
            upper_subdir.clone(),
            work_subdir.clone(),
            merged_subdir.clone(),
        )
        .unwrap();

        assert_eq!(upper_dir.volume, volume);
        assert_eq!(upper_dir.upper_subdir, upper_subdir);
        assert_eq!(upper_dir.work_subdir, work_subdir);
        assert_eq!(upper_dir.merged_subdir, merged_subdir);
    }

    #[test]
    fn test_upper_dir_new_absolute_paths_fail() {
        let temp_dir = TempDir::new().unwrap();
        let volume = temp_dir.path().join("volume");
        let absolute_path = PathBuf::from("/absolute/path");

        // Test absolute upper_subdir
        let result = UpperDir::new(
            volume.clone(),
            absolute_path.clone(),
            PathBuf::from("work"),
            PathBuf::from("merged"),
        );
        assert!(matches!(result, Err(ValidationError::NonRelative(_, _))));

        // Test absolute work_subdir
        let result = UpperDir::new(
            volume.clone(),
            PathBuf::from("upper"),
            absolute_path.clone(),
            PathBuf::from("merged"),
        );
        assert!(matches!(result, Err(ValidationError::NonRelative(_, _))));

        // Test absolute merged_subdir
        let result = UpperDir::new(
            volume.clone(),
            PathBuf::from("upper"),
            PathBuf::from("work"),
            absolute_path,
        );
        assert!(matches!(result, Err(ValidationError::NonRelative(_, _))));
    }

    #[test]
    fn test_upper_dir_paths() {
        let temp_dir = TempDir::new().unwrap();
        let volume = temp_dir.path().join("volume");
        let upper_dir = UpperDir::new(
            volume.clone(),
            PathBuf::from("upper"),
            PathBuf::from("work"),
            PathBuf::from("merged"),
        )
        .unwrap();

        assert_eq!(upper_dir.upper_path(), volume.join("upper"));
        assert_eq!(upper_dir.work_path(), volume.join("work"));
        assert_eq!(upper_dir.merged_path(), volume.join("merged"));
    }

    #[test]
    fn test_mount_config_create_directories() {
        let temp_dir = TempDir::new().unwrap();
        let volume = temp_dir.path().to_path_buf();

        let lower_dir = LowerDir::new(volume.join("lower"), None).unwrap();
        let upper_dir = UpperDir::new(
            volume.clone(),
            PathBuf::from("upper"),
            PathBuf::from("work"),
            PathBuf::from("merged"),
        )
        .unwrap();

        let config = MountConfig {
            lower_dirs: vec![lower_dir],
            upper_dir,
            allowed_masked_files: BTreeSet::new(),
        };

        config.create_directories().unwrap();

        assert!(volume.join("upper").exists());
        assert!(volume.join("work").exists());
        assert!(volume.join("merged").exists());
    }

    #[test]
    fn test_mount_config_no_masked_files() {
        let temp_dir = TempDir::new().unwrap();
        let volume = temp_dir.path().to_path_buf();

        // Create lower directory with some files
        let lower_path = volume.join("lower");
        fs::create_dir_all(&lower_path).unwrap();
        create_test_file(&lower_path, "config.txt", "lower config");
        create_test_file(&lower_path, "subdir/nested.txt", "nested file");

        let lower_dir = LowerDir::new(lower_path, None).unwrap();
        let upper_dir = UpperDir::new(
            volume.clone(),
            PathBuf::from("upper"),
            PathBuf::from("work"),
            PathBuf::from("merged"),
        )
        .unwrap();

        let config = MountConfig {
            lower_dirs: vec![lower_dir],
            upper_dir,
            allowed_masked_files: BTreeSet::new(),
        };

        let validated = config.validate().unwrap();
        assert!(matches!(validated, ValidatedMountConfig(_)));
    }

    #[test]
    fn test_mount_config_with_masked_files() {
        let temp_dir = TempDir::new().unwrap();
        let volume = temp_dir.path().to_path_buf();

        // Create lower directory with some files
        let lower_path = volume.join("lower");
        fs::create_dir_all(&lower_path).unwrap();
        create_test_file(&lower_path, "config.txt", "lower config");
        create_test_file(&lower_path, "subdir/nested.txt", "nested file");

        // Create upper directory with overlapping files
        let upper_path = volume.join("upper");
        fs::create_dir_all(&upper_path).unwrap();
        create_test_file(&upper_path, "config.txt", "upper config");

        let lower_dir = LowerDir::new(lower_path, None).unwrap();
        let upper_dir = UpperDir::new(
            volume.clone(),
            PathBuf::from("upper"),
            PathBuf::from("work"),
            PathBuf::from("merged"),
        )
        .unwrap();

        let config = MountConfig {
            lower_dirs: vec![lower_dir],
            upper_dir,
            allowed_masked_files: BTreeSet::new(),
        };

        let result = config.validate();
        assert!(matches!(
            result,
            Err(ConfigError::ValidationError(ValidationError::MaskedFiles(
                _
            )))
        ));

        if let Err(ConfigError::ValidationError(ValidationError::MaskedFiles(masked_files))) =
            result
        {
            assert_eq!(masked_files.len(), 1);
            assert!(masked_files[0].ends_with("config.txt"));
        }
    }

    #[test]
    fn test_mount_config_multiple_lower_dirs() {
        let temp_dir = TempDir::new().unwrap();
        let volume = temp_dir.path().to_path_buf();

        // Create first lower directory
        let lower1_path = volume.join("lower1");
        fs::create_dir_all(&lower1_path).unwrap();
        create_test_file(&lower1_path, "file1.txt", "content1");

        // Create second lower directory
        let lower2_path = volume.join("lower2");
        fs::create_dir_all(&lower2_path).unwrap();
        create_test_file(&lower2_path, "file2.txt", "content2");

        // Create upper directory with file that masks lower1
        let upper_path = volume.join("upper");
        fs::create_dir_all(&upper_path).unwrap();
        create_test_file(&upper_path, "file1.txt", "upper content");

        let lower_dir1 = LowerDir::new(lower1_path, None).unwrap();
        let lower_dir2 = LowerDir::new(lower2_path, None).unwrap();
        let upper_dir = UpperDir::new(
            volume.clone(),
            PathBuf::from("upper"),
            PathBuf::from("work"),
            PathBuf::from("merged"),
        )
        .unwrap();

        let config = MountConfig {
            lower_dirs: vec![lower_dir1, lower_dir2],
            upper_dir,
            allowed_masked_files: BTreeSet::new(),
        };

        let result = config.validate();
        assert!(matches!(
            result,
            Err(ConfigError::ValidationError(ValidationError::MaskedFiles(
                _
            )))
        ));
    }

    #[test]
    fn test_mount_config_with_subdirs() {
        let temp_dir = TempDir::new().unwrap();
        let volume = temp_dir.path().to_path_buf();

        // Create lower directory with subdir
        let lower_base = volume.join("lower_base");
        let lower_subdir_path = lower_base.join("subdir");
        fs::create_dir_all(&lower_subdir_path).unwrap();
        create_test_file(&lower_subdir_path, "config.txt", "lower config");

        let lower_dir = LowerDir::new(lower_base, Some(PathBuf::from("subdir"))).unwrap();
        let upper_dir = UpperDir::new(
            volume.clone(),
            PathBuf::from("upper"),
            PathBuf::from("work"),
            PathBuf::from("merged"),
        )
        .unwrap();

        let config = MountConfig {
            lower_dirs: vec![lower_dir],
            upper_dir,
            allowed_masked_files: BTreeSet::new(),
        };

        let validated = config.validate().unwrap();
        assert!(matches!(validated, ValidatedMountConfig(_)));
    }

    #[test]
    fn test_collect_file_paths() {
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path();

        // Create a directory structure
        create_test_file(base_path, "file1.txt", "content1");
        create_test_file(base_path, "subdir/file2.txt", "content2");
        create_test_file(base_path, "subdir/nested/file3.txt", "content3");

        let mut file_paths = std::collections::HashSet::new();
        MountConfig::collect_file_paths(base_path, base_path, &mut file_paths).unwrap();

        assert_eq!(file_paths.len(), 3);
        assert!(file_paths.contains(&PathBuf::from("file1.txt")));
        assert!(file_paths.contains(&PathBuf::from("subdir/file2.txt")));
        assert!(file_paths.contains(&PathBuf::from("subdir/nested/file3.txt")));
    }

    #[test]
    fn test_mount_config_with_allowed_masked_files() {
        let temp_dir = TempDir::new().unwrap();
        let volume = temp_dir.path().to_path_buf();

        // Create lower directory with some files
        let lower_path = volume.join("lower");
        fs::create_dir_all(&lower_path).unwrap();
        create_test_file(&lower_path, "config.txt", "lower config");
        create_test_file(&lower_path, "allowed.txt", "allowed file");

        // Create upper directory with overlapping files
        let upper_path = volume.join("upper");
        fs::create_dir_all(&upper_path).unwrap();
        create_test_file(&upper_path, "config.txt", "upper config");
        create_test_file(&upper_path, "allowed.txt", "upper allowed");

        let lower_dir = LowerDir::new(lower_path, None).unwrap();
        let upper_dir = UpperDir::new(
            volume.clone(),
            PathBuf::from("upper"),
            PathBuf::from("work"),
            PathBuf::from("merged"),
        )
        .unwrap();

        let config = MountConfig {
            lower_dirs: vec![lower_dir],
            upper_dir,
            allowed_masked_files: vec![PathBuf::from("allowed.txt")].into_iter().collect(),
        };

        let result = config.validate();
        assert!(matches!(
            result,
            Err(ConfigError::ValidationError(ValidationError::MaskedFiles(
                _
            )))
        ));

        if let Err(ConfigError::ValidationError(ValidationError::MaskedFiles(masked_files))) =
            result
        {
            assert_eq!(masked_files.len(), 1);
            assert!(masked_files[0].ends_with("config.txt"));
            assert!(!masked_files.iter().any(|p| p.ends_with("allowed.txt")));
        }
    }

    #[test]
    fn test_mount_config_all_files_allowed() {
        let temp_dir = TempDir::new().unwrap();
        let volume = temp_dir.path().to_path_buf();

        // Create lower directory with some files
        let lower_path = volume.join("lower");
        fs::create_dir_all(&lower_path).unwrap();
        create_test_file(&lower_path, "config.txt", "lower config");
        create_test_file(&lower_path, "other.txt", "other file");

        // Create upper directory with overlapping files
        let upper_path = volume.join("upper");
        fs::create_dir_all(&upper_path).unwrap();
        create_test_file(&upper_path, "config.txt", "upper config");
        create_test_file(&upper_path, "other.txt", "upper other");

        let lower_dir = LowerDir::new(lower_path, None).unwrap();
        let upper_dir = UpperDir::new(
            volume.clone(),
            PathBuf::from("upper"),
            PathBuf::from("work"),
            PathBuf::from("merged"),
        )
        .unwrap();

        let config = MountConfig {
            lower_dirs: vec![lower_dir],
            upper_dir,
            allowed_masked_files: vec![PathBuf::from("config.txt"), PathBuf::from("other.txt")]
                .into_iter()
                .collect(),
        };

        let validated = config.validate().unwrap();
        assert!(matches!(validated, ValidatedMountConfig(_)));
    }

    #[test]
    fn test_validated_mount_config_conversion() {
        let temp_dir = TempDir::new().unwrap();
        let volume = temp_dir.path().to_path_buf();

        let lower_dir = LowerDir::new(volume.join("lower"), None).unwrap();
        let upper_dir = UpperDir::new(
            volume.clone(),
            PathBuf::from("upper"),
            PathBuf::from("work"),
            PathBuf::from("merged"),
        )
        .unwrap();

        let original_config = MountConfig {
            lower_dirs: vec![lower_dir.clone()],
            upper_dir: upper_dir.clone(),
            allowed_masked_files: BTreeSet::new(),
        };

        let validated = original_config.validate().unwrap();
        let converted_config: MountConfig = validated.into();

        assert_eq!(converted_config.lower_dirs.len(), 1);
        assert_eq!(converted_config.lower_dirs[0].volume, lower_dir.volume);
        assert_eq!(converted_config.upper_dir.volume, upper_dir.volume);
    }
}
