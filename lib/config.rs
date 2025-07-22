use serde::Deserialize;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

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

#[derive(Debug, Clone, Deserialize)]
pub struct LowerDir {
    volume: PathBuf,
    subdir: Option<PathBuf>,
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
        Ok(Self { volume, subdir })
    }

    pub fn full_path(&self) -> PathBuf {
        match &self.subdir {
            Some(subdir) => self.volume.join(subdir),
            None => self.volume.clone(),
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
pub struct MountConfig {
    pub lower_dirs: Vec<LowerDir>,
    pub upper_dir: UpperDir,
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
            if upper_file_path.exists() {
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
