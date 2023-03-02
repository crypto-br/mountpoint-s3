//! Configuration for AWS partitions

use std::ptr::NonNull;

use aws_crt_s3_sys::{aws_partitions_config, aws_partitions_config_new_from_string, aws_partitions_config_release};

use crate::common::allocator::Allocator;
use crate::common::error::Error;
use crate::sdkutils::sdkutils_library_init;
use crate::{CrtError, StringExt};

/// Metadata about available AWS partitions
#[derive(Debug)]
pub struct PartitionsConfig {
    pub(crate) inner: NonNull<aws_partitions_config>,
}

impl PartitionsConfig {
    /// Create a new partition configuration from a `partitions.json` SDK input file
    pub fn new(allocator: &Allocator, json: &str) -> Result<Self, Error> {
        sdkutils_library_init(allocator);

        // SAFETY: the constructor copies the information it needs out of this string
        let byte_cursor = unsafe { json.as_aws_byte_cursor() };

        let inner =
            // SAFETY: `allocator` is a valid allocator and `byte_cursor` a valid byte cursor
            unsafe { aws_partitions_config_new_from_string(allocator.inner.as_ptr(), byte_cursor).ok_or_last_error()? };

        Ok(Self { inner })
    }
}

impl Drop for PartitionsConfig {
    fn drop(&mut self) {
        // SAFETY: `self.inner` must still be valid
        unsafe { aws_partitions_config_release(self.inner.as_ptr()) };
    }
}
