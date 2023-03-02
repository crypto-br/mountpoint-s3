//! Utilities for building AWS SDKs

use std::sync::Once;

use aws_crt_s3_sys::aws_sdkutils_library_init;

use crate::common::allocator::Allocator;

pub mod endpoints_rule_engine;
pub mod partitions;

static SDKUTILS_LIBRARY_INIT: Once = Once::new();

/// Set up the aws-c-sdkutils library using the given allocator.
fn sdkutils_library_init(allocator: &Allocator) {
    SDKUTILS_LIBRARY_INIT.call_once(|| {
        // Safety: the CRT ensures this call happens only once.
        unsafe {
            aws_sdkutils_library_init(allocator.inner.as_ptr());
        }
    });
}
