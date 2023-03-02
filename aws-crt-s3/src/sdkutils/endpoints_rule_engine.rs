//! Engine for executing endpoint resolution rules

use std::ptr::NonNull;

use aws_crt_s3_sys::{
    aws_byte_cursor, aws_endpoints_request_context, aws_endpoints_request_context_add_boolean,
    aws_endpoints_request_context_add_string, aws_endpoints_request_context_new, aws_endpoints_request_context_release,
    aws_endpoints_resolved_endpoint, aws_endpoints_resolved_endpoint_get_error,
    aws_endpoints_resolved_endpoint_get_type, aws_endpoints_resolved_endpoint_get_url,
    aws_endpoints_resolved_endpoint_release, aws_endpoints_resolved_endpoint_type, aws_endpoints_rule_engine,
    aws_endpoints_rule_engine_new, aws_endpoints_rule_engine_release, aws_endpoints_rule_engine_resolve,
    aws_endpoints_ruleset, aws_endpoints_ruleset_new_from_string, aws_endpoints_ruleset_release,
};

use crate::common::allocator::Allocator;
use crate::common::error::Error;
use crate::sdkutils::partitions::PartitionsConfig;
use crate::sdkutils::sdkutils_library_init;
use crate::{aws_byte_cursor_as_slice, CrtError, StringExt};

/// An engine for resolving endpoints
#[derive(Debug)]
pub struct RuleEngine {
    inner: NonNull<aws_endpoints_rule_engine>,
}

impl RuleEngine {
    /// Construct a new rule engine that will evaluate the given ruleset
    pub fn new(allocator: &Allocator, ruleset: &Ruleset, partitions_config: &PartitionsConfig) -> Result<Self, Error> {
        // SAFETY: all arguments are valid, and the constructor copies what it needs out of them
        let inner = unsafe {
            aws_endpoints_rule_engine_new(
                allocator.inner.as_ptr(),
                ruleset.inner.as_ptr(),
                partitions_config.inner.as_ptr(),
            )
            .ok_or_last_error()?
        };

        Ok(Self { inner })
    }

    /// Resolve an endpoint in the given request context
    pub fn resolve(&self, context: &RequestContext) -> Result<ResolvedEndpoint, EndpointResolutionError> {
        let mut output: *mut aws_endpoints_resolved_endpoint = std::ptr::null_mut();

        // SAFETY: all arguments are valid, and Rust ensures `&mut output` isn't dangling
        unsafe {
            aws_endpoints_rule_engine_resolve(self.inner.as_ptr(), context.inner.as_ptr(), &mut output)
                .ok_or_last_error()?;
        }

        let resolved = NonNull::new(output).expect("pointer should be non-null if resolve succeeded");

        // SAFETY: aws_endpoints_rule_engine_resolve succeeded so `resolved` is valid
        let typ = unsafe { aws_endpoints_resolved_endpoint_get_type(resolved.as_ptr()) };

        match typ {
            aws_endpoints_resolved_endpoint_type::AWS_ENDPOINTS_RESOLVED_ENDPOINT => {
                Ok(ResolvedEndpoint { inner: resolved })
            }
            aws_endpoints_resolved_endpoint_type::AWS_ENDPOINTS_RESOLVED_ERROR => {
                let mut byte_cursor: aws_byte_cursor = Default::default();
                // SAFETY: in this branch we know it's an ERROR type, so `get_error` will succeed,
                // and so `byte_cursor` will be a valid string that we'll copy out of.
                let err = unsafe {
                    aws_endpoints_resolved_endpoint_get_error(resolved.as_ptr(), &mut byte_cursor)
                        .ok_or_last_error()?;
                    String::from_utf8(aws_byte_cursor_as_slice(&byte_cursor).to_vec()).unwrap()
                };
                Err(EndpointResolutionError::FailedResolution(err))
            }
            _ => panic!("unknown endpoint type {typ:?}"),
        }
    }
}

impl Drop for RuleEngine {
    fn drop(&mut self) {
        // SAFETY: `self.inner` is still valid
        unsafe { aws_endpoints_rule_engine_release(self.inner.as_ptr()) };
    }
}

/// A set of rules to be used by a rules engine
#[derive(Debug)]
pub struct Ruleset {
    inner: NonNull<aws_endpoints_ruleset>,
}

impl Ruleset {
    /// Create a new ruleset from a JSON blob.
    pub fn new(allocator: &Allocator, json: &str) -> Result<Self, Error> {
        sdkutils_library_init(allocator);

        // SAFETY: the constructor copies the information it needs out of this string
        let byte_cursor = unsafe { json.as_aws_byte_cursor() };

        let inner =
            // SAFETY: `allocator` is a valid allocator and `byte_cursor` a valid byte cursor
            unsafe { aws_endpoints_ruleset_new_from_string(allocator.inner.as_ptr(), byte_cursor).ok_or_last_error()? };

        Ok(Self { inner })
    }
}

impl Drop for Ruleset {
    fn drop(&mut self) {
        // SAFETY: `self.inner` must still be valid
        unsafe { aws_endpoints_ruleset_release(self.inner.as_ptr()) };
    }
}

/// A resolved endpoint returned by an [EndpointsRuleEngine].
// Invariant: this has endpoint type AWS_ENDPOINTS_RESOLVED_ENDPOINT, other cases are handled
// separately.
#[derive(Debug)]
pub struct ResolvedEndpoint {
    inner: NonNull<aws_endpoints_resolved_endpoint>,
}

impl ResolvedEndpoint {
    /// Get the URL for the resolved endpoint
    pub fn url(&self) -> Result<String, Error> {
        let mut byte_cursor: aws_byte_cursor = Default::default();
        // SAFETY: by the invariant, we know this is an ENDPOINT type, so `get_url` will succeed,
        // and so `byte_cursor` will be a valid string that we'll copy out of.
        let url = unsafe {
            aws_endpoints_resolved_endpoint_get_url(self.inner.as_ptr(), &mut byte_cursor).ok_or_last_error()?;
            String::from_utf8(aws_byte_cursor_as_slice(&byte_cursor).to_vec()).unwrap()
        };
        Ok(url)
    }
}

impl Drop for ResolvedEndpoint {
    fn drop(&mut self) {
        // SAFETY: `self.inner` must still be valid
        unsafe { aws_endpoints_resolved_endpoint_release(self.inner.as_ptr()) };
    }
}

/// The context for an endpoint resolution request (a bag of properties). Keys are always strings,
/// and values can be strings or booleans.
#[derive(Debug)]
pub struct RequestContext {
    inner: NonNull<aws_endpoints_request_context>,
}

impl RequestContext {
    /// Create a new request context ready to be populated
    pub fn new(allocator: &Allocator) -> Result<Self, Error> {
        // SAFETY: the allocator is valid
        let inner = unsafe { aws_endpoints_request_context_new(allocator.inner.as_ptr()).ok_or_last_error()? };

        Ok(Self { inner })
    }

    /// Add a new key-value pair to the context. If the name exists, this will overwrite the
    /// previous value.
    pub fn insert(&mut self, allocator: &Allocator, name: &str, value: Value<'_>) -> Result<(), Error> {
        // SAFETY: `self.inner` is a valid request context, and we know `name` and `value` are valid
        // Rust strings (in the Value::String case).
        unsafe {
            match value {
                Value::String(s) => aws_endpoints_request_context_add_string(
                    allocator.inner.as_ptr(),
                    self.inner.as_ptr(),
                    name.as_aws_byte_cursor(),
                    s.as_aws_byte_cursor(),
                )
                .ok_or_last_error(),
                Value::Boolean(b) => aws_endpoints_request_context_add_boolean(
                    allocator.inner.as_ptr(),
                    self.inner.as_ptr(),
                    name.as_aws_byte_cursor(),
                    b,
                )
                .ok_or_last_error(),
            }
        }
    }
}

impl Drop for RequestContext {
    fn drop(&mut self) {
        // SAFETY: `self.inner` must still be valid
        unsafe { aws_endpoints_request_context_release(self.inner.as_ptr()) };
    }
}

/// Values in a request context can be either strings or booleans
#[derive(Debug)]
pub enum Value<'a> {
    /// A string
    String(&'a str),
    /// A bool
    Boolean(bool),
}

/// Errors in endpoint resolution
#[derive(Debug, thiserror::Error)]
pub enum EndpointResolutionError {
    /// An internal error occurred during resolution
    #[error("internal error")]
    InternalError(#[from] Error),

    /// The resolution resulted in an error
    #[error("failed resolution: {0}")]
    FailedResolution(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    /// https://github.com/awslabs/aws-c-sdkutils/blob/c98a34108aaeb9a7787e54873e579c9b7d27cd04/tests/resources/sample_partitions.json
    const SAMPLE_PARTITIONS: &str = r#"
{
    "version": "1.1",
    "partitions": [
      {
        "id": "aws",
        "regionRegex": "^(us|eu|ap|sa|ca|me|af)-\\w+-\\d+$",
        "regions": {
          "af-south-1": {
            "supportsFIPS": false 
          },
          "eu-west-1": {},
          "us-east-2": {},
          "us-west-2": {
            "description" : "US West (Oregon)"
          },
          "aws-global": {}
        },
        "outputs": {
          "name": "aws",
          "dnsSuffix": "amazonaws.com",
          "dualStackDnsSuffix": "api.aws",
          "supportsFIPS": true,
          "supportsDualStack": true
        }
      }
    ]
}"#;

    /// https://github.com/awslabs/aws-c-sdkutils/blob/c98a34108aaeb9a7787e54873e579c9b7d27cd04/tests/resources/sample_ruleset.json
    const SAMPLE_RULESET: &str = r#"
{
  "version": "1.0",
  "serviceId": "example",
  "parameters": {
    "Region": {
      "type": "string",
      "builtIn": "AWS::Region",
      "documentation": "The region to dispatch the request to"
    }
  },
  "rules": [
    {
      "documentation": "rules for when region isSet",
      "type": "tree",
      "conditions": [
        {"fn": "isSet", "argv": [{"ref": "Region"}]}
      ],
      "rules": [
        {
          "type": "endpoint",
          "conditions": [
            {"fn": "aws.partition", "argv": [{"ref": "Region"}], "assign": "partitionResult"}
          ],
          "endpoint": {"url": "https://example.{Region}.{partitionResult#dnsSuffix}"}
        },
        {
          "type": "error",
          "documentation": "invalid region value",
          "conditions": [],
          "error": "unable to determine endpoint for region: {Region}"
        }
      ]
    },
    {
      "type": "endpoint",
      "documentation": "the single service global endpoint",
      "conditions": [],
      "endpoint": {"url": "https://example.amazonaws.com"}
    }
  ]
}"#;

    #[test]
    fn rule_engine() {
        let allocator = Allocator::default();

        let ruleset = Ruleset::new(&allocator, SAMPLE_RULESET).unwrap();
        let partitions_config = PartitionsConfig::new(&allocator, SAMPLE_PARTITIONS).unwrap();

        let engine = RuleEngine::new(&allocator, &ruleset, &partitions_config).unwrap();

        let mut context = RequestContext::new(&allocator).unwrap();
        context
            .insert(&allocator, "Region", Value::String("us-west-2"))
            .unwrap();

        let resolved = engine.resolve(&context).expect("resolution should succeed");

        let url = resolved.url().unwrap();
        assert_eq!(url, "https://example.us-west-2.amazonaws.com");
    }
}
