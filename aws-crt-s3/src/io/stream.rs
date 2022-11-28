//! AWS input streams.

use crate::common::{allocator::Allocator, error::Error};
use crate::CrtError;
use aws_crt_s3_sys::*;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use std::collections::VecDeque;
use std::ffi::CString;
use std::fmt::Debug;
use std::io::{Cursor, Read};
use std::marker::PhantomData;
use std::os::unix::prelude::OsStrExt;
use std::path::Path;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use super::futures::FutureSpawner;

/// Status of an [InputStream].
#[derive(Debug)]
pub struct StreamStatus {
    is_valid: bool,
    is_end_of_stream: bool,
}

impl From<aws_stream_status> for StreamStatus {
    fn from(status: aws_stream_status) -> Self {
        Self {
            is_valid: status.is_valid,
            is_end_of_stream: status.is_end_of_stream,
        }
    }
}

impl From<StreamStatus> for aws_stream_status {
    fn from(status: StreamStatus) -> Self {
        Self {
            is_valid: status.is_valid,
            is_end_of_stream: status.is_end_of_stream,
        }
    }
}

/// Specifies where to seek from in an [InputStream].
#[derive(Debug)]
pub enum SeekBasis {
    /// Seek from the beginning of the stream.
    Begin,
    /// Seek from the end of the stream.
    End,
}

impl From<aws_stream_seek_basis> for SeekBasis {
    fn from(value: aws_stream_seek_basis) -> Self {
        match value {
            aws_stream_seek_basis::AWS_SSB_BEGIN => Self::Begin,
            aws_stream_seek_basis::AWS_SSB_END => Self::End,
            _ => panic!("invalid stream seek basis: {:?}", value),
        }
    }
}

impl From<SeekBasis> for aws_stream_seek_basis {
    fn from(value: SeekBasis) -> Self {
        match value {
            SeekBasis::Begin => aws_stream_seek_basis::AWS_SSB_BEGIN,
            SeekBasis::End => aws_stream_seek_basis::AWS_SSB_END,
        }
    }
}

/// An [InputStream] is a way to read bytes. They can be obtained either from CRT functions,
/// or by creating a new one based on a Rust type that implements the [GenericInputStream] trait.
#[derive(Debug)]
pub struct InputStream<'a> {
    /// The inner aws_input_stream
    pub(crate) inner: NonNull<aws_input_stream>,

    /// Phantom data to keep the lifetimes correct, for example, if this stream is created from an
    /// aws_byte_cursor that has some lifetime.
    _phantom: PhantomData<&'a [u8]>,
}

impl<'a> InputStream<'a> {
    /// Creates a stream that operates on a file opened from the provided path.
    pub fn new_from_file<P: AsRef<Path>>(allocator: &mut Allocator, path: &P) -> Result<Self, Error> {
        let path = CString::new(path.as_ref().as_os_str().as_bytes()).unwrap();

        let inner =
            // SAFETY: The allocator is valid and the path is a valid C string.
            unsafe { aws_input_stream_new_from_file(allocator.inner.as_ptr(), path.as_ptr()).ok_or_last_error()? };

        Ok(Self {
            inner,
            _phantom: Default::default(),
        })
    }

    /// Convert a [GenericInputStream] into an [InputStream] that we can use with CRT functions. The
    /// returned stream uses a vtable that "knows" how to call the trait methods for the stream
    /// object. It also knows how to map the CRT's acquire/release for proper reference counting.
    pub fn from_boxed_generic(stream: Box<dyn GenericInputStream>) -> Self {
        let ptr = Arc::new(GenericInputStreamWrapper {
            inner: aws_input_stream {
                // Point to the generic vtable
                vtable: &GENERIC_INPUT_STREAM_VTABLE,

                // ref_count is not used if we have our own acquire/release.
                ref_count: Default::default(),

                // Cannot set impl_ until we've created this Arc ptr.
                impl_: std::ptr::null_mut(),
            },
            stream,
        });

        // Turn out Arc pointer into a raw pointer (effectively leaking it).
        let ptr = Arc::into_raw(ptr) as *mut GenericInputStreamWrapper;

        // SAFETY: We know ptr isn't null because we just made it from Arc.
        unsafe {
            // Store the pointer into the aws_input_stream as `impl_`.
            // Now this structure is self-referential, since the GenericInputStream contains, through
            // aws_input_stream, a pointer to itself. We don't need Pin here though since the
            // GenericInputStream type is not exposed to users.
            (*ptr).inner.impl_ = ptr as *mut libc::c_void;

            // Wrap the inner aws_input_stream pointer into an [InputStream] object. We know it's not
            // null (we just created it), and the vtable functions below can undo this transformation
            // and go back to the wrapper struct by using the impl_ field we set just before this.
            Self {
                inner: NonNull::new_unchecked(&mut (*ptr).inner),
                _phantom: Default::default(),
            }
        }
    }

    /// Like [Self::from_boxed_generic] but unboxed.
    pub fn from_generic<S: GenericInputStream + 'static>(stream: S) -> Self {
        Self::from_boxed_generic(Box::new(stream))
    }

    /// Create a new [InputStream] from a slice. The slice is not copied, and so the resulting
    /// [InputStream] cannot outlive the slice (enforced by a lifetime restriction on the [InputStream].
    pub fn new_from_slice(allocator: &mut Allocator, buffer: &'a [u8]) -> Result<Self, Error> {
        let cursor = aws_byte_cursor {
            len: buffer.len(),
            ptr: buffer.as_ptr() as *mut u8,
        };

        // SAFETY: allocator is a valid aws_allocator. `Self` has a lifetime of 'a, so Rust
        // will ensure that the return value from this function doesn't out live the buffer.
        // We need to make sure in thigs that consume InputStream that we don't accidentally
        // cause it to live longer than expected. (Or just kill this function...)
        let inner = unsafe { aws_input_stream_new_from_cursor(allocator.inner.as_ptr(), &cursor).ok_or_last_error()? };

        Ok(Self {
            inner,
            _phantom: Default::default(),
        })
    }
}

impl<'a> Drop for InputStream<'a> {
    fn drop(&mut self) {
        // SAFETY: self.inner is a valid `aws_input_stream`.
        unsafe {
            aws_input_stream_release(self.inner.as_ptr());
        }
    }
}

/// A [GenericInputStream] describes a type that provides a way to read bytes into a buffer.
/// This allows Rust client code to define new ways of creating [InputStream]s.
pub trait GenericInputStream {
    /// Seek to the given offset. Basis is either BEGIN or END, and describes where to seek from.
    fn seek(&mut self, offset: i64, basis: SeekBasis) -> Result<(), Error>;

    /// Read some data into `buffer`, and return how many bytes were read.
    fn read(&mut self, buffer: &mut [u8]) -> Result<usize, Error>;

    /// Get the status of this stream. Can be used to indicate if stream is at the EOF.
    fn get_status(&self) -> Result<StreamStatus, Error>;

    /// Get the length of this input stream, in bytes. If a length cannot be determined, return Err.
    fn get_length(&self) -> Result<usize, Error>;
}

/// A [GenericInputStreamWrapper] is a way to turn objects of Rust types that implement the
/// [GenericInputStream] trait into a proper `aws_input_stream` we can use with the CRT.
struct GenericInputStreamWrapper {
    // The `aws_input_stream` that we can pass pointers to into the CRT functions.
    inner: aws_input_stream,

    // The client's stream, which implements the InputStream trait.
    stream: Box<dyn GenericInputStream>,
}

// A vtable for types that implement the [InputStream] trait so we can use them as an `aws_input_stream`.
static GENERIC_INPUT_STREAM_VTABLE: aws_input_stream_vtable = aws_input_stream_vtable {
    seek: Some(generic_seek),
    read: Some(generic_read),
    get_status: Some(generic_get_status),
    get_length: Some(generic_get_length),
    acquire: Some(generic_acquire),
    release: Some(generic_release),
};

/// Converts an aws_input_stream pointer into a GenericInputStream pointer.
/// Must only be called on streams that use the generic vtable.
/// The returned pointer will be an Arc pointer (i.e., it should be safe to use with Arc::from_raw).
/// This function doesn't do that automatically, since that will drop the the value if it's the last
/// Arc pointer, which isn't what we want unless we're in generic_release.
unsafe fn input_stream_to_generic_stream(stream: *mut aws_input_stream) -> *mut GenericInputStreamWrapper {
    assert!(!stream.is_null(), "stream should never be null");

    assert!(
        std::ptr::eq((*stream).vtable, &GENERIC_INPUT_STREAM_VTABLE),
        "this function should only be called on streams that use the generic vtable"
    );

    assert!(!(*stream).impl_.is_null(), "stream.impl_ should never be null");

    // SAFETY: The invariant of generic streams is that .impl_ always is an Arc ptr to a
    // GenericInputStream.
    let generic_ptr = (*stream).impl_ as *mut GenericInputStreamWrapper;

    assert!(
        std::ptr::eq(&(*generic_ptr).inner, stream),
        "&generic_input_stream.inner should be the same stream we started with"
    );

    generic_ptr
}

// SAFETY: Only should be used as a CRT callback. The suppressed improper ctypes warning warns us that
// aws_stream_seek_basis is not exhaustive and so the code is unsafe if the CRT passes us an invalid
// value: we trust that the CRT does not.
#[allow(improper_ctypes_definitions)]
unsafe extern "C" fn generic_seek(stream: *mut aws_input_stream, offset: i64, basis: aws_stream_seek_basis) -> i32 {
    let generic_stream = &mut *input_stream_to_generic_stream(stream);

    match generic_stream.stream.seek(offset, basis.into()) {
        Ok(()) => AWS_OP_SUCCESS,
        Err(err) => err.raise_error(),
    }
}

// SAFETY: Only should be used as a CRT callback.
unsafe extern "C" fn generic_read(stream: *mut aws_input_stream, dest: *mut aws_byte_buf) -> i32 {
    let generic_stream = &mut *input_stream_to_generic_stream(stream);
    let dest = dest.as_mut().expect("dest cannot be null");

    let buffer: &mut [u8] = if dest.capacity != 0 {
        assert!(!dest.buffer.is_null());
        std::slice::from_raw_parts_mut(dest.buffer, dest.capacity)
    } else {
        // It's tricky to use from_raw_parts with a size of 0 since the pointer must still be aligned.
        // So instead just make an empty slice.
        &mut []
    };

    assert!(dest.len <= dest.capacity, "invalid byte buffer");

    // reslice the buffer so that it starts at the place where new data should go.
    let buffer = &mut buffer[dest.len..];

    // call the generic read function from the trait, on this buffer.
    match generic_stream.stream.read(buffer) {
        // The read succeeded, update dest.len and return success.
        Ok(nread) => {
            assert!(nread <= buffer.len(), "cannot have read more than buffer size");
            dest.len += nread;
            assert!(dest.len <= dest.capacity, "len cannot be greater than capacity");
            AWS_OP_SUCCESS
        }

        Err(err) => err.raise_error(),
    }
}

// SAFETY: Only should be used as a CRT callback.
unsafe extern "C" fn generic_get_status(stream: *mut aws_input_stream, out_status: *mut aws_stream_status) -> i32 {
    let generic_stream = &*input_stream_to_generic_stream(stream);

    match generic_stream.stream.get_status() {
        Ok(status) => {
            (*out_status) = status.into();
            AWS_OP_SUCCESS
        }

        Err(err) => err.raise_error(),
    }
}

// SAFETY: Only should be used as a CRT callback.
unsafe extern "C" fn generic_get_length(stream: *mut aws_input_stream, out_length: *mut i64) -> i32 {
    let generic_stream = &*input_stream_to_generic_stream(stream);

    match generic_stream.stream.get_length() {
        Ok(length) => {
            *out_length = length.try_into().expect("Can't convert usize to i64");
            AWS_OP_SUCCESS
        }
        Err(err) => err.raise_error(),
    }
}

// SAFETY: Only should be used as a CRT callback.
unsafe extern "C" fn generic_acquire(stream: *mut aws_input_stream) {
    let generic_stream = input_stream_to_generic_stream(stream);

    // SAFETY: This is always an Arc pointer, since we know this is from the generic vtable.
    Arc::increment_strong_count(generic_stream);
}

// SAFETY: Only should be used as a CRT callback.
unsafe extern "C" fn generic_release(stream: *mut aws_input_stream) {
    let generic_stream = input_stream_to_generic_stream(stream);

    // SAFETY: This is always an Arc pointer, since we know this is from the generic vtable.
    // This will `drop` the stream's contents if it's the last pointer.
    Arc::decrement_strong_count(generic_stream);
}

// We can implement [GenericInputStream] for [InputStream] so that we can use any CRT-implemented
// input streams with the Rust trait-based interface.
impl<'a> GenericInputStream for InputStream<'a> {
    fn seek(&mut self, offset: i64, basis: SeekBasis) -> Result<(), Error> {
        // SAFETY: self.inner is a valid input stream.
        unsafe { aws_input_stream_seek(self.inner.as_ptr(), offset, basis.into()).ok_or_last_error() }
    }

    fn read(&mut self, buffer: &mut [u8]) -> Result<usize, Error> {
        let mut byte_buf = aws_byte_buf {
            len: 0,
            buffer: buffer.as_mut_ptr(),
            capacity: buffer.len(),
            allocator: std::ptr::null_mut(),
        };

        // SAFETY: we know that the aws_byte_buf we just made points to a valid buffer, and trust
        // the CRT function not to write outside that buffer's capacity. Also, self.inner is a
        // valid input stream.
        unsafe {
            aws_input_stream_read(self.inner.as_ptr(), &mut byte_buf).ok_or_last_error()?;
        };

        assert_eq!(byte_buf.capacity, buffer.len(), "capacity should not change");

        assert!(
            byte_buf.len <= buffer.len(),
            "should not have written more than available"
        );
        Ok(byte_buf.len)
    }

    fn get_status(&self) -> Result<StreamStatus, Error> {
        let mut status: aws_stream_status = Default::default();

        // SAFETY: self.inner is a valid input stream and status is a local variable.
        unsafe {
            aws_input_stream_get_status(self.inner.as_ptr(), &mut status).ok_or_last_error()?;
        }

        Ok(status.into())
    }

    fn get_length(&self) -> Result<usize, Error> {
        let mut out_length: i64 = 0;

        // SAFETY: self.inner is a valid input stream and out_length is a pointer to a local variable.
        unsafe {
            aws_input_stream_get_length(self.inner.as_ptr(), &mut out_length).ok_or_last_error()?;
        }

        Ok(out_length.try_into().expect("failed to convert i64 to usize"))
    }
}

/// An [AsyncInputStream] wraps an implementation of [AsyncRead] into an implementation of
/// [GenericInputStream], using a [FutureSpawner] (like an [EventLoop]) to handle background
/// asynchronous tasks. Due to CRT limitations, the InputStream API currently has no way to notify
/// the CRT when data becomes available. TODO: change that.
pub struct AsyncInputStream<B, Spawner>
where
    B: AsRef<[u8]> + Send + 'static,
    Spawner: FutureSpawner,
{
    /// A pointer to the inner implementor of [AsyncRead]. If it's None, that means we already have
    /// a scheduled task to read from it.
    stream_ptr: Arc<Mutex<Option<BoxStream<'static, B>>>>,

    /// The method by which to spawn futures to await upon reads in.
    spawner: Spawner,

    /// A queue of available buffers that we can give to the CRT on request.
    buffers: Arc<Mutex<VecDeque<Cursor<B>>>>,

    finished: Arc<AtomicBool>,
}

impl<B, Spawner> Debug for AsyncInputStream<B, Spawner>
where
    B: AsRef<[u8]> + Send + 'static,
    Spawner: FutureSpawner,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncInputStream").finish()
    }
}

impl<B, Spawner> AsyncInputStream<B, Spawner>
where
    B: AsRef<[u8]> + Send + 'static,
    Spawner: FutureSpawner,
{
    /// Create a new [AsyncInputStream] from an async stream that yields buffers.
    pub fn new(spawner: Spawner, stream: impl Stream<Item = B> + Send + 'static) -> Self {
        Self {
            stream_ptr: Arc::new(Mutex::new(Some(stream.boxed()))),
            spawner,
            buffers: Default::default(),
            finished: Arc::new(AtomicBool::new(false)),
        }
    }

    // Schedule the buffers to be refilled. If a refill is already scheduled, or if the stream
    // is done, does nothing.
    fn schedule_refill(&self) {
        // If the stream is present, schedule another refill asynchronously. Otherwise,
        // there's either a concurrently running refill, or the stream is finished yielding elements.
        if let Some(mut stream) = self.stream_ptr.lock().unwrap().take() {
            let stream_ptr = Arc::clone(&self.stream_ptr);
            let buffers = Arc::clone(&self.buffers);
            let finished = Arc::clone(&self.finished);

            // Spawn a future to read from the reader onto the scheduler (e.g., EventLoop).
            self.spawner.spawn_future(async move {
                if let Some(next_buffer) = stream.next().await {
                    // Put the reader back so that refill() can work again. Do this before adding the new
                    // buffer to prevent the case where there is no data left in the buffers, but refill()
                    // won't work because we haven't put this back.
                    stream_ptr.lock().unwrap().replace(stream);

                    // Push the next buffer onto the queue of buffers.
                    buffers.lock().unwrap().push_back(Cursor::new(next_buffer));

                    // TODO: Notify the CRT with a callback that more data has arrived.
                    // It is not possible to do so with the current aws_input_stream interface.
                } else {
                    finished.store(true, Ordering::SeqCst);
                }
            });
        }
    }
}

impl<B, Spawner> GenericInputStream for AsyncInputStream<B, Spawner>
where
    B: AsRef<[u8]> + Send + 'static,
    Spawner: FutureSpawner + Sync,
{
    fn seek(&mut self, _offset: i64, _basis: SeekBasis) -> Result<(), Error> {
        // Cannot support seeking within in async stream.
        Err((aws_common_error::AWS_ERROR_UNSUPPORTED_OPERATION as i32).into())
    }

    fn read(&mut self, out_buffer: &mut [u8]) -> Result<usize, Error> {
        let mut buffers = self.buffers.lock().unwrap();

        let mut total_bytes_read = 0;

        // Read as much as we can into the CRT buffer.
        while total_bytes_read < out_buffer.len() {
            // Grab the first buffer in the queue (if it exists).
            if let Some(buffer) = buffers.get_mut(0) {
                // Read as many new bytes as we can into out_buffer from the next cursor.
                let bytes_read = buffer
                    .read(&mut out_buffer[total_bytes_read..])
                    .expect("read from Cursor should not fail");

                // Until [Cursor::is_empty] is stable, check for EOF when read returns 0.
                // Remove the cursor if it's empty.
                if bytes_read == 0 {
                    buffers.pop_front().unwrap();
                }

                total_bytes_read += bytes_read;
            } else {
                // No buffers left
                break;
            }
        }

        // If we weren't able to read anything, schedule a refill so that maybe next time CRT calls
        // us we'll have data.
        if total_bytes_read == 0 {
            self.schedule_refill()
        }

        Ok(total_bytes_read)
    }

    fn get_status(&self) -> Result<StreamStatus, Error> {
        Ok(StreamStatus {
            is_valid: true,
            is_end_of_stream: self.finished.load(Ordering::SeqCst),
        })
    }

    fn get_length(&self) -> Result<usize, Error> {
        // Cannot support getting the length of an async stream.
        Err((aws_common_error::AWS_ERROR_UNSUPPORTED_OPERATION as i32).into())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::common::allocator::Allocator;
    use crate::io::event_loop::EventLoopGroup;
    use crate::io::stream::InputStream;

    // An implementation of [GenericInputStream] that always reads 0s.
    #[derive(Debug)]
    struct ZeroStream;

    impl GenericInputStream for ZeroStream {
        fn seek(&mut self, _offset: i64, _basis: SeekBasis) -> Result<(), Error> {
            Ok(())
        }

        fn read(&mut self, buffer: &mut [u8]) -> Result<usize, Error> {
            buffer.fill(0u8);
            Ok(buffer.len())
        }

        fn get_status(&self) -> Result<StreamStatus, Error> {
            Ok(StreamStatus {
                is_end_of_stream: false,
                is_valid: true,
            })
        }

        fn get_length(&self) -> Result<usize, Error> {
            // Cannot return size because it's infinite
            Err(Error::from(0))
        }
    }

    #[test]
    fn test_slice_cursor() {
        let mut allocator = Allocator::default();

        let bytes = b"Hello world!".to_vec();

        // Create a new CRT input stream from this slice.
        let mut stream =
            InputStream::new_from_slice(&mut allocator, &bytes[..]).expect("failed to make input stream from slice");

        let mut buffer = vec![0u8; 40];

        let nread = stream.read(&mut buffer).expect("read failed");

        assert_eq!(nread, bytes.len());
        assert_eq!(&buffer[..nread], &bytes[..]);

        let status = stream.get_status().expect("get_status failed");

        assert!(status.is_end_of_stream);

        let length = stream.get_length().expect("get_length failed");

        assert_eq!(length, bytes.len());
    }

    /// Test that the generic stream API works by making a really deep stack of
    /// nested generic streams.
    #[test]
    fn test_deep_generic_stream_stack() {
        let mut stream: Box<dyn GenericInputStream> = Box::new(ZeroStream);

        for _ in 0..100 {
            stream = Box::new(InputStream::from_boxed_generic(stream));
        }

        let mut buffer = vec![0xffu8; 40];

        let nread = stream.read(&mut buffer).expect("read failed");
        assert_eq!(nread, buffer.len());
    }

    #[test]
    fn test_async_input_stream() {
        let mut allocator = Allocator::default();

        let el_group = EventLoopGroup::new_default(&mut allocator, None, || {}).unwrap();
        let event_loop = el_group.get_next_loop().unwrap();

        let contents: Vec<u8> = (0..255).collect();

        let futures_stream = futures::stream::iter(contents.chunks(16).map(|b| b.to_owned()).collect::<Vec<_>>());

        let mut input_stream = AsyncInputStream::new(event_loop, futures_stream);

        let mut buffer = vec![0u8; contents.len()];
        let mut offset = 0;
        while !input_stream.get_status().unwrap().is_end_of_stream {
            offset += input_stream.read(&mut buffer[offset..]).unwrap();
        }
    }
}
