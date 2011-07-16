package org.elasticsearch.common.compress.lzf;

import java.io.IOException;
import java.io.InputStream;

public class LZFInputStream extends InputStream {
    private final BufferRecycler _recycler;

    /**
     * stream to be decompressed
     */
    protected final InputStream inputStream;

    /**
     * Flag that indicates if we have already called 'inputStream.close()'
     * (to avoid calling it multiple times)
     */
    protected boolean inputStreamClosed;

    /**
     * Flag that indicates whether we force full reads (reading of as many
     * bytes as requested), or 'optimal' reads (up to as many as available,
     * but at least one). Default is false, meaning that 'optimal' read
     * is used.
     */
    protected boolean cfgFullReads = false;

    /* the current buffer of compressed bytes (from which to decode) */
    private byte[] _inputBuffer;

    /* the buffer of uncompressed bytes from which content is read */
    private byte[] _decodedBytes;

    /* The current position (next char to output) in the uncompressed bytes buffer. */
    private int bufferPosition = 0;

    /* Length of the current uncompressed bytes buffer */
    private int bufferLength = 0;

    /*
    ///////////////////////////////////////////////////////////////////////
    // Construction
    ///////////////////////////////////////////////////////////////////////
     */

    public LZFInputStream(final InputStream inputStream) throws IOException {
        this(inputStream, false);
    }

    /**
     * @param inputStream Underlying input stream to use
     * @param fullReads   Whether {@link #read(byte[])} should try to read exactly
     *                    as many bytes as requested (true); or just however many happen to be
     *                    available (false)
     */
    public LZFInputStream(final InputStream in, boolean fullReads) throws IOException {
        super();
        _recycler = BufferRecycler.instance();
        inputStream = in;
        inputStreamClosed = false;
        cfgFullReads = fullReads;

        _inputBuffer = _recycler.allocInputBuffer(LZFChunk.MAX_CHUNK_LEN);
        _decodedBytes = _recycler.allocDecodeBuffer(LZFChunk.MAX_CHUNK_LEN);
    }

    /*
   ///////////////////////////////////////////////////////////////////////
   // InputStream impl
   ///////////////////////////////////////////////////////////////////////
    */

    /**
     * Method is overridden to report number of bytes that can now be read
     * from decoded data buffer, without reading bytes from the underlying
     * stream.
     * Never throws an exception; returns number of bytes available without
     * further reads from underlying source; -1 if stream has been closed, or
     * 0 if an actual read (and possible blocking) is needed to find out.
     */
    @Override
    public int available() {
        // if closed, return -1;
        if (inputStreamClosed) {
            return -1;
        }
        int left = (bufferLength - bufferPosition);
        return (left <= 0) ? 0 : left;
    }

    @Override
    public int read() throws IOException {
        if (!readyBuffer()) {
            return -1;
        }
        return _decodedBytes[bufferPosition++] & 255;
    }

    @Override
    public int read(final byte[] buffer) throws IOException {
        return read(buffer, 0, buffer.length);
    }

    @Override
    public int read(final byte[] buffer, int offset, int length) throws IOException {
        if (length < 1) {
            return 0;
        }
        if (!readyBuffer()) {
            return -1;
        }
        // First let's read however much data we happen to have...
        int chunkLength = Math.min(bufferLength - bufferPosition, length);
        System.arraycopy(_decodedBytes, bufferPosition, buffer, offset, chunkLength);
        bufferPosition += chunkLength;

        if (chunkLength == length || !cfgFullReads) {
            return chunkLength;
        }
        // Need more data, then
        int totalRead = chunkLength;
        do {
            offset += chunkLength;
            if (!readyBuffer()) {
                break;
            }
            chunkLength = Math.min(bufferLength - bufferPosition, (length - totalRead));
            System.arraycopy(_decodedBytes, bufferPosition, buffer, offset, chunkLength);
            bufferPosition += chunkLength;
            totalRead += chunkLength;
        } while (totalRead < length);

        return totalRead;
    }

    @Override
    public void close() throws IOException {
        bufferPosition = bufferLength = 0;
        byte[] buf = _inputBuffer;
        if (buf != null) {
            _inputBuffer = null;
            _recycler.releaseInputBuffer(buf);
        }
        buf = _decodedBytes;
        if (buf != null) {
            _decodedBytes = null;
            _recycler.releaseDecodeBuffer(buf);
        }
        if (!inputStreamClosed) {
            inputStreamClosed = true;
            inputStream.close();
        }
    }

    /*
    ///////////////////////////////////////////////////////////////////////
    // Additional public accessors
    ///////////////////////////////////////////////////////////////////////
     */

    /**
     * Method that can be used to find underlying {@link InputStream} that
     * we read from to get LZF encoded data to decode.
     * Will never return null; although underlying stream may be closed
     * (if this stream has been closed).
     *
     * @since 0.8
     */
    public InputStream getUnderlyingInputStream() {
        return inputStream;
    }

    /*
   ///////////////////////////////////////////////////////////////////////
   // Internal methods
   ///////////////////////////////////////////////////////////////////////
    */

    /**
     * Fill the uncompressed bytes buffer by reading the underlying inputStream.
     *
     * @throws IOException
     */
    protected boolean readyBuffer() throws IOException {
        if (bufferPosition < bufferLength) {
            return true;
        }
        if (inputStreamClosed) {
            return false;
        }
        bufferLength = LZFDecoder.decompressChunk(inputStream, _inputBuffer, _decodedBytes);
        if (bufferLength < 0) {
            return false;
        }
        bufferPosition = 0;
        return (bufferPosition < bufferLength);
    }
}
