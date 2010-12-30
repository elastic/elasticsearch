package org.elasticsearch.common.compress.lzf;

import java.io.IOException;
import java.io.InputStream;

public class LZFInputStream extends InputStream {
    public static final int EOF_FLAG = -1;

    /* stream to be decompressed */
    private final InputStream inputStream;

    /* the current buffer of compressed bytes */
    private final byte[] compressedBytes = new byte[LZFChunk.MAX_CHUNK_LEN];

    /* the buffer of uncompressed bytes from which */
    private final byte[] uncompressedBytes = new byte[LZFChunk.MAX_CHUNK_LEN];

    /* The current position (next char to output) in the uncompressed bytes buffer. */
    private int bufferPosition = 0;

    /* Length of the current uncompressed bytes buffer */
    private int bufferLength = 0;

    public LZFInputStream(final InputStream inputStream) throws IOException {
        super();
        this.inputStream = inputStream;
    }

    @Override
    public int read() throws IOException {
        int returnValue = EOF_FLAG;
        readyBuffer();
        if (bufferPosition < bufferLength) {
            returnValue = (uncompressedBytes[bufferPosition++] & 255);
        }
        return returnValue;
    }

    public int read(final byte[] buffer) throws IOException {
        return (read(buffer, 0, buffer.length));

    }

    public int read(final byte[] buffer, final int offset, final int length) throws IOException {
        // FIXED HERE: handle 0 length cases
        if (length == 0) {
            return 0;
        }
        int outputPos = offset;
        readyBuffer();
        if (bufferLength == -1) {
            return -1;
        }

        // FIXED HERE: fixed to use length
        while (outputPos < length && bufferPosition < bufferLength) {
            int chunkLength = Math.min(bufferLength - bufferPosition, length - outputPos);
            System.arraycopy(uncompressedBytes, bufferPosition, buffer, outputPos, chunkLength);
            outputPos += chunkLength;
            bufferPosition += chunkLength;
            readyBuffer();
        }
        return outputPos - offset;
    }

    public void close() throws IOException {
        inputStream.close();
    }

    /**
     * Fill the uncompressed bytes buffer by reading the underlying inputStream.
     *
     * @throws IOException
     */
    private void readyBuffer() throws IOException {
        if (bufferPosition >= bufferLength) {
            bufferLength = LZFDecoder.decompressChunk(inputStream, compressedBytes, uncompressedBytes);
            bufferPosition = 0;
        }
    }
}