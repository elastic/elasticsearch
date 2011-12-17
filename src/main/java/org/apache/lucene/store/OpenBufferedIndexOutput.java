package org.apache.lucene.store;

import java.io.IOException;

/**
 * Exactly the same as Lucene {@link BufferedIndexOutput} but with the ability to set the buffer size
 */
// LUCENE MONITOR
public abstract class OpenBufferedIndexOutput extends IndexOutput {

    public static final int DEFAULT_BUFFER_SIZE = BufferedIndexOutput.BUFFER_SIZE;

    final int BUFFER_SIZE;

    private final byte[] buffer;
    private long bufferStart = 0;           // position in file of buffer
    private int bufferPosition = 0;         // position in buffer

    protected OpenBufferedIndexOutput(int BUFFER_SIZE) {
        this.BUFFER_SIZE = BUFFER_SIZE;
        this.buffer = new byte[BUFFER_SIZE];
    }

    /**
     * Writes a single byte.
     *
     * @see IndexInput#readByte()
     */
    @Override
    public void writeByte(byte b) throws IOException {
        if (bufferPosition >= BUFFER_SIZE)
            flush();
        buffer[bufferPosition++] = b;
    }

    /**
     * Writes an array of bytes.
     *
     * @param b      the bytes to write
     * @param length the number of bytes to write
     * @see IndexInput#readBytes(byte[], int, int)
     */
    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
        int bytesLeft = BUFFER_SIZE - bufferPosition;
        // is there enough space in the buffer?
        if (bytesLeft >= length) {
            // we add the data to the end of the buffer
            System.arraycopy(b, offset, buffer, bufferPosition, length);
            bufferPosition += length;
            // if the buffer is full, flush it
            if (BUFFER_SIZE - bufferPosition == 0)
                flush();
        } else {
            // is data larger then buffer?
            if (length > BUFFER_SIZE) {
                // we flush the buffer
                if (bufferPosition > 0)
                    flush();
                // and write data at once
                flushBuffer(b, offset, length);
                bufferStart += length;
            } else {
                // we fill/flush the buffer (until the input is written)
                int pos = 0; // position in the input data
                int pieceLength;
                while (pos < length) {
                    pieceLength = (length - pos < bytesLeft) ? length - pos : bytesLeft;
                    System.arraycopy(b, pos + offset, buffer, bufferPosition, pieceLength);
                    pos += pieceLength;
                    bufferPosition += pieceLength;
                    // if the buffer is full, flush it
                    bytesLeft = BUFFER_SIZE - bufferPosition;
                    if (bytesLeft == 0) {
                        flush();
                        bytesLeft = BUFFER_SIZE;
                    }
                }
            }
        }
    }

    /**
     * Forces any buffered output to be written.
     */
    @Override
    public void flush() throws IOException {
        flushBuffer(buffer, bufferPosition);
        bufferStart += bufferPosition;
        bufferPosition = 0;
    }

    /**
     * Expert: implements buffer write.  Writes bytes at the current position in
     * the output.
     *
     * @param b   the bytes to write
     * @param len the number of bytes to write
     */
    private void flushBuffer(byte[] b, int len) throws IOException {
        flushBuffer(b, 0, len);
    }

    /**
     * Expert: implements buffer write.  Writes bytes at the current position in
     * the output.
     *
     * @param b      the bytes to write
     * @param offset the offset in the byte array
     * @param len    the number of bytes to write
     */
    protected abstract void flushBuffer(byte[] b, int offset, int len) throws IOException;

    /**
     * Closes this stream to further operations.
     */
    @Override
    public void close() throws IOException {
        flush();
    }

    /**
     * Returns the current position in this file, where the next write will
     * occur.
     *
     * @see #seek(long)
     */
    @Override
    public long getFilePointer() {
        return bufferStart + bufferPosition;
    }

    /**
     * Sets current position in this file, where the next write will occur.
     *
     * @see #getFilePointer()
     */
    @Override
    public void seek(long pos) throws IOException {
        flush();
        bufferStart = pos;
    }

    /**
     * The number of bytes in the file.
     */
    @Override
    public abstract long length() throws IOException;


}
