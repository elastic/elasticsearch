package com.carrotsearch.gradle.junit4

import org.gradle.api.logging.LogLevel
import org.gradle.api.logging.Logger

/**
 * Writes data passed to this stream as log messages.
 *
 * The stream will be flushed whenever a newline is detected.
 * Allows setting an optional prefix before each line of output.
 */
public class LoggingOutputStream extends OutputStream {

    /** The starting length of the buffer */
    static final int DEFAULT_BUFFER_LENGTH = 4096

    /** The buffer of bytes sent to the stream */
    byte[] buffer = new byte[DEFAULT_BUFFER_LENGTH]

    /** Offset of the start of unwritten data in the buffer */
    int start = 0

    /** Offset of the end (semi-open) of unwritten data in the buffer */
    int end = 0

    /** Logger to write stream data to */
    Logger logger

    /** Prefix to add before each line of output */
    String prefix = ""

    /** Log level to write log messages to */
    LogLevel level

    void write(final int b) throws IOException {
        if (b == 0) return;
        if (b == (int)'\n' as char) {
            // always flush with newlines instead of adding to the buffer
            flush()
            return
        }

        if (end == buffer.length) {
            if (start != 0) {
                // first try shifting the used buffer back to the beginning to make space
                System.arraycopy(buffer, start, buffer, 0, end - start)
            } else {
                // need more space, extend the buffer
            }
            final int newBufferLength = buffer.length + DEFAULT_BUFFER_LENGTH;
            final byte[] newBuffer = new byte[newBufferLength];
            System.arraycopy(buffer, 0, newBuffer, 0, buffer.length);
            buffer = newBuffer;
        }

        buffer[end++] = (byte) b;
    }

    void flush() {
        if (end == start) return
        logger.log(level, prefix + new String(buffer, start, end - start));
        start = end
    }
}
