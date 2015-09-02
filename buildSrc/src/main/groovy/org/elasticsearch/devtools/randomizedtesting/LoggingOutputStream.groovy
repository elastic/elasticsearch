package org.elasticsearch.devtools.randomizedtesting

import org.gradle.api.logging.LogLevel
import org.gradle.api.logging.Logger

public class LoggingOutputStream extends OutputStream {

    static final int DEFAULT_BUFFER_LENGTH = 4096;

    byte[] buffer = new byte[DEFAULT_BUFFER_LENGTH];
    int used = 0;

    Logger logger;
    LogLevel level;

    public void write(final int b) throws IOException {
        if (b == 0) return;

        if (used == buffer.length) {
            final int newBufferLength = buffer.length + DEFAULT_BUFFER_LENGTH;
            final byte[] newBuffer = new byte[newBufferLength];
            System.arraycopy(buffer, 0, newBuffer, 0, buffer.length);
            buffer = newBuffer;
        }

        buffer[used] = (byte) b;
        used++;
    }

    public void flush() {
        if (used == 0) return;
        logger.log(level, new String(buffer, 0, used));
        used = 0;
    }
}
