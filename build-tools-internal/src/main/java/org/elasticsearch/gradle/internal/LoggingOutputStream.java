/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

/**
 * Writes data passed to this stream as log messages.
 *
 * The stream will be flushed whenever a newline is detected.
 * Allows setting an optional prefix before each line of output.
 */
public abstract class LoggingOutputStream extends OutputStream {
    /** The starting length of the buffer */
    private static final int DEFAULT_BUFFER_LENGTH = 4096;

    /** The buffer of bytes sent to the stream */
    private byte[] buffer = new byte[DEFAULT_BUFFER_LENGTH];

    /** Offset of the start of unwritten data in the buffer */
    private int start = 0;

    /** Offset of the end (semi-open) of unwritten data in the buffer */
    private int end = 0;

    @Override
    public void write(final int b) throws IOException {
        if (b == 0) {
            return;
        }
        if (b == '\n') {
            // always flush with newlines instead of adding to the buffer
            flush();
            return;
        }

        if (end == buffer.length) {
            if (start != 0) {
                // first try shifting the used buffer back to the beginning to make space
                int len = end - start;
                System.arraycopy(buffer, start, buffer, 0, len);
                start = 0;
                end = len;
            } else {
                // otherwise extend the buffer
                buffer = Arrays.copyOf(buffer, buffer.length + DEFAULT_BUFFER_LENGTH);
            }
        }

        buffer[end++] = (byte) b;
    }

    @Override
    public void flush() {
        if (end == start) {
            return;
        }
        logLine(new String(buffer, start, end - start));
        start = end;
    }

    protected abstract void logLine(String line);
}
