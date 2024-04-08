/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * A stream whose output is sent to the configured logger, line by line.
 */
class LoggingOutputStream extends OutputStream {
    /** The starting length of the buffer */
    static final int DEFAULT_BUFFER_LENGTH = 1024;

    // limit a single log message to 64k
    static final int MAX_BUFFER_LENGTH = DEFAULT_BUFFER_LENGTH * 64;

    static class Buffer {

        /** The buffer of bytes sent to the stream */
        byte[] bytes = new byte[DEFAULT_BUFFER_LENGTH];

        /** Number of used bytes in the buffer */
        int used = 0;
    }

    // each thread gets its own buffer so messages don't get garbled
    ThreadLocal<Buffer> threadLocal = ThreadLocal.withInitial(Buffer::new);

    private final Logger logger;

    private final Level level;

    LoggingOutputStream(Logger logger, Level level) {
        this.logger = logger;
        this.level = level;
    }

    @Override
    public void write(int b) throws IOException {
        if (threadLocal == null) {
            throw new IOException("buffer closed");
        }
        if (b == 0) return;
        if (b == '\n') {
            // always flush with newlines instead of adding to the buffer
            flush();
            return;
        }

        Buffer buffer = threadLocal.get();

        if (buffer.used == buffer.bytes.length) {
            if (buffer.bytes.length >= MAX_BUFFER_LENGTH) {
                // don't let the buffer get infinitely big
                flush();
                // we reset the buffer in flush so get the new instance
                buffer = threadLocal.get();
            } else {
                // extend the buffer
                buffer.bytes = Arrays.copyOf(buffer.bytes, 2 * buffer.bytes.length);
            }
        }

        buffer.bytes[buffer.used++] = (byte) b;
    }

    @Override
    public void flush() {
        Buffer buffer = threadLocal.get();
        if (buffer.used == 0) return;
        int used = buffer.used;
        if (buffer.bytes[used - 1] == '\r') {
            // windows case: remove the first part of newlines there too
            --used;
        }
        if (used == 0) {
            // only windows \r was in the buffer
            buffer.used = 0;
            return;
        }
        log(new String(buffer.bytes, 0, used, StandardCharsets.UTF_8));
        if (buffer.bytes.length != DEFAULT_BUFFER_LENGTH) {
            threadLocal.set(new Buffer()); // reset size
        } else {
            buffer.used = 0;
        }
    }

    @Override
    public void close() {
        threadLocal = null;
    }

    // pkg private for testing
    protected void log(String msg) {
        logger.log(level, msg);
    }
}
