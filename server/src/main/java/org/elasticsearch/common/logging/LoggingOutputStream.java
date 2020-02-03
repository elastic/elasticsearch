/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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

    class Buffer {

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
    void log(String msg) {
        logger.log(level, msg);
    }
}
