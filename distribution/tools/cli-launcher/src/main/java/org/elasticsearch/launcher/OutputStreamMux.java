/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.launcher;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Multiplexes two logical output channels (stdout and stderr) onto a single underlying
 * {@link OutputStream} using byte-level mode switching. A one-byte mode marker is emitted
 * whenever the active channel changes, and all subsequent bytes belong to that channel
 * until the next marker.
 *
 * <p> This is analogous to terminal escape sequences: the consumer tracks the current mode
 * and routes bytes accordingly. Mode bytes are only emitted on transitions, so consecutive
 * writes to the same channel incur no overhead.
 *
 * <p> All writes are synchronized on the mux instance to guarantee that a mode byte and
 * its associated data are written atomically, preventing interleaving from concurrent
 * writers.
 *
 * <p> The mode bytes ({@link #STDOUT_MODE} = {@code 0x01}, {@link #STDERR_MODE} = {@code 0x02})
 * are ASCII control characters that never appear in valid UTF-8 multi-byte sequences,
 * making them safe sentinels for text output.
 *
 * @see "PreparerOutputPump in the server-launcher module"
 */
class OutputStreamMux {

    static final byte STDOUT_MODE = 0x01;
    static final byte STDERR_MODE = 0x02;

    private final OutputStream out;
    private byte currentMode = 0;

    OutputStreamMux(OutputStream out) {
        this.out = out;
    }

    /**
     * Returns an {@link OutputStream} that writes to the given mode channel.
     * Data written to the returned stream will be preceded by the mode byte
     * whenever the active mode differs from the requested one.
     */
    OutputStream channel(byte mode) {
        return new ChannelOutputStream(mode);
    }

    private synchronized void writeWithMode(byte mode, int b) throws IOException {
        ensureMode(mode);
        out.write(b);
    }

    private synchronized void writeWithMode(byte mode, byte[] b, int off, int len) throws IOException {
        if (len == 0) {
            return;
        }
        ensureMode(mode);
        out.write(b, off, len);
    }

    private synchronized void flushWithMode() throws IOException {
        out.flush();
    }

    private void ensureMode(byte mode) throws IOException {
        if (currentMode != mode) {
            out.write(mode);
            currentMode = mode;
        }
    }

    private class ChannelOutputStream extends OutputStream {
        private final byte mode;

        ChannelOutputStream(byte mode) {
            this.mode = mode;
        }

        @Override
        public void write(int b) throws IOException {
            writeWithMode(mode, b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            writeWithMode(mode, b, off, len);
        }

        @Override
        public void flush() throws IOException {
            flushWithMode();
        }
    }
}
