/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.server.launcher;

import org.elasticsearch.server.launcher.common.ProcessUtil;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;

/**
 * A thread that reads the preparer process's stderr pipe and demultiplexes it into
 * the launcher's stdout and stderr based on byte-level mode markers.
 *
 * <p> The preparer uses an {@code OutputStreamMux} (in the cli-launcher module) to
 * multiplex its stdout and stderr onto a single pipe. Mode byte {@link #STDOUT_MODE}
 * ({@code 0x01}) switches to stdout; {@link #STDERR_MODE} ({@code 0x02}) switches to
 * stderr. All bytes between mode markers belong to the currently active mode.
 * The default mode is stdout, so any bytes before the first marker are routed there.
 */
class PreparerOutputPump extends Thread {

    static final byte STDOUT_MODE = 0x01;
    static final byte STDERR_MODE = 0x02;

    private final InputStream input;
    private final OutputStream stdout;
    private final OutputStream stderr;

    PreparerOutputPump(InputStream input, OutputStream stdout, OutputStream stderr) {
        super("server-launcher[preparer_output]");
        this.input = input;
        this.stdout = stdout;
        this.stderr = stderr;
    }

    @Override
    public void run() {
        try {
            byte[] buf = new byte[8192];
            byte currentMode = STDOUT_MODE;
            int n;
            while ((n = input.read(buf, 0, buf.length)) != -1) {
                int start = 0;
                for (int i = 0; i < n; i++) {
                    if (buf[i] == STDOUT_MODE || buf[i] == STDERR_MODE) {
                        if (i > start) {
                            streamFor(currentMode).write(buf, start, i - start);
                        }
                        currentMode = buf[i];
                        start = i + 1;
                    }
                }
                if (start < n) {
                    streamFor(currentMode).write(buf, start, n - start);
                }
            }
            stdout.flush();
            stderr.flush();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            try {
                input.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private OutputStream streamFor(byte mode) {
        return mode == STDERR_MODE ? stderr : stdout;
    }

    /**
     * Waits for the pump thread to finish reading all output.
     */
    void drain() {
        ProcessUtil.nonInterruptibleVoid(this::join);
    }
}
