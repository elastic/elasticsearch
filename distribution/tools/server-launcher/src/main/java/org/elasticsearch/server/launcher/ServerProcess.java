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
import java.io.OutputStream;

/**
 * A helper to control a {@link Process} running the main Elasticsearch server.
 *
 * <p> The caller can:
 * <ul>
 *     <li>Block on the server process exiting, by calling {@link #waitFor()}</li>
 *     <li>Detach from the server process by calling {@link #detach()}</li>
 *     <li>Tell the server process to shutdown and wait for it to exit by calling {@link #stop()}</li>
 * </ul>
 */
public class ServerProcess {

    static final char SERVER_SHUTDOWN_MARKER = '\u001B';

    private final Process jvmProcess;
    private final ErrorPumpThread errorPump;
    private volatile boolean detached = false;

    public ServerProcess(Process jvmProcess, ErrorPumpThread errorPump) {
        this.jvmProcess = jvmProcess;
        this.errorPump = errorPump;
    }

    /**
     * Return the process id of the server.
     */
    public long pid() {
        return jvmProcess.pid();
    }

    /**
     * Detaches the server process from the current process, enabling the current process to exit.
     */
    public synchronized void detach() throws IOException {
        errorPump.drain();
        try {
            jvmProcess.getOutputStream().close();
            jvmProcess.getInputStream().close();
            jvmProcess.getErrorStream().close();
            errorPump.close();
        } finally {
            detached = true;
        }
    }

    /**
     * Waits for the subprocess to exit.
     */
    public int waitFor() throws IOException {
        errorPump.drain();
        int exitCode = ProcessUtil.nonInterruptible(jvmProcess::waitFor);
        errorPump.close();
        return exitCode;
    }

    /**
     * Stop the subprocess by sending the shutdown marker to stdin, then wait for exit.
     */
    public synchronized void stop() throws IOException {
        if (detached) {
            return;
        }
        sendShutdownMarker();
        waitFor();
    }

    private void sendShutdownMarker() {
        try {
            OutputStream os = jvmProcess.getOutputStream();
            os.write(SERVER_SHUTDOWN_MARKER);
            os.flush();
        } catch (IOException e) {
            // process is already effectively dead, fall through to wait for it
        }
    }
}
