/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.server.cli;

import org.elasticsearch.bootstrap.BootstrapInfo;
import org.elasticsearch.core.IOUtils;

import java.io.IOException;
import java.io.OutputStream;

import static org.elasticsearch.server.cli.ProcessUtil.nonInterruptible;

/**
 * A helper to control a {@link Process} running the main Elasticsearch server.
 *
 * <p> The process can be started by calling {@link ServerProcessBuilder#start()}.
 * The process is controlled by internally sending arguments and control signals on stdin,
 * and receiving control signals on stderr. The start method does not return until the
 * server is ready to process requests and has exited the bootstrap thread.
 *
 * <p> The caller starting a {@link ServerProcess} can then do one of several things:
 * <ul>
 *     <li>Block on the server process exiting, by calling {@link #waitFor()}</li>
 *     <li>Detach from the server process by calling {@link #detach()}</li>
 *     <li>Tell the server process to shutdown and wait for it to exit by calling {@link #stop()}</li>
 * </ul>
 */
public class ServerProcess {

    // the actual java process of the server
    private final Process jvmProcess;

    // the thread pumping stderr watching for state change messages
    private final ErrorPumpThread errorPump;

    // a flag marking whether the streams of the java subprocess have been closed
    private volatile boolean detached = false;

    ServerProcess(Process jvmProcess, ErrorPumpThread errorPump) {
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
     *
     * @throws IOException If an I/O error occurred while reading stderr or closing any of the standard streams
     */
    public synchronized void detach() throws IOException {
        errorPump.drain();
        try {
            IOUtils.close(jvmProcess.getOutputStream(), jvmProcess.getInputStream(), jvmProcess.getErrorStream(), errorPump);
        } finally {
            detached = true;
        }
    }

    /**
     * Waits for the subprocess to exit.
     */
    public int waitFor() throws IOException {
        errorPump.drain();
        int exitCode = nonInterruptible(jvmProcess::waitFor);
        errorPump.close();
        return exitCode;
    }

    /**
     * Stop the subprocess.
     *
     * <p> This sends a special code, {@link BootstrapInfo#SERVER_SHUTDOWN_MARKER} to the stdin
     * of the process, then waits for the process to exit.
     *
     * <p> Note that if {@link #detach()} has been called, this method is a no-op.
     */
    public synchronized void stop() throws IOException {
        if (detached) {
            return;
        }

        sendShutdownMarker();
        waitFor(); // ignore exit code, we are already shutting down
    }

    /**
     * Stop the subprocess, sending a SIGKILL.
     */
    public void forceStop() throws IOException {
        assert detached == false;
        jvmProcess.destroyForcibly();
        waitFor();
    }

    private void sendShutdownMarker() {
        try {
            OutputStream os = jvmProcess.getOutputStream();
            os.write(BootstrapInfo.SERVER_SHUTDOWN_MARKER);
            os.flush();
        } catch (IOException e) {
            // process is already effectively dead, fall through to wait for it, or should we SIGKILL?
        }
    }
}
