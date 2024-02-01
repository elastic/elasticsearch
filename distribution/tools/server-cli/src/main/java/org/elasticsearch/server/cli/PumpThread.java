/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.server.cli;

import org.elasticsearch.bootstrap.BootstrapInfo;
import org.elasticsearch.cli.Terminal;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.bootstrap.BootstrapInfo.SERVER_READY_MARKER;
import static org.elasticsearch.server.cli.ProcessUtil.nonInterruptibleVoid;

/**
 * A thread which reads stdout or stderr of the jvm process and writes it to this process' respective stdout or stderr.
 */
class PumpThread extends Thread {
    private final BufferedReader reader;
    private final PrintWriter writer;

    // an unexpected io failure that occurred while pumping stderr
    private volatile IOException ioFailure;

    private PumpThread(String name, PrintWriter output, InputStream input) {
        super(name);
        this.reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8));
        this.writer = output;
    }

    void checkForIoFailure() throws IOException {
        IOException failure = ioFailure;
        if (failure != null) {
            throw failure;
        }
    }

    /**
     * Waits for the stderr pump thread to exit.
     */
    void drain() {
        nonInterruptibleVoid(this::join);
    }

    void processLine(String line) {
        writer.println(line);
    }

    @Override
    public void run() {
        try {
            String line;
            while ((line = reader.readLine()) != null) {
                processLine(line);
            }
        } catch (IOException e) {
            ioFailure = e;
        } finally {
            writer.flush();
        }
    }

    static ErrorPumpThread stderrPump(Terminal terminal, Process jvmProcess) {
        return new ErrorPumpThread("server-cli[stderr_pump]", terminal.getErrorWriter(), jvmProcess.getErrorStream());
    }

    static PumpThread stdoutPump(Terminal terminal, Process jvmProcess) {
        return new PumpThread("server-cli[stdout_pump]", terminal.getWriter(), jvmProcess.getInputStream());

    }

    /**
     * <p> The ErrorPumpThread watches for a special state marker from the process. The ascii character
     * {@link BootstrapInfo#SERVER_READY_MARKER} signals the server is ready and the cli may
     * detach if daemonizing. All other messages are passed through to stderr.
     */
    static class ErrorPumpThread extends PumpThread {

        // a latch which changes state when the server is ready or has had a bootstrap error
        private final CountDownLatch readyOrDead = new CountDownLatch(1);

        private ErrorPumpThread(String name, PrintWriter errOutput, InputStream errInput) {
            super(name, errOutput, errInput);
        }

        /**
         * Waits until the server ready marker has been received.
         *
         * @throws IOException if there was a problem reading from stderr of the process
         */
        void waitUntilReady() throws IOException {
            nonInterruptibleVoid(readyOrDead::await);
            checkForIoFailure();
        }

        /** List of messages / lines to filter from the output. */
        List<String> filter = List.of("WARNING: Using incubator modules: jdk.incubator.vector");

        @Override
        void processLine(String line) {
            if (line.isEmpty() == false && line.charAt(0) == SERVER_READY_MARKER) {
                readyOrDead.countDown();
            } else if (filter.contains(line) == false) {
                super.processLine(line);
            }
        }

        @Override
        public void run() {
            try {
                super.run();
            } finally {
                readyOrDead.countDown();
            }
        }
    }
}
