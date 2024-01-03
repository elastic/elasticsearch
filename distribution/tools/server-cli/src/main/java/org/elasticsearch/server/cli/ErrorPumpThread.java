/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.server.cli;

import org.elasticsearch.bootstrap.BootstrapInfo;

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
 * A thread which reads stderr of the jvm process and writes it to this process' stderr.
 *
 * <p> The thread watches for a special state marker from the process. The ascii character
 * {@link BootstrapInfo#SERVER_READY_MARKER} signals the server is ready and the cli may
 * detach if daemonizing. All other messages are passed through to stderr.
 */
class ErrorPumpThread extends Thread {
    private final BufferedReader reader;
    private final PrintWriter writer;

    // a latch which changes state when the server is ready or has had a bootstrap error
    private final CountDownLatch readyOrDead = new CountDownLatch(1);

    // a flag denoting whether the ready marker has been received by the server process
    private volatile boolean ready;

    // an unexpected io failure that occurred while pumping stderr
    private volatile IOException ioFailure;

    ErrorPumpThread(PrintWriter errOutput, InputStream errInput) {
        super("server-cli[stderr_pump]");
        this.reader = new BufferedReader(new InputStreamReader(errInput, StandardCharsets.UTF_8));
        this.writer = errOutput;
    }

    /**
     * Waits until the server ready marker has been received.
     *
     * @return a bootstrap exeption message if a bootstrap error occurred, or null otherwise
     * @throws IOException if there was a problem reading from stderr of the process
     */
    String waitUntilReady() throws IOException {
        nonInterruptibleVoid(readyOrDead::await);
        if (ioFailure != null) {
            throw ioFailure;
        }
        return null;
    }

    /**
     * Waits for the stderr pump thread to exit.
     */
    void drain() {
        nonInterruptibleVoid(this::join);
    }

    /** List of messages / lines to filter from the output. */
    List<String> filter = List.of("WARNING: Using incubator modules: jdk.incubator.vector");

    @Override
    public void run() {
        try {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isEmpty() == false && line.charAt(0) == SERVER_READY_MARKER) {
                    ready = true;
                    readyOrDead.countDown();
                } else if (filter.contains(line) == false) {
                    writer.println(line);
                }
            }
        } catch (IOException e) {
            ioFailure = e;
        } finally {
            writer.flush();
            readyOrDead.countDown();
        }
    }
}
