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

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.function.Predicate;
import java.util.regex.Pattern;

/**
 * A thread which reads stderr of the server JVM process and writes it to this process' stderr.
 *
 * <p> The thread watches for a special state marker from the process. The ascii character
 * {@code \u0018} signals the server is ready and the launcher may detach if daemonizing.
 * All other messages are passed through to stderr.
 */
public class ErrorPumpThread extends Thread implements Closeable {
    /** Messages / lines predicate to filter from the output. */
    private static final Predicate<String> filter = Pattern.compile(
        "WARNING: Using incubator modules: jdk\\.incubator\\.vector"
            // requires log4j2 upgrade, see https://github.com/elastic/elasticsearch/issues/132035
            + "|WARNING: Use of the three-letter time zone ID .* is deprecated and it will be removed in a future release"
    ).asMatchPredicate();

    static final char SERVER_READY_MARKER = '\u0018';

    private final BufferedReader reader;
    private final PrintStream errOutput;

    private final CountDownLatch readyOrDead = new CountDownLatch(1);
    private volatile boolean ready;
    private volatile IOException ioFailure;

    public ErrorPumpThread(InputStream errInput, PrintStream errOutput) {
        super("server-launcher[stderr_pump]");
        this.reader = new BufferedReader(new InputStreamReader(errInput, StandardCharsets.UTF_8));
        this.errOutput = errOutput;
    }

    private void checkForIoFailure() throws IOException {
        IOException failure = ioFailure;
        ioFailure = null;
        if (failure != null) {
            throw failure;
        }
    }

    @Override
    public void close() throws IOException {
        assert isAlive() == false : "Pump thread must be drained first";
        checkForIoFailure();
    }

    /**
     * Waits until the server ready marker has been received.
     *
     * @return {@code true} if successful, {@code false} if a startup error occurred
     * @throws IOException if there was a problem reading from stderr of the process
     */
    public boolean waitUntilReady() throws IOException {
        ProcessUtil.nonInterruptibleVoid(readyOrDead::await);
        checkForIoFailure();
        return ready;
    }

    /**
     * Waits for the stderr pump thread to exit.
     */
    public void drain() {
        ProcessUtil.nonInterruptibleVoid(this::join);
    }

    @Override
    public void run() {
        try {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isEmpty() == false && line.charAt(0) == SERVER_READY_MARKER) {
                    ready = true;
                    readyOrDead.countDown();
                } else if (filter.test(line) == false) {
                    errOutput.println(line);
                }
            }
        } catch (IOException e) {
            ioFailure = e;
        } finally {
            errOutput.flush();
            readyOrDead.countDown();
        }
    }
}
