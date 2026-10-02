/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.server.launcher;

import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

/**
 * Tests for {@link ErrorPumpThread}.
 */
public class ErrorPumpThreadTests extends ESTestCase {

    private PipedOutputStream processStderr;
    private ByteArrayOutputStream capturedOutput;
    private ErrorPumpThread pump;

    private void startPump() throws IOException {
        processStderr = new PipedOutputStream();
        PipedInputStream pumpInput = new PipedInputStream(processStderr);
        capturedOutput = new ByteArrayOutputStream();
        pump = new ErrorPumpThread(pumpInput, new PrintStream(capturedOutput, true, StandardCharsets.UTF_8));
        pump.start();
    }

    private void writeLine(String line) throws IOException {
        processStderr.write((line + "\n").getBytes(StandardCharsets.UTF_8));
        processStderr.flush();
    }

    private void closeAndDrain() throws IOException {
        processStderr.close();
        pump.drain();
        pump.close();
    }

    private String output() {
        return capturedOutput.toString(StandardCharsets.UTF_8);
    }

    public void testReadyMarkerSignalsReady() throws Exception {
        startPump();
        writeLine(String.valueOf(ErrorPumpThread.SERVER_READY_MARKER));
        assertThat(pump.waitUntilReady(), is(true));
        closeAndDrain();
    }

    public void testReadyMarkerNotPassedToOutput() throws Exception {
        startPump();
        writeLine(String.valueOf(ErrorPumpThread.SERVER_READY_MARKER));
        pump.waitUntilReady();
        closeAndDrain();
        assertThat(output(), not(containsString(String.valueOf(ErrorPumpThread.SERVER_READY_MARKER))));
    }

    public void testStreamCloseWithoutMarkerSignalsNotReady() throws Exception {
        startPump();
        writeLine("some startup error");
        closeAndDrain();
        assertThat(pump.waitUntilReady(), is(false));
    }

    public void testRegularMessagesPassThrough() throws Exception {
        startPump();
        writeLine(String.valueOf(ErrorPumpThread.SERVER_READY_MARKER));
        pump.waitUntilReady();
        writeLine("hello world");
        writeLine("another message");
        closeAndDrain();
        assertThat(output(), containsString("hello world"));
        assertThat(output(), containsString("another message"));
    }

    public void testFiltersIncubatorModulesWarning() throws Exception {
        startPump();
        writeLine(String.valueOf(ErrorPumpThread.SERVER_READY_MARKER));
        pump.waitUntilReady();
        writeLine("WARNING: Using incubator modules: jdk.incubator.vector");
        writeLine("legitimate message");
        closeAndDrain();
        assertThat(output(), not(containsString("incubator")));
        assertThat(output(), containsString("legitimate message"));
    }

    public void testFiltersTimezoneDeprecationWarning() throws Exception {
        startPump();
        writeLine(String.valueOf(ErrorPumpThread.SERVER_READY_MARKER));
        pump.waitUntilReady();
        writeLine("WARNING: Use of the three-letter time zone ID EST is deprecated and it will be removed in a future release");
        writeLine("WARNING: Use of the three-letter time zone ID PST is deprecated and it will be removed in a future release");
        writeLine("keep this line");
        closeAndDrain();
        assertThat(output(), not(containsString("time zone")));
        assertThat(output(), containsString("keep this line"));
    }

    public void testDoesNotFilterPartialMatch() throws Exception {
        startPump();
        writeLine(String.valueOf(ErrorPumpThread.SERVER_READY_MARKER));
        pump.waitUntilReady();
        writeLine("WARNING: Using incubator modules: jdk.incubator.vector and more stuff");
        writeLine("prefix WARNING: Using incubator modules: jdk.incubator.vector");
        closeAndDrain();
        assertThat(output(), containsString("and more stuff"));
        assertThat(output(), containsString("prefix WARNING"));
    }

    public void testMessagesBeforeMarkerPassThrough() throws Exception {
        startPump();
        writeLine("early warning");
        writeLine(String.valueOf(ErrorPumpThread.SERVER_READY_MARKER));
        pump.waitUntilReady();
        closeAndDrain();
        assertThat(output(), containsString("early warning"));
    }
}
