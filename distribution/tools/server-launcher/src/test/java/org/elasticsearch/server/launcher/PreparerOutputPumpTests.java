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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * Tests for {@link PreparerOutputPump}.
 */
public class PreparerOutputPumpTests extends ESTestCase {

    private PipedOutputStream input;
    private ByteArrayOutputStream capturedStdout;
    private ByteArrayOutputStream capturedStderr;
    private PreparerOutputPump pump;

    private void startPump() throws IOException {
        input = new PipedOutputStream();
        PipedInputStream pumpInput = new PipedInputStream(input);
        capturedStdout = new ByteArrayOutputStream();
        capturedStderr = new ByteArrayOutputStream();
        pump = new PreparerOutputPump(
            pumpInput,
            new PrintStream(capturedStdout, true, StandardCharsets.UTF_8),
            new PrintStream(capturedStderr, true, StandardCharsets.UTF_8)
        );
        pump.start();
    }

    private void writeLine(String line) throws IOException {
        input.write((line + "\n").getBytes(StandardCharsets.UTF_8));
        input.flush();
    }

    private void writeTaggedLine(String line) throws IOException {
        writeLine(PreparerOutputPump.STDERR_LINE_TAG + line);
    }

    private void closeAndDrain() throws IOException {
        input.close();
        pump.drain();
    }

    private String stdout() {
        return capturedStdout.toString(StandardCharsets.UTF_8);
    }

    private String stderr() {
        return capturedStderr.toString(StandardCharsets.UTF_8);
    }

    public void testUntaggedLinesGoToStdout() throws Exception {
        startPump();
        writeLine("hello world");
        writeLine("version info");
        closeAndDrain();
        assertThat(stdout(), containsString("hello world"));
        assertThat(stdout(), containsString("version info"));
        assertThat(stderr(), equalTo(""));
    }

    public void testTaggedLinesGoToStderr() throws Exception {
        startPump();
        writeTaggedLine("error message");
        writeTaggedLine("warning message");
        closeAndDrain();
        assertThat(stderr(), containsString("error message"));
        assertThat(stderr(), containsString("warning message"));
        assertThat(stdout(), equalTo(""));
    }

    public void testTagIsStrippedFromStderrOutput() throws Exception {
        startPump();
        writeTaggedLine("error message");
        closeAndDrain();
        assertThat(stderr(), containsString("error message"));
        assertThat(stderr(), not(containsString(String.valueOf(PreparerOutputPump.STDERR_LINE_TAG))));
    }

    public void testMixedTaggedAndUntaggedLines() throws Exception {
        startPump();
        writeLine("stdout line 1");
        writeTaggedLine("stderr line 1");
        writeLine("stdout line 2");
        writeTaggedLine("stderr line 2");
        closeAndDrain();
        assertThat(stdout(), containsString("stdout line 1"));
        assertThat(stdout(), containsString("stdout line 2"));
        assertThat(stderr(), containsString("stderr line 1"));
        assertThat(stderr(), containsString("stderr line 2"));
        assertThat(stdout(), not(containsString("stderr line")));
        assertThat(stderr(), not(containsString("stdout line")));
    }

    public void testEmptyLinesGoToStdout() throws Exception {
        startPump();
        writeLine("");
        closeAndDrain();
        assertThat(stdout(), equalTo(System.lineSeparator()));
        assertThat(stderr(), equalTo(""));
    }

    public void testEmptyStream() throws Exception {
        startPump();
        closeAndDrain();
        assertThat(stdout(), equalTo(""));
        assertThat(stderr(), equalTo(""));
    }
}
