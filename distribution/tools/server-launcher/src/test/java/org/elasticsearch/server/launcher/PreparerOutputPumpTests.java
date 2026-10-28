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
import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.equalTo;

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
        pump = new PreparerOutputPump(pumpInput, capturedStdout, capturedStderr);
        pump.start();
    }

    private void writeRaw(byte[] bytes) throws IOException {
        input.write(bytes);
        input.flush();
    }

    private void writeStdout(String s) throws IOException {
        byte[] data = s.getBytes(StandardCharsets.UTF_8);
        byte[] out = new byte[data.length + 1];
        out[0] = PreparerOutputPump.STDOUT_MODE;
        System.arraycopy(data, 0, out, 1, data.length);
        writeRaw(out);
    }

    private void writeStderr(String s) throws IOException {
        byte[] data = s.getBytes(StandardCharsets.UTF_8);
        byte[] out = new byte[data.length + 1];
        out[0] = PreparerOutputPump.STDERR_MODE;
        System.arraycopy(data, 0, out, 1, data.length);
        writeRaw(out);
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

    public void testStdoutModeRoutesToStdout() throws Exception {
        startPump();
        writeStdout("hello world\n");
        writeStdout("version info\n");
        closeAndDrain();
        assertThat(stdout(), equalTo("hello world\nversion info\n"));
        assertThat(stderr(), equalTo(""));
    }

    public void testStderrModeRoutesToStderr() throws Exception {
        startPump();
        writeStderr("error message\n");
        writeStderr("warning message\n");
        closeAndDrain();
        assertThat(stderr(), equalTo("error message\nwarning message\n"));
        assertThat(stdout(), equalTo(""));
    }

    public void testModeMarkersAreStrippedFromOutput() throws Exception {
        startPump();
        writeStdout("hello\n");
        closeAndDrain();
        byte[] out = capturedStdout.toByteArray();
        for (byte b : out) {
            assertNotEquals("mode byte should not appear in output", PreparerOutputPump.STDOUT_MODE, b);
            assertNotEquals("mode byte should not appear in output", PreparerOutputPump.STDERR_MODE, b);
        }
    }

    public void testModeSwitching() throws Exception {
        startPump();
        writeStdout("stdout line 1\n");
        writeStderr("stderr line 1\n");
        writeStdout("stdout line 2\n");
        writeStderr("stderr line 2\n");
        closeAndDrain();
        assertThat(stdout(), equalTo("stdout line 1\nstdout line 2\n"));
        assertThat(stderr(), equalTo("stderr line 1\nstderr line 2\n"));
    }

    public void testDefaultModeIsStdout() throws Exception {
        startPump();
        writeRaw("no mode marker\n".getBytes(StandardCharsets.UTF_8));
        closeAndDrain();
        assertThat(stdout(), equalTo("no mode marker\n"));
        assertThat(stderr(), equalTo(""));
    }

    public void testPartialLineInterleaving() throws Exception {
        startPump();
        writeStderr("Enter password: ");
        writeStdout("Version: 9.0\n");
        writeStderr("Bad password\n");
        closeAndDrain();
        assertThat(stdout(), equalTo("Version: 9.0\n"));
        assertThat(stderr(), equalTo("Enter password: Bad password\n"));
    }

    public void testEmptyStream() throws Exception {
        startPump();
        closeAndDrain();
        assertThat(stdout(), equalTo(""));
        assertThat(stderr(), equalTo(""));
    }

    public void testConsecutiveModeSwitchesWithNoData() throws Exception {
        startPump();
        writeRaw(new byte[] { PreparerOutputPump.STDOUT_MODE, PreparerOutputPump.STDERR_MODE });
        writeRaw("err\n".getBytes(StandardCharsets.UTF_8));
        closeAndDrain();
        assertThat(stdout(), equalTo(""));
        assertThat(stderr(), equalTo("err\n"));
    }
}
