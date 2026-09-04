/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.launcher;

import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for {@link OutputStreamMux}.
 */
public class OutputStreamMuxTests extends ESTestCase {

    private ByteArrayOutputStream rawOutput;
    private OutputStreamMux mux;
    private OutputStream stdoutChannel;
    private OutputStream stderrChannel;

    private void createMux() {
        rawOutput = new ByteArrayOutputStream();
        mux = new OutputStreamMux(rawOutput);
        stdoutChannel = mux.channel(OutputStreamMux.STDOUT_MODE);
        stderrChannel = mux.channel(OutputStreamMux.STDERR_MODE);
    }

    private byte[] output() {
        return rawOutput.toByteArray();
    }

    private void writeString(OutputStream channel, String s) throws IOException {
        channel.write(s.getBytes(StandardCharsets.UTF_8));
    }

    public void testStdoutChannelEmitsModeByteOnFirstWrite() throws IOException {
        createMux();
        writeString(stdoutChannel, "hello");
        byte[] out = output();
        assertThat(out[0], equalTo(OutputStreamMux.STDOUT_MODE));
        assertThat(new String(out, 1, out.length - 1, StandardCharsets.UTF_8), equalTo("hello"));
    }

    public void testStderrChannelEmitsModeByteOnFirstWrite() throws IOException {
        createMux();
        writeString(stderrChannel, "error");
        byte[] out = output();
        assertThat(out[0], equalTo(OutputStreamMux.STDERR_MODE));
        assertThat(new String(out, 1, out.length - 1, StandardCharsets.UTF_8), equalTo("error"));
    }

    public void testSameChannelDoesNotRepeatModeByte() throws IOException {
        createMux();
        writeString(stdoutChannel, "hello ");
        writeString(stdoutChannel, "world\n");
        byte[] out = output();
        assertThat(out[0], equalTo(OutputStreamMux.STDOUT_MODE));
        assertThat(new String(out, 1, out.length - 1, StandardCharsets.UTF_8), equalTo("hello world\n"));
    }

    public void testModeSwitchEmitsNewModeByte() throws IOException {
        createMux();
        writeString(stdoutChannel, "out");
        writeString(stderrChannel, "err");
        byte[] out = output();
        assertThat(out[0], equalTo(OutputStreamMux.STDOUT_MODE));
        int errModePos = 1 + "out".length();
        assertThat(out[errModePos], equalTo(OutputStreamMux.STDERR_MODE));
        assertThat(new String(out, errModePos + 1, "err".length(), StandardCharsets.UTF_8), equalTo("err"));
    }

    public void testMultipleModeSwitches() throws IOException {
        createMux();
        writeString(stdoutChannel, "A");
        writeString(stderrChannel, "B");
        writeString(stdoutChannel, "C");
        writeString(stderrChannel, "D");
        byte[] out = output();
        byte[] expected = new byte[] {
            OutputStreamMux.STDOUT_MODE,
            'A',
            OutputStreamMux.STDERR_MODE,
            'B',
            OutputStreamMux.STDOUT_MODE,
            'C',
            OutputStreamMux.STDERR_MODE,
            'D' };
        assertArrayEquals(expected, out);
    }

    public void testPartialLineWithoutNewline() throws IOException {
        createMux();
        writeString(stdoutChannel, "partial");
        byte[] out = output();
        assertThat(out[0], equalTo(OutputStreamMux.STDOUT_MODE));
        assertThat(new String(out, 1, out.length - 1, StandardCharsets.UTF_8), equalTo("partial"));
    }

    public void testInterleavedPartialLines() throws IOException {
        createMux();
        writeString(stderrChannel, "Enter password: ");
        writeString(stdoutChannel, "Version: 9.0\n");
        writeString(stderrChannel, "Bad password\n");
        byte[] out = output();
        byte[] expected = concat(
            new byte[] { OutputStreamMux.STDERR_MODE },
            "Enter password: ".getBytes(StandardCharsets.UTF_8),
            new byte[] { OutputStreamMux.STDOUT_MODE },
            "Version: 9.0\n".getBytes(StandardCharsets.UTF_8),
            new byte[] { OutputStreamMux.STDERR_MODE },
            "Bad password\n".getBytes(StandardCharsets.UTF_8)
        );
        assertArrayEquals(expected, out);
    }

    public void testSingleByteWrites() throws IOException {
        createMux();
        for (byte b : "hi".getBytes(StandardCharsets.UTF_8)) {
            stdoutChannel.write(b);
        }
        byte[] out = output();
        assertThat(out[0], equalTo(OutputStreamMux.STDOUT_MODE));
        assertThat(new String(out, 1, out.length - 1, StandardCharsets.UTF_8), equalTo("hi"));
    }

    public void testEmptyWriteProducesNothing() throws IOException {
        createMux();
        stdoutChannel.write(new byte[0], 0, 0);
        assertThat(output().length, equalTo(0));
    }

    public void testUtf8MultiByteCharacters() throws IOException {
        createMux();
        writeString(stdoutChannel, "café\n");
        byte[] out = output();
        assertThat(out[0], equalTo(OutputStreamMux.STDOUT_MODE));
        assertThat(new String(out, 1, out.length - 1, StandardCharsets.UTF_8), equalTo("café\n"));
    }

    public void testModeSwitchOnSingleByteWrite() throws IOException {
        createMux();
        stdoutChannel.write('A');
        stderrChannel.write('B');
        byte[] out = output();
        assertArrayEquals(new byte[] { OutputStreamMux.STDOUT_MODE, 'A', OutputStreamMux.STDERR_MODE, 'B' }, out);
    }

    private static byte[] concat(byte[]... arrays) {
        int totalLen = 0;
        for (byte[] a : arrays) {
            totalLen += a.length;
        }
        byte[] result = new byte[totalLen];
        int pos = 0;
        for (byte[] a : arrays) {
            System.arraycopy(a, 0, result, pos, a.length);
            pos += a.length;
        }
        return result;
    }
}
