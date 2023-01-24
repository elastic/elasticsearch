/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cli;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FilterReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;

/**
 * A terminal for tests which captures all output, and
 * can be plugged with fake input.
 */
public class MockTerminal extends Terminal {

    /**
     * A ByteArrayInputStream that has its bytes set after construction.
     */
    private static class LazyByteArrayInputStream extends ByteArrayInputStream {
        LazyByteArrayInputStream() {
            super(new byte[128], 0, 0);
        }

        void append(byte[] bytes) {
            int availableSpace = buf.length - count;
            if (bytes.length > availableSpace) {
                // resize
                int remaining = count - pos;
                int newSize = Math.max(buf.length * 2, remaining + bytes.length);
                byte[] newBuf = new byte[newSize];
                System.arraycopy(buf, pos, newBuf, 0, remaining);
                buf = newBuf;
                pos = 0;
                count = remaining;
            }
            System.arraycopy(bytes, 0, buf, count, bytes.length);
            count += bytes.length;
        }

        void clear() {
            pos = 0;
            count = 0;
        }
    }

    static class ResettableInputStreamReader extends FilterReader {
        final LazyByteArrayInputStream stream;

        private ResettableInputStreamReader(LazyByteArrayInputStream stream) {
            super(createReader(stream));
            this.stream = stream;
        }

        static InputStreamReader createReader(InputStream stream) {
            return new InputStreamReader(stream, StandardCharsets.UTF_8);
        }

        @Override
        public void reset() {
            in = createReader(stream);
        }
    }

    // use the system line ending so we get coverage of windows line endings when running tests on windows
    private static final byte[] NEWLINE = System.lineSeparator().getBytes(StandardCharsets.UTF_8);

    private final ResettableInputStreamReader stdinReader;
    private final LazyByteArrayInputStream stdinBuffer;
    private final ByteArrayOutputStream stdoutBuffer;
    private final ByteArrayOutputStream stderrBuffer;
    private boolean supportsBinary = false;

    private MockTerminal(ResettableInputStreamReader stdinReader, ByteArrayOutputStream stdout, ByteArrayOutputStream stderr) {
        super(stdinReader, newPrintWriter(stdout), newPrintWriter(stderr));
        this.stdinReader = stdinReader;
        this.stdinBuffer = stdinReader.stream;
        this.stdoutBuffer = stdout;
        this.stderrBuffer = stderr;
    }

    @Override
    public InputStream getInputStream() {
        return supportsBinary ? stdinBuffer : null;
    }

    @Override
    public OutputStream getOutputStream() {
        return supportsBinary ? stdoutBuffer : null;
    }

    private static PrintWriter newPrintWriter(OutputStream out) {
        return new PrintWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8), true);
    }

    public static MockTerminal create() {
        var reader = new ResettableInputStreamReader(new LazyByteArrayInputStream());
        return new MockTerminal(reader, new ByteArrayOutputStream(), new ByteArrayOutputStream());
    }

    /** Adds a character input that will be returned from reading this Terminal. Values are read in FIFO order. */
    public void addTextInput(String input) {
        stdinBuffer.append(input.getBytes(StandardCharsets.UTF_8));
        stdinBuffer.append(NEWLINE);
    }

    /** Adds a character input that will be returned from reading a secret from this Terminal. Values are read in FIFO order. */
    public void addSecretInput(String input) {
        // for now this is just text input
        // TODO: add assertions this is only read with readSecret
        addTextInput(input);
    }

    /** Adds a binary input that will be returned from reading this Terminal. Values are read in FIFO order. */
    public void addBinaryInput(byte[] bytes) {
        stdinBuffer.append(bytes);
    }

    /** Returns all output written to this terminal. */
    public String getOutput() {
        return stdoutBuffer.toString(StandardCharsets.UTF_8);
    }

    /** Returns all bytes  written to this terminal. */
    public byte[] getOutputBytes() {
        return stdoutBuffer.toByteArray();
    }

    /** Returns all output written to this terminal. */
    public String getErrorOutput() {
        return stderrBuffer.toString(StandardCharsets.UTF_8);
    }

    public void setSupportsBinary(boolean supportsBinary) {
        this.supportsBinary = supportsBinary;
    }

    /** Wipes the input and output. */
    public void reset() {
        stdinBuffer.clear();
        stdinReader.reset();
        stdoutBuffer.reset();
        stderrBuffer.reset();
    }
}
