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

    private static final byte[] NEWLINE = new byte[] { '\n' };

    private final ByteArrayOutputStream stdoutBuffer;
    private final ByteArrayOutputStream stderrBuffer;

    // A deque would be a perfect data structure for the FIFO queue of input values needed here. However,
    // to support the valid return value of readText being null (defined by Console), we need to be able
    // to store nulls. However, java the java Deque api does not allow nulls because it uses null as
    // a special return value from certain methods like peek(). So instead of deque, we use an array list here,
    // and keep track of the last position which was read. It means that we will hold onto all input
    // setup for the mock terminal during its lifetime, but this is normally a very small amount of data
    // so in reality it will not matter.
    private final LazyByteArrayInputStream stdinBuffer;

    private MockTerminal(
        LazyByteArrayInputStream stdin,
        ByteArrayOutputStream stdout,
        ByteArrayOutputStream stderr,
        boolean supportsBinary
    ) {
        super(
            new InputStreamReader(stdin),
            newPrintWriter(stdout),
            newPrintWriter(stderr),
            supportsBinary ? stdin : null,
            supportsBinary ? stdout : null
        );
        this.stdinBuffer = stdin;
        this.stdoutBuffer = stdout;
        this.stderrBuffer = stderr;
    }

    private static PrintWriter newPrintWriter(OutputStream out) {
        return new PrintWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8));
    }

    public static MockTerminal create(boolean supportsBinary) {
        return new MockTerminal(new LazyByteArrayInputStream(), new ByteArrayOutputStream(), new ByteArrayOutputStream(), supportsBinary);
    }

    /** Adds a character input that will be returned from reading this Terminal. Values are read in FIFO order. */
    public void addTextInput(String input) {
        stdinBuffer.append(input.getBytes(StandardCharsets.UTF_8));
        stdinBuffer.append(NEWLINE);
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

    /** Wipes the input and output. */
    public void reset() {
        stdinBuffer.clear();
        stdoutBuffer.reset();
        stderrBuffer.reset();
    }
}
