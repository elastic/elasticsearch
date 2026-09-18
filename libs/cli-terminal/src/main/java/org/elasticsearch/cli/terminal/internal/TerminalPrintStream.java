/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cli.terminal.internal;

import org.elasticsearch.cli.terminal.Terminal;

import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

/**
 * A {@link PrintStream} that writes lines to the current {@link Terminal}.
 *
 * <p> To avoid interleaving when multiple threads write to the same stream, each thread
 * gets its own line buffer via {@link ThreadLocal}. This prevents partial-line writes from
 * one thread from being merged with writes from another thread.
 */
public class TerminalPrintStream extends PrintStream {

    private final Terminal terminal;
    private final boolean isError;

    private final ThreadLocal<LineBuffer> lineBuffer = new ThreadLocal<>();

    public TerminalPrintStream(Terminal terminal, boolean isError) {
        super(OutputStream.nullOutputStream(), true);
        this.terminal = terminal;
        this.isError = isError;
    }

    @Override
    public void write(byte[] bytes, int off, int len) {
        LineBuffer buf = getOrCreateLineBuffer();
        int start = off;
        int end = off + len;
        for (int i = off; i < end; i++) {
            if (bytes[i] == '\n') {
                int segLen = i - start;
                if (segLen > 0) {
                    buf.append(bytes, start, segLen);
                }
                writeToTerminal(buf.drain());
                start = i + 1;
            }
        }
        int remaining = end - start;
        if (remaining > 0) {
            buf.append(bytes, start, remaining);
        }
    }

    @Override
    public void write(int b) {
        LineBuffer buf = getOrCreateLineBuffer();
        if (b == '\n') {
            writeToTerminal(buf.drain());
        } else {
            buf.append((byte) b);
        }
    }

    @Override
    public void println(String x) {
        String str = x != null ? x : "null";
        LineBuffer buf = lineBuffer.get();
        writeToTerminal(buf != null && buf.count > 0 ? buf.drain() + str : str);
    }

    @Override
    public void println(Object x) {
        println(String.valueOf(x));
    }

    /* Writes the line buffer of the current thread (only) if exists and then flushes. */
    @Override
    public void flush() {
        LineBuffer buf = lineBuffer.get();
        if (buf != null && buf.count > 0) {
            writeToTerminal(buf.drain());
        }
    }

    private LineBuffer getOrCreateLineBuffer() {
        LineBuffer buf = lineBuffer.get();
        if (buf == null) {
            buf = new LineBuffer();
            lineBuffer.set(buf);
        }
        return buf;
    }

    private void writeToTerminal(String line) {
        if (line.isEmpty()) {
            return;
        }
        if (isError) {
            terminal.errorPrintln(line);
        } else {
            terminal.println(line);
        }
    }

    static class LineBuffer {
        private static final int DEFAULT_SIZE = 1024;
        byte[] buf = new byte[DEFAULT_SIZE];
        int count = 0;

        void append(byte b) {
            ensureCapacity(1);
            buf[count++] = b;
        }

        void append(byte[] src, int off, int len) {
            ensureCapacity(len);
            System.arraycopy(src, off, buf, count, len);
            count += len;
        }

        String drain() {
            if (count > 0 && buf[count - 1] == '\r') {
                count--;
            }
            if (count == 0) {
                return "";
            }
            String s = new String(buf, 0, count, StandardCharsets.UTF_8);
            count = 0;
            if (buf.length > DEFAULT_SIZE) {
                buf = new byte[DEFAULT_SIZE];
            }
            return s;
        }

        private void ensureCapacity(int extra) {
            if (count + extra > buf.length) {
                int newSize = Math.max(buf.length * 2, count + extra);
                byte[] newBuf = new byte[newSize];
                System.arraycopy(buf, 0, newBuf, 0, count);
                buf = newBuf;
            }
        }
    }
}
