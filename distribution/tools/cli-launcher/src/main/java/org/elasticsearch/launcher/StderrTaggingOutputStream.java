/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.launcher;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * A {@link FilterOutputStream} that prepends a tag byte ({@link #STDERR_LINE_TAG}) at the
 * start of every line written to the underlying stream. This allows a parent process reading
 * the stream to distinguish stderr-destined output from stdout-destined output when both are
 * multiplexed onto a single pipe.
 *
 * <p> Untagged lines default to stdout in the parent process. Lines tagged with {@link #STDERR_LINE_TAG}
 * are routed to stderr. This is safe with UTF-8 since the tag byte ({@code 0x01}) never appears
 * inside multi-byte sequences.
 *
 * @see "PreparerOutputPump in the server-launcher module"
 */
class StderrTaggingOutputStream extends FilterOutputStream {

    /**
     * Tag byte prepended to stderr-destined lines on the merged output stream.
     * Must match the value used by {@code PreparerOutputPump} in the server-launcher module.
     */
    static final byte STDERR_LINE_TAG = 0x01;

    private boolean atLineStart = true;

    StderrTaggingOutputStream(OutputStream out) {
        super(out);
    }

    @Override
    public synchronized void write(int b) throws IOException {
        if (atLineStart) {
            out.write(STDERR_LINE_TAG);
            atLineStart = false;
        }
        out.write(b);
        if (b == '\n') {
            atLineStart = true;
        }
    }

    @Override
    public synchronized void write(byte[] b, int off, int len) throws IOException {
        int end = off + len;
        int pos = off;
        while (pos < end) {
            if (atLineStart) {
                out.write(STDERR_LINE_TAG);
                atLineStart = false;
            }
            int nlPos = indexOf(b, (byte) '\n', pos, end);
            if (nlPos >= 0) {
                out.write(b, pos, nlPos - pos + 1);
                atLineStart = true;
                pos = nlPos + 1;
            } else {
                out.write(b, pos, end - pos);
                pos = end;
            }
        }
    }

    private static int indexOf(byte[] b, byte target, int from, int to) {
        for (int i = from; i < to; i++) {
            if (b[i] == target) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public synchronized void flush() throws IOException {
        out.flush();
    }
}
