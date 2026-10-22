/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.io.stream;

import java.io.IOException;
import java.io.OutputStream;

/**
 * An {@link OutputStream} decorator that forwards only bytes in the range {@code [offset, offset + length)},
 * discarding bytes outside that range. Accepts {@code long} offset and length so callers may pass
 * {@link Long#MAX_VALUE} to mean "to end".
 */
public final class SlicedOutputStream extends OutputStream {

    private final OutputStream out;
    private final long start;
    private final long end;
    private long bytesConsumed;

    public SlicedOutputStream(OutputStream out, long offset, long length) {
        this.out = out;
        this.start = offset;
        this.end = Long.MAX_VALUE - offset < length ? Long.MAX_VALUE : offset + length;
    }

    @Override
    public void write(int b) throws IOException {
        if (bytesConsumed >= start && bytesConsumed < end) out.write(b);
        bytesConsumed++;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        long writeEnd = bytesConsumed + len;
        if (writeEnd > start && bytesConsumed < end) {
            int copyStart = (int) Math.max(0L, start - bytesConsumed);
            int copyEnd = (int) Math.min((long) len, end - bytesConsumed);
            out.write(b, off + copyStart, copyEnd - copyStart);
        }
        bytesConsumed += len;
    }

    @Override
    public void close() throws IOException {
        out.close();
    }
}
