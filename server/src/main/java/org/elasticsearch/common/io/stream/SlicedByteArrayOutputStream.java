/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.io.stream;

import java.io.ByteArrayOutputStream;

/**
 * A {@link ByteArrayOutputStream} that collects only bytes in the range {@code [offset, offset + length)},
 * discarding bytes outside that range.
 */
public final class SlicedByteArrayOutputStream extends ByteArrayOutputStream {

    private final int start;
    private final int end;
    private int bytesConsumed;

    public SlicedByteArrayOutputStream(int offset, int length) {
        super(length);
        this.start = offset;
        this.end = offset + length;
    }

    @Override
    public void write(int b) {
        if (bytesConsumed >= start && bytesConsumed < end) super.write(b);
        bytesConsumed++;
    }

    @Override
    public void write(byte[] b, int off, int len) {
        int writeEnd = bytesConsumed + len;
        if (writeEnd > start && bytesConsumed < end) {
            int copyStart = Math.max(0, start - bytesConsumed);
            int copyEnd = Math.min(len, end - bytesConsumed);
            super.write(b, off + copyStart, copyEnd - copyStart);
        }
        bytesConsumed += len;
    }
}
