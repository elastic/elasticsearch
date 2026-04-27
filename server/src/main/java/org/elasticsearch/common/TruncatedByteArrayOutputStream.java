/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common;

import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;

final class TruncatedByteArrayOutputStream extends ByteArrayOutputStream {

    private final int maxBytes;

    TruncatedByteArrayOutputStream(int maxBytes) {
        this.maxBytes = maxBytes;
    }

    @Override
    public void write(int b) {
        if (isOverLimit()) {
            return;
        }
        super.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) {
        if (isOverLimit()) {
            return;
        }
        int remainingSpace = maxBytes - count;
        super.write(b, off, Math.min(len, remainingSpace));
    }

    boolean isOverLimit() {
        return count >= maxBytes;
    }

    @Override
    public String toString(Charset charset) {
        return new String(buf, 0, Math.min(count, maxBytes), charset);
    }
}
