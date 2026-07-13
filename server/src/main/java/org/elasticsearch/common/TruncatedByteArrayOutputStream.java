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

final class TruncatedByteArrayOutputStream extends ByteArrayOutputStream {

    private final int maxBytes;
    boolean truncated = false;

    TruncatedByteArrayOutputStream(int maxBytes) {
        this.maxBytes = maxBytes;
    }

    @Override
    public void write(int b) {
        if (isOverCapacity()) {
            truncated = true;
            return;
        }
        super.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) {
        if (isOverCapacity()) {
            truncated = true;
            return;
        }

        int remainingSpace = maxBytes - count;
        if (remainingSpace < len) {
            truncated = true;
            len = remainingSpace;
        }

        super.write(b, off, len);
    }

    private boolean isOverCapacity() {
        return count >= maxBytes;
    }

    /**
     * Returns {@code true} if this stream has actually truncated its contents.
     */
    boolean isTruncated() {
        return truncated;
    }
}
