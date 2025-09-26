/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.io.stream;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.function.IntSupplier;

/**
 * Truncates writes once the max size is exceeded.
 * However, when writing byte arrays, the stream does not check whether there is capacity for the full
 * array prior to writing, so there is overspill of up to b.length - 1.
 */
public class TruncatedOutputStream extends FilterOutputStream {
    private final IntSupplier currentSizeSupplier;
    private final int maxSize;
    private boolean hasCapacity = true;

    public TruncatedOutputStream(OutputStream out, IntSupplier currentSizeSupplier, int maxSize) {
        super(out);
        this.currentSizeSupplier = currentSizeSupplier;
        this.maxSize = maxSize;
    }

    private boolean hasCapacity() {
        if (hasCapacity) {
            hasCapacity = currentSizeSupplier.getAsInt() < maxSize;
        }
        return hasCapacity;
    }

    @Override
    public void write(int b) throws IOException {
        if (hasCapacity()) {
            out.write(b);
        }
    }

    @Override
    public void write(byte[] b) throws IOException {
        if (hasCapacity()) {
            out.write(b);
        }
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if (hasCapacity()) {
            out.write(b, off, len);
        }
    }
}
