/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

import java.io.Closeable;

/**
 * Off-heap storage for byte vectors.
 * <p>
 * This is a compile-time stub; the real implementation is loaded from the
 * {@code main21} multi-release source set at runtime.
 */
public class OffHeapByteVectorStore implements Closeable {

    public OffHeapByteVectorStore(int dim) {
        throw new UnsupportedOperationException();
    }

    public void addVector(byte[] vector) {
        throw new UnsupportedOperationException();
    }

    public byte[] getVector(int i) {
        throw new UnsupportedOperationException();
    }

    public int size() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException();
    }
}
