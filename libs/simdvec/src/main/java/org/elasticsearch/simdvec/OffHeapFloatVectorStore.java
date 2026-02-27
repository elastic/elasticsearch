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
 * Off-heap storage for float vectors.
 * <p>
 * This is a compile-time stub; the real implementation is loaded from the
 * {@code main21} multi-release source set at runtime.
 */
public class OffHeapFloatVectorStore implements Closeable {

    public OffHeapFloatVectorStore(int dim) {
        throw new UnsupportedOperationException();
    }

    public void addVector(float[] vector) {
        throw new UnsupportedOperationException();
    }

    public float[] getVector(int i) {
        throw new UnsupportedOperationException();
    }

    public int size() {
        throw new UnsupportedOperationException();
    }

    /**
     * Divides each vector's components by the corresponding magnitude.
     * Used for COSINE similarity normalization in the BQ path.
     *
     * @param magnitudes array containing magnitude values
     * @param offset     start index in the magnitudes array
     * @param length     number of vectors to normalize (must equal {@link #size()})
     */
    public void normalizeByMagnitudes(float[] magnitudes, int offset, int length) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException();
    }
}
