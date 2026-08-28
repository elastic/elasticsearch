/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

import java.io.IOException;

/**
 * Generic scorer interface for ASH-encoded vectors.
 * <p>
 * The type parameter {@code T} is the query representation:
 * <ul>
 *   <li>{@code float[]} for the float-query path ({@code queryBitsPerDim == 0})</li>
 *   <li>{@code byte[]} for the integer-query path ({@code queryBitsPerDim > 0})</li>
 * </ul>
 * <p>
 * Implementations read packed bit-plane codes from an {@link org.apache.lucene.store.IndexInput}
 * and compute raw (uncorrected) dot products. The caller is responsible for applying
 * per-vector corrections (scale, offset, docSum) after the codes have been read.
 */
public interface AshScorer<T> {

    /** Score a single vector. Advances the IndexInput past one vector's packed code bytes. */
    float score(T query) throws IOException;

    /** Score a bulk of vectors. Advances the IndexInput past {@code blockSize} vectors' packed code bytes. */
    void scoreBulk(T query, float[] scores, int blockSize) throws IOException;
}
