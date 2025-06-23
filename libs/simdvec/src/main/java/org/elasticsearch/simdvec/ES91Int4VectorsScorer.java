/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.simdvec;

import org.apache.lucene.store.IndexInput;

import java.io.IOException;

/** Scorer for quantized vectors stored as an {@link IndexInput}.
 * <p>
 * Similar to {@link org.apache.lucene.util.VectorUtil#int4DotProduct(byte[], byte[])} but
 * one value is read directly from an {@link IndexInput}.
 *
 * */
public class ES91Int4VectorsScorer {

    /** The wrapper {@link IndexInput}. */
    protected final IndexInput in;
    protected final int dimensions;

    /** Sole constructor, called by sub-classes. */
    public ES91Int4VectorsScorer(IndexInput in, int dimensions) {
        this.in = in;
        this.dimensions = dimensions;
    }

    public long int4DotProduct(byte[] b) throws IOException {
        int total = 0;
        for (int i = 0; i < dimensions; i++) {
            total += in.readByte() * b[i];
        }
        return total;
    }
}
