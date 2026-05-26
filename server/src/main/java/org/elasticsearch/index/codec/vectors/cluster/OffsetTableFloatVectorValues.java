/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.cluster;

import org.apache.lucene.store.IndexInput;

import java.io.IOException;

/**
 * Off-heap {@link ClusteringFloatVectorValues} where each vector is stored at a pre-computed
 * file offset rather than at a uniform stride. Used by formats whose centroid bytes are
 * interleaved with per-centroid metadata in the same file, so that the merge path can stream
 * the centroids one-at-a-time without materializing the entire {@code float[N][dim]} on the heap.
 *
 * <p>Returns the same reusable {@code float[dim]} scratch buffer from {@link #vectorValue(int)};
 * callers that need to retain a vector across subsequent calls must copy it.
 */
public final class OffsetTableFloatVectorValues extends ClusteringFloatVectorValues {

    private final IndexInput input;
    private final long[] offsets;
    private final int numVectors;
    private final int dims;
    private final float[] scratch;

    public OffsetTableFloatVectorValues(IndexInput input, long[] offsets, int numVectors, int dims) {
        assert offsets.length >= numVectors;
        this.input = input;
        this.offsets = offsets;
        this.numVectors = numVectors;
        this.dims = dims;
        this.scratch = new float[dims];
    }

    @Override
    public float[] vectorValue(int ord) throws IOException {
        input.seek(offsets[ord]);
        input.readFloats(scratch, 0, dims);
        return scratch;
    }

    @Override
    public int dimension() {
        return dims;
    }

    @Override
    public int size() {
        return numVectors;
    }

    @Override
    public OffsetTableFloatVectorValues copy() {
        return new OffsetTableFloatVectorValues(input.clone(), offsets, numVectors, dims);
    }
}
