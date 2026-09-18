/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.cluster;

import org.apache.lucene.util.hnsw.IntToIntFunction;

import java.io.IOException;

/**
 * A slice view of {@link ClusteringByteVectorValues} that remaps ordinals via a provided function.
 * Used during sliced merge to cluster subsets of byte vectors independently.
 */
public final class ClusteringByteVectorValuesSlice extends ClusteringByteVectorValues {

    private final ClusteringByteVectorValues allValues;
    private final IntToIntFunction ordTranslator;
    private final int size;

    public ClusteringByteVectorValuesSlice(ClusteringByteVectorValues allValues, IntToIntFunction ordTranslator, int size) {
        assert ordTranslator != null;
        assert allValues.size() >= size;
        this.allValues = allValues;
        this.ordTranslator = ordTranslator;
        this.size = size;
    }

    @Override
    public byte[] vectorValue(int ord) throws IOException {
        return this.allValues.vectorValue(ordTranslator.apply(ord));
    }

    @Override
    public int dimension() {
        return this.allValues.dimension();
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public int ordToDoc(int ord) {
        return ordTranslator.apply(ord);
    }

    @Override
    public ClusteringByteVectorValuesSlice copy() throws IOException {
        return new ClusteringByteVectorValuesSlice(this.allValues.copy(), this.ordTranslator, size);
    }
}
