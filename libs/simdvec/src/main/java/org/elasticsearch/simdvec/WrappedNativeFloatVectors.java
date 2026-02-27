/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

import java.util.AbstractList;

/**
 * A {@link java.util.List} view over an {@link OffHeapFloatVectorStore}.
 * Materializes vectors on demand via {@link OffHeapFloatVectorStore#getVector(int)}.
 * The scorer in simdvec can detect this class via reflection and use
 * {@link #getStore()} to access the underlying MemorySegments directly.
 */
public class WrappedNativeFloatVectors extends AbstractList<float[]> {
    private final OffHeapFloatVectorStore store;

    public WrappedNativeFloatVectors(OffHeapFloatVectorStore store) {
        this.store = store;
    }

    @Override
    public float[] get(int index) {
        return store.getVector(index);
    }

    @Override
    public int size() {
        return store.size();
    }

    public OffHeapFloatVectorStore getStore() {
        return store;
    }
}
