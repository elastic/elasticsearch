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
 * A {@link java.util.List} view over an {@link OffHeapByteVectorStore}.
 * Materializes vectors on demand via {@link OffHeapByteVectorStore#getVector(int)}.
 */
public class WrappedNativeByteVectors extends AbstractList<byte[]> {
    private final OffHeapByteVectorStore store;

    public WrappedNativeByteVectors(OffHeapByteVectorStore store) {
        this.store = store;
    }

    @Override
    public byte[] get(int index) {
        return store.getVector(index);
    }

    @Override
    public int size() {
        return store.size();
    }

    public OffHeapByteVectorStore getStore() {
        return store;
    }
}
