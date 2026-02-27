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
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.List;

/**
 * Off-heap byte vector storage backed by {@link MemorySegment}s.
 * Each vector is individually allocated in a shared {@link Arena},
 * keeping vector data out of the Java heap.
 */
public class OffHeapByteVectorStore implements Closeable {

    private final int dim;
    private final Arena arena;
    private final List<MemorySegment> vectors;

    public OffHeapByteVectorStore(int dim) {
        this.dim = dim;
        this.arena = Arena.ofShared();
        this.vectors = new ArrayList<>();
    }

    public void addVector(byte[] vector) {
        MemorySegment segment = arena.allocate(dim);
        MemorySegment.copy(vector, 0, segment, ValueLayout.JAVA_BYTE, 0, dim);
        vectors.add(segment);
    }

    public byte[] getVector(int i) {
        return vectors.get(i).toArray(ValueLayout.JAVA_BYTE);
    }

    public MemorySegment getVectorSegment(int i) {
        return vectors.get(i);
    }

    public int size() {
        return vectors.size();
    }

    @Override
    public void close() {
        arena.close();
    }
}
