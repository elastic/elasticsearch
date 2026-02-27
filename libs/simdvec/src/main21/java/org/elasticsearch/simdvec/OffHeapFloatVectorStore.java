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
 * Off-heap float vector storage backed by {@link MemorySegment}s.
 * Each vector is individually allocated in a shared {@link Arena},
 * keeping vector data out of the Java heap.
 */
public class OffHeapFloatVectorStore implements Closeable {

    private final int dim;
    private final Arena arena;
    private final List<MemorySegment> vectors;

    public OffHeapFloatVectorStore(int dim) {
        this.dim = dim;
        this.arena = Arena.ofShared();
        this.vectors = new ArrayList<>();
    }

    public void addVector(float[] vector) {
        MemorySegment segment = arena.allocate((long) dim * Float.BYTES);
        MemorySegment.copy(vector, 0, segment, ValueLayout.JAVA_FLOAT, 0, dim);
        vectors.add(segment);
    }

    public float[] getVector(int i) {
        return vectors.get(i).toArray(ValueLayout.JAVA_FLOAT);
    }

    public MemorySegment getVectorSegment(int i) {
        return vectors.get(i);
    }

    public int size() {
        return vectors.size();
    }

    /**
     * Divides each vector's components by the corresponding magnitude.
     * Mutates the off-heap segments in place.
     *
     * @param magnitudes array containing magnitude values
     * @param offset     start index in the magnitudes array
     * @param length     number of vectors to normalize (must equal {@link #size()})
     */
    public void normalizeByMagnitudes(float[] magnitudes, int offset, int length) {
        assert length == vectors.size();
        for (int i = 0; i < length; i++) {
            MemorySegment seg = vectors.get(i);
            float magnitude = magnitudes[offset + i];
            for (int j = 0; j < dim; j++) {
                float val = seg.getAtIndex(ValueLayout.JAVA_FLOAT, j);
                seg.setAtIndex(ValueLayout.JAVA_FLOAT, j, val / magnitude);
            }
        }
    }

    @Override
    public void close() {
        arena.close();
    }
}
