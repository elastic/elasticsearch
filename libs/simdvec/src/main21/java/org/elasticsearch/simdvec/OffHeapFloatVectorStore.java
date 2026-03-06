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
 * Off-heap float vector storage backed by paged {@link MemorySegment}s.
 * Vectors are packed into pages of {@link #PAGE_SIZE} vectors each,
 * reducing allocation overhead and improving spatial locality compared
 * to one-segment-per-vector. The last page may be partially filled.
 */
public class OffHeapFloatVectorStore implements Closeable {

    static final int PAGE_SIZE = 64;

    private final int dim;
    private final long vectorByteSize;
    private final Arena arena;
    private final List<MemorySegment> pages;
    private int count;

    public OffHeapFloatVectorStore(int dim) {
        this.dim = dim;
        this.vectorByteSize = (long) dim * Float.BYTES;
        this.arena = Arena.ofShared();
        this.pages = new ArrayList<>();
    }

    public void addVector(float[] vector) {
        int offsetInPage = count % PAGE_SIZE;
        if (offsetInPage == 0) {
            pages.add(arena.allocate(PAGE_SIZE * vectorByteSize));
        }
        MemorySegment page = pages.getLast();
        MemorySegment.copy(vector, 0, page, ValueLayout.JAVA_FLOAT, offsetInPage * vectorByteSize, dim);
        count++;
    }

    public float[] getVector(int i) {
        return getVectorSegment(i).toArray(ValueLayout.JAVA_FLOAT);
    }

    public MemorySegment getVectorSegment(int i) {
        return pages.get(i / PAGE_SIZE).asSlice((long) (i % PAGE_SIZE) * vectorByteSize, vectorByteSize);
    }

    public int size() {
        return count;
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
        assert length == count;
        for (int i = 0; i < length; i++) {
            MemorySegment seg = getVectorSegment(i);
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
