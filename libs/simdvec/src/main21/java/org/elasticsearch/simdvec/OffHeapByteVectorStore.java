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
 * Off-heap byte vector storage backed by paged {@link MemorySegment}s.
 * Vectors are packed into pages of {@link #PAGE_SIZE} vectors each,
 * reducing allocation overhead and improving spatial locality compared
 * to one-segment-per-vector. The last page may be partially filled.
 */
public class OffHeapByteVectorStore implements Closeable {

    static final int PAGE_SIZE = 64;

    private final int dim;
    private final Arena arena;
    private final List<MemorySegment> pages;
    private int count;

    public OffHeapByteVectorStore(int dim) {
        this.dim = dim;
        this.arena = Arena.ofShared();
        this.pages = new ArrayList<>();
    }

    public void addVector(byte[] vector) {
        int offsetInPage = count % PAGE_SIZE;
        if (offsetInPage == 0) {
            pages.add(arena.allocate((long) PAGE_SIZE * dim));
        }
        MemorySegment page = pages.getLast();
        MemorySegment.copy(vector, 0, page, ValueLayout.JAVA_BYTE, (long) offsetInPage * dim, dim);
        count++;
    }

    public byte[] getVector(int i) {
        return getVectorSegment(i).toArray(ValueLayout.JAVA_BYTE);
    }

    public MemorySegment getVectorSegment(int i) {
        return pages.get(i / PAGE_SIZE).asSlice((long) (i % PAGE_SIZE) * dim, dim);
    }

    public int size() {
        return count;
    }

    @Override
    public void close() {
        arena.close();
    }
}
