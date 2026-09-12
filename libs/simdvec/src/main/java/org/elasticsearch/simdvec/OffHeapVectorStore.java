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
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntFunction;

/**
 * A paged off-heap native store for vectors accumulated during ingestion.
 *
 * <p>Vectors are stored in a sequence of fixed-size native {@link MemorySegment} pages allocated
 * from a shared {@link Arena}. The page size is a multiple of {@code dims * elementLayout.byteSize()}
 * so that no vector ever straddles a page boundary.
 *
 * <p>The virtual byte layout matches the on-disk format used by {@code Lucene99FlatVectorsWriter}:
 * {@code offset = ordinal * dims * elementLayout.byteSize()}, contiguous per vector. This allows us
 * to use the store directly in {@code IndexInput}-consuming suppliers like {@code Float32VectorScorerSupplier}
 * or {@code Int8VectorScorerSupplier} via a simple {@link OffHeapVectorInput} wrapper (see {@link #asIndexInput()}.
 *
 * <p>Use the static factories {@link #forFloats(int)} and {@link #forBytes(int)} to construct
 * typed instances.
 *
 * @param <T> the vector array type: {@code float[]} for FLOAT32, {@code byte[]} for BYTE
 */
public final class OffHeapVectorStore<T> implements Closeable {

    /** Number of vectors per page. */
    static final int VECTORS_PER_PAGE = 1024;

    private final Arena arena;
    private final List<MemorySegment> pages = new ArrayList<>();
    private final int dims;
    private final ValueLayout elementLayout;
    private final IntFunction<T> arrayFactory;
    private final int vectorByteSize;
    private final int pageBytes;

    private int count = 0;
    private int currentPageCount = 0; // vectors written into the current (last) page
    private MemorySegment currentPage;

    /**
     * Creates a new store for {@code float[]} vectors of the given dimension.
     *
     * @param dims number of float components per vector
     */
    public static OffHeapVectorStore<float[]> forFloats(int dims) {
        return new OffHeapVectorStore<>(dims, ValueLayout.JAVA_FLOAT, float[]::new);
    }

    /**
     * Creates a new store for {@code byte[]} vectors of the given dimension.
     *
     * @param dims number of byte components per vector
     */
    public static OffHeapVectorStore<byte[]> forBytes(int dims) {
        return new OffHeapVectorStore<>(dims, ValueLayout.JAVA_BYTE, byte[]::new);
    }

    private OffHeapVectorStore(int dims, ValueLayout elementLayout, IntFunction<T> arrayFactory) {
        this.dims = dims;
        this.elementLayout = elementLayout;
        this.arrayFactory = arrayFactory;
        this.vectorByteSize = dims * (int) elementLayout.byteSize();
        this.pageBytes = VECTORS_PER_PAGE * vectorByteSize;
        this.arena = Arena.ofShared();
    }

    private void allocatePage() {
        currentPage = arena.allocate(pageBytes);
        pages.add(currentPage);
        currentPageCount = 0;
    }

    /**
     * Appends a vector to the store. Must be called by a single thread at a time (the indexing
     * thread). The vector is copied into native memory; the caller may reuse {@code vector}.
     *
     * @param vector a {@code float[]} or {@code byte[]} of length {@link #dimension()}
     */
    public void append(T vector) {
        assert Array.getLength(vector) == dims : "expected " + dims + " elements, got " + Array.getLength(vector);
        if (currentPage == null || currentPageCount == VECTORS_PER_PAGE) {
            allocatePage();
        }
        long pageOffset = (long) currentPageCount * vectorByteSize;
        MemorySegment.copy(vector, 0, currentPage, elementLayout, pageOffset, dims);
        currentPageCount++;
        count++;
    }

    /**
     * Materializes the vector at {@code ordinal} into a new array.
     *
     * @param ordinal zero-based ordinal
     * @return a heap copy of the vector
     */
    public T get(int ordinal) {
        int pageIdx = ordinal / VECTORS_PER_PAGE;
        long pageOffset = (long) (ordinal % VECTORS_PER_PAGE) * vectorByteSize;
        T result = arrayFactory.apply(dims);
        MemorySegment.copy(pages.get(pageIdx), elementLayout, pageOffset, result, 0, dims);
        return result;
    }

    /** Returns the number of vectors currently in the store. */
    public int size() {
        return count;
    }

    /**
     * Returns the number of bytes committed to native memory by this store (whole pages, including the
     * unused tail of the last one). Callers use this for flush accounting, as it reflects memory
     * actually held rather than memory occupied by data.
     */
    public long nativeBytes() {
        return (long) pages.size() * pageBytes;
    }

    /** Returns the number of elements per vector. */
    public int dimension() {
        return dims;
    }

    /**
     * Returns an {@link OffHeapVectorInput} view over this store, suitable for use as a
     * {@link org.apache.lucene.codecs.lucene95.HasIndexSlice} slice in the native scorer chain.
     *
     * <p>The returned input reflects the live {@link #size()} of the store (growing as vectors are
     * appended).
     *
     * <p>Multiple views may be created; all share the same underlying pages and arena.
     */
    public OffHeapVectorInput asIndexInput() {
        return new OffHeapVectorInput(
            "OffHeapVectorStore(dims=" + dims + ", elementSize=" + elementLayout.byteSize() + ")",
            pages,
            arena,
            this::size,
            pageBytes,
            vectorByteSize
        );
    }

    /**
     * Closes the backing arena, releasing all native memory. Every {@link OffHeapVectorInput} view handed
     * out by {@link #asIndexInput()} becomes unusable, as does anything that read addresses out of one;
     * the views detect this and throw, but scorers built on top of them must already have been discarded.
     */
    @Override
    public void close() {
        if (arena.scope().isAlive()) {
            arena.close();
        }
    }
}
