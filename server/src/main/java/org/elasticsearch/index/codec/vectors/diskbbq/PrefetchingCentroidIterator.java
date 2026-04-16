/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

import org.apache.lucene.store.IndexInput;

import java.io.IOException;

/**
 * A configurable iterator that prefetches posting lists ahead of consumption
 * to optimize disk I/O performance. This iterator wraps another CentroidIterator
 * and maintains a configurable buffer of prefetched posting list locations.
 *
 * The iterator is not thread-safe and is designed for single-threaded access.
 */
public final class PrefetchingCentroidIterator implements CentroidIterator {

    private final CentroidIterator delegate;
    private final IndexInput postingListSlice;
    private final int prefetchAhead;

    // Ring buffer for prefetched offsets and lengths
    private final PostingMetadata[] prefetchBuffer;
    private int readIndex = 0;  // Where we read from buffer
    private int writeIndex = 0; // Where we write to buffer
    private int bufferCount = 0; // Number of elements in buffer

    /**
     * Creates a prefetching iterator with default prefetch depth of 1.
     *
     * @param delegate the underlying centroid iterator
     * @param postingListSlice the index input for posting lists
     * @throws IOException if prefetching fails during initialization
     */
    public PrefetchingCentroidIterator(CentroidIterator delegate, IndexInput postingListSlice) throws IOException {
        this(delegate, postingListSlice, 1);
    }

    /**
     * Creates a prefetching iterator with configurable prefetch depth.
     *
     * @param delegate the underlying centroid iterator
     * @param postingListSlice the index input for posting lists
     * @param prefetchAhead number of posting lists to prefetch ahead (must be &gt;= 1)
     * @throws IOException if prefetching fails during initialization
     * @throws IllegalArgumentException if {@code prefetchAhead < 1}
     */
    public PrefetchingCentroidIterator(CentroidIterator delegate, IndexInput postingListSlice, int prefetchAhead) throws IOException {
        if (prefetchAhead < 1) {
            throw new IllegalArgumentException("prefetchAhead must be at least 1, got: " + prefetchAhead);
        }
        this.delegate = delegate;
        this.postingListSlice = postingListSlice;
        this.prefetchAhead = prefetchAhead;
        this.prefetchBuffer = new PostingMetadata[prefetchAhead];
        // Initialize buffer by prefetching up to prefetchAhead elements
        fillBuffer();
    }

    /**
     * Fills the prefetch buffer up to the configured capacity.
     */
    private void fillBuffer() throws IOException {
        while (bufferCount < prefetchAhead && delegate.hasNext()) {
            PostingMetadata offsetAndLength = delegate.nextPosting();
            prefetchBuffer[writeIndex] = offsetAndLength;
            writeIndex = (writeIndex + 1) % prefetchAhead;
            bufferCount++;

            // Trigger prefetch
            postingListSlice.prefetch(offsetAndLength.offset(), offsetAndLength.length());
        }
    }

    @Override
    public boolean hasNext() {
        return bufferCount > 0;
    }

    @Override
    public PostingMetadata nextPosting() throws IOException {
        if (bufferCount == 0) {
            throw new IllegalStateException("No more elements available");
        }

        // Get the next element from buffer
        PostingMetadata result = prefetchBuffer[readIndex];
        readIndex = (readIndex + 1) % prefetchAhead;
        bufferCount--;

        // Try to fill buffer with one more element
        if (delegate.hasNext()) {
            PostingMetadata offsetAndLength = delegate.nextPosting();
            prefetchBuffer[writeIndex] = offsetAndLength;
            writeIndex = (writeIndex + 1) % prefetchAhead;
            bufferCount++;

            // Trigger prefetch for the newly added element
            postingListSlice.prefetch(offsetAndLength.offset(), offsetAndLength.length());
        }

        return result;
    }
}
