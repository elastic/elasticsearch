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
import java.util.ArrayDeque;

/**
 * A {@link CentroidIterator} that prefetches upcoming posting lists ahead of consumption so that
 * disk / blob-cache I/O overlaps with scoring. This iterator wraps another {@link CentroidIterator}
 * and keeps a buffer of prefetched posting list locations.
 *
 * <p>Rather than prefetching a fixed number of posting lists, the depth is derived from the
 * iterator's own state: the iterator keeps issuing prefetch hints until the cumulative byte length
 * of the posting lists that have been prefetched but not yet returned reaches {@code maxPrefetchBytes}.
 * This naturally adapts the prefetch depth to the size of the posting lists &mdash; many small lists
 * or only a few large ones &mdash; while the byte budget bounds how far ahead we read so we do not
 * eagerly fetch posting lists the query is unlikely to score. At least one posting list is always
 * prefetched ahead, preserving the previous behaviour when the budget is too small to cover even a
 * single list.
 *
 * <p>The iterator is not thread-safe and is designed for single-threaded access.
 */
public final class PrefetchingCentroidIterator implements CentroidIterator {

    private final CentroidIterator delegate;
    private final IndexInput postingListSlice;
    private final long maxPrefetchBytes;

    // Buffer of prefetched-but-not-yet-returned posting lists, kept in iteration order.
    private final ArrayDeque<PostingMetadata> prefetchBuffer = new ArrayDeque<>();
    // Cumulative byte length of the posting lists currently held in the buffer.
    private long bytesInFlight = 0;

    /**
     * Creates a prefetching iterator.
     *
     * @param delegate the underlying centroid iterator
     * @param postingListSlice the index input for posting lists
     * @param maxPrefetchBytes the target number of posting list bytes to keep prefetched ahead of the
     *                         consumer; negative values are treated as zero, in which case a single
     *                         posting list is prefetched ahead
     * @throws IOException if prefetching fails during initialization
     */
    public PrefetchingCentroidIterator(CentroidIterator delegate, IndexInput postingListSlice, long maxPrefetchBytes) throws IOException {
        this.delegate = delegate;
        this.postingListSlice = postingListSlice;
        this.maxPrefetchBytes = Math.max(maxPrefetchBytes, 0);
        // Initialize the buffer by prefetching ahead up to the configured byte budget.
        fillBuffer();
    }

    /**
     * Tops up the prefetch buffer, issuing a prefetch hint for each newly buffered posting list.
     * Always buffers at least one posting list (so {@link #hasNext()} can make progress) and then
     * continues until the in-flight byte window reaches the configured budget or the delegate is
     * exhausted.
     */
    private void fillBuffer() throws IOException {
        while (delegate.hasNext() && (prefetchBuffer.isEmpty() || bytesInFlight < maxPrefetchBytes)) {
            PostingMetadata offsetAndLength = delegate.nextPosting();
            prefetchBuffer.addLast(offsetAndLength);
            bytesInFlight += offsetAndLength.length();

            // Trigger prefetch
            postingListSlice.prefetch(offsetAndLength.offset(), offsetAndLength.length());
        }
    }

    @Override
    public boolean hasNext() {
        return prefetchBuffer.isEmpty() == false;
    }

    @Override
    public PostingMetadata nextPosting() throws IOException {
        if (prefetchBuffer.isEmpty()) {
            throw new IllegalStateException("No more elements available");
        }

        // Get the next element from buffer
        PostingMetadata result = prefetchBuffer.pollFirst();
        bytesInFlight -= result.length();

        // Top up the buffer, prefetching any newly revealed posting lists.
        fillBuffer();

        return result;
    }
}
