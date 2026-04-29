/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.next;

import org.apache.lucene.store.IndexInput;
import org.elasticsearch.index.codec.vectors.diskbbq.CentroidIterator;
import org.elasticsearch.index.codec.vectors.diskbbq.PostingMetadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * {@link CentroidIterator} that hints posting reads in two phases. The first {@code initialCentroidBatchSize} posting lists from
 * the delegate are pulled immediately and {@link IndexInput#prefetch(long, long)} is invoked for each before any posting is
 * returned. The remaining posting lists use a fixed-depth ring buffer (same idea as {@code PrefetchingCentroidIterator})
 * so additional centroids are prefetched incrementally as the search explores more clusters.
 * This iterator is not thread-safe.
 */
public final class BudgetPrefetchCentroidIterator implements CentroidIterator {

    private final List<PostingMetadata> initialBatch;
    private int initialBatchIndex;
    private final CentroidIterator delegate;
    private final IndexInput postingListSlice;
    private final PostingMetadata[] ring;
    private final int ringCapacity;
    private int ringRead;
    private int ringWrite;
    private int ringCount;

    /**
     * @param delegate                   source of posting metadata in visit order (consumed progressively)
     * @param postingListSlice           slice whose posting bytes will be prefetched
     * @param initialCentroidBatchSize   non-negative number of centroids to include in the initial prefetch batch
     * @param ringPrefetchDepth          ring buffer depth for expansion prefetches ({@code >= 1})
     */
    public BudgetPrefetchCentroidIterator(
        CentroidIterator delegate,
        IndexInput postingListSlice,
        int initialCentroidBatchSize,
        int ringPrefetchDepth
    ) throws IOException {
        if (ringPrefetchDepth < 1) {
            throw new IllegalArgumentException("ringPrefetchDepth must be at least 1, got: " + ringPrefetchDepth);
        }
        if (initialCentroidBatchSize < 0) {
            throw new IllegalArgumentException("initialCentroidBatchSize must be non-negative, got: " + initialCentroidBatchSize);
        }
        this.delegate = delegate;
        this.postingListSlice = postingListSlice;
        this.ringCapacity = ringPrefetchDepth;
        this.ring = new PostingMetadata[ringPrefetchDepth];
        this.initialBatch = new ArrayList<>();
        for (int i = 0; i < initialCentroidBatchSize && delegate.hasNext(); i++) {
            PostingMetadata md = delegate.nextPosting();
            initialBatch.add(md);
            postingListSlice.prefetch(md.offset(), md.length());
        }
        refillRing();
    }

    private void refillRing() throws IOException {
        while (ringCount < ringCapacity && delegate.hasNext()) {
            PostingMetadata md = delegate.nextPosting();
            ring[ringWrite] = md;
            ringWrite = (ringWrite + 1) % ringCapacity;
            ringCount++;
            postingListSlice.prefetch(md.offset(), md.length());
        }
    }

    @Override
    public boolean hasNext() {
        return initialBatchIndex < initialBatch.size() || ringCount > 0 || delegate.hasNext();
    }

    @Override
    public PostingMetadata nextPosting() throws IOException {
        if (initialBatchIndex < initialBatch.size()) {
            return initialBatch.get(initialBatchIndex++);
        }
        if (ringCount == 0) {
            refillRing();
        }
        if (ringCount == 0) {
            throw new IllegalStateException("No more elements available");
        }
        PostingMetadata result = ring[ringRead];
        ringRead = (ringRead + 1) % ringCapacity;
        ringCount--;
        refillRing();
        return result;
    }
}
