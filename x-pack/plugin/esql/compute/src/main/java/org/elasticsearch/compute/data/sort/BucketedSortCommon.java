/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.sort;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.search.sort.SortOrder;

/**
 * Components common to BucketedSort implementations.
 */
class BucketedSortCommon implements Releasable {
    final BigArrays bigArrays;
    final SortOrder order;
    final int bucketSize;

    /**
     * {@code true} if the bucket is in heap mode, {@code false} if
     * it is still gathering.
     */
    private final BitArray heapMode;

    BucketedSortCommon(BigArrays bigArrays, SortOrder order, int bucketSize) {
        this.bigArrays = bigArrays;
        this.order = order;
        this.bucketSize = bucketSize;
        this.heapMode = new BitArray(0, bigArrays);
    }

    /**
     * The first index in a bucket. Note that this might not be <strong>used</strong>.
     * See {@link }
     */
    long rootIndex(int bucket) {
        return (long) bucket * bucketSize;
    }

    /**
     * The last index in a bucket.
     */
    long endIndex(long rootIndex) {
        return rootIndex + bucketSize;
    }

    boolean inHeapMode(int bucket) {
        return heapMode.get(bucket);
    }

    void enableHeapMode(int bucket) {
        heapMode.set(bucket);
    }

    void assertValidNextOffset(int next) {
        assert 0 <= next && next < bucketSize
            : "Expected next to be in the range of valid buckets [0 <= " + next + " < " + bucketSize + "]";
    }

    @Override
    public void close() {
        heapMode.close();
    }
}
