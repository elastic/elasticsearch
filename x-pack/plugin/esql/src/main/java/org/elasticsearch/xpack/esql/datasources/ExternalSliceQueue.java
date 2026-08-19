/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread-safe queue of {@link ExternalSplit} instances for parallel external source execution.
 * Analogous to {@code LuceneSliceQueue} but simpler: splits are claimed sequentially
 * via {@link #nextSplit()} until exhausted.
 */
public final class ExternalSliceQueue {

    private final ExternalSplit[] splits;
    private final AtomicInteger index;

    public ExternalSliceQueue(List<ExternalSplit> splits) {
        if (splits == null) {
            throw new IllegalArgumentException("splits must not be null");
        }
        this.splits = splits.toArray(ExternalSplit[]::new);
        this.index = new AtomicInteger(0);
    }

    @Nullable
    public ExternalSplit nextSplit() {
        int i = index.getAndIncrement();
        if (i < splits.length) {
            return splits[i];
        }
        return null;
    }

    public int totalSlices() {
        return splits.length;
    }

    /**
     * Returns the approximate number of splits not yet claimed. The value may be stale
     * under concurrent access but is accurate enough for batch-size heuristics.
     */
    public int remaining() {
        return Math.max(0, splits.length - index.get());
    }

    /**
     * Atomically claims up to {@code max} consecutive splits. Returns an empty list when
     * the queue is exhausted. The returned list may be smaller than {@code max} when
     * fewer splits remain.
     */
    public List<ExternalSplit> nextSplits(int max) {
        int start = index.getAndAdd(max);
        if (start >= splits.length) {
            return List.of();
        }
        int end = Math.min(start + max, splits.length);
        return List.of(Arrays.copyOfRange(splits, start, end));
    }
}
