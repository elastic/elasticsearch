/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;

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
}
