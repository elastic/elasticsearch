/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;

/**
 * A row that belongs to a group, identified by an integer group ID.
 * Stores encoded sort keys and values for a single row within a grouped top-N operation.
 */
final class GroupedRow implements Accountable, Comparable<GroupedRow>, Releasable {
    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(GroupedRow.class);

    private final CircuitBreaker breaker;

    /**
     * The sort keys, encoded into bytes so we can sort by calling {@link Arrays#compareUnsigned}.
     */
    private final BreakingBytesRefBuilder keys;

    /**
     * Values to reconstruct the row. When we reconstruct the row we read from both the
     * {@link #keys} and the {@link #values}. So this only contains what is required to
     * reconstruct the row that isn't already stored in {@link #keys}.
     */
    private final BreakingBytesRefBuilder values;

    /**
     * Reference counter for the shard this row belongs to, used for rows containing a DocVector
     * to ensure the shard context lives until we build the final result.
     */
    @Nullable
    private RefCounted shardRefCounter;

    /**
     * The group ID this row belongs to.
     */
    long groupId = -1;

    GroupedRow(CircuitBreaker breaker, int preAllocatedKeysSize, int preAllocatedValueSize) {
        breaker.addEstimateBytesAndMaybeBreak(SHALLOW_SIZE, "GroupedRow");
        this.breaker = breaker;
        boolean success = false;
        try {
            keys = new BreakingBytesRefBuilder(breaker, "topn", preAllocatedKeysSize);
            values = new BreakingBytesRefBuilder(breaker, "topn", preAllocatedValueSize);
            success = true;
        } finally {
            if (success == false) {
                close();
            }
        }
    }

    BreakingBytesRefBuilder keys() {
        return keys;
    }

    BreakingBytesRefBuilder values() {
        return values;
    }

    void setShardRefCounted(RefCounted shardRefCounted) {
        if (this.shardRefCounter != null) {
            this.shardRefCounter.decRef();
        }
        this.shardRefCounter = shardRefCounted;
        this.shardRefCounter.mustIncRef();
    }

    void clear() {
        keys.clear();
        values.clear();
        clearRefCounters();
        groupId = -1;
    }

    private void clearRefCounters() {
        if (shardRefCounter != null) {
            shardRefCounter.decRef();
        }
        shardRefCounter = null;
    }

    @Override
    public int compareTo(GroupedRow other) {
        return -keys.bytesRefView().compareTo(other.keys.bytesRefView());
    }

    @Override
    public long ramBytesUsed() {
        return SHALLOW_SIZE + keys.ramBytesUsed() + values.ramBytesUsed();
    }

    @Override
    public void close() {
        clearRefCounters();
        Releasables.closeExpectNoException(() -> breaker.addWithoutBreaking(-SHALLOW_SIZE), keys, values);
    }
}
