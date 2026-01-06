/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasables;

import java.util.List;

/**
 * Internal row to be used in the PriorityQueue instead of the full blown Page.
 * It mirrors somehow the Block build in the sense that it keeps around an array of offsets and a count of values (to account for
 * multivalues) to reference each position in each block of the Page.
 */
final class UngroupedRow implements Row {
    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(UngroupedRow.class);

    final CircuitBreaker breaker;

    /**
     * The sort key.
     */
    private final BreakingBytesRefBuilder keys;

    @Override
    public BreakingBytesRefBuilder keys() {
        return keys;
    }

    /**
     * A true/false value (bit set/unset) for each byte in the BytesRef above corresponding to an asc/desc ordering.
     * For ex, if a Long is represented as 8 bytes, each of these bytes will have the same value (set/unset) if the respective Long
     * value is used for sorting ascending/descending.
     */
    private final TopNOperator.BytesOrder bytesOrder;

    @Override
    public TopNOperator.BytesOrder bytesOrder() {
        return bytesOrder;
    }

    /**
     * Values to reconstruct the row. Sort of. When we reconstruct the row we read
     * from both the {@link #keys} and the {@link #values}. So this only contains
     * what is required to reconstruct the row that isn't already stored in {@link #values}.
     */
    private final BreakingBytesRefBuilder values;

    @Override
    public BreakingBytesRefBuilder values() {
        return values;
    }

    /**
     * Reference counter for the shard this row belongs to, used for rows containing a {@link DocVector} to ensure that the shard
     * context before we build the final result.
     */
    @Nullable
    private RefCounted shardRefCounter;

    UngroupedRow(CircuitBreaker breaker, List<TopNOperator.SortOrder> sortOrders, int preAllocatedKeysSize, int preAllocatedValueSize) {
        breaker.addEstimateBytesAndMaybeBreak(SHALLOW_SIZE, "topn");
        this.breaker = breaker;
        boolean success = false;
        try {
            keys = new BreakingBytesRefBuilder(breaker, "topn", preAllocatedKeysSize);
            values = new BreakingBytesRefBuilder(breaker, "topn", preAllocatedValueSize);
            bytesOrder = new TopNOperator.BytesOrder(sortOrders, breaker, "topn");
            success = true;
        } finally {
            if (success == false) {
                close();
            }
        }
    }

    @Override
    public long ramBytesUsed() {
        return SHALLOW_SIZE + keys.ramBytesUsed() + bytesOrder.ramBytesUsed() + values.ramBytesUsed();
    }

    @Override
    public void clear() {
        keys.clear();
        values.clear();
        clearRefCounters();
    }

    @Override
    public void close() {
        clearRefCounters();
        breaker.addWithoutBreaking(-SHALLOW_SIZE);
        Releasables.closeExpectNoException(keys);
        Releasables.closeExpectNoException(bytesOrder);
        Releasables.closeExpectNoException(values);
    }

    public void clearRefCounters() {
        if (shardRefCounter != null) {
            shardRefCounter.decRef();
        }
        shardRefCounter = null;
    }

    @Override
    public void setShardRefCounted(RefCounted shardRefCounted) {
        if (this.shardRefCounter != null) {
            this.shardRefCounter.decRef();
        }
        this.shardRefCounter = shardRefCounted;
        this.shardRefCounter.mustIncRef();
    }
}
