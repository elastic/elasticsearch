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
 * A single row in a top-N operation. Stores encoded sort keys and values.
 * Implements {@link Comparable} and {@link #equals} comparing the sort keys.
 */
final class TopNRow implements Accountable, Comparable<TopNRow>, Releasable {
    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(TopNRow.class);

    private final CircuitBreaker breaker;

    /**
     * The sort keys, encoded into bytes so we can sort by calling {@link Arrays#compareUnsigned}.
     */
    final BreakingBytesRefBuilder keys;

    /**
     * Values to reconstruct the row. When we reconstruct the row we read
     * from both the {@link #keys} and the {@link #values}. So this only contains
     * what is required to reconstruct the row that isn't already stored in {@link #keys}.
     * Null once the values went to a value segment, see {@link #valueSegment}: the lane takes the builder and nulls it.
     */
    @Nullable
    BreakingBytesRefBuilder values;

    /**
     * Where the values went when they no longer fit in memory: the segment of the operator's value store, or -1 while
     * they are in {@link #values}; then the offset and the length of the values in that segment.
     */
    int valueSegment = -1;
    long valueOffset;
    int valueLength;

    /**
     * Reference counter for the shard this row belongs to, used for rows containing a
     * DocVector to ensure the shard context lives until we build the final result.
     */
    @Nullable
    RefCounted shardRefCounter;

    TopNRow(CircuitBreaker breaker, int preAllocatedKeysSize, int preAllocatedValueSize) {
        breaker.addEstimateBytesAndMaybeBreak(SHALLOW_SIZE, "topn");
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

    @Override
    public long ramBytesUsed() {
        return SHALLOW_SIZE + keys.ramBytesUsed() + (values == null ? 0 : values.ramBytesUsed());
    }

    /**
     * Whether the values live in a value segment rather than in {@link #values}.
     */
    boolean spilled() {
        return valueSegment >= 0;
    }

    /**
     * Gives a row whose values were handed over a builder again, for the next row's values to be extracted into.
     */
    void ensureValues(int preAllocatedValueSize) {
        if (values == null) {
            values = new BreakingBytesRefBuilder(breaker, "topn", preAllocatedValueSize);
        }
    }

    @Override
    public void close() {
        clearRefCounters();
        Releasables.closeExpectNoException(() -> breaker.addWithoutBreaking(-SHALLOW_SIZE), keys, values);
    }

    void clear() {
        keys.clear();
        if (values != null) {
            values.clear();
        }
        valueSegment = -1;
        valueOffset = 0;
        valueLength = 0;
        clearRefCounters();
    }

    void clearRefCounters() {
        if (shardRefCounter != null) {
            shardRefCounter.decRef();
        }
        shardRefCounter = null;
    }

    void setShardRefCounted(RefCounted shardRefCounted) {
        if (this.shardRefCounter != null) {
            this.shardRefCounter.decRef();
        }
        this.shardRefCounter = shardRefCounted;
        this.shardRefCounter.mustIncRef();
    }

    @Override
    public int compareTo(TopNRow rhs) {
        // TODO if we fill the trailing bytes with 0 we could do a comparison on the entire array
        // When Nik measured this it was marginally faster. But it's worth a bit of research.
        return -keys.bytesRefView().compareTo(rhs.keys.bytesRefView());
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TopNRow row = (TopNRow) o;
        return keys.bytesRefView().equals(row.keys.bytesRefView());
    }

    @Override
    public int hashCode() {
        return keys.bytesRefView().hashCode();
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder("TopNRow[key=");
        b.append(keys.bytesRefView());
        b.append(", values=");

        if (values == null) {
            b.append("segment ").append(valueSegment).append('@').append(valueOffset).append('+').append(valueLength);
        } else if (values.length() < 100) {
            b.append(values.bytesRefView());
        } else {
            b.append('[');
            assert values.bytesRefView().offset == 0;
            for (int i = 0; i < 100; i++) {
                if (i != 0) {
                    b.append(" ");
                }
                b.append(Integer.toHexString(values.bytesRefView().bytes[i] & 255));
            }
            b.append("...");
        }
        return b.append("]").toString();
    }
}
