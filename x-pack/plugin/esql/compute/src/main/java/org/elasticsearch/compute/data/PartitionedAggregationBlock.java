/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.PartitionedHashTable;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.io.IOException;

/**
 * Not a row-by-row block, but holds partitioned keys and aggregation states of a partial hash aggregation.
 * When the partial and final aggregations run on the same node and there are many groups, the partial emits
 * this instead of intermediate rows, so the final only combines partitions
 */
public class PartitionedAggregationBlock implements Block {
    private PartitionedHashTable.PartitionedHashKeys keys;
    private GroupingAggregatorFunction.PartitionedState[] aggs;
    private BlockFactory blockFactory;

    final AbstractRefCounted refs;
    final int numKeys;
    private Releasable attachedReleasable = null;

    @SuppressWarnings("this-escape")
    public PartitionedAggregationBlock(
        BlockFactory blockFactory,
        int numKeys,
        PartitionedHashTable.PartitionedHashKeys keys,
        GroupingAggregatorFunction.PartitionedState[] aggs
    ) {
        this.blockFactory = blockFactory;
        this.numKeys = numKeys;
        this.keys = keys;
        this.aggs = aggs;
        this.refs = AbstractRefCounted.of(() -> {
            final CircuitBreaker breaker = this.blockFactory.breaker();
            var currentKeys = this.keys;
            if (currentKeys != null) {
                this.keys = null;
                currentKeys.releaseAll(breaker);
            }
            var currentAggs = this.aggs;
            if (currentAggs != null) {
                this.aggs = null;
                for (var agg : currentAggs) {
                    agg.releaseAll(breaker);
                }
            }
            Releasables.close(attachedReleasable);
        });
    }

    PartitionedHashTable.PartitionedHashKeys keys() {
        return keys;
    }

    GroupingAggregatorFunction.PartitionedState[] aggs() {
        return aggs;
    }

    /**
     * Moves the keys out of this block; the caller becomes responsible for releasing them. See also {@link #takeAggs()}.
     */
    public PartitionedHashTable.PartitionedHashKeys takeKeys() {
        var result = keys;
        keys = null;
        return result;
    }

    /**
     * Moves the aggregation states out of this block; the caller becomes responsible for releasing them.
     */
    public GroupingAggregatorFunction.PartitionedState[] takeAggs() {
        var result = aggs;
        aggs = null;
        return result;
    }

    @Override
    public Vector asVector() {
        return null;
    }

    @Override
    public int getPositionCount() {
        return numKeys;
    }

    @Override
    public int getFirstValueIndex(int position) {
        return position;
    }

    @Override
    public int getValueCount(int position) {
        return 1;
    }

    @Override
    public int getTotalValueCount() {
        return numKeys;
    }

    @Override
    public ElementType elementType() {
        return ElementType.UNKNOWN;
    }

    @Override
    public int valueMaxByteSize() {
        return 0;
    }

    @Override
    public BlockFactory blockFactory() {
        return blockFactory;
    }

    @Override
    public void allowPassingToDifferentDriver() {
        blockFactory = blockFactory.parent();
    }

    @Override
    public boolean isReleased() {
        return refs.hasReferences() == false;
    }

    @Override
    public void attachReleasable(Releasable releasable) {
        this.attachedReleasable = attachedReleasable == null ? releasable : Releasables.wrap(attachedReleasable, releasable);
    }

    @Override
    public boolean isNull(int position) {
        return false;
    }

    @Override
    public boolean mayHaveNulls() {
        return false;
    }

    @Override
    public boolean areAllValuesNull() {
        return false;
    }

    @Override
    public boolean mayHaveMultivaluedFields() {
        return false;
    }

    @Override
    public boolean doesHaveMultivaluedFields() {
        return false;
    }

    @Override
    public Block filter(boolean mayContainDuplicates, int[] positions, int offset, int length) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Block keepMask(BooleanVector mask) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ReleasableIterator<? extends Block> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        throw new UnsupportedOperationException();
    }

    @Override
    public MvOrdering mvOrdering() {
        return MvOrdering.UNORDERED;
    }

    @Override
    public Block slice(int beginInclusive, int endExclusive) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Block expand() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Block deepCopy(BlockFactory blockFactory) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long ramBytesUsed() {
        // TODO: expose ramBytesUsed from keys and aggs then return the total here
        return 1;
    }

    @Override
    public void incRef() {
        refs.incRef();
    }

    @Override
    public boolean tryIncRef() {
        return refs.tryIncRef();
    }

    @Override
    public boolean decRef() {
        return refs.decRef();
    }

    @Override
    public boolean hasReferences() {
        return refs.hasReferences();
    }

    @Override
    public void close() {
        refs.decRef();
    }
}
