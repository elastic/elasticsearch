/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.io.IOException;

public final class LongRangeArrayBlock extends AbstractNonThreadSafeRefCounted implements LongRangeBlock {
    private final LongBlock fromBlock;
    private final LongBlock toBlock;

    public LongRangeArrayBlock(LongBlock fromBlock, LongBlock toBlock) {
        this.fromBlock = fromBlock;
        this.toBlock = toBlock;
    }

    @Override
    public LongBlock getFromBlock() {
        return fromBlock;
    }

    @Override
    public LongBlock getToBlock() {
        return toBlock;
    }

    @Override
    protected void closeInternal() {
        Releasables.close(fromBlock, toBlock);
    }

    @Override
    public Vector asVector() {
        return null;
    }

    @Override
    public int getTotalValueCount() {
        return fromBlock.getTotalValueCount() + toBlock.getTotalValueCount();
    }

    @Override
    public int getPositionCount() {
        return fromBlock.getPositionCount();
    }

    @Override
    public int getFirstValueIndex(int position) {
        return fromBlock.getFirstValueIndex(position);
    }

    @Override
    public int getValueCount(int position) {
        return Math.max(fromBlock.getValueCount(position), toBlock.getValueCount(position));
    }

    @Override
    public ElementType elementType() {
        return ElementType.LONG_RANGE;
    }

    @Override
    public BlockFactory blockFactory() {
        return fromBlock.blockFactory();
    }

    @Override
    public void allowPassingToDifferentDriver() {
        fromBlock.allowPassingToDifferentDriver();
        toBlock.allowPassingToDifferentDriver();
    }

    @Override
    public boolean isNull(int position) {
        return fromBlock.isNull(position) || toBlock.isNull(position);
    }

    @Override
    public boolean mayHaveNulls() {
        return fromBlock.mayHaveNulls() || toBlock.mayHaveNulls();
    }

    @Override
    public boolean areAllValuesNull() {
        return fromBlock.areAllValuesNull() && toBlock.areAllValuesNull();
    }

    @Override
    public boolean mayHaveMultivaluedFields() {
        return fromBlock.mayHaveMultivaluedFields() || toBlock.mayHaveMultivaluedFields();
    }

    @Override
    public boolean doesHaveMultivaluedFields() {
        return fromBlock.doesHaveMultivaluedFields() || toBlock.doesHaveMultivaluedFields();
    }

    @Override
    public LongRangeBlock filter(boolean mayContainDuplicates, int... positions) {
        LongRangeBlock result = null;
        LongBlock newFromBlock = null;
        LongBlock newToBlock = null;
        try {
            newFromBlock = fromBlock.filter(mayContainDuplicates, positions);
            newToBlock = toBlock.filter(mayContainDuplicates, positions);
            result = new LongRangeArrayBlock(newFromBlock, newToBlock);
            return result;
        } finally {
            if (result == null) {
                Releasables.close(newFromBlock, newToBlock);
            }
        }
    }

    @Override
    public LongRangeBlock keepMask(BooleanVector mask) {
        LongRangeBlock result = null;
        LongBlock newFromBlock = null;
        LongBlock newToBlock = null;
        try {
            newFromBlock = fromBlock.keepMask(mask);
            newToBlock = toBlock.keepMask(mask);
            result = new LongRangeArrayBlock(newFromBlock, newToBlock);
            return result;
        } finally {
            if (result == null) {
                Releasables.close(newFromBlock, newToBlock);
            }
        }
    }

    @Override
    public ReleasableIterator<? extends LongRangeBlock> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        // TODO: support
        throw new UnsupportedOperationException("can't lookup values from DateRangeBlock");
    }

    @Override
    public MvOrdering mvOrdering() {
        // TODO: determine based on sub-blocks
        return MvOrdering.UNORDERED;
    }

    @Override
    public LongRangeBlock expand() {
        this.incRef();
        return this;
    }

    @Override
    public Block deepCopy(BlockFactory blockFactory) {
        LongRangeBlock ret = null;
        LongBlock newFromBlock = null;
        LongBlock newToBlock = null;
        try {
            newFromBlock = fromBlock.deepCopy(blockFactory);
            newToBlock = toBlock.deepCopy(blockFactory);
            ret = new LongRangeArrayBlock(newFromBlock, newToBlock);
            return ret;
        } finally {
            if (ret == null) {
                Releasables.close(newFromBlock, newToBlock);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        fromBlock.writeTo(out);
        toBlock.writeTo(out);
    }

    public static Block readFrom(StreamInput in) throws IOException {
        boolean success = false;
        LongBlock from = null;
        LongBlock to = null;
        BlockStreamInput blockStreamInput = (BlockStreamInput) in;
        try {
            from = LongBlock.readFrom(blockStreamInput);
            to = LongBlock.readFrom(blockStreamInput);
            var result = new LongRangeArrayBlock(from, to);
            success = true;
            return result;
        } finally {
            if (success == false) {
                Releasables.close(from, to);
            }
        }
    }

    @Override
    public long ramBytesUsed() {
        return fromBlock.ramBytesUsed() + toBlock.ramBytesUsed();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof LongRangeBlock that) {
            return LongRangeBlock.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return LongRangeBlock.hash(this);
    }
}
