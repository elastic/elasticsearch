/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.io.IOException;

public final class DoubleRangeArrayBlock extends AbstractBlockRefCounted implements DoubleRangeBlock {
    private final DoubleBlock fromBlock;
    private final DoubleBlock toBlock;

    public DoubleRangeArrayBlock(DoubleBlock fromBlock, DoubleBlock toBlock) {
        this.fromBlock = fromBlock;
        this.toBlock = toBlock;
    }

    @Override
    public DoubleBlock getDoubleFromBlock() {
        return fromBlock;
    }

    @Override
    public DoubleBlock getDoubleToBlock() {
        return toBlock;
    }

    @Override
    public DoubleRangeBlockBuilder.DoubleRange getDoubleRange(int valueIndex, DoubleRangeBlockBuilder.DoubleRange scratch) {
        return scratch.reset(fromBlock.getDouble(valueIndex), toBlock.getDouble(valueIndex));
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
        return fromBlock.getTotalValueCount();
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
        return fromBlock.getValueCount(position);
    }

    @Override
    public ElementType elementType() {
        return ElementType.DOUBLE_RANGE;
    }

    @Override
    public int valueMaxByteSize() {
        return Double.BYTES * 2;
    }

    @Override
    public BlockFactory blockFactory() {
        return fromBlock.blockFactory();
    }

    @Override
    public void allowPassingToDifferentDriver() {
        makeRefCountsThreadSafe();
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
    public DoubleRangeBlock slice(int beginInclusive, int endExclusive) {
        if (beginInclusive == 0 && endExclusive == getPositionCount()) {
            incRef();
            return this;
        }
        DoubleRangeBlock result = null;
        DoubleBlock newFromBlock = null;
        DoubleBlock newToBlock = null;
        try {
            newFromBlock = fromBlock.slice(beginInclusive, endExclusive);
            newToBlock = toBlock.slice(beginInclusive, endExclusive);
            result = new DoubleRangeArrayBlock(newFromBlock, newToBlock);
            return result;
        } finally {
            if (result == null) {
                Releasables.closeExpectNoException(newFromBlock, newToBlock);
            }
        }
    }

    @Override
    public DoubleRangeBlock filter(boolean mayContainDuplicates, int[] positions, int offset, int length) {
        DoubleRangeBlock result = null;
        DoubleBlock newFromBlock = null;
        DoubleBlock newToBlock = null;
        try {
            newFromBlock = fromBlock.filter(mayContainDuplicates, positions, offset, length);
            newToBlock = toBlock.filter(mayContainDuplicates, positions, offset, length);
            result = new DoubleRangeArrayBlock(newFromBlock, newToBlock);
            return result;
        } finally {
            if (result == null) {
                Releasables.close(newFromBlock, newToBlock);
            }
        }
    }

    @Override
    public DoubleRangeBlock keepMask(BooleanVector mask) {
        DoubleRangeBlock result = null;
        DoubleBlock newFromBlock = null;
        DoubleBlock newToBlock = null;
        try {
            newFromBlock = fromBlock.keepMask(mask);
            newToBlock = toBlock.keepMask(mask);
            result = new DoubleRangeArrayBlock(newFromBlock, newToBlock);
            return result;
        } finally {
            if (result == null) {
                Releasables.close(newFromBlock, newToBlock);
            }
        }
    }

    @Override
    public ReleasableIterator<? extends DoubleRangeBlock> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        throw new UnsupportedOperationException("can't lookup values from DoubleRangeBlock");
    }

    @Override
    public MvOrdering mvOrdering() {
        return fromBlock.mvOrdering();
    }

    @Override
    public DoubleRangeBlock expand() {
        if (doesHaveMultivaluedFields() == false) {
            incRef();
            return this;
        }
        DoubleRangeBlock result = null;
        DoubleBlock newFromBlock = null;
        DoubleBlock newToBlock = null;
        try {
            newFromBlock = fromBlock.expand();
            newToBlock = toBlock.expand();
            result = new DoubleRangeArrayBlock(newFromBlock, newToBlock);
            return result;
        } finally {
            if (result == null) {
                Releasables.closeExpectNoException(newFromBlock, newToBlock);
            }
        }
    }

    @Override
    public Block deepCopy(BlockFactory blockFactory) {
        DoubleRangeBlock ret = null;
        DoubleBlock newFromBlock = null;
        DoubleBlock newToBlock = null;
        try {
            newFromBlock = fromBlock.deepCopy(blockFactory);
            newToBlock = toBlock.deepCopy(blockFactory);
            ret = new DoubleRangeArrayBlock(newFromBlock, newToBlock);
            return ret;
        } finally {
            if (ret == null) {
                Releasables.close(newFromBlock, newToBlock);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Block.writeTypedBlock(fromBlock, out);
        Block.writeTypedBlock(toBlock, out);
    }

    public static Block readFrom(BlockStreamInput in) throws IOException {
        boolean success = false;
        DoubleBlock from = null;
        DoubleBlock to = null;
        try {
            from = (DoubleBlock) Block.readTypedBlock(in);
            to = (DoubleBlock) Block.readTypedBlock(in);
            var result = new DoubleRangeArrayBlock(from, to);
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
        if (obj instanceof DoubleRangeBlock that) {
            return DoubleRangeBlock.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return DoubleRangeBlock.hash(this);
    }
}
