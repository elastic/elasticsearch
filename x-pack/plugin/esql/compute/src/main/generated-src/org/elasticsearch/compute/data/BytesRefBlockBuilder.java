/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.core.Releasables;

/**
 * Block build of BytesRefBlocks.
 * This class is generated. Do not edit it.
 */
final class BytesRefBlockBuilder extends AbstractBlockBuilder implements BytesRefBlock.Builder {

    private BytesRefArray values;

    BytesRefBlockBuilder(int estimatedSize, BlockFactory blockFactory) {
        this(estimatedSize, BigArrays.NON_RECYCLING_INSTANCE, blockFactory);
    }

    BytesRefBlockBuilder(int estimatedSize, BigArrays bigArrays, BlockFactory blockFactory) {
        super(blockFactory);
        values = new BytesRefArray(Math.max(estimatedSize, 2), bigArrays);
    }

    @Override
    public BytesRefBlockBuilder appendBytesRef(BytesRef value) {
        ensureCapacity();
        values.append(value);
        hasNonNullValue = true;
        valueCount++;
        updatePosition();
        return this;
    }

    @Override
    protected int elementSize() {
        return -1;
    }

    @Override
    protected int valuesLength() {
        return Integer.MAX_VALUE; // allow the BytesRefArray through its own append
    }

    @Override
    protected void growValuesArray(int newSize) {
        throw new AssertionError("should not reach here");
    }

    @Override
    public BytesRefBlockBuilder appendNull() {
        super.appendNull();
        return this;
    }

    @Override
    public BytesRefBlockBuilder beginPositionEntry() {
        super.beginPositionEntry();
        return this;
    }

    @Override
    public BytesRefBlockBuilder endPositionEntry() {
        super.endPositionEntry();
        return this;
    }

    @Override
    protected void writeNullValue() {
        values.append(BytesRefBlock.NULL_VALUE);
    }

    /**
     * Appends the all values of the given block into a the current position
     * in this builder.
     */
    @Override
    public BytesRefBlockBuilder appendAllValuesToCurrentPosition(Block block) {
        if (block.areAllValuesNull()) {
            return appendNull();
        }
        return appendAllValuesToCurrentPosition((BytesRefBlock) block);
    }

    /**
     * Appends the all values of the given block into a the current position
     * in this builder.
     */
    @Override
    public BytesRefBlockBuilder appendAllValuesToCurrentPosition(BytesRefBlock block) {
        final int positionCount = block.getPositionCount();
        if (positionCount == 0) {
            return appendNull();
        }
        final int totalValueCount = block.getTotalValueCount();
        if (totalValueCount == 0) {
            return appendNull();
        }
        if (totalValueCount > 1) {
            beginPositionEntry();
        }
        BytesRef scratch = new BytesRef();
        final BytesRefVector vector = block.asVector();
        if (vector != null) {
            for (int p = 0; p < positionCount; p++) {
                appendBytesRef(vector.getBytesRef(p, scratch));
            }
        } else {
            for (int p = 0; p < positionCount; p++) {
                int count = block.getValueCount(p);
                int i = block.getFirstValueIndex(p);
                for (int v = 0; v < count; v++) {
                    appendBytesRef(block.getBytesRef(i++, scratch));
                }
            }
        }
        if (totalValueCount > 1) {
            endPositionEntry();
        }
        return this;
    }

    @Override
    public BytesRefBlockBuilder copyFrom(Block block, int beginInclusive, int endExclusive) {
        if (block.areAllValuesNull()) {
            for (int p = beginInclusive; p < endExclusive; p++) {
                appendNull();
            }
            return this;
        }
        return copyFrom((BytesRefBlock) block, beginInclusive, endExclusive);
    }

    /**
     * Copy the values in {@code block} from {@code beginInclusive} to
     * {@code endExclusive} into this builder.
     */
    public BytesRefBlockBuilder copyFrom(BytesRefBlock block, int beginInclusive, int endExclusive) {
        if (endExclusive > block.getPositionCount()) {
            throw new IllegalArgumentException("can't copy past the end [" + endExclusive + " > " + block.getPositionCount() + "]");
        }
        BytesRefVector vector = block.asVector();
        if (vector != null) {
            copyFromVector(vector, beginInclusive, endExclusive);
        } else {
            copyFromBlock(block, beginInclusive, endExclusive);
        }
        return this;
    }

    private void copyFromBlock(BytesRefBlock block, int beginInclusive, int endExclusive) {
        BytesRef scratch = new BytesRef();
        for (int p = beginInclusive; p < endExclusive; p++) {
            if (block.isNull(p)) {
                appendNull();
                continue;
            }
            int count = block.getValueCount(p);
            if (count > 1) {
                beginPositionEntry();
            }
            int i = block.getFirstValueIndex(p);
            for (int v = 0; v < count; v++) {
                appendBytesRef(block.getBytesRef(i++, scratch));
            }
            if (count > 1) {
                endPositionEntry();
            }
        }
    }

    private void copyFromVector(BytesRefVector vector, int beginInclusive, int endExclusive) {
        BytesRef scratch = new BytesRef();
        for (int p = beginInclusive; p < endExclusive; p++) {
            appendBytesRef(vector.getBytesRef(p, scratch));
        }
    }

    @Override
    public BytesRefBlockBuilder mvOrdering(Block.MvOrdering mvOrdering) {
        this.mvOrdering = mvOrdering;
        return this;
    }

    @Override
    public BytesRefBlock build() {
        try {
            finish();
            BytesRefBlock theBlock;
            assert estimatedBytes == 0 || firstValueIndexes != null;
            if (hasNonNullValue && positionCount == 1 && valueCount == 1) {
                theBlock = new ConstantBytesRefVector(BytesRef.deepCopyOf(values.get(0, new BytesRef())), 1, blockFactory).asBlock();
                /*
                 * Update the breaker with the actual bytes used.
                 * We pass false below even though we've used the bytes. That's weird,
                 * but if we break here we will throw away the used memory, letting
                 * it be deallocated. The exception will bubble up and the builder will
                 * still technically be open, meaning the calling code should close it
                 * which will return all used memory to the breaker.
                 */
                blockFactory.adjustBreaker(theBlock.ramBytesUsed() - estimatedBytes, false);
                Releasables.closeExpectNoException(values);
            } else {
                if (isDense() && singleValued()) {
                    theBlock = new BytesRefArrayVector(values, positionCount, blockFactory).asBlock();
                } else {
                    theBlock = new BytesRefArrayBlock(values, positionCount, firstValueIndexes, nullsMask, mvOrdering, blockFactory);
                }
                /*
                 * Update the breaker with the actual bytes used.
                 * We pass false below even though we've used the bytes. That's weird,
                 * but if we break here we will throw away the used memory, letting
                 * it be deallocated. The exception will bubble up and the builder will
                 * still technically be open, meaning the calling code should close it
                 * which will return all used memory to the breaker.
                 */
                blockFactory.adjustBreaker(theBlock.ramBytesUsed() - estimatedBytes - values.bigArraysRamBytesUsed(), false);
            }
            values = null;
            built();
            return theBlock;
        } catch (CircuitBreakingException e) {
            close();
            throw e;
        }
    }

    @Override
    public void extraClose() {
        Releasables.closeExpectNoException(values);
    }
}
