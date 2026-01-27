/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

// begin generated imports
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;
// end generated imports

/**
 * Block build of BytesRefBlocks.
 * This class is generated. Edit {@code X-BlockBuilder.java.st} instead.
 */
final class BytesRefBlockBuilder extends AbstractBlockBuilder implements BytesRefBlock.Builder {

    private BytesRefArray values;

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
     * <p>
     *     For single-position copies see {@link #copyFrom(BytesRefBlock, int, BytesRef scratch)}.
     * </p>
     */
    @Override
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
            copyFrom(block, p, scratch);
        }
    }

    private void copyFromVector(BytesRefVector vector, int beginInclusive, int endExclusive) {
        BytesRef scratch = new BytesRef();
        for (int p = beginInclusive; p < endExclusive; p++) {
            appendBytesRef(vector.getBytesRef(p, scratch));
        }
    }

    /**
     * Copy the values in {@code block} at {@code position}. If this position
     * has a single value, this'll copy a single value. If this positions has
     * many values, it'll copy all of them. If this is {@code null}, then it'll
     * copy the {@code null}.
     * @param scratch Scratch string used to prevent allocation. Share this
                      between many calls to this function.
     * <p>
     *     Note that there isn't a version of this method on {@link Block.Builder} that takes
     *     {@link Block}. That'd be quite slow, running position by position. And it's important
     *     to know if you are copying {@link BytesRef}s so you can have the scratch.
     * </p>
     */
    @Override
    public BytesRefBlockBuilder copyFrom(BytesRefBlock block, int position, BytesRef scratch) {
        if (block.isNull(position)) {
            appendNull();
            return this;
        }
        int count = block.getValueCount(position);
        int i = block.getFirstValueIndex(position);
        if (count == 1) {
            appendBytesRef(block.getBytesRef(i++, scratch));
            return this;
        }
        beginPositionEntry();
        for (int v = 0; v < count; v++) {
            appendBytesRef(block.getBytesRef(i++, scratch));
        }
        endPositionEntry();
        return this;
    }

    @Override
    public BytesRefBlockBuilder mvOrdering(Block.MvOrdering mvOrdering) {
        this.mvOrdering = mvOrdering;
        return this;
    }

    @Override
    public long estimatedBytes() {
        return super.estimatedBytes() + BytesRefArrayBlock.BASE_RAM_BYTES_USED + values.ramBytesUsed();
    }

    private BytesRefBlock buildFromBytesArray() {
        assert estimatedBytes == 0 || firstValueIndexes != null;
        final BytesRefBlock theBlock;
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
            blockFactory.adjustBreaker(theBlock.ramBytesUsed() - estimatedBytes);
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
            blockFactory.adjustBreaker(theBlock.ramBytesUsed() - estimatedBytes - values.bigArraysRamBytesUsed());
        }
        return theBlock;
    }

    @Override
    public BytesRefBlock build() {
        try {
            finish();
            BytesRefBlock theBlock;
            theBlock = buildFromBytesArray();
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
