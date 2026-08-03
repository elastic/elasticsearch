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
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;
// end generated imports

/**
 * Block build of BooleanBlocks.
 * This class is generated. Edit {@code X-BlockBuilder.java.st} instead.
 */
final class BooleanBlockBuilder extends AbstractBlockBuilder implements BooleanBlock.Builder {

    private boolean[] values;

    BooleanBlockBuilder(int estimatedSize, BlockFactory blockFactory) {
        super(blockFactory);
        int initialSize = Math.max(estimatedSize, 2);
        adjustBreaker(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) initialSize * elementSize());
        values = new boolean[initialSize];
    }

    @Override
    public BooleanBlockBuilder appendBoolean(boolean value) {
        ensureCapacity();
        values[valueCount] = value;
        hasNonNullValue = true;
        valueCount++;
        updatePosition();
        return this;
    }

    @Override
    protected int elementSize() {
        return Byte.BYTES;
    }

    @Override
    protected int valuesLength() {
        return values.length;
    }

    @Override
    protected void growValuesArray(int newSize) {
        values = Arrays.copyOf(values, newSize);
    }

    @Override
    public BooleanBlockBuilder appendNull() {
        super.appendNull();
        return this;
    }

    @Override
    public BooleanBlockBuilder beginPositionEntry() {
        super.beginPositionEntry();
        return this;
    }

    @Override
    public BooleanBlockBuilder endPositionEntry() {
        super.endPositionEntry();
        return this;
    }

    @Override
    public BooleanBlockBuilder copyFrom(Block block, int beginInclusive, int endExclusive) {
        if (block.areAllValuesNull()) {
            for (int p = beginInclusive; p < endExclusive; p++) {
                appendNull();
            }
            return this;
        }
        return copyFrom((BooleanBlock) block, beginInclusive, endExclusive);
    }

    /**
     * Copy the values in {@code block} from {@code beginInclusive} to
     * {@code endExclusive} into this builder.
     * <p>
     *     For single-position copies see {@link #copyFrom(BooleanBlock, int)}.
     * </p>
     */
    @Override
    public BooleanBlockBuilder copyFrom(BooleanBlock block, int beginInclusive, int endExclusive) {
        if (endExclusive > block.getPositionCount()) {
            throw new IllegalArgumentException("can't copy past the end [" + endExclusive + " > " + block.getPositionCount() + "]");
        }
        BooleanVector vector = block.asVector();
        if (vector != null) {
            copyFromVector(vector, beginInclusive, endExclusive);
        } else {
            copyFromBlock(block, beginInclusive, endExclusive);
        }
        return this;
    }

    private void copyFromBlock(BooleanBlock block, int beginInclusive, int endExclusive) {
        for (int p = beginInclusive; p < endExclusive; p++) {
            copyFrom(block, p);
        }
    }

    private void copyFromVector(BooleanVector vector, int beginInclusive, int endExclusive) {
        int count = endExclusive - beginInclusive;
        if (count == 0) {
            return;
        }
        ensureCapacity(count);
        vector.copyTo(beginInclusive, values, valueCount, count);
        hasNonNullValue = true;
        valueCount += count;
        updatePositions(count);
    }

    /**
     * Copy the values in {@code block} at {@code position}. If this position
     * has a single value, this'll copy a single value. If this positions has
     * many values, it'll copy all of them. If this is {@code null}, then it'll
     * copy the {@code null}.
     * <p>
     *     Note that there isn't a version of this method on {@link Block.Builder} that takes
     *     {@link Block}. That'd be quite slow, running position by position. And it's important
     *     to know if you are copying {@link BytesRef}s so you can have the scratch.
     * </p>
     */
    @Override
    public BooleanBlockBuilder copyFrom(BooleanBlock block, int position) {
        if (block.isNull(position)) {
            appendNull();
            return this;
        }
        int count = block.getValueCount(position);
        int i = block.getFirstValueIndex(position);
        if (count == 1) {
            appendBoolean(block.getBoolean(i++));
            return this;
        }
        beginPositionEntry();
        for (int v = 0; v < count; v++) {
            appendBoolean(block.getBoolean(i++));
        }
        endPositionEntry();
        return this;
    }

    @Override
    public BooleanBlockBuilder mvOrdering(Block.MvOrdering mvOrdering) {
        this.mvOrdering = mvOrdering;
        return this;
    }

    private BooleanBlock buildBigArraysBlock() {
        /*
         * If adjustBreaker throws after the BigArray is wrapped, release the incomplete
         * block/vector here. BigArrayBlock.closeInternal debits block overhead, so that
         * overhead is credited before wrapping when needed so failure cleanup stays balanced.
         */
        BitArray array = new BitArray(valueCount, blockFactory.bigArrays());
        try {
            for (int i = 0; i < valueCount; i++) {
                if (values[i]) {
                    array.set(i);
                }
            }
            final long arrayBytes = array.ramBytesUsed();
            final int vectorPositions = firstValueIndexes == null ? positionCount : firstValueIndexes[positionCount];
            BooleanBigArrayVector vector = new BooleanBigArrayVector(array, vectorPositions, blockFactory);
            array = null; // ownership transferred to vector
            try {
                final BooleanBlock theBlock;
                if (isDense() && singleValued()) {
                    theBlock = vector.asBlock();
                    vector = null; // ownership transferred to theBlock
                    /*
                     * Update the breaker with the actual bytes used.
                     * We pass false below even though we've used the bytes. That's weird,
                     * but if we break here we will throw away the used memory, letting
                     * it be deallocated. The exception will bubble up and the builder will
                     * still technically be open, meaning the calling code should close it
                     * which will return all used memory to the breaker.
                     */
                    try {
                        blockFactory.adjustBreaker(theBlock.ramBytesUsed() - estimatedBytes - arrayBytes);
                    } catch (CircuitBreakingException e) {
                        // VectorBlock.close releases the BigArray without debiting block overhead.
                        Releasables.closeExpectNoException(theBlock);
                        throw e;
                    }
                    return theBlock;
                } else {
                    final long overhead = BlockRamUsageEstimator.sizeOf(firstValueIndexes) + BlockRamUsageEstimator.sizeOfBitSet(
                        nullsMask
                    );
                    blockFactory.adjustBreaker(overhead);
                    theBlock = new BooleanBigArrayBlock(vector, positionCount, firstValueIndexes, nullsMask, mvOrdering);
                    vector = null; // ownership transferred to theBlock
                    try {
                        blockFactory.adjustBreaker(theBlock.ramBytesUsed() - estimatedBytes - arrayBytes - overhead);
                    } catch (CircuitBreakingException e) {
                        // closeInternal debits {@code overhead} which we credited above.
                        Releasables.closeExpectNoException(theBlock);
                        throw e;
                    }
                    return theBlock;
                }
            } finally {
                Releasables.close(vector);
            }
        } finally {
            Releasables.close(array);
        }
    }

    @Override
    public BooleanBlock build() {
        try {
            finish();
            BooleanBlock theBlock;
            if (hasNonNullValue && positionCount == 1 && valueCount == 1) {
                theBlock = blockFactory.newConstantBooleanBlockWith(values[0], 1, estimatedBytes);
            } else if (estimatedBytes > blockFactory.maxPrimitiveArrayBytes()) {
                theBlock = buildBigArraysBlock();
            } else if (isDense() && singleValued()) {
                theBlock = blockFactory.newBooleanArrayVector(values, positionCount, estimatedBytes).asBlock();
            } else {
                theBlock = blockFactory.newBooleanArrayBlock(
                    values, // stylecheck
                    positionCount,
                    firstValueIndexes,
                    nullsMask,
                    mvOrdering,
                    estimatedBytes
                );
            }
            built();
            return theBlock;
        } catch (CircuitBreakingException e) {
            close();
            throw e;
        }
    }
}
