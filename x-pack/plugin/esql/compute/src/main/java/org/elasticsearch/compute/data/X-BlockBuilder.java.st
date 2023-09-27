/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

$if(BytesRef)$
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.core.Releasables;

$else$
import org.apache.lucene.util.RamUsageEstimator;

import java.util.Arrays;
$endif$

/**
 * Block build of $Type$Blocks.
 * This class is generated. Do not edit it.
 */
final class $Type$BlockBuilder extends AbstractBlockBuilder implements $Type$Block.Builder {

$if(BytesRef)$
    private BytesRefArray values;

    BytesRefBlockBuilder(int estimatedSize, BlockFactory blockFactory) {
        this(estimatedSize, BigArrays.NON_RECYCLING_INSTANCE, blockFactory);
    }

    BytesRefBlockBuilder(int estimatedSize, BigArrays bigArrays, BlockFactory blockFactory) {
        super(blockFactory);
        values = new BytesRefArray(Math.max(estimatedSize, 2), bigArrays);
    }

$else$
    private $type$[] values;

    $Type$BlockBuilder(int estimatedSize, BlockFactory blockFactory) {
        super(blockFactory);
        int initialSize = Math.max(estimatedSize, 2);
        adjustBreaker(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + initialSize * elementSize());
        values = new $type$[initialSize];
    }
$endif$

    @Override
    public $Type$BlockBuilder append$Type$($type$ value) {
        ensureCapacity();
$if(BytesRef)$
        values.append(value);
$else$
        values[valueCount] = value;
$endif$
        hasNonNullValue = true;
        valueCount++;
        updatePosition();
        return this;
    }

    @Override
    protected int elementSize() {
        return $if(BytesRef)$-1$else$$BYTES$$endif$;
    }

    @Override
    protected int valuesLength() {
$if(BytesRef)$
        return Integer.MAX_VALUE; // allow the BytesRefArray through its own append
$else$
        return values.length;
$endif$
    }

    @Override
    protected void growValuesArray(int newSize) {
$if(BytesRef)$
        throw new AssertionError("should not reach here");
$else$
        values = Arrays.copyOf(values, newSize);
$endif$
    }

    @Override
    public $Type$BlockBuilder appendNull() {
        super.appendNull();
        return this;
    }

    @Override
    public $Type$BlockBuilder beginPositionEntry() {
        super.beginPositionEntry();
        return this;
    }

    @Override
    public $Type$BlockBuilder endPositionEntry() {
        super.endPositionEntry();
        return this;
    }

$if(BytesRef)$
    @Override
    protected void writeNullValue() {
        values.append(BytesRefBlock.NULL_VALUE);
    }
$endif$

    /**
     * Appends the all values of the given block into a the current position
     * in this builder.
     */
    @Override
    public $Type$BlockBuilder appendAllValuesToCurrentPosition(Block block) {
        if (block.areAllValuesNull()) {
            return appendNull();
        }
        return appendAllValuesToCurrentPosition(($Type$Block) block);
    }

    /**
     * Appends the all values of the given block into a the current position
     * in this builder.
     */
    @Override
    public $Type$BlockBuilder appendAllValuesToCurrentPosition($Type$Block block) {
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
$if(BytesRef)$
        BytesRef scratch = new BytesRef();
$endif$
        final $Type$Vector vector = block.asVector();
        if (vector != null) {
            for (int p = 0; p < positionCount; p++) {
$if(BytesRef)$
                appendBytesRef(vector.getBytesRef(p, scratch));
$else$
                append$Type$(vector.get$Type$(p));
$endif$
            }
        } else {
            for (int p = 0; p < positionCount; p++) {
                int count = block.getValueCount(p);
                int i = block.getFirstValueIndex(p);
                for (int v = 0; v < count; v++) {
$if(BytesRef)$
                    appendBytesRef(block.getBytesRef(i++, scratch));
$else$
                    append$Type$(block.get$Type$(i++));
$endif$
                }
            }
        }
        if (totalValueCount > 1) {
            endPositionEntry();
        }
        return this;
    }

    @Override
    public $Type$BlockBuilder copyFrom(Block block, int beginInclusive, int endExclusive) {
        if (block.areAllValuesNull()) {
            for (int p = beginInclusive; p < endExclusive; p++) {
                appendNull();
            }
            return this;
        }
        return copyFrom(($Type$Block) block, beginInclusive, endExclusive);
    }

    /**
     * Copy the values in {@code block} from {@code beginInclusive} to
     * {@code endExclusive} into this builder.
     */
    public $Type$BlockBuilder copyFrom($Type$Block block, int beginInclusive, int endExclusive) {
        if (endExclusive > block.getPositionCount()) {
            throw new IllegalArgumentException("can't copy past the end [" + endExclusive + " > " + block.getPositionCount() + "]");
        }
        $Type$Vector vector = block.asVector();
        if (vector != null) {
            copyFromVector(vector, beginInclusive, endExclusive);
        } else {
            copyFromBlock(block, beginInclusive, endExclusive);
        }
        return this;
    }

    private void copyFromBlock($Type$Block block, int beginInclusive, int endExclusive) {
$if(BytesRef)$
        BytesRef scratch = new BytesRef();
$endif$
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
$if(BytesRef)$
                appendBytesRef(block.getBytesRef(i++, scratch));
$else$
                append$Type$(block.get$Type$(i++));
$endif$
            }
            if (count > 1) {
                endPositionEntry();
            }
        }
    }

    private void copyFromVector($Type$Vector vector, int beginInclusive, int endExclusive) {
$if(BytesRef)$
        BytesRef scratch = new BytesRef();
$endif$
        for (int p = beginInclusive; p < endExclusive; p++) {
$if(BytesRef)$
            appendBytesRef(vector.getBytesRef(p, scratch));
$else$
            append$Type$(vector.get$Type$(p));
$endif$
        }
    }

    @Override
    public $Type$BlockBuilder mvOrdering(Block.MvOrdering mvOrdering) {
        this.mvOrdering = mvOrdering;
        return this;
    }

    @Override
    public $Type$Block build() {
        finish();
        $Type$Block block;
$if(BytesRef)$
        assert estimatedBytes == 0 || firstValueIndexes != null;
        if (hasNonNullValue && positionCount == 1 && valueCount == 1) {
            block = new ConstantBytesRefVector(BytesRef.deepCopyOf(values.get(0, new BytesRef())), 1, blockFactory).asBlock();
            /*
             * Update the breaker with the actual bytes used.
             * We pass false below even though we've used the bytes. That's weird,
             * but if we break here we will throw away the used memory, letting
             * it be deallocated. The exception will bubble up and the builder will
             * still technically be open, meaning the calling code should close it
             * which will return all used memory to the breaker.
             */
            blockFactory.adjustBreaker(block.ramBytesUsed() - estimatedBytes, false);
            Releasables.closeExpectNoException(values);
        } else {
            if (isDense() && singleValued()) {
                block = new $Type$ArrayVector(values, positionCount, blockFactory).asBlock();
            } else {
                block = new $Type$ArrayBlock(values, positionCount, firstValueIndexes, nullsMask, mvOrdering, blockFactory);
            }
            /*
             * Update the breaker with the actual bytes used.
             * We pass false below even though we've used the bytes. That's weird,
             * but if we break here we will throw away the used memory, letting
             * it be deallocated. The exception will bubble up and the builder will
             * still technically be open, meaning the calling code should close it
             * which will return all used memory to the breaker.
             */
            blockFactory.adjustBreaker(block.ramBytesUsed() - estimatedBytes - values.bigArraysRamBytesUsed(), false);
        }
        values = null;
$else$
        if (hasNonNullValue && positionCount == 1 && valueCount == 1) {
            block = new Constant$Type$Vector(values[0], 1, blockFactory).asBlock();
        } else {
            if (values.length - valueCount > 1024 || valueCount < (values.length / 2)) {
                values = Arrays.copyOf(values, valueCount);
            }
            if (isDense() && singleValued()) {
                block = new $Type$ArrayVector(values, positionCount, blockFactory).asBlock();
            } else {
                block = new $Type$ArrayBlock(values, positionCount, firstValueIndexes, nullsMask, mvOrdering, blockFactory);
            }
        }
        /*
         * Update the breaker with the actual bytes used.
         * We pass false below even though we've used the bytes. That's weird,
         * but if we break here we will throw away the used memory, letting
         * it be deallocated. The exception will bubble up and the builder will
         * still technically be open, meaning the calling code should close it
         * which will return all used memory to the breaker.
         */
        blockFactory.adjustBreaker(block.ramBytesUsed() - estimatedBytes, false);
$endif$
        built();
        return block;
    }
$if(BytesRef)$

    @Override
    public void extraClose() {
        Releasables.closeExpectNoException(values);
    }
$endif$
}
