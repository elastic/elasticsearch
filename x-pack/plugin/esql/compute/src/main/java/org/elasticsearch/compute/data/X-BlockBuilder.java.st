/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

$if(BytesRef)$
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.core.Releasables;

$else$
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.$if(boolean)$Bit$else$$Type$$endif$Array;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

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
    private interface $Type$Values extends Releasable {
        void setValue(int index, $type$ value);
    }

    private static class Primitive$Type$Values implements $Type$Values {
        $type$[] array;

        Primitive$Type$Values($type$[] array) {
            this.array = array;
        }

        @Override
        public void setValue(int index, $type$ value) {
            array[index] = value;
        }

        @Override
        public void close() {

        }
    }

    private static class Array$Type$Values implements $Type$Values {
    $if(boolean)$
        final BitArray array;

        Array$Type$Values(BigArrays bigArrays, long size) {
            this.array = new BitArray(size, bigArrays);
        }

        @Override
        public void setValue(int index, boolean value) {
            if (value) {
                array.set(index);
            }
        }

        void grow(int minSize) {

        }

    $else$
        final BigArrays bigArrays;
        $Type$Array array;

        Array$Type$Values(BigArrays bigArrays, long size) {
            this.bigArrays = bigArrays;
            this.array = bigArrays.new$Type$Array(size, false);
        }

        @Override
        public void setValue(int index, $type$ value) {
            array.set(index, value);
        }

        void grow(int minSize) {
            array = bigArrays.grow(array, minSize);
        }
    $endif$

        @Override
        public void close() {
            array.close();
        }

        long ramBytesUsed() {
            return array.ramBytesUsed();
        }
    }

    private $Type$Values values;

    $Type$BlockBuilder(int estimatedSize, BlockFactory blockFactory) {
        super(blockFactory);
        int initialSize = Math.max(estimatedSize, 2);
        long initialBytes = estimateBytesFor$Type$Array(initialSize);
        if (initialBytes <= blockFactory.maxPrimitiveArrayBytes()) {
            adjustBreaker(initialBytes);
            values = new Primitive$Type$Values(new $type$[initialSize]);
        } else {
            values = new Array$Type$Values(blockFactory.bigArrays(), initialSize);
        }
    }
$endif$

    @Override
    public $Type$BlockBuilder append$Type$($type$ value) {
        ensureCapacity();
$if(BytesRef)$
        values.append(value);
$else$
        values.setValue(valueCount, value);
$endif$
        hasNonNullValue = true;
        valueCount++;
        updatePosition();
        return this;
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

$if(BytesRef)$
    private $Type$Block buildFromBytesArray() {
        assert estimatedBytes == 0 || firstValueIndexes != null;
        final $Type$Block theBlock;
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
                theBlock = new $Type$ArrayVector(values, positionCount, blockFactory).asBlock();
            } else {
                theBlock = new $Type$ArrayBlock(values, positionCount, firstValueIndexes, nullsMask, mvOrdering, blockFactory);
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
        return theBlock;
    }

$else$
    private $Type$Block buildFromBigArrays(Array$Type$Values values) {
        assert estimatedBytes == 0 || firstValueIndexes != null;
        final $Type$Block theBlock;
        if (isDense() && singleValued()) {
            theBlock = new $Type$BigArrayVector(values.array, positionCount, blockFactory).asBlock();
        } else {
            theBlock = new $Type$BigArrayBlock(values.array, positionCount, firstValueIndexes, nullsMask, mvOrdering, blockFactory);
        }
        /*
        * Update the breaker with the actual bytes used.
        * We pass false below even though we've used the bytes. That's weird,
        * but if we break here we will throw away the used memory, letting
        * it be deallocated. The exception will bubble up and the builder will
        * still technically be open, meaning the calling code should close it
        * which will return all used memory to the breaker.
        */
        blockFactory.adjustBreaker(theBlock.ramBytesUsed() - estimatedBytes - values.ramBytesUsed(), false);
        return theBlock;
    }

$endif$
    @Override
    public $Type$Block build() {
        try {
            finish();
    $if(BytesRef)$
            assert estimatedBytes == 0 || firstValueIndexes != null;
            final $Type$Block theBlock = buildFromBytesArray();
    $else$
            final $Type$Block theBlock;
            if (values instanceof Array$Type$Values arrayValues) {
                theBlock = buildFromBigArrays(arrayValues);
            } else {
                $type$[] array = ((Primitive$Type$Values) values).array;
                if (hasNonNullValue && positionCount == 1 && valueCount == 1) {
                    theBlock = blockFactory.newConstant$Type$BlockWith(array[0], 1, estimatedBytes);
                } else {
                    int currentLength = array.length;
                    // TODO: should be ANDed instead?
                    if (currentLength - valueCount > 1024 || valueCount < (currentLength / 2)) {
                        adjustBreaker(estimateBytesFor$Type$Array(valueCount));
                        array = Arrays.copyOf(array, valueCount);
                        adjustBreaker(estimateBytesFor$Type$Array(currentLength));
                    }
                    if (isDense() && singleValued()) {
                        theBlock = blockFactory.new$Type$ArrayVector(array, positionCount, estimatedBytes).asBlock();
                    } else {
                        theBlock = blockFactory.new$Type$ArrayBlock(
                            array,
                            positionCount,
                            firstValueIndexes,
                            nullsMask,
                            mvOrdering,
                            estimatedBytes
                        );
                    }
                }
            }
    $endif$
            values = null;
            built();
            return theBlock;
        } catch (CircuitBreakingException e) {
            close();
            throw e;
        }
    }

    @Override
    $if(BytesRef)$
    protected void ensureCapacity() {}

    $else$
    protected void ensureCapacity() {
        if (values instanceof Array$Type$Values array) {
            array.grow(valueCount + 1);
            return;
        }
        final $type$[] array = ((Primitive$Type$Values) values).array;
        final int currentLength = array.length;
        if (valueCount < currentLength) {
            return;
        }
        final int newSize = currentLength + (currentLength >> 1);  // trivially, grows array by 50%
        final long newEstimatedBytes = estimateBytesFor$Type$Array(newSize);
        if (newEstimatedBytes <= blockFactory.maxPrimitiveArrayBytes()) {
            // extend the primitive array
            adjustBreaker(newEstimatedBytes);
            values = new Primitive$Type$Values(Arrays.copyOf(array, newSize));
            adjustBreaker(-estimateBytesFor$Type$Array(currentLength));
            return;
        } else {
            // switch to a big array
            values = new Array$Type$Values(blockFactory.bigArrays(), valueCount + 1);
            for (int i = 0; i < currentLength; i++) {
                // TODO: bulk copy
                values.setValue(i, array[i]);
            }
            adjustBreaker(-estimateBytesFor$Type$Array(currentLength));
        }
    }

    static long estimateBytesFor$Type$Array(long arraySize) {
        return RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + arraySize * (long) $BYTES$;
    }
    $endif$

    @Override
    public void extraClose() {
        Releasables.closeExpectNoException(values);
    }
}
