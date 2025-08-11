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
import org.elasticsearch.common.util.$Array$;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;
// end generated imports

/**
 * Block build of $Type$Blocks.
 * This class is generated. Edit {@code X-BlockBuilder.java.st} instead.
 */
final class $Type$BlockBuilder extends AbstractBlockBuilder implements $Type$Block.Builder {

$if(BytesRef)$
    private BytesRefArray values;

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
     * <p>
     *     For single-position copies see {@link #copyFrom($Type$Block, int$if(BytesRef)$, BytesRef scratch$endif$)}.
     * </p>
     */
    @Override
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
            copyFrom(block, p$if(BytesRef)$, scratch$endif$);
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

    /**
     * Copy the values in {@code block} at {@code position}. If this position
     * has a single value, this'll copy a single value. If this positions has
     * many values, it'll copy all of them. If this is {@code null}, then it'll
     * copy the {@code null}.
$if(BytesRef)$
     * @param scratch Scratch string used to prevent allocation. Share this
                      between many calls to this function.
$endif$
     * <p>
     *     Note that there isn't a version of this method on {@link Block.Builder} that takes
     *     {@link Block}. That'd be quite slow, running position by position. And it's important
     *     to know if you are copying {@link BytesRef}s so you can have the scratch.
     * </p>
     */
    @Override
    public $Type$BlockBuilder copyFrom($Type$Block block, int position$if(BytesRef)$, BytesRef scratch$endif$) {
        if (block.isNull(position)) {
            appendNull();
            return this;
        }
        int count = block.getValueCount(position);
        int i = block.getFirstValueIndex(position);
        if (count == 1) {
            append$Type$(block.get$Type$(i++$if(BytesRef)$, scratch$endif$));
            return this;
        }
        beginPositionEntry();
        for (int v = 0; v < count; v++) {
            append$Type$(block.get$Type$(i++$if(BytesRef)$, scratch$endif$));
        }
        endPositionEntry();
        return this;
    }

    @Override
    public $Type$BlockBuilder mvOrdering(Block.MvOrdering mvOrdering) {
        this.mvOrdering = mvOrdering;
        return this;
    }

$if(BytesRef)$
    @Override
    public long estimatedBytes() {
        return super.estimatedBytes() + BytesRefArrayBlock.BASE_RAM_BYTES_USED + values.ramBytesUsed();
    }

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
            blockFactory.adjustBreaker(theBlock.ramBytesUsed() - estimatedBytes);
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
            blockFactory.adjustBreaker(theBlock.ramBytesUsed() - estimatedBytes - values.bigArraysRamBytesUsed());
        }
        return theBlock;
    }

$else$
    private $Type$Block buildBigArraysBlock() {
        final $Type$Block theBlock;
    $if(boolean)$
        final BitArray array = new BitArray(valueCount, blockFactory.bigArrays());
        for (int i = 0; i < valueCount; i++) {
            if (values[i]) {
                array.set(i);
            }
        }
    $else$
        final $Array$ array = blockFactory.bigArrays().new$Array$(valueCount, false);
        for (int i = 0; i < valueCount; i++) {
            array.set(i, values[i]);
        }
    $endif$
        if (isDense() && singleValued()) {
            theBlock = new $Type$BigArrayVector(array, positionCount, blockFactory).asBlock();
        } else {
            theBlock = new $Type$BigArrayBlock(array, positionCount, firstValueIndexes, nullsMask, mvOrdering, blockFactory);
        }
        /*
        * Update the breaker with the actual bytes used.
        * We pass false below even though we've used the bytes. That's weird,
        * but if we break here we will throw away the used memory, letting
        * it be deallocated. The exception will bubble up and the builder will
        * still technically be open, meaning the calling code should close it
        * which will return all used memory to the breaker.
        */
        blockFactory.adjustBreaker(theBlock.ramBytesUsed() - estimatedBytes - array.ramBytesUsed());
        return theBlock;
    }
$endif$

    @Override
    public $Type$Block build() {
        try {
            finish();
            $Type$Block theBlock;
    $if(BytesRef)$
            theBlock = buildFromBytesArray();
            values = null;
    $else$
            if (hasNonNullValue && positionCount == 1 && valueCount == 1) {
                theBlock = blockFactory.newConstant$Type$BlockWith(values[0], 1, estimatedBytes);
            } else if (estimatedBytes > blockFactory.maxPrimitiveArrayBytes()) {
                theBlock = buildBigArraysBlock();
            } else if (isDense() && singleValued()) {
                theBlock = blockFactory.new$Type$ArrayVector(values, positionCount, estimatedBytes).asBlock();
            } else {
                theBlock = blockFactory.new$Type$ArrayBlock(
                    values, // stylecheck
                    positionCount,
                    firstValueIndexes,
                    nullsMask,
                    mvOrdering,
                    estimatedBytes
                );
            }
    $endif$
            built();
            return theBlock;
        } catch (CircuitBreakingException e) {
            close();
            throw e;
        }
    }
$if(BytesRef)$

    @Override
    public void extraClose() {
        Releasables.closeExpectNoException(values);
    }
$endif$
}
