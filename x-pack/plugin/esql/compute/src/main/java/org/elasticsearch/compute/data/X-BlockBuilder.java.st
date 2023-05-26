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

$else$
import java.util.Arrays;
$endif$

/**
 * Block build of $Type$Blocks.
 * This class is generated. Do not edit it.
 */
final class $Type$BlockBuilder extends AbstractBlockBuilder implements $Type$Block.Builder {

$if(BytesRef)$
    private static final BytesRef NULL_VALUE = new BytesRef();

    private BytesRefArray values;

    BytesRefBlockBuilder(int estimatedSize) {
        this(estimatedSize, BigArrays.NON_RECYCLING_INSTANCE);
    }

    BytesRefBlockBuilder(int estimatedSize, BigArrays bigArrays) {
        values = new BytesRefArray(Math.max(estimatedSize, 2), bigArrays);
    }

$else$
    private $type$[] values;

    $Type$BlockBuilder(int estimatedSize) {
        values = new $type$[Math.max(estimatedSize, 2)];
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
        values.append(NULL_VALUE);
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
        if (hasNonNullValue && positionCount == 1 && valueCount == 1) {
$if(BytesRef)$
            return new ConstantBytesRefVector(values.get(0, new BytesRef()), 1).asBlock();
        } else {
$else$
            return new Constant$Type$Vector(values[0], 1).asBlock();
        } else {
            if (values.length - valueCount > 1024 || valueCount < (values.length / 2)) {
                values = Arrays.copyOf(values, valueCount);
            }
$endif$
            if (isDense() && singleValued()) {
                return new $Type$ArrayVector(values, positionCount).asBlock();
            } else {
                return new $Type$ArrayBlock(values, positionCount, firstValueIndexes, nullsMask, mvOrdering);
            }
        }
    }
}
