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
    public $Type$Block build() {
        if (positionEntryIsOpen) {
            endPositionEntry();
        }
        if (hasNonNullValue && positionCount == 1 && valueCount == 1) {
$if(BytesRef)$
            return new ConstantBytesRefVector(values.get(0, new BytesRef()), 1).asBlock();
$else$
            return new Constant$Type$Vector(values[0], 1).asBlock();
$endif$
        } else {
            // TODO: may wanna trim the array, if there N% unused tail space
            if (isDense() && singleValued()) {
                return new $Type$ArrayVector(values, positionCount).asBlock();
            } else {
                return new $Type$ArrayBlock(values, positionCount, firstValueIndexes, nullsMask);
            }
        }
    }
}
