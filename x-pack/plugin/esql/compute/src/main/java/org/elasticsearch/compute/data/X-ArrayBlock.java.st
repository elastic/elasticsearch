/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

$if(BytesRef)$
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BytesRefArray;

$else$
import java.util.Arrays;
$endif$
import java.util.BitSet;
import java.util.stream.IntStream;

/**
 * Block implementation that stores an array of $type$.
 * This class is generated. Do not edit it.
 */
public final class $Type$ArrayBlock extends AbstractArrayBlock implements $Type$Block {

$if(BytesRef)$
    private final BytesRefArray values;

$else$
    private final $type$[] values;
$endif$

$if(BytesRef)$
    public $Type$ArrayBlock(BytesRefArray values, int positionCount, int[] firstValueIndexes, BitSet nulls, MvOrdering mvOrdering) {
$else$
    public $Type$ArrayBlock($type$[] values, int positionCount, int[] firstValueIndexes, BitSet nulls, MvOrdering mvOrdering) {
$endif$
        super(positionCount, firstValueIndexes, nulls, mvOrdering);
        this.values = values;
    }

    @Override
    public $Type$Vector asVector() {
        return null;
    }

    @Override
$if(BytesRef)$
    public BytesRef getBytesRef(int valueIndex, BytesRef dest) {
        return values.get(valueIndex, dest);
$else$
    public $type$ get$Type$(int valueIndex) {
        return values[valueIndex];
$endif$
    }

    @Override
    public $Type$Block filter(int... positions) {
        return new Filter$Type$Block(this, positions);
    }

    @Override
    public ElementType elementType() {
        return ElementType.$TYPE$;
    }

    @Override
    public $Type$Block expand() {
        if (firstValueIndexes == null) {
            return this;
        }
        int end = firstValueIndexes[getPositionCount()];
        if (nullsMask == null) {
            return new $Type$ArrayVector(values, end).asBlock();
        }
        int[] firstValues = IntStream.range(0, end + 1).toArray();
        return new $Type$ArrayBlock(values, end, firstValues, shiftNullsToExpandedPositions(), MvOrdering.UNORDERED);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof $Type$Block that) {
            return $Type$Block.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return $Type$Block.hash(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName()
            + "[positions="
            + getPositionCount()
            + ", mvOrdering="
            + mvOrdering()
$if(BytesRef)$
            + ", values="
            + values.size()
$else$
            + ", values="
            + Arrays.toString(values)
$endif$
            + ']';
    }
}
