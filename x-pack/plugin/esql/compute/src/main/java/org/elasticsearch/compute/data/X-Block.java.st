/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

$if(BytesRef)$
import org.apache.lucene.util.BytesRef;
$endif$

/**
 * Block that stores $type$ values.
 * This class is generated. Do not edit it.
 */
public sealed interface $Type$Block extends Block permits Filter$Type$Block,$Type$ArrayBlock,$Type$VectorBlock {

    /**
     * Retrieves the $type$ value stored at the given value index.
     *
     * <p> Values for a given position are between getFirstValueIndex(position) (inclusive) and
     * getFirstValueIndex(position) + getValueCount(position) (exclusive).
     *
     * @param valueIndex the value index
$if(BytesRef)$
     * @param dest the destination
$endif$
     * @return the data value (as a $type$)
     */
$if(BytesRef)$
    BytesRef getBytesRef(int valueIndex, BytesRef dest);

$else$
    $type$ get$Type$(int valueIndex);
$endif$

    @Override
    $Type$Vector asVector();

    @Override
    $Type$Block getRow(int position);

    @Override
    $Type$Block filter(int... positions);

$if(int)$
    LongBlock asLongBlock();
$endif$

    static Builder newBlockBuilder(int estimatedSize) {
        return new $Type$BlockBuilder(estimatedSize);
    }

    static $Type$Block newConstantBlockWith($type$ value, int positions) {
        return new Constant$Type$Vector(value, positions).asBlock();
    }

    sealed interface Builder extends Block.Builder permits $Type$BlockBuilder {

        /**
         * Appends a $type$ to the current entry.
         */
        Builder append$Type$($type$ value);

        @Override
        Builder appendNull();

        @Override
        Builder beginPositionEntry();

        @Override
        Builder endPositionEntry();

        @Override
        $Type$Block build();
    }
}
