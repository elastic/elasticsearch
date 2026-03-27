/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.test.operator.blocksource;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Tuple;

import java.util.List;
import java.util.stream.Stream;

/**
 * A source operator whose output is the given tuple values of (Long, float[]).
 * This operator produces pages with two Blocks:
 * - A LongBlock for the group id
 * - A FloatBlock with multiple values per position representing dense vectors
 */
public class LongDenseVectorFloatTupleBlockSourceOperator extends AbstractBlockSourceOperator {

    private static final int DEFAULT_MAX_PAGE_POSITIONS = 8 * 1024;

    private final List<Tuple<Long, float[]>> values;

    public LongDenseVectorFloatTupleBlockSourceOperator(BlockFactory blockFactory, Stream<Tuple<Long, float[]>> values) {
        this(blockFactory, values, DEFAULT_MAX_PAGE_POSITIONS);
    }

    public LongDenseVectorFloatTupleBlockSourceOperator(
        BlockFactory blockFactory,
        Stream<Tuple<Long, float[]>> values,
        int maxPagePositions
    ) {
        super(blockFactory, maxPagePositions);
        this.values = values.toList();
    }

    @Override
    protected Page createPage(int positionOffset, int length) {
        var longBlockBuilder = blockFactory.newLongBlockBuilder(length);
        var floatBlockBuilder = blockFactory.newFloatBlockBuilder(length);
        for (int i = 0; i < length; i++) {
            Tuple<Long, float[]> item = values.get(positionOffset + i);
            if (item.v1() == null) {
                longBlockBuilder.appendNull();
            } else {
                longBlockBuilder.appendLong(item.v1());
            }
            if (item.v2() == null) {
                floatBlockBuilder.appendNull();
            } else {
                floatBlockBuilder.beginPositionEntry();
                for (float f : item.v2()) {
                    floatBlockBuilder.appendFloat(f);
                }
                floatBlockBuilder.endPositionEntry();
            }
        }
        currentPosition += length;
        return new Page(longBlockBuilder.build(), floatBlockBuilder.build());
    }

    @Override
    protected int remaining() {
        return values.size() - currentPosition;
    }
}
