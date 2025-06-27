/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.mapper.BlockLoader;

import java.util.List;
import java.util.stream.Stream;

import static org.elasticsearch.compute.data.ElementType.LONG;

/**
 * A source operator whose output is the given tuple values. This operator produces pages
 * with two Blocks. The returned pages preserve the order of values as given in the in initial list.
 */
public class TupleLongLongBlockSourceOperator extends TupleAbstractBlockSourceOperator<Long, Long> {

    public TupleLongLongBlockSourceOperator(BlockFactory blockFactory, Stream<Tuple<Long, Long>> values) {
        super(blockFactory, values.toList(), LONG, LONG);
    }

    public TupleLongLongBlockSourceOperator(BlockFactory blockFactory, Stream<Tuple<Long, Long>> values, int maxPagePositions) {
        super(blockFactory, values.toList(), maxPagePositions, LONG, LONG);
    }

    public TupleLongLongBlockSourceOperator(BlockFactory blockFactory, List<Tuple<Long, Long>> values) {
        super(blockFactory, values, LONG, LONG);
    }

    public TupleLongLongBlockSourceOperator(BlockFactory blockFactory, List<Tuple<Long, Long>> values, int maxPagePositions) {
        super(blockFactory, values, maxPagePositions, LONG, LONG);
    }

    @Override
    protected void consumeFirstElement(Long l, Block.Builder blockBuilder) {
        ((BlockLoader.LongBuilder) blockBuilder).appendLong(l);
    }

    @Override
    protected void consumeSecondElement(Long l, Block.Builder blockBuilder) {
        consumeFirstElement(l, blockBuilder);
    }
}
