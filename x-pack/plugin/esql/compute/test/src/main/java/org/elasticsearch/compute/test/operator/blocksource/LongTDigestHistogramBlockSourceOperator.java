/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.test.operator.blocksource;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.TDigestBlockBuilder;
import org.elasticsearch.compute.data.TDigestHolder;
import org.elasticsearch.core.Tuple;

import java.util.List;
import java.util.stream.Stream;

/**
 * A source operator whose output is the given tuple values. This operator produces pages
 * with two Blocks. The returned pages preserve the order of values as given in the initial list.
 */
public class LongTDigestHistogramBlockSourceOperator extends TupleAbstractBlockSourceOperator<Long, TDigestHolder> {

    public LongTDigestHistogramBlockSourceOperator(BlockFactory blockFactory, Stream<Tuple<Long, TDigestHolder>> values) {
        this(blockFactory, values.toList());
    }

    public LongTDigestHistogramBlockSourceOperator(BlockFactory blockFactory, List<Tuple<Long, TDigestHolder>> values) {
        super(blockFactory, values, ElementType.LONG, ElementType.TDIGEST);
    }

    @Override
    protected void consumeFirstElement(Long longVal, Block.Builder longBlockBuilder) {
        ((LongBlock.Builder) longBlockBuilder).appendLong(longVal);

    }

    @Override
    protected void consumeSecondElement(TDigestHolder histogram, Block.Builder histogramBlockBuilder) {
        ((TDigestBlockBuilder) histogramBlockBuilder).appendTDigest(histogram);
    }
}
