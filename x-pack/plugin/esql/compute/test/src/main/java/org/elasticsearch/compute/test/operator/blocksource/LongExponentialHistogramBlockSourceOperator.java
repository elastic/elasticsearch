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
import org.elasticsearch.compute.data.ExponentialHistogramBlockBuilder;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;

import java.util.List;
import java.util.stream.Stream;

/**
 * A source operator whose output is the given tuple values. This operator produces pages
 * with two Blocks. The returned pages preserve the order of values as given in the in initial list.
 */
public class LongExponentialHistogramBlockSourceOperator extends TupleAbstractBlockSourceOperator<Long, ExponentialHistogram> {

    public LongExponentialHistogramBlockSourceOperator(BlockFactory blockFactory, Stream<Tuple<Long, ExponentialHistogram>> values) {
        this(blockFactory, values.toList());
    }

    public LongExponentialHistogramBlockSourceOperator(BlockFactory blockFactory, List<Tuple<Long, ExponentialHistogram>> values) {
        super(blockFactory, values, ElementType.LONG, ElementType.EXPONENTIAL_HISTOGRAM);
    }

    @Override
    protected void consumeFirstElement(Long longVal, Block.Builder longBlockBuilder) {
        ((LongBlock.Builder) longBlockBuilder).appendLong(longVal);

    }

    @Override
    protected void consumeSecondElement(ExponentialHistogram histogram, Block.Builder histogramBlockBuilder) {
        ((ExponentialHistogramBlockBuilder) histogramBlockBuilder).append(histogram);
    }
}
