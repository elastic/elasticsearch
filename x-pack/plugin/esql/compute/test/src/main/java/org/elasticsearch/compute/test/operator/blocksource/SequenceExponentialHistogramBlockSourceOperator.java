/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.test.operator.blocksource;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * A source operator whose output is the given {@link ExponentialHistogram} values. This operator produces pages
 * containing a single Block. The Block contains the histogram values from the given list, in order.
 */
public class SequenceExponentialHistogramBlockSourceOperator extends AbstractBlockSourceOperator {

    static final int DEFAULT_MAX_PAGE_POSITIONS = 8 * 1024;

    private final List<ExponentialHistogram> values;

    public SequenceExponentialHistogramBlockSourceOperator(BlockFactory blockFactory, Stream<? extends ExponentialHistogram> values) {
        this(blockFactory, values, DEFAULT_MAX_PAGE_POSITIONS);
    }

    public SequenceExponentialHistogramBlockSourceOperator(
        BlockFactory blockFactory,
        Stream<? extends ExponentialHistogram> values,
        int maxPagePositions
    ) {
        super(blockFactory, maxPagePositions);
        this.values = values.map(value -> (ExponentialHistogram) value).toList();
    }

    public SequenceExponentialHistogramBlockSourceOperator(BlockFactory blockFactory, List<? extends ExponentialHistogram> values) {
        this(blockFactory, values, DEFAULT_MAX_PAGE_POSITIONS);
    }

    public SequenceExponentialHistogramBlockSourceOperator(
        BlockFactory blockFactory,
        List<? extends ExponentialHistogram> values,
        int maxPagePositions
    ) {
        super(blockFactory, maxPagePositions);
        this.values = new ArrayList<>(values);
    }

    @Override
    protected Page createPage(int positionOffset, int length) {
        try (var builder = blockFactory.newExponentialHistogramBlockBuilder(length)) {
            for (int i = 0; i < length; i++) {
                builder.append(values.get(positionOffset + i));
            }
            currentPosition += length;
            return new Page(builder.build());
        }
    }

    protected int remaining() {
        return values.size() - currentPosition;
    }
}
