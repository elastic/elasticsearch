/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.AbstractBlockSourceOperator;

import java.util.List;
import java.util.stream.DoubleStream;

/**
 * A source operator whose output is the given double values. This operator produces pages
 * containing a single Block. The Block contains the double values from the given list, in order.
 */
public class SequenceDoubleBlockSourceOperator extends AbstractBlockSourceOperator {

    static final int DEFAULT_MAX_PAGE_POSITIONS = 8 * 1024;

    private final double[] values;

    public SequenceDoubleBlockSourceOperator(BlockFactory blockFactory, DoubleStream values) {
        this(blockFactory, values, DEFAULT_MAX_PAGE_POSITIONS);
    }

    public SequenceDoubleBlockSourceOperator(BlockFactory blockFactory, DoubleStream values, int maxPagePositions) {
        super(blockFactory, maxPagePositions);
        this.values = values.toArray();
    }

    public SequenceDoubleBlockSourceOperator(BlockFactory blockFactory, List<Double> values) {
        this(blockFactory, values, DEFAULT_MAX_PAGE_POSITIONS);
    }

    public SequenceDoubleBlockSourceOperator(BlockFactory blockFactory, List<Double> values, int maxPagePositions) {
        super(blockFactory, maxPagePositions);
        this.values = values.stream().mapToDouble(Double::doubleValue).toArray();
    }

    @Override
    protected Page createPage(int positionOffset, int length) {
        DoubleVector.FixedBuilder builder = blockFactory.newDoubleVectorFixedBuilder(length);
        for (int i = 0; i < length; i++) {
            builder.appendDouble(values[positionOffset + i]);
        }
        currentPosition += length;
        return new Page(builder.build().asBlock());
    }

    protected int remaining() {
        return values.length - currentPosition;
    }
}
