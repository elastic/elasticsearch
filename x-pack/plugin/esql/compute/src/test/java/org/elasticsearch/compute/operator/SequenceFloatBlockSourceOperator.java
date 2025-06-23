/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.FloatVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.AbstractBlockSourceOperator;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * A source operator whose output is the given float values. This operator produces pages
 * containing a single Block. The Block contains the float values from the given list, in order.
 */
public class SequenceFloatBlockSourceOperator extends AbstractBlockSourceOperator {

    static final int DEFAULT_MAX_PAGE_POSITIONS = 8 * 1024;

    private final float[] values;

    public SequenceFloatBlockSourceOperator(BlockFactory blockFactory, Stream<Float> values) {
        this(blockFactory, values, DEFAULT_MAX_PAGE_POSITIONS);
    }

    public SequenceFloatBlockSourceOperator(BlockFactory blockFactory, Stream<Float> values, int maxPagePositions) {
        super(blockFactory, maxPagePositions);
        var l = values.toList();
        this.values = new float[l.size()];
        IntStream.range(0, l.size()).forEach(i -> this.values[i] = l.get(i));
    }

    public SequenceFloatBlockSourceOperator(BlockFactory blockFactory, List<Float> values) {
        this(blockFactory, values, DEFAULT_MAX_PAGE_POSITIONS);
    }

    public SequenceFloatBlockSourceOperator(BlockFactory blockFactory, List<Float> values, int maxPagePositions) {
        super(blockFactory, maxPagePositions);
        this.values = new float[values.size()];
        IntStream.range(0, this.values.length).forEach(i -> this.values[i] = values.get(i));
    }

    @Override
    protected Page createPage(int positionOffset, int length) {
        FloatVector.FixedBuilder builder = blockFactory.newFloatVectorFixedBuilder(length);
        for (int i = 0; i < length; i++) {
            builder.appendFloat(values[positionOffset + i]);
        }
        currentPosition += length;
        return new Page(builder.build().asBlock());
    }

    protected int remaining() {
        return values.length - currentPosition;
    }
}
