/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.test.operator.blocksource;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;

import java.util.List;
import java.util.stream.Stream;

/**
 * A source operator whose output is the given float array values. This operator produces pages
 * containing a single FloatBlock where each position is a multi-valued entry representing a dense vector.
 */
public class DenseVectorFloatBlockSourceOperator extends AbstractBlockSourceOperator {

    private static final int DEFAULT_MAX_PAGE_POSITIONS = 8 * 1024;

    private final List<float[]> values;

    public DenseVectorFloatBlockSourceOperator(BlockFactory blockFactory, Stream<float[]> values) {
        this(blockFactory, values, DEFAULT_MAX_PAGE_POSITIONS);
    }

    public DenseVectorFloatBlockSourceOperator(BlockFactory blockFactory, Stream<float[]> values, int maxPagePositions) {
        super(blockFactory, maxPagePositions);
        this.values = values.toList();
    }

    @Override
    protected Page createPage(int positionOffset, int length) {
        var builder = blockFactory.newFloatBlockBuilder(length);
        for (int i = 0; i < length; i++) {
            float[] vector = values.get(positionOffset + i);
            if (vector == null) {
                builder.appendNull();
            } else {
                builder.beginPositionEntry();
                for (float f : vector) {
                    builder.appendFloat(f);
                }
                builder.endPositionEntry();
            }
        }
        currentPosition += length;
        return new Page(builder.build());
    }

    @Override
    protected int remaining() {
        return values.size() - currentPosition;
    }
}
