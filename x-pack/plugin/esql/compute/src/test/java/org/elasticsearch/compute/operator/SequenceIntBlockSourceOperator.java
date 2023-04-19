/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;

import java.util.List;
import java.util.stream.IntStream;

/**
 * A source operator whose output is the given integer values. This operator produces pages
 * containing a single Block. The Block contains the integer values from the given list, in order.
 */
public class SequenceIntBlockSourceOperator extends AbstractBlockSourceOperator {

    static final int DEFAULT_MAX_PAGE_POSITIONS = 8 * 1024;

    private final int[] values;

    public SequenceIntBlockSourceOperator(IntStream values) {
        this(values, DEFAULT_MAX_PAGE_POSITIONS);
    }

    public SequenceIntBlockSourceOperator(IntStream values, int maxPagePositions) {
        super(maxPagePositions);
        this.values = values.toArray();
    }

    public SequenceIntBlockSourceOperator(List<Integer> values) {
        this(values, DEFAULT_MAX_PAGE_POSITIONS);
    }

    public SequenceIntBlockSourceOperator(List<Integer> values, int maxPagePositions) {
        super(maxPagePositions);
        this.values = values.stream().mapToInt(Integer::intValue).toArray();
    }

    @Override
    protected Page createPage(int positionOffset, int length) {
        IntVector.Builder builder = IntVector.newVectorBuilder(length);
        for (int i = 0; i < length; i++) {
            builder.appendInt(values[positionOffset + i]);
        }
        currentPosition += length;
        return new Page(builder.build().asBlock());
    }

    protected int remaining() {
        return values.length - currentPosition;
    }
}
