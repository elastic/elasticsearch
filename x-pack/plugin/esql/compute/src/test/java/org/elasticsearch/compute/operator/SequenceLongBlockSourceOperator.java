/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.LongArrayVector;
import org.elasticsearch.compute.data.Page;

import java.util.List;
import java.util.stream.LongStream;

/**
 * A source operator whose output is the given long values. This operator produces pages
 * containing a single Block. The Block contains the long values from the given list, in order.
 */
public class SequenceLongBlockSourceOperator extends AbstractBlockSourceOperator {

    static final int DEFAULT_MAX_PAGE_POSITIONS = 8 * 1024;

    private final long[] values;

    public SequenceLongBlockSourceOperator(LongStream values) {
        this(values, DEFAULT_MAX_PAGE_POSITIONS);
    }

    public SequenceLongBlockSourceOperator(LongStream values, int maxPagePositions) {
        super(maxPagePositions);
        this.values = values.toArray();
    }

    public SequenceLongBlockSourceOperator(List<Long> values) {
        this(values, DEFAULT_MAX_PAGE_POSITIONS);
    }

    public SequenceLongBlockSourceOperator(List<Long> values, int maxPagePositions) {
        super(maxPagePositions);
        this.values = values.stream().mapToLong(Long::longValue).toArray();
    }

    @Override
    protected Page createPage(int positionOffset, int length) {
        final long[] array = new long[length];
        for (int i = 0; i < length; i++) {
            array[i] = values[positionOffset + i];
        }
        currentPosition += length;
        return new Page(new LongArrayVector(array, array.length).asBlock());
    }

    protected int remaining() {
        return values.length - currentPosition;
    }
}
