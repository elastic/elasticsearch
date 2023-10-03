/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.Page;

import java.util.List;
import java.util.stream.DoubleStream;

/**
 * A source operator whose output is the given double values. This operator produces pages
 * containing a single Block. The Block contains the double values from the given list, in order.
 */
public class SequenceDoubleBlockSourceOperator extends AbstractBlockSourceOperator {

    static final int DEFAULT_MAX_PAGE_POSITIONS = 8 * 1024;

    private final double[] values;
    private final DriverContext driverContext;

    public SequenceDoubleBlockSourceOperator(DriverContext driverContext, DoubleStream values) {
        this(driverContext, values, DEFAULT_MAX_PAGE_POSITIONS);
    }

    public SequenceDoubleBlockSourceOperator(DriverContext driverContext, DoubleStream values, int maxPagePositions) {
        super(maxPagePositions);
        this.values = values.toArray();
        this.driverContext = driverContext;
    }

    public SequenceDoubleBlockSourceOperator(DriverContext driverContext, List<Double> values) {
        this(driverContext, values, DEFAULT_MAX_PAGE_POSITIONS);
    }

    public SequenceDoubleBlockSourceOperator(DriverContext driverContext, List<Double> values, int maxPagePositions) {
        super(maxPagePositions);
        this.values = values.stream().mapToDouble(Double::doubleValue).toArray();
        this.driverContext = driverContext;
    }

    @Override
    protected Page createPage(int positionOffset, int length) {
        final double[] array = new double[length];
        for (int i = 0; i < length; i++) {
            array[i] = values[positionOffset + i];
        }
        currentPosition += length;
        return new Page(driverContext.blockFactory().newDoubleArrayVector(array, array.length).asBlock());
    }

    protected int remaining() {
        return values.length - currentPosition;
    }
}
