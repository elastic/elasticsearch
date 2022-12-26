/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.Experimental;

@Experimental
final class GroupingMaxAggregator extends GroupingAbstractMinMaxAggregator {

    private static final double INITIAL_DEFAULT_VALUE = Double.NEGATIVE_INFINITY;

    static GroupingMaxAggregator create(BigArrays bigArrays, int inputChannel) {
        if (inputChannel < 0) {
            throw new IllegalArgumentException();
        }
        return new GroupingMaxAggregator(inputChannel, new DoubleArrayState(bigArrays, INITIAL_DEFAULT_VALUE));
    }

    static GroupingMaxAggregator createIntermediate(BigArrays bigArrays) {
        return new GroupingMaxAggregator(-1, new DoubleArrayState(bigArrays, INITIAL_DEFAULT_VALUE));
    }

    private GroupingMaxAggregator(int channel, DoubleArrayState state) {
        super(channel, state);
    }

    @Override
    protected double operator(double v1, double v2) {
        return Math.max(v1, v2);
    }

    @Override
    protected double initialDefaultValue() {
        return INITIAL_DEFAULT_VALUE;
    }
}
