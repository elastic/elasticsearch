/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.aggregation;

final class GroupingMaxAggregator extends AbstractGroupingMinMaxAggregator {

    private static final double INITIAL_VALUE = Double.MIN_VALUE;

    static GroupingMaxAggregator create(int inputChannel) {
        if (inputChannel < 0) {
            throw new IllegalArgumentException();
        }
        return new GroupingMaxAggregator(inputChannel, new DoubleArrayState(INITIAL_VALUE));
    }

    static GroupingMaxAggregator createIntermediate() {
        return new GroupingMaxAggregator(-1, new DoubleArrayState(INITIAL_VALUE));
    }

    private GroupingMaxAggregator(int channel, DoubleArrayState state) {
        super(channel, state);
    }

    @Override
    protected double operator(double v1, double v2) {
        return Math.max(v1, v2);
    }

    @Override
    protected double boundaryValue() {
        return INITIAL_VALUE;
    }
}
