/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.Experimental;

@Experimental
final class SumDoubleAggregator extends AbstractDoubleAggregator {
    static SumDoubleAggregator create(int inputChannel) {
        return new SumDoubleAggregator(inputChannel, new DoubleState());
    }

    private SumDoubleAggregator(int channel, DoubleState state) {
        super(channel, state);
    }

    @Override
    protected double combine(double current, double v) {
        return current + v;
    }
}
