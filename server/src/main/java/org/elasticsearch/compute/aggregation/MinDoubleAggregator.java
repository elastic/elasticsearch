/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.Experimental;

@Experimental
final class MinDoubleAggregator extends AbstractDoubleAggregator {
    static MinDoubleAggregator create(int inputChannel) {
        return new MinDoubleAggregator(inputChannel, new DoubleState(Double.POSITIVE_INFINITY));
    }

    private MinDoubleAggregator(int channel, DoubleState state) {
        super(channel, state);
    }

    @Override
    protected double combine(double current, double v) {
        return Math.min(current, v);
    }
}
