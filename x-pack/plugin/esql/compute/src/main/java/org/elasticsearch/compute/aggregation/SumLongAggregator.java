/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.Experimental;

@Experimental
final class SumLongAggregator extends AbstractLongAggregator {
    static SumLongAggregator create(int inputChannel) {
        return new SumLongAggregator(inputChannel, new LongState());
    }

    private SumLongAggregator(int channel, LongState state) {
        super(channel, state);
    }

    @Override
    protected long combine(long current, long v) {
        return Math.addExact(current, v);
    }
}
