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
final class LongSumAggregator extends AbstractLongAggregator {
    static LongSumAggregator create(int inputChannel) {
        return new LongSumAggregator(inputChannel, new LongState());
    }

    private LongSumAggregator(int channel, LongState state) {
        super(channel, state);
    }

    @Override
    protected long combine(long current, long v) {
        return Math.addExact(current, v);
    }
}
