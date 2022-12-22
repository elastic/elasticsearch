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
final class MinLongAggregator extends AbstractLongAggregator {
    static MinLongAggregator create(int inputChannel) {
        /*
         * If you don't see any values this spits out Long.MAX_VALUE but
         * PostgreSQL spits out *nothing* when it gets an empty table:
         * # SELECT max(a) FROM foo;
         *  max
         * -----
         *
         * (1 row)
         */
        return new MinLongAggregator(inputChannel, new LongState(Long.MAX_VALUE));
    }

    private MinLongAggregator(int channel, LongState state) {
        super(channel, state);
    }

    @Override
    protected long combine(long current, long v) {
        return Math.min(current, v);
    }
}
