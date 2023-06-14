/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;

@Aggregator
@GroupingAggregator
class SumIntAggregator {
    public static long init() {
        return 0;
    }

    public static long combine(long current, int v) {
        return Math.addExact(current, v);
    }

    public static void combineStates(LongState current, LongState state) {
        current.longValue(Math.addExact(current.longValue(), state.longValue()));
    }

    public static void combineStates(LongArrayState current, int groupId, LongArrayState state, int position) {
        current.set(Math.addExact(current.getOrDefault(groupId), state.get(position)), groupId);
    }
}
