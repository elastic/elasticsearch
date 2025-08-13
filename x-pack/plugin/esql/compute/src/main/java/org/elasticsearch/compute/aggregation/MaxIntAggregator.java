/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;

@Aggregator({ @IntermediateState(name = "max", type = "INT"), @IntermediateState(name = "seen", type = "BOOLEAN") })
@GroupingAggregator
class MaxIntAggregator {

    public static int init() {
        return Integer.MIN_VALUE;
    }

    public static int combine(int current, int v) {
        return Math.max(current, v);
    }
}
