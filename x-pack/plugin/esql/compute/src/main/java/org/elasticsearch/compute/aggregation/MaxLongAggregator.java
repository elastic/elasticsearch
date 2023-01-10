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
class MaxLongAggregator {
    public static long init() {
        return Long.MIN_VALUE;
    }

    public static long combine(long current, long v) {
        return Math.max(current, v);
    }
}
