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

@Aggregator({ @IntermediateState(name = "aggstate", type = "UNKNOWN") })
@GroupingAggregator
class MinDoubleAggregator {

    public static double init() {
        return Double.POSITIVE_INFINITY;
    }

    public static double combine(double current, double v) {
        return Math.min(current, v);
    }
}
