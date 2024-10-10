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

@Aggregator({ @IntermediateState(name = "max", type = "FLOAT"), @IntermediateState(name = "seen", type = "BOOLEAN") })
@GroupingAggregator
class MaxFloatAggregator {

    public static float init() {
        return -Float.MAX_VALUE;
    }

    public static float combine(float current, float v) {
        return Math.max(current, v);
    }
}
