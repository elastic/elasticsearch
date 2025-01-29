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

@Aggregator({ @IntermediateState(name = "max", type = "BOOLEAN"), @IntermediateState(name = "seen", type = "BOOLEAN") })
@GroupingAggregator
class MaxBooleanAggregator {

    public static boolean init() {
        return false;
    }

    public static boolean combine(boolean current, boolean v) {
        return current || v;
    }
}
