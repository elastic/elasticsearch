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

@Aggregator(
    {
        @IntermediateState(name = "value", type = "DOUBLE"),
        @IntermediateState(name = "delta", type = "DOUBLE"),
        @IntermediateState(name = "seen", type = "BOOLEAN") }
)
@GroupingAggregator
class SumFloatAggregator extends SumDoubleAggregator {

    public static void combine(SumState current, float v) {
        current.add(v);
    }

    public static void combine(GroupingSumState current, int groupId, float v) {
        current.add(v, groupId);
    }

}
