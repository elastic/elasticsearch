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

/**
 * Double maximum aggregator for compatibility with old versions: a {@code NaN} input makes the result {@code NaN}.
 * <p>
 *     Replaced by {@link MaxDoubleAggregator}, which skips {@code NaN}, once every node supports
 *     {@code Max.ESQL_MINMAX_SKIP_NAN}. A node that predates it combines partial results with this rule, so a cluster
 *     that still holds such a node must keep using this aggregator on every node.
 * </p>
 */
@Aggregator({ @IntermediateState(name = "max", type = "DOUBLE"), @IntermediateState(name = "seen", type = "BOOLEAN") })
@GroupingAggregator(supportsPartitioning = true)
class MaxNanPropagatingDoubleAggregator {

    public static double init() {
        return -Double.MAX_VALUE;
    }

    public static double combine(double current, double v) {
        return Math.max(current, v);
    }

    public static void combine(DoubleArrayState state, int groupId, double v) {
        state.max(groupId, v);
    }
}
