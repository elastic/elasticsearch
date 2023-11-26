/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;

@Aggregator({ @IntermediateState(name = "min", type = "BYTES_REF") })
@GroupingAggregator
class MinBytesRefAggregator {

    public static BytesRefState.SingleState initSingle() {
        return new BytesRefState.SingleState(BytesRefState.Operations.MIN);
    }

    public static void combine(BytesRefState.SingleState current, BytesRef v) {
        current.checkAndSet(v);
    }

    public static void combineStates(BytesRefState.SingleState current, BytesRefState.SingleState state) {
        current.checkAndSet(state.value());
    }

    public static void combineIntermediate(BytesRefState.SingleState state, BytesRef inValue) {
        state.checkAndSet(inValue);
    }

    public static Block evaluateFinal(BytesRefState.SingleState state, DriverContext driverContext) {
        return state.evaluate(driverContext);
    }

    public static BytesRefState.GroupingState initGrouping(BigArrays bigArrays) {
        return new BytesRefState.GroupingState(bigArrays, BytesRefState.Operations.MIN);
    }

    public static void combine(BytesRefState.GroupingState state, int groupId, BytesRef v) {
        state.add(groupId, v);
    }

    public static void combineIntermediate(BytesRefState.GroupingState state, int groupId, BytesRef inValue) {
        state.add(groupId, inValue);
    }

    public static void combineStates(
        BytesRefState.GroupingState current,
            int currentGroupId,
        BytesRefState.GroupingState state,
            int statePosition) {
        current.add(currentGroupId, state.getOrNull(statePosition));
    }

    public static Block evaluateFinal(BytesRefState.GroupingState state, IntVector selected, DriverContext driverContext) {
        return state.evaluate(selected, driverContext);
    }
}
