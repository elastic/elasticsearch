/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;

class LastValueIntAggregator {

    public static LastValueStates.SingleState initSingle() {
        return new LastValueStates.SingleState();
    }

    public static void combine(LastValueStates.SingleState current, int v, long ts) {
        current.add(v, ts);
    }

    public static void combineStates(LastValueStates.SingleState current, LastValueStates.SingleState state) {
        current.add(state);
    }

    public static void combineIntermediate(LastValueStates.SingleState state, BytesRef inValue) {
        state.add(inValue);
    }

    public static Block evaluateFinal(LastValueStates.SingleState state, DriverContext driverContext) {
        return state.evaluateLastValue(driverContext);
    }

    public static LastValueStates.GroupingState initGrouping(BigArrays bigArrays) {
        return new LastValueStates.GroupingState(bigArrays);
    }

    public static void combine(LastValueStates.GroupingState state, int groupId, int v, long ts) {
        state.add(groupId, v, ts);
    }

    public static void combineIntermediate(LastValueStates.GroupingState state, int groupId, BytesRef inValue) {
        state.add(groupId, inValue);
    }

    public static void combineStates(
        LastValueStates.GroupingState current,
        int currentGroupId,
        LastValueStates.GroupingState state,
        int statePosition
    ) {
        current.add(currentGroupId, state.getOrNull(statePosition));
    }

    public static Block evaluateFinal(LastValueStates.GroupingState state, IntVector selected, DriverContext driverContext) {
        return state.evaluateLastValue(selected, driverContext);
    }
}
