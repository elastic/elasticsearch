/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;

@Aggregator({
    @IntermediateState(name = "agg", type = "BYTES_REF")
})
@GroupingAggregator
class FirstValueLongAggregator {

    // single

    public static FirstValueLongSingleState initSingle() {
        return new FirstValueLongSingleState();
    }

    public static void combine(FirstValueLongSingleState current, long v) {
        current.add(v, /*TODO get timestamp value*/ 0L);
    }

    public static void combineIntermediate(FirstValueLongSingleState current, BytesRef state) {

    }

    public static Block evaluateFinal(FirstValueLongSingleState state, DriverContext driverContext) {
        return driverContext.blockFactory().newConstantLongBlockWith(state.value, 1);
    }


    // grouping

    public static FirstValueLongGroupingState initGrouping() {
        return new FirstValueLongGroupingState();
    }

    public static void combine(FirstValueLongGroupingState state, int groupId, long v) {

    }

    public static  void combineIntermediate(FirstValueLongGroupingState current, int groupId, BytesRef inValue) {

    }

    public static void combineStates(FirstValueLongGroupingState state, int groupId, FirstValueLongGroupingState otherState, int otherGroupId) {
    }

    public static Block evaluateFinal(FirstValueLongGroupingState state, IntVector selected, DriverContext driverContext) {
        return driverContext.blockFactory().newConstantLongBlockWith(state.value, 1);
    }


    public static class FirstValueLongSingleState implements AggregatorState {

        private long value = 0;
        private long byTimestamp = Long.MAX_VALUE;

        public void add(long value, long byTimestamp) {
            if (byTimestamp > this.byTimestamp) {
                this.value = value;
                this.byTimestamp = byTimestamp;
            }
        }


        @Override
        public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {

        }

        @Override
        public void close() {

        }
    }

    public static class FirstValueLongGroupingState implements GroupingAggregatorState {

        private long value = 0;
        private long byTimestamp = Long.MAX_VALUE;

        public void add(long value, long byTimestamp) {
            if (byTimestamp > this.byTimestamp) {
                this.value = value;
                this.byTimestamp = byTimestamp;
            }
        }

        void enableGroupIdTracking(SeenGroupIds seen) {
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {

        }

        @Override
        public void close() {}
    }
}
