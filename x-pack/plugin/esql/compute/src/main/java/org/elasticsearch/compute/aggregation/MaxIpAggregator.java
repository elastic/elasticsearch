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
import org.elasticsearch.core.Releasable;

@Aggregator({ @IntermediateState(name = "max", type = "BYTES_REF"), @IntermediateState(name = "seen", type = "BOOLEAN") })
@GroupingAggregator
class MaxIpAggregator {
    private static final BytesRef MIN_VALUE = new BytesRef(new byte[16]);

    public static SingleState initSingle() {
        return new SingleState();
    }

    public static void combine(SingleState state, BytesRef value) {
        state.add(value);
    }

    public static void combineIntermediate(SingleState state, BytesRef value, boolean seen) {
        if (seen) {
            combine(state, value);
        }
    }

    public static Block evaluateFinal(SingleState state, DriverContext driverContext) {
        return state.toBlock(driverContext);
    }

    public static IpArrayState initGrouping(BigArrays bigArrays) {
        return new IpArrayState(bigArrays, MIN_VALUE);
    }

    public static void combine(IpArrayState state, int groupId, BytesRef value) {
        if (isBetter(value, state.getOrDefault(groupId))) {
            state.set(groupId, value);
        }
    }

    public static void combineIntermediate(IpArrayState state, int groupId, BytesRef value, boolean seen) {
        if (seen) {
            combine(state, groupId, value);
        }
    }

    public static void combineStates(IpArrayState current, int groupId, IpArrayState state, int otherGroupId) {
        if (state.hasValue(otherGroupId)) {
            combine(current, groupId, state.get(otherGroupId));
        }
    }

    public static Block evaluateFinal(IpArrayState state, IntVector selected, DriverContext driverContext) {
        return state.toValuesBlock(selected, driverContext);
    }

    private static boolean isBetter(BytesRef value, BytesRef otherValue) {
        return value.compareTo(otherValue) > 0;
    }

    public static class SingleState implements Releasable {
        private final BytesRef internalState;
        private boolean seen;

        private SingleState() {
            this.internalState = BytesRef.deepCopyOf(MIN_VALUE);
            this.seen = false;
        }

        public void add(BytesRef value) {
            if (isBetter(value, internalState)) {
                seen = true;
                System.arraycopy(value.bytes, value.offset, internalState.bytes, 0, internalState.length);
            }
        }

        void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            blocks[offset] = driverContext.blockFactory().newConstantBytesRefBlockWith(internalState, 1);
            blocks[offset + 1] = driverContext.blockFactory().newConstantBooleanBlockWith(seen, 1);
        }

        Block toBlock(DriverContext driverContext) {
            if (seen == false) {
                return driverContext.blockFactory().newConstantNullBlock(1);
            }

            return driverContext.blockFactory().newConstantBytesRefBlockWith(internalState, 1);
        }

        @Override
        public void close() {
            // Nothing to close
        }
    }
}
