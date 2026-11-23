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
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntVector;

@Aggregator({ @IntermediateState(name = "sum", type = "LONG"), @IntermediateState(name = "seen", type = "BOOLEAN") })
@GroupingAggregator(supportsPartitioning = true)
class SumIntAggregator {

    public static long init() {
        return 0;
    }

    public static long combine(long current, int v) {
        return Math.addExact(current, v);
    }

    public static long combine(long current, long v) {
        return Math.addExact(current, v);
    }

    public static void combine(LongArrayState state, int groupId, int v) {
        state.addExact(groupId, v);
    }

    public static void combine(LongArrayState state, int groupId, long v) {
        state.addExact(groupId, v);
    }

    /**
     * When the group ids are constant or come with their {@link IntVector#runEnds() run ends}, sums each run in a
     * {@code long}, which cannot overflow for the ints of one page, and adds it to the group once.
     */
    public static GroupingAggregatorFunction.AddInput wrapAddInput(
        GroupingAggregatorFunction.AddInput delegate,
        LongArrayState state,
        IntVector values
    ) {
        return new GroupingAggregatorFunction.AddInput() {
            @Override
            public void add(int positionOffset, IntArrayBlock groupIds) {
                delegate.add(positionOffset, groupIds);
            }

            @Override
            public void add(int positionOffset, IntBigArrayBlock groupIds) {
                delegate.add(positionOffset, groupIds);
            }

            @Override
            public void add(int positionOffset, IntVector groupIds) {
                if (groupIds.isConstant() && groupIds.getPositionCount() > 0) {
                    long sum = 0;
                    for (int p = 0; p < groupIds.getPositionCount(); p++) {
                        sum += values.getInt(positionOffset + p);
                    }
                    state.addExact(groupIds.getInt(0), sum);
                    return;
                }
                int[] runEnds = groupIds.runEnds();
                if (runEnds == null) {
                    delegate.add(positionOffset, groupIds);
                    return;
                }
                int start = 0;
                for (int end : runEnds) {
                    long sum = 0;
                    for (int p = start; p < end; p++) {
                        sum += values.getInt(positionOffset + p);
                    }
                    state.addExact(groupIds.getInt(start), sum);
                    start = end;
                }
            }

            @Override
            public void close() {
                delegate.close();
            }
        };
    }
}
