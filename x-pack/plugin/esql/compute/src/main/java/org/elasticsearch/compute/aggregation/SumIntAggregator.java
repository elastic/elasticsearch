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
import org.elasticsearch.core.Releasables;

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
     * Wraps the generated vector-of-values input so group ids that arrive in runs touch the state once per run: the
     * run is summed in a {@code long}, which cannot overflow for the ints of one page, and added to its group
     * once. A group that appears in several runs is added by each of them.
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
                delegate.add(positionOffset, groupIds);
            }

            @Override
            public void addRuns(int positionOffset, IntVector groupIds) {
                final int positions = groupIds.getPositionCount();
                if (positions == 0) {
                    return;
                }
                int runGroupId = groupIds.getInt(0);
                long runSum = values.getInt(positionOffset);
                for (int groupPosition = 1; groupPosition < positions; groupPosition++) {
                    final int groupId = groupIds.getInt(groupPosition);
                    final int value = values.getInt(positionOffset + groupPosition);
                    if (groupId == runGroupId) {
                        runSum += value;
                    } else {
                        state.addExact(runGroupId, runSum);
                        runGroupId = groupId;
                        runSum = value;
                    }
                }
                state.addExact(runGroupId, runSum);
            }

            @Override
            public void close() {
                Releasables.close(delegate);
            }
        };
    }
}
