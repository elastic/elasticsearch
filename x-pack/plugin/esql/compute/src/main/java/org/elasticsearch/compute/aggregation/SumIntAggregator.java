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
import org.elasticsearch.compute.data.IntArrayVector;
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

    /**
     * Folds a whole {@link IntArrayVector} into {@code current}. The vector's sum cannot overflow a
     * {@code long}, so there is a single overflow check per vector instead of one per element.
     */
    public static long combineBulk(long current, IntArrayVector v) {
        return Math.addExact(current, v.sum());
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
     * Wraps the generated input for a page of int values without nulls or multivalues, so that group ids that arrive as
     * {@link GroupRuns runs} touch the state once per run. A run is summed in a {@code long}, which cannot overflow for
     * the ints of one page, and added to its group once. A single run, the common case when the grouping key is the
     * primary index sort and one key spans the whole page, is one sum and one {@code addExact}.
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
            public void addRuns(int positionOffset, GroupRuns runs) {
                for (int run = 0; run < runs.runCount(); run++) {
                    long sum = 0;
                    for (int p = positionOffset + runs.start(run), end = positionOffset + runs.end(run); p < end; p++) {
                        sum += values.getInt(p);
                    }
                    state.addExact(runs.groupId(run), sum);
                }
            }

            @Override
            public void close() {
                Releasables.close(delegate);
            }
        };
    }
}
