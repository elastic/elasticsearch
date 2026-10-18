/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.aggregations.metrics.CompensatedSum;

/**
 * Fits a line using relative timestamps so epoch-sized values do not destroy the covariance.
 * Bucket states can be reduced without retaining or sorting the observations.
 */
@GroupingAggregator(
    {
        @IntermediateState(name = "count", type = "LONG"),
        @IntermediateState(name = "origin", type = "LONG"),
        @IntermediateState(name = "x", type = "DOUBLE"),
        @IntermediateState(name = "cx", type = "DOUBLE"),
        @IntermediateState(name = "y", type = "DOUBLE"),
        @IntermediateState(name = "cy", type = "DOUBLE"),
        @IntermediateState(name = "xx", type = "DOUBLE"),
        @IntermediateState(name = "cxx", type = "DOUBLE"),
        @IntermediateState(name = "xy", type = "DOUBLE"),
        @IntermediateState(name = "cxy", type = "DOUBLE"),
        @IntermediateState(name = "constant", type = "DOUBLE") }
)
class PredictLinearDoubleAggregator {
    public static GroupingState initGrouping(DriverContext driverContext, double horizon) {
        return new GroupingState(driverContext.bigArrays(), driverContext.breaker(), horizon);
    }

    public static void combine(GroupingState state, int groupId, double value, long timestamp) {
        state.getOrCreate(groupId).add(timestamp, value);
    }

    public static void combineIntermediate(
        GroupingState state,
        int groupId,
        long count,
        long origin,
        double x,
        double cx,
        double y,
        double cy,
        double xx,
        double cxx,
        double xy,
        double cxy,
        double constant
    ) {
        if (count != 0) {
            state.getOrCreate(groupId).merge(count, origin, x, cx, y, cy, xx, cxx, xy, cxy, constant);
        }
    }

    public static Block evaluateFinal(GroupingState state, IntVector selected, GroupingAggregatorEvaluationContext context) {
        if (context instanceof TimeSeriesGroupingAggregatorEvaluationContext == false) {
            throw new IllegalStateException("predict_linear requires time-series evaluation context");
        }
        var timeSeriesContext = (TimeSeriesGroupingAggregatorEvaluationContext) context;
        try (DoubleBlock.Builder result = context.blockFactory().newDoubleBlockBuilder(selected.getPositionCount())) {
            for (int p = 0; p < selected.getPositionCount(); p++) {
                int groupId = selected.getInt(p);
                Regression regression = state.get(groupId);
                if (regression == null || regression.count < 2) {
                    result.appendNull();
                } else {
                    result.appendDouble(regression.predict(timeSeriesContext.rangeEndInMillis(groupId), state.horizon));
                }
            }
            return result.build();
        }
    }

    private static final class Regression {
        private static final long RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(Regression.class) + 4 * RamUsageEstimator
            .shallowSizeOfInstance(CompensatedSum.class);
        private long count;
        private long origin;
        // NaN marks a non-constant series; IEEE equality also handles signed zero.
        private double constant;
        private final CompensatedSum x = new CompensatedSum();
        private final CompensatedSum y = new CompensatedSum();
        private final CompensatedSum xx = new CompensatedSum();
        private final CompensatedSum xy = new CompensatedSum();

        void add(long timestamp, double value) {
            if (count == 0) {
                origin = timestamp;
                constant = value;
            } else if (constant != value) {
                constant = Double.NaN;
            }
            double relative = secondsBetween(timestamp, origin);
            count++;
            x.add(relative);
            y.add(value);
            xx.add(relative * relative);
            xy.add(relative * value);
        }

        void merge(
            long n,
            long otherOrigin,
            double sx,
            double cx,
            double sy,
            double cy,
            double sxx,
            double cxx,
            double sxy,
            double cxy,
            double otherConstant
        ) {
            if (count == 0) {
                origin = otherOrigin;
                constant = otherConstant;
            } else if (constant != otherConstant) {
                constant = Double.NaN;
            }
            // Translate the other partition's moments from x to x + shift, preserving their correction terms.
            double shift = secondsBetween(otherOrigin, origin);
            x.add(sx, cx).add(n * shift);
            y.add(sy, cy);
            xx.add(sxx, cxx).add(2 * shift * sx).add(2 * shift * cx).add(n * shift * shift);
            xy.add(sxy, cxy).add(shift * sy).add(shift * cy);
            count += n;
        }

        double predict(long evaluationTime, double horizon) {
            if (Double.isFinite(constant)) {
                // Retain IEEE multiplication: a constant series with an infinite horizon yields NaN.
                return 0.0 * horizon + constant;
            }
            double sx = x.value() + x.delta();
            double sy = y.value() + y.delta();
            double sxx = xx.value() + xx.delta();
            double sxy = xy.value() + xy.delta();
            double slope = (sxy - sx * sy / count) / (sxx - sx * sx / count);
            double intercept = sy / count - slope * (sx / count + secondsBetween(origin, evaluationTime));
            return slope * horizon + intercept;
        }

        private static double secondsBetween(long timestamp, long origin) {
            long difference = timestamp - origin;
            if (((timestamp ^ origin) & (timestamp ^ difference)) >= 0) {
                return difference / 1000.0;
            }
            // Only the subtraction can overflow: preserve integer precision in the normal, nearby-timestamp case.
            return timestamp / 1000.0 - origin / 1000.0;
        }
    }

    /** Owns both the group index and the fixed-size regression objects charged to the request breaker. */
    public static final class GroupingState implements GroupingAggregatorState, Accountable {
        private final BigArrays bigArrays;
        private final CircuitBreaker breaker;
        private final double horizon;
        private ObjectArray<Regression> states;
        private long stateBytes;

        GroupingState(BigArrays bigArrays, CircuitBreaker breaker, double horizon) {
            this.bigArrays = bigArrays;
            this.breaker = breaker;
            this.horizon = horizon;
            states = bigArrays.newObjectArray(1);
        }

        private Regression get(int groupId) {
            return groupId < states.size() ? states.get(groupId) : null;
        }

        private Regression getOrCreate(int groupId) {
            states = bigArrays.grow(states, groupId + 1);
            Regression state = states.get(groupId);
            if (state == null) {
                breaker.addEstimateBytesAndMaybeBreak(Regression.RAM_BYTES_USED, "predict_linear");
                boolean success = false;
                try {
                    state = new Regression();
                    states.set(groupId, state);
                    stateBytes += Regression.RAM_BYTES_USED;
                    success = true;
                } finally {
                    if (success == false) {
                        breaker.addWithoutBreaking(-Regression.RAM_BYTES_USED);
                    }
                }
            }
            return state;
        }

        @Override
        public void enableGroupIdTracking(SeenGroupIds seenGroupIds) {
            // A missing object identifies an unseen group, including during intermediate evaluation.
        }

        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext context) {
            int size = selected.getPositionCount();
            DoubleVector.FixedBuilder[] sums = new DoubleVector.FixedBuilder[9];
            Block[] built = new Block[11];
            try (
                var counts = context.blockFactory().newLongVectorFixedBuilder(size);
                var origins = context.blockFactory().newLongVectorFixedBuilder(size)
            ) {
                for (int i = 0; i < sums.length; i++) {
                    sums[i] = context.blockFactory().newDoubleVectorFixedBuilder(size);
                }
                for (int p = 0; p < size; p++) {
                    Regression state = get(selected.getInt(p));
                    counts.appendLong(state == null ? 0 : state.count);
                    origins.appendLong(state == null ? 0 : state.origin);
                    append(sums, 0, state == null ? null : state.x);
                    append(sums, 2, state == null ? null : state.y);
                    append(sums, 4, state == null ? null : state.xx);
                    append(sums, 6, state == null ? null : state.xy);
                    sums[8].appendDouble(state == null ? Double.NaN : state.constant);
                }
                built[0] = counts.build().asBlock();
                built[1] = origins.build().asBlock();
                for (int i = 0; i < sums.length; i++) {
                    built[2 + i] = sums[i].build().asBlock();
                }
                System.arraycopy(built, 0, blocks, offset, built.length);
                built = null; // Ownership transfers only once every block has been built.
            } finally {
                Releasables.close(sums);
                if (built != null) {
                    Releasables.close(built);
                }
            }
        }

        private static void append(DoubleVector.FixedBuilder[] builders, int offset, CompensatedSum sum) {
            builders[offset].appendDouble(sum == null ? 0 : sum.value());
            builders[offset + 1].appendDouble(sum == null ? 0 : sum.delta());
        }

        @Override
        public long ramBytesUsed() {
            return states.ramBytesUsed() + stateBytes;
        }

        @Override
        public void close() {
            Releasables.close(states, () -> {
                breaker.addWithoutBreaking(-stateBytes);
                stateBytes = 0;
            });
        }
    }
}
