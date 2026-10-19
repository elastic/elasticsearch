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
 * Fits a straight line to the samples in a series' window, then evaluates that line at the query's
 * evaluation time plus the prediction horizon. Ordinary least squares chooses the line that minimizes
 * the sum of squared differences between the observed values and the line's values at the sample times.
 * <p>
 * For sample i, let xi be {@code (timestampMillis - originMillis) / 1000.0} and yi be its metric value.
 * The origin is the first timestamp seen by an accumulator; samples need not arrive in timestamp order.
 * These five statistics are enough to fit the line without retaining the individual samples:
 * <pre>
 * field     mathematical quantity
 * count     n, the number of samples
 * x         sumX  = sum(xi)
 * y         sumY  = sum(yi)
 * xx        sumXX = sum(xi * xi)
 * xy        sumXY = sum(xi * yi)
 * </pre>
 * Each of the four sums uses {@link CompensatedSum} to carry a small correction for rounding errors.
 * The intermediate fields cx, cy, cxx and cxy preserve these corrections across reductions. Relative
 * timestamps keep the time products small compared with Unix-epoch products, reducing cancellation
 * when we subtract them below. Time differences and the horizon are in seconds, including fractions.
 * <p>
 * The slope compares how time and value vary together with how much time varies. Expanding the
 * products around the means gives expressions using only the stored sums:
 * <pre>
 * meanX = sumX / n
 * meanY = sumY / n
 * timeValueVariation = sum((xi - meanX) * (yi - meanY)) = sumXY - sumX * sumY / n
 * timeVariation      = sum((xi - meanX) * (xi - meanX)) = sumXX - sumX * sumX / n
 * slope = timeValueVariation / timeVariation
 * </pre>
 * The fitted line passes through (meanX, meanY). Its value at any target time is therefore
 * {@code meanY + slope * (targetX - meanX)}, where targetX is seconds since the same origin.
 * The slope has units of metric value per second.
 * <p>
 * For example, samples (0 seconds, 10) and (2 seconds, 14), with origin 0, give n=2, sumX=2,
 * sumY=24, sumXX=4 and sumXY=28. Thus meanX=1, meanY=12 and slope=(28-24)/(4-2)=2.
 * At evaluation time 5 seconds with a 500ms horizon, targetX=5.5 and the prediction is
 * {@code 12 + 2 * (5.5 - 1) = 21}. The target is measured from the evaluation time, not the last sample.
 * <p>
 * Each bucket or worker can accumulate these statistics independently. Their origins may differ;
 * {@link Regression#merge} translates the incoming sums to the receiving origin before adding them.
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

        /**
         * Adds another partition without revisiting its samples. With d = otherOrigin - origin in seconds,
         * each incoming coordinate becomes {@code newX = oldX + d}. Expanding the products gives:
         * <pre>
         * sum(oldX + d)          = Sx  + n*d
         * sum((oldX + d)^2)      = Sxx + 2*d*Sx + n*d*d
         * sum((oldX + d) * oldY) = Sxy + d*Sy
         * </pre>
         * Here Sx, Sy, Sxx and Sxy are the incoming partition's original sums, including their corrections;
         * all right-hand sides use those original sums. Values do not shift, so Sy is added unchanged.
         * We translate the main sums and their corrections separately to retain compensation while merging.
         * <p>
         * In the class example, a partition containing only (2 seconds, 14) has origin=2 seconds and local
         * Sx=Sxx=Sxy=0. Merging it into the partition at origin=0 shifts it by d=2, contributing
         * Sx=2, Sxx=4 and Sxy=28, exactly as if both samples had been accumulated with origin=0.
         */
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
            double shift = secondsBetween(otherOrigin, origin);
            x.add(sx, cx).add(n * shift);
            y.add(sy, cy);
            xx.add(sxx, cxx).add(2 * shift * sx).add(2 * shift * cx).add(n * shift * shift);
            xy.add(sxy, cxy).add(shift * sy).add(shift * cy);
            count += n;
        }

        /**
         * Evaluates the line once at least two samples have been collected. In the code below, sx, sy,
         * sxx and sxy are the corrected sums described in the class documentation.
         * <p>
         * Let evaluationX be seconds from origin to evaluationTime. The value at evaluationTime is
         * {@code meanY + slope * (evaluationX - meanX)}. We call this value intercept because it is the
         * intercept of the line when evaluationTime becomes time zero. Since
         * {@code secondsBetween(origin, evaluationTime) = -evaluationX}, the expression below is the
         * same formula. Advancing by horizon seconds then gives {@code intercept + slope * horizon}.
         * In the class example, intercept=20 and the prediction is {@code 20 + 2 * 0.5 = 21}.
         */
        double predict(long evaluationTime, double horizon) {
            if (Double.isFinite(constant)) {
                // A finite constant series has slope zero. Its value remains valid even if summing its values overflowed.
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

        /** Returns a signed difference in seconds; floating-point division preserves millisecond fractions. */
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
