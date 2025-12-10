/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.TDigestHolder;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramCircuitBreaker;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramMerger;
import org.elasticsearch.exponentialhistogram.ReleasableExponentialHistogram;
import org.elasticsearch.search.aggregations.metrics.TDigestExecutionHint;
import org.elasticsearch.search.aggregations.metrics.TDigestState;

import java.util.function.DoubleBinaryOperator;

public final class TDigestStates {

    // Currently we use the same defaults as for queryDSL, we might make this configurable later
    private static final TDigestExecutionHint EXECUTION_HINT = TDigestExecutionHint.DEFAULT;
    static final double COMPRESSION = 100.0;

    private TDigestStates() {}

    private static double nanAwareAgg(double v1, double v2, DoubleBinaryOperator op) {
        if (Double.isNaN(v1)) {
            return v2;
        }
        if (Double.isNaN(v2)) {
            return v1;
        }
        return op.applyAsDouble(v1, v2);
    }

    static final class SingleState implements AggregatorState {

        private final CircuitBreaker breaker;
        // initialize lazily
        private TDigestState merger;

        double sum = Double.NaN;
        double min = Double.NaN;
        double max = Double.NaN;
        long count = 0;

        SingleState(CircuitBreaker breaker) {
            this.breaker = breaker;
        }

        public void add(TDigestHolder histogram) {
            if (histogram == null) {
                return;
            }
            if (merger == null) {
                merger = TDigestState.create(breaker, COMPRESSION, EXECUTION_HINT);
            }
            histogram.addTo(merger);
            sum = nanAwareAgg(histogram.getSum(), sum, Double::sum);
            count += histogram.getValueCount();
            min = nanAwareAgg(histogram.getMin(), min, Double::min);
            max = nanAwareAgg(histogram.getMax(), max, Double::max);
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            assert blocks.length >= offset + 2;
            BlockFactory blockFactory = driverContext.blockFactory();
            // in case of error, the blocks are closed by the caller
            if (merger == null) {
                blocks[offset] = blockFactory.newConstantTDigestBlock(TDigestHolder.empty(), 1);
                blocks[offset + 1] = blockFactory.newConstantBooleanBlockWith(false, 1);
            } else {
                TDigestHolder resultHolder = new TDigestHolder(merger, min, max, sum, count);
                blocks[offset] = blockFactory.newConstantTDigestBlock(resultHolder, 1);
                blocks[offset + 1] = blockFactory.newConstantBooleanBlockWith(true, 1);
            }
        }

        @Override
        public void close() {
            Releasables.close(merger);
            merger = null;
        }

        public Block evaluateFinal(DriverContext driverContext) {
            BlockFactory blockFactory = driverContext.blockFactory();
            if (merger == null) {
                return blockFactory.newConstantNullBlock(1);
            } else {
                TDigestHolder resultHolder = new TDigestHolder(merger, min, max, sum, count);
                return blockFactory.newConstantTDigestBlock(resultHolder, 1);
            }
        }
    }

    static final class GroupingState implements GroupingAggregatorState {

        private ObjectArray<TDigestState> states;
        private DoubleArray minima;
        private DoubleArray maxima;
        private DoubleArray sums;
        private LongArray counts;

        private final CircuitBreaker breaker;
        private final BigArrays bigArrays;

        GroupingState(BigArrays bigArrays, CircuitBreaker breaker) {
            this.states = bigArrays.newObjectArray(1);
            this.bigArrays = bigArrays;
            this.breaker = breaker;
        }

        TDigestState getOrNull(int position) {
            if (position < states.size()) {
                return states.get(position);
            } else {
                return null;
            }
        }

        public void add(int groupId, TDigestHolder histogram) {
            if (histogram == null) {
                return;
            }
            ensureCapacity(groupId);
            var state = states.get(groupId);
            if (state == null) {
                state = ExponentialHistogramMerger.create(MAX_BUCKET_COUNT, breaker);
                states.set(groupId, state);
            }
            if (allowUpscale) {
                state.add(histogram);
            } else {
                state.addWithoutUpscaling(histogram);
            }
        }

        private void ensureCapacity(int groupId) {
            states = bigArrays.grow(states, groupId + 1);
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            assert blocks.length >= offset + 2 : "blocks=" + blocks.length + ",offset=" + offset;
            try (
                var histoBuilder = driverContext.blockFactory().newExponentialHistogramBlockBuilder(selected.getPositionCount());
                var seenBuilder = driverContext.blockFactory().newBooleanBlockBuilder(selected.getPositionCount());
            ) {
                for (int i = 0; i < selected.getPositionCount(); i++) {
                    int groupId = selected.getInt(i);
                    ExponentialHistogramMerger state = getOrNull(groupId);
                    if (state != null) {
                        seenBuilder.appendBoolean(true);
                        histoBuilder.append(state.get());
                    } else {
                        seenBuilder.appendBoolean(false);
                        histoBuilder.append(ExponentialHistogram.empty());
                    }
                }
                blocks[offset] = histoBuilder.build();
                blocks[offset + 1] = seenBuilder.build();
            }
        }

        public Block evaluateFinal(IntVector selected, DriverContext driverContext) {
            try (var builder = driverContext.blockFactory().newExponentialHistogramBlockBuilder(selected.getPositionCount());) {
                for (int i = 0; i < selected.getPositionCount(); i++) {
                    int groupId = selected.getInt(i);
                    ExponentialHistogramMerger state = getOrNull(groupId);
                    if (state != null) {
                        builder.append(state.get());
                    } else {
                        builder.appendNull();
                    }
                }
                return builder.build();
            }
        }

        @Override
        public void close() {
            for (int i = 0; i < states.size(); i++) {
                Releasables.close(states.get(i));
            }
            Releasables.close(states);
            states = null;
        }

        @Override
        public void enableGroupIdTracking(SeenGroupIds seenGroupIds) {
            // noop - we handle the null states inside `toIntermediate` and `evaluateFinal`
        }
    }

    /**
     * A state consisting of a single {@code long} value with a {@link ExponentialHistogram}.
     * The intermediate state contains three values in order: the long, the histogram, and a boolean specifying if a value was set or not.
     */
    public static final class WithLongSingleState implements AggregatorState {

        private final CircuitBreaker breaker;
        private long longValue;
        private ReleasableExponentialHistogram histogramValue;

        public WithLongSingleState(CircuitBreaker breaker) {
            this.breaker = breaker;
        }

        public boolean isSeen() {
            return histogramValue != null;
        }

        public long longValue() {
            assert isSeen();
            return longValue;
        }

        public ReleasableExponentialHistogram histogramValue() {
            assert isSeen();
            return histogramValue;
        }

        public void set(long longValue, ExponentialHistogram histogram) {
            assert histogram != null;
            this.longValue = longValue;
            ReleasableExponentialHistogram newValue;
            try (var copyBuilder = ExponentialHistogram.builder(histogram, new HistoBreaker(breaker))) {
                newValue = copyBuilder.build();
            }
            Releasables.close(histogramValue);
            this.histogramValue = newValue;
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            assert blocks.length >= offset + 3;
            BlockFactory blockFactory = driverContext.blockFactory();
            // in case of error, the blocks are closed by the caller
            if (histogramValue == null) {
                blocks[offset] = blockFactory.newConstantLongBlockWith(0L, 1);
                blocks[offset + 1] = blockFactory.newConstantExponentialHistogramBlock(ExponentialHistogram.empty(), 1);
                blocks[offset + 2] = blockFactory.newConstantBooleanBlockWith(false, 1);
            } else {
                blocks[offset] = blockFactory.newConstantLongBlockWith(longValue, 1);
                blocks[offset + 1] = blockFactory.newConstantExponentialHistogramBlock(histogramValue, 1);
                blocks[offset + 2] = blockFactory.newConstantBooleanBlockWith(true, 1);
            }
        }

        public Block evaluateFinalHistogram(DriverContext driverContext) {
            BlockFactory blockFactory = driverContext.blockFactory();
            if (histogramValue == null) {
                return blockFactory.newConstantNullBlock(1);
            } else {
                return blockFactory.newConstantExponentialHistogramBlock(histogramValue, 1);
            }
        }

        @Override
        public void close() {
            Releasables.close(histogramValue);
            histogramValue = null;
        }
    }

    /**
     * A grouping state consisting of a single {@code long} value with a {@link ExponentialHistogram} per group.
     * The intermediate state contains three values in order: the long, the histogram, and a boolean specifying if a value was set or not.
     */
    public static final class WithLongGroupingState implements GroupingAggregatorState {

        private LongArray longValues;
        private ObjectArray<ReleasableExponentialHistogram> histogramValues;
        private final HistoBreaker breaker;
        private final BigArrays bigArrays;

        WithLongGroupingState(BigArrays bigArrays, CircuitBreaker breaker) {
            LongArray longValues = null;
            ObjectArray<ReleasableExponentialHistogram> histogramValues = null;
            boolean success = false;
            try {
                longValues = bigArrays.newLongArray(1);
                histogramValues = bigArrays.newObjectArray(1);
                success = true;
            } finally {
                if (success == false) {
                    Releasables.close(histogramValues, longValues);
                }
            }
            this.longValues = longValues;
            this.histogramValues = histogramValues;
            this.bigArrays = bigArrays;
            this.breaker = new HistoBreaker(breaker);
        }

        public void set(int groupId, long longValue, ExponentialHistogram histogramValue) {
            assert histogramValue != null;
            ensureCapacity(groupId);
            try (var copyBuilder = ExponentialHistogram.builder(histogramValue, breaker)) {
                ReleasableExponentialHistogram old = histogramValues.getAndSet(groupId, copyBuilder.build());
                Releasables.close(old);
            }
            longValues.set(groupId, longValue);
        }

        private void ensureCapacity(int groupId) {
            histogramValues = bigArrays.grow(histogramValues, groupId + 1);
            longValues = bigArrays.grow(longValues, groupId + 1);
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            assert blocks.length >= offset + 3;
            try (
                var longBuilder = driverContext.blockFactory().newLongVectorFixedBuilder(selected.getPositionCount());
                var histoBuilder = driverContext.blockFactory().newExponentialHistogramBlockBuilder(selected.getPositionCount());
                var seenBuilder = driverContext.blockFactory().newBooleanVectorFixedBuilder(selected.getPositionCount());
            ) {
                for (int i = 0; i < selected.getPositionCount(); i++) {
                    int groupId = selected.getInt(i);
                    if (seen(groupId)) {
                        seenBuilder.appendBoolean(true);
                        longBuilder.appendLong(longValues.get(groupId));
                        histoBuilder.append(histogramValues.get(groupId));
                    } else {
                        seenBuilder.appendBoolean(false);
                        longBuilder.appendLong(0L);
                        histoBuilder.append(ExponentialHistogram.empty());
                    }
                }
                blocks[offset] = longBuilder.build().asBlock();
                blocks[offset + 1] = histoBuilder.build();
                blocks[offset + 2] = seenBuilder.build().asBlock();
            }
        }

        public boolean seen(int groupId) {
            return groupId < histogramValues.size() && histogramValues.get(groupId) != null;
        }

        public long longValue(int groupId) {
            assert seen(groupId);
            return longValues.get(groupId);
        }

        @Override
        public void close() {
            for (int i = 0; i < histogramValues.size(); i++) {
                Releasables.close(histogramValues.get(i));
            }
            Releasables.close(histogramValues, longValues);
            histogramValues = null;
            longValues = null;
        }

        public Block evaluateFinalHistograms(IntVector selected, DriverContext driverContext) {
            try (var builder = driverContext.blockFactory().newExponentialHistogramBlockBuilder(selected.getPositionCount());) {
                for (int i = 0; i < selected.getPositionCount(); i++) {
                    int groupId = selected.getInt(i);
                    if (seen(groupId)) {
                        builder.append(histogramValues.get(groupId));
                    } else {
                        builder.appendNull();
                    }
                }
                return builder.build();
            }
        }

        @Override
        public void enableGroupIdTracking(SeenGroupIds seenGroupIds) {
            // noop
        }

    }
}
