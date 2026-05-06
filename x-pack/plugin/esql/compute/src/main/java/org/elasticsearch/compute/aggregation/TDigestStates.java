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
import org.elasticsearch.compute.data.BreakingTDigestHolder;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.TDigestHolder;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.aggregations.metrics.MemoryTrackingTDigestArrays;
import org.elasticsearch.tdigest.TDigest;

import java.util.function.DoubleBinaryOperator;

public final class TDigestStates {

    // Currently we use the same defaults as for queryDSL, we might make this configurable later
    public static final double COMPRESSION = 100.0;

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
        private final MemoryTrackingTDigestArrays tdigestArrays;
        // initialize lazily
        private TDigest merger;

        double sum = Double.NaN;
        double min = Double.NaN;
        double max = Double.NaN;

        SingleState(CircuitBreaker breaker) {
            this.breaker = breaker;
            this.tdigestArrays = new MemoryTrackingTDigestArrays(breaker);
        }

        public void add(TDigestHolder histogram) {
            if (histogram == null) {
                return;
            }
            if (merger == null) {
                merger = TDigest.createMergingDigest(tdigestArrays, COMPRESSION);
            }
            merger.add(histogram);
            sum = nanAwareAgg(histogram.getSum(), sum, Double::sum);
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
                try (var tempHolder = BreakingTDigestHolder.create(breaker)) {
                    tempHolder.set(merger, sum, min, max);
                    blocks[offset] = blockFactory.newConstantTDigestBlock(tempHolder.accessor(), 1);
                    blocks[offset + 1] = blockFactory.newConstantBooleanBlockWith(true, 1);
                }
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
                try (var tempHolder = BreakingTDigestHolder.create(breaker)) {
                    tempHolder.set(merger, sum, min, max);
                    return blockFactory.newConstantTDigestBlock(tempHolder.accessor(), 1);
                }
            }
        }
    }

    static final class GroupingState implements GroupingAggregatorState {

        private ObjectArray<TDigest> states;
        private DoubleArray minima;
        private DoubleArray maxima;
        private DoubleArray sums;

        private final CircuitBreaker breaker;
        private final BigArrays bigArrays;
        private final MemoryTrackingTDigestArrays tdigestArrays;

        GroupingState(BigArrays bigArrays, CircuitBreaker breaker) {
            this.bigArrays = bigArrays;
            this.breaker = breaker;
            this.tdigestArrays = new MemoryTrackingTDigestArrays(breaker);
            ObjectArray<TDigest> states = null;
            DoubleArray minima = null;
            DoubleArray maxima = null;
            DoubleArray sums = null;
            boolean success = false;
            try {
                states = bigArrays.newObjectArray(1);
                minima = bigArrays.newDoubleArray(1);
                maxima = bigArrays.newDoubleArray(1);
                sums = bigArrays.newDoubleArray(1);
                success = true;
            } finally {
                if (success == false) {
                    Releasables.close(states, minima, maxima, sums);
                }
            }
            this.states = states;
            this.minima = minima;
            this.maxima = maxima;
            this.sums = sums;
        }

        TDigest getOrNull(int position) {
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
            double min;
            double max;
            double sum;
            if (state == null) {
                state = TDigest.createMergingDigest(tdigestArrays, COMPRESSION);
                states.set(groupId, state);
                min = Double.NaN;
                max = Double.NaN;
                sum = Double.NaN;
            } else {
                min = minima.get(groupId);
                max = maxima.get(groupId);
                sum = sums.get(groupId);
            }
            state.add(histogram);
            minima.set(groupId, nanAwareAgg(min, histogram.getMin(), Double::min));
            maxima.set(groupId, nanAwareAgg(max, histogram.getMax(), Double::max));
            sums.set(groupId, nanAwareAgg(sum, histogram.getSum(), Double::sum));
        }

        private void ensureCapacity(int groupId) {
            states = bigArrays.grow(states, groupId + 1);
            minima = bigArrays.grow(minima, groupId + 1);
            maxima = bigArrays.grow(maxima, groupId + 1);
            sums = bigArrays.grow(sums, groupId + 1);
        }

        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            assert blocks.length >= offset + 2 : "blocks=" + blocks.length + ",offset=" + offset;
            try (
                var histoBuilder = driverContext.blockFactory().newTDigestBlockBuilder(selected.getPositionCount());
                var seenBuilder = driverContext.blockFactory().newBooleanBlockBuilder(selected.getPositionCount());
                var tempHolder = BreakingTDigestHolder.create(breaker);
            ) {
                for (int i = 0; i < selected.getPositionCount(); i++) {
                    int groupId = selected.getInt(i);
                    TDigest state = getOrNull(groupId);
                    if (state != null) {
                        seenBuilder.appendBoolean(true);
                        tempHolder.set(state, sums.get(groupId), minima.get(groupId), maxima.get(groupId));
                        histoBuilder.appendTDigest(tempHolder.accessor());
                    } else {
                        seenBuilder.appendBoolean(false);
                        histoBuilder.appendTDigest(TDigestHolder.empty());
                    }
                }
                blocks[offset] = histoBuilder.build();
                blocks[offset + 1] = seenBuilder.build();
            }
        }

        public Block evaluateFinal(IntVector selected, DriverContext driverContext) {
            try (
                var histoBuilder = driverContext.blockFactory().newTDigestBlockBuilder(selected.getPositionCount());
                var tempHolder = BreakingTDigestHolder.create(breaker);
            ) {
                for (int i = 0; i < selected.getPositionCount(); i++) {
                    int groupId = selected.getInt(i);
                    TDigest state = getOrNull(groupId);
                    if (state != null) {
                        tempHolder.set(state, sums.get(groupId), minima.get(groupId), maxima.get(groupId));
                        histoBuilder.appendTDigest(tempHolder.accessor());
                    } else {
                        histoBuilder.appendNull();
                    }
                }
                return histoBuilder.build();
            }
        }

        @Override
        public void close() {
            for (int i = 0; i < states.size(); i++) {
                Releasables.close(states.get(i));
            }
            Releasables.close(states, minima, maxima, sums);
            states = null;
        }

        @Override
        public void enableGroupIdTracking(SeenGroupIds seenGroupIds) {
            // noop - we handle the null states inside `toIntermediate` and `evaluateFinal`
        }
    }

    /**
     * A state consisting of a single {@code long} value with a {@link TDigestHolder}.
     * The intermediate state contains three values in order: the long, the digest, and a boolean specifying if a value was set or not.
     */
    public static final class WithLongSingleState implements AggregatorState {

        private final CircuitBreaker breaker;
        private long longValue;
        private BreakingTDigestHolder value;

        public WithLongSingleState(CircuitBreaker breaker) {
            this.breaker = breaker;
        }

        public boolean isSeen() {
            return value != null;
        }

        public long longValue() {
            assert isSeen();
            return longValue;
        }

        public void set(long longValue, TDigestHolder digestValue) {
            assert digestValue != null;
            this.longValue = longValue;
            if (value == null) {
                value = BreakingTDigestHolder.create(breaker);
            }
            value.set(digestValue);
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            assert blocks.length >= offset + 3;
            BlockFactory blockFactory = driverContext.blockFactory();
            // in case of error, the blocks are closed by the caller
            if (value == null) {
                blocks[offset] = blockFactory.newConstantLongBlockWith(0L, 1);
                blocks[offset + 1] = blockFactory.newConstantTDigestBlock(TDigestHolder.empty(), 1);
                blocks[offset + 2] = blockFactory.newConstantBooleanBlockWith(false, 1);
            } else {
                blocks[offset] = blockFactory.newConstantLongBlockWith(longValue, 1);
                blocks[offset + 1] = blockFactory.newConstantTDigestBlock(value.accessor(), 1);
                blocks[offset + 2] = blockFactory.newConstantBooleanBlockWith(true, 1);
            }
        }

        public Block evaluateFinalTDigest(DriverContext driverContext) {
            BlockFactory blockFactory = driverContext.blockFactory();
            if (value == null) {
                return blockFactory.newConstantNullBlock(1);
            } else {
                return blockFactory.newConstantTDigestBlock(value.accessor(), 1);
            }
        }

        @Override
        public void close() {
            Releasables.close(value);
            value = null;
        }
    }

    /**
     * A grouping state consisting of a single {@code long} value with a {@link TDigestHolder} per group.
     * The intermediate state contains three values in order: the long, the digest, and a boolean specifying if a value was set or not.
     */
    public static final class WithLongGroupingState implements GroupingAggregatorState {
        private LongArray longValues;
        private ObjectArray<BreakingTDigestHolder> values;
        private final CircuitBreaker breaker;
        private final BigArrays bigArrays;

        WithLongGroupingState(BigArrays bigArrays, CircuitBreaker breaker) {
            LongArray longValues = null;
            ObjectArray<BreakingTDigestHolder> values = null;
            boolean success = false;
            try {
                longValues = bigArrays.newLongArray(1);
                values = bigArrays.newObjectArray(1);
                success = true;
            } finally {
                if (success == false) {
                    Releasables.close(values, longValues);
                }
            }
            this.longValues = longValues;
            this.values = values;
            this.bigArrays = bigArrays;
            this.breaker = breaker;
        }

        public void set(int groupId, long longValue, TDigestHolder digestValue) {
            assert digestValue != null;
            ensureCapacity(groupId);
            BreakingTDigestHolder value = values.get(groupId);
            if (value == null) {
                value = BreakingTDigestHolder.create(breaker);
                values.set(groupId, value);
            }
            value.set(digestValue);
            longValues.set(groupId, longValue);
        }

        private void ensureCapacity(int groupId) {
            values = bigArrays.grow(values, groupId + 1);
            longValues = bigArrays.grow(longValues, groupId + 1);
        }

        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            assert blocks.length >= offset + 3;
            try (
                var longBuilder = driverContext.blockFactory().newLongVectorFixedBuilder(selected.getPositionCount());
                var valueBuilder = driverContext.blockFactory().newTDigestBlockBuilder(selected.getPositionCount());
                var seenBuilder = driverContext.blockFactory().newBooleanVectorFixedBuilder(selected.getPositionCount());
            ) {
                for (int i = 0; i < selected.getPositionCount(); i++) {
                    int groupId = selected.getInt(i);
                    if (seen(groupId)) {
                        seenBuilder.appendBoolean(true);
                        longBuilder.appendLong(longValues.get(groupId));
                        valueBuilder.appendTDigest(values.get(groupId).accessor());
                    } else {
                        seenBuilder.appendBoolean(false);
                        longBuilder.appendLong(0L);
                        valueBuilder.appendTDigest(TDigestHolder.empty());
                    }
                }
                blocks[offset] = longBuilder.build().asBlock();
                blocks[offset + 1] = valueBuilder.build();
                blocks[offset + 2] = seenBuilder.build().asBlock();
            }
        }

        public boolean seen(int groupId) {
            return groupId < values.size() && values.get(groupId) != null;
        }

        public long longValue(int groupId) {
            assert seen(groupId);
            return longValues.get(groupId);
        }

        @Override
        public void close() {
            for (int i = 0; i < values.size(); i++) {
                Releasables.close(values.get(i));
            }
            Releasables.close(values, longValues);
            values = null;
            longValues = null;
        }

        public Block evaluateFinalTDigests(IntVector selected, DriverContext driverContext) {
            try (var builder = driverContext.blockFactory().newTDigestBlockBuilder(selected.getPositionCount())) {
                for (int i = 0; i < selected.getPositionCount(); i++) {
                    int groupId = selected.getInt(i);
                    if (seen(groupId)) {
                        builder.appendTDigest(values.get(groupId).accessor());
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
