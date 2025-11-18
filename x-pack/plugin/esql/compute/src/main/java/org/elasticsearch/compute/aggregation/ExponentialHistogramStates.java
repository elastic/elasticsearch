/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramCircuitBreaker;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramMerger;

public final class ExponentialHistogramStates {

    // We currently use a hardcoded limit for the number of buckets, we might make this configurable / an aggregation parameter later
    // The current default is what's also used by the OpenTelemetry SDKs
    public static final int MAX_BUCKET_COUNT = 320;

    private record HistoBreaker(CircuitBreaker delegate) implements ExponentialHistogramCircuitBreaker {
        @Override
        public void adjustBreaker(long bytesAllocated) {
            if (bytesAllocated < 0) {
                delegate.addWithoutBreaking(bytesAllocated);
            } else {
                delegate.addEstimateBytesAndMaybeBreak(bytesAllocated, "ExponentialHistogram aggregation state");
            }
        }
    }

    private ExponentialHistogramStates() {}

    static final class SingleState implements AggregatorState {

        private final CircuitBreaker breaker;
        // initialize lazily
        private ExponentialHistogramMerger merger;

        SingleState(CircuitBreaker breaker) {
            this.breaker = breaker;
        }

        public void add(ExponentialHistogram histogram, boolean allowUpscale) {
            if (histogram == null) {
                return;
            }
            if (merger == null) {
                merger = ExponentialHistogramMerger.create(MAX_BUCKET_COUNT, new HistoBreaker(breaker));
            }
            if (allowUpscale) {
                merger.add(histogram);
            } else {
                merger.addWithoutUpscaling(histogram);
            }
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            assert blocks.length >= offset + 1;
            blocks[offset] = evaluateFinal(driverContext);
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
                return blockFactory.newConstantExponentialHistogramBlock(merger.get(), 1);
            }
        }
    }

    static final class GroupingState implements GroupingAggregatorState {

        private ObjectArray<ExponentialHistogramMerger> states;
        private final HistoBreaker breaker;
        private final BigArrays bigArrays;

        GroupingState(BigArrays bigArrays, CircuitBreaker breaker) {
            this.states = bigArrays.newObjectArray(1);
            this.bigArrays = bigArrays;
            this.breaker = new HistoBreaker(breaker);
        }

        ExponentialHistogramMerger getOrNull(int position) {
            if (position < states.size()) {
                return states.get(position);
            } else {
                return null;
            }
        }

        public void add(int groupId, ExponentialHistogram histogram, boolean allowUpscale) {
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
            assert blocks.length >= offset + 1 : "blocks=" + blocks.length + ",offset=" + offset;
            blocks[offset] = evaluateFinal(selected, driverContext);
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
}
