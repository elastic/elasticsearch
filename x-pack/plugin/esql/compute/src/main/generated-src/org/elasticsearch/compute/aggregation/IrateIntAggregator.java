/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

// begin generated imports
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
// end generated imports

/**
 * A rate grouping aggregation definition for int. This implementation supports the `irate` and `idelta` functions.
 * This class is generated. Edit `X-IrateAggregator.java.st` instead.
 */
@GroupingAggregator(
    value = { @IntermediateState(name = "timestamps", type = "LONG_BLOCK"), @IntermediateState(name = "values", type = "INT_BLOCK") }
)
public class IrateIntAggregator {
    public static IntIrateGroupingState initGrouping(DriverContext driverContext, boolean isDelta, boolean isDateNanos) {
        final int dateFactor = isDateNanos ? 1_000_000_000 : 1000;
        return new IntIrateGroupingState(driverContext.bigArrays(), driverContext.breaker(), isDelta, dateFactor);
    }

    public static void combine(IntIrateGroupingState current, int groupId, int value, long timestamp) {
        current.ensureCapacity(groupId);
        current.append(groupId, timestamp, value);
    }

    public static String describe() {
        return "instant change of ints";
    }

    public static void combineIntermediate(
        IntIrateGroupingState current,
        int groupId,
        LongBlock timestamps,
        IntBlock values,
        int otherPosition
    ) {
        current.combine(groupId, timestamps, values, otherPosition);
    }

    public static Block evaluateFinal(IntIrateGroupingState state, IntVector selected, GroupingAggregatorEvaluationContext evalContext) {
        return state.evaluateFinal(selected, evalContext);
    }

    private static class IntIrateState {
        static final long BASE_RAM_USAGE = RamUsageEstimator.sizeOfObject(IntIrateState.class);
        long lastTimestamp;
        long secondLastTimestamp = -1;
        int lastValue;
        int secondLastValue;
        boolean hasSecond;

        IntIrateState(long lastTimestamp, int lastValue) {
            this.lastTimestamp = lastTimestamp;
            this.lastValue = lastValue;
            this.hasSecond = false;
        }

        long bytesUsed() {
            return BASE_RAM_USAGE;
        }
    }

    public static final class IntIrateGroupingState implements Releasable, Accountable, GroupingAggregatorState {
        private ObjectArray<IntIrateState> states;
        private final BigArrays bigArrays;
        private final CircuitBreaker breaker;
        private long stateBytes; // for individual states
        private final boolean isDelta;
        private final int dateFactor;

        IntIrateGroupingState(BigArrays bigArrays, CircuitBreaker breaker, boolean isDelta, int dateFactor) {
            this.bigArrays = bigArrays;
            this.breaker = breaker;
            this.states = bigArrays.newObjectArray(1);
            this.isDelta = isDelta;
            this.dateFactor = dateFactor;
        }

        void ensureCapacity(int groupId) {
            states = bigArrays.grow(states, groupId + 1);
        }

        void adjustBreaker(long bytes) {
            breaker.addEstimateBytesAndMaybeBreak(bytes, "<<rate aggregation>>");
            stateBytes += bytes;
            assert stateBytes >= 0 : stateBytes;
        }

        void append(int groupId, long timestamp, int value) {
            var state = states.get(groupId);
            if (state == null) {
                state = new IntIrateState(timestamp, value);
                states.set(groupId, state);
                adjustBreaker(state.bytesUsed());
            } else {
                // We only need the last two values, but we need to keep them sorted by timestamp.
                if (timestamp > state.lastTimestamp) {
                    // new timestamp is the most recent
                    state.secondLastTimestamp = state.lastTimestamp;
                    state.secondLastValue = state.lastValue;
                    state.lastTimestamp = timestamp;
                    state.lastValue = value;
                    state.hasSecond = true;
                } else if (timestamp > state.secondLastTimestamp) {
                    // new timestamp is the second most recent
                    state.secondLastTimestamp = timestamp;
                    state.secondLastValue = value;
                    state.hasSecond = true;
                } // else: ignore, too old
            }
        }

        void combine(int groupId, LongBlock timestamps, IntBlock values, int otherPosition) {
            final int valueCount = timestamps.getValueCount(otherPosition);
            if (valueCount == 0) {
                return;
            }
            final int firstTs = timestamps.getFirstValueIndex(otherPosition);
            final int firstIndex = values.getFirstValueIndex(otherPosition);
            ensureCapacity(groupId);
            append(groupId, timestamps.getLong(firstTs), values.getInt(firstIndex));
            if (valueCount > 1) {
                ensureCapacity(groupId);
                append(groupId, timestamps.getLong(firstTs + 1), values.getInt(firstIndex + 1));
            }
        }

        @Override
        public long ramBytesUsed() {
            return states.ramBytesUsed() + stateBytes;
        }

        @Override
        public void close() {
            Releasables.close(states, () -> adjustBreaker(-stateBytes));
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            assert blocks.length >= offset + 2 : "blocks=" + blocks.length + ",offset=" + offset;
            final BlockFactory blockFactory = driverContext.blockFactory();
            final int positionCount = selected.getPositionCount();
            try (
                LongBlock.Builder timestamps = blockFactory.newLongBlockBuilder(positionCount * 2);
                IntBlock.Builder values = blockFactory.newIntBlockBuilder(positionCount * 2);
            ) {
                for (int i = 0; i < positionCount; i++) {
                    final var groupId = selected.getInt(i);
                    final var state = groupId < states.size() ? states.get(groupId) : null;
                    if (state != null) {
                        timestamps.beginPositionEntry();
                        timestamps.appendLong(state.lastTimestamp);
                        if (state.hasSecond) {
                            timestamps.appendLong(state.secondLastTimestamp);
                        }
                        timestamps.endPositionEntry();

                        values.beginPositionEntry();
                        values.appendInt(state.lastValue);
                        if (state.hasSecond) {
                            values.appendInt(state.secondLastValue);
                        }
                        values.endPositionEntry();
                    } else {
                        timestamps.appendNull();
                        values.appendNull();
                    }
                }
                blocks[offset] = timestamps.build();
                blocks[offset + 1] = values.build();
            }
        }

        Block evaluateFinal(IntVector selected, GroupingAggregatorEvaluationContext evalContext) {
            int positionCount = selected.getPositionCount();
            try (DoubleBlock.Builder rates = evalContext.blockFactory().newDoubleBlockBuilder(positionCount)) {
                for (int p = 0; p < positionCount; p++) {
                    final var groupId = selected.getInt(p);
                    final var state = groupId < states.size() ? states.get(groupId) : null;
                    if (state == null || state.hasSecond == false) {
                        rates.appendNull();
                        continue;
                    }
                    if (isDelta) {
                        // delta: just return the difference
                        rates.appendDouble(state.lastValue - state.secondLastValue);
                    } else {
                        // When the last value is less than the previous one, we assume a reset
                        // and use the last value directly.
                        final double ydiff = state.lastValue >= state.secondLastValue
                            ? state.lastValue - state.secondLastValue
                            : state.lastValue;
                        final long xdiff = state.lastTimestamp - state.secondLastTimestamp;
                        rates.appendDouble(ydiff / xdiff * dateFactor);
                    }
                }
                return rates.build();
            }
        }

        @Override
        public void enableGroupIdTracking(SeenGroupIds seenGroupIds) {
            // noop - we handle the null states inside `toIntermediate` and `evaluateFinal`
        }
    }
}
