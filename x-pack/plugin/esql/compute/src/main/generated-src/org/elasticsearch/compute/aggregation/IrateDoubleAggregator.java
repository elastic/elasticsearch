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
 * A rate grouping aggregation definition for double.
 * This class is generated. Edit `X-IrateAggregator.java.st` instead.
 */
@GroupingAggregator(
    value = { @IntermediateState(name = "timestamps", type = "LONG_BLOCK"), @IntermediateState(name = "values", type = "DOUBLE_BLOCK") }
)
public class IrateDoubleAggregator {
    public static DoubleIrateGroupingState initGrouping(DriverContext driverContext) {
        return new DoubleIrateGroupingState(driverContext.bigArrays(), driverContext.breaker());
    }

    public static void combine(DoubleIrateGroupingState current, int groupId, double value, long timestamp) {
        current.ensureCapacity(groupId);
        DoubleIrateGroupingState.append(current.states, groupId, timestamp, value);
    }

    public static void combineIntermediate(
        DoubleIrateGroupingState current,
        int groupId,
        LongBlock timestamps,
        DoubleBlock values,
        int otherPosition
    ) {
        current.combine(groupId, timestamps, values, otherPosition);
    }

    public static Block evaluateFinal(DoubleIrateGroupingState state, IntVector selected, GroupingAggregatorEvaluationContext evalContext) {
        return state.evaluateFinal(selected, evalContext);
    }

    private static class DoubleIrateState {
        static final long BASE_RAM_USAGE = RamUsageEstimator.sizeOfObject(DoubleIrateState.class);
        long lastTimestamp;
        long secondLastTimestamp;
        double lastValue;
        double secondLastValue;
        boolean hasSecond;

        DoubleIrateState() {
            hasSecond = false;
        }

        DoubleIrateState(long lastTimestamp, double lastValue) {
            this.lastTimestamp = lastTimestamp;
            this.lastValue = lastValue;
            this.hasSecond = false;
        }

        static long bytesUsed() {
            return BASE_RAM_USAGE;
        }
    }

    public static final class DoubleIrateGroupingState implements Releasable, Accountable, GroupingAggregatorState {
        private ObjectArray<DoubleIrateState> states;
        private final BigArrays bigArrays;
        private final CircuitBreaker breaker;
        private long stateBytes; // for individual states

        DoubleIrateGroupingState(BigArrays bigArrays, CircuitBreaker breaker) {
            this.bigArrays = bigArrays;
            this.breaker = breaker;
            this.states = bigArrays.newObjectArray(1);
        }

        void ensureCapacity(int groupId) {
            states = bigArrays.grow(states, groupId + 1);
        }

        void adjustBreaker(long bytes) {
            breaker.addEstimateBytesAndMaybeBreak(bytes, "<<rate aggregation>>");
            stateBytes += bytes;
            assert stateBytes >= 0 : stateBytes;
        }

        static Long append(ObjectArray<DoubleIrateState> states, int groupId, long timestamp, double value) {
            var state = states.get(groupId);
            if (state == null) {
                state = new DoubleIrateState(timestamp, value);
                states.set(groupId, state);
                return DoubleIrateState.bytesUsed();
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
                return 0L;
            }
        }

        void combine(int groupId, LongBlock timestamps, DoubleBlock values, int otherPosition) {
            final int valueCount = timestamps.getValueCount(otherPosition);
            if (valueCount == 0) {
                return;
            }
            final int firstIndex = timestamps.getFirstValueIndex(otherPosition);
            ensureCapacity(groupId);
            var incr = append(states, groupId, timestamps.getLong(firstIndex), values.getDouble(firstIndex));
            adjustBreaker(incr);
            if (valueCount > 1) {
                ensureCapacity(groupId);
                incr = append(states, groupId, timestamps.getLong(firstIndex + 1), values.getDouble(firstIndex + 1));
                adjustBreaker(incr);
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
                DoubleBlock.Builder values = blockFactory.newDoubleBlockBuilder(positionCount * 2);
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
                        values.appendDouble(state.lastValue);
                        if (state.hasSecond) {
                            values.appendDouble(state.secondLastValue);
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
                    // When the last value is less than the previous one, we assume a reset
                    // and use the last value directly.
                    final double ydiff = state.lastValue >= state.secondLastValue
                        ? state.lastValue - state.secondLastValue
                        : state.lastValue;
                    final long xdiff = state.lastTimestamp - state.secondLastTimestamp;
                    rates.appendDouble(ydiff / xdiff * 1000);
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
