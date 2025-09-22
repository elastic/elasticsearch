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
 * A rate grouping aggregation definition for long. This implementation supports the `Delta` and `idelta` functions.
 * This class is generated. Edit `X-DeltaAggregator.java.st` instead.
 */
@GroupingAggregator(
    value = { @IntermediateState(name = "timestamps", type = "LONG_BLOCK"), @IntermediateState(name = "values", type = "LONG_BLOCK") }
)
public class DeltaLongAggregator {
    public static LongDeltaGroupingState initGrouping(DriverContext driverContext) {
        return new LongDeltaGroupingState(driverContext.bigArrays(), driverContext.breaker());
    }

    public static void combine(LongDeltaGroupingState current, int groupId, long value, long timestamp) {
        current.ensureCapacity(groupId);
        current.append(groupId, timestamp, value);
    }

    public static void combineIntermediate(
        LongDeltaGroupingState current,
        int groupId,
        LongBlock timestamps,
        LongBlock values,
        int otherPosition
    ) {
        current.combine(groupId, timestamps, values, otherPosition);
    }

    public static Block evaluateFinal(LongDeltaGroupingState state, IntVector selected, GroupingAggregatorEvaluationContext evalContext) {
        return state.evaluateFinal(selected, evalContext);
    }

    private static class LongDeltaState {
        static final long BASE_RAM_USAGE = RamUsageEstimator.sizeOfObject(LongDeltaState.class);
        long lastTimestamp = -1;
        long firstTimestamp = Long.MAX_VALUE;
        long lastValue;
        long firstValue;
        long valuesSeen;

        LongDeltaState(long seenTs, long seenValue) {
            this.lastTimestamp = seenTs;
            this.lastValue = seenValue;
            this.firstTimestamp = seenTs;
            this.firstValue = seenValue;
            this.valuesSeen = 1L;
        }

        long bytesUsed() {
            return BASE_RAM_USAGE;
        }
    }

    public static final class LongDeltaGroupingState implements Releasable, Accountable, GroupingAggregatorState {
        private ObjectArray<LongDeltaState> states;
        private final BigArrays bigArrays;
        private final CircuitBreaker breaker;
        private long stateBytes; // for individual states

        LongDeltaGroupingState(BigArrays bigArrays, CircuitBreaker breaker) {
            this.bigArrays = bigArrays;
            this.breaker = breaker;
            this.states = bigArrays.newObjectArray(1);
        }

        void ensureCapacity(int groupId) {
            states = bigArrays.grow(states, groupId + 1);
        }

        void adjustBreaker(long bytes) {
            breaker.addEstimateBytesAndMaybeBreak(bytes, "<<delta aggregation>>");
            stateBytes += bytes;
            assert stateBytes >= 0 : stateBytes;
        }

        void append(int groupId, long timestamp, long value) {
            var state = states.get(groupId);
            if (state == null) {
                state = new LongDeltaState(timestamp, value);
                states.set(groupId, state);
                adjustBreaker(state.bytesUsed());
            } else {
                if (timestamp >= state.lastTimestamp) {
                    state.lastTimestamp = timestamp;
                    state.lastValue = value;
                    state.valuesSeen++;
                } else if (timestamp <= state.firstTimestamp) {
                    state.firstTimestamp = timestamp;
                    state.firstValue = value;
                    state.valuesSeen++;
                } // else: ignore, too old
            }
        }

        void combine(int groupId, LongBlock timestamps, LongBlock values, int otherPosition) {
            final int valueCount = timestamps.getValueCount(otherPosition);
            if (valueCount == 0) {
                return;
            }
            final int valuesSeenIdx = timestamps.getFirstValueIndex(otherPosition);
            final int firstTs = valuesSeenIdx + 1;
            final int firstIndex = values.getFirstValueIndex(otherPosition);
            final long valuesSeen = timestamps.getLong(valuesSeenIdx);
            ensureCapacity(groupId);
            append(groupId, timestamps.getLong(firstTs), values.getLong(firstIndex));
            if (valueCount > 2) {
                ensureCapacity(groupId);
                append(groupId, timestamps.getLong(firstTs + 1), values.getLong(firstIndex + 1));
            }
            states.get(groupId).valuesSeen = valuesSeen;
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
                LongBlock.Builder values = blockFactory.newLongBlockBuilder(positionCount * 2);
            ) {
                for (int i = 0; i < positionCount; i++) {
                    final var groupId = selected.getInt(i);
                    final var state = groupId < states.size() ? states.get(groupId) : null;
                    if (state != null) {
                        timestamps.beginPositionEntry();
                        // We store the count of values seen in the first position
                        // for timestamps, so that we can reconstruct the state.
                        timestamps.appendLong(state.valuesSeen);
                        timestamps.appendLong(state.lastTimestamp);
                        if (state.valuesSeen > 1) {
                            timestamps.appendLong(state.firstTimestamp);
                        }
                        timestamps.endPositionEntry();

                        values.beginPositionEntry();
                        values.appendLong(state.lastValue);
                        if (state.valuesSeen > 1) {
                            values.appendLong(state.firstValue);
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
                    if (state == null || state.valuesSeen < 2) {
                        rates.appendNull();
                        continue;
                    }
                    rates.appendDouble(state.lastValue - state.firstValue);
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
