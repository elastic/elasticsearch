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
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
// end generated imports

/**
 * A rate grouping aggregation definition for int. This implementation supports the `Delta` and `idelta` functions.
 * This class is generated. Edit `X-DeltaAggregator.java.st` instead.
 */
@GroupingAggregator(
    value = {
        @IntermediateState(name = "samples", type = "LONG_BLOCK"),
        @IntermediateState(name = "timestamps", type = "LONG_BLOCK"),
        @IntermediateState(name = "values", type = "INT_BLOCK") }
)
public class DeltaIntAggregator {
    public static IntDeltaGroupingState initGrouping(DriverContext driverContext) {
        return new IntDeltaGroupingState(driverContext.bigArrays(), driverContext.breaker());
    }

    public static void combine(IntDeltaGroupingState current, int groupId, int value, long timestamp) {
        current.ensureCapacity(groupId);
        current.append(groupId, timestamp, value);
    }

    public static void combineIntermediate(
        IntDeltaGroupingState current,
        int groupId,
        LongBlock samples,
        LongBlock timestamps,
        IntBlock values,
        int otherPosition
    ) {
        current.combine(groupId, samples, timestamps, values, otherPosition);
    }

    public static Block evaluateFinal(IntDeltaGroupingState state, IntVector selected, GroupingAggregatorEvaluationContext evalContext) {
        return state.evaluateFinal(selected, evalContext);
    }

    private static class IntDeltaState {
        static final long BASE_RAM_USAGE = RamUsageEstimator.sizeOfObject(IntDeltaState.class);
        long lastTimestamp = -1;
        long firstTimestamp = Long.MAX_VALUE;
        int lastValue;
        int firstValue;
        long valuesSeen;

        IntDeltaState(long seenTs, int seenValue) {
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

    public static final class IntDeltaGroupingState implements Releasable, Accountable, GroupingAggregatorState {
        private ObjectArray<IntDeltaState> states;
        private final BigArrays bigArrays;
        private final CircuitBreaker breaker;
        private long stateBytes; // for individual states

        IntDeltaGroupingState(BigArrays bigArrays, CircuitBreaker breaker) {
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

        void append(int groupId, long timestamp, int value) {
            var state = states.get(groupId);
            if (state == null) {
                state = new IntDeltaState(timestamp, value);
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

        void combine(int groupId, LongBlock samples, LongBlock timestamps, IntBlock values, int otherPosition) {
            final int valueCount = timestamps.getValueCount(otherPosition);
            if (valueCount == 0) {
                return;
            }
            final long valuesSeen = samples.getLong(samples.getFirstValueIndex(otherPosition));
            final int firstTs = timestamps.getFirstValueIndex(otherPosition);
            final int firstIndex = values.getFirstValueIndex(otherPosition);
            ensureCapacity(groupId);
            append(groupId, timestamps.getLong(firstTs), values.getInt(firstIndex));
            if (valueCount > 1) {
                append(groupId, timestamps.getLong(firstTs + 1), values.getInt(firstIndex + 1));
            }
            // We are merging the state from upstream, which means we have seen
            // `valuesSeen` values, but we have already counted one or two of them,
            // which is represented by `valueCount - 1`.
            states.get(groupId).valuesSeen += valuesSeen - valueCount;
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
                LongVector.FixedBuilder samples = blockFactory.newLongVectorFixedBuilder(positionCount);
                LongBlock.Builder timestamps = blockFactory.newLongBlockBuilder(positionCount * 2);
                IntBlock.Builder values = blockFactory.newIntBlockBuilder(positionCount * 2);
            ) {
                for (int i = 0; i < positionCount; i++) {
                    final var groupId = selected.getInt(i);
                    final var state = groupId < states.size() ? states.get(groupId) : null;
                    if (state != null) {
                        samples.appendLong(state.valuesSeen);
                        timestamps.beginPositionEntry();
                        timestamps.appendLong(state.lastTimestamp);
                        if (state.valuesSeen > 1) {
                            timestamps.appendLong(state.firstTimestamp);
                        }
                        timestamps.endPositionEntry();

                        values.beginPositionEntry();
                        values.appendInt(state.lastValue);
                        if (state.valuesSeen > 1) {
                            values.appendInt(state.firstValue);
                        }
                        values.endPositionEntry();
                    } else {
                        samples.appendLong(0L);
                        timestamps.appendNull();
                        values.appendNull();
                    }
                }
                blocks[offset] = samples.build().asBlock();
                blocks[offset + 1] = timestamps.build();
                blocks[offset + 2] = values.build();
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
                    if (evalContext instanceof TimeSeriesGroupingAggregatorEvaluationContext tsContext) {
                        // At this point we want to apply extrapolation
                        var rangeStart = tsContext.rangeStartInMillis(groupId);
                        var rangeEnd = tsContext.rangeEndInMillis(groupId);
                        if (state.lastTimestamp - state.firstTimestamp == 0) {
                            rates.appendNull();
                            continue;
                        }
                        double startGap = state.firstTimestamp - rangeStart;
                        final double averageSampleInterval = ((double) state.lastTimestamp - state.firstTimestamp) / state.valuesSeen;
                        final double slope = (state.lastValue - state.firstValue) / (state.lastTimestamp - state.firstTimestamp);
                        double endGap = rangeEnd - state.lastTimestamp;
                        double calculatedFirstValue = state.firstValue;
                        if (startGap > 0) {
                            if (startGap > averageSampleInterval * 1.1) {
                                startGap = averageSampleInterval / 2.0;
                            }
                            calculatedFirstValue = calculatedFirstValue - startGap * slope;
                        }
                        double calculatedLastValue = state.lastValue;
                        if (endGap > 0) {
                            if (endGap > averageSampleInterval * 1.1) {
                                endGap = averageSampleInterval / 2.0;
                            }
                            calculatedLastValue = calculatedLastValue + endGap * slope;
                        }
                        rates.appendDouble(calculatedLastValue - calculatedFirstValue);
                    } else {
                        rates.appendDouble(state.lastValue - state.firstValue);
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
