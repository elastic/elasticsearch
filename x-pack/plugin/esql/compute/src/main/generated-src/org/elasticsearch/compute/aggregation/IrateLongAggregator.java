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
 * A rate grouping aggregation definition for long.
 * This class is generated. Edit `X-RateAggregator.java.st` instead.
 */
@GroupingAggregator(
    value = { @IntermediateState(name = "timestamps", type = "LONG_BLOCK"), @IntermediateState(name = "values", type = "LONG_BLOCK") }
)
public class IrateLongAggregator {

    public static LongIrateGroupingState initGrouping(DriverContext driverContext) {
        return new LongIrateGroupingState(driverContext.bigArrays(), driverContext.breaker());
    }

    public static void combine(LongIrateGroupingState current, int groupId, long value, long timestamp) {
        current.append(groupId, timestamp, value);
    }

    public static void combineIntermediate(
        LongIrateGroupingState current,
        int groupId,
        LongBlock timestamps,
        LongBlock values,
        int otherPosition
    ) {
        current.combine(groupId, timestamps, values, otherPosition);
    }

    public static Block evaluateFinal(LongIrateGroupingState state, IntVector selected, GroupingAggregatorEvaluationContext evalContext) {
        return state.evaluateFinal(selected, evalContext);
    }

    private static class LongIrateState {
        static final long BASE_RAM_USAGE = RamUsageEstimator.sizeOfObject(LongIrateState.class);
        final long[] timestamps; // descending order
        final long[] values;

        LongIrateState(int initialSize) {
            this.timestamps = new long[initialSize];
            this.values = new long[initialSize];
        }

        LongIrateState(long[] ts, long[] vs) {
            this.timestamps = ts;
            this.values = vs;
        }

        void append(long t, long v) {
            assert timestamps.length == 2 : "expected two timestamps; got " + timestamps.length;
            assert t < timestamps[1] : "@timestamp goes backward: " + t + " >= " + timestamps[1];
            // This method does not need to do anything because we only need the last two values
            // and timestamps, which are already in place.
        }

        int entries() {
            return timestamps.length;
        }

        static long bytesUsed(int entries) {
            var ts = RamUsageEstimator.alignObjectSize(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) Long.BYTES * entries);
            var vs = RamUsageEstimator.alignObjectSize(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) Long.BYTES * entries);
            return BASE_RAM_USAGE + ts + vs;
        }
    }

    public static final class LongIrateGroupingState implements Releasable, Accountable, GroupingAggregatorState {
        private ObjectArray<LongIrateState> states;
        private final BigArrays bigArrays;
        private final CircuitBreaker breaker;
        private long stateBytes; // for individual states

        LongIrateGroupingState(BigArrays bigArrays, CircuitBreaker breaker) {
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

        void append(int groupId, long timestamp, long value) {
            ensureCapacity(groupId);
            var state = states.get(groupId);
            if (state == null) {
                adjustBreaker(LongIrateState.bytesUsed(1));
                state = new LongIrateState(new long[] { timestamp }, new long[] { value });
                states.set(groupId, state);
            } else {
                if (state.entries() == 1) {
                    adjustBreaker(LongIrateState.bytesUsed(2));
                    state = new LongIrateState(new long[] { state.timestamps[0], timestamp }, new long[] { state.values[0], value });
                    states.set(groupId, state);
                    adjustBreaker(-LongIrateState.bytesUsed(1)); // old state
                }
            }
        }

        void combine(int groupId, LongBlock timestamps, LongBlock values, int otherPosition) {
            // TODO: Check this method pabloem
            final int valueCount = timestamps.getValueCount(otherPosition);
            if (valueCount == 0) {
                return;
            }
            final int firstIndex = timestamps.getFirstValueIndex(otherPosition);
            ensureCapacity(groupId);
            var state = states.get(groupId);
            if (state == null) {
                adjustBreaker(LongIrateState.bytesUsed(valueCount));
                state = new LongIrateState(valueCount);
                states.set(groupId, state);
                // TODO: add bulk_copy to Block
                for (int i = 0; i < valueCount; i++) {
                    state.timestamps[i] = timestamps.getLong(firstIndex + i);
                    state.values[i] = values.getLong(firstIndex + i);
                }
            } else {
                adjustBreaker(LongIrateState.bytesUsed(state.entries() + valueCount));
                var newState = new LongIrateState(state.entries() + valueCount);
                states.set(groupId, newState);
                merge(state, newState, firstIndex, valueCount, timestamps, values);
                adjustBreaker(-LongIrateState.bytesUsed(state.entries())); // old state
            }
        }

        void merge(LongIrateState curr, LongIrateState dst, int firstIndex, int rightCount, LongBlock timestamps, LongBlock values) {
            int i = 0, j = 0, k = 0;
            final int leftCount = curr.entries();
            // We do not merge more than two entries because we only need the last two.
            // This merge thus ends when we have two entries in dst.
            while (i < leftCount && j < rightCount && k < 2) {
                final var t1 = curr.timestamps[i];
                final var t2 = timestamps.getLong(firstIndex + j);
                if (t1 > t2) {
                    dst.timestamps[k] = t1;
                    dst.values[k] = curr.values[i];
                    ++i;
                } else {
                    dst.timestamps[k] = t2;
                    dst.values[k] = values.getLong(firstIndex + j);
                    ++j;
                }
                ++k;
            }
        }

        LongIrateState mergeState(LongIrateState s1, LongIrateState s2) {
            adjustBreaker(LongIrateState.bytesUsed(2));
            var dst = new LongIrateState(2);
            int i = 0, j = 0, k = 0;
            while (i < s1.entries() && j < s2.entries() && k < 2) {
                if (s1.timestamps[i] > s2.timestamps[j]) {
                    dst.timestamps[k] = s1.timestamps[i];
                    dst.values[k] = s1.values[i];
                    ++i;
                } else {
                    dst.timestamps[k] = s2.timestamps[j];
                    dst.values[k] = s2.values[j];
                    ++j;
                }
                ++k;
            }
            return dst;
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
            assert blocks.length >= offset + 3 : "blocks=" + blocks.length + ",offset=" + offset;
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
                        for (long t : state.timestamps) {
                            timestamps.appendLong(t);
                        }
                        timestamps.endPositionEntry();

                        values.beginPositionEntry();
                        for (long v : state.values) {
                            values.appendLong(v);
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
                    if (state == null || state.values.length < 2) {
                        rates.appendNull();
                        continue;
                    }
                    int len = state.entries();
                    final double ydiff = state.values[0] > state.values[1]
                        ? state.values[0] - state.values[1]
                        : state.values[1] - state.values[0];
                    final long xdiff = state.timestamps[0] - state.timestamps[1];
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
