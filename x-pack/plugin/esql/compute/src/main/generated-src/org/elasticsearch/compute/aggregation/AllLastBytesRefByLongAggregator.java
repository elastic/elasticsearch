/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

// begin generated imports
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.ann.Position;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;

import java.util.BitSet;
// end generated imports

/**
 * A time-series aggregation function that collects the Last occurrence value of a time series in a specified interval.
 * This class is generated. Edit `X-AllValueByTimestampAggregator.java.st` instead.
 */
@Aggregator(
    processNulls = true,
    value = {
        @IntermediateState(name = "observed", type = "BOOLEAN"),
        @IntermediateState(name = "timestampPresent", type = "BOOLEAN"),
        @IntermediateState(name = "timestamp", type = "LONG"),
        @IntermediateState(name = "values", type = "BYTES_REF_BLOCK") }
)
@GroupingAggregator(
    processNulls = true,
    value = {
        @IntermediateState(name = "observed", type = "BOOLEAN_BLOCK"),
        @IntermediateState(name = "timestampsPresent", type = "BOOLEAN_BLOCK"),
        @IntermediateState(name = "timestamps", type = "LONG_BLOCK"),
        @IntermediateState(name = "values", type = "BYTES_REF_BLOCK") }
)
public class AllLastBytesRefByLongAggregator {
    public static String describe() {
        return "all_last_bytesref_by_long";
    }

    public static AllLongBytesRefState initSingle(DriverContext driverContext) {
        return new AllLongBytesRefState(driverContext.bigArrays());
    }

    private static void overrideState(
        AllLongBytesRefState current,
        boolean timestampPresent,
        long timestamp,
        BytesRefBlock values,
        int position
    ) {
        current.observed(true);
        current.v1(timestampPresent ? timestamp : -1L);
        current.v1Seen(timestampPresent);
        if (values.isNull(position)) {
            Releasables.close(current.v2());
            current.v2(null);
        } else {
            int count = values.getValueCount(position);
            int offset = values.getFirstValueIndex(position);
            BytesRefArray a = null;
            boolean success = false;
            try {
                a = new BytesRefArray(0, current.bigArrays());
                for (int i = 0; i < count; ++i) {
                    BytesRef bytesScratch = new BytesRef();
                    values.getBytesRef(offset + i, bytesScratch);
                    a.append(bytesScratch);
                }
                success = true;
                Releasables.close(current.v2());
                current.v2(a);
            } finally {
                if (success == false) {
                    Releasables.close(a);
                }
            }
        }
    }

    private static long dominantTimestampAtPosition(int position, LongBlock timestamps) {
        assert timestamps.isNull(position) == false : "The timestamp is null at this position";
        int lo = timestamps.getFirstValueIndex(position);
        int hi = lo + timestamps.getValueCount(position);
        long result = timestamps.getLong(lo++);

        for (int i = lo; i < hi; i++) {
            result = Math.max(result, timestamps.getLong(i));
        }

        return result;
    }

    public static void combine(AllLongBytesRefState current, @Position int position, BytesRefBlock values, LongBlock timestamps) {
        long timestamp = timestamps.isNull(position) ? -1 : dominantTimestampAtPosition(position, timestamps);
        boolean timestampPresent = timestamps.isNull(position) == false;

        if (current.observed() == false) {
            // We never saw a timestamp before, regardless of nullability.
            overrideState(current, timestampPresent, timestamp, values, position);
        } else if (timestampPresent && (current.v1Seen() == false || timestamp > current.v1())) {
            // The incoming timestamp wins against the current one because the latter was either null or older/newer.
            overrideState(current, true, timestamp, values, position);
        }
    }

    public static void combineIntermediate(
        AllLongBytesRefState current,
        boolean observed,
        boolean timestampPresent,
        long timestamp,
        BytesRefBlock values
    ) {
        if (observed == false) {
            // The incoming state hasn't observed anything. No work is needed.
            return;
        }

        if (current.observed()) {
            // Both the incoming shard and the current shard observed a value, so we must compare timestamps.
            if (current.v1Seen() == false && timestampPresent == false) {
                // Both observations have null timestamps. No work is needed.
                return;
            }
            if (timestampPresent && (current.v1Seen() == false || timestamp > current.v1())) {
                overrideState(current, timestampPresent, timestamp, values, 0);
            }
        } else {
            // The incoming state has observed a value, but we didn't. So we must update.
            overrideState(current, timestampPresent, timestamp, values, 0);
        }
    }

    public static Block evaluateFinal(AllLongBytesRefState current, DriverContext ctx) {
        return current.valuesBlock(ctx);
    }

    public static GroupingState initGrouping(DriverContext driverContext) {
        return new GroupingState(driverContext.bigArrays(), driverContext.breaker());
    }

    public static void combine(GroupingState current, int group, @Position int position, BytesRefBlock values, LongBlock timestamps) {
        long timestamp = timestamps.isNull(position) ? 0L : dominantTimestampAtPosition(position, timestamps);
        current.collectValue(group, timestamps.isNull(position) == false, timestamp, position, values);
    }

    public static void combineIntermediate(
        GroupingState current,
        int group,
        BooleanBlock observed,
        BooleanBlock timestampPresent,
        LongBlock timestamps,
        BytesRefBlock values,
        int otherPosition
    ) {
        if (group < observed.getPositionCount() && observed.getBoolean(observed.getFirstValueIndex(otherPosition)) == false) {
            // The incoming state hasn't observed anything for this particular group. No work is needed.
            return;
        }
        long timestamp = timestamps.isNull(otherPosition) ? -1 : timestamps.getLong(timestamps.getFirstValueIndex(otherPosition));
        boolean hasTimestamp = timestampPresent.getBoolean(timestampPresent.getFirstValueIndex(otherPosition));
        current.collectValue(group, hasTimestamp, timestamp, otherPosition, values);
    }

    public static Block evaluateFinal(GroupingState state, IntVector selected, GroupingAggregatorEvaluationContext ctx) {
        return state.evaluateFinal(selected, ctx);
    }

    public static final class GroupingState extends AbstractAllByLongGroupingState {

        /**
         * First values, stored in a dense array to minimize per-group overhead.
         */
        private ObjectArray<BreakingBytesRefBuilder> firstValues;
        private final CircuitBreaker breaker;
        /**
         * The second-and-beyond values. Null for groups with zero or one value.
         */
        private ObjectArray<BytesRefArray> tailValues;

        private int maxGroupId = -1;

        GroupingState(BigArrays bigArrays, CircuitBreaker breaker) {
            super(bigArrays);
            this.breaker = breaker;
            boolean success = false;
            try {
                this.firstValues = bigArrays.newObjectArray(1);
                this.firstValues.set(0, null);
                success = true;
            } finally {
                if (success == false) {
                    Releasables.close(firstValues, super::close);
                }
            }
        }

        void collectValue(int group, boolean timestampPresent, long timestamp, int position, BytesRefBlock valuesBlock) {
            if (group <= maxGroupId) {
                if (hasValue(group) == false // We never saw this group before, even if it's within bounds.
                    || (nullKey(group) && timestampPresent) // Or, the incoming non-null timestamp wins against the null one in the state.
                    || (timestampPresent && timestamp > key(group)) // Or, we found a better timestamp for this group.
                ) {
                    updateValue(group, timestampPresent, timestamp, valuesBlock, position);
                }
            } else {
                // We must grow all arrays to accommodate this group id
                grow(group);
                maxGroupId = group;
                updateValue(group, timestampPresent, timestamp, valuesBlock, position);
            }
            trackGroupId(group);
        }

        private void updateValue(int group, boolean timestampPresent, long timestamp, BytesRefBlock valuesBlock, int position) {
            if (valuesBlock.isNull(position)) {
                markNullValue(group);
            } else {
                clearNullValue(group);
            }
            if (timestampPresent == false) {
                markNullKey(group);
            } else {
                clearNullKey(group);
            }
            key(group, timestamp);
            if (valuesBlock.isNull(position) == false) {
                int count = valuesBlock.getValueCount(position);
                int offset = valuesBlock.getFirstValueIndex(position);
                BytesRef firstScratch = new BytesRef();
                valuesBlock.getBytesRef(offset, firstScratch);
                BreakingBytesRefBuilder firstBuilder = firstValues.get(group);
                if (firstBuilder == null) {
                    firstBuilder = new BreakingBytesRefBuilder(breaker, "Last", firstScratch.length);
                    firstValues.set(group, firstBuilder);
                }
                firstBuilder.copyBytes(firstScratch);
                if (count > 1) {
                    BytesRefArray tail = getTailForWriting(group, count - 1);
                    BytesRef scratch = new BytesRef();
                    for (int i = 1; i < count; ++i) {
                        tail.append(valuesBlock.getBytesRef(offset + i, scratch));
                    }
                } else {
                    clearTailValues(group);
                }
            } else {
                clearTailValues(group);
            }
        }

        @Override
        protected void grow(int group) {
            super.grow(group);
            firstValues = bigArrays.grow(firstValues, group + 1);
        }

        @Override
        public void close() {
            for (long i = 0; i < firstValues.size(); i++) {
                Releasables.close(firstValues.get(i));
            }
            if (tailValues != null) {
                for (long i = 0; i < tailValues.size(); i++) {
                    Releasables.close(tailValues.get(i));
                }
            }
            Releasables.close(firstValues, tailValues, super::close);
        }

        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            try (
                var observedBlockBuilder = driverContext.blockFactory().newBooleanBlockBuilder(selected.getPositionCount());
                var hasTimestampBuilder = driverContext.blockFactory().newBooleanBlockBuilder(selected.getPositionCount());
                var timestampsBuilder = driverContext.blockFactory().newLongBlockBuilder(selected.getPositionCount())
            ) {
                for (int p = 0; p < selected.getPositionCount(); p++) {
                    int group = selected.getInt(p);
                    if (group <= maxGroupId) {
                        // We must have seen this group before and saved its state
                        observedBlockBuilder.appendBoolean(hasValue(group));
                        hasTimestampBuilder.appendBoolean(nullKey(group) == false);
                        timestampsBuilder.appendLong(key(group));
                    } else {
                        // Unknown group so we append nulls everywhere
                        observedBlockBuilder.appendBoolean(false);
                        hasTimestampBuilder.appendBoolean(false);
                        timestampsBuilder.appendNull();
                    }
                }

                // Create all intermediate state blocks
                blocks[offset + 0] = observedBlockBuilder.build();
                blocks[offset + 1] = hasTimestampBuilder.build();
                blocks[offset + 2] = timestampsBuilder.build();
                blocks[offset + 3] = valuesBlock(selected, driverContext.blockFactory());
            }
        }

        Block evaluateFinal(IntVector groups, GroupingAggregatorEvaluationContext evalContext) {
            return valuesBlock(groups, evalContext.blockFactory());
        }

        private BytesRefArray getTail(int group) {
            if (tailValues == null) {
                return null;
            }
            if (group >= tailValues.size()) {
                return null;
            }
            return tailValues.get(group);
        }

        private BytesRefArray getTailForWriting(int group, int count) {
            if (tailValues == null) {
                tailValues = bigArrays.newObjectArray(group + 1);
            } else if (group >= tailValues.size()) {
                tailValues = bigArrays.grow(tailValues, group + 1);
            } else {
                Releasables.close(tailValues.get(group));
            }
            // BytesRefArray is append-only so we always need a fresh one; there is no way to clear and reuse it.
            BytesRefArray tail = new BytesRefArray(count, bigArrays);
            tailValues.set(group, tail);
            return tail;
        }

        private void clearTailValues(int group) {
            BytesRefArray tail = getTail(group);
            if (tail != null) {
                Releasables.close(tail);
                tailValues.set(group, null);
            }
        }

        private Block valuesBlock(IntVector groups, BlockFactory blockFactory) {
            try (var valuesBuilder = blockFactory.newBytesRefBlockBuilder(groups.getPositionCount())) {
                for (int p = 0; p < groups.getPositionCount(); p++) {
                    int group = groups.getInt(p);
                    if (group <= maxGroupId && hasValue(group) && nullValue(group) == false) {
                        BytesRefArray tail = getTail(group);
                        int tailCount = tail == null ? 0 : (int) tail.size();
                        if (tailCount == 0) {
                            valuesBuilder.appendBytesRef(firstValues.get(group).bytesRefView());
                        } else {
                            valuesBuilder.beginPositionEntry();
                            valuesBuilder.appendBytesRef(firstValues.get(group).bytesRefView());
                            for (int i = 0; i < tailCount; ++i) {
                                BytesRef bytesScratch = new BytesRef();
                                tail.get(i, bytesScratch);
                                valuesBuilder.appendBytesRef(bytesScratch);
                            }
                            valuesBuilder.endPositionEntry();
                        }
                    } else {
                        valuesBuilder.appendNull();
                    }
                }
                return valuesBuilder.build();
            }
        }
    }
}
