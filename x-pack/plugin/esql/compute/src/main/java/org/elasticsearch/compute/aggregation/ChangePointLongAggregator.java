/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObjectHelper;
import org.elasticsearch.xpack.ml.aggs.MlAggsHelper;
import org.elasticsearch.xpack.ml.aggs.changepoint.ChangePointDetector;
import org.elasticsearch.xpack.ml.aggs.changepoint.ChangeType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Change point detection for series of long values.
 */

// TODO: make .java.st from this to support different types

// TODO: add non-grouping @Aggregator, like this:
/*
@Aggregator(
    includeTimestamps = true,
    value = { @IntermediateState(name = "timestamps", type = "LONG_BLOCK"), @IntermediateState(name = "values", type = "LONG_BLOCK") }
)
*/
// This need "includeTimestamps" support in @Aggregator.

@GroupingAggregator(
    includeTimestamps = true,
    value = { @IntermediateState(name = "timestamps", type = "LONG_BLOCK"), @IntermediateState(name = "values", type = "LONG_BLOCK") }
)
class ChangePointLongAggregator {

    public static GroupingState initGrouping(DriverContext driverContext) {
        return new GroupingState(driverContext.bigArrays());
    }

    public static void combine(GroupingState current, int groupId, long timestamp, long value) {
        current.add(groupId, timestamp, value);
    }

    public static void combineIntermediate(GroupingState current, int groupId, LongBlock timestamps, LongBlock values, int otherPosition) {
        current.combine(groupId, timestamps, values, otherPosition);
    }

    public static void combineStates(GroupingState current, int currentGroupId, GroupingState otherState, int otherGroupId) {
        current.combineState(currentGroupId, otherState, otherGroupId);
    }

    public static Block evaluateFinal(GroupingState state, IntVector selected, DriverContext driverContext) {
        return state.evaluateFinal(selected, driverContext.blockFactory());
    }

    public static class SingleState implements Releasable {
        private final BigArrays bigArrays;
        private int count;
        private LongArray timestamps;
        private LongArray values;

        private SingleState(BigArrays bigArrays) {
            this.bigArrays = bigArrays;
            count = 0;
            timestamps = bigArrays.newLongArray(0);
            values = bigArrays.newLongArray(0);
        }

        void add(long timestamp, long value) {
            count++;
            timestamps = bigArrays.grow(timestamps, count);
            timestamps.set(count - 1, timestamp);
            values = bigArrays.grow(values, count);
            values.set(count - 1, value);
        }

        void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            blocks[offset] = toBlock(timestamps, driverContext.blockFactory());
            blocks[offset + 1] = toBlock(values, driverContext.blockFactory());
        }

        Block toBlock(LongArray arr, BlockFactory blockFactory) {
            if (arr.size() == 0) {
                return blockFactory.newConstantNullBlock(1);
            }
            if (values.size() == 1) {
                return blockFactory.newConstantLongBlockWith(arr.get(0), 1);
            }
            try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder((int) arr.size())) {
                builder.beginPositionEntry();
                for (int id = 0; id < arr.size(); id++) {
                    builder.appendLong(arr.get(id));
                }
                builder.endPositionEntry();
                return builder.build();
            }
        }

        record TimeAndValue(long timestamp, long value) implements Comparable<TimeAndValue> {
            @Override
            public int compareTo(TimeAndValue other) {
                return Long.compare(timestamp, other.timestamp);
            }
        }

        void sort() {
            // TODO: this copying is a bit inefficient and doesn't account for memory
            List<TimeAndValue> list = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                list.add(new TimeAndValue(timestamps.get(i), values.get(i)));
            }
            Collections.sort(list);
            for (int i = 0; i < count; i++) {
                timestamps.set(i, list.get(i).timestamp);
                values.set(i, list.get(i).value);
            }
        }

        @Override
        public void close() {
            timestamps.close();
            values.close();
        }
    }

    public static class GroupingState implements Releasable {
        private final BigArrays bigArrays;
        private final Map<Integer, SingleState> states;

        private GroupingState(BigArrays bigArrays) {
            this.bigArrays = bigArrays;
            states = new HashMap<>();
        }

        void add(int groupId, long timestamp, long value) {
            SingleState state = states.computeIfAbsent(groupId, key -> new SingleState(bigArrays));
            state.add(timestamp, value);
        }

        void combine(int groupId, LongBlock timestamps, LongBlock values, int otherPosition) {
            final int valueCount = timestamps.getValueCount(otherPosition);
            if (valueCount == 0) {
                return;
            }
            final int firstIndex = timestamps.getFirstValueIndex(otherPosition);
            SingleState state = states.computeIfAbsent(groupId, key -> new SingleState(bigArrays));
            for (int i = 0; i < valueCount; i++) {
                state.add(timestamps.getLong(firstIndex + i), values.getLong(firstIndex + i));
            }
        }

        void combineState(int groupId, GroupingState otherState, int otherGroupId) {
            SingleState other = otherState.states.get(otherGroupId);
            if (other == null) {
                return;
            }
            var state = states.computeIfAbsent(groupId, key -> new SingleState(bigArrays));
            for (int i = 0; i < other.timestamps.size(); i++) {
                state.add(state.timestamps.get(i), state.values.get(i));
            }
        }

        void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            blocks[offset] = toBlock(s -> s.timestamps, driverContext.blockFactory(), selected);
            blocks[offset + 1] = toBlock(s -> s.values, driverContext.blockFactory(), selected);
        }

        public Block evaluateFinal(IntVector selected, BlockFactory blockFactory) {
            // TODO: this needs to output multiple columns or a composite object, not a JSON blob.
            try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(selected.getPositionCount())) {
                for (int s = 0; s < selected.getPositionCount(); s++) {
                    SingleState state = states.get(selected.getInt(s));
                    state.sort();
                    double[] values = new double[state.count];
                    for (int i = 0; i < state.count; i++) {
                        values[i] = state.values.get(i);
                    }
                    MlAggsHelper.DoubleBucketValues bucketValues = new MlAggsHelper.DoubleBucketValues(null, values);
                    ChangeType changeType = ChangePointDetector.getChangeType(bucketValues);
                    try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()) {
                        xContentBuilder.startObject();
                        NamedXContentObjectHelper.writeNamedObject(xContentBuilder, ToXContent.EMPTY_PARAMS, "type", changeType);
                        xContentBuilder.endObject();
                        String xContent = Strings.toString(xContentBuilder);
                        builder.appendBytesRef(new BytesRef(xContent));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
                return builder.build();
            }
        }

        Block toBlock(Function<SingleState, LongArray> getArray, BlockFactory blockFactory, IntVector selected) {
            if (states.isEmpty()) {
                return blockFactory.newConstantNullBlock(selected.getPositionCount());
            }
            try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(selected.getPositionCount())) {
                for (int s = 0; s < selected.getPositionCount(); s++) {
                    int selectedGroup = selected.getInt(s);
                    SingleState state = states.get(selectedGroup);
                    LongArray values = getArray.apply(state);
                    int count = 0;
                    long first = 0;
                    for (int i = 0; i < state.count; i++) {
                        long value = values.get(i);
                        switch (count) {
                            case 0 -> first = value;
                            case 1 -> {
                                builder.beginPositionEntry();
                                builder.appendLong(first);
                                builder.appendLong(value);
                            }
                            default -> builder.appendLong(value);
                        }
                        count++;
                    }
                    switch (count) {
                        case 0 -> builder.appendNull();
                        case 1 -> builder.appendLong(first);
                        default -> builder.endPositionEntry();
                    }
                }
                return builder.build();
            }
        }

        void enableGroupIdTracking(SeenGroupIds seenGroupIds) {}

        @Override
        public void close() {
            for (SingleState state : states.values()) {
                state.close();
            }
        }
    }
}
