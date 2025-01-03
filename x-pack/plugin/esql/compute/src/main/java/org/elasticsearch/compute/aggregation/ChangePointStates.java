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
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
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

public class ChangePointStates {

    record TimeAndValue(long timestamp, double value) implements Comparable<TimeAndValue> {
        @Override
        public int compareTo(TimeAndValue other) {
            return Long.compare(timestamp, other.timestamp);
        }
    }

    static class SingleState implements Releasable {
        private final BigArrays bigArrays;
        private int count;
        private LongArray timestamps;
        private DoubleArray values;

        SingleState(BigArrays bigArrays) {
            this.bigArrays = bigArrays;
            count = 0;
            timestamps = bigArrays.newLongArray(0);
            values = bigArrays.newDoubleArray(0);
        }

        void add(long timestamp, double value) {
            count++;
            timestamps = bigArrays.grow(timestamps, count);
            timestamps.set(count - 1, timestamp);
            values = bigArrays.grow(values, count);
            values.set(count - 1, value);
        }

        void add(LongBlock timestamps, DoubleBlock values) {
            add(timestamps, values, 0);
        }

        void add(LongBlock timestamps, DoubleBlock values, int otherPosition) {
            final int valueCount = timestamps.getValueCount(otherPosition);
            final int firstIndex = timestamps.getFirstValueIndex(otherPosition);
            for (int i = 0; i < valueCount; i++) {
                add(timestamps.getLong(firstIndex + i), values.getDouble(firstIndex + i));
            }
        }

        void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            blocks[offset] = buildTimestampsBlock(driverContext.blockFactory());
            blocks[offset + 1] = buildValuesBlock(driverContext.blockFactory());
        }

        // TODO: this needs to output multiple columns or a composite object, not a JSON blob.
        Block toBlock(BlockFactory blockFactory) {
            return blockFactory.newConstantBytesRefBlockWith(getChangePoint(), 1);
        }

        private Block buildTimestampsBlock(BlockFactory blockFactory) {
            if (timestamps.size() == 0) {
                return blockFactory.newConstantNullBlock(1);
            }
            if (values.size() == 1) {
                return blockFactory.newConstantLongBlockWith(timestamps.get(0), 1);
            }
            try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder((int) timestamps.size())) {
                builder.beginPositionEntry();
                for (int id = 0; id < count; id++) {
                    builder.appendLong(timestamps.get(id));
                }
                builder.endPositionEntry();
                return builder.build();
            }
        }

        private Block buildValuesBlock(BlockFactory blockFactory) {
            if (values.size() == 0) {
                return blockFactory.newConstantNullBlock(1);
            }
            if (values.size() == 1) {
                return blockFactory.newConstantDoubleBlockWith(values.get(0), 1);
            }
            try (DoubleBlock.Builder builder = blockFactory.newDoubleBlockBuilder((int) values.size())) {
                builder.beginPositionEntry();
                for (int id = 0; id < count; id++) {
                    builder.appendDouble(values.get(id));
                }
                builder.endPositionEntry();
                return builder.build();
            }
        }

        // TODO: this needs to output multiple columns or a composite object, not a JSON blob.
        private BytesRef getChangePoint() {
            // TODO: probably reuse ES|QL sort/orderBy to get results in order
            // TODO: this copying/sorting doesn't account for memory
            List<TimeAndValue> list = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                list.add(new TimeAndValue(timestamps.get(i), values.get(i)));
            }
            Collections.sort(list);
            double[] values = new double[count];
            for (int i = 0; i < count; i++) {
                values[i] = list.get(i).value;
            }
            MlAggsHelper.DoubleBucketValues bucketValues = new MlAggsHelper.DoubleBucketValues(null, values);
            ChangeType changeType = ChangePointDetector.getChangeType(bucketValues);
            try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()) {
                xContentBuilder.startObject();
                NamedXContentObjectHelper.writeNamedObject(xContentBuilder, ToXContent.EMPTY_PARAMS, "type", changeType);
                xContentBuilder.endObject();
                String xContent = Strings.toString(xContentBuilder);
                return new BytesRef(xContent);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() {
            Releasables.close(timestamps, values);
        }
    }

    static class GroupingState implements Releasable {
        private final BigArrays bigArrays;
        private final Map<Integer, SingleState> states;

        GroupingState(BigArrays bigArrays) {
            this.bigArrays = bigArrays;
            states = new HashMap<>();
        }

        void add(int groupId, long timestamp, double value) {
            SingleState state = states.computeIfAbsent(groupId, key -> new SingleState(bigArrays));
            state.add(timestamp, value);
        }

        void combine(int groupId, LongBlock timestamps, DoubleBlock values, int otherPosition) {
            if (timestamps.getValueCount(otherPosition) == 0) {
                return;
            }
            SingleState state = states.computeIfAbsent(groupId, key -> new SingleState(bigArrays));
            state.add(timestamps, values, otherPosition);
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
            blocks[offset] = buildTimestampsBlock(driverContext.blockFactory(), selected);
            blocks[offset + 1] = buildValuesBlock(driverContext.blockFactory(), selected);
        }

        // TODO: this needs to output multiple columns or a composite object, not a JSON blob.
        Block evaluateFinal(IntVector selected, BlockFactory blockFactory) {
            try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(selected.getPositionCount())) {
                for (int s = 0; s < selected.getPositionCount(); s++) {
                    builder.appendBytesRef(states.get(selected.getInt(s)).getChangePoint());
                }
                return builder.build();
            }
        }

        private Block buildTimestampsBlock(BlockFactory blockFactory, IntVector selected) {
            if (states.isEmpty()) {
                return blockFactory.newConstantNullBlock(selected.getPositionCount());
            }
            try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(selected.getPositionCount())) {
                for (int s = 0; s < selected.getPositionCount(); s++) {
                    int selectedGroup = selected.getInt(s);
                    SingleState state = states.get(selectedGroup);
                    int count = 0;
                    long first = 0;
                    for (int i = 0; i < state.count; i++) {
                        long timestamp = state.timestamps.get(i);
                        switch (count) {
                            case 0 -> first = timestamp;
                            case 1 -> {
                                builder.beginPositionEntry();
                                builder.appendLong(first);
                                builder.appendLong(timestamp);
                            }
                            default -> builder.appendLong(timestamp);
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

        private Block buildValuesBlock(BlockFactory blockFactory, IntVector selected) {
            if (states.isEmpty()) {
                return blockFactory.newConstantNullBlock(selected.getPositionCount());
            }
            try (DoubleBlock.Builder builder = blockFactory.newDoubleBlockBuilder(selected.getPositionCount())) {
                for (int s = 0; s < selected.getPositionCount(); s++) {
                    int selectedGroup = selected.getInt(s);
                    SingleState state = states.get(selectedGroup);
                    int count = 0;
                    double first = 0;
                    for (int i = 0; i < state.count; i++) {
                        double value = state.values.get(i);
                        switch (count) {
                            case 0 -> first = value;
                            case 1 -> {
                                builder.beginPositionEntry();
                                builder.appendDouble(first);
                                builder.appendDouble(value);
                            }
                            default -> builder.appendDouble(value);
                        }
                        count++;
                    }
                    switch (count) {
                        case 0 -> builder.appendNull();
                        case 1 -> builder.appendDouble(first);
                        default -> builder.endPositionEntry();
                    }
                }
                return builder.build();
            }
        }

        void enableGroupIdTracking(SeenGroupIds seenGroupIds) {}

        @Override
        public void close() {
            Releasables.close(states.values());
        }
    }
}
