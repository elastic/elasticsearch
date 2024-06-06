/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.DriverContext;

import java.util.Comparator;
import java.util.HashMap;
import java.util.stream.StreamSupport;

/**
 * Aggregates the top N field values for long.
 */
@Aggregator({ @IntermediateState(name = "topValuesList", type = "LONG_BLOCK") })
@GroupingAggregator
class TopValuesListLongAggregator {
    public static SingleState initSingle(int limit, boolean ascending) {
        return new SingleState(limit, ascending);
    }

    public static void combine(SingleState state, long v) {
        state.add(v);
    }

    public static void combineIntermediate(SingleState state, LongBlock values) {
        int start = values.getFirstValueIndex(0);
        int end = start + values.getValueCount(0);
        for (int i = start; i < end; i++) {
            combine(state, values.getLong(i));
        }
    }

    public static Block evaluateFinal(SingleState state, DriverContext driverContext) {
        return state.toBlock(driverContext.blockFactory());
    }

    public static GroupingState initGrouping(int limit, boolean ascending) {
        return new GroupingState(limit, ascending);
    }

    public static void combine(GroupingState state, int groupId, long v) {
        state.add(groupId, v);
    }

    public static void combineIntermediate(GroupingState state, int groupId, LongBlock values, int valuesPosition) {
        int start = values.getFirstValueIndex(valuesPosition);
        int end = start + values.getValueCount(valuesPosition);
        for (int i = start; i < end; i++) {
            combine(state, groupId, values.getLong(i));
        }
    }

    public static void combineStates(GroupingState current, int groupId, GroupingState state, int statePosition) {
        current.merge(groupId, state, statePosition);
    }

    public static Block evaluateFinal(GroupingState state, IntVector selected, DriverContext driverContext) {
        return state.toBlock(driverContext.blockFactory(), selected);
    }

    public static class SingleState {
        private final Queue queue;
        private final Comparator<Long> finalSortComparator;
        private final int limit;

        private SingleState(int limit, boolean ascending) {
            // We use the inverted comparator to pop the least significant values from the queue
            this.queue = new Queue(ascending ? Comparator.reverseOrder() : Comparator.naturalOrder(), limit);
            this.finalSortComparator = ascending ? Comparator.naturalOrder() : Comparator.reverseOrder();
            this.limit = limit;
        }

        public void add(long value) {
            queue.safeAdd(value, limit);
        }

        void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            blocks[offset] = toBlock(driverContext.blockFactory());
        }

        Block toBlock(BlockFactory blockFactory) {
            if (queue.size() == 0) {
                return blockFactory.newConstantNullBlock(1);
            }
            if (queue.size() == 1) {
                return blockFactory.newConstantLongBlockWith(queue.top(), 1);
            }
            try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(queue.size())) {
                builder.beginPositionEntry();

                StreamSupport.stream(queue.spliterator(), false).sorted(finalSortComparator).forEach(builder::appendLong);

                builder.endPositionEntry();

                return builder.build();
            }
        }

        void close() {
            // Nothing to do
        }
    }

    public static class GroupingState {
        private final HashMap<Integer, Queue> queuesByGroup;
        private final Comparator<Long> queueComparator;
        private final Comparator<Long> finalSortComparator;
        private final int limit;

        private GroupingState(int limit, boolean ascending) {
            this.queuesByGroup = new HashMap<>();
            this.queueComparator = ascending ? Comparator.reverseOrder() : Comparator.naturalOrder();
            this.finalSortComparator = ascending ? Comparator.naturalOrder() : Comparator.reverseOrder();
            this.limit = limit;
        }

        public void add(int groupId, long value) {
            var queue = queuesByGroup.computeIfAbsent(groupId, k -> new Queue(queueComparator, limit));

            queue.safeAdd(value, limit);
        }

        public void merge(int groupId, GroupingState other, int otherGroupId) {
            var queue = queuesByGroup.computeIfAbsent(groupId, k -> new Queue(queueComparator, limit));
            var otherQueue = other.queuesByGroup.get(otherGroupId);

            if (otherQueue == null) {
                return;
            }

            for (var value : otherQueue) {
                queue.safeAdd(value, limit);
            }
        }

        void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            blocks[offset] = toBlock(driverContext.blockFactory(), selected);
        }

        Block toBlock(BlockFactory blockFactory, IntVector selected) {
            if (queuesByGroup.isEmpty()) {
                return blockFactory.newConstantNullBlock(selected.getPositionCount());
            }
            try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(selected.getPositionCount())) {
                for (int s = 0; s < selected.getPositionCount(); s++) {
                    int selectedGroup = selected.getInt(s);

                    var queue = queuesByGroup.get(selectedGroup);

                    if (queue == null) {
                        builder.appendNull();
                        continue;
                    }

                    if (queue.size() == 1) {
                        builder.appendLong(queue.top());
                        continue;
                    }

                    builder.beginPositionEntry();

                    StreamSupport.stream(queue.spliterator(), false).sorted(finalSortComparator).forEach(builder::appendLong);

                    builder.endPositionEntry();
                }
                return builder.build();
            }
        }

        void enableGroupIdTracking(SeenGroupIds seen) {
            // we figure out seen values from nulls on the values block
        }

        void close() {
            // Nothing to do
        }
    }

    private static class Queue extends PriorityQueue<Long> {
        private final Comparator<Long> comparator;

        public Queue(Comparator<Long> comparator, int limit) {
            super(limit + 1);
            this.comparator = comparator;
        }

        public void safeAdd(long value, int limit) {
            add(value);

            if (size() > limit) {
                pop();
            }
        }

        @Override
        protected boolean lessThan(Long a, Long b) {
            return comparator.compare(a, b) < 0;
        }
    }
}
