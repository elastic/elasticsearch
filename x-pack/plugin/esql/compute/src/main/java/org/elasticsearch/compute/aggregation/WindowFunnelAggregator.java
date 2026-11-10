/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.IntroSorter;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.LongLongHashTable;
import org.elasticsearch.compute.aggregation.blockhash.HashImplFactory;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;

/**
 * Level of the longest ordered chain of conditions that ends at most {@code span} after its first condition.
 * Each group keeps one entry per distinct timestamp, OR-ing the condition bit masks of the rows that share it.
 * <p>
 * For example, {@code STATS f = WINDOW_FUNNEL(15 minutes, @timestamp, event == "view", event == "cart", event == "checkout") BY user}
 * sets bit {@code i} of a row's mask when condition {@code i} matches. A data node receiving these rows:
 * <pre>
 * | user  | @timestamp | event    | mask |
 * |-------|------------|----------|------|
 * | alice | 10:00      | view     | 001  |
 * | bob   | 10:00      | view     | 001  |
 * | alice | 10:05      | cart     | 010  |
 * | alice | 10:10      | checkout | 100  |
 * | bob   | 10:30      | cart     | 010  |
 * </pre>
 * stores the same five {@code (group, timestamp) -> mask} entries and emits this intermediate state:
 * <pre>
 * | user  | timestamps     | masks      |
 * |-------|----------------|------------|
 * | alice | [10:00]        | [111]      |
 * | bob   | [10:00, 10:30] | [001, 010] |
 * </pre>
 * alice's chain reaches every step within 15 minutes, so it collapses to one entry with all bits, which marks alice
 * complete on the node that merges it. bob's cart comes after 10:00 + 15 minutes, so the final levels are 3 for alice
 * and 1 for bob.
 */
@Aggregator({ @IntermediateState(name = "timestamps", type = "LONG_BLOCK"), @IntermediateState(name = "masks", type = "LONG_BLOCK") })
@GroupingAggregator
class WindowFunnelAggregator {

    public static String describe() {
        return "window_funnel";
    }

    public static SingleState initSingle(DriverContext driverContext, long span, int steps) {
        return new SingleState(driverContext.blockFactory(), span, steps);
    }

    public static void combine(SingleState state, long timestamp, long mask) {
        state.internal.add(0, timestamp, mask);
    }

    public static void combineIntermediate(SingleState state, LongBlock timestamps, LongBlock masks) {
        state.internal.addIntermediate(0, timestamps, masks, 0);
    }

    public static Block evaluateFinal(SingleState state, DriverContext driverContext) {
        return state.toFinal(driverContext.blockFactory());
    }

    public static GroupingState initGrouping(DriverContext driverContext, long span, int steps) {
        return new GroupingState(driverContext.blockFactory(), span, steps);
    }

    public static void combine(GroupingState state, int groupId, long timestamp, long mask) {
        state.add(groupId, timestamp, mask);
    }

    public static void combineIntermediate(GroupingState state, int groupId, LongBlock timestamps, LongBlock masks, int position) {
        state.addIntermediate(groupId, timestamps, masks, position);
    }

    public static GroupingAggregatorFunction.PreparedForEvaluation prepareEvaluateIntermediate(
        GroupingState state,
        IntVector selected,
        GroupingAggregatorEvaluationContext ctx
    ) {
        return state.prepare(ctx.blockFactory(), selected, false);
    }

    public static GroupingAggregatorFunction.PreparedForEvaluation prepareEvaluateFinal(
        GroupingState state,
        IntVector selected,
        GroupingAggregatorEvaluationContext ctx
    ) {
        return state.prepare(ctx.blockFactory(), selected, true);
    }

    public static class SingleState implements AggregatorState {
        private final GroupingState internal;

        private SingleState(BlockFactory blockFactory, long span, int steps) {
            this.internal = new GroupingState(blockFactory, span, steps);
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            evaluate(driverContext.blockFactory(), blocks, offset, false);
        }

        Block toFinal(BlockFactory blockFactory) {
            if (internal.keys.size() == 0) {
                return blockFactory.newConstantNullBlock(1);
            }
            Block[] blocks = new Block[1];
            evaluate(blockFactory, blocks, 0, true);
            return blocks[0];
        }

        private void evaluate(BlockFactory blockFactory, Block[] blocks, int offset, boolean last) {
            try (
                IntVector selected = blockFactory.newConstantIntVector(0, 1);
                GroupingAggregatorFunction.PreparedForEvaluation prepared = internal.prepare(blockFactory, selected, last)
            ) {
                prepared.evaluate(blocks, offset, selected);
            }
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(internal);
        }
    }

    public static class GroupingState implements GroupingAggregatorState {
        private final BigArrays bigArrays; // breaker-tracked allocator for masks and complete
        private final LongLongHashTable keys; // one id per distinct (groupId, timestamp); the id indexes masks
        private final long span;
        private final int steps;
        private final long allSteps; // the lowest steps bits set: an entry with this mask matches every condition
        private final BitArray complete; // groups holding an allSteps entry: their level is steps, so later events are skipped
        private LongArray masks;

        private GroupingState(BlockFactory blockFactory, long span, int steps) {
            assert steps > 0 && steps < Long.SIZE : steps;
            assert span >= 0 : span;
            this.bigArrays = blockFactory.bigArrays();
            this.span = span;
            this.steps = steps;
            this.allSteps = (1L << steps) - 1;
            LongLongHashTable keys = HashImplFactory.newLongLongHash(blockFactory);
            BitArray complete = null;
            boolean success = false;
            try {
                this.masks = bigArrays.newLongArray(1, false);
                complete = new BitArray(1, bigArrays);
                success = true;
            } finally {
                if (success == false) {
                    Releasables.closeExpectNoException(keys, masks);
                }
            }
            this.keys = keys;
            this.complete = complete;
        }

        void add(int groupId, long timestamp, long mask) {
            if (mask == 0 || complete.get(groupId)) {
                return;
            }
            long id = keys.add(groupId, timestamp);
            if (id >= 0) {
                masks = bigArrays.grow(masks, id + 1);
            } else {
                id = -1 - id;
                mask |= masks.get(id);
            }
            masks.set(id, mask);
            // skipping later events is exact only while all steps may share a timestamp and the span is never negative
            if (mask == allSteps) {
                complete.set(groupId);
            }
        }

        void addIntermediate(int groupId, LongBlock timestamps, LongBlock intermediateMasks, int position) {
            assert timestamps.getValueCount(position) == intermediateMasks.getValueCount(position);
            int start = timestamps.getFirstValueIndex(position);
            int end = start + timestamps.getValueCount(position);
            int maskStart = intermediateMasks.getFirstValueIndex(position);
            for (int i = start; i < end; i++) {
                add(groupId, timestamps.getLong(i), intermediateMasks.getLong(maskStart + i - start));
            }
        }

        @Override
        public void enableGroupIdTracking(SeenGroupIds seenGroupIds) {
            // groups without entries are emitted as null
        }

        GroupingAggregatorFunction.PreparedForEvaluation prepare(BlockFactory blockFactory, IntVector selected, boolean last) {
            ValuesNextPreparedForEmitting layout = ValuesNextPreparedForEmitting.build(blockFactory, selected, keys);
            return new Prepared(blockFactory, layout, last);
        }

        /**
         * Emits the selected groups, either as final levels or as intermediate {@code timestamps} and {@code masks} blocks.
         * {@code layout.ids} lists each group's entry ids as one contiguous slice in insertion order, so a slice is sorted
         * by timestamp in place before {@code level} walks it; complete groups skip the sort.
         * <p>
         * For the rows in the class example, with alice as group 0 and bob as group 1:
         * <pre>
         * | id | (group, timestamp) | mask |
         * |----|--------------------|------|
         * | 0  | (0, 10:00)         | 001  |
         * | 1  | (1, 10:00)         | 001  |
         * | 2  | (0, 10:05)         | 010  |
         * | 3  | (0, 10:10)         | 100  |
         * | 4  | (1, 10:30)         | 010  |
         * </pre>
         * {@code layout.ids} is {@code [0, 2, 3, 1, 4]}: alice's slice is {@code [0, 3)} and bob's is {@code [3, 5)}.
         * The final evaluation emits the levels {@code [3, 1]} and the intermediate one emits the state shown in the class
         * example.
         */
        private final class Prepared implements GroupingAggregatorFunction.PreparedForEvaluation {
            private final BlockFactory blockFactory;
            private final ValuesNextPreparedForEmitting layout;
            private final boolean last;

            Prepared(BlockFactory blockFactory, ValuesNextPreparedForEmitting layout, boolean last) {
                this.blockFactory = blockFactory;
                this.layout = layout;
                this.last = last;
            }

            @Override
            public void evaluate(Block[] blocks, int offset, IntVector selectedInPage) {
                if (last) {
                    blocks[offset] = buildLevels(selectedInPage);
                } else {
                    buildIntermediate(blocks, offset, selectedInPage);
                }
            }

            /**
             * One level per selected group: null for a group without entries, {@code steps} for a complete group, and
             * otherwise the {@code level} of its slice sorted by timestamp. In the class example: {@code [3, 1]}.
             */
            private Block buildLevels(IntVector selectedInPage) {
                long[] firsts = new long[steps];
                IntroSorter sorter = timestampSorter(layout.ids);
                try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(selectedInPage.getPositionCount())) {
                    for (int s = 0; s < selectedInPage.getPositionCount(); s++) {
                        int group = selectedInPage.getInt(s);
                        int start = layout.nextValuesStart(group);
                        int end = layout.nextValuesEnd(group);
                        if (start == end) {
                            builder.appendNull();
                            continue;
                        }
                        if (complete.get(group)) {
                            builder.appendInt(steps);
                            continue;
                        }
                        sorter.sort(start, end);
                        builder.appendInt(level(layout.ids, start, end, firsts));
                    }
                    return builder.build();
                }
            }

            /**
             * Parallel {@code timestamps} and {@code masks} values per selected group: nulls for a group without entries, a
             * single entry with every bit for a group that already reaches every step, and otherwise all of the group's
             * entries. In the class example alice becomes {@code [10:00] / [111]} and bob keeps {@code [10:00, 10:30] / [001, 010]}.
             */
            private void buildIntermediate(Block[] blocks, int offset, IntVector selectedInPage) {
                int positions = selectedInPage.getPositionCount();
                long[] firsts = new long[steps];
                IntroSorter sorter = timestampSorter(layout.ids);
                try (
                    LongBlock.Builder timestamps = blockFactory.newLongBlockBuilder(positions);
                    LongBlock.Builder entryMasks = blockFactory.newLongBlockBuilder(positions)
                ) {
                    for (int s = 0; s < positions; s++) {
                        int group = selectedInPage.getInt(s);
                        int start = layout.nextValuesStart(group);
                        int end = layout.nextValuesEnd(group);
                        if (start == end) {
                            timestamps.appendNull();
                            entryMasks.appendNull();
                            continue;
                        }
                        if (complete.get(group) || reachesAllSteps(start, end, sorter, firsts)) {
                            timestamps.appendLong(keys.getKey2(layout.ids[start]));
                            entryMasks.appendLong(allSteps);
                            continue;
                        }
                        if (end - start == 1) {
                            int id = layout.ids[start];
                            timestamps.appendLong(keys.getKey2(id));
                            entryMasks.appendLong(masks.get(id));
                            continue;
                        }
                        timestamps.beginPositionEntry();
                        entryMasks.beginPositionEntry();
                        for (int i = start; i < end; i++) {
                            int id = layout.ids[i];
                            timestamps.appendLong(keys.getKey2(id));
                            entryMasks.appendLong(masks.get(id));
                        }
                        timestamps.endPositionEntry();
                        entryMasks.endPositionEntry();
                    }
                    blocks[offset] = timestamps.build();
                    blocks[offset + 1] = entryMasks.build();
                }
            }

            private boolean reachesAllSteps(int start, int end, IntroSorter sorter, long[] firsts) {
                if (end - start == 1) {
                    return false;
                }
                long seen = 0;
                for (int i = start; i < end; i++) {
                    seen |= masks.get(layout.ids[i]);
                }
                if (seen != allSteps) {
                    return false;
                }
                sorter.sort(start, end);
                return level(layout.ids, start, end, firsts) == steps;
            }

            @Override
            public void close() {
                layout.close();
            }
        }

        private IntroSorter timestampSorter(int[] ids) {
            return new IntroSorter() {
                long pivot;

                @Override
                protected void setPivot(int i) {
                    pivot = keys.getKey2(ids[i]);
                }

                @Override
                protected int comparePivot(int j) {
                    return Long.compare(pivot, keys.getKey2(ids[j]));
                }

                @Override
                protected int compare(int i, int j) {
                    return Long.compare(keys.getKey2(ids[i]), keys.getKey2(ids[j]));
                }

                @Override
                protected void swap(int i, int j) {
                    int tmp = ids[i];
                    ids[i] = ids[j];
                    ids[j] = tmp;
                }
            };
        }

        /**
         * Deepest step reached by the entries {@code ids[start, end)}, which must be sorted by timestamp. {@code firsts[k]}
         * is the latest start of a chain reaching step {@code k}: a later start keeps the deadline as late as possible, so
         * step {@code k + 1} extends the chain if its timestamp is within {@code deadline(firsts[k])}. The bits of one entry
         * are taken in step order, so conditions sharing a timestamp chain.
         * <p>
         * With a one hour span and three steps, where {@code bN} is an entry whose mask has bit {@code N}:
         * <pre>{@code
         * u1: 10:00 b0 -> firsts[0]=10:00, best=1
         *     10:05 b1 -> 10:05 <= 11:00 -> firsts[1]=10:00, best=2
         *     10:10 b2 -> 10:10 <= 11:00 -> best=3 == steps -> return 3
         * u2: 10:00 b0 -> best=1
         *     10:30 b1 -> 10:30 <= 11:00 -> best=2 (no b2 entry) -> return 2
         * }</pre>
         */
        private int level(int[] ids, int start, int end, long[] firsts) {
            long reached = 0;
            int best = 0;
            for (int i = start; i < end; i++) {
                long timestamp = keys.getKey2(ids[i]);
                long mask = masks.get(ids[i]);
                while (mask != 0) {
                    int step = Long.numberOfTrailingZeros(mask);
                    assert step < steps : "mask bit " + step + " beyond " + steps + " steps";
                    mask &= mask - 1;
                    if (step == 0) {
                        firsts[0] = timestamp;
                        reached |= 1L;
                    } else if ((reached & (1L << (step - 1))) != 0 && timestamp <= deadline(firsts[step - 1])) {
                        firsts[step] = firsts[step - 1];
                        reached |= 1L << step;
                    } else {
                        continue;
                    }
                    best = Math.max(best, step + 1);
                    if (best == steps) {
                        return best;
                    }
                }
            }
            return best;
        }

        /**
         * Latest timestamp a chain started at {@code first} may extend to, saturated at {@link Long#MAX_VALUE} instead of
         * overflowing.
         */
        private long deadline(long first) {
            return first > Long.MAX_VALUE - span ? Long.MAX_VALUE : first + span;
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(keys, masks, complete);
        }
    }
}
