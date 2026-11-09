/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.IntroSorter;
import org.elasticsearch.common.util.BigArrays;
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
        private final BigArrays bigArrays;
        private final LongLongHashTable keys;
        private final long span;
        private final int steps;
        private LongArray masks;

        private GroupingState(BlockFactory blockFactory, long span, int steps) {
            this.bigArrays = blockFactory.bigArrays();
            this.span = span;
            this.steps = steps;
            LongLongHashTable keys = HashImplFactory.newLongLongHash(blockFactory);
            boolean success = false;
            try {
                this.masks = bigArrays.newLongArray(1, false);
                success = true;
            } finally {
                if (success == false) {
                    keys.close();
                }
            }
            this.keys = keys;
        }

        void add(int groupId, long timestamp, long mask) {
            if (mask == 0) {
                return;
            }
            long id = keys.add(groupId, timestamp);
            if (id >= 0) {
                masks = bigArrays.grow(masks, id + 1);
                masks.set(id, mask);
            } else {
                id = -1 - id;
                masks.set(id, masks.get(id) | mask);
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
                        sorter.sort(start, end);
                        builder.appendInt(level(layout.ids, start, end, firsts));
                    }
                    return builder.build();
                }
            }

            private void buildIntermediate(Block[] blocks, int offset, IntVector selectedInPage) {
                int positions = selectedInPage.getPositionCount();
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

        private long deadline(long first) {
            return first > Long.MAX_VALUE - span ? Long.MAX_VALUE : first + span;
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(keys, masks);
        }
    }
}
