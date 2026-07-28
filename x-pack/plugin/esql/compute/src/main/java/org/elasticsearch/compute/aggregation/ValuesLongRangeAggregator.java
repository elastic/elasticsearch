/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.common.util.LongLongHashTable;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.aggregation.blockhash.HashImplFactory;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongRangeBlock;
import org.elasticsearch.compute.data.LongRangeBlockBuilder;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;

/**
 * Aggregates field values for date_range.
 * This class is hand-written rather than generated from {@code X-ValuesAggregator.java.st} because
 * date_range values are pairs of longs {@code (from, to)}, not a single primitive or BytesRef.
 * Edit this file to change the aggregation logic; the three generated classes
 * ({@code ValuesLongRangeAggregatorFunction}, {@code ValuesLongRangeGroupingAggregatorFunction},
 * {@code ValuesLongRangeAggregatorFunctionSupplier}) will be regenerated on next build.
 */
@Aggregator({ @IntermediateState(name = "values", type = "LONG_RANGE_BLOCK") })
@GroupingAggregator
class ValuesLongRangeAggregator {

    public static SingleState initSingle(DriverContext driverContext) {
        return new SingleState(driverContext.blockFactory());
    }

    public static void combine(SingleState state, LongRangeBlockBuilder.LongRange v) {
        state.values.add(v.from(), v.to());
    }

    public static void combineIntermediate(SingleState state, LongRangeBlock values) {
        LongRangeBlockBuilder.LongRange scratch = new LongRangeBlockBuilder.LongRange();
        int start = values.getFirstValueIndex(0);
        int end = start + values.getValueCount(0);
        for (int i = start; i < end; i++) {
            values.getLongRange(i, scratch);
            state.values.add(scratch.from(), scratch.to());
        }
    }

    public static Block evaluateFinal(SingleState state, DriverContext driverContext) {
        return state.toBlock(driverContext.blockFactory());
    }

    public static GroupingState initGrouping(DriverContext driverContext) {
        return new GroupingState(driverContext);
    }

    public static void combine(GroupingState state, int groupId, LongRangeBlockBuilder.LongRange v) {
        state.addValue(groupId, v.from(), v.to());
    }

    public static void combineIntermediate(GroupingState state, int groupId, LongRangeBlock values, int valuesPosition) {
        LongRangeBlockBuilder.LongRange scratch = new LongRangeBlockBuilder.LongRange();
        int start = values.getFirstValueIndex(valuesPosition);
        int end = start + values.getValueCount(valuesPosition);
        for (int i = start; i < end; i++) {
            values.getLongRange(i, scratch);
            state.addValue(groupId, scratch.from(), scratch.to());
        }
    }

    public static GroupingAggregatorFunction.PreparedForEvaluation prepareEvaluateIntermediate(
        GroupingState state,
        IntVector selected,
        GroupingAggregatorEvaluationContext ctx
    ) {
        return state.prepareForEmitting(ctx.blockFactory(), selected);
    }

    public static GroupingAggregatorFunction.PreparedForEvaluation prepareEvaluateFinal(
        GroupingState state,
        IntVector selected,
        GroupingAggregatorEvaluationContext ctx
    ) {
        return state.prepareForEmitting(ctx.blockFactory(), selected);
    }

    public static class SingleState implements AggregatorState {
        private final LongLongHashTable values;

        private SingleState(BlockFactory blockFactory) {
            values = HashImplFactory.newLongLongHash(blockFactory);
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            blocks[offset] = toBlock(driverContext.blockFactory());
        }

        Block toBlock(BlockFactory blockFactory) {
            if (values.size() == 0) {
                return blockFactory.newConstantNullBlock(1);
            }
            try (LongRangeBlock.Builder builder = blockFactory.newLongRangeBlockBuilder((int) values.size())) {
                LongRangeBlockBuilder.LongRange range = new LongRangeBlockBuilder.LongRange();
                if (values.size() == 1) {
                    range.reset(values.getKey1(0), values.getKey2(0));
                    builder.appendLongRange(range);
                } else {
                    builder.beginPositionEntry();
                    for (long id = 0; id < values.size(); id++) {
                        range.reset(values.getKey1(id), values.getKey2(id));
                        builder.appendLongRange(range);
                    }
                    builder.endPositionEntry();
                }
                return builder.build();
            }
        }

        @Override
        public void close() {
            values.close();
        }
    }

    /**
     * State for a grouped {@code VALUES} aggregation over {@code date_range} values.
     * Unique {@code (from, to)} pairs are stored as 16-byte entries in {@code bytes};
     * the resulting ordinal tracks first and subsequent occurrences per group.
     */
    public static class GroupingState implements GroupingAggregatorState {
        private final BlockFactory blockFactory;
        BytesRefHash bytes;
        private IntArray firstValues;
        private final ValuesNextLong nextValues;

        private final byte[] encodeBuf = new byte[16];
        private final BytesRef encodeScratch = new BytesRef(encodeBuf, 0, 16);

        private GroupingState(DriverContext driverContext) {
            this.blockFactory = driverContext.blockFactory();
            boolean success = false;
            try {
                this.bytes = new BytesRefHash(1, driverContext.bigArrays());
                this.firstValues = driverContext.bigArrays().newIntArray(1, true);
                this.nextValues = new ValuesNextLong(driverContext.blockFactory());
                success = true;
            } finally {
                if (success == false) {
                    close();
                }
            }
        }

        private BytesRef encode(long from, long to) {
            for (int i = 7; i >= 0; i--) {
                encodeBuf[i] = (byte) (from & 0xff);
                from >>= 8;
                encodeBuf[8 + i] = (byte) (to & 0xff);
                to >>= 8;
            }
            encodeScratch.offset = 0;
            encodeScratch.length = 16;
            return encodeScratch;
        }

        void addValue(int groupId, long from, long to) {
            int valueOrdinal = Math.toIntExact(BlockHash.hashOrdToGroup(bytes.add(encode(from, to))));
            if (groupId < firstValues.size()) {
                int current = firstValues.get(groupId) - 1;
                if (current < 0) {
                    firstValues.set(groupId, valueOrdinal + 1);
                } else if (current != valueOrdinal) {
                    nextValues.add(groupId, valueOrdinal);
                }
            } else {
                firstValues = blockFactory.bigArrays().grow(firstValues, groupId + 1);
                firstValues.set(groupId, valueOrdinal + 1);
            }
        }

        @Override
        public void enableGroupIdTracking(SeenGroupIds seenGroupIds) {
            // we figure out seen values from firstValues since ordinals are non-negative
        }

        GroupingAggregatorFunction.PreparedForEvaluation prepareForEmitting(BlockFactory blockFactory, IntVector selected) {
            return new PreparedForEmitting(selected, blockFactory);
        }

        private class PreparedForEmitting implements GroupingAggregatorFunction.PreparedForEvaluation {
            private final BlockFactory blockFactory;
            private final ValuesNextPreparedForEmitting next;

            PreparedForEmitting(IntVector selected, BlockFactory blockFactory) {
                this.blockFactory = blockFactory;
                this.next = nextValues.prepareForEmitting(blockFactory, selected);
            }

            @Override
            public void evaluate(Block[] blocks, int offset, IntVector selectedInPage) {
                blocks[offset] = buildOutputBlock(blockFactory, selectedInPage, next);
            }

            @Override
            public void close() {
                next.close();
            }
        }

        Block buildOutputBlock(BlockFactory blockFactory, IntVector selected, ValuesNextPreparedForEmitting next) {
            BytesRef scratch = new BytesRef(16);
            try (LongRangeBlock.Builder builder = blockFactory.newLongRangeBlockBuilder(selected.getPositionCount())) {
                LongRangeBlockBuilder.LongRange range = new LongRangeBlockBuilder.LongRange();
                for (int s = 0; s < selected.getPositionCount(); s++) {
                    int group = selected.getInt(s);
                    int firstValue = group >= firstValues.size() ? -1 : firstValues.get(group) - 1;
                    if (firstValue < 0) {
                        builder.appendNull();
                        continue;
                    }
                    final int nextValuesStart = next.nextValuesStart(group);
                    final int nextValuesEnd = next.nextValuesEnd(group);
                    if (nextValuesEnd == nextValuesStart) {
                        decode(bytes.get(firstValue, scratch), range);
                        builder.appendLongRange(range);
                    } else {
                        builder.beginPositionEntry();
                        decode(bytes.get(firstValue, scratch), range);
                        builder.appendLongRange(range);
                        for (int i = nextValuesStart; i < nextValuesEnd; i++) {
                            decode(bytes.get(nextValues.getInt(next, i), scratch), range);
                            builder.appendLongRange(range);
                        }
                        builder.endPositionEntry();
                    }
                }
                return builder.build();
            }
        }

        private static void decode(BytesRef encoded, LongRangeBlockBuilder.LongRange range) {
            long from = 0, to = 0;
            for (int i = 0; i < 8; i++) {
                from = (from << 8) | (encoded.bytes[encoded.offset + i] & 0xff);
                to = (to << 8) | (encoded.bytes[encoded.offset + 8 + i] & 0xff);
            }
            range.reset(from, to);
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(bytes, firstValues, nextValues);
        }
    }
}
