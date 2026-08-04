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
import org.elasticsearch.compute.data.DoubleRangeBlock;
import org.elasticsearch.compute.data.DoubleRangeBlockBuilder;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;

/**
 * Aggregates distinct {@code double_range} values.
 */
@Aggregator({ @IntermediateState(name = "values", type = "DOUBLE_RANGE_BLOCK") })
@GroupingAggregator
class ValuesDoubleRangeAggregator {

    public static SingleState initSingle(DriverContext driverContext) {
        return new SingleState(driverContext.blockFactory());
    }

    public static void combine(SingleState state, DoubleRangeBlockBuilder.DoubleRange value) {
        state.values.add(Double.doubleToLongBits(value.from()), Double.doubleToLongBits(value.to()));
    }

    public static void combineIntermediate(SingleState state, DoubleRangeBlock values) {
        DoubleRangeBlockBuilder.DoubleRange scratch = new DoubleRangeBlockBuilder.DoubleRange();
        int start = values.getFirstValueIndex(0);
        int end = start + values.getValueCount(0);
        for (int i = start; i < end; i++) {
            DoubleRangeBlockBuilder.DoubleRange value = values.getDoubleRange(i, scratch);
            state.values.add(Double.doubleToLongBits(value.from()), Double.doubleToLongBits(value.to()));
        }
    }

    public static Block evaluateFinal(SingleState state, DriverContext driverContext) {
        return state.toBlock(driverContext.blockFactory());
    }

    public static GroupingState initGrouping(DriverContext driverContext) {
        return new GroupingState(driverContext);
    }

    public static void combine(GroupingState state, int groupId, DoubleRangeBlockBuilder.DoubleRange value) {
        state.addValue(groupId, value.from(), value.to());
    }

    public static void combineIntermediate(GroupingState state, int groupId, DoubleRangeBlock values, int valuesPosition) {
        DoubleRangeBlockBuilder.DoubleRange scratch = new DoubleRangeBlockBuilder.DoubleRange();
        int start = values.getFirstValueIndex(valuesPosition);
        int end = start + values.getValueCount(valuesPosition);
        for (int i = start; i < end; i++) {
            DoubleRangeBlockBuilder.DoubleRange value = values.getDoubleRange(i, scratch);
            state.addValue(groupId, value.from(), value.to());
        }
    }

    public static GroupingAggregatorFunction.PreparedForEvaluation prepareEvaluateIntermediate(
        GroupingState state,
        IntVector selected,
        GroupingAggregatorEvaluationContext context
    ) {
        return state.prepareForEmitting(context.blockFactory(), selected);
    }

    public static GroupingAggregatorFunction.PreparedForEvaluation prepareEvaluateFinal(
        GroupingState state,
        IntVector selected,
        GroupingAggregatorEvaluationContext context
    ) {
        return state.prepareForEmitting(context.blockFactory(), selected);
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
            try (DoubleRangeBlock.Builder builder = blockFactory.newDoubleRangeBlockBuilder((int) values.size())) {
                if (values.size() > 1) {
                    builder.beginPositionEntry();
                }
                for (long id = 0; id < values.size(); id++) {
                    builder.appendDoubleRange(Double.longBitsToDouble(values.getKey1(id)), Double.longBitsToDouble(values.getKey2(id)));
                }
                if (values.size() > 1) {
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
     * State for grouped {@code VALUES} over {@code double_range} values.
     */
    public static class GroupingState implements GroupingAggregatorState {
        private final BlockFactory blockFactory;
        private final BytesRefHash bytes;
        private IntArray firstValues;
        private final ValuesNextLong nextValues;
        private final byte[] encodeBuffer = new byte[16];
        private final BytesRef encodeScratch = new BytesRef(encodeBuffer, 0, 16);

        private GroupingState(DriverContext driverContext) {
            blockFactory = driverContext.blockFactory();
            BytesRefHash newBytes = null;
            IntArray newFirstValues = null;
            ValuesNextLong newNextValues = null;
            boolean success = false;
            try {
                newBytes = new BytesRefHash(1, driverContext.bigArrays());
                newFirstValues = driverContext.bigArrays().newIntArray(1, true);
                newNextValues = new ValuesNextLong(driverContext.blockFactory());
                success = true;
            } finally {
                if (success == false) {
                    Releasables.closeExpectNoException(newBytes, newFirstValues, newNextValues);
                }
            }
            bytes = newBytes;
            firstValues = newFirstValues;
            nextValues = newNextValues;
        }

        void addValue(int groupId, double from, double to) {
            int valueOrdinal = Math.toIntExact(
                BlockHash.hashOrdToGroup(bytes.add(encode(Double.doubleToLongBits(from), Double.doubleToLongBits(to))))
            );
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

        private BytesRef encode(long from, long to) {
            for (int i = 7; i >= 0; i--) {
                encodeBuffer[i] = (byte) (from & 0xff);
                from >>= 8;
                encodeBuffer[8 + i] = (byte) (to & 0xff);
                to >>= 8;
            }
            return encodeScratch;
        }

        @Override
        public void enableGroupIdTracking(SeenGroupIds seenGroupIds) {}

        GroupingAggregatorFunction.PreparedForEvaluation prepareForEmitting(BlockFactory blockFactory, IntVector selected) {
            return new PreparedForEmitting(selected, blockFactory);
        }

        private class PreparedForEmitting implements GroupingAggregatorFunction.PreparedForEvaluation {
            private final BlockFactory blockFactory;
            private final ValuesNextPreparedForEmitting next;

            private PreparedForEmitting(IntVector selected, BlockFactory blockFactory) {
                this.blockFactory = blockFactory;
                next = nextValues.prepareForEmitting(blockFactory, selected);
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
            try (DoubleRangeBlock.Builder builder = blockFactory.newDoubleRangeBlockBuilder(selected.getPositionCount())) {
                DoubleRangeBlockBuilder.DoubleRange range = new DoubleRangeBlockBuilder.DoubleRange();
                for (int s = 0; s < selected.getPositionCount(); s++) {
                    int group = selected.getInt(s);
                    int firstValue = group >= firstValues.size() ? -1 : firstValues.get(group) - 1;
                    if (firstValue < 0) {
                        builder.appendNull();
                        continue;
                    }
                    int nextValuesStart = next.nextValuesStart(group);
                    int nextValuesEnd = next.nextValuesEnd(group);
                    if (nextValuesEnd == nextValuesStart) {
                        builder.appendDoubleRange(decode(bytes.get(firstValue, scratch), range));
                    } else {
                        builder.beginPositionEntry();
                        builder.appendDoubleRange(decode(bytes.get(firstValue, scratch), range));
                        for (int i = nextValuesStart; i < nextValuesEnd; i++) {
                            builder.appendDoubleRange(decode(bytes.get(nextValues.getInt(next, i), scratch), range));
                        }
                        builder.endPositionEntry();
                    }
                }
                return builder.build();
            }
        }

        private static DoubleRangeBlockBuilder.DoubleRange decode(BytesRef encoded, DoubleRangeBlockBuilder.DoubleRange range) {
            long from = 0;
            long to = 0;
            for (int i = 0; i < 8; i++) {
                from = (from << 8) | (encoded.bytes[encoded.offset + i] & 0xff);
                to = (to << 8) | (encoded.bytes[encoded.offset + 8 + i] & 0xff);
            }
            return range.reset(Double.longBitsToDouble(from), Double.longBitsToDouble(to));
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(bytes, firstValues, nextValues);
        }
    }
}
