/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.search.MultiValueMode;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

/**
 * Factory that generates an operator that finds the max value of a field using the {@link LuceneMinMaxOperator}.
 */
public final class LuceneMaxFactory extends LuceneOperator.Factory {

    public enum NumberType implements LuceneMinMaxOperator.NumberType {
        INTEGER {
            @Override
            public Block buildResult(BlockFactory blockFactory, long result, int pageSize) {
                return blockFactory.newConstantIntBlockWith(Math.toIntExact(result), pageSize);
            }

            @Override
            public Block buildEmptyResult(BlockFactory blockFactory, int pageSize) {
                return blockFactory.newConstantIntBlockWith(Integer.MIN_VALUE, pageSize);
            }

            @Override
            long bytesToLong(byte[] bytes) {
                return NumericUtils.sortableBytesToInt(bytes, 0);
            }
        },
        FLOAT {
            @Override
            public Block buildResult(BlockFactory blockFactory, long result, int pageSize) {
                return blockFactory.newConstantFloatBlockWith(NumericUtils.sortableIntToFloat(Math.toIntExact(result)), pageSize);
            }

            @Override
            public Block buildEmptyResult(BlockFactory blockFactory, int pageSize) {
                return blockFactory.newConstantFloatBlockWith(-Float.MAX_VALUE, pageSize);
            }

            @Override
            long bytesToLong(byte[] bytes) {
                return NumericUtils.sortableBytesToInt(bytes, 0);
            }
        },
        LONG {
            @Override
            public Block buildResult(BlockFactory blockFactory, long result, int pageSize) {
                return blockFactory.newConstantLongBlockWith(result, pageSize);
            }

            @Override
            public Block buildEmptyResult(BlockFactory blockFactory, int pageSize) {
                return blockFactory.newConstantLongBlockWith(Long.MIN_VALUE, pageSize);
            }

            @Override
            long bytesToLong(byte[] bytes) {
                return NumericUtils.sortableBytesToLong(bytes, 0);
            }
        },
        DOUBLE {
            @Override
            public Block buildResult(BlockFactory blockFactory, long result, int pageSize) {
                return blockFactory.newConstantDoubleBlockWith(NumericUtils.sortableLongToDouble(result), pageSize);
            }

            @Override
            public Block buildEmptyResult(BlockFactory blockFactory, int pageSize) {
                return blockFactory.newConstantDoubleBlockWith(-Double.MAX_VALUE, pageSize);
            }

            @Override
            long bytesToLong(byte[] bytes) {
                return NumericUtils.sortableBytesToLong(bytes, 0);
            }
        };

        public final NumericDocValues multiValueMode(SortedNumericDocValues sortedNumericDocValues) {
            return MultiValueMode.MAX.select(sortedNumericDocValues);
        }

        public final long fromPointValues(PointValues pointValues) throws IOException {
            return bytesToLong(pointValues.getMaxPackedValue());
        }

        public final long evaluate(long value1, long value2) {
            return Math.max(value1, value2);
        }

        abstract long bytesToLong(byte[] bytes);
    }

    private final String fieldName;
    private final NumberType numberType;

    public LuceneMaxFactory(
        List<? extends ShardContext> contexts,
        Function<ShardContext, Query> queryFunction,
        DataPartitioning dataPartitioning,
        int taskConcurrency,
        String fieldName,
        NumberType numberType,
        int limit
    ) {
        super(
            contexts,
            queryFunction,
            dataPartitioning,
            query -> LuceneSliceQueue.PartitioningStrategy.SHARD,
            taskConcurrency,
            limit,
            false,
            ScoreMode.COMPLETE_NO_SCORES
        );
        this.fieldName = fieldName;
        this.numberType = numberType;
    }

    @Override
    public SourceOperator get(DriverContext driverContext) {
        return new LuceneMinMaxOperator(driverContext.blockFactory(), sliceQueue, fieldName, numberType, limit, Long.MIN_VALUE);
    }

    @Override
    public String describe() {
        return "LuceneMaxOperator[type = "
            + numberType.name()
            + ", dataPartitioning = "
            + dataPartitioning
            + ", fieldName = "
            + fieldName
            + ", limit = "
            + limit
            + "]";
    }
}
