/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Limiter;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.RefCounted;

import java.util.List;
import java.util.function.Function;

/**
 * Extension of {@link LuceneSourceOperator} for time-series aggregation that inserts metadata blocks,
 * such as slice index and future max timestamp, to allow downstream operators to optimize processing.
 */
public final class TimeSeriesSourceOperator extends LuceneSourceOperator {

    public static final class Factory extends LuceneSourceOperator.Factory {
        public Factory(
            List<? extends ShardContext> contexts,
            Function<ShardContext, List<LuceneSliceQueue.QueryAndTags>> queryFunction,
            int taskConcurrency,
            int maxPageSize,
            int limit
        ) {
            super(
                contexts,
                queryFunction,
                DataPartitioning.SHARD,
                query -> { throw new UnsupportedOperationException("locked to SHARD partitioning"); },
                taskConcurrency,
                maxPageSize,
                limit,
                false
            );
        }

        @Override
        public SourceOperator get(DriverContext driverContext) {
            return new TimeSeriesSourceOperator(contexts, driverContext.blockFactory(), maxPageSize, sliceQueue, limit, limiter);
        }

        @Override
        public String describe() {
            return "TimeSeriesSourceOperator[maxPageSize = " + maxPageSize + ", limit = " + limit + "]";
        }
    }

    public TimeSeriesSourceOperator(
        List<? extends RefCounted> shardContextCounters,
        BlockFactory blockFactory,
        int maxPageSize,
        LuceneSliceQueue sliceQueue,
        int limit,
        Limiter limiter
    ) {
        super(shardContextCounters, blockFactory, maxPageSize, sliceQueue, limit, limiter, false);
    }

    @Override
    protected int numMetadataBlocks() {
        // See EsQueryExec#TIME_SERIES_SOURCE_FIELDS
        return 2;
    }

    @Override
    protected void buildMetadataBlocks(Block[] blocks, int offset, int currentPagePos) {
        blocks[offset] = blockFactory.newConstantIntVector(currentSlice.slicePosition(), currentPagePos).asBlock();
        blocks[offset + 1] = blockFactory.newConstantLongVector(Long.MAX_VALUE, currentPagePos).asBlock();
    }
}
