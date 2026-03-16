/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.lucene.ShardContext;
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
    private static final int MAX_TARGET_PAGE_SIZE = 2048;
    private static final int CHUNK_SIZE = 128;

    public static final class Factory extends LuceneSourceOperator.Factory {
        public Factory(
            IndexedByShardId<? extends ShardContext> contexts,
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
            return new TimeSeriesSourceOperator(refCounteds, driverContext.blockFactory(), maxPageSize, sliceQueue, limit, limiter);
        }

        @Override
        public String describe() {
            return "TimeSeriesSourceOperator[maxPageSize = " + maxPageSize + ", limit = " + limit + "]";
        }
    }

    public TimeSeriesSourceOperator(
        IndexedByShardId<? extends RefCounted> shardContextCounters,
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

    /**
     * For time-series queries, try to use a page size that is a multiple of CHUNK_SIZE (see NUMERIC_BLOCK_SIZE in the tsdb codec)
     * to enable bulk loading of numeric or tsid fields. Avoid pages that are too large, as this can disable bulk loading if there are
     * holes in the doc IDs and disable constant block optimizations. Therefore, we cap the page size at 2048, which balances the
     * overhead per page with the benefits of bulk loading and constant blocks.
     */
    public static int pageSize(long estimateRowSizeInBytes, long maxPageSizeInBytes) {
        long chunkSizeInBytes = CHUNK_SIZE * estimateRowSizeInBytes;
        long numChunks = Math.ceilDiv(maxPageSizeInBytes, chunkSizeInBytes);
        long pageSize = Math.clamp(numChunks * CHUNK_SIZE, CHUNK_SIZE, MAX_TARGET_PAGE_SIZE);
        return Math.toIntExact(pageSize);
    }
}
