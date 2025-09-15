/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import java.util.List;
import java.util.function.Function;

public class TimeSeriesSourceOperatorFactory extends LuceneSourceOperator.Factory {
    private TimeSeriesSourceOperatorFactory(
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

    public static TimeSeriesSourceOperatorFactory create(
        int limit,
        int maxPageSize,
        int taskConcurrency,
        IndexedByShardId<? extends ShardContext> contexts,
        Function<ShardContext, List<LuceneSliceQueue.QueryAndTags>> queryFunction
    ) {
        // TODO: custom slice and return the next max_timestamp
        return new TimeSeriesSourceOperatorFactory(contexts, queryFunction, taskConcurrency, maxPageSize, limit);
    }
}
