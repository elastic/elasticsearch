/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;

import java.util.List;
import java.util.function.Function;

/**
 * Creates a source operator that takes advantage of the natural sorting of segments in a tsdb index.
 * <p>
 * This source operator loads the _tsid and @timestamp fields, which is used for emitting documents in the correct order. These field values
 * are included in the page as seperate blocks and downstream operators can make use of these loaded time series ids and timestamps.
 * <p>
 * The source operator includes all documents of a time serie with the same page. So the same time series never exists in multiple pages.
 * Downstream operators can make use of this implementation detail.
 * <p>
 * This operator currently only supports shard level concurrency. A new concurrency mechanism should be introduced at the time serie level
 * in order to read tsdb indices in parallel.
 */
public class TimeSeriesSourceOperatorFactory extends LuceneOperator.Factory {
    private final List<? extends ShardContext> contexts;
    private final int maxPageSize;

    private TimeSeriesSourceOperatorFactory(
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
            limit,
            false,
            ScoreMode.COMPLETE_NO_SCORES
        );
        this.contexts = contexts;
        this.maxPageSize = maxPageSize;
    }

    @Override
    public SourceOperator get(DriverContext driverContext) {
        return new TimeSeriesSourceOperator(contexts, driverContext.blockFactory(), sliceQueue, maxPageSize, limit);
    }

    @Override
    public String describe() {
        return "TimeSeriesSourceOperator[maxPageSize = " + maxPageSize + ", limit = " + limit + "]";
    }

    public static TimeSeriesSourceOperatorFactory create(
        int limit,
        int maxPageSize,
        int taskConcurrency,
        List<? extends ShardContext> contexts,
        Function<ShardContext, List<LuceneSliceQueue.QueryAndTags>> queryFunction
    ) {
        return new TimeSeriesSourceOperatorFactory(contexts, queryFunction, taskConcurrency, maxPageSize, limit);
    }
}
