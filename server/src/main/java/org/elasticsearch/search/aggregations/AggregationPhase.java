/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations;

import org.elasticsearch.action.search.SearchShardTask;
import org.elasticsearch.search.aggregations.support.TimeSeriesIndexSearcher;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.query.QueryPhase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * Aggregation phase of a search request, used to collect aggregations
 */
public class AggregationPhase {

    private AggregationPhase() {}

    public static void preProcess(SearchContext context) {
        if (context.aggregations() == null) {
            return;
        }
        final Supplier<AggregatorCollector> collectorSupplier;
        if (context.aggregations().isInSortOrderExecutionRequired()) {
            AggregatorCollector collector = newAggregatorCollector(context);
            executeInSortOrder(context, collector.bucketCollector);
            collectorSupplier = () -> new AggregatorCollector(collector.aggregators, BucketCollector.NO_OP_BUCKET_COLLECTOR);
        } else {
            collectorSupplier = () -> newAggregatorCollector(context);
        }
        context.aggregations()
            .registerAggsCollectorManager(
                new AggregatorCollectorManager(
                    collectorSupplier,
                    internalAggregations -> context.queryResult().aggregations(internalAggregations),
                    () -> context.aggregations().getAggregationReduceContextBuilder().forPartialReduction()
                )
            );
    }

    private static AggregatorCollector newAggregatorCollector(SearchContext context) {
        try {
            Aggregator[] aggregators = context.aggregations().factories().createTopLevelAggregators();
            BucketCollector bucketCollector = MultiBucketCollector.wrap(true, List.of(aggregators));
            bucketCollector.preCollection();
            return new AggregatorCollector(aggregators, bucketCollector);
        } catch (IOException e) {
            throw new AggregationInitializationException("Could not initialize aggregators", e);
        }
    }

    private static void executeInSortOrder(SearchContext context, BucketCollector collector) {
        TimeSeriesIndexSearcher searcher = new TimeSeriesIndexSearcher(context.searcher(), getCancellationChecks(context));
        searcher.setMinimumScore(context.minimumScore());
        searcher.setProfiler(context);
        try {
            searcher.search(context.rewrittenQuery(), collector);
        } catch (IOException e) {
            // Seems like this should be 400 (non-retryable), but we clearly intentionally throw a 500 here. Why?
            throw new AggregationExecutionException("Could not perform time series aggregation", e);
        }
    }

    private static List<Runnable> getCancellationChecks(SearchContext context) {
        List<Runnable> cancellationChecks = new ArrayList<>();
        if (context.lowLevelCancellation()) {
            // This searching doesn't live beyond this phase, so we don't need to remove query cancellation
            cancellationChecks.add(() -> {
                final SearchShardTask task = context.getTask();
                if (task != null) {
                    task.ensureNotCancelled();
                }
            });
        }

        final Runnable timeoutRunnable = QueryPhase.getTimeoutCheck(context);
        if (timeoutRunnable != null) {
            cancellationChecks.add(timeoutRunnable);
        }

        return cancellationChecks;
    }
}
