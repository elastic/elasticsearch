/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.aggregations;

import org.elasticsearch.search.aggregations.support.TimeSeriesIndexSearcher;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.query.QueryPhaseExecutionException;

import java.io.IOException;
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

            final List<Runnable> cancellationChecks = context.getCancellationChecks();
            rewriteWithCancellation(context, cancellationChecks);
            if (context.searcher().timeExceeded()) {
                return;
            }
            AggregatorCollector collector = newAggregatorCollector(context);
            executeInSortOrder(context, collector.bucketCollector, cancellationChecks);
            collectorSupplier = () -> new AggregatorCollector(collector.aggregators, BucketCollector.NO_OP_BUCKET_COLLECTOR);
        } else {
            collectorSupplier = () -> newAggregatorCollector(context);
        }
        context.aggregations()
            .registerAggsCollectorManager(
                new AggregatorCollectorManager(
                    collectorSupplier,
                    internalAggregations -> context.queryResult().aggregations(internalAggregations),
                    () -> context.aggregations()
                        .getAggregationReduceContextBuilder()
                        .forPartialReduction(context.queryResult().topHitsToReleaseCollector())
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

    private static void rewriteWithCancellation(SearchContext context, List<Runnable> cancellationChecks) {
        ContextIndexSearcher searcher = context.searcher();
        cancellationChecks.forEach(searcher::addQueryCancellation);
        try {
            context.rewrittenQuery();
        } catch (RuntimeException e) {
            throw new QueryPhaseExecutionException(context.shardTarget(), "Failed to rewrite query", e);
        } finally {
            cancellationChecks.forEach(searcher::removeQueryCancellation);
        }
    }

    private static void executeInSortOrder(SearchContext context, BucketCollector collector, List<Runnable> cancellationChecks) {
        TimeSeriesIndexSearcher searcher = new TimeSeriesIndexSearcher(context.searcher(), cancellationChecks);
        searcher.setMinimumScore(context.minimumScore());
        searcher.setProfiler(context);
        try {
            searcher.search(context.rewrittenQuery(), collector);
        } catch (IOException e) {
            // Seems like this should be 400 (non-retryable), but we clearly intentionally throw a 500 here. Why?
            throw new AggregationExecutionException("Could not perform time series aggregation", e);
        }
    }

}
