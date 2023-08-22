/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations;

import org.apache.lucene.search.CollectorManager;
import org.elasticsearch.action.search.SearchShardTask;
import org.elasticsearch.search.aggregations.support.TimeSeriesIndexSearcher;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.query.QueryPhase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
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
        context.aggregations().registerAggsCollectorManager(new CollectorManager<>() {
            @Override
            public AggregatorCollector newCollector() {
                return collectorSupplier.get();
            }

            @Override
            public Void reduce(Collection<AggregatorCollector> collectors) {
                if (context.queryResult().hasAggs()) {
                    // no need to compute the aggs twice, they should be computed on a per context basis
                    return null;
                }
                if (collectors.size() > 1) {
                    // we execute this search using more than one slice. In order to keep memory requirements
                    // low, we do a partial reduction here.
                    final List<InternalAggregations> internalAggregations = new ArrayList<>(collectors.size());
                    collectors.forEach(c -> internalAggregations.add(InternalAggregations.from(c.internalAggregations)));
                    context.queryResult()
                        .aggregations(
                            InternalAggregations.topLevelReduce(
                                internalAggregations,
                                context.aggregations().getAggregationReduceContextBuilder().forPartialReduction()
                            )
                        );
                } else if (collectors.size() == 1) {
                    context.queryResult().aggregations(InternalAggregations.from(collectors.iterator().next().internalAggregations));
                }
                // disable aggregations so that they don't run on next pages in case of scrolling
                context.aggregations(null);
                return null;
            }
        });
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
            collector.postCollection();
        } catch (IOException e) {
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
