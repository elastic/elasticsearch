/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.elasticsearch.action.search.SearchShardTask;
import org.elasticsearch.search.aggregations.support.TimeSeriesIndexSearcher;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.profile.query.CollectorResult;
import org.elasticsearch.search.profile.query.InternalProfileCollector;
import org.elasticsearch.search.profile.query.InternalProfileCollectorManager;
import org.elasticsearch.search.query.BaseCollectorManager;
import org.elasticsearch.search.query.QueryPhase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Aggregation phase of a search request, used to collect aggregations
 */
public class AggregationPhase {

    private AggregationPhase() {}

    public static void preProcess(SearchContext context) {
        if (context.aggregations() == null) {
            return;
        }
        final CollectorManager<Collector, Void> collectorManager;
        if (context.aggregations().isInSortOrderExecutionRequired()) {
            collectorManager = buildTimeSeriesCollector(context);
        } else {
            collectorManager = buildCollector(context);
        }

        if (context.getProfilers() != null) {
            try {
                InternalProfileCollector profileCollector = new InternalProfileCollector(
                    collectorManager.newCollector(),
                    CollectorResult.REASON_AGGREGATION
                );
                context.aggregations().registerAggsCollectorManager(new InternalProfileCollectorManager(profileCollector));
            } catch (Exception e) {
                throw new AggregationInitializationException("Could not initialize aggregators", e);
            }
        } else {
            context.aggregations().registerAggsCollectorManager(collectorManager);
        }
    }

    private static CollectorManager<Collector, Void> buildCollector(SearchContext context) {
        return new BaseCollectorManager() {
            @Override
            public Collector newCollector() throws IOException {
                Aggregator[] aggregators = context.aggregations().factories().createTopLevelAggregators();
                context.aggregations().aggregators(aggregators);
                BucketCollector bucketCollector = MultiBucketCollector.wrap(true, List.of(aggregators));
                bucketCollector.preCollection();
                return bucketCollector.asCollector();
            }
        };
    }

    private static CollectorManager<Collector, Void> buildTimeSeriesCollector(SearchContext context) {
        BucketCollector bucketCollector;
        try {
            Aggregator[] aggregators = context.aggregations().factories().createTopLevelAggregators();
            context.aggregations().aggregators(aggregators);
            bucketCollector = MultiBucketCollector.wrap(true, List.of(aggregators));
            bucketCollector.preCollection();
        } catch (IOException e) {
            throw new AggregationInitializationException("Could not initialize aggregators", e);
        }

        TimeSeriesIndexSearcher searcher = new TimeSeriesIndexSearcher(context.searcher(), getCancellationChecks(context));
        searcher.setProfiler(context);
        try {
            searcher.search(context.rewrittenQuery(), bucketCollector);
        } catch (IOException e) {
            throw new AggregationExecutionException("Could not perform time series aggregation", e);
        }

        return new BaseCollectorManager() {
            @Override
            public Collector newCollector() {
                return BucketCollector.NO_OP_COLLECTOR;
            }
        };
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

    public static void execute(SearchContext context) {
        if (context.aggregations() == null) {
            context.queryResult().aggregations(null);
            return;
        }

        if (context.queryResult().hasAggs()) {
            // no need to compute the aggs twice, they should be computed on a per context basis
            return;
        }

        List<InternalAggregations> internalAggregations = new ArrayList<>(context.aggregations().aggregators().size());
        for (Aggregator[] aggregators : context.aggregations().aggregators()) {
            List<InternalAggregation> aggregations = new ArrayList<>(aggregators.length);
            for (Aggregator aggregator : aggregators) {
                try {
                    aggregator.postCollection();
                    aggregations.add(aggregator.buildTopLevel());
                } catch (IOException e) {
                    throw new AggregationExecutionException("Failed to build aggregation [" + aggregator.name() + "]", e);
                }
                // release the aggregator to claim the used bytes as we don't need it anymore
                aggregator.releaseAggregations();
            }
            internalAggregations.add(InternalAggregations.from(aggregations));
        }

        if (internalAggregations.size() > 1) {
            context.queryResult()
                .aggregations(
                    InternalAggregations.topLevelReduce(
                        internalAggregations,
                        context.aggregations().getAggregationReduceContextBuilder().forPartialReduction()
                    )
                );
        } else {
            context.queryResult().aggregations(internalAggregations.get(0));
        }

        // disable aggregations so that they don't run on next pages in case of scrolling
        context.aggregations(null);
    }
}
