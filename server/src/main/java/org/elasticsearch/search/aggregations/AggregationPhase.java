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
import org.elasticsearch.search.query.QueryPhase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
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
        final BucketCollector bucketCollector;
        final Aggregator[] aggregators;
        try {
            aggregators = context.aggregations().factories().createTopLevelAggregators();
            bucketCollector = MultiBucketCollector.wrap(true, List.of(aggregators));
            bucketCollector.preCollection();
        } catch (IOException e) {
            throw new AggregationInitializationException("Could not initialize aggregators", e);
        }
        final Collector collector;
        if (context.aggregations().factories().context() != null
            && context.aggregations().factories().context().isInSortOrderExecutionRequired()) {
            TimeSeriesIndexSearcher searcher = new TimeSeriesIndexSearcher(context.searcher(), getCancellationChecks(context));
            searcher.setMinimumScore(context.minimumScore());
            searcher.setProfiler(context);
            try {
                searcher.search(context.rewrittenQuery(), bucketCollector);
            } catch (IOException e) {
                throw new AggregationExecutionException("Could not perform time series aggregation", e);
            }
            collector = BucketCollector.NO_OP_COLLECTOR;
        } else {
            collector = bucketCollector.asCollector();
        }
        final CollectorManager<Collector, Void> manager = new CollectorManager<>() {

            private boolean newCollectorCalled;

            @Override
            public Collector newCollector() {
                assert newCollectorCalled == false;
                newCollectorCalled = true;
                return collector;
            }

            @Override
            public Void reduce(Collection<Collector> collectors) {
                assert collectors.size() == 1;
                assert collector == collectors.iterator().next();
                final List<InternalAggregation> aggregations = new ArrayList<>(aggregators.length);
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
                context.queryResult().aggregations(InternalAggregations.from(aggregations));
                // disable aggregations so that they don't run on next pages in case of scrolling
                context.aggregations(null);
                return null;
            }
        };
        context.aggregations().registerAggsCollectorManager(manager);
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
