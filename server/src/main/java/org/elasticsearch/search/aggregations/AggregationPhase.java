/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations;

import org.apache.lucene.search.Collector;
import org.elasticsearch.action.search.SearchShardTask;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.aggregations.support.TimeSeriesIndexSearcher;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.profile.query.CollectorResult;
import org.elasticsearch.search.profile.query.InternalProfileCollectorManager;
import org.elasticsearch.search.query.QueryPhase;
import org.elasticsearch.search.query.SingleThreadCollectorManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Aggregation phase of a search request, used to collect aggregations
 */
public class AggregationPhase {
    @Inject
    public AggregationPhase() {}

    public static void preProcess(SearchContext context) {
        if (context.aggregations() == null) {
            return;
        }
        AggregationCollectorManager collectorManager = new AggregationCollectorManager(context);
        if (context.aggregations().factories().context() != null
            && context.aggregations().factories().context().isInSortOrderExecutionRequired()) {
            TimeSeriesIndexSearcher searcher = new TimeSeriesIndexSearcher(context.searcher(), getCancellationChecks(context));
            try {
                // Execute here the time_series aggregations. The collecting top docs for the main query is done in QueryPhase
                Collector collector = collectorManager.newCollector();
                searcher.search(context.rewrittenQuery(), collectorManager.bucketCollector);
                collectorManager.reduce(List.of(collector));
            } catch (IOException e) {
                throw new AggregationExecutionException("Could not perform time series aggregation", e);
            }
            context.registerAggsCollectorManager(SingleThreadCollectorManager.wrap(BucketCollector.NO_OP_COLLECTOR));
        } else {
            if (context.getProfilers() != null) {
                context.registerAggsCollectorManager(
                    new InternalProfileCollectorManager(collectorManager, CollectorResult.REASON_AGGREGATION, List.of())
                );
            } else {
                context.registerAggsCollectorManager(collectorManager);
            }
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

        boolean timeoutSet = context.scrollContext() == null
            && context.timeout() != null
            && context.timeout().equals(SearchService.NO_TIMEOUT) == false;

        if (timeoutSet) {
            final long startTime = context.getRelativeTimeInMillis();
            final long timeout = context.timeout().millis();
            final long maxTime = startTime + timeout;
            cancellationChecks.add(() -> {
                final long time = context.getRelativeTimeInMillis();
                if (time > maxTime) {
                    throw new QueryPhase.TimeExceededException();
                }
            });
        }
        return cancellationChecks;
    }

    private static class AggregationCollectorManager extends SingleThreadCollectorManager {
        private final SearchContext context;
        Aggregator[] aggregators;
        BucketCollector bucketCollector;

        AggregationCollectorManager(SearchContext context) {
            this.context = context;
        }

        @Override
        protected Collector getNewCollector() {
            try {
                aggregators = context.aggregations().factories().createTopLevelAggregators();
                bucketCollector = MultiBucketCollector.wrap(true, List.of(aggregators));
                bucketCollector.preCollection();
                return bucketCollector.asCollector();
            } catch (IOException e) {
                throw new AggregationInitializationException("Could not initialize aggregators", e);
            }
        }

        @Override
        protected void reduce(Collector collector) throws IOException {
            if (context.queryResult().hasAggs()) { // TODO: why would this happen?
                // no need to compute the aggs twice, they should be computed on a per context basis
                return;
            }
            List<InternalAggregation> aggregations = new ArrayList<>(aggregators.length);
            if (context.aggregations().factories().context() != null) { // TODO: not sure why we need this
                // Rollup can end up here with a null context but not null factories.....
                context.aggregations().factories().context().multiBucketConsumer().reset();
            }
            for (Aggregator aggregator : aggregators) {
                try {
                    aggregator.postCollection();
                    aggregations.add(aggregator.buildTopLevel());
                } catch (IOException e) {
                    throw new AggregationExecutionException("Failed to build aggregation [" + aggregator.name() + "]", e);
                }
            }
            // register the internal aggregations
            context.queryResult().aggregations(InternalAggregations.from(aggregations));
            // disable aggregations so that they don't run on next pages in case of scrolling
            context.aggregations(null);
            context.registerAggsCollectorManager(null);
        }
    }
}
