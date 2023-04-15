/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.ScoreMode;
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
                // TODO: we are executing here the time series aggregation and let we will execute the query?
                AggregatorCollector aggregatorCollector = (AggregatorCollector) collectorManager.newCollector();
                searcher.search(context.rewrittenQuery(), aggregatorCollector.bucketCollector);
                collectorManager.reduce(List.of(aggregatorCollector));
            } catch (IOException e) {
                throw new AggregationExecutionException("Could not perform time series aggregation", e);
            }
            context.registerAggsCollectorManager(SingleThreadCollectorManager.wrap(BucketCollector.NO_OP_COLLECTOR));
        } else {
            if (context.getProfilers() != null) {fi
                context.registerAggsCollectorManager(
                    new InternalProfileCollectorManager(collectorManager, CollectorResult.REASON_AGGREGATION)
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

        AggregationCollectorManager(SearchContext context) {
            this.context = context;
        }

        @Override
        protected Collector getNewCollector() {
            try {
                return new AggregatorCollector(context.aggregations().factories().createTopLevelAggregators());
            } catch (IOException e) {
                throw new AggregationInitializationException("Could not initialize aggregators", e);
            }
        }

        @Override
        protected void reduce(Collector collector) throws IOException {
            assert collector instanceof AggregatorCollector;
            if (context.queryResult().hasAggs()) { // TODO: why would this happen?
                // no need to compute the aggs twice, they should be computed on a per context basis
                return;
            }
            Aggregator[] aggregators = ((AggregatorCollector) collector).aggregators;
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

    private static class AggregatorCollector implements Collector {

        final Aggregator[] aggregators;
        final Collector collector;
        final BucketCollector bucketCollector;

        AggregatorCollector(Aggregator[] aggregators) throws IOException {
            this.aggregators = aggregators;
            bucketCollector = MultiBucketCollector.wrap(true, List.of(aggregators));
            bucketCollector.preCollection();
            collector = bucketCollector.asCollector();
        }

        @Override
        public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
            return collector.getLeafCollector(context);
        }

        @Override
        public ScoreMode scoreMode() {
            return collector.scoreMode();
        }
    }
}
