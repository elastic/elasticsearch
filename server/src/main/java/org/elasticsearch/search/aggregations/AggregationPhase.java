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

    private AggregationPhase() {}

    public static void preProcess(SearchContext context) {
        if (context.aggregations() == null) {
            return;
        }
        BucketCollector bucketCollector;
        try {
            context.aggregations().aggregators(context.aggregations().factories().createTopLevelAggregators());
            bucketCollector = MultiBucketCollector.wrap(true, List.of(context.aggregations().aggregators()));
            bucketCollector.preCollection();
        } catch (IOException e) {
            throw new AggregationInitializationException("Could not initialize aggregators", e);
        }
        final Collector collector;
        if (context.aggregations().factories().context() != null
            && context.aggregations().factories().context().isInSortOrderExecutionRequired()) {
            TimeSeriesIndexSearcher searcher = new TimeSeriesIndexSearcher(context.searcher(), getCancellationChecks(context));
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
        CollectorManager<? extends Collector, Void> aggsCollectorManager = new SingleThreadCollectorManager(collector);
        if (context.getProfilers() != null) {
            aggsCollectorManager = new InternalProfileCollectorManager(aggsCollectorManager, CollectorResult.REASON_AGGREGATION);
        }
        context.aggregations().registerAggsCollectorManager(aggsCollectorManager);
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

        Aggregator[] aggregators = context.aggregations().aggregators();

        List<InternalAggregation> aggregations = new ArrayList<>(aggregators.length);
        for (Aggregator aggregator : context.aggregations().aggregators()) {
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
    }
}
