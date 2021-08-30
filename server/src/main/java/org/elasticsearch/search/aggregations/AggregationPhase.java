/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations;

import org.apache.lucene.search.Collector;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.profile.query.CollectorResult;
import org.elasticsearch.search.profile.query.InternalProfileCollector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Aggregation phase of a search request, used to collect aggregations
 */
public class AggregationPhase {

    @Inject
    public AggregationPhase() {}

    public void preProcess(SearchContext context) {
        if (context.aggregations() == null) {
            return;
        }
        BucketCollector bucketCollector;
        try {
            context.aggregations().aggregators(context.aggregations().factories().createTopLevelAggregators());
            bucketCollector = MultiBucketCollector.wrap(true, org.elasticsearch.core.List.of(context.aggregations().aggregators()));
            bucketCollector.preCollection();
        } catch (IOException e) {
            throw new AggregationInitializationException("Could not initialize aggregators", e);
        }
        Collector collector = context.getProfilers() == null
            ? bucketCollector
            : new InternalProfileCollector(bucketCollector, CollectorResult.REASON_AGGREGATION, org.elasticsearch.core.List.of());
        context.queryCollectors().put(AggregationPhase.class, collector);
    }

    public void execute(SearchContext context) {
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
        if (context.aggregations().factories().context() != null) {
            // Rollup can end up here with a null context but not null factories.....
            context.aggregations().factories().context().multiBucketConsumer().reset();
        }
        for (Aggregator aggregator : context.aggregations().aggregators()) {
            try {
                aggregator.postCollection();
                aggregations.add(aggregator.buildTopLevel());
            } catch (IOException e) {
                throw new AggregationExecutionException("Failed to build aggregation [" + aggregator.name() + "]", e);
            }
        }
        context.queryResult()
            .aggregations(new InternalAggregations(aggregations, context.request().source().aggregations()::buildPipelineTree));

        // disable aggregations so that they don't run on next pages in case of scrolling
        context.aggregations(null);
        context.queryCollectors().remove(AggregationPhase.class);
    }
}
