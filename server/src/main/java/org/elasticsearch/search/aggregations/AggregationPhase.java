/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.search.aggregations;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.search.SearchPhase;
import org.elasticsearch.search.aggregations.bucket.global.GlobalAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.SiblingPipelineAggregator;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.profile.query.CollectorResult;
import org.elasticsearch.search.profile.query.InternalProfileCollector;
import org.elasticsearch.search.query.QueryPhaseExecutionException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Aggregation phase of a search request, used to collect aggregations
 */
public class AggregationPhase implements SearchPhase {

    @Inject
    public AggregationPhase() {
    }

    @Override
    public void preProcess(SearchContext context) {
        if (context.aggregations() != null) {
            List<Aggregator> collectors = new ArrayList<>();
            Aggregator[] aggregators;
            try {
                AggregatorFactories factories = context.aggregations().factories();
                aggregators = factories.createTopLevelAggregators(context);
                for (int i = 0; i < aggregators.length; i++) {
                    if (aggregators[i] instanceof GlobalAggregator == false) {
                        collectors.add(aggregators[i]);
                    }
                }
                context.aggregations().aggregators(aggregators);
                if (!collectors.isEmpty()) {
                    Collector collector = MultiBucketCollector.wrap(collectors);
                    ((BucketCollector)collector).preCollection();
                    if (context.getProfilers() != null) {
                        collector = new InternalProfileCollector(collector, CollectorResult.REASON_AGGREGATION,
                                // TODO: report on child aggs as well
                                Collections.emptyList());
                    }
                    context.queryCollectors().put(AggregationPhase.class, collector);
                }
            } catch (IOException e) {
                throw new AggregationInitializationException("Could not initialize aggregators", e);
            }
        }
    }

    @Override
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
        List<Aggregator> globals = new ArrayList<>();
        for (int i = 0; i < aggregators.length; i++) {
            if (aggregators[i] instanceof GlobalAggregator) {
                globals.add(aggregators[i]);
            }
        }

        // optimize the global collector based execution
        if (!globals.isEmpty()) {
            BucketCollector globalsCollector = MultiBucketCollector.wrap(globals);
            Query query = context.buildFilteredQuery(Queries.newMatchAllQuery());

            try {
                final Collector collector;
                if (context.getProfilers() == null) {
                    collector = globalsCollector;
                } else {
                    InternalProfileCollector profileCollector = new InternalProfileCollector(
                            globalsCollector, CollectorResult.REASON_AGGREGATION_GLOBAL,
                            // TODO: report on sub collectors
                            Collections.emptyList());
                    collector = profileCollector;
                    // start a new profile with this collector
                    context.getProfilers().addQueryProfiler().setCollector(profileCollector);
                }
                globalsCollector.preCollection();
                context.searcher().search(query, collector);
            } catch (Exception e) {
                throw new QueryPhaseExecutionException(context.shardTarget(), "Failed to execute global aggregators", e);
            } finally {
                context.clearReleasables(SearchContext.Lifetime.COLLECTION);
            }
        }

        List<InternalAggregation> aggregations = new ArrayList<>(aggregators.length);
        context.aggregations().resetBucketMultiConsumer();
        for (Aggregator aggregator : context.aggregations().aggregators()) {
            try {
                aggregator.postCollection();
                aggregations.add(aggregator.buildAggregation(0));
            } catch (IOException e) {
                throw new AggregationExecutionException("Failed to build aggregation [" + aggregator.name() + "]", e);
            }
        }
        List<PipelineAggregator> pipelineAggregators = context.aggregations().factories().createPipelineAggregators();
        List<SiblingPipelineAggregator> siblingPipelineAggregators = new ArrayList<>(pipelineAggregators.size());
        for (PipelineAggregator pipelineAggregator : pipelineAggregators) {
            if (pipelineAggregator instanceof SiblingPipelineAggregator) {
                siblingPipelineAggregators.add((SiblingPipelineAggregator) pipelineAggregator);
            } else {
                throw new AggregationExecutionException("Invalid pipeline aggregation named [" + pipelineAggregator.name()
                    + "] of type [" + pipelineAggregator.getWriteableName() + "]. Only sibling pipeline aggregations are "
                    + "allowed at the top level");
            }
        }
        context.queryResult().aggregations(new InternalAggregations(aggregations, siblingPipelineAggregators));

        // disable aggregations so that they don't run on next pages in case of scrolling
        context.aggregations(null);
        context.queryCollectors().remove(AggregationPhase.class);
    }

}
