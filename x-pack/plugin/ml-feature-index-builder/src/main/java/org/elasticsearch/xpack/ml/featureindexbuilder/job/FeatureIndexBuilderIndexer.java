/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.featureindexbuilder.job;

import org.apache.log4j.Logger;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.indexing.AsyncTwoPhaseIndexer;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.indexing.IterationResult;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings.DOC_TYPE;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public abstract class FeatureIndexBuilderIndexer extends AsyncTwoPhaseIndexer<Map<String, Object>, FeatureIndexBuilderJobStats> {
    private static final String PIVOT_INDEX = "pivot-reviews";
    private static final String SOURCE_INDEX = "anonreviews";

    private static final Logger logger = Logger.getLogger(FeatureIndexBuilderIndexer.class.getName());
    private FeatureIndexBuilderJob job;

    public FeatureIndexBuilderIndexer(Executor executor, FeatureIndexBuilderJob job, AtomicReference<IndexerState> initialState,
            Map<String, Object> initialPosition) {
        super(executor, initialState, initialPosition, new FeatureIndexBuilderJobStats());

        this.job = job;
    }

    @Override
    protected String getJobId() {
        return job.getConfig().getId();
    }

    @Override
    protected void onStartJob(long now) {
    }

    @Override
    protected IterationResult<Map<String, Object>> doProcess(SearchResponse searchResponse) {
        final CompositeAggregation agg = searchResponse.getAggregations().get("feature");
        return new IterationResult<>(processBuckets(agg), agg.afterKey(), agg.getBuckets().isEmpty());
    }

    /*
     * Mocked demo case
     *
     * TODO: replace with proper implementation
     */
    private List<IndexRequest> processBuckets(CompositeAggregation agg) {
        return agg.getBuckets().stream().map(b -> {
            InternalAvg avgAgg = b.getAggregations().get("avg_rating");
            XContentBuilder builder;
            try {
                builder = jsonBuilder();

                builder.startObject();
                builder.field("reviewerId", b.getKey().get("reviewerId"));
                builder.field("avg_rating", avgAgg.getValue());
                builder.endObject();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            String indexName = PIVOT_INDEX + "_" + job.getConfig().getId();
            IndexRequest request = new IndexRequest(indexName, DOC_TYPE).source(builder);
            return request;
        }).collect(Collectors.toList());
    }

    @Override
    protected SearchRequest buildSearchRequest() {

        final Map<String, Object> position = getPosition();
        SearchRequest request = buildFeatureQuery(position);
        return request;
    }

    /*
     * Mocked demo case
     * 
     * TODO: everything below will be replaced with proper implementation read from job configuration 
     */
    private static SearchRequest buildFeatureQuery(Map<String, Object> after) {
        QueryBuilder queryBuilder = new MatchAllQueryBuilder();
        SearchRequest searchRequest = new SearchRequest(SOURCE_INDEX);

        List<CompositeValuesSourceBuilder<?>> sources = new ArrayList<>();
        sources.add(new TermsValuesSourceBuilder("reviewerId").field("reviewerId"));

        CompositeAggregationBuilder compositeAggregation = new CompositeAggregationBuilder("feature", sources);
        compositeAggregation.size(1000);

        if (after != null) {
            compositeAggregation.aggregateAfter(after);
        }

        compositeAggregation.subAggregation(AggregationBuilders.avg("avg_rating").field("rating"));
        compositeAggregation.subAggregation(AggregationBuilders.cardinality("dc_vendors").field("vendorId"));
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.aggregation(compositeAggregation);
        sourceBuilder.size(0);
        sourceBuilder.query(queryBuilder);
        searchRequest.source(sourceBuilder);

        return searchRequest;
    }
}
