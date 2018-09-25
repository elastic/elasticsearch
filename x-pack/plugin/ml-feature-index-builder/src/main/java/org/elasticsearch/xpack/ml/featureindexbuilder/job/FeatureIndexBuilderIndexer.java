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
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.indexing.AsyncTwoPhaseIndexer;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.indexing.IterationResult;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings.DOC_TYPE;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public abstract class FeatureIndexBuilderIndexer extends AsyncTwoPhaseIndexer<Map<String, Object>, FeatureIndexBuilderJobStats> {

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
        // for now only 1 source supported
        String destinationFieldName = job.getConfig().getSourceConfig().getSources().get(0).name();
        String aggName = job.getConfig().getAggregationConfig().getAggregatorFactories().get(0).getName();

        return agg.getBuckets().stream().map(b -> {
            NumericMetricsAggregation.SingleValue aggResult = b.getAggregations().get(aggName);
            XContentBuilder builder;
            try {
                builder = jsonBuilder();
                builder.startObject();
                builder.field(destinationFieldName, b.getKey().get(destinationFieldName));
                builder.field(aggName, aggResult.value());
                builder.endObject();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            String indexName = job.getConfig().getDestinationIndex();
            IndexRequest request = new IndexRequest(indexName, DOC_TYPE).source(builder);
            return request;
        }).collect(Collectors.toList());
    }

    @Override
    protected SearchRequest buildSearchRequest() {
        final Map<String, Object> position = getPosition();

        QueryBuilder queryBuilder = new MatchAllQueryBuilder();
        SearchRequest searchRequest = new SearchRequest(job.getConfig().getIndexPattern());

        List<CompositeValuesSourceBuilder<?>> sources = job.getConfig().getSourceConfig().getSources();

        CompositeAggregationBuilder compositeAggregation = new CompositeAggregationBuilder("feature", sources);
        compositeAggregation.size(1000);

        if (position != null) {
            compositeAggregation.aggregateAfter(position);
        }

        for (AggregationBuilder agg : job.getConfig().getAggregationConfig().getAggregatorFactories()) {
            compositeAggregation.subAggregation(agg);
        }

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.aggregation(compositeAggregation);
        sourceBuilder.size(0);
        sourceBuilder.query(queryBuilder);
        searchRequest.source(sourceBuilder);

        return searchRequest;
    }
}
