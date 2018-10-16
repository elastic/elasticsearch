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
import java.util.Collection;
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
     * Parses the result and creates indexable documents
     */
    private List<IndexRequest> processBuckets(CompositeAggregation agg) {
        String indexName = job.getConfig().getDestinationIndex();
        List<CompositeValuesSourceBuilder<?>> sources = job.getConfig().getSourceConfig().getSources();
        Collection<AggregationBuilder> aggregationBuilders = job.getConfig().getAggregationConfig().getAggregatorFactories();

        return agg.getBuckets().stream().map(b -> {
            XContentBuilder builder;
            try {
                builder = jsonBuilder();
                builder.startObject();
                for (CompositeValuesSourceBuilder<?> source : sources) {
                    String destinationFieldName = source.name();
                    builder.field(destinationFieldName, b.getKey().get(destinationFieldName));
                }
                for (AggregationBuilder aggregationBuilder : aggregationBuilders) {
                    String aggName = aggregationBuilder.getName();

                    // TODO: support other aggregation types
                    NumericMetricsAggregation.SingleValue aggResult = b.getAggregations().get(aggName);

                    if (aggResult != null) {
                        builder.field(aggName, aggResult.value());
                    } else {
                        // should never be reached as we should prevent creating
                        // jobs with unsupported aggregations
                        logger.error("Unsupported aggregation type for job [" + getJobId() + "]. Ignoring aggregation.");
                    }
                }
                builder.endObject();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

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
