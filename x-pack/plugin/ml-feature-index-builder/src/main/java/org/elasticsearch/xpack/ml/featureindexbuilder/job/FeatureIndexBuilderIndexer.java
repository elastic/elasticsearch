/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.featureindexbuilder.job;

import org.apache.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.indexing.AsyncTwoPhaseIndexer;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.indexing.IterationResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings.DOC_TYPE;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class FeatureIndexBuilderIndexer extends AsyncTwoPhaseIndexer<Map<String, Object>, FeatureIndexBuilderJobStats> {
    private static final String PIVOT_INDEX = "pivot-reviews";
    private static final String SOURCE_INDEX = "anonreviews";

    private static final Logger logger = Logger.getLogger(FeatureIndexBuilderIndexer.class.getName());
    private FeatureIndexBuilderJob job;
    private Client client;

    public FeatureIndexBuilderIndexer(Executor executor, FeatureIndexBuilderJob job, AtomicReference<IndexerState> initialState,
            Map<String, Object> initialPosition, Client client) {
        super(executor, initialState, initialPosition, new FeatureIndexBuilderJobStats());

        this.job = job;
        this.client = client;
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
                throw new RuntimeException(e);
            }
            IndexRequest request = new IndexRequest(PIVOT_INDEX, DOC_TYPE).source(builder);
            return request;
        }).collect(Collectors.toList());
    }

    @Override
    protected SearchRequest buildSearchRequest() {

        final Map<String, Object> position = getPosition();

        if (position == null) {
            deleteIndex(client);
            createIndex(client);
        }

        SearchRequest request = buildFeatureQuery(position);

        return request;
    }

    @Override
    protected void doNextSearch(SearchRequest request, ActionListener<SearchResponse> nextPhase) {
        ClientHelper.executeWithHeadersAsync(job.getHeaders(), ClientHelper.ML_ORIGIN, client, SearchAction.INSTANCE, request, nextPhase);
    }

    @Override
    protected void doNextBulk(BulkRequest request, ActionListener<BulkResponse> nextPhase) {
        ClientHelper.executeWithHeadersAsync(job.getHeaders(), ClientHelper.ML_ORIGIN, client, BulkAction.INSTANCE, request, nextPhase);
    }

    @Override
    protected void doSaveState(IndexerState indexerState, Map<String, Object> position, Runnable next) {
        if (indexerState.equals(IndexerState.ABORTING)) {
            // If we're aborting, just invoke `next` (which is likely an onFailure handler)
            next.run();
        } else {
            // to be implemented

            final FeatureIndexBuilderJobState state = new FeatureIndexBuilderJobState(indexerState);
            logger.info("Updating persistent state of job [" + job.getConfig().getId() + "] to [" + state.toString() + "]");

            // TODO: we can not persist the state right now, need to be called from the task
            next.run();
            // updatePersistentTaskState(state, ActionListener.wrap(task -> next.run(), exc
            // -> {
            // We failed to update the persistent task for some reason,
            // set our flag back to what it was before
            // next.run();
            // }));

        }
    }

    @Override
    protected void onFailure(Exception exc) {
        logger.warn("FeatureIndexBuilder job [" + job.getConfig().getId() + "] failed with an exception: ", exc);
    }

    @Override
    protected void onFinish() {
        logger.info("Finished indexing for job [" + job.getConfig().getId() + "]");
    }

    @Override
    protected void onAbort() {
        logger.info("FeatureIndexBuilder job [" + job.getConfig().getId() + "] received abort request, stopping indexer");
    }

    /*
     * Hardcoded demo case for pivoting
     */

    private static void deleteIndex(Client client) {
        DeleteIndexRequest deleteIndex = new DeleteIndexRequest(PIVOT_INDEX);

        IndicesAdminClient adminClient = client.admin().indices();
        try {
            adminClient.delete(deleteIndex).actionGet();
        } catch (IndexNotFoundException e) {
        }
    }

    private static void createIndex(Client client) {

        CreateIndexRequest request = new CreateIndexRequest(PIVOT_INDEX);
        request.settings(Settings.builder() // <1>
                .put("index.number_of_shards", 1).put("index.number_of_replicas", 0));
        request.mapping(DOC_TYPE, // <1>
                "{\n" + "  \"" + DOC_TYPE + "\": {\n" + "    \"properties\": {\n" + "      \"reviewerId\": {\n"
                        + "        \"type\": \"keyword\"\n" + "      },\n" + "      \"avg_rating\": {\n" + "        \"type\": \"integer\"\n"
                        + "      }\n" + "    }\n" + "  }\n" + "}", // <2>
                XContentType.JSON);
        IndicesAdminClient adminClient = client.admin().indices();
        adminClient.create(request).actionGet();
    }

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
