/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.featureindexbuilder.job;

import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
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
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation.Bucket;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings.DOC_TYPE;

public class FeatureIndexBuilderIndexer {
    private static final String PIVOT_INDEX = "pivot-reviews";
    private static final String SOURCE_INDEX = "anonreviews";

    private static final Logger logger = Logger.getLogger(FeatureIndexBuilderIndexer.class.getName());
    private FeatureIndexBuilderJob job;
    private Client client;

    public FeatureIndexBuilderIndexer(FeatureIndexBuilderJob job, Client client) {

        this.job = job;
        this.client = client;
        logger.info("delete pivot-reviews");
        
    }

    public synchronized void start() {
        deleteIndex(client);

        createIndex(client);
        
        int runs = 0;

        Map<String, Object> after = null;
        logger.info("start feature indexing");
        SearchResponse response;
        
        try {
            response = runQuery(client, after);

            CompositeAggregation compositeAggregation = response.getAggregations().get("feature");
            after = compositeAggregation.afterKey();

            while (after != null) {
                indexBuckets(compositeAggregation);

                ++runs;
                response = runQuery(client, after);

                compositeAggregation = response.getAggregations().get("feature");
                after = compositeAggregation.afterKey();
                
                //after = null;
            }
            
            indexBuckets(compositeAggregation);
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Failed to build feature index", e);
        }

        logger.info("Finished feature indexing");
    }

    private void indexBuckets(CompositeAggregation compositeAggregation) {
        BulkRequest bulkIndexRequest = new BulkRequest();
        try {
            for (Bucket b : compositeAggregation.getBuckets()) {

                InternalAvg avgAgg = b.getAggregations().get("avg_rating");

                XContentBuilder builder;
                builder = jsonBuilder();
                builder.startObject();
                builder.field("reviewerId", b.getKey().get("reviewerId"));
                builder.field("avg_rating", avgAgg.getValue());
                builder.endObject();
                bulkIndexRequest.add(new IndexRequest(PIVOT_INDEX, DOC_TYPE).source(builder));

            }
            client.bulk(bulkIndexRequest);
        } catch (IOException e) {
            logger.error("Failed to index", e);
        }
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
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
            );
        request.mapping(DOC_TYPE, // <1>
                "{\n" +
                "  \"" + DOC_TYPE + "\": {\n" +
                "    \"properties\": {\n" +
                "      \"reviewerId\": {\n" +
                "        \"type\": \"keyword\"\n" +
                "      },\n" +
                "      \"avg_rating\": {\n" +
                "        \"type\": \"integer\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}", // <2>
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
    
    private static SearchResponse runQuery(Client client, Map<String, Object> after) throws InterruptedException, ExecutionException {
        
        SearchRequest request = buildFeatureQuery(after);
        SearchResponse response  = client.search(request).get();
        
        return response;
    }
    
    private static void indexResult() {
        
        
        
    }
}
