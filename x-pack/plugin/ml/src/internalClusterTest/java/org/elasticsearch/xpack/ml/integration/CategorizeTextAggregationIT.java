/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.Min;
import org.elasticsearch.xpack.ml.aggs.categorization.CategorizeTextAggregationBuilder;
import org.elasticsearch.xpack.ml.aggs.categorization.InternalCategorizationAggregation;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;
import org.junit.Before;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notANumber;

public class CategorizeTextAggregationIT extends BaseMlIntegTestCase {

    private static final String DATA_INDEX = "categorization-agg-data";

    @Before
    public void setupCluster() {
        internalCluster().ensureAtLeastNumDataNodes(3);
        ensureStableCluster();
        createSourceData();
    }

    public void testAggregation() {
        SearchResponse response = client().prepareSearch(DATA_INDEX)
            .setSize(0)
            .setTrackTotalHits(false)
            .addAggregation(
                new CategorizeTextAggregationBuilder("categorize", "msg").subAggregation(AggregationBuilders.max("max").field("time"))
                    .subAggregation(AggregationBuilders.min("min").field("time"))
            )
            .get();

        InternalCategorizationAggregation agg = response.getAggregations().get("categorize");
        assertThat(agg.getBuckets(), hasSize(3));

        assertCategorizationBucket(agg.getBuckets().get(0), "Node started", 3);
        assertCategorizationBucket(agg.getBuckets().get(1), "Failed to shutdown error org.aaaa.bbbb.Cccc line caused by foo exception", 2);
        assertCategorizationBucket(agg.getBuckets().get(2), "Node stopped", 1);
    }

    public void testAggregationWithOnlyOneBucket() {
        SearchResponse response = client().prepareSearch(DATA_INDEX)
            .setSize(0)
            .setTrackTotalHits(false)
            .addAggregation(
                new CategorizeTextAggregationBuilder("categorize", "msg").size(1)
                    .subAggregation(AggregationBuilders.max("max").field("time"))
                    .subAggregation(AggregationBuilders.min("min").field("time"))
            )
            .get();
        InternalCategorizationAggregation agg = response.getAggregations().get("categorize");
        assertThat(agg.getBuckets(), hasSize(1));

        assertCategorizationBucket(agg.getBuckets().get(0), "Node started", 3);
    }

    public void testAggregationWithBroadCategories() {
        SearchResponse response = client().prepareSearch(DATA_INDEX)
            .setSize(0)
            .setTrackTotalHits(false)
            .addAggregation(
                // Overriding the similarity threshold to just 11% (default is 70%) results in the
                // "Node started" and "Node stopped" messages being grouped in the same category
                new CategorizeTextAggregationBuilder("categorize", "msg").setSimilarityThreshold(11)
                    .subAggregation(AggregationBuilders.max("max").field("time"))
                    .subAggregation(AggregationBuilders.min("min").field("time"))
            )
            .get();
        InternalCategorizationAggregation agg = response.getAggregations().get("categorize");
        assertThat(agg.getBuckets(), hasSize(2));

        assertCategorizationBucket(agg.getBuckets().get(0), "Node", 4);
        assertCategorizationBucket(agg.getBuckets().get(1), "Failed to shutdown error org.aaaa.bbbb.Cccc line caused by foo exception", 2);
    }

    private void assertCategorizationBucket(InternalCategorizationAggregation.Bucket bucket, String key, long docCount) {
        assertThat(bucket.getKeyAsString(), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(docCount));
        assertThat(((Max) bucket.getAggregations().get("max")).value(), not(notANumber()));
        assertThat(((Min) bucket.getAggregations().get("min")).value(), not(notANumber()));
    }

    private void createSourceData() {
        client().admin().indices().prepareCreate(DATA_INDEX).setMapping("time", "type=date,format=epoch_millis", "msg", "type=text").get();

        long nowMillis = System.currentTimeMillis();

        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        IndexRequest indexRequest = new IndexRequest(DATA_INDEX);
        indexRequest.source("time", nowMillis - TimeValue.timeValueHours(2).millis(), "msg", "Node 1 started", "part", "nodes");
        bulkRequestBuilder.add(indexRequest);
        indexRequest = new IndexRequest(DATA_INDEX);
        indexRequest.source(
            "time",
            nowMillis - TimeValue.timeValueHours(2).millis() + 1,
            "msg",
            "Failed to shutdown [error org.aaaa.bbbb.Cccc line 54 caused by foo exception]",
            "part",
            "shutdowns"
        );
        bulkRequestBuilder.add(indexRequest);
        indexRequest = new IndexRequest(DATA_INDEX);
        indexRequest.source(
            "time",
            nowMillis - TimeValue.timeValueHours(2).millis() + 1,
            "msg",
            "Failed to shutdown [error org.aaaa.bbbb.Cccc line 55 caused by foo exception]",
            "part",
            "shutdowns"
        );
        bulkRequestBuilder.add(indexRequest);
        indexRequest = new IndexRequest(DATA_INDEX);
        indexRequest.source("time", nowMillis - TimeValue.timeValueHours(1).millis(), "msg", "Node 2 started", "part", "nodes");
        bulkRequestBuilder.add(indexRequest);
        indexRequest = new IndexRequest(DATA_INDEX);
        indexRequest.source("time", nowMillis, "msg", "Node 3 started", "part", "nodes");
        bulkRequestBuilder.add(indexRequest);

        indexRequest = new IndexRequest(DATA_INDEX);
        indexRequest.source("time", nowMillis, "msg", "Node 3 stopped", "part", "nodes");
        bulkRequestBuilder.add(indexRequest);

        BulkResponse bulkResponse = bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        assertThat(bulkResponse.hasFailures(), is(false));
    }

}
