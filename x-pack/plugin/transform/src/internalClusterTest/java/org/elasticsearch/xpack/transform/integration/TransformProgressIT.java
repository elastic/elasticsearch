/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.integration;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.transform.transforms.DestConfig;
import org.elasticsearch.xpack.core.transform.transforms.SettingsConfig;
import org.elasticsearch.xpack.core.transform.transforms.SourceConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformProgress;
import org.elasticsearch.xpack.core.transform.transforms.pivot.AggregationConfig;
import org.elasticsearch.xpack.core.transform.transforms.pivot.GroupConfig;
import org.elasticsearch.xpack.core.transform.transforms.pivot.HistogramGroupSource;
import org.elasticsearch.xpack.core.transform.transforms.pivot.PivotConfig;
import org.elasticsearch.xpack.transform.TransformSingleNodeTestCase;
import org.elasticsearch.xpack.transform.transforms.Function;
import org.elasticsearch.xpack.transform.transforms.pivot.Pivot;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@SuppressWarnings("removal")
public class TransformProgressIT extends TransformSingleNodeTestCase {
    private static final String REVIEWS_INDEX_NAME = "reviews";

    protected void createReviewsIndex(int userWithMissingBuckets) throws Exception {
        final int numDocs = 1000;

        // create mapping
        try (XContentBuilder builder = jsonBuilder()) {
            builder.startObject();
            {
                builder.startObject("properties")
                    .startObject("timestamp")
                    .field("type", "date")
                    .endObject()
                    .startObject("user_id")
                    .field("type", "keyword")
                    .endObject()
                    .startObject("count")
                    .field("type", "integer")
                    .endObject()
                    .startObject("business_id")
                    .field("type", "keyword")
                    .endObject()
                    .startObject("stars")
                    .field("type", "integer")
                    .endObject()
                    .endObject();
            }
            builder.endObject();

            var response = client().admin().indices().prepareCreate(REVIEWS_INDEX_NAME).setMapping(builder).get();
            assertThat(response.isAcknowledged(), is(true));
        }

        // create index
        BulkRequest bulk = new BulkRequest(REVIEWS_INDEX_NAME);
        int day = 10;
        for (int i = 0; i < numDocs; i++) {
            long user = i % 28;
            int stars = (i + 20) % 5;
            long business = (i + 100) % 50;
            int hour = 10 + (i % 13);
            int min = 10 + (i % 49);
            int sec = 10 + (i % 49);

            String date_string = "2017-01-" + day + "T" + hour + ":" + min + ":" + sec + "Z";

            StringBuilder sourceBuilder = new StringBuilder();
            sourceBuilder.append("{");
            sourceBuilder.append("\"user_id\":\"").append("user_").append(user).append("\",");

            if (user != userWithMissingBuckets) {
                sourceBuilder.append("\"count\":").append(i).append(",");
            }

            sourceBuilder.append("\"business_id\":\"")
                .append("business_")
                .append(business)
                .append("\",\"stars\":")
                .append(stars)
                .append(",\"timestamp\":\"")
                .append(date_string)
                .append("\"}");
            bulk.add(new IndexRequest().source(sourceBuilder.toString(), XContentType.JSON));

            if (i % 50 == 0) {
                BulkResponse response = client().bulk(bulk).actionGet();
                assertThat(response.buildFailureMessage(), response.hasFailures(), is(false));
                bulk = new BulkRequest(REVIEWS_INDEX_NAME);
                day += 1;
            }
        }
        BulkResponse bulkResponse = client().bulk(bulk).actionGet();
        assertFalse(bulkResponse.hasFailures());
        client().admin().indices().prepareRefresh(REVIEWS_INDEX_NAME).get();
    }

    public void testGetProgress() throws Exception {
        assertGetProgress(-1);
    }

    public void testGetProgressMissingBucket() throws Exception {
        assertGetProgress(randomIntBetween(1, 25));
    }

    public void assertGetProgress(int userWithMissingBuckets) throws Exception {
        String transformId = "get_progress_transform";
        boolean missingBucket = userWithMissingBuckets > 0;
        createReviewsIndex(userWithMissingBuckets);
        SourceConfig sourceConfig = new SourceConfig(REVIEWS_INDEX_NAME);
        DestConfig destConfig = new DestConfig("unnecessary", null, null);
        GroupConfig histgramGroupConfig = new GroupConfig(
            Collections.emptyMap(),
            Collections.singletonMap("every_50", new HistogramGroupSource("count", null, missingBucket, 50.0))
        );
        AggregatorFactories.Builder aggs = new AggregatorFactories.Builder();
        aggs.addAggregator(AggregationBuilders.avg("avg_rating").field("stars"));
        AggregationConfig aggregationConfig = new AggregationConfig(Collections.emptyMap(), aggs);
        PivotConfig pivotConfig = new PivotConfig(histgramGroupConfig, aggregationConfig, null);
        TransformConfig config = new TransformConfig(
            transformId,
            sourceConfig,
            destConfig,
            null,
            null,
            null,
            pivotConfig,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );

        Pivot pivot = new Pivot(pivotConfig, new SettingsConfig(), Version.CURRENT, Collections.emptySet());

        TransformProgress progress = getProgress(pivot, getProgressQuery(pivot, config.getSource().getIndex(), null));

        assertThat(progress.getTotalDocs(), equalTo(1000L));
        assertThat(progress.getDocumentsProcessed(), equalTo(0L));
        assertThat(progress.getPercentComplete(), equalTo(0.0));

        progress = getProgress(pivot, getProgressQuery(pivot, config.getSource().getIndex(), QueryBuilders.rangeQuery("stars").gte(2)));

        assertThat(progress.getTotalDocs(), equalTo(600L));
        assertThat(progress.getDocumentsProcessed(), equalTo(0L));
        assertThat(progress.getPercentComplete(), equalTo(0.0));

        progress = getProgress(
            pivot,
            getProgressQuery(pivot, config.getSource().getIndex(), QueryBuilders.termQuery("user_id", "user_26"))
        );

        assertThat(progress.getTotalDocs(), equalTo(35L));
        assertThat(progress.getDocumentsProcessed(), equalTo(0L));
        assertThat(progress.getPercentComplete(), equalTo(0.0));

        histgramGroupConfig = new GroupConfig(
            Collections.emptyMap(),
            Collections.singletonMap("every_50", new HistogramGroupSource("missing_field", null, missingBucket, 50.0))
        );
        pivotConfig = new PivotConfig(histgramGroupConfig, aggregationConfig, null);
        pivot = new Pivot(pivotConfig, new SettingsConfig(), Version.CURRENT, Collections.emptySet());

        progress = getProgress(
            pivot,
            getProgressQuery(pivot, config.getSource().getIndex(), QueryBuilders.termQuery("user_id", "user_26"))
        );

        assertThat(progress.getDocumentsProcessed(), equalTo(0L));
        if (missingBucket) {
            assertThat(progress.getTotalDocs(), equalTo(35L));
            assertThat(progress.getPercentComplete(), equalTo(0.0));
        } else {
            assertThat(progress.getTotalDocs(), equalTo(0L));
            assertThat(progress.getPercentComplete(), equalTo(100.0));
        }

        var ackResponse = client().admin().indices().prepareDelete(REVIEWS_INDEX_NAME).get();
        assertTrue(ackResponse.isAcknowledged());
    }

    private TransformProgress getProgress(Function function, SearchRequest searchRequest) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<TransformProgress> progressHolder = new AtomicReference<>();
        final AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        SearchResponse response = client().search(searchRequest).actionGet();
        function.getInitialProgressFromResponse(
            response,
            new LatchedActionListener<>(ActionListener.wrap(progressHolder::set, exceptionHolder::set), latch)
        );

        assertTrue("timed out after 20s", latch.await(20, TimeUnit.SECONDS));
        if (exceptionHolder.get() != null) {
            throw exceptionHolder.get();
        }

        return progressHolder.get();
    }

    private static SearchRequest getProgressQuery(Function function, String[] source, QueryBuilder query) {
        SearchRequest searchRequest = new SearchRequest(source);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        function.buildSearchQueryForInitialProgress(searchSourceBuilder);

        if (query != null) {
            searchSourceBuilder.query(QueryBuilders.boolQuery().filter(query).filter(searchSourceBuilder.query()));
        }
        searchRequest.allowPartialSearchResults(false).source(searchSourceBuilder);
        return searchRequest;
    }
}
