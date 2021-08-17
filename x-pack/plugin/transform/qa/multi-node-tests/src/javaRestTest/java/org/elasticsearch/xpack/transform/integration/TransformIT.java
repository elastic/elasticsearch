/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.integration;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.transform.transforms.DestConfig;
import org.elasticsearch.client.transform.transforms.SettingsConfig;
import org.elasticsearch.client.transform.transforms.TimeSyncConfig;
import org.elasticsearch.client.transform.transforms.TransformConfig;
import org.elasticsearch.client.transform.transforms.TransformConfigUpdate;
import org.elasticsearch.client.transform.transforms.TransformStats;
import org.elasticsearch.client.transform.transforms.pivot.SingleGroupSource;
import org.elasticsearch.client.transform.transforms.pivot.TermsGroupSource;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.oneOf;

public class TransformIT extends TransformIntegTestCase {

    private static final int NUM_USERS = 28;

    static Integer getUserIdForRow(int row) {
        return row % NUM_USERS;
    }

    static String getDateStringForRow(int row) {
        int day = (11 + (row / 100)) % 28;
        int hour = 10 + (row % 13);
        int min = 10 + (row % 49);
        int sec = 10 + (row % 49);
        return "2017-01-" + (day < 10 ? "0" + day : day) + "T" + hour + ":" + min + ":" + sec + "Z";
    }

    @Before
    public void setClusterSettings() throws IOException {
        Request settingsRequest = new Request("PUT", "/_cluster/settings");
        settingsRequest.setJsonEntity(
            "{\"transient\": {"
                + "\"logger.org.elasticsearch.xpack.core.indexing.AsyncTwoPhaseIndexer\": \"debug\","
                + "\"logger.org.elasticsearch.xpack.transform\": \"debug\"}}"
        );
        client().performRequest(settingsRequest);
    }

    @After
    public void cleanTransforms() throws IOException {
        cleanUp();
    }

    public void testTransformCrud() throws Exception {
        String indexName = "basic-crud-reviews";
        String transformId = "transform-crud";
        createReviewsIndex(indexName, 100, NUM_USERS, TransformIT::getUserIdForRow, TransformIT::getDateStringForRow);

        Map<String, SingleGroupSource> groups = new HashMap<>();
        groups.put("by-day", createDateHistogramGroupSourceWithCalendarInterval("timestamp", DateHistogramInterval.DAY, null));
        groups.put("by-user", TermsGroupSource.builder().setField("user_id").build());
        groups.put("by-business", TermsGroupSource.builder().setField("business_id").build());

        AggregatorFactories.Builder aggs = AggregatorFactories.builder()
            .addAggregator(AggregationBuilders.avg("review_score").field("stars"))
            .addAggregator(AggregationBuilders.max("timestamp").field("timestamp"));

        TransformConfig config =
            createTransformConfigBuilder(transformId, "reviews-by-user-business-day", QueryBuilders.matchAllQuery(), indexName)
                .setPivotConfig(createPivotConfig(groups, aggs))
                .build();

        assertTrue(putTransform(config, RequestOptions.DEFAULT).isAcknowledged());
        assertTrue(startTransform(config.getId(), RequestOptions.DEFAULT).isAcknowledged());

        waitUntilCheckpoint(config.getId(), 1L);

        stopTransform(config.getId());
        assertBusy(() -> {
            assertEquals(TransformStats.State.STOPPED, getTransformStats(config.getId()).getTransformsStats().get(0).getState());
        });

        TransformConfig storedConfig = getTransform(config.getId()).getTransformConfigurations().get(0);
        assertThat(storedConfig.getVersion(), equalTo(Version.CURRENT));
        Instant now = Instant.now();
        assertTrue("[create_time] is not before current time", storedConfig.getCreateTime().isBefore(now));
        deleteTransform(config.getId());
    }

    public void testContinuousTransformCrud() throws Exception {
        String indexName = "continuous-crud-reviews";
        String transformId = "transform-continuous-crud";
        createReviewsIndex(indexName, 100, NUM_USERS, TransformIT::getUserIdForRow, TransformIT::getDateStringForRow);

        Map<String, SingleGroupSource> groups = new HashMap<>();
        groups.put("by-day", createDateHistogramGroupSourceWithCalendarInterval("timestamp", DateHistogramInterval.DAY, null));
        groups.put("by-user", TermsGroupSource.builder().setField("user_id").build());
        groups.put("by-business", TermsGroupSource.builder().setField("business_id").build());

        AggregatorFactories.Builder aggs = AggregatorFactories.builder()
            .addAggregator(AggregationBuilders.avg("review_score").field("stars"))
            .addAggregator(AggregationBuilders.max("timestamp").field("timestamp"));

        TransformConfig config =
            createTransformConfigBuilder(transformId, "reviews-by-user-business-day", QueryBuilders.matchAllQuery(), indexName)
                .setPivotConfig(createPivotConfig(groups, aggs))
                .setSyncConfig(TimeSyncConfig.builder().setField("timestamp").setDelay(TimeValue.timeValueSeconds(1)).build())
                .setSettings(SettingsConfig.builder().setAlignCheckpoints(false).build())
                .build();

        assertTrue(putTransform(config, RequestOptions.DEFAULT).isAcknowledged());
        assertTrue(startTransform(config.getId(), RequestOptions.DEFAULT).isAcknowledged());

        waitUntilCheckpoint(config.getId(), 1L);
        assertThat(getTransformStats(config.getId()).getTransformsStats().get(0).getState(), equalTo(TransformStats.State.STARTED));

        long docsIndexed = getTransformStats(config.getId()).getTransformsStats().get(0).getIndexerStats().getDocumentsIndexed();

        TransformConfig storedConfig = getTransform(config.getId()).getTransformConfigurations().get(0);
        assertThat(storedConfig.getVersion(), equalTo(Version.CURRENT));
        Instant now = Instant.now();
        assertTrue("[create_time] is not before current time", storedConfig.getCreateTime().isBefore(now));

        // index some more docs
        long timeStamp = Instant.now().toEpochMilli() - 1_000;
        long user = 42;
        indexMoreDocs(timeStamp, user, indexName);
        waitUntilCheckpoint(config.getId(), 2L);

        // Assert that we wrote the new docs
        assertThat(
            getTransformStats(config.getId()).getTransformsStats().get(0).getIndexerStats().getDocumentsIndexed(),
            greaterThan(docsIndexed)
        );

        stopTransform(config.getId());
        deleteTransform(config.getId());
    }

    public void testContinuousTransformUpdate() throws Exception {
        String indexName = "continuous-reviews-update";
        createReviewsIndex(indexName, 10, NUM_USERS, TransformIT::getUserIdForRow, TransformIT::getDateStringForRow);

        Map<String, SingleGroupSource> groups = new HashMap<>();
        groups.put("by-user", TermsGroupSource.builder().setField("user_id").build());

        AggregatorFactories.Builder aggs = AggregatorFactories.builder()
            .addAggregator(AggregationBuilders.avg("review_score").field("stars"))
            .addAggregator(AggregationBuilders.max("timestamp").field("timestamp"));

        String id = "transform-to-update";
        String dest = "reviews-by-user-business-day-to-update";
        TransformConfig config =
            createTransformConfigBuilder(id, dest, QueryBuilders.matchAllQuery(), indexName)
                .setPivotConfig(createPivotConfig(groups, aggs))
                .setSyncConfig(TimeSyncConfig.builder().setField("timestamp").setDelay(TimeValue.timeValueSeconds(1)).build())
                .build();

        assertTrue(putTransform(config, RequestOptions.DEFAULT).isAcknowledged());
        assertTrue(startTransform(config.getId(), RequestOptions.DEFAULT).isAcknowledged());

        waitUntilCheckpoint(config.getId(), 1L);
        assertThat(
            getTransformStats(config.getId()).getTransformsStats().get(0).getState(),
            oneOf(TransformStats.State.STARTED, TransformStats.State.INDEXING)
        );

        long docsIndexed = getTransformStats(config.getId()).getTransformsStats().get(0).getIndexerStats().getDocumentsIndexed();

        TransformConfig storedConfig = getTransform(config.getId()).getTransformConfigurations().get(0);
        assertThat(storedConfig.getVersion(), equalTo(Version.CURRENT));
        Instant now = Instant.now();
        assertTrue("[create_time] is not before current time", storedConfig.getCreateTime().isBefore(now));

        String pipelineId = "add_forty_two";
        TransformConfigUpdate update = TransformConfigUpdate.builder()
            .setDescription("updated config")
            .setDest(DestConfig.builder().setIndex(dest).setPipeline(pipelineId).build())
            .build();

        try (RestHighLevelClient hlrc = new TestRestHighLevelClient()) {
            final XContentBuilder pipelineBuilder = jsonBuilder().startObject()
                .startArray("processors")
                .startObject()
                .startObject("set")
                .field("field", "static_forty_two")
                .field("value", 42)
                .endObject()
                .endObject()
                .endArray()
                .endObject();
            hlrc.ingest()
                .putPipeline(
                    new PutPipelineRequest(pipelineId, BytesReference.bytes(pipelineBuilder), XContentType.JSON),
                    RequestOptions.DEFAULT
                );

            updateConfig(id, update);

            // index some more docs
            long timeStamp = Instant.now().toEpochMilli() - 1_000;
            long user = 42;
            indexMoreDocs(timeStamp, user, indexName);

            // Since updates are loaded on checkpoint start, we should see the updated config on this next run
            waitUntilCheckpoint(config.getId(), 2L);
            long numDocsAfterCp2 = getTransformStats(config.getId()).getTransformsStats().get(0).getIndexerStats().getDocumentsIndexed();
            assertThat(numDocsAfterCp2, greaterThan(docsIndexed));

            final SearchRequest searchRequest = new SearchRequest(dest).source(
                new SearchSourceBuilder().trackTotalHits(true)
                    .query(QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("static_forty_two", 42)))
            );
            // assert that we have the new field and its value is 42 in at least some docs
            assertBusy(() -> {
                final SearchResponse searchResponse = hlrc.search(searchRequest, RequestOptions.DEFAULT);
                assertThat(searchResponse.getHits().getTotalHits().value, greaterThan(0L));
                hlrc.indices().refresh(new RefreshRequest(dest), RequestOptions.DEFAULT);
            }, 30, TimeUnit.SECONDS);
        }
        stopTransform(config.getId());
        deleteTransform(config.getId());
    }

    public void testStopWaitForCheckpoint() throws Exception {
        String indexName = "wait-for-checkpoint-reviews";
        String transformId = "transform-wait-for-checkpoint";
        createReviewsIndex(indexName, 1000, NUM_USERS, TransformIT::getUserIdForRow, TransformIT::getDateStringForRow);

        Map<String, SingleGroupSource> groups = new HashMap<>();
        groups.put("by-day", createDateHistogramGroupSourceWithCalendarInterval("timestamp", DateHistogramInterval.DAY, null));
        groups.put("by-user", TermsGroupSource.builder().setField("user_id").build());
        groups.put("by-business", TermsGroupSource.builder().setField("business_id").build());

        AggregatorFactories.Builder aggs = AggregatorFactories.builder()
            .addAggregator(AggregationBuilders.avg("review_score").field("stars"))
            .addAggregator(AggregationBuilders.max("timestamp").field("timestamp"));

        TransformConfig config =
            createTransformConfigBuilder(transformId, "reviews-by-user-business-day", QueryBuilders.matchAllQuery(), indexName)
                .setPivotConfig(createPivotConfig(groups, aggs))
                .setSyncConfig(TimeSyncConfig.builder().setField("timestamp").setDelay(TimeValue.timeValueSeconds(1)).build())
                .build();

        assertTrue(putTransform(config, RequestOptions.DEFAULT).isAcknowledged());

        assertTrue(startTransform(config.getId(), RequestOptions.DEFAULT).isAcknowledged());

        // waitForCheckpoint: true should make the transform continue until we hit the first checkpoint, then it will stop
        assertTrue(stopTransform(transformId, false, null, true).isAcknowledged());

        // Wait until the first checkpoint
        waitUntilCheckpoint(config.getId(), 1L);

        // Even though we are continuous, we should be stopped now as we needed to stop at the first checkpoint
        assertBusy(() -> {
            TransformStats stateAndStats = getTransformStats(config.getId()).getTransformsStats().get(0);
            assertThat(stateAndStats.getState(), equalTo(TransformStats.State.STOPPED));
            assertThat(stateAndStats.getIndexerStats().getDocumentsIndexed(), equalTo(1000L));
        });

        int additionalRuns = randomIntBetween(1, 10);

        for (int i = 0; i < additionalRuns; ++i) {
            // index some more docs using a new user
            long timeStamp = Instant.now().toEpochMilli() - 1_000;
            long user = 42 + i;
            indexMoreDocs(timeStamp, user, indexName);
            assertTrue(startTransformWithRetryOnConflict(config.getId(), RequestOptions.DEFAULT).isAcknowledged());

            boolean waitForCompletion = randomBoolean();
            assertTrue(stopTransform(transformId, waitForCompletion, null, true).isAcknowledged());

            assertBusy(() -> {
                TransformStats stateAndStats = getTransformStats(config.getId()).getTransformsStats().get(0);
                assertThat(stateAndStats.getState(), equalTo(TransformStats.State.STOPPED));
            });
            TransformStats stateAndStats = getTransformStats(config.getId()).getTransformsStats().get(0);
            assertThat(stateAndStats.getState(), equalTo(TransformStats.State.STOPPED));
        }

        TransformStats stateAndStats = getTransformStats(config.getId()).getTransformsStats().get(0);
        assertThat(stateAndStats.getState(), equalTo(TransformStats.State.STOPPED));
        // Despite indexing new documents into the source index, the number of documents in the destination index stays the same.
        assertThat(stateAndStats.getIndexerStats().getDocumentsIndexed(), equalTo(1000L));

        assertTrue(stopTransform(transformId).isAcknowledged());
        deleteTransform(config.getId());
    }

    public void testContinuousTransformRethrottle() throws Exception {
        String indexName = "continuous-crud-reviews-throttled";
        String transformId = "transform-continuous-crud-throttled";

        createReviewsIndex(indexName, 1000, NUM_USERS, TransformIT::getUserIdForRow, TransformIT::getDateStringForRow);

        Map<String, SingleGroupSource> groups = new HashMap<>();
        groups.put("by-day", createDateHistogramGroupSourceWithCalendarInterval("timestamp", DateHistogramInterval.DAY, null));
        groups.put("by-user", TermsGroupSource.builder().setField("user_id").build());
        groups.put("by-business", TermsGroupSource.builder().setField("business_id").build());

        AggregatorFactories.Builder aggs = AggregatorFactories.builder()
            .addAggregator(AggregationBuilders.avg("review_score").field("stars"))
            .addAggregator(AggregationBuilders.max("timestamp").field("timestamp"));

        TransformConfig config =
            createTransformConfigBuilder(transformId, "reviews-by-user-business-day", QueryBuilders.matchAllQuery(), indexName)
                .setPivotConfig(createPivotConfig(groups, aggs))
                .setSyncConfig(TimeSyncConfig.builder().setField("timestamp").setDelay(TimeValue.timeValueSeconds(1)).build())
                // set requests per second and page size low enough to fail the test if update does not succeed,
                .setSettings(SettingsConfig.builder().setRequestsPerSecond(1F).setMaxPageSearchSize(10).build())
                .build();

        assertTrue(putTransform(config, RequestOptions.DEFAULT).isAcknowledged());
        assertTrue(startTransform(config.getId(), RequestOptions.DEFAULT).isAcknowledged());

        assertBusy(() -> {
            TransformStats stateAndStats = getTransformStats(config.getId()).getTransformsStats().get(0);
            assertThat(stateAndStats.getState(), equalTo(TransformStats.State.INDEXING));
        });

        TransformConfigUpdate update = TransformConfigUpdate.builder()
            // test randomly: with explicit settings and reset to default
            .setSettings(
                SettingsConfig.builder()
                    .setRequestsPerSecond(randomBoolean() ? 1000F : null)
                    .setMaxPageSearchSize(randomBoolean() ? 1000 : null)
                    .build()
            )
            .build();

        updateConfig(config.getId(), update);

        waitUntilCheckpoint(config.getId(), 1L);
        assertThat(getTransformStats(config.getId()).getTransformsStats().get(0).getState(), equalTo(TransformStats.State.STARTED));

        long docsIndexed = getTransformStats(config.getId()).getTransformsStats().get(0).getIndexerStats().getDocumentsIndexed();
        long pagesProcessed = getTransformStats(config.getId()).getTransformsStats().get(0).getIndexerStats().getPagesProcessed();

        TransformConfig storedConfig = getTransform(config.getId()).getTransformConfigurations().get(0);
        assertThat(storedConfig.getVersion(), equalTo(Version.CURRENT));
        Instant now = Instant.now();
        assertTrue("[create_time] is not before current time", storedConfig.getCreateTime().isBefore(now));

        // index some more docs
        long timeStamp = Instant.now().toEpochMilli() - 1_000;
        long user = 42;
        indexMoreDocs(timeStamp, user, indexName);
        waitUntilCheckpoint(config.getId(), 2L);

        // Assert that we wrote the new docs
        assertThat(
            getTransformStats(config.getId()).getTransformsStats().get(0).getIndexerStats().getDocumentsIndexed(),
            greaterThan(docsIndexed)
        );

        // Assert less than 500 pages processed, so update worked
        assertThat(pagesProcessed, lessThan(1000L));

        stopTransform(config.getId());
        deleteTransform(config.getId());
    }

    private void indexMoreDocs(long timestamp, long userId, String index) throws Exception {
        BulkRequest bulk = new BulkRequest(index);
        for (int i = 0; i < 25; i++) {
            int stars = (i + 20) % 5;
            long business = (i + 100) % 50;

            StringBuilder sourceBuilder = new StringBuilder();
            sourceBuilder.append("{\"user_id\":\"")
                .append("user_")
                .append(userId)
                .append("\",\"count\":")
                .append(i)
                .append(",\"business_id\":\"")
                .append("business_")
                .append(business)
                .append("\",\"stars\":")
                .append(stars)
                .append(",\"timestamp\":")
                .append(timestamp)
                .append("}");
            bulk.add(new IndexRequest().source(sourceBuilder.toString(), XContentType.JSON));
        }
        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        bulkIndexDocs(bulk);
    }
}
