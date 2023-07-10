/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.integration;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.transform.transforms.QueryConfig;
import org.elasticsearch.xpack.core.transform.transforms.SettingsConfig;
import org.elasticsearch.xpack.core.transform.transforms.TimeRetentionPolicyConfig;
import org.elasticsearch.xpack.core.transform.transforms.TimeSyncConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.pivot.SingleGroupSource;
import org.elasticsearch.xpack.core.transform.transforms.pivot.TermsGroupSource;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.oneOf;

@SuppressWarnings("removal")
public class TransformIT extends TransformRestTestCase {

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
        settingsRequest.setJsonEntity("""
            {
              "persistent": {
                "logger.org.elasticsearch.xpack.core.indexing.AsyncTwoPhaseIndexer": "debug",
                "logger.org.elasticsearch.xpack.transform": "debug"
              }
            }""");
        client().performRequest(settingsRequest);
    }

    @After
    public void cleanTransforms() throws Exception {
        cleanUp();
    }

    public void testTransformCrud() throws Exception {
        String indexName = "basic-crud-reviews";
        String transformId = "transform-crud";
        createReviewsIndex(indexName, 100, NUM_USERS, TransformIT::getUserIdForRow, TransformIT::getDateStringForRow);

        Map<String, SingleGroupSource> groups = new HashMap<>();
        groups.put("by-day", createDateHistogramGroupSourceWithCalendarInterval("timestamp", DateHistogramInterval.DAY, null));
        groups.put("by-user", new TermsGroupSource("user_id", null, false));
        groups.put("by-business", new TermsGroupSource("business_id", null, false));

        AggregatorFactories.Builder aggs = AggregatorFactories.builder()
            .addAggregator(AggregationBuilders.avg("review_score").field("stars"))
            .addAggregator(AggregationBuilders.max("timestamp").field("timestamp"));

        TransformConfig config = createTransformConfigBuilder(
            transformId,
            "reviews-by-user-business-day",
            QueryConfig.matchAll(),
            indexName
        ).setPivotConfig(createPivotConfig(groups, aggs)).build();

        putTransform(transformId, Strings.toString(config), RequestOptions.DEFAULT);
        startTransform(config.getId(), RequestOptions.DEFAULT);

        waitUntilCheckpoint(config.getId(), 1L);

        stopTransform(config.getId());
        assertBusy(() -> { assertEquals("stopped", getTransformStats(config.getId()).get("state")); });

        var storedConfig = getTransform(config.getId());
        assertThat(storedConfig.get("version"), equalTo(Version.CURRENT.toString()));
        Instant now = Instant.now();
        long createTime = (long) storedConfig.get("create_time");
        assertTrue("[create_time] is not before current time", Instant.ofEpochMilli(createTime).isBefore(now));
        deleteTransform(config.getId());
    }

    public void testContinuousTransformCrud() throws Exception {
        String indexName = "continuous-crud-reviews";
        String transformId = "transform-continuous-crud";
        createReviewsIndex(indexName, 100, NUM_USERS, TransformIT::getUserIdForRow, TransformIT::getDateStringForRow);

        Map<String, SingleGroupSource> groups = new HashMap<>();
        groups.put("by-day", createDateHistogramGroupSourceWithCalendarInterval("timestamp", DateHistogramInterval.DAY, null));
        groups.put("by-user", new TermsGroupSource("user_id", null, false));
        groups.put("by-business", new TermsGroupSource("business_id", null, false));

        AggregatorFactories.Builder aggs = AggregatorFactories.builder()
            .addAggregator(AggregationBuilders.avg("review_score").field("stars"))
            .addAggregator(AggregationBuilders.max("timestamp").field("timestamp"));

        TransformConfig config = createTransformConfigBuilder(
            transformId,
            "reviews-by-user-business-day",
            QueryConfig.matchAll(),
            indexName
        ).setPivotConfig(createPivotConfig(groups, aggs))
            .setSyncConfig(new TimeSyncConfig("timestamp", TimeValue.timeValueSeconds(1)))
            .setSettings(new SettingsConfig.Builder().setAlignCheckpoints(false).build())
            .build();

        putTransform(transformId, Strings.toString(config), RequestOptions.DEFAULT);
        startTransform(config.getId(), RequestOptions.DEFAULT);

        waitUntilCheckpoint(config.getId(), 1L);
        var transformStats = getTransformStats(config.getId());
        assertThat(transformStats.get("state"), equalTo("started"));

        int docsIndexed = (Integer) XContentMapValues.extractValue("stats.documents_indexed", transformStats);

        var storedConfig = getTransform(config.getId());
        assertThat(storedConfig.get("version"), equalTo(Version.CURRENT.toString()));
        Instant now = Instant.now();
        long createTime = (long) storedConfig.get("create_time");
        assertTrue("[create_time] is not before current time", Instant.ofEpochMilli(createTime).isBefore(now));

        // index some more docs
        long timeStamp = Instant.now().toEpochMilli() - 1_000;
        long user = 42;
        indexMoreDocs(timeStamp, user, indexName);
        waitUntilCheckpoint(config.getId(), 2L);

        // Assert that we wrote the new docs

        assertThat(
            (Integer) XContentMapValues.extractValue("stats.documents_indexed", getTransformStats(config.getId())),
            greaterThan(docsIndexed)
        );

        stopTransform(config.getId());
        deleteTransform(config.getId());
    }

    public void testContinuousTransformUpdate() throws Exception {
        String indexName = "continuous-reviews-update";
        createReviewsIndex(indexName, 10, NUM_USERS, TransformIT::getUserIdForRow, TransformIT::getDateStringForRow);

        Map<String, SingleGroupSource> groups = new HashMap<>();
        groups.put("by-user", new TermsGroupSource("user_id", null, false));

        AggregatorFactories.Builder aggs = AggregatorFactories.builder()
            .addAggregator(AggregationBuilders.avg("review_score").field("stars"))
            .addAggregator(AggregationBuilders.max("timestamp").field("timestamp"));

        String id = "transform-to-update";
        String dest = "reviews-by-user-business-day-to-update";
        TransformConfig config = createTransformConfigBuilder(id, dest, QueryConfig.matchAll(), indexName).setPivotConfig(
            createPivotConfig(groups, aggs)
        ).setSyncConfig(new TimeSyncConfig("timestamp", TimeValue.timeValueSeconds(1))).build();

        putTransform(id, Strings.toString(config), RequestOptions.DEFAULT);
        startTransform(config.getId(), RequestOptions.DEFAULT);

        waitUntilCheckpoint(config.getId(), 1L);
        assertThat(getTransformState(config.getId()), oneOf("started", "indexing"));

        int docsIndexed = (Integer) XContentMapValues.extractValue("stats.documents_indexed", getTransformStats(config.getId()));

        var storedConfig = getTransform(config.getId());
        assertThat(storedConfig.get("version"), equalTo(Version.CURRENT.toString()));
        Instant now = Instant.now();
        long createTime = (long) storedConfig.get("create_time");
        assertTrue("[create_time] is not before current time", Instant.ofEpochMilli(createTime).isBefore(now));

        String pipelineId = "add_forty_two";
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
        Request putPipeline = new Request("PUT", "/_ingest/pipeline/" + pipelineId);
        putPipeline.setEntity(new StringEntity(Strings.toString(pipelineBuilder), ContentType.APPLICATION_JSON));
        assertOK(client().performRequest(putPipeline));

        String update = Strings.format("""
            {
                "description": "updated config",
                "dest": {
                   "index": "%s",
                   "pipeline": "%s"
                }
            }
            """, dest, pipelineId);
        updateConfig(id, update, RequestOptions.DEFAULT);

        // index some more docs
        long timeStamp = Instant.now().toEpochMilli() - 1_000;
        long user = 42;
        indexMoreDocs(timeStamp, user, indexName);

        // Since updates are loaded on checkpoint start, we should see the updated config on this next run
        waitUntilCheckpoint(config.getId(), 2L);
        int numDocsAfterCp2 = (Integer) XContentMapValues.extractValue("stats.documents_indexed", getTransformStats(config.getId()));
        assertThat(numDocsAfterCp2, greaterThan(docsIndexed));

        Request searchRequest = new Request("GET", dest + "/_search");
        searchRequest.addParameter("track_total_hits", "true");
        searchRequest.setJsonEntity("""
            {
                "query": {
                    "term": {
                        "static_forty_two": {
                            "value": 42
                        }
                    }
                }
            }
            """);

        // assert that we have the new field and its value is 42 in at least some docs
        assertBusy(() -> {
            final Response searchResponse = client().performRequest(searchRequest);
            assertOK(searchResponse);
            var responseMap = entityAsMap(searchResponse);
            assertThat((Integer) XContentMapValues.extractValue("hits.total.value", responseMap), greaterThan(0));
            refreshIndex(dest, RequestOptions.DEFAULT);
        }, 30, TimeUnit.SECONDS);

        stopTransform(config.getId());
        deleteTransform(config.getId());
    }

    public void testRetentionPolicyDelete() throws Exception {
        String indexName = "retention-index";
        String transformId = "transform-retention-update";
        String dest = "retention-policy-dest";
        createReviewsIndex(indexName, 10, NUM_USERS, TransformIT::getUserIdForRow, TransformIT::getDateStringForRow);

        Map<String, SingleGroupSource> groups = new HashMap<>();
        groups.put("by-user", new TermsGroupSource("user_id", null, false));

        AggregatorFactories.Builder aggs = AggregatorFactories.builder()
            .addAggregator(AggregationBuilders.max("timestamp").field("timestamp"));

        TransformConfig config = createTransformConfigBuilder(transformId, dest, QueryConfig.matchAll(), indexName).setPivotConfig(
            createPivotConfig(groups, aggs)
        )
            .setSyncConfig(new TimeSyncConfig("timestamp", TimeValue.timeValueSeconds(1)))
            .setRetentionPolicyConfig(new TimeRetentionPolicyConfig("timestamp", TimeValue.timeValueDays(1)))
            .build();
        putTransform(transformId, Strings.toString(config), RequestOptions.DEFAULT);
        assertThat(getTransform(transformId), hasKey("retention_policy"));
        String update = """
            {
                "retention_policy": null
            }
            """;
        updateConfig(transformId, update, RequestOptions.DEFAULT);
        assertThat(getTransform(transformId), not(hasKey("retention_policy")));
    }

    public void testStopWaitForCheckpoint() throws Exception {
        String indexName = "wait-for-checkpoint-reviews";
        String transformId = "transform-wait-for-checkpoint";
        createReviewsIndex(indexName, 1000, NUM_USERS, TransformIT::getUserIdForRow, TransformIT::getDateStringForRow);

        Map<String, SingleGroupSource> groups = new HashMap<>();
        groups.put("by-day", createDateHistogramGroupSourceWithCalendarInterval("timestamp", DateHistogramInterval.DAY, null));
        groups.put("by-user", new TermsGroupSource("user_id", null, false));
        groups.put("by-business", new TermsGroupSource("business_id", null, false));

        AggregatorFactories.Builder aggs = AggregatorFactories.builder()
            .addAggregator(AggregationBuilders.avg("review_score").field("stars"))
            .addAggregator(AggregationBuilders.max("timestamp").field("timestamp"));

        TransformConfig config = createTransformConfigBuilder(
            transformId,
            "reviews-by-user-business-day",
            QueryConfig.matchAll(),
            indexName
        ).setPivotConfig(createPivotConfig(groups, aggs))
            .setSyncConfig(new TimeSyncConfig("timestamp", TimeValue.timeValueSeconds(1)))
            .build();

        putTransform(transformId, Strings.toString(config), RequestOptions.DEFAULT);

        startTransform(config.getId(), RequestOptions.DEFAULT);

        // wait until transform has been triggered and indexed at least 1 document
        assertBusy(() -> {
            var stateAndStats = getTransformStats(config.getId());
            assertThat((Integer) XContentMapValues.extractValue("stats.documents_indexed", stateAndStats), greaterThan(1));
        });

        // waitForCheckpoint: true should make the transform continue until we hit the first checkpoint, then it will stop
        stopTransform(transformId, false, null, true);

        // Wait until the first checkpoint
        waitUntilCheckpoint(config.getId(), 1L);

        // Even though we are continuous, we should be stopped now as we needed to stop at the first checkpoint
        assertBusy(() -> {
            var stateAndStats = getTransformStats(config.getId());
            assertThat(stateAndStats.get("state"), equalTo("stopped"));
            assertThat((Integer) XContentMapValues.extractValue("stats.documents_indexed", stateAndStats), equalTo(1000));
        });

        int additionalRuns = randomIntBetween(1, 10);

        for (int i = 0; i < additionalRuns; ++i) {
            // index some more docs using a new user
            long timeStamp = Instant.now().toEpochMilli() - 1_000;
            long user = 42 + i;
            indexMoreDocs(timeStamp, user, indexName);
            startTransformWithRetryOnConflict(config.getId(), RequestOptions.DEFAULT);

            boolean waitForCompletion = randomBoolean();
            stopTransform(transformId, waitForCompletion, null, true);

            assertBusy(() -> {
                var stateAndStats = getTransformStats(config.getId());
                assertThat(stateAndStats.get("state"), equalTo("stopped"));
            });
        }

        var stateAndStats = getTransformStats(config.getId());
        assertThat(stateAndStats.get("state"), equalTo("stopped"));
        // Despite indexing new documents into the source index, the number of documents in the destination index stays the same.
        assertThat((Integer) XContentMapValues.extractValue("stats.documents_indexed", stateAndStats), equalTo(1000));

        stopTransform(transformId);
        deleteTransform(config.getId());
    }

    public void testContinuousTransformRethrottle() throws Exception {
        String indexName = "continuous-crud-reviews-throttled";
        String transformId = "transform-continuous-crud-throttled";

        createReviewsIndex(indexName, 1000, NUM_USERS, TransformIT::getUserIdForRow, TransformIT::getDateStringForRow);

        Map<String, SingleGroupSource> groups = new HashMap<>();
        groups.put("by-day", createDateHistogramGroupSourceWithCalendarInterval("timestamp", DateHistogramInterval.DAY, null));
        groups.put("by-user", new TermsGroupSource("user_id", null, false));
        groups.put("by-business", new TermsGroupSource("business_id", null, false));

        AggregatorFactories.Builder aggs = AggregatorFactories.builder()
            .addAggregator(AggregationBuilders.avg("review_score").field("stars"))
            .addAggregator(AggregationBuilders.max("timestamp").field("timestamp"));

        TransformConfig config = createTransformConfigBuilder(
            transformId,
            "reviews-by-user-business-day",
            QueryConfig.matchAll(),
            indexName
        ).setPivotConfig(createPivotConfig(groups, aggs))
            .setSyncConfig(new TimeSyncConfig("timestamp", TimeValue.timeValueSeconds(1)))
            // set requests per second and page size low enough to fail the test if update does not succeed,
            .setSettings(new SettingsConfig.Builder().setMaxPageSearchSize(20).setRequestsPerSecond(1F).setAlignCheckpoints(false).build())
            .build();

        putTransform(transformId, Strings.toString(config), RequestOptions.DEFAULT);
        startTransform(config.getId(), RequestOptions.DEFAULT);

        assertBusy(() -> {
            var stateAndStats = getTransformStats(config.getId());
            assertThat(stateAndStats.get("state"), equalTo("indexing"));
        });

        // test randomly: with explicit settings and reset to default
        String reqsPerSec = randomBoolean() ? "1000" : "null";
        String maxPageSize = randomBoolean() ? "1000" : "null";
        String update = Strings.format("""
            {
                "settings" : {
                    "docs_per_second": %s,
                    "max_page_search_size": %s
                }
            }
            """, reqsPerSec, maxPageSize);

        updateConfig(config.getId(), update, RequestOptions.DEFAULT);

        waitUntilCheckpoint(config.getId(), 1L);
        assertThat(getTransformState(config.getId()), equalTo("started"));

        var transformStats = getTransformStats(config.getId());
        int docsIndexed = (Integer) XContentMapValues.extractValue("stats.documents_indexed", transformStats);
        int pagesProcessed = (Integer) XContentMapValues.extractValue("stats.pages_processed", transformStats);

        var storedConfig = getTransform(config.getId());
        assertThat(storedConfig.get("version"), equalTo(Version.CURRENT.toString()));
        Instant now = Instant.now();
        long createTime = (long) storedConfig.get("create_time");
        assertTrue("[create_time] is not before current time", Instant.ofEpochMilli(createTime).isBefore(now));

        // index some more docs
        long timeStamp = Instant.now().toEpochMilli() - 1_000;
        long user = 42;
        indexMoreDocs(timeStamp, user, indexName);
        waitUntilCheckpoint(config.getId(), 2L);

        // Assert that we wrote the new docs
        assertThat(
            (Integer) XContentMapValues.extractValue("stats.documents_indexed", getTransformStats(config.getId())),
            greaterThan(docsIndexed)
        );

        // Assert less than 500 pages processed, so update worked
        assertThat(pagesProcessed, lessThan(1000));

        stopTransform(config.getId());
        deleteTransform(config.getId());
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/97536")
    public void testStartTransform_GivenTimeout_Returns408() throws Exception {
        String indexName = "start-transform-timeout-index";
        String transformId = "start-transform-timeout";
        createReviewsIndex(indexName, 10, NUM_USERS, TransformIT::getUserIdForRow, TransformIT::getDateStringForRow);

        Map<String, SingleGroupSource> groups = new HashMap<>();
        groups.put("by-day", createDateHistogramGroupSourceWithCalendarInterval("timestamp", DateHistogramInterval.DAY, null));
        groups.put("by-user", new TermsGroupSource("user_id", null, false));
        groups.put("by-business", new TermsGroupSource("business_id", null, false));

        AggregatorFactories.Builder aggs = AggregatorFactories.builder()
            .addAggregator(AggregationBuilders.avg("review_score").field("stars"))
            .addAggregator(AggregationBuilders.max("timestamp").field("timestamp"));

        TransformConfig config = createTransformConfigBuilder(transformId, indexName + "-dest", QueryConfig.matchAll(), indexName)
            .setPivotConfig(createPivotConfig(groups, aggs))
            .setSyncConfig(new TimeSyncConfig("timestamp", TimeValue.timeValueSeconds(1)))
            .setSettings(new SettingsConfig.Builder().setAlignCheckpoints(false).build())
            .build();

        putTransform(transformId, Strings.toString(config), RequestOptions.DEFAULT);
        ResponseException e = expectThrows(
            ResponseException.class,
            () -> startTransform(config.getId(), RequestOptions.DEFAULT.toBuilder().addParameter("timeout", "1nanos").build())
        );

        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(RestStatus.REQUEST_TIMEOUT.getStatus()));
    }

    private void indexMoreDocs(long timestamp, long userId, String index) throws Exception {
        StringBuilder bulkBuilder = new StringBuilder();
        for (int i = 0; i < 25; i++) {
            bulkBuilder.append(Strings.format("""
                {"create":{"_index":"%s"}}
                """, index));

            int stars = (i + 20) % 5;
            long business = (i + 100) % 50;

            String source = Strings.format("""
                {"user_id":"user_%s","count":%s,"business_id":"business_%s","stars":%s,"timestamp":%s}
                """, userId, i, business, stars, timestamp);
            bulkBuilder.append(source);
        }
        bulkBuilder.append("\r\n");
        doBulk(bulkBuilder.toString(), true);
    }
}
