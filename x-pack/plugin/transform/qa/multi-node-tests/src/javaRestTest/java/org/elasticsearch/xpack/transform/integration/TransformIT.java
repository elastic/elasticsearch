/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.integration;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
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
import org.elasticsearch.xpack.core.transform.TransformConfigVersion;
import org.elasticsearch.xpack.core.transform.transforms.QueryConfig;
import org.elasticsearch.xpack.core.transform.transforms.SettingsConfig;
import org.elasticsearch.xpack.core.transform.transforms.SyncConfig;
import org.elasticsearch.xpack.core.transform.transforms.TimeRetentionPolicyConfig;
import org.elasticsearch.xpack.core.transform.transforms.TimeSyncConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformStats;
import org.elasticsearch.xpack.core.transform.transforms.pivot.PivotConfig;
import org.elasticsearch.xpack.core.transform.transforms.pivot.SingleGroupSource;
import org.elasticsearch.xpack.core.transform.transforms.pivot.TermsGroupSource;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.oneOf;

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
        var transformId = "transform-crud";
        createStoppedTransform("basic-crud-reviews", transformId);
        assertBusy(() -> { assertEquals("stopped", getTransformState(transformId)); });

        var storedConfig = getTransform(transformId);
        assertThat(storedConfig.get("version"), equalTo(TransformConfigVersion.CURRENT.toString()));
        Instant now = Instant.now();
        long createTime = (long) storedConfig.get("create_time");
        assertTrue("[create_time] is not before current time", Instant.ofEpochMilli(createTime).isBefore(now));
        deleteTransform(transformId);
    }

    private void createStoppedTransform(String indexName, String transformId) throws Exception {
        createReviewsIndex(indexName, 100, NUM_USERS, TransformIT::getUserIdForRow, TransformIT::getDateStringForRow);

        var groups = Map.of(
            "by-day",
            createDateHistogramGroupSourceWithCalendarInterval("timestamp", DateHistogramInterval.DAY, null),
            "by-user",
            new TermsGroupSource("user_id", null, false),
            "by-business",
            new TermsGroupSource("business_id", null, false)
        );

        var aggs = AggregatorFactories.builder()
            .addAggregator(AggregationBuilders.avg("review_score").field("stars"))
            .addAggregator(AggregationBuilders.max("timestamp").field("timestamp"));

        var config = createTransformConfigBuilder(transformId, "reviews-by-user-business-day", QueryConfig.matchAll(), indexName)
            .setPivotConfig(createPivotConfig(groups, aggs))
            .build();

        putTransform(transformId, Strings.toString(config), RequestOptions.DEFAULT);
        startTransform(config.getId(), RequestOptions.DEFAULT);

        waitUntilCheckpoint(config.getId(), 1L);
        stopTransform(config.getId());
    }

    /**
     * Verify the basic stats API, which includes state, health, and optionally progress (if it exists).
     * These are required for Kibana 8.13+.
     */
    @SuppressWarnings("unchecked")
    public void testBasicTransformStats() throws Exception {
        var transformId = "transform-basic-stats";
        createStoppedTransform("basic-stats-reviews", transformId);
        var transformStats = getBasicTransformStats(transformId);

        assertBusy(() -> assertEquals("stopped", XContentMapValues.extractValue("state", transformStats)));
        assertEquals("green", XContentMapValues.extractValue("health.status", transformStats));
        assertThat(
            "percent_complete is not 100.0",
            XContentMapValues.extractValue("checkpointing.next.checkpoint_progress.percent_complete", transformStats),
            equalTo(100.0)
        );

        deleteTransform(transformId);
    }

    public void testContinuousTransformCrud() throws Exception {
        var transformId = "transform-continuous-crud";
        var indexName = "continuous-crud-reviews";
        createContinuousTransform(indexName, transformId, "reviews-by-user-business-day");
        var transformStats = getBasicTransformStats(transformId);
        assertThat(transformStats.get("state"), equalTo("started"));

        int docsIndexed = (Integer) XContentMapValues.extractValue("stats.documents_indexed", transformStats);

        var storedConfig = getTransform(transformId);
        assertThat(storedConfig.get("version"), equalTo(TransformConfigVersion.CURRENT.toString()));
        Instant now = Instant.now();
        long createTime = (long) storedConfig.get("create_time");
        assertTrue("[create_time] is not before current time", Instant.ofEpochMilli(createTime).isBefore(now));

        // index some more docs
        long timeStamp = Instant.now().toEpochMilli() - 1_000;
        long user = 42;
        indexMoreDocs(timeStamp, user, indexName);
        waitUntilCheckpoint(transformId, 2L);

        // Assert that we wrote the new docs

        assertThat(
            (Integer) XContentMapValues.extractValue("stats.documents_indexed", getBasicTransformStats(transformId)),
            greaterThan(docsIndexed)
        );

        stopTransform(transformId);
        deleteTransform(transformId);
    }

    private void createContinuousTransform(String indexName, String transformId, String destinationIndex) throws Exception {
        createReviewsIndex(indexName, 100, NUM_USERS, TransformIT::getUserIdForRow, TransformIT::getDateStringForRow);

        var groups = Map.of(
            "by-day",
            createDateHistogramGroupSourceWithCalendarInterval("timestamp", DateHistogramInterval.DAY, null),
            "by-user",
            new TermsGroupSource("user_id", null, false),
            "by-business",
            new TermsGroupSource("business_id", null, false)
        );

        var aggs = AggregatorFactories.builder()
            .addAggregator(AggregationBuilders.avg("review_score").field("stars"))
            .addAggregator(AggregationBuilders.max("timestamp").field("timestamp"));

        var config = createTransformConfigBuilder(transformId, destinationIndex, QueryConfig.matchAll(), indexName).setPivotConfig(
            createPivotConfig(groups, aggs)
        )
            .setSyncConfig(new TimeSyncConfig("timestamp", TimeValue.timeValueSeconds(1)))
            .setSettings(new SettingsConfig.Builder().setAlignCheckpoints(false).build())
            .build();

        putTransform(transformId, Strings.toString(config), RequestOptions.DEFAULT);
        startTransform(config.getId(), RequestOptions.DEFAULT);

        waitUntilCheckpoint(config.getId(), 1L);
    }

    /**
     * Verify the basic stats API, which includes state, health, and optionally progress (if it exists).
     * These are required for Kibana 8.13+.
     */
    @SuppressWarnings("unchecked")
    public void testBasicContinuousTransformStats() throws Exception {
        var transformId = "transform-continuous-basic-stats";
        createContinuousTransform("continuous-basic-stats-reviews", transformId, "reviews-by-user-business-day");
        var transformStats = getBasicTransformStats(transformId);

        assertEquals("started", XContentMapValues.extractValue("state", transformStats));
        assertEquals("green", XContentMapValues.extractValue("health.status", transformStats));

        // We aren't testing for 'checkpointing.next.checkpoint_progress.percent_complete'.
        // It's difficult to get the integration test to reliably call the stats API while that data is available, since continuous
        // transforms start and finish the next checkpoint quickly (<1ms).

        stopTransform(transformId);
        deleteTransform(transformId);
    }

    public void testDestinationIndexBlocked() throws Exception {
        var transformId = "transform-continuous-blocked-destination";
        var sourceIndexName = "source-reviews";
        var destIndexName = "destination-reviews";

        // create transform & indices, wait until 1st checkpoint is finished
        createContinuousTransform(sourceIndexName, transformId, destIndexName);

        // block destination index
        Request request = new Request("PUT", destIndexName + "/_block/write");
        assertAcknowledged(adminClient().performRequest(request));

        // index more docs so the checkpoint tries to run, wait until transform stops
        indexDoc(42, sourceIndexName);
        assertBusy(() -> { assertEquals(TransformStats.State.WAITING.value(), getTransformState(transformId)); }, 30, TimeUnit.SECONDS);

        // unblock index
        request = new Request("PUT", destIndexName + "/_settings");
        request.setJsonEntity("""
                { "blocks.write": false }
            """);
        assertAcknowledged(adminClient().performRequest(request));

        assertBusy(() -> {
            indexDoc(42, sourceIndexName);
            assertEquals(TransformStats.State.STARTED.value(), getTransformState(transformId));
        }, 30, TimeUnit.SECONDS);

        stopTransform(transformId);
        deleteTransform(transformId);
    }

    public void testUnblockWithNewDestinationIndex() throws Exception {
        var transformId = "transform-continuous-unblock-destination";
        var sourceIndexName = "source-reviews";
        var destIndexName = "destination-reviews-old";
        var newDestIndexName = "destination-reviews-new";

        // create transform & indices, wait until 1st checkpoint is finished
        createReviewsIndex(newDestIndexName, 100, NUM_USERS, TransformIT::getUserIdForRow, TransformIT::getDateStringForRow);
        createContinuousTransform(sourceIndexName, transformId, destIndexName);

        // block destination index
        Request request = new Request("PUT", destIndexName + "/_block/write");
        assertAcknowledged(adminClient().performRequest(request));

        // index more docs so the checkpoint tries to run, wait until transform stops
        indexDoc(42, sourceIndexName);
        assertBusy(() -> { assertEquals(TransformStats.State.WAITING.value(), getTransformState(transformId)); }, 30, TimeUnit.SECONDS);

        // change destination index
        var update = format("""
            {
                "description": "updated config",
                "dest": {
                   "index": "%s"
                }
            }
            """, newDestIndexName);
        updateConfig(transformId, update, true, RequestOptions.DEFAULT);

        assertBusy(() -> {
            assertThat(
                getTransformState(transformId),
                in(Set.of(TransformStats.State.STARTED.value(), TransformStats.State.INDEXING.value()))
            );
        }, 30, TimeUnit.SECONDS);

        stopTransform(transformId);
        deleteTransform(transformId);
    }

    public void testTransformLifecycleInALoop() throws Exception {
        String transformId = "lifecycle-in-a-loop";
        String indexName = transformId + "-src";
        createReviewsIndex(indexName, 100, NUM_USERS, TransformIT::getUserIdForRow, TransformIT::getDateStringForRow);

        String destIndex = transformId + "-dest";
        String config = createConfig(transformId, indexName, destIndex);
        for (int i = 0; i < 100; ++i) {
            long sleepAfterStartMillis = randomLongBetween(0, 5_000);
            boolean force = randomBoolean();
            try {
                // Create the continuous transform.
                putTransform(transformId, config, RequestOptions.DEFAULT);
                assertThat(getTransformTasks(), is(empty()));
                assertThat(getTransformTasksFromClusterState(transformId), is(empty()));
                assertThat("Node stats were: " + entityAsMap(getNodeStats()), getTotalRegisteredTransformCount(), is(equalTo(0)));

                startTransform(transformId, RequestOptions.DEFAULT);
                // There is 1 transform task after start.
                assertThat(getTransformTasks(), hasSize(1));
                assertThat(getTransformTasksFromClusterState(transformId), hasSize(1));
                assertThat("Node stats were: " + entityAsMap(getNodeStats()), getTotalRegisteredTransformCount(), is(equalTo(1)));

                Thread.sleep(sleepAfterStartMillis);
                // There should still be 1 transform task as the transform is continuous.
                assertThat(getTransformTasks(), hasSize(1));
                assertThat(getTransformTasksFromClusterState(transformId), hasSize(1));
                assertThat("Node stats were: " + entityAsMap(getNodeStats()), getTotalRegisteredTransformCount(), is(equalTo(1)));

                // Stop the transform with force set randomly.
                stopTransform(transformId, true, null, false, force);
                if (force) {
                    // If the "force" has been used, then the persistent task is removed from the cluster state but the local task can still
                    // be seen by the PersistentTasksNodeService. We need to wait until PersistentTasksNodeService reconciles the state.
                    assertBusy(() -> assertThat(getTransformTasks(), is(empty())));
                } else {
                    // If the "force" hasn't been used then we can expect the local task to be already gone.
                    assertThat(getTransformTasks(), is(empty()));
                }
                // After the transform is stopped, there should be no transform task left in the cluster state.
                assertThat(getTransformTasksFromClusterState(transformId), is(empty()));
                assertThat("Node stats were: " + entityAsMap(getNodeStats()), getTotalRegisteredTransformCount(), is(equalTo(0)));

                // Delete the transform
                deleteTransform(transformId);
            } catch (AssertionError | Exception e) {
                throw new AssertionError(
                    format("Failure at iteration %d (sleepAfterStart=%sms,force=%s): %s", i, sleepAfterStartMillis, force, e.getMessage()),
                    e
                );
            }
        }
    }

    private String createConfig(String transformId, String sourceIndex, String destIndex) throws Exception {
        Map<String, SingleGroupSource> groups = new HashMap<>();
        groups.put("by-day", createDateHistogramGroupSourceWithCalendarInterval("timestamp", DateHistogramInterval.DAY, null));
        groups.put("by-user", new TermsGroupSource("user_id", null, false));
        groups.put("by-business", new TermsGroupSource("business_id", null, false));

        AggregatorFactories.Builder aggs = AggregatorFactories.builder()
            .addAggregator(AggregationBuilders.avg("review_score").field("stars"))
            .addAggregator(AggregationBuilders.max("timestamp").field("timestamp"));

        PivotConfig pivotConfig = createPivotConfig(groups, aggs);

        SyncConfig syncConfig = new TimeSyncConfig("timestamp", TimeValue.timeValueSeconds(1));

        TransformConfig config = createTransformConfigBuilder(transformId, destIndex, QueryConfig.matchAll(), sourceIndex).setFrequency(
            TimeValue.timeValueSeconds(1)
        ).setSyncConfig(syncConfig).setPivotConfig(pivotConfig).build();

        return Strings.toString(config);
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

        int docsIndexed = (Integer) XContentMapValues.extractValue("stats.documents_indexed", getBasicTransformStats(config.getId()));

        var storedConfig = getTransform(config.getId());
        assertThat(storedConfig.get("version"), equalTo(TransformConfigVersion.CURRENT.toString()));
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

        String update = format("""
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
        int numDocsAfterCp2 = (Integer) XContentMapValues.extractValue("stats.documents_indexed", getBasicTransformStats(config.getId()));
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
            refreshIndex(dest);
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
            var stateAndStats = getBasicTransformStats(transformId);
            assertThat((Integer) XContentMapValues.extractValue("stats.documents_indexed", stateAndStats), greaterThan(1));
        });

        // waitForCheckpoint: true should make the transform continue until we hit the first checkpoint, then it will stop
        stopTransform(transformId, false, null, true, false);

        // Wait until the first checkpoint
        waitUntilCheckpoint(config.getId(), 1L);
        var previousTriggerCount = new AtomicInteger(0);

        // Even though we are continuous, we should be stopped now as we needed to stop at the first checkpoint
        assertBusy(() -> {
            var stateAndStats = getBasicTransformStats(transformId);
            assertThat(stateAndStats.get("state"), equalTo("stopped"));
            assertThat((Integer) XContentMapValues.extractValue("stats.documents_indexed", stateAndStats), equalTo(1000));
            previousTriggerCount.set((int) XContentMapValues.extractValue("stats.trigger_count", stateAndStats));
        });

        // Create N additional runs of starting and stopping
        int additionalRuns = randomIntBetween(1, 10);

        for (int i = 0; i < additionalRuns; ++i) {
            var testFailureMessage = format("Can't determine if Transform ran for iteration number [%d] out of [%d].", i, additionalRuns);
            // index some more docs using a new user
            var timeStamp = Instant.now().toEpochMilli() - 1_000;
            var user = 42 + i;
            indexMoreDocs(timeStamp, user, indexName);
            startTransformWithRetryOnConflict(transformId, RequestOptions.DEFAULT);

            assertBusy(() -> {
                var stateAndStats = getBasicTransformStats(transformId);
                var currentTriggerCount = (int) XContentMapValues.extractValue("stats.trigger_count", stateAndStats);
                // We should verify that we are retrieving the stats *after* this run had been started.
                // If the trigger_count has increased, we know we have started this test iteration.
                assertThat(testFailureMessage, previousTriggerCount.get(), lessThan(currentTriggerCount));
            });

            var waitForCompletion = randomBoolean();
            stopTransform(transformId, waitForCompletion, null, true, false);
            assertBusy(() -> {
                var stateAndStats = getBasicTransformStats(transformId);
                assertThat(stateAndStats.get("state"), equalTo("stopped"));
                previousTriggerCount.set((int) XContentMapValues.extractValue("stats.trigger_count", stateAndStats));
            });
        }

        var stateAndStats = getBasicTransformStats(transformId);
        assertThat(stateAndStats.get("state"), equalTo("stopped"));
        // Despite indexing new documents into the source index, the number of documents in the destination index stays the same.
        assertThat((Integer) XContentMapValues.extractValue("stats.documents_indexed", stateAndStats), equalTo(1000));

        stopTransform(transformId);
        deleteTransform(transformId);
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

        assertBusy(() -> { assertThat(getTransformState(config.getId()), equalTo("indexing")); });

        // test randomly: with explicit settings and reset to default
        String reqsPerSec = randomBoolean() ? "1000" : "null";
        String maxPageSize = randomBoolean() ? "1000" : "null";
        String update = format("""
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

        var transformStats = getBasicTransformStats(config.getId());
        int docsIndexed = (Integer) XContentMapValues.extractValue("stats.documents_indexed", transformStats);
        int pagesProcessed = (Integer) XContentMapValues.extractValue("stats.pages_processed", transformStats);

        var storedConfig = getTransform(config.getId());
        assertThat(storedConfig.get("version"), equalTo(TransformConfigVersion.CURRENT.toString()));
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

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/109101")
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
        assertThat(e.getMessage(), containsString("Starting transform [" + transformId + "] timed out after [1nanos]"));

        // After we've verified that the _start call timed out, we stop the transform (which might have been started despite timeout).
        try {
            stopTransform(transformId);
        } catch (ResponseException e2) {
            // It can be that the _stop call timed out because of the race condition, i.e.: the _stop call executed *before* the _start call
            // because of how threads were scheduled by the generic thread pool and now the current transform state is STARTED.
            // In such a case, we repeat the _stop call to make the transform STOPPED.
            assertThat(e2.getResponse().getStatusLine().getStatusCode(), equalTo(RestStatus.REQUEST_TIMEOUT.getStatus()));
            assertThat(e2.getMessage(), containsString("Could not stop the transforms [" + transformId + "] as they timed out [30s]."));
            stopTransform(transformId);
        }
    }

    private void indexMoreDocs(long timestamp, long userId, String index) throws Exception {
        StringBuilder bulkBuilder = new StringBuilder();
        for (int i = 0; i < 25; i++) {
            bulkBuilder.append(format("""
                {"create":{"_index":"%s"}}
                """, index));

            int stars = (i + 20) % 5;
            long business = (i + 100) % 50;

            String source = format("""
                {"user_id":"user_%s","count":%s,"business_id":"business_%s","stars":%s,"timestamp":%s}
                """, userId, i, business, stars, timestamp);
            bulkBuilder.append(source);
        }
        bulkBuilder.append("\r\n");
        doBulk(bulkBuilder.toString(), true);
    }

    private void indexDoc(long userId, String index) throws Exception {
        StringBuilder bulkBuilder = new StringBuilder();
        bulkBuilder.append(format("""
            {"create":{"_index":"%s"}}
            """, index));
        String source = format("""
            {"user_id":"user_%s","count":%s,"business_id":"business_%s","stars":%s,"timestamp":%s}
            """, userId, 1, 2, 5, Instant.now().toEpochMilli());
        bulkBuilder.append(source);
        bulkBuilder.append("\r\n");
        doBulk(bulkBuilder.toString(), true);
    }
}
