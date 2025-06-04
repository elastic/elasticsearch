/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr;

import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.CcrSingleNodeTestCase;
import org.elasticsearch.xpack.core.ccr.action.CcrStatsAction;
import org.elasticsearch.xpack.core.ccr.action.FollowStatsAction;
import org.elasticsearch.xpack.core.ccr.action.PauseFollowAction;
import org.elasticsearch.xpack.core.ccr.action.PutAutoFollowPatternAction;
import org.elasticsearch.xpack.core.ccr.action.PutFollowAction;
import org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.stream.StreamSupport;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

public class LocalIndexFollowingIT extends CcrSingleNodeTestCase {

    public void testFollowIndex() throws Exception {
        final String leaderIndexSettings = getIndexSettings(2, 0, Collections.emptyMap());
        assertAcked(client().admin().indices().prepareCreate("leader").setSource(leaderIndexSettings, XContentType.JSON));
        ensureGreen("leader");

        final long firstBatchNumDocs = randomIntBetween(2, 64);
        for (int i = 0; i < firstBatchNumDocs; i++) {
            prepareIndex("leader").setSource("{}", XContentType.JSON).get();
        }

        final PutFollowAction.Request followRequest = getPutFollowRequest("leader", "follower");
        client().execute(PutFollowAction.INSTANCE, followRequest).get();

        assertBusy(() -> assertHitCount(client().prepareSearch("follower"), firstBatchNumDocs));

        final long secondBatchNumDocs = randomIntBetween(2, 64);
        for (int i = 0; i < secondBatchNumDocs; i++) {
            prepareIndex("leader").setSource("{}", XContentType.JSON).get();
        }

        assertBusy(() -> assertHitCount(client().prepareSearch("follower"), firstBatchNumDocs + secondBatchNumDocs));

        PauseFollowAction.Request pauseRequest = new PauseFollowAction.Request(TEST_REQUEST_TIMEOUT, "follower");
        client().execute(PauseFollowAction.INSTANCE, pauseRequest);

        final long thirdBatchNumDocs = randomIntBetween(2, 64);
        for (int i = 0; i < thirdBatchNumDocs; i++) {
            prepareIndex("leader").setSource("{}", XContentType.JSON).get();
        }

        client().execute(ResumeFollowAction.INSTANCE, getResumeFollowRequest("follower")).get();
        assertBusy(() -> assertHitCount(client().prepareSearch("follower"), firstBatchNumDocs + secondBatchNumDocs + thirdBatchNumDocs));
        ensureEmptyWriteBuffers();
    }

    public void testIndexingMetricsIncremented() throws Exception {
        final String leaderIndexSettings = getIndexSettings(1, 0, Collections.emptyMap());
        assertAcked(client().admin().indices().prepareCreate("leader").setSource(leaderIndexSettings, XContentType.JSON));
        ensureGreen("leader");

        // Use a sufficiently small number of docs to ensure that they are well below the number of docs that
        // can be sent in a single TransportBulkShardOperationsAction
        final long firstBatchNumDocs = randomIntBetween(10, 20);
        long sourceSize = 0;
        for (int i = 0; i < firstBatchNumDocs; i++) {
            BytesArray source = new BytesArray("{}");
            sourceSize += source.length();
            prepareIndex("leader").setSource(source, XContentType.JSON).get();
        }

        ThreadPool nodeThreadPool = getInstanceFromNode(ThreadPool.class);
        ThreadPool.Info writeInfo = StreamSupport.stream(nodeThreadPool.info().spliterator(), false)
            .filter(i -> i.getName().equals(ThreadPool.Names.WRITE))
            .findAny()
            .get();
        int numberOfThreads = writeInfo.getMax();
        CountDownLatch threadBlockedLatch = new CountDownLatch(numberOfThreads);
        CountDownLatch blocker = new CountDownLatch(1);

        for (int i = 0; i < numberOfThreads; ++i) {
            nodeThreadPool.executor(ThreadPool.Names.WRITE).execute(() -> {
                try {
                    threadBlockedLatch.countDown();
                    blocker.await();
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
            });
        }
        threadBlockedLatch.await();

        try {
            final PutFollowAction.Request followRequest = getPutFollowRequest("leader", "follower");
            client().execute(PutFollowAction.INSTANCE, followRequest).get();

            IndexingPressure indexingPressure = getInstanceFromNode(IndexingPressure.class);
            final long finalSourceSize = sourceSize;
            assertBusy(() -> {
                // The actual write bytes will be greater due to other request fields. However, this test is
                // just spot checking that the bytes are incremented at all.
                assertTrue(indexingPressure.stats().getCurrentCombinedCoordinatingAndPrimaryBytes() > finalSourceSize);
                assertEquals(firstBatchNumDocs, indexingPressure.stats().getCurrentPrimaryOps());
            });
            blocker.countDown();
            assertBusy(() -> assertHitCount(client().prepareSearch("follower"), firstBatchNumDocs));
            ensureEmptyWriteBuffers();
        } finally {
            if (blocker.getCount() > 0) {
                blocker.countDown();
            }
        }

    }

    public void testRemoveRemoteConnection() throws Exception {
        PutAutoFollowPatternAction.Request request = new PutAutoFollowPatternAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT);
        request.setName("my_pattern");
        request.setRemoteCluster("local");
        request.setLeaderIndexPatterns(Collections.singletonList("logs-*"));
        request.setFollowIndexNamePattern("copy-{{leader_index}}");
        request.getParameters().setReadPollTimeout(TimeValue.timeValueMillis(10));
        assertTrue(client().execute(PutAutoFollowPatternAction.INSTANCE, request).actionGet().isAcknowledged());
        long previousNumberOfSuccessfulFollowedIndices = getAutoFollowStats().getNumberOfSuccessfulFollowIndices();

        Settings leaderIndexSettings = Settings.builder()
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
            .build();
        createIndex("logs-20200101", leaderIndexSettings);
        prepareIndex("logs-20200101").setSource("{}", XContentType.JSON).get();
        assertBusy(() -> {
            CcrStatsAction.Response response = client().execute(CcrStatsAction.INSTANCE, new CcrStatsAction.Request(TEST_REQUEST_TIMEOUT))
                .actionGet();
            assertThat(
                response.getAutoFollowStats().getNumberOfSuccessfulFollowIndices(),
                equalTo(previousNumberOfSuccessfulFollowedIndices + 1)
            );
            assertThat(response.getFollowStats().getStatsResponses().size(), equalTo(1));
            assertThat(response.getFollowStats().getStatsResponses().get(0).status().followerGlobalCheckpoint(), equalTo(0L));
        });

        // Both auto follow patterns and index following should be resilient to remote connection being missing:
        removeLocalRemote();
        // This triggers a cluster state update, which should let auto follow coordinator retry auto following:
        setupLocalRemote();

        // This new index should be picked up by auto follow coordinator
        createIndex("logs-20200102", leaderIndexSettings);
        // This new document should be replicated to follower index:
        prepareIndex("logs-20200101").setSource("{}", XContentType.JSON).get();
        assertBusy(() -> {
            CcrStatsAction.Response response = client().execute(CcrStatsAction.INSTANCE, new CcrStatsAction.Request(TEST_REQUEST_TIMEOUT))
                .actionGet();
            assertThat(
                response.getAutoFollowStats().getNumberOfSuccessfulFollowIndices(),
                equalTo(previousNumberOfSuccessfulFollowedIndices + 2)
            );

            FollowStatsAction.StatsRequest statsRequest = new FollowStatsAction.StatsRequest();
            statsRequest.setIndices(new String[] { "copy-logs-20200101" });
            FollowStatsAction.StatsResponses responses = client().execute(FollowStatsAction.INSTANCE, statsRequest).actionGet();
            assertThat(responses.getStatsResponses().size(), equalTo(1));
            assertThat(responses.getStatsResponses().get(0).status().getFatalException(), nullValue());
            assertThat(responses.getStatsResponses().get(0).status().followerGlobalCheckpoint(), equalTo(1L));
        });
    }

    public void testChangeLeaderIndex() throws Exception {
        final String settings = getIndexSettings(1, 0, Collections.emptyMap());

        // First, let index-1 is writable and index-2 follows index-1
        assertAcked(client().admin().indices().prepareCreate("index-1").setSource(settings, XContentType.JSON));
        ensureGreen("index-1");
        int numDocs = between(1, 100);
        for (int i = 0; i < numDocs; i++) {
            prepareIndex("index-1").setSource("{}", XContentType.JSON).get();
        }
        client().execute(PutFollowAction.INSTANCE, getPutFollowRequest("index-1", "index-2")).get();
        assertBusy(() -> assertHitCount(client().prepareSearch("index-2"), numDocs));

        // Then switch index-1 to be a follower of index-0
        assertAcked(client().admin().indices().prepareCreate("index-0").setSource(settings, XContentType.JSON));
        final int newDocs;
        if (randomBoolean()) {
            newDocs = randomIntBetween(0, numDocs);
        } else {
            newDocs = numDocs + randomIntBetween(1, 100);
        }
        for (int i = 0; i < newDocs; i++) {
            prepareIndex("index-0").setSource("{}", XContentType.JSON).get();
        }
        if (randomBoolean()) {
            client().admin().indices().prepareFlush("index-0").get();
        }
        assertAcked(client().admin().indices().prepareClose("index-1"));
        client().execute(PutFollowAction.INSTANCE, getPutFollowRequest("index-0", "index-1")).get();

        // index-2 should detect that the leader index has changed
        assertBusy(() -> {
            FollowStatsAction.StatsRequest statsRequest = new FollowStatsAction.StatsRequest();
            statsRequest.setIndices(new String[] { "index-2" });
            FollowStatsAction.StatsResponses resp = client().execute(FollowStatsAction.INSTANCE, statsRequest).actionGet();
            assertThat(resp.getStatsResponses(), hasSize(1));
            FollowStatsAction.StatsResponse stats = resp.getStatsResponses().get(0);
            assertNotNull(stats.status().getFatalException());
            Throwable unwrapped = ExceptionsHelper.unwrap(stats.status().getFatalException(), IllegalStateException.class);
            assertNotNull(unwrapped);
            assertThat(unwrapped.getMessage(), containsString("unexpected history uuid"));
        });
    }

    public static String getIndexSettings(
        final int numberOfShards,
        final int numberOfReplicas,
        final Map<String, String> additionalIndexSettings
    ) throws IOException {
        final String settings;
        try (XContentBuilder builder = jsonBuilder()) {
            builder.startObject();
            {
                builder.startObject("settings");
                {
                    builder.field("index.number_of_shards", numberOfShards);
                    builder.field("index.number_of_replicas", numberOfReplicas);
                    for (final Map.Entry<String, String> additionalSetting : additionalIndexSettings.entrySet()) {
                        builder.field(additionalSetting.getKey(), additionalSetting.getValue());
                    }
                }
                builder.endObject();
            }
            builder.endObject();
            settings = BytesReference.bytes(builder).utf8ToString();
        }
        return settings;
    }

}
