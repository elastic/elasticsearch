/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.CheckedRunnable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.xpack.CCRIntegTestCase;
import org.elasticsearch.xpack.ccr.action.ShardFollowTask;
import org.elasticsearch.xpack.ccr.index.engine.FollowingEngine;
import org.elasticsearch.xpack.core.ccr.ShardFollowNodeTaskStatus;
import org.elasticsearch.xpack.core.ccr.action.FollowStatsAction;
import org.elasticsearch.xpack.core.ccr.action.FollowStatsAction.StatsRequest;
import org.elasticsearch.xpack.core.ccr.action.FollowStatsAction.StatsResponses;
import org.elasticsearch.xpack.core.ccr.action.PauseFollowAction;
import org.elasticsearch.xpack.core.ccr.action.PutFollowAction;
import org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction;
import org.elasticsearch.xpack.core.ccr.action.UnfollowAction;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class IndexFollowingIT extends CCRIntegTestCase {

    public void testFollowIndex() throws Exception {
        final int numberOfPrimaryShards = randomIntBetween(1, 3);
        final String leaderIndexSettings = getIndexSettings(numberOfPrimaryShards, between(0, 1),
            singletonMap(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), "true"));
        assertAcked(leaderClient().admin().indices().prepareCreate("index1").setSource(leaderIndexSettings, XContentType.JSON));
        ensureLeaderYellow("index1");

        final PutFollowAction.Request followRequest = follow("index1", "index2");
        followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();

        final int firstBatchNumDocs = randomIntBetween(2, 64);
        logger.info("Indexing [{}] docs as first batch", firstBatchNumDocs);
        for (int i = 0; i < firstBatchNumDocs; i++) {
            final String source = String.format(Locale.ROOT, "{\"f\":%d}", i);
            leaderClient().prepareIndex("index1", "doc", Integer.toString(i)).setSource(source, XContentType.JSON).get();
        }

        final Map<ShardId, Long> firstBatchNumDocsPerShard = new HashMap<>();
        final ShardStats[] firstBatchShardStats =
            leaderClient().admin().indices().prepareStats("index1").get().getIndex("index1").getShards();
        for (final ShardStats shardStats : firstBatchShardStats) {
            if (shardStats.getShardRouting().primary()) {
                long value = shardStats.getStats().getIndexing().getTotal().getIndexCount() - 1;
                firstBatchNumDocsPerShard.put(shardStats.getShardRouting().shardId(), value);
            }
        }

        assertBusy(assertTask(numberOfPrimaryShards, firstBatchNumDocsPerShard));

        for (int i = 0; i < firstBatchNumDocs; i++) {
            assertBusy(assertExpectedDocumentRunnable(i));
        }
        assertTotalNumberOfOptimizedIndexing(resolveFollowerIndex("index2"), numberOfPrimaryShards, firstBatchNumDocs);
        unfollowIndex("index2");
        followerClient().execute(ResumeFollowAction.INSTANCE, followRequest.getFollowRequest()).get();
        final int secondBatchNumDocs = randomIntBetween(2, 64);
        logger.info("Indexing [{}] docs as second batch", secondBatchNumDocs);
        for (int i = firstBatchNumDocs; i < firstBatchNumDocs + secondBatchNumDocs; i++) {
            final String source = String.format(Locale.ROOT, "{\"f\":%d}", i);
            leaderClient().prepareIndex("index1", "doc", Integer.toString(i)).setSource(source, XContentType.JSON).get();
        }

        final Map<ShardId, Long> secondBatchNumDocsPerShard = new HashMap<>();
        final ShardStats[] secondBatchShardStats =
            leaderClient().admin().indices().prepareStats("index1").get().getIndex("index1").getShards();
        for (final ShardStats shardStats : secondBatchShardStats) {
            if (shardStats.getShardRouting().primary()) {
                final long value = shardStats.getStats().getIndexing().getTotal().getIndexCount() - 1;
                secondBatchNumDocsPerShard.put(shardStats.getShardRouting().shardId(), value);
            }
        }

        assertBusy(assertTask(numberOfPrimaryShards, secondBatchNumDocsPerShard));

        for (int i = firstBatchNumDocs; i < firstBatchNumDocs + secondBatchNumDocs; i++) {
            assertBusy(assertExpectedDocumentRunnable(i));
        }
        assertTotalNumberOfOptimizedIndexing(resolveFollowerIndex("index2"), numberOfPrimaryShards,
            firstBatchNumDocs + secondBatchNumDocs);
        unfollowIndex("index2");
        assertMaxSeqNoOfUpdatesIsTransferred(resolveLeaderIndex("index1"), resolveFollowerIndex("index2"), numberOfPrimaryShards);
    }

    public void testSyncMappings() throws Exception {
        final String leaderIndexSettings = getIndexSettings(2, between(0, 1),
            singletonMap(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), "true"));
        assertAcked(leaderClient().admin().indices().prepareCreate("index1").setSource(leaderIndexSettings, XContentType.JSON));
        ensureLeaderYellow("index1");

        final PutFollowAction.Request followRequest = follow("index1", "index2");
        followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();

        final long firstBatchNumDocs = randomIntBetween(2, 64);
        for (long i = 0; i < firstBatchNumDocs; i++) {
            final String source = String.format(Locale.ROOT, "{\"f\":%d}", i);
            leaderClient().prepareIndex("index1", "doc", Long.toString(i)).setSource(source, XContentType.JSON).get();
        }

        assertBusy(() -> assertThat(followerClient().prepareSearch("index2").get().getHits().totalHits, equalTo(firstBatchNumDocs)));
        MappingMetaData mappingMetaData = followerClient().admin().indices().prepareGetMappings("index2").get().getMappings()
            .get("index2").get("doc");
        assertThat(XContentMapValues.extractValue("properties.f.type", mappingMetaData.sourceAsMap()), equalTo("integer"));
        assertThat(XContentMapValues.extractValue("properties.k", mappingMetaData.sourceAsMap()), nullValue());

        final int secondBatchNumDocs = randomIntBetween(2, 64);
        for (long i = firstBatchNumDocs; i < firstBatchNumDocs + secondBatchNumDocs; i++) {
            final String source = String.format(Locale.ROOT, "{\"k\":%d}", i);
            leaderClient().prepareIndex("index1", "doc", Long.toString(i)).setSource(source, XContentType.JSON).get();
        }

        assertBusy(() -> assertThat(followerClient().prepareSearch("index2").get().getHits().totalHits,
            equalTo(firstBatchNumDocs + secondBatchNumDocs)));
        mappingMetaData = followerClient().admin().indices().prepareGetMappings("index2").get().getMappings()
            .get("index2").get("doc");
        assertThat(XContentMapValues.extractValue("properties.f.type", mappingMetaData.sourceAsMap()), equalTo("integer"));
        assertThat(XContentMapValues.extractValue("properties.k.type", mappingMetaData.sourceAsMap()), equalTo("long"));
        unfollowIndex("index2");
        assertMaxSeqNoOfUpdatesIsTransferred(resolveLeaderIndex("index1"), resolveFollowerIndex("index2"), 2);
    }

    public void testNoMappingDefined() throws Exception {
        assertAcked(leaderClient().admin().indices().prepareCreate("index1")
            .setSettings(Settings.builder()
                .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()));
        ensureLeaderGreen("index1");

        final PutFollowAction.Request followRequest = follow("index1", "index2");
        followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();

        leaderClient().prepareIndex("index1", "doc", "1").setSource("{\"f\":1}", XContentType.JSON).get();
        assertBusy(() -> assertThat(followerClient().prepareSearch("index2").get().getHits().totalHits, equalTo(1L)));
        unfollowIndex("index2");

        MappingMetaData mappingMetaData = followerClient().admin().indices().prepareGetMappings("index2").get().getMappings()
            .get("index2").get("doc");
        assertThat(XContentMapValues.extractValue("properties.f.type", mappingMetaData.sourceAsMap()), equalTo("long"));
        assertThat(XContentMapValues.extractValue("properties.k", mappingMetaData.sourceAsMap()), nullValue());
    }

    public void testFollowIndex_backlog() throws Exception {
        int numberOfShards = between(1, 5);
        String leaderIndexSettings = getIndexSettings(numberOfShards, between(0, 1),
            singletonMap(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), "true"));
        assertAcked(leaderClient().admin().indices().prepareCreate("index1").setSource(leaderIndexSettings, XContentType.JSON));
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {}

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {}

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {}
        };
        BulkProcessor bulkProcessor = BulkProcessor.builder(leaderClient(), listener)
            .setBulkActions(100)
            .setConcurrentRequests(4)
            .build();
        AtomicBoolean run = new AtomicBoolean(true);
        Thread thread = new Thread(() -> {
            int counter = 0;
            while (run.get()) {
                final String source = String.format(Locale.ROOT, "{\"f\":%d}", counter++);
                IndexRequest indexRequest = new IndexRequest("index1", "doc")
                    .source(source, XContentType.JSON)
                    .timeout(TimeValue.timeValueSeconds(1));
                bulkProcessor.add(indexRequest);
            }
        });
        thread.start();

        // Waiting for some document being index before following the index:
        int maxReadSize = randomIntBetween(128, 2048);
        long numDocsIndexed = Math.min(3000 * 2, randomLongBetween(maxReadSize, maxReadSize * 10));
        atLeastDocsIndexed(leaderClient(), "index1", numDocsIndexed / 3);

        PutFollowAction.Request followRequest = follow("index1", "index2");
        followRequest.getFollowRequest().setMaxBatchOperationCount(maxReadSize);
        followRequest.getFollowRequest().setMaxConcurrentReadBatches(randomIntBetween(2, 10));
        followRequest.getFollowRequest().setMaxConcurrentWriteBatches(randomIntBetween(2, 10));
        followRequest.getFollowRequest().setMaxWriteBufferSize(randomIntBetween(1024, 10240));
        followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();

        atLeastDocsIndexed(leaderClient(), "index1", numDocsIndexed);
        run.set(false);
        thread.join();
        assertThat(bulkProcessor.awaitClose(1L, TimeUnit.MINUTES), is(true));

        assertSameDocCount("index1", "index2");
        assertTotalNumberOfOptimizedIndexing(resolveFollowerIndex("index2"), numberOfShards,
            leaderClient().prepareSearch("index1").get().getHits().totalHits);
        unfollowIndex("index2");
        assertMaxSeqNoOfUpdatesIsTransferred(resolveLeaderIndex("index1"), resolveFollowerIndex("index2"), numberOfShards);
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/33337")
    public void testFollowIndexAndCloseNode() throws Exception {
        getFollowerCluster().ensureAtLeastNumDataNodes(3);
        String leaderIndexSettings = getIndexSettings(3, 1, singletonMap(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), "true"));
        assertAcked(leaderClient().admin().indices().prepareCreate("index1").setSource(leaderIndexSettings, XContentType.JSON));
        ensureLeaderGreen("index1");

        AtomicBoolean run = new AtomicBoolean(true);
        Thread thread = new Thread(() -> {
            int counter = 0;
            while (run.get()) {
                final String source = String.format(Locale.ROOT, "{\"f\":%d}", counter++);
                try {
                    leaderClient().prepareIndex("index1", "doc")
                        .setSource(source, XContentType.JSON)
                        .setTimeout(TimeValue.timeValueSeconds(1))
                        .get();
                } catch (Exception e) {
                    logger.error("Error while indexing into leader index", e);
                }
            }
        });
        thread.start();

        PutFollowAction.Request followRequest = follow("index1", "index2");
        followRequest.getFollowRequest().setMaxBatchOperationCount(randomIntBetween(32, 2048));
        followRequest.getFollowRequest().setMaxConcurrentReadBatches(randomIntBetween(2, 10));
        followRequest.getFollowRequest().setMaxConcurrentWriteBatches(randomIntBetween(2, 10));
        followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();

        long maxNumDocsReplicated = Math.min(1000, randomLongBetween(followRequest.getFollowRequest().getMaxBatchOperationCount(),
            followRequest.getFollowRequest().getMaxBatchOperationCount() * 10));
        long minNumDocsReplicated = maxNumDocsReplicated / 3L;
        logger.info("waiting for at least [{}] documents to be indexed and then stop a random data node", minNumDocsReplicated);
        atLeastDocsIndexed(followerClient(), "index2", minNumDocsReplicated);
        getFollowerCluster().stopRandomNonMasterNode();
        logger.info("waiting for at least [{}] documents to be indexed", maxNumDocsReplicated);
        atLeastDocsIndexed(followerClient(), "index2", maxNumDocsReplicated);
        run.set(false);
        thread.join();

        assertSameDocCount("index1", "index2");
        unfollowIndex("index2");
        assertMaxSeqNoOfUpdatesIsTransferred(resolveLeaderIndex("index1"), resolveFollowerIndex("index2"), 3);
    }

    public void testFollowIndexWithNestedField() throws Exception {
        final String leaderIndexSettings =
            getIndexSettingsWithNestedMapping(1, between(0, 1), singletonMap(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), "true"));
        assertAcked(leaderClient().admin().indices().prepareCreate("index1").setSource(leaderIndexSettings, XContentType.JSON));
        ensureLeaderGreen("index1");

        final PutFollowAction.Request followRequest = follow("index1", "index2");
        followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();

        final int numDocs = randomIntBetween(2, 64);
        for (int i = 0; i < numDocs; i++) {
            try (XContentBuilder builder = jsonBuilder()) {
                builder.startObject();
                builder.field("field", "value");
                builder.startArray("objects");
                {
                    builder.startObject();
                    builder.field("field", i);
                    builder.endObject();
                }
                builder.endArray();
                builder.endObject();
                leaderClient().prepareIndex("index1", "doc", Integer.toString(i)).setSource(builder).get();
            }
        }

        for (int i = 0; i < numDocs; i++) {
            int value = i;
            assertBusy(() -> {
                final GetResponse getResponse = followerClient().prepareGet("index2", "doc", Integer.toString(value)).get();
                assertTrue(getResponse.isExists());
                assertTrue((getResponse.getSource().containsKey("field")));
                assertThat(XContentMapValues.extractValue("objects.field", getResponse.getSource()),
                    equalTo(Collections.singletonList(value)));
            });
        }
        unfollowIndex("index2");
        assertMaxSeqNoOfUpdatesIsTransferred(resolveLeaderIndex("index1"), resolveFollowerIndex("index2"), 1);
        assertTotalNumberOfOptimizedIndexing(resolveFollowerIndex("index2"), 1, numDocs);
    }

    public void testUnfollowNonExistingIndex() {
        PauseFollowAction.Request unfollowRequest = new PauseFollowAction.Request();
        unfollowRequest.setFollowIndex("non-existing-index");
        expectThrows(IllegalArgumentException.class,
            () -> followerClient().execute(PauseFollowAction.INSTANCE, unfollowRequest).actionGet());
    }

    public void testFollowNonExistentIndex() throws Exception {
        assertAcked(leaderClient().admin().indices().prepareCreate("test-leader").get());
        assertAcked(followerClient().admin().indices().prepareCreate("test-follower").get());
        // Leader index does not exist.
        ResumeFollowAction.Request followRequest1 = resumeFollow("non-existent-leader", "test-follower");
        expectThrows(IndexNotFoundException.class, () -> followerClient().execute(ResumeFollowAction.INSTANCE, followRequest1).actionGet());
        expectThrows(IndexNotFoundException.class,
            () -> followerClient().execute(PutFollowAction.INSTANCE, new PutFollowAction.Request(followRequest1))
                .actionGet());
        // Follower index does not exist.
        ResumeFollowAction.Request followRequest2 = resumeFollow("non-test-leader", "non-existent-follower");
        expectThrows(IndexNotFoundException.class, () -> followerClient().execute(ResumeFollowAction.INSTANCE, followRequest2).actionGet());
        expectThrows(IndexNotFoundException.class,
            () -> followerClient().execute(PutFollowAction.INSTANCE, new PutFollowAction.Request(followRequest2))
                .actionGet());
        // Both indices do not exist.
        ResumeFollowAction.Request followRequest3 = resumeFollow("non-existent-leader", "non-existent-follower");
        expectThrows(IndexNotFoundException.class, () -> followerClient().execute(ResumeFollowAction.INSTANCE, followRequest3).actionGet());
        expectThrows(IndexNotFoundException.class,
            () -> followerClient().execute(PutFollowAction.INSTANCE, new PutFollowAction.Request(followRequest3))
                .actionGet());
    }

    public void testFollowIndexMaxOperationSizeInBytes() throws Exception {
        final String leaderIndexSettings = getIndexSettings(1, between(0, 1),
            singletonMap(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), "true"));
        assertAcked(leaderClient().admin().indices().prepareCreate("index1").setSource(leaderIndexSettings, XContentType.JSON));
        ensureLeaderYellow("index1");

        final int numDocs = between(10, 1024);
        logger.info("Indexing [{}] docs", numDocs);
        for (int i = 0; i < numDocs; i++) {
            final String source = String.format(Locale.ROOT, "{\"f\":%d}", i);
            leaderClient().prepareIndex("index1", "doc", Integer.toString(i)).setSource(source, XContentType.JSON).get();
        }

        PutFollowAction.Request followRequest = follow("index1", "index2");
        followRequest.getFollowRequest().setMaxBatchSize(new ByteSizeValue(1, ByteSizeUnit.BYTES));
        followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();

        final Map<ShardId, Long> firstBatchNumDocsPerShard = new HashMap<>();
        final ShardStats[] firstBatchShardStats =
            leaderClient().admin().indices().prepareStats("index1").get().getIndex("index1").getShards();
        for (final ShardStats shardStats : firstBatchShardStats) {
            if (shardStats.getShardRouting().primary()) {
                long value = shardStats.getStats().getIndexing().getTotal().getIndexCount() - 1;
                firstBatchNumDocsPerShard.put(shardStats.getShardRouting().shardId(), value);
            }
        }

        assertBusy(assertTask(1, firstBatchNumDocsPerShard));
        for (int i = 0; i < numDocs; i++) {
            assertBusy(assertExpectedDocumentRunnable(i));
        }
        unfollowIndex("index2");
        assertMaxSeqNoOfUpdatesIsTransferred(resolveLeaderIndex("index1"), resolveFollowerIndex("index2"), 1);
        assertTotalNumberOfOptimizedIndexing(resolveFollowerIndex("index2"), 1, numDocs);
    }

    public void testDontFollowTheWrongIndex() throws Exception {
        String leaderIndexSettings = getIndexSettings(1, 0,
            Collections.singletonMap(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), "true"));
        assertAcked(leaderClient().admin().indices().prepareCreate("index1").setSource(leaderIndexSettings, XContentType.JSON));
        ensureLeaderGreen("index1");
        assertAcked(leaderClient().admin().indices().prepareCreate("index3").setSource(leaderIndexSettings, XContentType.JSON));
        ensureLeaderGreen("index3");

        PutFollowAction.Request followRequest = follow("index1", "index2");
        followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();

        followRequest = follow("index3", "index4");
        followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();
        unfollowIndex("index2", "index4");

        ResumeFollowAction.Request wrongRequest1 = resumeFollow("index1", "index4");
        Exception e = expectThrows(IllegalArgumentException.class,
            () -> followerClient().execute(ResumeFollowAction.INSTANCE, wrongRequest1).actionGet());
        assertThat(e.getMessage(), containsString("follow index [index4] should reference"));

        ResumeFollowAction.Request wrongRequest2 = resumeFollow("index3", "index2");
        e = expectThrows(IllegalArgumentException.class,
            () -> followerClient().execute(ResumeFollowAction.INSTANCE, wrongRequest2).actionGet());
        assertThat(e.getMessage(), containsString("follow index [index2] should reference"));
    }

    public void testAttemptToChangeCcrFollowingIndexSetting() throws Exception {
        String leaderIndexSettings = getIndexSettings(1, 0, singletonMap(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), "true"));
        assertAcked(leaderClient().admin().indices().prepareCreate("index1").setSource(leaderIndexSettings, XContentType.JSON).get());
        ensureLeaderYellow("index1");
        PutFollowAction.Request followRequest = follow("index1", "index2");
        followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();
        unfollowIndex("index2");
        followerClient().admin().indices().close(new CloseIndexRequest("index2")).actionGet();

        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest("index2");
        updateSettingsRequest.settings(Settings.builder().put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), false).build());
        Exception e = expectThrows(IllegalArgumentException.class,
            () -> followerClient().admin().indices().updateSettings(updateSettingsRequest).actionGet());
        assertThat(e.getMessage(), equalTo("can not update internal setting [index.xpack.ccr.following_index]; " +
            "this setting is managed via a dedicated API"));
    }

    public void testCloseLeaderIndex() throws Exception {
        assertAcked(leaderClient().admin().indices().prepareCreate("index1")
            .setSettings(Settings.builder()
                .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()));

        final PutFollowAction.Request followRequest = follow("index1", "index2");
        followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();

        leaderClient().prepareIndex("index1", "doc", "1").setSource("{}", XContentType.JSON).get();
        assertBusy(() -> assertThat(followerClient().prepareSearch("index2").get().getHits().totalHits, equalTo(1L)));

        leaderClient().admin().indices().close(new CloseIndexRequest("index1")).actionGet();
        assertBusy(() -> {
            StatsResponses response = followerClient().execute(FollowStatsAction.INSTANCE, new StatsRequest()).actionGet();
            assertThat(response.getNodeFailures(), empty());
            assertThat(response.getTaskFailures(), empty());
            assertThat(response.getStatsResponses(), hasSize(1));
            assertThat(response.getStatsResponses().get(0).status().numberOfFailedFetches(), greaterThanOrEqualTo(1L));
            assertThat(response.getStatsResponses().get(0).status().fetchExceptions().size(), equalTo(1));
            ElasticsearchException exception = response.getStatsResponses().get(0).status()
                .fetchExceptions().entrySet().iterator().next().getValue().v2();
            assertThat(exception.getRootCause().getMessage(), equalTo("blocked by: [FORBIDDEN/4/index closed];"));
        });

        leaderClient().admin().indices().open(new OpenIndexRequest("index1")).actionGet();
        leaderClient().prepareIndex("index1", "doc", "2").setSource("{}", XContentType.JSON).get();
        assertBusy(() -> assertThat(followerClient().prepareSearch("index2").get().getHits().totalHits, equalTo(2L)));

        unfollowIndex("index2");
    }

    public void testCloseFollowIndex() throws Exception {
        assertAcked(leaderClient().admin().indices().prepareCreate("index1")
            .setSettings(Settings.builder()
                .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()));

        final PutFollowAction.Request followRequest = follow("index1", "index2");
        followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();

        leaderClient().prepareIndex("index1", "doc", "1").setSource("{}", XContentType.JSON).get();
        assertBusy(() -> assertThat(followerClient().prepareSearch("index2").get().getHits().totalHits, equalTo(1L)));

        followerClient().admin().indices().close(new CloseIndexRequest("index2")).actionGet();
        leaderClient().prepareIndex("index1", "doc", "2").setSource("{}", XContentType.JSON).get();
        assertBusy(() -> {
            StatsResponses response = followerClient().execute(FollowStatsAction.INSTANCE, new StatsRequest()).actionGet();
            assertThat(response.getNodeFailures(), empty());
            assertThat(response.getTaskFailures(), empty());
            assertThat(response.getStatsResponses(), hasSize(1));
            assertThat(response.getStatsResponses().get(0).status().numberOfFailedBulkOperations(), greaterThanOrEqualTo(1L));
        });
        followerClient().admin().indices().open(new OpenIndexRequest("index2")).actionGet();
        assertBusy(() -> assertThat(followerClient().prepareSearch("index2").get().getHits().totalHits, equalTo(2L)));

        unfollowIndex("index2");
    }

    public void testDeleteLeaderIndex() throws Exception {
        assertAcked(leaderClient().admin().indices().prepareCreate("index1")
            .setSettings(Settings.builder()
                .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()));

        final PutFollowAction.Request followRequest = follow("index1", "index2");
        followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();

        leaderClient().prepareIndex("index1", "doc", "1").setSource("{}", XContentType.JSON).get();
        assertBusy(() -> assertThat(followerClient().prepareSearch("index2").get().getHits().totalHits, equalTo(1L)));

        leaderClient().admin().indices().delete(new DeleteIndexRequest("index1")).actionGet();
        ensureNoCcrTasks();
    }

    public void testDeleteFollowerIndex() throws Exception {
        assertAcked(leaderClient().admin().indices().prepareCreate("index1")
            .setSettings(Settings.builder()
                .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()));

        final PutFollowAction.Request followRequest = follow("index1", "index2");
        followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();

        leaderClient().prepareIndex("index1", "doc", "1").setSource("{}", XContentType.JSON).get();
        assertBusy(() -> assertThat(followerClient().prepareSearch("index2").get().getHits().totalHits, equalTo(1L)));

        followerClient().admin().indices().delete(new DeleteIndexRequest("index2")).actionGet();
        leaderClient().prepareIndex("index1", "doc", "2").setSource("{}", XContentType.JSON).get();
        ensureNoCcrTasks();
    }

    public void testUnfollowIndex() throws Exception {
        String leaderIndexSettings = getIndexSettings(1, 0, singletonMap(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), "true"));
        assertAcked(leaderClient().admin().indices().prepareCreate("index1").setSource(leaderIndexSettings, XContentType.JSON).get());
        PutFollowAction.Request followRequest = follow("index1", "index2");
        followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();
        leaderClient().prepareIndex("index1", "doc").setSource("{}", XContentType.JSON).get();
        assertBusy(() -> {
            assertThat(followerClient().prepareSearch("index2").get().getHits().getTotalHits(), equalTo(1L));
        });

        // Indexing directly into index2 would fail now, because index2 is a follow index.
        // We can't test this here because an assertion trips before an actual error is thrown and then index call hangs.

        // Turn follow index into a regular index by: pausing shard follow, close index, unfollow index and then open index:
        unfollowIndex("index2");
        followerClient().admin().indices().close(new CloseIndexRequest("index2")).actionGet();
        assertAcked(followerClient().execute(UnfollowAction.INSTANCE, new UnfollowAction.Request("index2")).actionGet());
        followerClient().admin().indices().open(new OpenIndexRequest("index2")).actionGet();
        ensureFollowerGreen("index2");

        // Indexing succeeds now, because index2 is no longer a follow index:
        followerClient().prepareIndex("index2", "doc").setSource("{}", XContentType.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        assertThat(followerClient().prepareSearch("index2").get().getHits().getTotalHits(), equalTo(2L));
    }

    private CheckedRunnable<Exception> assertTask(final int numberOfPrimaryShards, final Map<ShardId, Long> numDocsPerShard) {
        return () -> {
            final ClusterState clusterState = followerClient().admin().cluster().prepareState().get().getState();
            final PersistentTasksCustomMetaData taskMetadata = clusterState.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);

            ListTasksRequest listTasksRequest = new ListTasksRequest();
            listTasksRequest.setDetailed(true);
            listTasksRequest.setActions(ShardFollowTask.NAME + "[c]");
            ListTasksResponse listTasksResponse = followerClient().admin().cluster().listTasks(listTasksRequest).actionGet();
            assertThat(listTasksResponse.getNodeFailures().size(), equalTo(0));
            assertThat(listTasksResponse.getTaskFailures().size(), equalTo(0));

            List<TaskInfo> taskInfos = listTasksResponse.getTasks();
            assertThat(taskInfos.size(), equalTo(numberOfPrimaryShards));
            Collection<PersistentTasksCustomMetaData.PersistentTask<?>> shardFollowTasks =
                taskMetadata.findTasks(ShardFollowTask.NAME, Objects::nonNull);
            for (PersistentTasksCustomMetaData.PersistentTask<?> shardFollowTask : shardFollowTasks) {
                final ShardFollowTask shardFollowTaskParams = (ShardFollowTask) shardFollowTask.getParams();
                TaskInfo taskInfo = null;
                String expectedId = "id=" + shardFollowTask.getId();
                for (TaskInfo info : taskInfos) {
                    if (expectedId.equals(info.getDescription())) {
                        taskInfo = info;
                        break;
                    }
                }
                assertThat(taskInfo, notNullValue());
                ShardFollowNodeTaskStatus status = (ShardFollowNodeTaskStatus) taskInfo.getStatus();
                assertThat(status, notNullValue());
                assertThat("incorrect global checkpoint " + shardFollowTaskParams,
                    status.followerGlobalCheckpoint(),
                    equalTo(numDocsPerShard.get(shardFollowTaskParams.getLeaderShardId())));
            }
        };
    }

    private void unfollowIndex(String... indices) throws Exception {
        for (String index : indices) {
            final PauseFollowAction.Request unfollowRequest = new PauseFollowAction.Request();
            unfollowRequest.setFollowIndex(index);
            followerClient().execute(PauseFollowAction.INSTANCE, unfollowRequest).get();
        }
        ensureNoCcrTasks();
    }

    private void ensureNoCcrTasks() throws Exception {
        assertBusy(() -> {
            final ClusterState clusterState = followerClient().admin().cluster().prepareState().get().getState();
            final PersistentTasksCustomMetaData tasks = clusterState.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
            assertThat(tasks.tasks(), empty());

            ListTasksRequest listTasksRequest = new ListTasksRequest();
            listTasksRequest.setDetailed(true);
            ListTasksResponse listTasksResponse = followerClient().admin().cluster().listTasks(listTasksRequest).get();
            int numNodeTasks = 0;
            for (TaskInfo taskInfo : listTasksResponse.getTasks()) {
                if (taskInfo.getAction().startsWith(ListTasksAction.NAME) == false) {
                    numNodeTasks++;
                }
            }
            assertThat(numNodeTasks, equalTo(0));
        }, 30, TimeUnit.SECONDS);
    }

    private CheckedRunnable<Exception> assertExpectedDocumentRunnable(final int value) {
        return () -> {
            final GetResponse getResponse = followerClient().prepareGet("index2", "doc", Integer.toString(value)).get();
            assertTrue("Doc with id [" + value + "] is missing", getResponse.isExists());
            assertTrue((getResponse.getSource().containsKey("f")));
            assertThat(getResponse.getSource().get("f"), equalTo(value));
        };
    }

    private String getIndexSettings(final int numberOfShards, final int numberOfReplicas,
                                    final Map<String, String> additionalIndexSettings) throws IOException {
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
                builder.startObject("mappings");
                {
                    builder.startObject("doc");
                    {
                        builder.startObject("properties");
                        {
                            builder.startObject("f");
                            {
                                builder.field("type", "integer");
                            }
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
            settings = BytesReference.bytes(builder).utf8ToString();
        }
        return settings;
    }

    private String getIndexSettingsWithNestedMapping(final int numberOfShards, final int numberOfReplicas,
                                                     final Map<String, String> additionalIndexSettings) throws IOException {
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
                builder.startObject("mappings");
                {
                    builder.startObject("doc");
                    {
                        builder.startObject("properties");
                        {
                            builder.startObject("objects");
                            {
                                builder.field("type", "nested");
                                builder.startObject("properties");
                                {
                                    builder.startObject("field");
                                    {
                                        builder.field("type", "long");
                                    }
                                    builder.endObject();
                                }
                                builder.endObject();
                            }
                            builder.endObject();
                            builder.startObject("field");
                            {
                                builder.field("type", "keyword");
                            }
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
            settings = BytesReference.bytes(builder).utf8ToString();
        }
        return settings;
    }

    private void atLeastDocsIndexed(Client client, String index, long numDocsReplicated) throws InterruptedException {
        logger.info("waiting for at least [{}] documents to be indexed into index [{}]", numDocsReplicated, index);
        awaitBusy(() -> {
            refresh(client, index);
            SearchRequest request = new SearchRequest(index);
            request.source(new SearchSourceBuilder().size(0));
            SearchResponse response = client.search(request).actionGet();
            return response.getHits().getTotalHits() >= numDocsReplicated;
        }, 60, TimeUnit.SECONDS);
    }

    private void assertSameDocCount(String leaderIndex, String followerIndex) throws Exception {
        refresh(leaderClient(), leaderIndex);
        SearchRequest request1 = new SearchRequest(leaderIndex);
        request1.source(new SearchSourceBuilder().size(0));
        SearchResponse response1 = leaderClient().search(request1).actionGet();
        assertBusy(() -> {
            refresh(followerClient(), followerIndex);
            SearchRequest request2 = new SearchRequest(followerIndex);
            request2.source(new SearchSourceBuilder().size(0));
            SearchResponse response2 = followerClient().search(request2).actionGet();
            assertThat(response2.getHits().getTotalHits(), equalTo(response1.getHits().getTotalHits()));
        }, 60, TimeUnit.SECONDS);
    }

    private void assertMaxSeqNoOfUpdatesIsTransferred(Index leaderIndex, Index followerIndex, int numberOfShards) throws Exception {
        assertBusy(() -> {
            long[] msuOnLeader = new long[numberOfShards];
            for (int i = 0; i < msuOnLeader.length; i++) {
                msuOnLeader[i] = SequenceNumbers.UNASSIGNED_SEQ_NO;
            }
            Set<String> leaderNodes = getLeaderCluster().nodesInclude(leaderIndex.getName());
            for (String leaderNode : leaderNodes) {
                IndicesService indicesService = getLeaderCluster().getInstance(IndicesService.class, leaderNode);
                for (int i = 0; i < numberOfShards; i++) {
                    IndexShard shard = indicesService.getShardOrNull(new ShardId(leaderIndex, i));
                    if (shard != null) {
                        try {
                            msuOnLeader[i] = SequenceNumbers.max(msuOnLeader[i], shard.getMaxSeqNoOfUpdatesOrDeletes());
                        } catch (AlreadyClosedException ignored) {
                            return;
                        }
                    }
                }
            }

            Set<String> followerNodes = getFollowerCluster().nodesInclude(followerIndex.getName());
            for (String followerNode : followerNodes) {
                IndicesService indicesService = getFollowerCluster().getInstance(IndicesService.class, followerNode);
                for (int i = 0; i < numberOfShards; i++) {
                    IndexShard shard = indicesService.getShardOrNull(new ShardId(leaderIndex, i));
                    if (shard != null) {
                        try {
                            assertThat(shard.getMaxSeqNoOfUpdatesOrDeletes(), equalTo(msuOnLeader[i]));
                        } catch (AlreadyClosedException ignored) {

                        }
                    }
                }
            }
        });
    }

    private void assertTotalNumberOfOptimizedIndexing(Index followerIndex, int numberOfShards, long expectedTotal) throws Exception {
        assertBusy(() -> {
            long[] numOfOptimizedOps = new long[numberOfShards];
            for (int shardId = 0; shardId < numberOfShards; shardId++) {
                for (String node : getFollowerCluster().nodesInclude(followerIndex.getName())) {
                    IndicesService indicesService = getFollowerCluster().getInstance(IndicesService.class, node);
                    IndexShard shard = indicesService.getShardOrNull(new ShardId(followerIndex, shardId));
                    if (shard != null && shard.routingEntry().primary()) {
                        try {
                            FollowingEngine engine = ((FollowingEngine) IndexShardTestCase.getEngine(shard));
                            numOfOptimizedOps[shardId] = engine.getNumberOfOptimizedIndexing();
                        } catch (AlreadyClosedException e) {
                            throw new AssertionError(e); // causes assertBusy to retry
                        }
                    }
                }
            }
            assertThat(Arrays.stream(numOfOptimizedOps).sum(), equalTo(expectedTotal));
        });
    }

    public static PutFollowAction.Request follow(String leaderIndex, String followerIndex) {
        return new PutFollowAction.Request(resumeFollow(leaderIndex, followerIndex));
    }

    public static ResumeFollowAction.Request resumeFollow(String leaderIndex, String followerIndex) {
        ResumeFollowAction.Request request = new ResumeFollowAction.Request();
        request.setLeaderIndex("leader_cluster:" + leaderIndex);
        request.setFollowerIndex(followerIndex);
        request.setMaxRetryDelay(TimeValue.timeValueMillis(10));
        request.setPollTimeout(TimeValue.timeValueMillis(10));
        return request;
    }
}
