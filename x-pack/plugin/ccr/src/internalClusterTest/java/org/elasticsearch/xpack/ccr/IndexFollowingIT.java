/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.cluster.health.ClusterShardHealth;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.index.seqno.RetentionLeaseActions;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.snapshots.SnapshotRestoreException;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.BackgroundIndexer;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.Compression;
import org.elasticsearch.transport.NoSuchRemoteClusterException;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.SniffConnectionStrategy;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.CcrIntegTestCase;
import org.elasticsearch.xpack.core.ccr.action.ShardFollowTask;
import org.elasticsearch.xpack.core.ccr.ShardFollowNodeTaskStatus;
import org.elasticsearch.xpack.core.ccr.action.FollowStatsAction;
import org.elasticsearch.xpack.core.ccr.action.FollowStatsAction.StatsRequest;
import org.elasticsearch.xpack.core.ccr.action.FollowStatsAction.StatsResponses;
import org.elasticsearch.xpack.core.ccr.action.PauseFollowAction;
import org.elasticsearch.xpack.core.ccr.action.PutAutoFollowPatternAction;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.ccr.CcrRetentionLeases.retentionLeaseId;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class IndexFollowingIT extends CcrIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(super.nodePlugins().stream(), Stream.of(PrivateSettingPlugin.class)).collect(Collectors.toList());
    }

    public void testFollowIndex() throws Exception {
        final int numberOfPrimaryShards = randomIntBetween(1, 3);
        int numberOfReplicas = between(0, 1);

        followerClient().admin().cluster().prepareUpdateSettings().setMasterNodeTimeout(TimeValue.MAX_VALUE)
            .setTransientSettings(Settings.builder().put(CcrSettings.RECOVERY_CHUNK_SIZE.getKey(),
                new ByteSizeValue(randomIntBetween(1, 1000), ByteSizeUnit.KB)))
            .get();

        final String leaderIndexSettings = getIndexSettings(numberOfPrimaryShards, numberOfReplicas);
        assertAcked(leaderClient().admin().indices().prepareCreate("index1").setSource(leaderIndexSettings, XContentType.JSON));
        ensureLeaderYellow("index1");

        final int firstBatchNumDocs;
        // Sometimes we want to index a lot of documents to ensure that the recovery works with larger files
        if (rarely()) {
            firstBatchNumDocs = randomIntBetween(1800, 10000);
        } else {
            firstBatchNumDocs = randomIntBetween(10, 64);
        }

        logger.info("Indexing [{}] docs as first batch", firstBatchNumDocs);
        try (BackgroundIndexer indexer = new BackgroundIndexer("index1", "_doc", leaderClient(), firstBatchNumDocs,
            randomIntBetween(1, 5))) {
            waitForDocs(randomInt(firstBatchNumDocs), indexer);
            leaderClient().admin().indices().prepareFlush("index1").setWaitIfOngoing(true).get();
            waitForDocs(firstBatchNumDocs, indexer);
            indexer.assertNoFailures();

            logger.info("Executing put follow");
            boolean waitOnAll = randomBoolean();

            final PutFollowAction.Request followRequest;
            if (waitOnAll) {
                followRequest = putFollow("index1", "index2", ActiveShardCount.ALL);
            } else {
                followRequest = putFollow("index1", "index2", ActiveShardCount.ONE);
            }
            PutFollowAction.Response response = followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();
            assertTrue(response.isFollowIndexCreated());
            assertTrue(response.isFollowIndexShardsAcked());
            assertTrue(response.isIndexFollowingStarted());

            ClusterHealthRequest healthRequest = Requests.clusterHealthRequest("index2").waitForNoRelocatingShards(true);
            ClusterIndexHealth indexHealth = followerClient().admin().cluster().health(healthRequest).get().getIndices().get("index2");
            for (ClusterShardHealth shardHealth : indexHealth.getShards().values()) {
                if (waitOnAll) {
                    assertTrue(shardHealth.isPrimaryActive());
                    assertEquals(1 + numberOfReplicas, shardHealth.getActiveShards());
                } else {
                    assertTrue(shardHealth.isPrimaryActive());
                }
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

            for (String docId : indexer.getIds()) {
                assertBusy(() -> {
                    final GetResponse getResponse = followerClient().prepareGet("index2", docId).get();
                    assertTrue("Doc with id [" + docId + "] is missing", getResponse.isExists());
                });
            }

            pauseFollow("index2");
            followerClient().execute(ResumeFollowAction.INSTANCE, resumeFollow("index2")).get();
            final int secondBatchNumDocs = randomIntBetween(2, 64);
            logger.info("Indexing [{}] docs as second batch", secondBatchNumDocs);
            indexer.continueIndexing(secondBatchNumDocs);

            waitForDocs(firstBatchNumDocs + secondBatchNumDocs, indexer);

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

            for (String docId : indexer.getIds()) {
                assertBusy(() -> {
                    final GetResponse getResponse = followerClient().prepareGet("index2", docId).get();
                    assertTrue("Doc with id [" + docId + "] is missing", getResponse.isExists());
                });
            }

            pauseFollow("index2");
            assertMaxSeqNoOfUpdatesIsTransferred(resolveLeaderIndex("index1"), resolveFollowerIndex("index2"), numberOfPrimaryShards);
        }
    }

    public void testFollowIndexWithConcurrentMappingChanges() throws Exception {
        final int numberOfPrimaryShards = randomIntBetween(1, 3);
        final String leaderIndexSettings = getIndexSettings(numberOfPrimaryShards, between(0, 1));
        assertAcked(leaderClient().admin().indices().prepareCreate("index1").setSource(leaderIndexSettings, XContentType.JSON));
        ensureLeaderYellow("index1");

        final int firstBatchNumDocs = randomIntBetween(2, 64);
        logger.info("Indexing [{}] docs as first batch", firstBatchNumDocs);
        for (int i = 0; i < firstBatchNumDocs; i++) {
            leaderClient().prepareIndex("index1").setId(Integer.toString(i)).setSource("f", i).get();
        }

        AtomicBoolean isRunning = new AtomicBoolean(true);

        // Concurrently index new docs with mapping changes
        int numFields = between(10, 20);
        Thread thread = new Thread(() -> {
            int numDocs = between(10, 200);
            for (int i = 0; i < numDocs; i++) {
                if (isRunning.get() == false) {
                    break;
                }
                final String field = "f-" + between(1, numFields);
                leaderClient().prepareIndex("index1").setSource(field, between(0, 1000)).get();
                if (rarely()) {
                    leaderClient().admin().indices().prepareFlush("index1").setWaitIfOngoing(false).setForce(false).get();
                }
            }
        });
        thread.start();

        final PutFollowAction.Request followRequest = putFollow("index1", "index2", ActiveShardCount.NONE);
        followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();

        ensureFollowerGreen("index2");

        for (int i = 0; i < firstBatchNumDocs; i++) {
            assertBusy(assertExpectedDocumentRunnable(i), 1, TimeUnit.MINUTES);
        }

        final int secondBatchNumDocs = randomIntBetween(2, 64);
        logger.info("Indexing [{}] docs as second batch", secondBatchNumDocs);
        for (int i = firstBatchNumDocs; i < firstBatchNumDocs + secondBatchNumDocs; i++) {
            leaderClient().prepareIndex("index1").setId(Integer.toString(i)).setSource("f", i).get();
        }
        for (int i = 0; i < firstBatchNumDocs + secondBatchNumDocs; i++) {
            assertBusy(assertExpectedDocumentRunnable(i), 1, TimeUnit.MINUTES);
        }
        isRunning.set(false);
        thread.join();
        assertIndexFullyReplicatedToFollower("index1", "index2");
    }

    public void testFollowIndexWithoutWaitForComplete() throws Exception {
        final int numberOfPrimaryShards = randomIntBetween(1, 3);
        final String leaderIndexSettings = getIndexSettings(numberOfPrimaryShards, between(0, 1));
        assertAcked(leaderClient().admin().indices().prepareCreate("index1").setSource(leaderIndexSettings, XContentType.JSON));
        ensureLeaderYellow("index1");

        final int firstBatchNumDocs = randomIntBetween(2, 64);
        logger.info("Indexing [{}] docs as first batch", firstBatchNumDocs);
        for (int i = 0; i < firstBatchNumDocs; i++) {
            final String source = String.format(Locale.ROOT, "{\"f\":%d}", i);
            leaderClient().prepareIndex("index1").setId(Integer.toString(i)).setSource(source, XContentType.JSON).get();
        }

        final PutFollowAction.Request followRequest = putFollow("index1", "index2", ActiveShardCount.NONE);
        PutFollowAction.Response response = followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();

        assertTrue(response.isFollowIndexCreated());
        assertFalse(response.isFollowIndexShardsAcked());
        assertFalse(response.isIndexFollowingStarted());

        // Check that the index exists, would throw index not found exception if the index is missing
        followerClient().admin().indices().prepareGetIndex().addIndices("index2").get();
        ensureFollowerGreen(true, "index2");

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
        pauseFollow("index2");
    }

    public void testSyncMappings() throws Exception {
        final String leaderIndexSettings = getIndexSettings(2, between(0, 1));
        assertAcked(leaderClient().admin().indices().prepareCreate("index1").setSource(leaderIndexSettings, XContentType.JSON));
        ensureLeaderYellow("index1");

        final PutFollowAction.Request followRequest = putFollow("index1", "index2");
        followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();

        final long firstBatchNumDocs = randomIntBetween(2, 64);
        for (long i = 0; i < firstBatchNumDocs; i++) {
            final String source = String.format(Locale.ROOT, "{\"f\":%d}", i);
            leaderClient().prepareIndex("index1").setId(Long.toString(i)).setSource(source, XContentType.JSON).get();
        }

        assertBusy(() -> assertThat(followerClient().prepareSearch("index2").get()
            .getHits().getTotalHits().value, equalTo(firstBatchNumDocs)));
        MappingMetadata mappingMetadata = followerClient().admin().indices().prepareGetMappings("index2").get().getMappings()
            .get("index2");
        assertThat(XContentMapValues.extractValue("properties.f.type", mappingMetadata.sourceAsMap()), equalTo("integer"));
        assertThat(XContentMapValues.extractValue("properties.k", mappingMetadata.sourceAsMap()), nullValue());

        final int secondBatchNumDocs = randomIntBetween(2, 64);
        for (long i = firstBatchNumDocs; i < firstBatchNumDocs + secondBatchNumDocs; i++) {
            final String source = String.format(Locale.ROOT, "{\"k\":%d}", i);
            leaderClient().prepareIndex("index1").setId(Long.toString(i)).setSource(source, XContentType.JSON).get();
        }

        assertBusy(() -> assertThat(followerClient().prepareSearch("index2").get().getHits().getTotalHits().value,
            equalTo(firstBatchNumDocs + secondBatchNumDocs)));
        mappingMetadata = followerClient().admin().indices().prepareGetMappings("index2").get().getMappings()
            .get("index2");
        assertThat(XContentMapValues.extractValue("properties.f.type", mappingMetadata.sourceAsMap()), equalTo("integer"));
        assertThat(XContentMapValues.extractValue("properties.k.type", mappingMetadata.sourceAsMap()), equalTo("long"));
        pauseFollow("index2");
        assertMaxSeqNoOfUpdatesIsTransferred(resolveLeaderIndex("index1"), resolveFollowerIndex("index2"), 2);
    }

    public void testNoMappingDefined() throws Exception {
        assertAcked(leaderClient().admin().indices().prepareCreate("index1")
            .setSettings(Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()));
        ensureLeaderGreen("index1");

        final PutFollowAction.Request followRequest = putFollow("index1", "index2");
        followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();

        leaderClient().prepareIndex("index1").setId("1").setSource("{\"f\":1}", XContentType.JSON).get();
        assertBusy(() -> assertThat(followerClient().prepareSearch("index2").get().getHits().getTotalHits().value, equalTo(1L)));
        pauseFollow("index2");

        MappingMetadata mappingMetadata = followerClient().admin().indices().prepareGetMappings("index2").get().getMappings()
            .get("index2");
        assertThat(XContentMapValues.extractValue("properties.f.type", mappingMetadata.sourceAsMap()), equalTo("long"));
        assertThat(XContentMapValues.extractValue("properties.k", mappingMetadata.sourceAsMap()), nullValue());
    }

    public void testDoNotAllowPutMappingToFollower() throws Exception {
        removeMasterNodeRequestsValidatorOnFollowerCluster();
        final String leaderIndexSettings = getIndexSettings(between(1, 2), between(0, 1));
        assertAcked(leaderClient().admin().indices().prepareCreate("index-1").setSource(leaderIndexSettings, XContentType.JSON));
        followerClient().execute(PutFollowAction.INSTANCE, putFollow("index-1", "index-2")).get();
        PutMappingRequest putMappingRequest = new PutMappingRequest("index-2").source("new_field", "type=keyword");
        ElasticsearchStatusException forbiddenException = expectThrows(ElasticsearchStatusException.class,
            () -> followerClient().admin().indices().putMapping(putMappingRequest).actionGet());
        assertThat(forbiddenException.getMessage(),
            equalTo("can't put mapping to the following indices [index-2]; " +
                "the mapping of the following indices are self-replicated from its leader indices"));
        assertThat(forbiddenException.status(), equalTo(RestStatus.FORBIDDEN));
        pauseFollow("index-2");
        followerClient().admin().indices().close(new CloseIndexRequest("index-2")).actionGet();
        assertAcked(followerClient().execute(UnfollowAction.INSTANCE, new UnfollowAction.Request("index-2")).actionGet());
        followerClient().admin().indices().open(new OpenIndexRequest("index-2")).actionGet();
        assertAcked(followerClient().admin().indices().putMapping(putMappingRequest).actionGet());
    }

    public void testDoNotAllowAddAliasToFollower() throws Exception {
        final String leaderIndexSettings =
                getIndexSettings(between(1, 2), between(0, 1));
        assertAcked(leaderClient().admin().indices().prepareCreate("leader").setSource(leaderIndexSettings, XContentType.JSON));
        followerClient().execute(PutFollowAction.INSTANCE, putFollow("leader", "follower")).get();
        final IndicesAliasesRequest request = new IndicesAliasesRequest().masterNodeTimeout(TimeValue.MAX_VALUE)
                .addAliasAction(IndicesAliasesRequest.AliasActions.add().index("follower").alias("follower_alias"));
        final ElasticsearchStatusException e =
                expectThrows(ElasticsearchStatusException.class, () -> followerClient().admin().indices().aliases(request).actionGet());
        assertThat(
                e,
                hasToString(containsString("can't modify aliases on indices [follower]; "
                        + "aliases of following indices are self-replicated from their leader indices")));
        assertThat(e.status(), equalTo(RestStatus.FORBIDDEN));
    }

    public void testAddAliasAfterUnfollow() throws Exception {
        final String leaderIndexSettings =
                getIndexSettings(between(1, 2), between(0, 1));
        assertAcked(leaderClient().admin().indices().prepareCreate("leader").setSource(leaderIndexSettings, XContentType.JSON));
        followerClient().execute(PutFollowAction.INSTANCE, putFollow("leader", "follower")).get();
        pauseFollow("follower");
        followerClient().admin().indices().close(new CloseIndexRequest("follower").masterNodeTimeout(TimeValue.MAX_VALUE)).actionGet();
        assertAcked(followerClient().execute(UnfollowAction.INSTANCE, new UnfollowAction.Request("follower")).actionGet());
        followerClient().admin().indices().open(new OpenIndexRequest("follower").masterNodeTimeout(TimeValue.MAX_VALUE)).actionGet();
        final IndicesAliasesRequest request = new IndicesAliasesRequest().masterNodeTimeout(TimeValue.MAX_VALUE)
                .addAliasAction(IndicesAliasesRequest.AliasActions.add().index("follower").alias("follower_alias"));
        assertAcked(followerClient().admin().indices().aliases(request).actionGet());
        final GetAliasesResponse response =
                followerClient().admin().indices().getAliases(new GetAliasesRequest("follower_alias")).actionGet();
        assertThat(response.getAliases().keys().size(), equalTo(1));
        assertThat(response.getAliases().keys().iterator().next().value, equalTo("follower"));
        final List<AliasMetadata> aliasMetadata = response.getAliases().get("follower");
        assertThat(aliasMetadata, hasSize(1));
        assertThat(aliasMetadata.get(0).alias(), equalTo("follower_alias"));
    }

    public void testFollowIndex_backlog() throws Exception {
        int numberOfShards = between(1, 5);
        String leaderIndexSettings = getIndexSettings(numberOfShards, between(0, 1));
        assertAcked(leaderClient().admin().indices().prepareCreate("index1").setSource(leaderIndexSettings, XContentType.JSON));
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {}

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {}

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {}
        };
        int bulkSize = between(1, 20);
        BulkProcessor bulkProcessor = BulkProcessor.builder(leaderClient()::bulk, listener, "IndexFollowingIT")
            .setBulkActions(bulkSize)
            .setConcurrentRequests(4)
            .build();
        AtomicBoolean run = new AtomicBoolean(true);
        Semaphore availableDocs = new Semaphore(0);
        Thread thread = new Thread(() -> {
            int counter = 0;
            while (run.get()) {
                try {
                    if (availableDocs.tryAcquire(10, TimeUnit.MILLISECONDS) == false) {
                        continue;
                    }
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
                final String source = String.format(Locale.ROOT, "{\"f\":%d}", counter++);
                IndexRequest indexRequest = new IndexRequest("index1")
                    .source(source, XContentType.JSON)
                    .timeout(TimeValue.timeValueSeconds(1));
                bulkProcessor.add(indexRequest);
            }
        });
        thread.start();

        // Waiting for some document being index before following the index:
        int maxOpsPerRead = randomIntBetween(10, 100);
        int numDocsIndexed = Math.min(between(20, 300), between(maxOpsPerRead, maxOpsPerRead * 10));
        availableDocs.release(numDocsIndexed / 2 + bulkSize);
        atLeastDocsIndexed(leaderClient(), "index1", numDocsIndexed / 3);

        PutFollowAction.Request followRequest = putFollow("index1", "index2");
        followRequest.getParameters().setMaxReadRequestOperationCount(maxOpsPerRead);
        followRequest.getParameters().setMaxOutstandingReadRequests(randomIntBetween(1, 10));
        followRequest.getParameters().setMaxOutstandingWriteRequests(randomIntBetween(1, 10));
        followRequest.getParameters().setMaxWriteBufferCount(randomIntBetween(1024, 10240));
        followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();
        availableDocs.release(numDocsIndexed * 2  + bulkSize);
        atLeastDocsIndexed(leaderClient(), "index1", numDocsIndexed);
        run.set(false);
        thread.join();
        assertThat(bulkProcessor.awaitClose(1L, TimeUnit.MINUTES), is(true));

        assertIndexFullyReplicatedToFollower("index1", "index2");
        pauseFollow("index2");
        leaderClient().admin().indices().prepareRefresh("index1").get();
        assertMaxSeqNoOfUpdatesIsTransferred(resolveLeaderIndex("index1"), resolveFollowerIndex("index2"), numberOfShards);
    }

    public void testFollowIndexWithNestedField() throws Exception {
        final String leaderIndexSettings = getIndexSettingsWithNestedMapping(1, between(0, 1), Collections.emptyMap());
        assertAcked(leaderClient().admin().indices().prepareCreate("index1").setSource(leaderIndexSettings, XContentType.JSON));
        ensureLeaderGreen("index1");

        final PutFollowAction.Request followRequest = putFollow("index1", "index2");
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
                leaderClient().prepareIndex("index1").setId(Integer.toString(i)).setSource(builder).get();
            }
        }

        for (int i = 0; i < numDocs; i++) {
            int value = i;
            assertBusy(() -> {
                final GetResponse getResponse = followerClient().prepareGet("index2", Integer.toString(value)).get();
                assertTrue(getResponse.isExists());
                assertTrue((getResponse.getSource().containsKey("field")));
                assertThat(XContentMapValues.extractValue("objects.field", getResponse.getSource()),
                    equalTo(Collections.singletonList(value)));
            });
        }
        pauseFollow("index2");
        assertMaxSeqNoOfUpdatesIsTransferred(resolveLeaderIndex("index1"), resolveFollowerIndex("index2"), 1);
    }

    public void testUnfollowNonExistingIndex() {
        PauseFollowAction.Request unfollowRequest = new PauseFollowAction.Request("non-existing-index");
        expectThrows(IndexNotFoundException.class,
            () -> followerClient().execute(PauseFollowAction.INSTANCE, unfollowRequest).actionGet());
    }

    public void testFollowNonExistentIndex() throws Exception {
        String indexSettings = getIndexSettings(1, 0);
        assertAcked(leaderClient().admin().indices().prepareCreate("test-leader").setSource(indexSettings, XContentType.JSON).get());
        assertAcked(followerClient().admin().indices().prepareCreate("test-follower")
            .setSource(indexSettings, XContentType.JSON)
            .setMasterNodeTimeout(TimeValue.MAX_VALUE)
            .get());
        ensureLeaderGreen("test-leader");
        ensureFollowerGreen("test-follower");
        // Leader index does not exist.
        expectThrows(IndexNotFoundException.class,
            () -> followerClient().execute(PutFollowAction.INSTANCE, putFollow("non-existent-leader", "test-follower"))
                .actionGet());
        // Follower index does not exist.
        ResumeFollowAction.Request followRequest1 = resumeFollow("non-existent-follower");
        expectThrows(IndexNotFoundException.class, () -> followerClient().execute(ResumeFollowAction.INSTANCE, followRequest1).actionGet());
        // Both indices do not exist.
        ResumeFollowAction.Request followRequest2 = resumeFollow("non-existent-follower");
        expectThrows(IndexNotFoundException.class, () -> followerClient().execute(ResumeFollowAction.INSTANCE, followRequest2).actionGet());
        expectThrows(IndexNotFoundException.class,
            () -> followerClient().execute(PutFollowAction.INSTANCE, putFollow("non-existing-leader", "non-existing-follower"))
                .actionGet());
    }

    public void testFollowIndexMaxOperationSizeInBytes() throws Exception {
        final String leaderIndexSettings = getIndexSettings(1, between(0, 1));
        assertAcked(leaderClient().admin().indices().prepareCreate("index1").setSource(leaderIndexSettings, XContentType.JSON));
        ensureLeaderYellow("index1");

        final int numDocs = between(10, 1024);
        logger.info("Indexing [{}] docs", numDocs);
        for (int i = 0; i < numDocs; i++) {
            final String source = String.format(Locale.ROOT, "{\"f\":%d}", i);
            leaderClient().prepareIndex("index1").setId(Integer.toString(i)).setSource(source, XContentType.JSON).get();
        }

        PutFollowAction.Request followRequest = putFollow("index1", "index2");
        followRequest.getParameters().setMaxReadRequestSize(new ByteSizeValue(randomIntBetween(1, 1024), ByteSizeUnit.BYTES));
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

        assertBusy(assertTask(1, firstBatchNumDocsPerShard), 60, TimeUnit.SECONDS);
        for (int i = 0; i < numDocs; i++) {
            assertBusy(assertExpectedDocumentRunnable(i));
        }
        pauseFollow("index2");
        assertMaxSeqNoOfUpdatesIsTransferred(resolveLeaderIndex("index1"), resolveFollowerIndex("index2"), 1);
    }

    public void testAttemptToChangeCcrFollowingIndexSetting() throws Exception {
        String leaderIndexSettings = getIndexSettings(1, 0);
        assertAcked(leaderClient().admin().indices().prepareCreate("index1").setSource(leaderIndexSettings, XContentType.JSON).get());
        ensureLeaderYellow("index1");
        PutFollowAction.Request followRequest = putFollow("index1", "index2");
        followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();
        pauseFollow("index2");
        followerClient().admin().indices().close(new CloseIndexRequest("index2").masterNodeTimeout(TimeValue.MAX_VALUE)).actionGet();

        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest("index2").masterNodeTimeout(TimeValue.MAX_VALUE);
        updateSettingsRequest.settings(Settings.builder().put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), false).build());
        Exception e = expectThrows(IllegalArgumentException.class,
            () -> followerClient().admin().indices().updateSettings(updateSettingsRequest).actionGet());
        assertThat(e.getMessage(), equalTo("can not update internal setting [index.xpack.ccr.following_index]; " +
            "this setting is managed via a dedicated API"));
    }

    public void testCloseLeaderIndex() throws Exception {
        assertAcked(leaderClient().admin().indices().prepareCreate("index1")
            .setSettings(Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()));

        final PutFollowAction.Request followRequest = putFollow("index1", "index2");
        followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();

        leaderClient().prepareIndex("index1").setId("1").setSource("{}", XContentType.JSON).get();
        assertBusy(() -> assertThat(followerClient().prepareSearch("index2").get().getHits().getTotalHits().value, equalTo(1L)));

        leaderClient().admin().indices().close(new CloseIndexRequest("index1")).actionGet();
        assertBusy(() -> {
            StatsResponses response = followerClient().execute(FollowStatsAction.INSTANCE, new StatsRequest()).actionGet();
            assertThat(response.getNodeFailures(), empty());
            assertThat(response.getTaskFailures(), empty());
            assertThat(response.getStatsResponses(), hasSize(1));
            assertThat(response.getStatsResponses().get(0).status().failedReadRequests(), greaterThanOrEqualTo(1L));
            assertThat(response.getStatsResponses().get(0).status().readExceptions().size(), equalTo(1));
            ElasticsearchException exception = response.getStatsResponses().get(0).status()
                .readExceptions().entrySet().iterator().next().getValue().v2();
            assertThat(exception.getRootCause().getMessage(), equalTo("index [index1] blocked by: [FORBIDDEN/4/index closed];"));
        });

        leaderClient().admin().indices().open(new OpenIndexRequest("index1")).actionGet();
        leaderClient().prepareIndex("index1").setId("2").setSource("{}", XContentType.JSON).get();
        assertBusy(() -> assertThat(followerClient().prepareSearch("index2").get().getHits().getTotalHits().value, equalTo(2L)));

        pauseFollow("index2");
    }

    public void testCloseFollowIndex() throws Exception {
        assertAcked(leaderClient().admin().indices().prepareCreate("index1")
            .setSettings(Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()));

        final PutFollowAction.Request followRequest = putFollow("index1", "index2");
        followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();

        leaderClient().prepareIndex("index1").setId("1").setSource("{}", XContentType.JSON).get();
        assertBusy(() -> assertThat(followerClient().prepareSearch("index2").get().getHits().getTotalHits().value, equalTo(1L)));

        followerClient().admin().indices().close(new CloseIndexRequest("index2").masterNodeTimeout(TimeValue.MAX_VALUE)).actionGet();
        leaderClient().prepareIndex("index1").setId("2").setSource("{}", XContentType.JSON).get();
        assertBusy(() -> {
            StatsResponses response = followerClient().execute(FollowStatsAction.INSTANCE, new StatsRequest()).actionGet();
            assertThat(response.getNodeFailures(), empty());
            assertThat(response.getTaskFailures(), empty());
            assertThat(response.getStatsResponses(), hasSize(1));
            assertThat(response.getStatsResponses().get(0).status().failedWriteRequests(), greaterThanOrEqualTo(1L));
        });
        followerClient().admin().indices().open(new OpenIndexRequest("index2").masterNodeTimeout(TimeValue.MAX_VALUE)).actionGet();
        assertBusy(() -> assertThat(followerClient().prepareSearch("index2").get().getHits().getTotalHits().value, equalTo(2L)));

        pauseFollow("index2");
    }

    public void testDeleteLeaderIndex() throws Exception {
        assertAcked(leaderClient().admin().indices().prepareCreate("index1")
            .setSettings(Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()));

        final PutFollowAction.Request followRequest = putFollow("index1", "index2");
        followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();

        leaderClient().prepareIndex("index1").setId("1").setSource("{}", XContentType.JSON).get();
        assertBusy(() -> assertThat(followerClient().prepareSearch("index2").get().getHits().getTotalHits().value, equalTo(1L)));

        leaderClient().admin().indices().delete(new DeleteIndexRequest("index1")).actionGet();
        assertBusy(() -> {
            StatsResponses response = followerClient().execute(FollowStatsAction.INSTANCE, new StatsRequest()).actionGet();
            assertThat(response.getNodeFailures(), empty());
            assertThat(response.getTaskFailures(), empty());
            assertThat(response.getStatsResponses(), hasSize(1));
            assertThat(response.getStatsResponses().get(0).status().failedReadRequests(), greaterThanOrEqualTo(1L));
            ElasticsearchException fatalException = response.getStatsResponses().get(0).status().getFatalException();
            assertThat(fatalException, notNullValue());
            assertThat(fatalException.getRootCause().getMessage(), equalTo("no such index [index1]"));
        });
        pauseFollow("index2");
        ensureNoCcrTasks();
    }

    public void testFollowClosedIndex() {
        final String leaderIndex = "test-index";
        assertAcked(leaderClient().admin().indices().prepareCreate(leaderIndex));
        assertAcked(leaderClient().admin().indices().prepareClose(leaderIndex));

        final String followerIndex = "follow-test-index";
        expectThrows(IndexClosedException.class,
            () -> followerClient().execute(PutFollowAction.INSTANCE, putFollow(leaderIndex, followerIndex)).actionGet());
        assertFalse(ESIntegTestCase.indexExists(followerIndex, followerClient()));
    }

    public void testResumeFollowOnClosedIndex() throws Exception {
        final String leaderIndex = "test-index";
        assertAcked(leaderClient().admin().indices().prepareCreate(leaderIndex)
            .setSettings(Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()));
        ensureLeaderGreen(leaderIndex);

        final int nbDocs = randomIntBetween(10, 100);
        IntStream.of(nbDocs).forEach(i -> leaderClient().prepareIndex().setIndex(leaderIndex).setSource("field", i).get());

        final String followerIndex = "follow-test-index";
        PutFollowAction.Response response =
            followerClient().execute(PutFollowAction.INSTANCE, putFollow(leaderIndex, followerIndex)).actionGet();
        assertTrue(response.isFollowIndexCreated());
        assertTrue(response.isFollowIndexShardsAcked());
        assertTrue(response.isIndexFollowingStarted());

        pauseFollow(followerIndex);
        assertAcked(leaderClient().admin().indices().prepareClose(leaderIndex).setMasterNodeTimeout(TimeValue.MAX_VALUE));

        expectThrows(IndexClosedException.class, () ->
            followerClient().execute(ResumeFollowAction.INSTANCE, resumeFollow(followerIndex)).actionGet());
    }

    public void testDeleteFollowerIndex() throws Exception {
        assertAcked(leaderClient().admin().indices().prepareCreate("index1")
            .setSettings(Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()));

        final PutFollowAction.Request followRequest = putFollow("index1", "index2");
        followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();

        leaderClient().prepareIndex("index1").setId("1").setSource("{}", XContentType.JSON).get();
        assertBusy(() -> assertThat(followerClient().prepareSearch("index2").get().getHits().getTotalHits().value, equalTo(1L)));

        followerClient().admin().indices().delete(new DeleteIndexRequest("index2").masterNodeTimeout(TimeValue.MAX_VALUE)).actionGet();
        leaderClient().prepareIndex("index1").setId("2").setSource("{}", XContentType.JSON).get();
        assertBusy(() -> {
            StatsResponses response = followerClient().execute(FollowStatsAction.INSTANCE, new StatsRequest()).actionGet();
            assertThat(response.getNodeFailures(), empty());
            assertThat(response.getTaskFailures(), empty());
            if (response.getStatsResponses().isEmpty() == false) {
                assertThat(response.getStatsResponses(), hasSize(1));
                assertThat(response.getStatsResponses().get(0).status().failedWriteRequests(), greaterThanOrEqualTo(1L));
                ElasticsearchException fatalException = response.getStatsResponses().get(0).status().getFatalException();
                assertThat(fatalException, notNullValue());
                assertThat(fatalException.getMessage(), equalTo("no such index [index2]"));
            }
        });
        ensureNoCcrTasks();
    }

    public void testPauseIndex() throws Exception {
        assertAcked(leaderClient().admin().indices().prepareCreate("leader")
            .setSettings(Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()));
        followerClient().execute(PutFollowAction.INSTANCE, putFollow("leader", "follower")).get();
        assertAcked(followerClient().admin().indices().prepareCreate("regular-index").setMasterNodeTimeout(TimeValue.MAX_VALUE));
        assertAcked(followerClient().execute(PauseFollowAction.INSTANCE, new PauseFollowAction.Request("follower")).actionGet());
        assertThat(expectThrows(IllegalArgumentException.class, () -> followerClient().execute(
            PauseFollowAction.INSTANCE, new PauseFollowAction.Request("follower")).actionGet()).getMessage(),
            equalTo("no shard follow tasks for [follower]"));
        assertThat(expectThrows(IllegalArgumentException.class, () -> followerClient().execute(
            PauseFollowAction.INSTANCE, new PauseFollowAction.Request("regular-index")).actionGet()).getMessage(),
            equalTo("index [regular-index] is not a follower index"));
        assertThat(expectThrows(IndexNotFoundException.class, () -> followerClient().execute(
            PauseFollowAction.INSTANCE, new PauseFollowAction.Request("xyz")).actionGet()).getMessage(),
            equalTo("no such index [xyz]"));
    }

    public void testUnfollowIndex() throws Exception {
        String leaderIndexSettings = getIndexSettings(1, 0);
        assertAcked(leaderClient().admin().indices().prepareCreate("index1").setSource(leaderIndexSettings, XContentType.JSON).get());
        PutFollowAction.Request followRequest = putFollow("index1", "index2");
        followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();
        leaderClient().prepareIndex("index1").setSource("{}", XContentType.JSON).get();
        assertBusy(() -> {
            assertThat(followerClient().prepareSearch("index2").get().getHits().getTotalHits().value, equalTo(1L));
        });

        // Indexing directly into index2 would fail now, because index2 is a follow index.
        // We can't test this here because an assertion trips before an actual error is thrown and then index call hangs.

        // Turn follow index into a regular index by: pausing shard follow, close index, unfollow index and then open index:
        pauseFollow("index2");
        followerClient().admin().indices().close(new CloseIndexRequest("index2").masterNodeTimeout(TimeValue.MAX_VALUE)).actionGet();
        assertAcked(followerClient().execute(UnfollowAction.INSTANCE, new UnfollowAction.Request("index2")).actionGet());
        followerClient().admin().indices().open(new OpenIndexRequest("index2").masterNodeTimeout(TimeValue.MAX_VALUE)).actionGet();
        ensureFollowerGreen("index2");

        // Indexing succeeds now, because index2 is no longer a follow index:
        followerClient().prepareIndex("index2").setSource("{}", XContentType.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        assertThat(followerClient().prepareSearch("index2").get().getHits().getTotalHits().value, equalTo(2L));
    }

    public void testUnknownClusterAlias() throws Exception {
        String leaderIndexSettings = getIndexSettings(1, 0);
        assertAcked(leaderClient().admin().indices().prepareCreate("index1").setSource(leaderIndexSettings, XContentType.JSON));
        ensureLeaderGreen("index1");
        PutFollowAction.Request followRequest = putFollow("index1", "index2");
        followRequest.setRemoteCluster("another_cluster");
        Exception e = expectThrows(NoSuchRemoteClusterException.class,
            () -> followerClient().execute(PutFollowAction.INSTANCE, followRequest).actionGet());
        assertThat(e.getMessage(), equalTo("no such remote cluster: [another_cluster]"));
        PutAutoFollowPatternAction.Request putAutoFollowRequest = new PutAutoFollowPatternAction.Request();
        putAutoFollowRequest.setName("name");
        putAutoFollowRequest.setRemoteCluster("another_cluster");
        putAutoFollowRequest.setLeaderIndexPatterns(Collections.singletonList("logs-*"));
        e = expectThrows(NoSuchRemoteClusterException.class,
            () -> followerClient().execute(PutAutoFollowPatternAction.INSTANCE, putAutoFollowRequest).actionGet());
        assertThat(e.getMessage(), equalTo("no such remote cluster: [another_cluster]"));
    }

    public void testLeaderIndexRed() throws Exception {
        try {
            ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
            updateSettingsRequest.transientSettings(Settings.builder().put("cluster.routing.allocation.enable", "none"));
            assertAcked(leaderClient().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
            assertAcked(leaderClient().admin().indices().prepareCreate("index1")
                .setWaitForActiveShards(ActiveShardCount.NONE)
                .setSettings(Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .build()));

            final PutFollowAction.Request followRequest = putFollow("index1", "index2");
            Exception e = expectThrows(IllegalArgumentException.class,
                () -> followerClient().execute(PutFollowAction.INSTANCE, followRequest).actionGet());
            assertThat(e.getMessage(), equalTo("no index stats available for the leader index"));

            assertThat(ESIntegTestCase.indexExists("index2", followerClient()), is(false));
        } finally {
            // Always unset allocation enable setting to avoid other assertions from failing too when this test fails:
            ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
            updateSettingsRequest.transientSettings(Settings.builder().put("cluster.routing.allocation.enable", (String) null));
            assertAcked(leaderClient().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
        }
    }

    public void testUpdateDynamicLeaderIndexSettings() throws Exception {
        final String leaderIndexSettings = getIndexSettings(1, 0);
        assertAcked(leaderClient().admin().indices().prepareCreate("leader").setSource(leaderIndexSettings, XContentType.JSON));
        ensureLeaderYellow("leader");

        final PutFollowAction.Request followRequest = putFollow("leader", "follower");
        followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();
        BooleanSupplier hasFollowIndexBeenClosedChecker = hasFollowIndexBeenClosed("follower");

        final long firstBatchNumDocs = randomIntBetween(2, 64);
        for (long i = 0; i < firstBatchNumDocs; i++) {
            leaderClient().prepareIndex("leader").setSource("{}", XContentType.JSON).get();
        }
        assertBusy(() -> assertThat(followerClient().prepareSearch("follower").get()
            .getHits().getTotalHits().value, equalTo(firstBatchNumDocs)));

        // Sanity check that the setting has not been set in follower index:
        {
            GetSettingsRequest getSettingsRequest = new GetSettingsRequest();
            getSettingsRequest.indices("follower");
            GetSettingsResponse getSettingsResponse = followerClient().admin().indices().getSettings(getSettingsRequest).actionGet();
            assertThat(getSettingsResponse.getSetting("follower", "index.max_ngram_diff"), nullValue());
        }
        assertThat(getFollowTaskSettingsVersion("follower"), equalTo(1L));
        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest("leader");
        updateSettingsRequest.settings(Settings.builder().put("index.max_ngram_diff", 2));
        assertAcked(leaderClient().admin().indices().updateSettings(updateSettingsRequest).actionGet());

        final int secondBatchNumDocs = randomIntBetween(2, 64);
        for (long i = firstBatchNumDocs; i < firstBatchNumDocs + secondBatchNumDocs; i++) {
            leaderClient().prepareIndex("leader").setSource("{}", XContentType.JSON).get();
        }
        assertBusy(() -> {
            // Check that the setting has been set in follower index:
            GetSettingsRequest getSettingsRequest = new GetSettingsRequest();
            getSettingsRequest.indices("follower");
            GetSettingsResponse getSettingsResponse = followerClient().admin().indices().getSettings(getSettingsRequest).actionGet();
            assertThat(getSettingsResponse.getSetting("follower", "index.max_ngram_diff"), equalTo("2"));
            assertThat(getFollowTaskSettingsVersion("follower"), equalTo(2L));

            try {
                assertThat(followerClient().prepareSearch("follower").get().getHits().getTotalHits().value,
                    equalTo(firstBatchNumDocs + secondBatchNumDocs));
            } catch (Exception e) {
                throw new AssertionError("error while searching", e);
            }
        });
        assertThat(hasFollowIndexBeenClosedChecker.getAsBoolean(), is(false));
    }

    public void testLeaderIndexSettingNotPercolatedToFollower() throws Exception {
        // Sets an index setting on leader index that is excluded from being replicated to the follower index and
        // expects that this setting is not replicated to the follower index, but does expect that the settings version
        // is incremented.
        final String leaderIndexSettings = getIndexSettings(1, 0);
        assertAcked(leaderClient().admin().indices().prepareCreate("leader").setSource(leaderIndexSettings, XContentType.JSON));
        ensureLeaderYellow("leader");

        final PutFollowAction.Request followRequest = putFollow("leader", "follower");
        followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();
        BooleanSupplier hasFollowIndexBeenClosedChecker = hasFollowIndexBeenClosed("follower");

        final long firstBatchNumDocs = randomIntBetween(2, 64);
        for (long i = 0; i < firstBatchNumDocs; i++) {
            leaderClient().prepareIndex("leader").setSource("{}", XContentType.JSON).get();
        }
        assertBusy(() -> assertThat(followerClient().prepareSearch("follower").get()
            .getHits().getTotalHits().value, equalTo(firstBatchNumDocs)));

        // Sanity check that the setting has not been set in follower index:
        {
            GetSettingsRequest getSettingsRequest = new GetSettingsRequest();
            getSettingsRequest.indices("follower");
            GetSettingsResponse getSettingsResponse = followerClient().admin().indices().getSettings(getSettingsRequest).actionGet();
            assertThat(getSettingsResponse.getSetting("follower", "index.number_of_replicas"), equalTo("0"));
        }
        assertThat(getFollowTaskSettingsVersion("follower"), equalTo(1L));
        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest("leader");
        updateSettingsRequest.settings(Settings.builder().put("index.number_of_replicas", 1));
        assertAcked(leaderClient().admin().indices().updateSettings(updateSettingsRequest).actionGet());

        final int secondBatchNumDocs = randomIntBetween(2, 64);
        for (long i = firstBatchNumDocs; i < firstBatchNumDocs + secondBatchNumDocs; i++) {
            leaderClient().prepareIndex("leader").setSource("{}", XContentType.JSON).get();
        }
        assertBusy(() -> {
            GetSettingsRequest getSettingsRequest = new GetSettingsRequest();
            getSettingsRequest.indices("follower");
            GetSettingsResponse getSettingsResponse = followerClient().admin().indices().getSettings(getSettingsRequest).actionGet();
            assertThat(getSettingsResponse.getSetting("follower", "index.number_of_replicas"), equalTo("0"));
            assertThat(getFollowTaskSettingsVersion("follower"), equalTo(2L));

            try {
                assertThat(followerClient().prepareSearch("follower").get().getHits().getTotalHits().value,
                    equalTo(firstBatchNumDocs + secondBatchNumDocs));
            } catch (Exception e) {
                throw new AssertionError("error while searching", e);
            }
        });
        assertThat(hasFollowIndexBeenClosedChecker.getAsBoolean(), is(false));
    }

    public void testUpdateAnalysisLeaderIndexSettings() throws Exception {
        final String leaderIndexSettings = getIndexSettings(1, 0);
        assertAcked(leaderClient().admin().indices().prepareCreate("leader").setSource(leaderIndexSettings, XContentType.JSON));
        ensureLeaderYellow("leader");

        final PutFollowAction.Request followRequest = putFollow("leader", "follower");
        followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();
        BooleanSupplier hasFollowIndexBeenClosedChecker = hasFollowIndexBeenClosed("follower");

        final long firstBatchNumDocs = randomIntBetween(2, 64);
        for (long i = 0; i < firstBatchNumDocs; i++) {
            leaderClient().prepareIndex("leader").setSource("{}", XContentType.JSON).get();
        }

        assertBusy(() -> assertThat(followerClient().prepareSearch("follower").get()
            .getHits().getTotalHits().value, equalTo(firstBatchNumDocs)));
        assertThat(getFollowTaskSettingsVersion("follower"), equalTo(1L));
        assertThat(getFollowTaskMappingVersion("follower"), equalTo(1L));

        CloseIndexRequest closeIndexRequest = new CloseIndexRequest("leader");
        assertAcked(leaderClient().admin().indices().close(closeIndexRequest).actionGet());

        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest("leader");
        updateSettingsRequest.settings(Settings.builder()
            .put("index.analysis.analyzer.my_analyzer.type", "custom")
            .put("index.analysis.analyzer.my_analyzer.tokenizer", "keyword")
        );
        assertAcked(leaderClient().admin().indices().updateSettings(updateSettingsRequest).actionGet());

        OpenIndexRequest openIndexRequest = new OpenIndexRequest("leader");
        assertAcked(leaderClient().admin().indices().open(openIndexRequest).actionGet());
        ensureLeaderGreen("leader");

        PutMappingRequest putMappingRequest = new PutMappingRequest("leader");
        putMappingRequest.source("new_field", "type=text,analyzer=my_analyzer");
        assertAcked(leaderClient().admin().indices().putMapping(putMappingRequest).actionGet());

        final int secondBatchNumDocs = randomIntBetween(2, 64);
        for (long i = firstBatchNumDocs; i < firstBatchNumDocs + secondBatchNumDocs; i++) {
            final String source = String.format(Locale.ROOT, "{\"new_field\":\"value %d\"}", i);
            leaderClient().prepareIndex("leader").setSource(source, XContentType.JSON).get();
        }

        assertBusy(() -> {
            assertThat(getFollowTaskSettingsVersion("follower"), equalTo(4L));
            assertThat(getFollowTaskMappingVersion("follower"), equalTo(2L));

            GetSettingsRequest getSettingsRequest = new GetSettingsRequest();
            getSettingsRequest.indices("follower");
            GetSettingsResponse getSettingsResponse = followerClient().admin().indices().getSettings(getSettingsRequest).actionGet();
            assertThat(getSettingsResponse.getSetting("follower", "index.analysis.analyzer.my_analyzer.type"), equalTo("custom"));
            assertThat(getSettingsResponse.getSetting("follower", "index.analysis.analyzer.my_analyzer.tokenizer"), equalTo("keyword"));

            GetMappingsRequest getMappingsRequest = new GetMappingsRequest();
            getMappingsRequest.indices("follower");
            GetMappingsResponse getMappingsResponse = followerClient().admin().indices().getMappings(getMappingsRequest).actionGet();
            MappingMetadata mappingMetadata = getMappingsResponse.getMappings().get("follower");
            assertThat(XContentMapValues.extractValue("properties.new_field.type", mappingMetadata.sourceAsMap()), equalTo("text"));
            assertThat(XContentMapValues.extractValue("properties.new_field.analyzer", mappingMetadata.sourceAsMap()),
                equalTo("my_analyzer"));

            try {
                assertThat(followerClient().prepareSearch("follower").get().getHits().getTotalHits().value,
                    equalTo(firstBatchNumDocs + secondBatchNumDocs));
            } catch (Exception e) {
                throw new AssertionError("error while searching", e);
            }
        }, 30, TimeUnit.SECONDS);
        assertThat(hasFollowIndexBeenClosedChecker.getAsBoolean(), is(true));
    }

    public void testDoNotReplicatePrivateSettings() throws Exception {
        assertAcked(leaderClient().admin().indices().prepareCreate("leader").setSource(getIndexSettings(1, 0), XContentType.JSON));
        ensureLeaderGreen("leader");
        final PutFollowAction.Request followRequest = putFollow("leader", "follower");
        followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();
        ClusterService clusterService = getLeaderCluster().getInstance(ClusterService.class, getLeaderCluster().getMasterName());
        clusterService.submitStateUpdateTask("test", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                final IndexMetadata indexMetadata = currentState.metadata().index("leader");
                Settings.Builder settings = Settings.builder()
                    .put(indexMetadata.getSettings())
                    .put("index.max_ngram_diff", 2);
                if (randomBoolean()) {
                    settings.put(PrivateSettingPlugin.INDEX_INTERNAL_SETTING.getKey(), "private-value");
                }
                if (randomBoolean()) {
                    settings.put(PrivateSettingPlugin.INDEX_PRIVATE_SETTING.getKey(), "interval-value");
                }
                final Metadata.Builder metadata = Metadata.builder(currentState.metadata())
                    .put(IndexMetadata.builder(indexMetadata)
                        .settingsVersion(indexMetadata.getSettingsVersion() + 1)
                        .settings(settings).build(), true);
                return ClusterState.builder(currentState).metadata(metadata).build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                throw new AssertionError(e);
            }
        });
        assertBusy(() -> {
            GetSettingsResponse resp = followerClient().admin().indices().prepareGetSettings("follower").get();
            assertThat(resp.getSetting("follower", "index.max_ngram_diff"), equalTo("2"));
            assertThat(resp.getSetting("follower", PrivateSettingPlugin.INDEX_INTERNAL_SETTING.getKey()), nullValue());
            assertThat(resp.getSetting("follower", PrivateSettingPlugin.INDEX_PRIVATE_SETTING.getKey()), nullValue());
        });
    }

    public void testReplicatePrivateSettingsOnly() throws Exception {
        assertAcked(leaderClient().admin().indices().prepareCreate("leader").setSource(getIndexSettings(1, 0), XContentType.JSON));
        ensureLeaderGreen("leader");
        followerClient().execute(PutFollowAction.INSTANCE, putFollow("leader", "follower")).get();
        final ClusterService clusterService = getLeaderCluster().getInstance(ClusterService.class, getLeaderCluster().getMasterName());
        final SetOnce<Long> settingVersionOnLeader = new SetOnce<>();
        final CountDownLatch latch = new CountDownLatch(1);
        clusterService.submitStateUpdateTask("test", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                final IndexMetadata indexMetadata = currentState.metadata().index("leader");
                Settings.Builder settings = Settings.builder().put(indexMetadata.getSettings());
                settings.put(PrivateSettingPlugin.INDEX_PRIVATE_SETTING.getKey(), "internal-value");
                settings.put(PrivateSettingPlugin.INDEX_INTERNAL_SETTING.getKey(), "internal-value");
                final Metadata.Builder metadata = Metadata.builder(currentState.metadata())
                    .put(IndexMetadata.builder(indexMetadata)
                        .settingsVersion(indexMetadata.getSettingsVersion() + 1)
                        .settings(settings).build(), true);
                return ClusterState.builder(currentState).metadata(metadata).build();
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                settingVersionOnLeader.set(newState.metadata().index("leader").getSettingsVersion());
                latch.countDown();
            }

            @Override
            public void onFailure(String source, Exception e) {
                throw new AssertionError(e);
            }
        });
        latch.await();
        assertBusy(() -> assertThat(getFollowTaskSettingsVersion("follower"), equalTo(settingVersionOnLeader.get())));
        GetSettingsResponse resp = followerClient().admin().indices().prepareGetSettings("follower").get();
        assertThat(resp.getSetting("follower", PrivateSettingPlugin.INDEX_INTERNAL_SETTING.getKey()), nullValue());
        assertThat(resp.getSetting("follower", PrivateSettingPlugin.INDEX_PRIVATE_SETTING.getKey()), nullValue());
    }

    public void testMustCloseIndexAndPauseToRestartWithPutFollowing() throws Exception {
        removeMasterNodeRequestsValidatorOnFollowerCluster();
        final int numberOfPrimaryShards = randomIntBetween(1, 3);
        final String leaderIndexSettings = getIndexSettings(numberOfPrimaryShards, between(0, 1));
        assertAcked(leaderClient().admin().indices().prepareCreate("index1").setSource(leaderIndexSettings, XContentType.JSON));
        ensureLeaderYellow("index1");

        final PutFollowAction.Request followRequest = putFollow("index1", "index2");
        PutFollowAction.Response response = followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();
        assertTrue(response.isFollowIndexCreated());
        assertTrue(response.isFollowIndexShardsAcked());
        assertTrue(response.isIndexFollowingStarted());

        final PutFollowAction.Request followRequest2 = putFollow("index1", "index2");
        expectThrows(SnapshotRestoreException.class,
            () -> followerClient().execute(PutFollowAction.INSTANCE, followRequest2).actionGet());

        followerClient().admin().indices().prepareClose("index2").get();
        expectThrows(ResourceAlreadyExistsException.class,
            () -> followerClient().execute(PutFollowAction.INSTANCE, followRequest2).actionGet());
    }

    public void testIndexFallBehind() throws Exception {
        runFallBehindTest(
                () -> {
                    // we have to remove the retention leases on the leader shards to ensure the follower falls behind
                    final ClusterStateResponse followerIndexClusterState =
                            followerClient().admin().cluster().prepareState().clear().setMetadata(true).setIndices("index2").get();
                    final String followerUUID = followerIndexClusterState.getState().metadata().index("index2").getIndexUUID();
                    final ClusterStateResponse leaderIndexClusterState =
                            leaderClient().admin().cluster().prepareState().clear().setMetadata(true).setIndices("index1").get();
                    final String leaderUUID = leaderIndexClusterState.getState().metadata().index("index1").getIndexUUID();

                    final RoutingTable leaderRoutingTable = leaderClient()
                            .admin()
                            .cluster()
                            .prepareState()
                            .clear()
                            .setIndices("index1")
                            .setRoutingTable(true)
                            .get()
                            .getState()
                            .routingTable();

                    final String retentionLeaseId = retentionLeaseId(
                            getFollowerCluster().getClusterName(),
                            new Index("index2", followerUUID),
                            getLeaderCluster().getClusterName(),
                            new Index("index1", leaderUUID));

                    for (final ObjectCursor<IndexShardRoutingTable> shardRoutingTable
                            : leaderRoutingTable.index("index1").shards().values()) {
                        final ShardId shardId = shardRoutingTable.value.shardId();
                        leaderClient().execute(
                                RetentionLeaseActions.Remove.INSTANCE,
                                new RetentionLeaseActions.RemoveRequest(shardId, retentionLeaseId))
                                .get();
                    }
                },
                exceptions -> assertThat(exceptions.size(), greaterThan(0)));
    }

    public void testIndexDoesNotFallBehind() throws Exception {
        runFallBehindTest(
                () -> {},
                exceptions -> assertThat(exceptions.size(), equalTo(0)));
    }

    /**
     * Runs a fall behind test. In this test, we construct a situation where a follower is paused. While the follower is paused we index
     * more documents that causes soft deletes on the leader, flush them, and run a force merge. This is to set up a situation where the
     * operations will not necessarily be there. With retention leases in place, we would actually expect the operations to be there. After
     * pausing the follower, the specified callback is executed. This gives a test an opportunity to set up assumptions. For example, a test
     * might remove all the retention leases on the leader to set up a situation where the follower will fall behind when it is resumed
     * because the operations will no longer be held on the leader. The specified exceptions callback is invoked after resuming the follower
     * to give a test an opportunity to assert on the resource not found exceptions (either present or not present).
     *
     * @param afterPausingFollower the callback to run after pausing the follower
     * @param exceptionConsumer    the callback to run on a collection of resource not found exceptions after resuming the follower
     * @throws Exception if a checked exception is thrown during the test
     */
    private void runFallBehindTest(
            final CheckedRunnable<Exception> afterPausingFollower,
            final Consumer<Collection<ResourceNotFoundException>> exceptionConsumer) throws Exception {
        final int numberOfPrimaryShards = randomIntBetween(1, 3);
        final Map<String, String> extraSettingsMap = new HashMap<>(2);
        extraSettingsMap.put(IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(), "200ms");
        final String leaderIndexSettings = getIndexSettings(numberOfPrimaryShards, between(0, 1), extraSettingsMap);
        assertAcked(leaderClient().admin().indices().prepareCreate("index1").setSource(leaderIndexSettings, XContentType.JSON));
        ensureLeaderYellow("index1");

        final int numDocs = randomIntBetween(2, 64);
        logger.info("Indexing [{}] docs as first batch", numDocs);
        for (int i = 0; i < numDocs; i++) {
            final String source = String.format(Locale.ROOT, "{\"f\":%d}", i);
            leaderClient().prepareIndex("index1").setId(Integer.toString(i)).setSource(source, XContentType.JSON).get();
        }

        final PutFollowAction.Request followRequest = putFollow("index1", "index2");
        PutFollowAction.Response response = followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();
        assertTrue(response.isFollowIndexCreated());
        assertTrue(response.isFollowIndexShardsAcked());
        assertTrue(response.isIndexFollowingStarted());

        assertIndexFullyReplicatedToFollower("index1", "index2");
        for (int i = 0; i < numDocs; i++) {
            assertBusy(assertExpectedDocumentRunnable(i));
        }

        pauseFollow("index2");

        afterPausingFollower.run();

        for (int i = 0; i < numDocs; i++) {
            final String source = String.format(Locale.ROOT, "{\"f\":%d}", i * 2);
            leaderClient().prepareIndex("index1").setId(Integer.toString(i)).setSource(source, XContentType.JSON).get();
        }
        leaderClient().prepareDelete("index1", "1").get();
        leaderClient().admin().indices().refresh(new RefreshRequest("index1")).actionGet();
        leaderClient().admin().indices().flush(new FlushRequest("index1").force(true)).actionGet();
        assertBusy(() -> {
            final ShardStats[] shardsStats = leaderClient().admin().indices().prepareStats("index1").get().getIndex("index1").getShards();
            for (final ShardStats shardStats : shardsStats) {
                final long maxSeqNo = shardStats.getSeqNoStats().getMaxSeqNo();
                assertTrue(shardStats.getRetentionLeaseStats().retentionLeases().leases().stream()
                    .filter(retentionLease -> ReplicationTracker.PEER_RECOVERY_RETENTION_LEASE_SOURCE.equals(retentionLease.source()))
                    .allMatch(retentionLease -> retentionLease.retainingSequenceNumber() == maxSeqNo + 1));
            }
        });
        ForceMergeRequest forceMergeRequest = new ForceMergeRequest("index1");
        forceMergeRequest.maxNumSegments(1);
        leaderClient().admin().indices().forceMerge(forceMergeRequest).actionGet();

        followerClient().execute(ResumeFollowAction.INSTANCE, resumeFollow("index2")).get();

        assertBusy(() -> {
            List<ShardFollowNodeTaskStatus> statuses = getFollowTaskStatuses("index2");
            Set<ResourceNotFoundException> exceptions = statuses.stream()
                    .map(ShardFollowNodeTaskStatus::getFatalException)
                    .filter(Objects::nonNull)
                    .map(ExceptionsHelper::unwrapCause)
                    .filter(e -> e instanceof ResourceNotFoundException)
                    .map(e -> (ResourceNotFoundException) e)
                    .filter(e -> e.getMetadataKeys().contains("es.requested_operations_missing"))
                    .collect(Collectors.toSet());
            exceptionConsumer.accept(exceptions);
        });

        followerClient().admin().indices().prepareClose("index2").setMasterNodeTimeout(TimeValue.MAX_VALUE).get();
        pauseFollow("index2");
        if (randomBoolean()) {
            assertAcked(followerClient().execute(UnfollowAction.INSTANCE, new UnfollowAction.Request("index2")).actionGet());
        }

        final PutFollowAction.Request followRequest2 = putFollow("index1", "index2");
        PutFollowAction.Response response2 = followerClient().execute(PutFollowAction.INSTANCE, followRequest2).get();
        assertTrue(response2.isFollowIndexCreated());
        assertTrue(response2.isFollowIndexShardsAcked());
        assertTrue(response2.isIndexFollowingStarted());

        ensureFollowerGreen("index2");
        assertIndexFullyReplicatedToFollower("index1", "index2");
        for (int i = 2; i < numDocs; i++) {
            assertBusy(assertExpectedDocumentRunnable(i, i * 2));
        }
    }

    public void testUpdateRemoteConfigsDuringFollowing() throws Exception {
        final int numberOfPrimaryShards = randomIntBetween(1, 3);
        int numberOfReplicas = between(0, 1);

        final String leaderIndexSettings = getIndexSettings(numberOfPrimaryShards, numberOfReplicas);
        assertAcked(leaderClient().admin().indices().prepareCreate("index1").setSource(leaderIndexSettings, XContentType.JSON));
        ensureLeaderYellow("index1");

        final int firstBatchNumDocs = randomIntBetween(200, 800);

        logger.info("Executing put follow");
        final PutFollowAction.Request followRequest = putFollow("index1", "index2");
        PutFollowAction.Response response = followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();
        assertTrue(response.isFollowIndexCreated());
        assertTrue(response.isFollowIndexShardsAcked());
        assertTrue(response.isIndexFollowingStarted());

        logger.info("Indexing [{}] docs while updating remote config", firstBatchNumDocs);
        try (BackgroundIndexer indexer = new BackgroundIndexer("index1", "_doc", leaderClient(), firstBatchNumDocs,
            randomIntBetween(1, 5))) {

            ClusterUpdateSettingsRequest settingsRequest = new ClusterUpdateSettingsRequest().masterNodeTimeout(TimeValue.MAX_VALUE);
            String address = getLeaderCluster().getDataNodeInstance(TransportService.class).boundAddress().publishAddress().toString();
            Setting<Compression.Enabled> compress =
                RemoteClusterService.REMOTE_CLUSTER_COMPRESS.getConcreteSettingForNamespace("leader_cluster");
            Setting<List<String>> seeds = SniffConnectionStrategy.REMOTE_CLUSTER_SEEDS.getConcreteSettingForNamespace("leader_cluster");
            settingsRequest.persistentSettings(Settings.builder().put(compress.getKey(), true).put(seeds.getKey(), address));
            assertAcked(followerClient().admin().cluster().updateSettings(settingsRequest).actionGet());

            waitForDocs(firstBatchNumDocs, indexer);
            indexer.assertNoFailures();

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

            for (String docId : indexer.getIds()) {
                assertBusy(() -> {
                    final GetResponse getResponse = followerClient().prepareGet("index2", docId).get();
                    assertTrue("Doc with id [" + docId + "] is missing", getResponse.isExists());
                });
            }

            assertMaxSeqNoOfUpdatesIsTransferred(resolveLeaderIndex("index1"), resolveFollowerIndex("index2"), numberOfPrimaryShards);
        } finally {
            ClusterUpdateSettingsRequest settingsRequest = new ClusterUpdateSettingsRequest().masterNodeTimeout(TimeValue.MAX_VALUE);
            String address = getLeaderCluster().getDataNodeInstance(TransportService.class).boundAddress().publishAddress().toString();
            Setting<Compression.Enabled> compress =
                RemoteClusterService.REMOTE_CLUSTER_COMPRESS.getConcreteSettingForNamespace("leader_cluster");
            Setting<List<String>> seeds = SniffConnectionStrategy.REMOTE_CLUSTER_SEEDS.getConcreteSettingForNamespace("leader_cluster");
            settingsRequest.persistentSettings(Settings.builder().put(compress.getKey(), compress.getDefault(Settings.EMPTY))
                .put(seeds.getKey(), address));
            assertAcked(followerClient().admin().cluster().updateSettings(settingsRequest).actionGet());
        }
    }

    public void testCleanUpShardFollowTasksForDeletedIndices() throws Exception {
        final int numberOfShards = randomIntBetween(1, 10);
        assertAcked(leaderClient().admin().indices().prepareCreate("index1")
            .setSettings(Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, randomIntBetween(0, 1))
                .build()));

        final PutFollowAction.Request followRequest = putFollow("index1", "index2");
        followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();

        leaderClient().prepareIndex("index1").setId("1").setSource("{}", XContentType.JSON).get();
        assertBusy(() -> assertThat(followerClient().prepareSearch("index2").get().getHits().getTotalHits().value, equalTo(1L)));

        assertBusy(() -> {
            String action = ShardFollowTask.NAME + "[c]";
            ListTasksResponse listTasksResponse = followerClient().admin().cluster().prepareListTasks().setActions(action).get();
            assertThat(listTasksResponse.getTasks(), hasSize(numberOfShards));
        });

        assertAcked(followerClient().admin().indices().prepareDelete("index2").setMasterNodeTimeout(TimeValue.MAX_VALUE));

        assertBusy(() -> {
            String action = ShardFollowTask.NAME + "[c]";
            ListTasksResponse listTasksResponse = followerClient().admin().cluster().prepareListTasks().setActions(action).get();
            assertThat(listTasksResponse.getTasks(), hasSize(0));
        }, 60, TimeUnit.SECONDS);
        ensureNoCcrTasks();
    }

    private long getFollowTaskSettingsVersion(String followerIndex) {
        long settingsVersion = -1L;
        for (ShardFollowNodeTaskStatus status : getFollowTaskStatuses(followerIndex)) {
            if (settingsVersion == -1L) {
                settingsVersion = status.followerSettingsVersion();
            } else {
                assert settingsVersion == status.followerSettingsVersion();
            }
        }
        return settingsVersion;
    }

    private long getFollowTaskMappingVersion(String followerIndex) {
        long mappingVersion = -1L;
        for (ShardFollowNodeTaskStatus status : getFollowTaskStatuses(followerIndex)) {
            if (mappingVersion == -1L) {
                mappingVersion = status.followerMappingVersion();
            } else {
                assert mappingVersion == status.followerMappingVersion();
            }
        }
        return mappingVersion;
    }

    private List<ShardFollowNodeTaskStatus> getFollowTaskStatuses(String followerIndex) {
        FollowStatsAction.StatsRequest request = new StatsRequest();
        request.setIndices(new String[]{followerIndex});
        FollowStatsAction.StatsResponses response = followerClient().execute(FollowStatsAction.INSTANCE, request).actionGet();
        return response.getStatsResponses().stream()
            .map(FollowStatsAction.StatsResponse::status)
            .filter(status -> status.followerIndex().equals(followerIndex))
            .collect(Collectors.toList());
    }

    private BooleanSupplier hasFollowIndexBeenClosed(String indexName) {
        String electedMasterNode = getFollowerCluster().getMasterName();
        ClusterService clusterService = getFollowerCluster().getInstance(ClusterService.class, electedMasterNode);
        AtomicBoolean closed = new AtomicBoolean(false);
        clusterService.addListener(event -> {
            IndexMetadata indexMetadata = event.state().metadata().index(indexName);
            if (indexMetadata != null  && indexMetadata.getState() == IndexMetadata.State.CLOSE) {
                closed.set(true);
            }
        });
        return closed::get;
    }

    private CheckedRunnable<Exception> assertTask(final int numberOfPrimaryShards, final Map<ShardId, Long> numDocsPerShard) {
        return () -> {
            final ClusterState clusterState = followerClient().admin().cluster().prepareState().get().getState();
            final PersistentTasksCustomMetadata taskMetadata = clusterState.getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
            assertNotNull(taskMetadata);

            ListTasksRequest listTasksRequest = new ListTasksRequest();
            listTasksRequest.setDetailed(true);
            listTasksRequest.setActions(ShardFollowTask.NAME + "[c]");
            ListTasksResponse listTasksResponse = followerClient().admin().cluster().listTasks(listTasksRequest).actionGet();
            assertThat(listTasksResponse.getNodeFailures().size(), equalTo(0));
            assertThat(listTasksResponse.getTaskFailures().size(), equalTo(0));

            List<TaskInfo> taskInfos = listTasksResponse.getTasks();
            assertThat(taskInfos.size(), equalTo(numberOfPrimaryShards));
            Collection<PersistentTasksCustomMetadata.PersistentTask<?>> shardFollowTasks =
                taskMetadata.findTasks(ShardFollowTask.NAME, Objects::nonNull);
            for (PersistentTasksCustomMetadata.PersistentTask<?> shardFollowTask : shardFollowTasks) {
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

    private CheckedRunnable<Exception> assertExpectedDocumentRunnable(final int value) {
        return assertExpectedDocumentRunnable(value, value);
    }

    private CheckedRunnable<Exception> assertExpectedDocumentRunnable(final int key, final int value) {
        return () -> {
            final GetResponse getResponse = followerClient().prepareGet("index2", Integer.toString(key)).get();
            assertTrue("Doc with id [" + key + "] is missing", getResponse.isExists());
            if (sourceEnabled) {
                assertTrue((getResponse.getSource().containsKey("f")));
                assertThat(getResponse.getSource().get("f"), equalTo(value));
            }
        };
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

    public static class PrivateSettingPlugin extends Plugin {
        static final Setting<String> INDEX_INTERNAL_SETTING =
            Setting.simpleString("index.internal", Setting.Property.IndexScope, Setting.Property.InternalIndex);
        static final Setting<String> INDEX_PRIVATE_SETTING =
            Setting.simpleString("index.private", Setting.Property.IndexScope, Setting.Property.PrivateIndex);

        @Override
        public List<Setting<?>> getSettings() {
            return Arrays.asList(INDEX_INTERNAL_SETTING, INDEX_PRIVATE_SETTING);
        }
    }
}
