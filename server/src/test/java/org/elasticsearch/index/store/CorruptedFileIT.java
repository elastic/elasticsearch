/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.index.store;

import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoveryFileChunkRequest;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.CorruptionUtils;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.MockIndexEventListener;
import org.elasticsearch.test.store.MockFSIndexStore;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.common.util.CollectionUtils.iterableAsArrayList;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAllSuccessful;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class CorruptedFileIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            // we really need local GW here since this also checks for corruption etc.
            // and we need to make sure primaries are not just trashed if we don't have replicas
            .put(super.nodeSettings(nodeOrdinal))
            // speed up recoveries
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(), 5)
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), 5)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
                MockTransportService.TestPlugin.class,
                MockIndexEventListener.TestPlugin.class,
                MockFSIndexStore.TestPlugin.class,
                InternalSettingsPlugin.class);
    }

    /**
     * Tests that we can actually recover from a corruption on the primary given that we have replica shards around.
     */
    public void testCorruptFileAndRecover() throws ExecutionException, InterruptedException, IOException {
        int numDocs = scaledRandomIntBetween(100, 1000);
        // have enough space for 3 copies
        internalCluster().ensureAtLeastNumDataNodes(3);
        if (cluster().numDataNodes() == 3) {
            logger.info("--> cluster has [3] data nodes, corrupted primary will be overwritten");
        }

        assertThat(cluster().numDataNodes(), greaterThanOrEqualTo(3));

        assertAcked(prepareCreate("test").setSettings(Settings.builder()
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, "1")
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, "1")
            .put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
            // no checkindex - we corrupt shards on purpose
            .put(MockFSIndexStore.INDEX_CHECK_INDEX_ON_CLOSE_SETTING.getKey(), false)
            // no translog based flush - it might change the .liv / segments.N files
            .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), new ByteSizeValue(1, ByteSizeUnit.PB))
        ));
        ensureGreen();
        disableAllocation("test");
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex("test").setSource("field", "value");
        }
        indexRandom(true, builders);
        ensureGreen();
        // double flush to create safe commit in case of async durability
        assertAllSuccessful(client().admin().indices().prepareFlush().setForce(true).get());
        assertAllSuccessful(client().admin().indices().prepareFlush().setForce(true).get());
        // we have to flush at least once here since we don't corrupt the translog
        SearchResponse countResponse = client().prepareSearch().setSize(0).get();
        assertHitCount(countResponse, numDocs);

        final int numShards = numShards("test");
        ShardRouting corruptedShardRouting = corruptRandomPrimaryFile();
        logger.info("--> {} corrupted", corruptedShardRouting);
        enableAllocation("test");
         /*
         * we corrupted the primary shard - now lets make sure we never recover from it successfully
         */
        Settings build = Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, "2").build();
        client().admin().indices().prepareUpdateSettings("test").setSettings(build).get();
        ClusterHealthResponse health = client().admin().cluster()
            .health(Requests.clusterHealthRequest("test").waitForGreenStatus()
                .timeout("5m") // sometimes due to cluster rebalacing and random settings default timeout is just not enough.
                .waitForNoRelocatingShards(true)).actionGet();
        if (health.isTimedOut()) {
            logger.info("cluster state:\n{}\n{}",
                client().admin().cluster().prepareState().get().getState(), client().admin().cluster().preparePendingClusterTasks().get());
            assertThat("timed out waiting for green state", health.isTimedOut(), equalTo(false));
        }
        assertThat(health.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        final int numIterations = scaledRandomIntBetween(5, 20);
        for (int i = 0; i < numIterations; i++) {
            SearchResponse response = client().prepareSearch().setSize(numDocs).get();
            assertHitCount(response, numDocs);
        }



        /*
         * now hook into the IndicesService and register a close listener to
         * run the checkindex. if the corruption is still there we will catch it.
         */
        final CountDownLatch latch = new CountDownLatch(numShards * 3); // primary + 2 replicas
        final CopyOnWriteArrayList<Exception> exception = new CopyOnWriteArrayList<>();
        final IndexEventListener listener = new IndexEventListener() {
            @Override
            public void afterIndexShardClosed(ShardId sid, @Nullable IndexShard indexShard, Settings indexSettings) {
                if (indexShard != null) {
                    Store store = indexShard.store();
                    store.incRef();
                    try {
                        if (!Lucene.indexExists(store.directory()) && indexShard.state() == IndexShardState.STARTED) {
                            return;
                        }
                        BytesStreamOutput os = new BytesStreamOutput();
                        PrintStream out = new PrintStream(os, false, StandardCharsets.UTF_8.name());
                        CheckIndex.Status status = store.checkIndex(out);
                        out.flush();
                        if (!status.clean) {
                            logger.warn("check index [failure]\n{}", os.bytes().utf8ToString());
                            throw new IOException("index check failure");
                        }
                    } catch (Exception e) {
                        exception.add(e);
                    } finally {
                        store.decRef();
                        latch.countDown();
                    }
                }
            }
        };

        for (MockIndexEventListener.TestEventListener eventListener :
                internalCluster().getDataNodeInstances(MockIndexEventListener.TestEventListener.class)) {
            eventListener.setNewDelegate(listener);
        }
        try {
            client().admin().indices().prepareDelete("test").get();
            latch.await();
            assertThat(exception, empty());
        } finally {
            for (MockIndexEventListener.TestEventListener eventListener :
                    internalCluster().getDataNodeInstances(MockIndexEventListener.TestEventListener.class)) {
                eventListener.setNewDelegate(null);
            }
        }
    }

    /**
     * Tests corruption that happens on a single shard when no replicas are present. We make sure that the primary stays unassigned
     * and all other replicas for the healthy shards happens
     */
    public void testCorruptPrimaryNoReplica() throws ExecutionException, InterruptedException, IOException {
        int numDocs = scaledRandomIntBetween(100, 1000);
        internalCluster().ensureAtLeastNumDataNodes(2);

        assertAcked(prepareCreate("test").setSettings(Settings.builder()
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, "0")
            .put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
            .put(MockFSIndexStore.INDEX_CHECK_INDEX_ON_CLOSE_SETTING.getKey(), false) // no checkindex - we corrupt shards on purpose
            // no translog based flush - it might change the .liv / segments.N files
            .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), new ByteSizeValue(1, ByteSizeUnit.PB))
        ));
        ensureGreen();
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex("test").setSource("field", "value");
        }
        indexRandom(true, builders);
        ensureGreen();
        // double flush to create safe commit in case of async durability
        assertAllSuccessful(client().admin().indices().prepareFlush().setForce(true).get());
        assertAllSuccessful(client().admin().indices().prepareFlush().setForce(true).get());
        // we have to flush at least once here since we don't corrupt the translog
        SearchResponse countResponse = client().prepareSearch().setSize(0).get();
        assertHitCount(countResponse, numDocs);

        ShardRouting shardRouting = corruptRandomPrimaryFile();
        /*
         * we corrupted the primary shard - now lets make sure we never recover from it successfully
         */
        Settings build = Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, "1").build();
        client().admin().indices().prepareUpdateSettings("test").setSettings(build).get();
        client().admin().cluster().prepareReroute().get();

        boolean didClusterTurnRed = waitUntil(() -> {
            ClusterHealthStatus test = client().admin().cluster()
                .health(Requests.clusterHealthRequest("test")).actionGet().getStatus();
            return test == ClusterHealthStatus.RED;
        }, 5, TimeUnit.MINUTES);// sometimes on slow nodes the replication / recovery is just dead slow

        final ClusterHealthResponse response = client().admin().cluster()
            .health(Requests.clusterHealthRequest("test")).get();

        if (response.getStatus() != ClusterHealthStatus.RED) {
            logger.info("Cluster turned red in busy loop: {}", didClusterTurnRed);
            logger.info("cluster state:\n{}\n{}",
                client().admin().cluster().prepareState().get().getState(),
                client().admin().cluster().preparePendingClusterTasks().get());
        }
        assertThat(response.getStatus(), is(ClusterHealthStatus.RED));
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        GroupShardsIterator<ShardIterator> shardIterators =
            state.getRoutingTable().activePrimaryShardsGrouped(new String[]{"test"}, false);
        for (ShardIterator iterator : shardIterators) {
            ShardRouting routing;
            while ((routing = iterator.nextOrNull()) != null) {
                if (routing.getId() == shardRouting.getId()) {
                    assertThat(routing.state(), equalTo(ShardRoutingState.UNASSIGNED));
                } else {
                    assertThat(routing.state(), anyOf(equalTo(ShardRoutingState.RELOCATING), equalTo(ShardRoutingState.STARTED)));
                }
            }
        }
        final List<Path> files = listShardFiles(shardRouting);
        Path corruptedFile = null;
        for (Path file : files) {
            if (file.getFileName().toString().startsWith("corrupted_")) {
                corruptedFile = file;
                break;
            }
        }
        assertThat(corruptedFile, notNullValue());
    }

    /**
     * This test triggers a corrupt index exception during finalization size if an empty commit point is transferred
     * during recovery we don't know the version of the segments_N file because it has no segments we can take it from.
     * This simulates recoveries from old indices or even without checksums and makes sure if we fail during finalization
     * we also check if the primary is ok. Without the relevant checks this test fails with a RED cluster
     */
    public void testCorruptionOnNetworkLayerFinalizingRecovery() throws ExecutionException, InterruptedException, IOException {
        internalCluster().ensureAtLeastNumDataNodes(2);
        NodesStatsResponse nodeStats = client().admin().cluster().prepareNodesStats().get();
        List<NodeStats> dataNodeStats = new ArrayList<>();
        for (NodeStats stat : nodeStats.getNodes()) {
            if (stat.getNode().isDataNode()) {
                dataNodeStats.add(stat);
            }
        }

        assertThat(dataNodeStats.size(), greaterThanOrEqualTo(2));
        Collections.shuffle(dataNodeStats, random());
        NodeStats primariesNode = dataNodeStats.get(0);
        NodeStats unluckyNode = dataNodeStats.get(1);
        assertAcked(prepareCreate("test").setSettings(Settings.builder()
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, "0")
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put("index.routing.allocation.include._name", primariesNode.getNode().getName())
            .put(EnableAllocationDecider.INDEX_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Rebalance.NONE)
            .put("index.allocation.max_retries", Integer.MAX_VALUE) // keep on retrying

        ));
        ensureGreen(); // allocated with empty commit
        final AtomicBoolean corrupt = new AtomicBoolean(true);
        final CountDownLatch hasCorrupted = new CountDownLatch(1);
        for (NodeStats dataNode : dataNodeStats) {
            MockTransportService mockTransportService =
                ((MockTransportService) internalCluster().getInstance(TransportService.class, dataNode.getNode().getName()));
            mockTransportService.addSendBehavior(internalCluster().getInstance(TransportService.class, unluckyNode.getNode().getName()),
                (connection, requestId, action, request, options) -> {
                if (corrupt.get() && action.equals(PeerRecoveryTargetService.Actions.FILE_CHUNK)) {
                    RecoveryFileChunkRequest req = (RecoveryFileChunkRequest) request;
                    byte[] array = BytesRef.deepCopyOf(req.content().toBytesRef()).bytes;
                    int i = randomIntBetween(0, req.content().length() - 1);
                    array[i] = (byte) ~array[i]; // flip one byte in the content
                    hasCorrupted.countDown();
                }
                connection.sendRequest(requestId, action, request, options);
            });
        }

        Settings build = Settings.builder()
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, "1")
            .put("index.routing.allocation.include._name",
                primariesNode.getNode().getName() + "," + unluckyNode.getNode().getName()).build();
        client().admin().indices().prepareUpdateSettings("test").setSettings(build).get();
        client().admin().cluster().prepareReroute().get();
        hasCorrupted.await();
        corrupt.set(false);
        ensureGreen();
    }

    /**
     * Tests corruption that happens on the network layer and that the primary does not get affected by corruption that happens on the way
     * to the replica. The file on disk stays uncorrupted
     */
    public void testCorruptionOnNetworkLayer() throws ExecutionException, InterruptedException {
        int numDocs = scaledRandomIntBetween(100, 1000);
        internalCluster().ensureAtLeastNumDataNodes(2);
        if (cluster().numDataNodes() < 3) {
            internalCluster().startNode(Settings.builder()
                .put(Node.NODE_DATA_SETTING.getKey(), true)
                .put(Node.NODE_MASTER_SETTING.getKey(), false));
        }
        NodesStatsResponse nodeStats = client().admin().cluster().prepareNodesStats().get();
        List<NodeStats> dataNodeStats = new ArrayList<>();
        for (NodeStats stat : nodeStats.getNodes()) {
            if (stat.getNode().isDataNode()) {
                dataNodeStats.add(stat);
            }
        }

        assertThat(dataNodeStats.size(), greaterThanOrEqualTo(2));
        Collections.shuffle(dataNodeStats, random());
        NodeStats primariesNode = dataNodeStats.get(0);
        NodeStats unluckyNode = dataNodeStats.get(1);


        assertAcked(prepareCreate("test").setSettings(Settings.builder()
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, "0")
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, between(1, 4)) // don't go crazy here it must recovery fast
            // This does corrupt files on the replica, so we can't check:
            .put(MockFSIndexStore.INDEX_CHECK_INDEX_ON_CLOSE_SETTING.getKey(), false)
            .put("index.routing.allocation.include._name", primariesNode.getNode().getName())
            .put(EnableAllocationDecider.INDEX_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Rebalance.NONE)
        ));
        ensureGreen();
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex("test").setSource("field", "value");
        }
        indexRandom(true, builders);
        ensureGreen();
        assertAllSuccessful(client().admin().indices().prepareFlush().setForce(true).execute().actionGet());
        // we have to flush at least once here since we don't corrupt the translog
        SearchResponse countResponse = client().prepareSearch().setSize(0).get();
        assertHitCount(countResponse, numDocs);
        final boolean truncate = randomBoolean();
        for (NodeStats dataNode : dataNodeStats) {
            MockTransportService mockTransportService =
                ((MockTransportService) internalCluster().getInstance(TransportService.class, dataNode.getNode().getName()));
            mockTransportService.addSendBehavior(internalCluster().getInstance(TransportService.class, unluckyNode.getNode().getName()),
                    (connection, requestId, action, request, options) -> {
                if (action.equals(PeerRecoveryTargetService.Actions.FILE_CHUNK)) {
                    RecoveryFileChunkRequest req = (RecoveryFileChunkRequest) request;
                    if (truncate && req.length() > 1) {
                        BytesRef bytesRef = req.content().toBytesRef();
                        BytesArray array = new BytesArray(bytesRef.bytes, bytesRef.offset, (int) req.length() - 1);
                        request = new RecoveryFileChunkRequest(req.recoveryId(), req.shardId(), req.metadata(), req.position(),
                            array, req.lastChunk(), req.totalTranslogOps(), req.sourceThrottleTimeInNanos());
                    } else {
                        assert req.content().toBytesRef().bytes == req.content().toBytesRef().bytes : "no internal reference!!";
                        final byte[] array = req.content().toBytesRef().bytes;
                        int i = randomIntBetween(0, req.content().length() - 1);
                        array[i] = (byte) ~array[i]; // flip one byte in the content
                    }
                }
                connection.sendRequest(requestId, action, request, options);
            });
        }

        Settings build = Settings.builder()
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, "1")
            .put("index.routing.allocation.include._name", "*").build();
        client().admin().indices().prepareUpdateSettings("test").setSettings(build).get();
        client().admin().cluster().prepareReroute().get();
        ClusterHealthResponse actionGet = client().admin().cluster()
            .health(Requests.clusterHealthRequest("test").waitForGreenStatus()).actionGet();
        if (actionGet.isTimedOut()) {
            logger.info("ensureGreen timed out, cluster state:\n{}\n{}",
                client().admin().cluster().prepareState().get().getState(), client().admin().cluster().preparePendingClusterTasks().get());
            assertThat("timed out waiting for green state", actionGet.isTimedOut(), equalTo(false));
        }
        // we are green so primaries got not corrupted.
        // ensure that no shard is actually allocated on the unlucky node
        ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().get();
        for (IndexShardRoutingTable table : clusterStateResponse.getState().getRoutingTable().index("test")) {
            for (ShardRouting routing : table) {
                if (unluckyNode.getNode().getId().equals(routing.currentNodeId())) {
                    assertThat(routing.state(), not(equalTo(ShardRoutingState.STARTED)));
                    assertThat(routing.state(), not(equalTo(ShardRoutingState.RELOCATING)));
                }
            }
        }
        final int numIterations = scaledRandomIntBetween(5, 20);
        for (int i = 0; i < numIterations; i++) {
            SearchResponse response = client().prepareSearch().setSize(numDocs).get();
            assertHitCount(response, numDocs);
        }

    }


    /**
     * Tests that restoring of a corrupted shard fails and we get a partial snapshot.
     * TODO once checksum verification on snapshotting is implemented this test needs to be fixed or split into several
     * parts... We should also corrupt files on the actual snapshot and check that we don't restore the corrupted shard.
     */
    public void testCorruptFileThenSnapshotAndRestore() throws ExecutionException, InterruptedException, IOException {
        int numDocs = scaledRandomIntBetween(100, 1000);
        internalCluster().ensureAtLeastNumDataNodes(2);

        assertAcked(
            prepareCreate("test").setSettings(Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, "0") // no replicas for this test
                .put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
                // no checkindex - we corrupt shards on purpose
                .put(MockFSIndexStore.INDEX_CHECK_INDEX_ON_CLOSE_SETTING.getKey(), false)
                // no translog based flush - it might change the .liv / segments.N files
                .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), new ByteSizeValue(1, ByteSizeUnit.PB))
            )
        );
        ensureGreen();
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex("test").setSource("field", "value");
        }
        indexRandom(true, builders);
        ensureGreen();
        assertAllSuccessful(client().admin().indices().prepareFlush().setForce(true).execute().actionGet());
        // we have to flush at least once here since we don't corrupt the translog
        SearchResponse countResponse = client().prepareSearch().setSize(0).get();
        assertHitCount(countResponse, numDocs);

        ShardRouting shardRouting = corruptRandomPrimaryFile(false);
        logger.info("--> shard {} has a corrupted file", shardRouting);
        // we don't corrupt segments.gen since S/R doesn't snapshot this file
        // the other problem here why we can't corrupt segments.X files is that the snapshot flushes again before
        // it snapshots and that will write a new segments.X+1 file
        logger.info("-->  creating repository");
        assertAcked(client().admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.builder()
                        .put("location", randomRepoPath().toAbsolutePath())
                        .put("compress", randomBoolean())
                        .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));
        logger.info("--> snapshot");
        final CreateSnapshotResponse createSnapshotResponse =
            client().admin().cluster().prepareCreateSnapshot("test-repo", "test-snap")
                .setWaitForCompletion(true)
                .setIndices("test")
                .get();
        final SnapshotState snapshotState = createSnapshotResponse.getSnapshotInfo().state();
        logger.info("--> snapshot terminated with state " + snapshotState);
        final List<Path> files = listShardFiles(shardRouting);
        Path corruptedFile = null;
        for (Path file : files) {
            if (file.getFileName().toString().startsWith("corrupted_")) {
                corruptedFile = file;
                break;
            }
        }
        assertThat(createSnapshotResponse.getSnapshotInfo().state(), equalTo(SnapshotState.PARTIAL));
        assertThat(corruptedFile, notNullValue());
    }

    /**
     * This test verifies that if we corrupt a replica, we can still get to green, even though
     * listing its store fails. Note, we need to make sure that replicas are allocated on all data
     * nodes, so that replica won't be sneaky and allocated on a node that doesn't have a corrupted
     * replica.
     */
    public void testReplicaCorruption() throws Exception {
        int numDocs = scaledRandomIntBetween(100, 1000);
        internalCluster().ensureAtLeastNumDataNodes(2);

        assertAcked(prepareCreate("test").setSettings(Settings.builder()
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, cluster().numDataNodes() - 1)
            .put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
            .put(MockFSIndexStore.INDEX_CHECK_INDEX_ON_CLOSE_SETTING.getKey(), false) // no checkindex - we corrupt shards on purpose
            .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(),
                new ByteSizeValue(1, ByteSizeUnit.PB)) // no translog based flush - it might change the .liv / segments.N files
        ));
        ensureGreen();
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex("test").setSource("field", "value");
        }
        indexRandom(true, builders);
        ensureGreen();
        assertAllSuccessful(client().admin().indices().prepareFlush().setForce(true).execute().actionGet());
        // we have to flush at least once here since we don't corrupt the translog
        SearchResponse countResponse = client().prepareSearch().setSize(0).get();
        assertHitCount(countResponse, numDocs);

        // disable allocations of replicas post restart (the restart will change replicas to primaries, so we have
        // to capture replicas post restart)
        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(
            Settings.builder().put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "primaries")
        ));

        internalCluster().fullRestart();

        ensureYellow();

        final Index index = resolveIndex("test");

        final IndicesShardStoresResponse stores = client().admin().indices().prepareShardStores(index.getName()).get();


        for (IntObjectCursor<List<IndicesShardStoresResponse.StoreStatus>> shards :
            stores.getStoreStatuses().get(index.getName())) {
            for (IndicesShardStoresResponse.StoreStatus store : shards.value) {
                final ShardId shardId = new ShardId(index, shards.key);
                if (store.getAllocationStatus().equals(IndicesShardStoresResponse.StoreStatus.AllocationStatus.UNUSED)) {
                    for (Path path : findFilesToCorruptOnNode(store.getNode().getName(), shardId)) {
                        try (OutputStream os = Files.newOutputStream(path)) {
                            os.write(0);
                        }
                        logger.info("corrupting file {} on node {}", path, store.getNode().getName());
                    }
                }
            }
        }

        // enable allocation
        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(
            Settings.builder().putNull(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey())
        ));

        ensureGreen(TimeValue.timeValueSeconds(60));
    }

    private int numShards(String... index) {
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        GroupShardsIterator shardIterators = state.getRoutingTable().activePrimaryShardsGrouped(index, false);
        return shardIterators.size();
    }

    private List<Path> findFilesToCorruptOnNode(final String nodeName, final ShardId shardId) throws IOException {
        List<Path> files = new ArrayList<>();
        for (Path path : internalCluster().getInstance(NodeEnvironment.class, nodeName).availableShardPaths(shardId)) {
            path = path.resolve("index");
            if (Files.exists(path)) { // multi data path might only have one path in use
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
                    for (Path item : stream) {
                        if (item.getFileName().toString().startsWith("segments_")) {
                            files.add(item);
                        }
                    }
                }
            }
        }
        return files;
    }

    private ShardRouting corruptRandomPrimaryFile() throws IOException {
        return corruptRandomPrimaryFile(true);
    }

    private ShardRouting corruptRandomPrimaryFile(final boolean includePerCommitFiles) throws IOException {
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        Index test = state.metaData().index("test").getIndex();
        GroupShardsIterator shardIterators = state.getRoutingTable().activePrimaryShardsGrouped(new String[]{"test"}, false);
        List<ShardIterator> iterators = iterableAsArrayList(shardIterators);
        ShardIterator shardIterator = RandomPicks.randomFrom(random(), iterators);
        ShardRouting shardRouting = shardIterator.nextOrNull();
        assertNotNull(shardRouting);
        assertTrue(shardRouting.primary());
        assertTrue(shardRouting.assignedToNode());
        String nodeId = shardRouting.currentNodeId();
        NodesStatsResponse nodeStatses = client().admin().cluster().prepareNodesStats(nodeId).setFs(true).get();
        Set<Path> files = new TreeSet<>(); // treeset makes sure iteration order is deterministic
        for (FsInfo.Path info : nodeStatses.getNodes().get(0).getFs()) {
            String path = info.getPath();
            Path file = PathUtils.get(path)
                .resolve("indices").resolve(test.getUUID()).resolve(Integer.toString(shardRouting.getId())).resolve("index");
            if (Files.exists(file)) { // multi data path might only have one path in use
                try (Directory dir = FSDirectory.open(file)) {
                    SegmentInfos segmentCommitInfos = Lucene.readSegmentInfos(dir);
                    if (includePerCommitFiles) {
                        files.add(file.resolve(segmentCommitInfos.getSegmentsFileName()));
                    }
                    for (SegmentCommitInfo commitInfo : segmentCommitInfos) {
                        if (commitInfo.getDelCount() + commitInfo.getSoftDelCount() == commitInfo.info.maxDoc()) {
                            // don't corrupt fully deleted segments - they might be removed on snapshot
                            continue;
                        }
                        for (String commitFile : commitInfo.files()) {
                            if (includePerCommitFiles || isPerSegmentFile(commitFile)) {
                                files.add(file.resolve(commitFile));
                            }
                        }
                    }

                }
            }
        }
        CorruptionUtils.corruptFile(random(), files.toArray(new Path[0]));
        return shardRouting;
    }

    private static boolean isPerCommitFile(String fileName) {
        // .liv and segments_N are per commit files and might change after corruption
        return fileName.startsWith("segments") || fileName.endsWith(".liv");
    }

    private static boolean isPerSegmentFile(String fileName) {
        return isPerCommitFile(fileName) == false;
    }

    public List<Path> listShardFiles(ShardRouting routing) throws IOException {
        NodesStatsResponse nodeStatses = client().admin().cluster().prepareNodesStats(routing.currentNodeId()).setFs(true).get();
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        final Index test = state.metaData().index("test").getIndex();
        assertThat(routing.toString(), nodeStatses.getNodes().size(), equalTo(1));
        List<Path> files = new ArrayList<>();
        for (FsInfo.Path info : nodeStatses.getNodes().get(0).getFs()) {
            String path = info.getPath();
            Path file = PathUtils.get(path).resolve("indices/" + test.getUUID() + "/" + Integer.toString(routing.getId()) + "/index");
            if (Files.exists(file)) { // multi data path might only have one path in use
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(file)) {
                    for (Path item : stream) {
                        files.add(item);
                    }
                }
            }
        }
        return files;
    }
}
