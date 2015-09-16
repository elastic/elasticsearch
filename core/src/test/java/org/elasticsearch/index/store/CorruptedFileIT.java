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

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import java.nio.charset.StandardCharsets;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
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
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.gateway.PrimaryShardAllocator;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.MergePolicyConfig;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.TranslogService;
import org.elasticsearch.indices.IndicesLifecycle;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoveryFileChunkRequest;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.indices.recovery.RecoveryTarget;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.store.MockFSDirectoryService;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;
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

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE)
public class CorruptedFileIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                // we really need local GW here since this also checks for corruption etc.
                // and we need to make sure primaries are not just trashed if we don't have replicas
                .put(super.nodeSettings(nodeOrdinal))
                        // speed up recoveries
                .put(RecoverySettings.INDICES_RECOVERY_CONCURRENT_STREAMS, 10)
                .put(RecoverySettings.INDICES_RECOVERY_CONCURRENT_SMALL_FILE_STREAMS, 10)
                .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES, 5)
                .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(MockTransportService.TestPlugin.class);
    }

    /**
     * Tests that we can actually recover from a corruption on the primary given that we have replica shards around.
     */
    @Test
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
                        .put(MockFSDirectoryService.CHECK_INDEX_ON_CLOSE, false) // no checkindex - we corrupt shards on purpose
                        .put(TranslogService.INDEX_TRANSLOG_DISABLE_FLUSH, true) // no translog based flush - it might change the .liv / segments.N files
                        .put("indices.recovery.concurrent_streams", 10)
        ));
        ensureGreen();
        disableAllocation("test");
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex("test", "type").setSource("field", "value");
        }
        indexRandom(true, builders);
        ensureGreen();
        assertAllSuccessful(client().admin().indices().prepareFlush().setForce(true).setWaitIfOngoing(true).execute().actionGet());
        // we have to flush at least once here since we don't corrupt the translog
        CountResponse countResponse = client().prepareCount().get();
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
                        .waitForRelocatingShards(0)).actionGet();
        if (health.isTimedOut()) {
            logger.info("cluster state:\n{}\n{}", client().admin().cluster().prepareState().get().getState().prettyPrint(), client().admin().cluster().preparePendingClusterTasks().get().prettyPrint());
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
        final CopyOnWriteArrayList<Throwable> exception = new CopyOnWriteArrayList<>();
        final IndicesLifecycle.Listener listener = new IndicesLifecycle.Listener() {
            @Override
            public void afterIndexShardClosed(ShardId sid, @Nullable IndexShard indexShard, @IndexSettings Settings indexSettings) {
                if (indexShard != null) {
                    Store store = ((IndexShard) indexShard).store();
                    store.incRef();
                    try {
                        if (!Lucene.indexExists(store.directory()) && indexShard.state() == IndexShardState.STARTED) {
                            return;
                        }
                        try (CheckIndex checkIndex = new CheckIndex(store.directory())) {
                            BytesStreamOutput os = new BytesStreamOutput();
                            PrintStream out = new PrintStream(os, false, StandardCharsets.UTF_8.name());
                            checkIndex.setInfoStream(out);
                            out.flush();
                            CheckIndex.Status status = checkIndex.checkIndex();
                            if (!status.clean) {
                                logger.warn("check index [failure]\n{}", new String(os.bytes().toBytes(), StandardCharsets.UTF_8));
                                throw new IOException("index check failure");
                            }
                        }
                    } catch (Throwable t) {
                        exception.add(t);
                    } finally {
                        store.decRef();
                        latch.countDown();
                    }
                }
            }
        };

        for (IndicesService service : internalCluster().getDataNodeInstances(IndicesService.class)) {
            service.indicesLifecycle().addListener(listener);
        }
        try {
            client().admin().indices().prepareDelete("test").get();
            latch.await();
            assertThat(exception, empty());
        } finally {
            for (IndicesService service : internalCluster().getDataNodeInstances(IndicesService.class)) {
                service.indicesLifecycle().removeListener(listener);
            }
        }
    }

    /**
     * Tests corruption that happens on a single shard when no replicas are present. We make sure that the primary stays unassigned
     * and all other replicas for the healthy shards happens
     */
    @Test
    public void testCorruptPrimaryNoReplica() throws ExecutionException, InterruptedException, IOException {
        int numDocs = scaledRandomIntBetween(100, 1000);
        internalCluster().ensureAtLeastNumDataNodes(2);

        assertAcked(prepareCreate("test").setSettings(Settings.builder()
                        .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, "0")
                        .put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
                        .put(MockFSDirectoryService.CHECK_INDEX_ON_CLOSE, false) // no checkindex - we corrupt shards on purpose
                        .put(TranslogService.INDEX_TRANSLOG_DISABLE_FLUSH, true) // no translog based flush - it might change the .liv / segments.N files
                        .put("indices.recovery.concurrent_streams", 10)
        ));
        ensureGreen();
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex("test", "type").setSource("field", "value");
        }
        indexRandom(true, builders);
        ensureGreen();
        assertAllSuccessful(client().admin().indices().prepareFlush().setForce(true).setWaitIfOngoing(true).execute().actionGet());
        // we have to flush at least once here since we don't corrupt the translog
        CountResponse countResponse = client().prepareCount().get();
        assertHitCount(countResponse, numDocs);

        ShardRouting shardRouting = corruptRandomPrimaryFile();
        /*
         * we corrupted the primary shard - now lets make sure we never recover from it successfully
         */
        Settings build = Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, "1").build();
        client().admin().indices().prepareUpdateSettings("test").setSettings(build).get();
        client().admin().cluster().prepareReroute().get();

        boolean didClusterTurnRed = awaitBusy(() -> {
                ClusterHealthStatus test = client().admin().cluster()
                        .health(Requests.clusterHealthRequest("test")).actionGet().getStatus();
                return test == ClusterHealthStatus.RED;
        }, 5, TimeUnit.MINUTES);// sometimes on slow nodes the replication / recovery is just dead slow
        final ClusterHealthResponse response = client().admin().cluster()
                .health(Requests.clusterHealthRequest("test")).get();
        if (response.getStatus() != ClusterHealthStatus.RED) {
            logger.info("Cluster turned red in busy loop: {}", didClusterTurnRed);
            logger.info("cluster state:\n{}\n{}", client().admin().cluster().prepareState().get().getState().prettyPrint(), client().admin().cluster().preparePendingClusterTasks().get().prettyPrint());
        }
        assertThat(response.getStatus(), is(ClusterHealthStatus.RED));
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        GroupShardsIterator shardIterators = state.getRoutingNodes().getRoutingTable().activePrimaryShardsGrouped(new String[]{"test"}, false);
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
        Collections.shuffle(dataNodeStats, getRandom());
        NodeStats primariesNode = dataNodeStats.get(0);
        NodeStats unluckyNode = dataNodeStats.get(1);
        assertAcked(prepareCreate("test").setSettings(Settings.builder()
                        .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, "0")
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                        .put("index.routing.allocation.include._name", primariesNode.getNode().name())
                        .put(EnableAllocationDecider.INDEX_ROUTING_REBALANCE_ENABLE, EnableAllocationDecider.Rebalance.NONE)

        ));
        ensureGreen(); // allocated with empty commit
        final AtomicBoolean corrupt = new AtomicBoolean(true);
        final CountDownLatch hasCorrupted = new CountDownLatch(1);
        for (NodeStats dataNode : dataNodeStats) {
            MockTransportService mockTransportService = ((MockTransportService) internalCluster().getInstance(TransportService.class, dataNode.getNode().name()));
            mockTransportService.addDelegate(internalCluster().getInstance(Discovery.class, unluckyNode.getNode().name()).localNode(), new MockTransportService.DelegateTransport(mockTransportService.original()) {

                @Override
                public void sendRequest(DiscoveryNode node, long requestId, String action, TransportRequest request, TransportRequestOptions options) throws IOException, TransportException {
                    if (corrupt.get() && action.equals(RecoveryTarget.Actions.FILE_CHUNK)) {
                        RecoveryFileChunkRequest req = (RecoveryFileChunkRequest) request;
                        byte[] array = req.content().array();
                        int i = randomIntBetween(0, req.content().length() - 1);
                        array[i] = (byte) ~array[i]; // flip one byte in the content
                        hasCorrupted.countDown();
                    }
                    super.sendRequest(node, requestId, action, request, options);
                }
            });
        }

        Settings build = Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, "1")
                .put("index.routing.allocation.include._name", primariesNode.getNode().name() + "," + unluckyNode.getNode().name()).build();
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
    @Test
    public void testCorruptionOnNetworkLayer() throws ExecutionException, InterruptedException {
        int numDocs = scaledRandomIntBetween(100, 1000);
        internalCluster().ensureAtLeastNumDataNodes(2);
        if (cluster().numDataNodes() < 3) {
            internalCluster().startNode(Settings.builder().put("node.data", true).put("node.client", false).put("node.master", false));
        }
        NodesStatsResponse nodeStats = client().admin().cluster().prepareNodesStats().get();
        List<NodeStats> dataNodeStats = new ArrayList<>();
        for (NodeStats stat : nodeStats.getNodes()) {
            if (stat.getNode().isDataNode()) {
                dataNodeStats.add(stat);
            }
        }

        assertThat(dataNodeStats.size(), greaterThanOrEqualTo(2));
        Collections.shuffle(dataNodeStats, getRandom());
        NodeStats primariesNode = dataNodeStats.get(0);
        NodeStats unluckyNode = dataNodeStats.get(1);


        assertAcked(prepareCreate("test").setSettings(Settings.builder()
                        .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, "0")
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, between(1, 4)) // don't go crazy here it must recovery fast
                                // This does corrupt files on the replica, so we can't check:
                        .put(MockFSDirectoryService.CHECK_INDEX_ON_CLOSE, false)
                        .put("index.routing.allocation.include._name", primariesNode.getNode().name())
                        .put(EnableAllocationDecider.INDEX_ROUTING_REBALANCE_ENABLE, EnableAllocationDecider.Rebalance.NONE)
        ));
        ensureGreen();
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex("test", "type").setSource("field", "value");
        }
        indexRandom(true, builders);
        ensureGreen();
        assertAllSuccessful(client().admin().indices().prepareFlush().setForce(true).setWaitIfOngoing(true).execute().actionGet());
        // we have to flush at least once here since we don't corrupt the translog
        CountResponse countResponse = client().prepareCount().get();
        assertHitCount(countResponse, numDocs);
        final boolean truncate = randomBoolean();
        for (NodeStats dataNode : dataNodeStats) {
            MockTransportService mockTransportService = ((MockTransportService) internalCluster().getInstance(TransportService.class, dataNode.getNode().name()));
            mockTransportService.addDelegate(internalCluster().getInstance(Discovery.class, unluckyNode.getNode().name()).localNode(), new MockTransportService.DelegateTransport(mockTransportService.original()) {

                @Override
                public void sendRequest(DiscoveryNode node, long requestId, String action, TransportRequest request, TransportRequestOptions options) throws IOException, TransportException {
                    if (action.equals(RecoveryTarget.Actions.FILE_CHUNK)) {
                        RecoveryFileChunkRequest req = (RecoveryFileChunkRequest) request;
                        if (truncate && req.length() > 1) {
                            BytesArray array = new BytesArray(req.content().array(), req.content().arrayOffset(), (int) req.length() - 1);
                            request = new RecoveryFileChunkRequest(req.recoveryId(), req.shardId(), req.metadata(), req.position(), array, req.lastChunk(), req.totalTranslogOps(), req.sourceThrottleTimeInNanos());
                        } else {
                            byte[] array = req.content().array();
                            int i = randomIntBetween(0, req.content().length() - 1);
                            array[i] = (byte) ~array[i]; // flip one byte in the content
                        }
                    }
                    super.sendRequest(node, requestId, action, request, options);
                }
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
            logger.info("ensureGreen timed out, cluster state:\n{}\n{}", client().admin().cluster().prepareState().get().getState().prettyPrint(), client().admin().cluster().preparePendingClusterTasks().get().prettyPrint());
            assertThat("timed out waiting for green state", actionGet.isTimedOut(), equalTo(false));
        }
        // we are green so primaries got not corrupted.
        // ensure that no shard is actually allocated on the unlucky node
        ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().get();
        for (IndexShardRoutingTable table : clusterStateResponse.getState().getRoutingNodes().getRoutingTable().index("test")) {
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
    @Test
    public void testCorruptFileThenSnapshotAndRestore() throws ExecutionException, InterruptedException, IOException {
        int numDocs = scaledRandomIntBetween(100, 1000);
        internalCluster().ensureAtLeastNumDataNodes(2);

        assertAcked(prepareCreate("test").setSettings(Settings.builder()
                        .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, "0") // no replicas for this test
                        .put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
                        .put(MockFSDirectoryService.CHECK_INDEX_ON_CLOSE, false) // no checkindex - we corrupt shards on purpose
                        .put(TranslogService.INDEX_TRANSLOG_DISABLE_FLUSH, true) // no translog based flush - it might change the .liv / segments.N files
                        .put("indices.recovery.concurrent_streams", 10)
        ));
        ensureGreen();
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex("test", "type").setSource("field", "value");
        }
        indexRandom(true, builders);
        ensureGreen();
        assertAllSuccessful(client().admin().indices().prepareFlush().setForce(true).setWaitIfOngoing(true).execute().actionGet());
        // we have to flush at least once here since we don't corrupt the translog
        CountResponse countResponse = client().prepareCount().get();
        assertHitCount(countResponse, numDocs);

        ShardRouting shardRouting = corruptRandomPrimaryFile(false);
        // we don't corrupt segments.gen since S/R doesn't snapshot this file
        // the other problem here why we can't corrupt segments.X files is that the snapshot flushes again before
        // it snapshots and that will write a new segments.X+1 file
        logger.info("-->  creating repository");
        assertAcked(client().admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(settingsBuilder()
                        .put("location", randomRepoPath().toAbsolutePath())
                        .put("compress", randomBoolean())
                        .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));
        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = client().admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(true).setIndices("test").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().state(), equalTo(SnapshotState.PARTIAL));
        logger.info("failed during snapshot -- maybe SI file got corrupted");
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
     * This test verifies that if we corrupt a replica, we can still get to green, even though
     * listing its store fails. Note, we need to make sure that replicas are allocated on all data
     * nodes, so that replica won't be sneaky and allocated on a node that doesn't have a corrupted
     * replica.
     */
    @Test
    public void testReplicaCorruption() throws Exception {
        int numDocs = scaledRandomIntBetween(100, 1000);
        internalCluster().ensureAtLeastNumDataNodes(2);

        assertAcked(prepareCreate("test").setSettings(Settings.builder()
                        .put(PrimaryShardAllocator.INDEX_RECOVERY_INITIAL_SHARDS, "one")
                        .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, cluster().numDataNodes() - 1)
                        .put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
                        .put(MockFSDirectoryService.CHECK_INDEX_ON_CLOSE, false) // no checkindex - we corrupt shards on purpose
                        .put(TranslogService.INDEX_TRANSLOG_DISABLE_FLUSH, true) // no translog based flush - it might change the .liv / segments.N files
                        .put("indices.recovery.concurrent_streams", 10)
        ));
        ensureGreen();
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex("test", "type").setSource("field", "value");
        }
        indexRandom(true, builders);
        ensureGreen();
        assertAllSuccessful(client().admin().indices().prepareFlush().setForce(true).setWaitIfOngoing(true).execute().actionGet());
        // we have to flush at least once here since we don't corrupt the translog
        CountResponse countResponse = client().prepareCount().get();
        assertHitCount(countResponse, numDocs);

        final Map<String, List<Path>> filesToCorrupt = findFilesToCorruptForReplica();
        internalCluster().fullRestart(new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                List<Path> paths = filesToCorrupt.get(nodeName);
                if (paths != null) {
                    for (Path path : paths) {
                        try (OutputStream os = Files.newOutputStream(path)) {
                            os.write(0);
                        }
                        logger.info("corrupting file {} on node {}", path, nodeName);
                    }
                }
                return null;
            }
        });
        ensureGreen();
    }

    private int numShards(String... index) {
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        GroupShardsIterator shardIterators = state.getRoutingNodes().getRoutingTable().activePrimaryShardsGrouped(index, false);
        return shardIterators.size();
    }

    private Map<String, List<Path>> findFilesToCorruptForReplica() throws IOException {
        Map<String, List<Path>> filesToNodes = new HashMap<>();
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        for (ShardRouting shardRouting : state.getRoutingTable().allShards("test")) {
            if (shardRouting.primary() == true) {
                continue;
            }
            assertTrue(shardRouting.assignedToNode());
            NodesStatsResponse nodeStatses = client().admin().cluster().prepareNodesStats(shardRouting.currentNodeId()).setFs(true).get();
            NodeStats nodeStats = nodeStatses.getNodes()[0];
            List<Path> files = new ArrayList<>();
            filesToNodes.put(nodeStats.getNode().getName(), files);
            for (FsInfo.Path info : nodeStats.getFs()) {
                String path = info.getPath();
                final String relativeDataLocationPath = "indices/test/" + Integer.toString(shardRouting.getId()) + "/index";
                Path file = PathUtils.get(path).resolve(relativeDataLocationPath);
                if (Files.exists(file)) { // multi data path might only have one path in use
                    try (DirectoryStream<Path> stream = Files.newDirectoryStream(file)) {
                        for (Path item : stream) {
                            if (item.getFileName().toString().startsWith("segments_")) {
                                files.add(item);
                            }
                        }
                    }
                }
            }
        }
        return filesToNodes;
    }

    private ShardRouting corruptRandomPrimaryFile() throws IOException {
        return corruptRandomPrimaryFile(true);
    }

    private ShardRouting corruptRandomPrimaryFile(final boolean includePerCommitFiles) throws IOException {
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        GroupShardsIterator shardIterators = state.getRoutingNodes().getRoutingTable().activePrimaryShardsGrouped(new String[]{"test"}, false);
        List<ShardIterator> iterators = iterableAsArrayList(shardIterators);
        ShardIterator shardIterator = RandomPicks.randomFrom(getRandom(), iterators);
        ShardRouting shardRouting = shardIterator.nextOrNull();
        assertNotNull(shardRouting);
        assertTrue(shardRouting.primary());
        assertTrue(shardRouting.assignedToNode());
        String nodeId = shardRouting.currentNodeId();
        NodesStatsResponse nodeStatses = client().admin().cluster().prepareNodesStats(nodeId).setFs(true).get();
        Set<Path> files = new TreeSet<>(); // treeset makes sure iteration order is deterministic
        for (FsInfo.Path info : nodeStatses.getNodes()[0].getFs()) {
            String path = info.getPath();
            final String relativeDataLocationPath = "indices/test/" + Integer.toString(shardRouting.getId()) + "/index";
            Path file = PathUtils.get(path).resolve(relativeDataLocationPath);
            if (Files.exists(file)) { // multi data path might only have one path in use
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(file)) {
                    for (Path item : stream) {
                        if (Files.isRegularFile(item) && "write.lock".equals(item.getFileName().toString()) == false) {
                            if (includePerCommitFiles || isPerSegmentFile(item.getFileName().toString())) {
                                files.add(item);
                            }
                        }
                    }
                }
            }
        }
        pruneOldDeleteGenerations(files);
        Path fileToCorrupt = null;
        if (!files.isEmpty()) {
            fileToCorrupt = RandomPicks.randomFrom(getRandom(), files);
            try (Directory dir = FSDirectory.open(fileToCorrupt.toAbsolutePath().getParent())) {
                long checksumBeforeCorruption;
                try (IndexInput input = dir.openInput(fileToCorrupt.getFileName().toString(), IOContext.DEFAULT)) {
                    checksumBeforeCorruption = CodecUtil.retrieveChecksum(input);
                }
                try (FileChannel raf = FileChannel.open(fileToCorrupt, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                    // read
                    raf.position(randomIntBetween(0, (int) Math.min(Integer.MAX_VALUE, raf.size() - 1)));
                    long filePointer = raf.position();
                    ByteBuffer bb = ByteBuffer.wrap(new byte[1]);
                    raf.read(bb);
                    bb.flip();

                    // corrupt
                    byte oldValue = bb.get(0);
                    byte newValue = (byte) (oldValue + 1);
                    bb.put(0, newValue);

                    // rewrite
                    raf.position(filePointer);
                    raf.write(bb);
                    logger.info("Corrupting file for shard {} --  flipping at position {} from {} to {} file: {}", shardRouting, filePointer, Integer.toHexString(oldValue), Integer.toHexString(newValue), fileToCorrupt.getFileName());
                }
                long checksumAfterCorruption;
                long actualChecksumAfterCorruption;
                try (ChecksumIndexInput input = dir.openChecksumInput(fileToCorrupt.getFileName().toString(), IOContext.DEFAULT)) {
                    assertThat(input.getFilePointer(), is(0l));
                    input.seek(input.length() - 8); // one long is the checksum... 8 bytes
                    checksumAfterCorruption = input.getChecksum();
                    actualChecksumAfterCorruption = input.readLong();
                }
                // we need to add assumptions here that the checksums actually really don't match there is a small chance to get collisions
                // in the checksum which is ok though....
                StringBuilder msg = new StringBuilder();
                msg.append("Checksum before: [").append(checksumBeforeCorruption).append("]");
                msg.append(" after: [").append(checksumAfterCorruption).append("]");
                msg.append(" checksum value after corruption: ").append(actualChecksumAfterCorruption).append("]");
                msg.append(" file: ").append(fileToCorrupt.getFileName()).append(" length: ").append(dir.fileLength(fileToCorrupt.getFileName().toString()));
                logger.info(msg.toString());
                assumeTrue("Checksum collision - " + msg.toString(),
                        checksumAfterCorruption != checksumBeforeCorruption // collision
                                || actualChecksumAfterCorruption != checksumBeforeCorruption); // checksum corrupted
            }
        }
        assertThat("no file corrupted", fileToCorrupt, notNullValue());
        return shardRouting;
    }

    private static final boolean isPerCommitFile(String fileName) {
        // .liv and segments_N are per commit files and might change after corruption
        return fileName.startsWith("segments") || fileName.endsWith(".liv");
    }

    private static final boolean isPerSegmentFile(String fileName) {
        return isPerCommitFile(fileName) == false;
    }

    /**
     * prunes the list of index files such that only the latest del generation files are contained.
     */
    private void pruneOldDeleteGenerations(Set<Path> files) {
        final TreeSet<Path> delFiles = new TreeSet<>();
        for (Path file : files) {
            if (file.getFileName().toString().endsWith(".liv")) {
                delFiles.add(file);
            }
        }
        Path last = null;
        for (Path current : delFiles) {
            if (last != null) {
                final String newSegmentName = IndexFileNames.parseSegmentName(current.getFileName().toString());
                final String oldSegmentName = IndexFileNames.parseSegmentName(last.getFileName().toString());
                if (newSegmentName.equals(oldSegmentName)) {
                    int oldGen = Integer.parseInt(IndexFileNames.stripExtension(IndexFileNames.stripSegmentName(last.getFileName().toString())).replace("_", ""), Character.MAX_RADIX);
                    int newGen = Integer.parseInt(IndexFileNames.stripExtension(IndexFileNames.stripSegmentName(current.getFileName().toString())).replace("_", ""), Character.MAX_RADIX);
                    if (newGen > oldGen) {
                        files.remove(last);
                    } else {
                        files.remove(current);
                        continue;
                    }
                }
            }
            last = current;
        }
    }

    public List<Path> listShardFiles(ShardRouting routing) throws IOException {
        NodesStatsResponse nodeStatses = client().admin().cluster().prepareNodesStats(routing.currentNodeId()).setFs(true).get();

        assertThat(routing.toString(), nodeStatses.getNodes().length, equalTo(1));
        List<Path> files = new ArrayList<>();
        for (FsInfo.Path info : nodeStatses.getNodes()[0].getFs()) {
            String path = info.getPath();
            Path file = PathUtils.get(path).resolve("indices/test/" + Integer.toString(routing.getId()) + "/index");
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
