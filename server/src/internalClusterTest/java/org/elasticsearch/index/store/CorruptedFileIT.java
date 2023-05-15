/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.store;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.TimeValue;
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
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toCollection;
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
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            // we really need local GW here since this also checks for corruption etc.
            // and we need to make sure primaries are not just trashed if we don't have replicas
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
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
            InternalSettingsPlugin.class
        );
    }

    /**
     * Tests that we can actually recover from a corruption on the primary given that we have replica shards around.
     */
    public void testCorruptFileAndRecover() throws InterruptedException, IOException {
        int numDocs = scaledRandomIntBetween(100, 1000);
        // have enough space for 3 copies
        internalCluster().ensureAtLeastNumDataNodes(3);
        if (cluster().numDataNodes() == 3) {
            logger.info("--> cluster has [3] data nodes, corrupted primary will be overwritten");
        }

        assertThat(cluster().numDataNodes(), greaterThanOrEqualTo(3));

        assertAcked(
            prepareCreate("test").setSettings(
                indexSettings(1, 1).put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
                    // no checkindex - we corrupt shards on purpose
                    .put(MockFSIndexStore.INDEX_CHECK_INDEX_ON_CLOSE_SETTING.getKey(), false)
                    // no translog based flush - it might change the .liv / segments.N files
                    .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), new ByteSizeValue(1, ByteSizeUnit.PB))
            )
        );
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
        assertHitCount(client().prepareSearch().setSize(0).get(), numDocs);

        final int numShards = numShards("test");
        ShardRouting corruptedShardRouting = corruptRandomPrimaryFile();
        logger.info("--> {} corrupted", corruptedShardRouting);
        enableAllocation("test");
        /*
         * we corrupted the primary shard - now lets make sure we never recover from it successfully
         */
        setReplicaCount(2, "test");
        ClusterHealthResponse health = clusterAdmin().health(
            new ClusterHealthRequest("test").waitForGreenStatus()
                .timeout("5m") // sometimes due to cluster rebalacing and random settings default timeout is just not enough.
                .waitForNoRelocatingShards(true)
        ).actionGet();
        if (health.isTimedOut()) {
            logger.info(
                "cluster state:\n{}\n{}",
                clusterAdmin().prepareState().get().getState(),
                clusterAdmin().preparePendingClusterTasks().get()
            );
            assertThat("timed out waiting for green state", health.isTimedOut(), equalTo(false));
        }
        assertThat(health.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        final int numIterations = scaledRandomIntBetween(5, 20);
        for (int i = 0; i < numIterations; i++) {
            assertHitCount(client().prepareSearch().setSize(numDocs).get(), numDocs);
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
                        if (Lucene.indexExists(store.directory()) == false && indexShard.state() == IndexShardState.STARTED) {
                            return;
                        }
                        BytesStreamOutput os = new BytesStreamOutput();
                        PrintStream out = new PrintStream(os, false, StandardCharsets.UTF_8.name());
                        CheckIndex.Status status = store.checkIndex(out);
                        out.flush();
                        if (status.clean == false) {
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

        for (MockIndexEventListener.TestEventListener eventListener : internalCluster().getDataNodeInstances(
            MockIndexEventListener.TestEventListener.class
        )) {
            eventListener.setNewDelegate(listener);
        }
        try {
            client().admin().indices().prepareDelete("test").get();
            latch.await();
            assertThat(exception, empty());
        } finally {
            for (MockIndexEventListener.TestEventListener eventListener : internalCluster().getDataNodeInstances(
                MockIndexEventListener.TestEventListener.class
            )) {
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

        assertAcked(
            prepareCreate("test").setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, "0")
                    .put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
                    .put(MockFSIndexStore.INDEX_CHECK_INDEX_ON_CLOSE_SETTING.getKey(), false) // no checkindex - we corrupt shards on
                                                                                              // purpose
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
        // double flush to create safe commit in case of async durability
        assertAllSuccessful(client().admin().indices().prepareFlush().setForce(true).get());
        assertAllSuccessful(client().admin().indices().prepareFlush().setForce(true).get());
        // we have to flush at least once here since we don't corrupt the translog
        assertHitCount(client().prepareSearch().setSize(0).get(), numDocs);

        ShardRouting shardRouting = corruptRandomPrimaryFile();
        /*
         * we corrupted the primary shard - now lets make sure we never recover from it successfully
         */
        setReplicaCount(1, "test");
        clusterAdmin().prepareReroute().get();

        boolean didClusterTurnRed = waitUntil(() -> {
            ClusterHealthStatus test = clusterAdmin().health(new ClusterHealthRequest("test")).actionGet().getStatus();
            return test == ClusterHealthStatus.RED;
        }, 5, TimeUnit.MINUTES);// sometimes on slow nodes the replication / recovery is just dead slow

        final ClusterHealthResponse response = clusterAdmin().health(new ClusterHealthRequest("test")).get();

        if (response.getStatus() != ClusterHealthStatus.RED) {
            logger.info("Cluster turned red in busy loop: {}", didClusterTurnRed);
            logger.info(
                "cluster state:\n{}\n{}",
                clusterAdmin().prepareState().get().getState(),
                clusterAdmin().preparePendingClusterTasks().get()
            );
        }
        assertThat(response.getStatus(), is(ClusterHealthStatus.RED));
        ClusterState state = clusterAdmin().prepareState().get().getState();
        GroupShardsIterator<ShardIterator> shardIterators = state.getRoutingTable()
            .activePrimaryShardsGrouped(new String[] { "test" }, false);
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
    public void testCorruptionOnNetworkLayerFinalizingRecovery() throws InterruptedException {
        internalCluster().ensureAtLeastNumDataNodes(2);

        var dataNodes = getShuffledDataNodes();

        var primariesNode = dataNodes.get(0);
        var unluckyNode = dataNodes.get(1);
        assertAcked(
            prepareCreate("test").setSettings(
                indexSettings(1, 0).put("index.routing.allocation.include._name", primariesNode.getName())
                    .put(EnableAllocationDecider.INDEX_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Rebalance.NONE)
                    .put("index.allocation.max_retries", Integer.MAX_VALUE) // keep on retrying
            )
        );
        ensureGreen(); // allocated with empty commit
        final AtomicBoolean corrupt = new AtomicBoolean(true);
        final CountDownLatch hasCorrupted = new CountDownLatch(1);
        for (var dataNode : dataNodes) {
            MockTransportService mockTransportService = ((MockTransportService) internalCluster().getInstance(
                TransportService.class,
                dataNode.getName()
            ));
            mockTransportService.addSendBehavior(
                internalCluster().getInstance(TransportService.class, unluckyNode.getName()),
                (connection, requestId, action, request, options) -> {
                    if (corrupt.get() && action.equals(PeerRecoveryTargetService.Actions.FILE_CHUNK)) {
                        RecoveryFileChunkRequest req = (RecoveryFileChunkRequest) request;
                        byte[] array = BytesRef.deepCopyOf(req.content().toBytesRef()).bytes;
                        int i = randomIntBetween(0, req.content().length() - 1);
                        array[i] = (byte) ~array[i]; // flip one byte in the content
                        hasCorrupted.countDown();
                    }
                    connection.sendRequest(requestId, action, request, options);
                }
            );
        }

        updateIndexSettings(
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, "1")
                .put("index.routing.allocation.include._name", primariesNode.getName() + "," + unluckyNode.getName()),
            "test"
        );
        clusterAdmin().prepareReroute().get();
        hasCorrupted.await();
        corrupt.set(false);
        ensureGreen();
    }

    /**
     * Tests corruption that happens on the network layer and that the primary does not get affected by corruption that happens on the way
     * to the replica. The file on disk stays uncorrupted
     */
    public void testCorruptionOnNetworkLayer() throws InterruptedException {
        int numDocs = scaledRandomIntBetween(100, 1000);
        internalCluster().ensureAtLeastNumDataNodes(3);

        var dataNodes = getShuffledDataNodes();
        var primariesNode = dataNodes.get(0);
        var unluckyNode = dataNodes.get(1);

        assertAcked(
            prepareCreate("test").setSettings(
                indexSettings(between(1, 4), 0) // don't go crazy here it must recovery fast
                    // This does corrupt files on the replica, so we can't check:
                    .put(MockFSIndexStore.INDEX_CHECK_INDEX_ON_CLOSE_SETTING.getKey(), false)
                    .put("index.routing.allocation.include._name", primariesNode.getName())
                    .put(EnableAllocationDecider.INDEX_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Rebalance.NONE)
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
        assertHitCount(client().prepareSearch().setSize(0).get(), numDocs);

        var source = (MockTransportService) internalCluster().getInstance(TransportService.class, primariesNode.getName());
        var target = internalCluster().getInstance(TransportService.class, unluckyNode.getName());

        final boolean truncate = randomBoolean();
        source.addSendBehavior(target, (connection, requestId, action, request, options) -> {
            if (action.equals(PeerRecoveryTargetService.Actions.FILE_CHUNK)) {
                RecoveryFileChunkRequest req = (RecoveryFileChunkRequest) request;
                if (truncate && req.length() > 1) {
                    BytesRef bytesRef = req.content().toBytesRef();
                    BytesArray array = new BytesArray(bytesRef.bytes, bytesRef.offset, (int) req.length() - 1);
                    request = new RecoveryFileChunkRequest(
                        req.recoveryId(),
                        req.requestSeqNo(),
                        req.shardId(),
                        req.metadata(),
                        req.position(),
                        ReleasableBytesReference.wrap(array),
                        req.lastChunk(),
                        req.totalTranslogOps(),
                        req.sourceThrottleTimeInNanos()
                    );
                } else {
                    assert req.content().toBytesRef().bytes == req.content().toBytesRef().bytes : "no internal reference!!";
                    final byte[] array = req.content().toBytesRef().bytes;
                    int i = randomIntBetween(0, req.content().length() - 1);
                    array[i] = (byte) ~array[i]; // flip one byte in the content
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });

        final var allocationGivenUpFuture = new PlainActionFuture<Void>();
        final var maxRetries = MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.get(Settings.EMPTY);
        new ClusterStateObserver(
            internalCluster().getCurrentMasterNodeInstance(ClusterService.class),
            logger,
            new ThreadContext(Settings.EMPTY)
        ).waitForNextChange(new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {
                allocationGivenUpFuture.onResponse(null);
            }

            @Override
            public void onClusterServiceClose() {
                allocationGivenUpFuture.onFailure(new ElasticsearchException("closed"));
            }

            @Override
            public void onTimeout(TimeValue timeout) {
                allocationGivenUpFuture.onFailure(new ElasticsearchException("timed out"));
            }
        }, state -> {
            final var indexRoutingTable = state.routingTable().index("test");
            for (int shardId = 0; shardId < indexRoutingTable.size(); shardId++) {
                final var replicaShards = indexRoutingTable.shard(shardId).replicaShards();
                if (replicaShards.isEmpty()
                    || replicaShards.stream()
                        .anyMatch(sr -> sr.unassigned() == false || sr.unassignedInfo().getNumFailedAllocations() < maxRetries)) {
                    return false;
                }
            }
            return true;
        }, TimeValue.timeValueSeconds(30));

        // can not allocate on unluckyNode
        updateIndexSettings(
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, "1")
                .put("index.routing.allocation.include._name", primariesNode.getName() + "," + unluckyNode.getName()),
            "test"
        );
        allocationGivenUpFuture.actionGet();
        assertThatAllShards("test", shard -> {
            assertThat(shard.primaryShard().currentNodeId(), equalTo(primariesNode.getId()));
            assertThat(shard.replicaShards().get(0).state(), equalTo(ShardRoutingState.UNASSIGNED));
        });

        // can allocate on any other data node
        updateIndexSettings(
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, "1")
                .putNull("index.routing.allocation.include._name")
                .put("index.routing.allocation.exclude._name", unluckyNode.getName()),
            "test"
        );
        clusterAdmin().prepareReroute().setRetryFailed(true).get();
        ensureGreen("test");
        assertThatAllShards("test", shard -> {
            assertThat(shard.primaryShard().currentNodeId(), not(equalTo(unluckyNode.getId())));
            assertThat(shard.replicaShards().get(0).state(), equalTo(ShardRoutingState.STARTED));
            assertThat(shard.replicaShards().get(0).currentNodeId(), not(equalTo(unluckyNode.getId())));
        });

        final int numIterations = scaledRandomIntBetween(5, 20);
        for (int i = 0; i < numIterations; i++) {
            assertHitCount(client().prepareSearch().setSize(numDocs).get(), numDocs);
        }
    }

    private void assertThatAllShards(String index, Consumer<IndexShardRoutingTable> verifier) {
        var clusterStateResponse = clusterAdmin().state(new ClusterStateRequest().routingTable(true)).actionGet();
        var indexRoutingTable = clusterStateResponse.getState().getRoutingTable().index(index);
        for (int shardId = 0; shardId < indexRoutingTable.size(); shardId++) {
            verifier.accept(indexRoutingTable.shard(shardId));
        }
    }

    /**
     * Tests that restoring of a corrupted shard fails and we get a partial snapshot.
     * TODO once checksum verification on snapshotting is implemented this test needs to be fixed or split into several
     * parts... We should also corrupt files on the actual snapshot and check that we don't restore the corrupted shard.
     */
    public void testCorruptFileThenSnapshotAndRestore() throws InterruptedException, IOException {
        int numDocs = scaledRandomIntBetween(100, 1000);
        internalCluster().ensureAtLeastNumDataNodes(2);

        assertAcked(
            prepareCreate("test").setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, "0") // no replicas for this test
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
        assertHitCount(client().prepareSearch().setSize(0).get(), numDocs);

        ShardRouting shardRouting = corruptRandomPrimaryFile(false);
        logger.info("--> shard {} has a corrupted file", shardRouting);
        // we don't corrupt segments.gen since S/R doesn't snapshot this file
        // the other problem here why we can't corrupt segments.X files is that the snapshot flushes again before
        // it snapshots and that will write a new segments.X+1 file
        logger.info("-->  creating repository");
        assertAcked(
            clusterAdmin().preparePutRepository("test-repo")
                .setType("fs")
                .setSettings(
                    Settings.builder()
                        .put("location", randomRepoPath().toAbsolutePath())
                        .put("compress", randomBoolean())
                        .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
                )
        );
        logger.info("--> snapshot");
        final CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot("test-repo", "test-snap")
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

        assertAcked(
            prepareCreate("test").setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, cluster().numDataNodes() - 1)
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
        assertHitCount(client().prepareSearch().setSize(0).get(), numDocs);

        // disable allocations of replicas post restart (the restart will change replicas to primaries, so we have
        // to capture replicas post restart)
        updateClusterSettings(
            Settings.builder().put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "primaries")
        );

        internalCluster().fullRestart();

        ensureYellow();

        final Index index = resolveIndex("test");

        final IndicesShardStoresResponse stores = client().admin().indices().prepareShardStores(index.getName()).get();

        for (Map.Entry<Integer, List<IndicesShardStoresResponse.StoreStatus>> shards : stores.getStoreStatuses()
            .get(index.getName())
            .entrySet()) {
            for (IndicesShardStoresResponse.StoreStatus store : shards.getValue()) {
                final ShardId shardId = new ShardId(index, shards.getKey());
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
        updateClusterSettings(Settings.builder().putNull(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey()));

        ensureGreen(TimeValue.timeValueSeconds(60));
    }

    private int numShards(String... index) {
        ClusterState state = clusterAdmin().prepareState().get().getState();
        GroupShardsIterator<?> shardIterators = state.getRoutingTable().activePrimaryShardsGrouped(index, false);
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
        ClusterState state = clusterAdmin().prepareState().get().getState();
        Index test = state.metadata().index("test").getIndex();
        GroupShardsIterator<ShardIterator> shardIterators = state.getRoutingTable()
            .activePrimaryShardsGrouped(new String[] { "test" }, false);
        List<ShardIterator> iterators = iterableAsArrayList(shardIterators);
        ShardIterator shardIterator = RandomPicks.randomFrom(random(), iterators);
        ShardRouting shardRouting = shardIterator.nextOrNull();
        assertNotNull(shardRouting);
        assertTrue(shardRouting.primary());
        assertTrue(shardRouting.assignedToNode());
        String nodeId = shardRouting.currentNodeId();
        NodesStatsResponse nodeStatses = clusterAdmin().prepareNodesStats(nodeId).setFs(true).get();
        Set<Path> files = new TreeSet<>(); // treeset makes sure iteration order is deterministic
        for (FsInfo.Path info : nodeStatses.getNodes().get(0).getFs()) {
            String path = info.getPath();
            Path file = PathUtils.get(path)
                .resolve("indices")
                .resolve(test.getUUID())
                .resolve(Integer.toString(shardRouting.getId()))
                .resolve("index");
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
        NodesStatsResponse nodeStatses = clusterAdmin().prepareNodesStats(routing.currentNodeId()).setFs(true).get();
        ClusterState state = clusterAdmin().prepareState().get().getState();
        final Index test = state.metadata().index("test").getIndex();
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

    private List<DiscoveryNode> getShuffledDataNodes() {
        var response = clusterAdmin().prepareNodesStats().get();
        return response.getNodes()
            .stream()
            .map(BaseNodeResponse::getNode)
            .filter(DiscoveryNode::canContainData)
            .collect(collectingAndThen(toCollection(ArrayList::new), list -> {
                Collections.shuffle(list, random());
                return list;
            }));
    }
}
