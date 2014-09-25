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

import com.carrotsearch.ant.tasks.junit4.dependencies.com.google.common.collect.Lists;
import com.carrotsearch.randomizedtesting.LifecycleScope;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import com.google.common.base.Charsets;
import com.google.common.base.Predicate;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.*;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.*;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.index.engine.internal.InternalEngine;
import org.elasticsearch.index.merge.policy.AbstractMergePolicyProvider;
import org.elasticsearch.index.merge.policy.MergePolicyModule;
import org.elasticsearch.index.shard.IndexShardException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.index.shard.service.InternalIndexShard;
import org.elasticsearch.index.translog.TranslogService;
import org.elasticsearch.indices.IndicesLifecycle;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoveryFileChunkRequest;
import org.elasticsearch.indices.recovery.RecoveryTarget;
import org.elasticsearch.monitor.fs.FsStats;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.store.MockFSDirectoryService;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.*;
import org.junit.Test;

import java.io.*;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;
import static org.hamcrest.Matchers.*;

@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.SUITE)
public class CorruptedFileTest extends ElasticsearchIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.builder()
                // we really need local GW here since this also checks for corruption etc.
                // and we need to make sure primaries are not just trashed if we don'tmvn have replicas
                .put(super.nodeSettings(nodeOrdinal)).put("gateway.type", "local")
                .put(TransportModule.TRANSPORT_SERVICE_TYPE_KEY, MockTransportService.class.getName()).build();
    }

    /**
     * Tests that we can actually recover from a corruption on the primary given that we have replica shards around.
     */
    @Test
    public void testCorruptFileAndRecover() throws ExecutionException, InterruptedException, IOException {
        int numDocs = scaledRandomIntBetween(100, 1000);
        assertThat(cluster().numDataNodes(), greaterThanOrEqualTo(2));

        while (cluster().numDataNodes() < 4) {
            /**
             * We need 4 nodes since if we have 2 replicas and only 3 nodes we can't get into green state since
             * the corrupted node will never be used reallocate a replica since it's marked as corrupted
             */
            internalCluster().startNode(ImmutableSettings.builder().put("node.data", true).put("node.client", false).put("node.master", false));
        }
        assertThat(cluster().numDataNodes(), greaterThanOrEqualTo(3));

        final boolean failOnCorruption = randomBoolean();
        assertAcked(prepareCreate("test").setSettings(ImmutableSettings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, "1")
                .put(MergePolicyModule.MERGE_POLICY_TYPE_KEY, NoMergePolicyProvider.class)
                .put(MockFSDirectoryService.CHECK_INDEX_ON_CLOSE, false) // no checkindex - we corrupt shards on purpose
                .put(InternalEngine.INDEX_FAIL_ON_CORRUPTION, failOnCorruption)
                .put(TranslogService.INDEX_TRANSLOG_DISABLE_FLUSH, true) // no translog based flush - it might change the .del / segments.N files
                .put("indices.recovery.concurrent_streams", 10)
        ));
        if (failOnCorruption == false) { // test the dynamic setting
            client().admin().indices().prepareUpdateSettings("test").setSettings(ImmutableSettings.builder()
                    .put(InternalEngine.INDEX_FAIL_ON_CORRUPTION, true)).get();
        }
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
        ShardRouting corruptedShardRouting = corruptRandomFile();
        enableAllocation("test");
         /*
         * we corrupted the primary shard - now lets make sure we never recover from it successfully
         */
        Settings build = ImmutableSettings.builder().put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, "2").build();
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
            public void beforeIndexShardClosed(ShardId sid, @Nullable IndexShard indexShard) {
                if (indexShard != null) {
                    Store store = ((InternalIndexShard) indexShard).store();
                    store.incRef();
                    try {
                        if (!Lucene.indexExists(store.directory()) && indexShard.state() == IndexShardState.STARTED) {
                            return;
                        }
                        CheckIndex checkIndex = new CheckIndex(store.directory());
                        BytesStreamOutput os = new BytesStreamOutput();
                        PrintStream out = new PrintStream(os, false, Charsets.UTF_8.name());
                        checkIndex.setInfoStream(out);
                        out.flush();
                        CheckIndex.Status status = checkIndex.checkIndex();
                        if (!status.clean) {
                            logger.warn("check index [failure]\n{}", new String(os.bytes().toBytes(), Charsets.UTF_8));
                            throw new IndexShardException(sid, "index check failure");
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
        assertThat(cluster().numDataNodes(), greaterThanOrEqualTo(2));

        assertAcked(prepareCreate("test").setSettings(ImmutableSettings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, "0")
                .put(MergePolicyModule.MERGE_POLICY_TYPE_KEY, NoMergePolicyProvider.class)
                .put(MockFSDirectoryService.CHECK_INDEX_ON_CLOSE, false) // no checkindex - we corrupt shards on purpose
                .put(InternalEngine.INDEX_FAIL_ON_CORRUPTION, true)
                .put(TranslogService.INDEX_TRANSLOG_DISABLE_FLUSH, true) // no translog based flush - it might change the .del / segments.N files
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

        ShardRouting shardRouting = corruptRandomFile();
        /*
         * we corrupted the primary shard - now lets make sure we never recover from it successfully
         */
        Settings build = ImmutableSettings.builder().put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, "1").build();
        client().admin().indices().prepareUpdateSettings("test").setSettings(build).get();
        client().admin().cluster().prepareReroute().get();

        boolean didClusterTurnRed = awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object input) {
                ClusterHealthStatus test = client().admin().cluster()
                        .health(Requests.clusterHealthRequest("test")).actionGet().getStatus();
                return test == ClusterHealthStatus.RED;
            }
        }, 5, TimeUnit.MINUTES);// sometimes on slow nodes the replication / recovery is just dead slow
        final ClusterHealthResponse response = client().admin().cluster()
                .health(Requests.clusterHealthRequest("test")).get();
        if (response.getStatus() != ClusterHealthStatus.RED) {
            logger.info("Cluster turned red in busy loop: {}", didClusterTurnRed);
            logger.info("cluster state:\n{}\n{}", client().admin().cluster().prepareState().get().getState().prettyPrint(), client().admin().cluster().preparePendingClusterTasks().get().prettyPrint());
        }
        assertThat(response.getStatus(), is(ClusterHealthStatus.RED));
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        GroupShardsIterator shardIterators = state.getRoutingNodes().getRoutingTable().activePrimaryShardsGrouped(new String[] {"test"}, false);
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
        final List<File> files = listShardFiles(shardRouting);
        File corruptedFile = null;
        for (File file : files) {
            if (file.getName().startsWith("corrupted_")) {
                corruptedFile = file;
                break;
            }
        }
        assertThat(corruptedFile, notNullValue());
    }

    /**
     * Tests corruption that happens on the network layer and that the primary does not get affected by corruption that happens on the way
     * to the replica. The file on disk stays uncorrupted
     */
    @Test
    public void testCorruptionOnNetworkLayer() throws ExecutionException, InterruptedException {
        int numDocs = scaledRandomIntBetween(100, 1000);
        assertThat(cluster().numDataNodes(), greaterThanOrEqualTo(2));
        if (cluster().numDataNodes() < 3) {
            internalCluster().startNode(ImmutableSettings.builder().put("node.data", true).put("node.client", false).put("node.master", false));
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


        assertAcked(prepareCreate("test").setSettings(ImmutableSettings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, "0")
                .put(InternalEngine.INDEX_FAIL_ON_CORRUPTION, true)
                // This does corrupt files on the replica, so we can't check:
                .put(MockFSDirectoryService.CHECK_INDEX_ON_CLOSE, false)
                .put("index.routing.allocation.include._name", primariesNode.getNode().name())
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
        final boolean truncate = randomBoolean();
        for (NodeStats dataNode : dataNodeStats) {
            MockTransportService mockTransportService = ((MockTransportService) internalCluster().getInstance(TransportService.class, dataNode.getNode().name()));
            mockTransportService.addDelegate(internalCluster().getInstance(Discovery.class, unluckyNode.getNode().name()).localNode(), new MockTransportService.DelegateTransport(mockTransportService.original()) {

                @Override
                public void sendRequest(DiscoveryNode node, long requestId, String action, TransportRequest request, TransportRequestOptions options) throws IOException, TransportException {
                    if (action.equals(RecoveryTarget.Actions.FILE_CHUNK)) {
                        RecoveryFileChunkRequest req = (RecoveryFileChunkRequest) request;
                        if (truncate && req.length() > 1) {
                            BytesArray array = new BytesArray(req.content().array(), req.content().arrayOffset(), (int)req.length()-1);
                            request = new RecoveryFileChunkRequest(req.recoveryId(), req.shardId(), req.metadata(), req.position(), array, req.lastChunk());
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

        Settings build = ImmutableSettings.builder()
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
        for (IndexShardRoutingTable table : clusterStateResponse.getState().routingNodes().getRoutingTable().index("test")) {
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
        assertThat(cluster().numDataNodes(), greaterThanOrEqualTo(2));

        assertAcked(prepareCreate("test").setSettings(ImmutableSettings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, "0") // no replicas for this test
                .put(MergePolicyModule.MERGE_POLICY_TYPE_KEY, NoMergePolicyProvider.class)
                .put(MockFSDirectoryService.CHECK_INDEX_ON_CLOSE, false) // no checkindex - we corrupt shards on purpose
                .put(InternalEngine.INDEX_FAIL_ON_CORRUPTION, true)
                .put(TranslogService.INDEX_TRANSLOG_DISABLE_FLUSH, true) // no translog based flush - it might change the .del / segments.N files
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

        ShardRouting shardRouting = corruptRandomFile(false);
        // we don't corrupt segments.gen since S/R doesn't snapshot this file
        // the other problem here why we can't corrupt segments.X files is that the snapshot flushes again before
        // it snapshots and that will write a new segments.X+1 file
        logger.info("-->  creating repository");
        assertAcked(client().admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(ImmutableSettings.settingsBuilder()
                        .put("location", newTempDir(LifecycleScope.SUITE).getAbsolutePath())
                        .put("compress", randomBoolean())
                        .put("chunk_size", randomIntBetween(100, 1000))));
        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = client().admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(true).setIndices("test").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().state(), equalTo(SnapshotState.PARTIAL));
        logger.info("failed during snapshot -- maybe SI file got corrupted");
        final List<File> files = listShardFiles(shardRouting);
        File corruptedFile = null;
        for (File file : files) {
            if (file.getName().startsWith("corrupted_")) {
                corruptedFile = file;
                break;
            }
        }
        assertThat(corruptedFile, notNullValue());
    }

    private int numShards(String... index) {
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        GroupShardsIterator shardIterators = state.getRoutingNodes().getRoutingTable().activePrimaryShardsGrouped(index, false);
        return shardIterators.size();
    }


    private ShardRouting corruptRandomFile() throws IOException {
        return corruptRandomFile(true);
    }

    private ShardRouting corruptRandomFile(final boolean includePerCommitFiles) throws IOException {
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        GroupShardsIterator shardIterators = state.getRoutingNodes().getRoutingTable().activePrimaryShardsGrouped(new String[]{"test"}, false);
        List<ShardIterator>  iterators = Lists.newArrayList(shardIterators);
        ShardIterator shardIterator = RandomPicks.randomFrom(getRandom(), iterators);
        ShardRouting shardRouting = shardIterator.nextOrNull();
        assertNotNull(shardRouting);
        assertTrue(shardRouting.primary());
        assertTrue(shardRouting.assignedToNode());
        String nodeId = shardRouting.currentNodeId();
        NodesStatsResponse nodeStatses = client().admin().cluster().prepareNodesStats(nodeId).setFs(true).get();
        Set<File> files = new TreeSet<>(); // treeset makes sure iteration order is deterministic
        for (FsStats.Info info : nodeStatses.getNodes()[0].getFs()) {
            String path = info.getPath();
            final String relativeDataLocationPath =  "indices/test/" + Integer.toString(shardRouting.getId()) + "/index";
            File file = new File(path, relativeDataLocationPath);
            files.addAll(Arrays.asList(file.listFiles(new FileFilter() {
                @Override
                public boolean accept(File pathname) {
                    if (pathname.isFile() && "write.lock".equals(pathname.getName()) == false) {
                        return (includePerCommitFiles || isPerSegmentFile(pathname.getName()));
                    }
                    return false; // no dirs no write.locks
                }
            })));
        }
        pruneOldDeleteGenerations(files);
        File fileToCorrupt = null;
        if (!files.isEmpty()) {
            fileToCorrupt = RandomPicks.randomFrom(getRandom(), files);
            try (Directory dir = FSDirectory.open(fileToCorrupt.getParentFile())) {
                long checksumBeforeCorruption;
                try (IndexInput input = dir.openInput(fileToCorrupt.getName(), IOContext.DEFAULT)) {
                    checksumBeforeCorruption = CodecUtil.retrieveChecksum(input);
                }
                try (RandomAccessFile raf = new RandomAccessFile(fileToCorrupt, "rw")) {
                    raf.seek(randomIntBetween(0, (int)Math.min(Integer.MAX_VALUE, raf.length()-1)));
                    long filePointer = raf.getFilePointer();
                    byte b = raf.readByte();
                    raf.seek(filePointer);
                    raf.writeByte(~b);
                    raf.getFD().sync();
                    logger.info("Corrupting file for shard {} --  flipping at position {} from {} to {} file: {}", shardRouting, filePointer, Integer.toHexString(b),  Integer.toHexString(~b), fileToCorrupt.getName());
                }
                long checksumAfterCorruption;
                long actualChecksumAfterCorruption;
                try (ChecksumIndexInput input = dir.openChecksumInput(fileToCorrupt.getName(), IOContext.DEFAULT)) {
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
                msg.append(" file: ").append(fileToCorrupt.getName()).append(" length: ").append(dir.fileLength(fileToCorrupt.getName()));
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
        // .del and segments_N are per commit files and might change after corruption
        return fileName.startsWith("segments") || fileName.endsWith(".del");
    }

    private static final boolean isPerSegmentFile(String fileName) {
        return isPerCommitFile(fileName) == false;
    }

    /**
     * prunes the list of index files such that only the latest del generation files are contained.
     */
    private void pruneOldDeleteGenerations(Set<File> files) {
        final TreeSet<File> delFiles = new TreeSet<>();
        for (File file : files) {
            if (file.getName().endsWith(".del")) {
                delFiles.add(file);
            }
        }
        File last = null;
        for (File current : delFiles) {
            if (last != null) {
                final String newSegmentName = IndexFileNames.parseSegmentName(current.getName());
                final String oldSegmentName = IndexFileNames.parseSegmentName(last.getName());
                if (newSegmentName.equals(oldSegmentName)) {
                    int oldGen = Integer.parseInt(IndexFileNames.stripExtension(IndexFileNames.stripSegmentName(last.getName())).replace("_", ""), Character.MAX_RADIX);
                    int newGen = Integer.parseInt(IndexFileNames.stripExtension(IndexFileNames.stripSegmentName(current.getName())).replace("_", ""), Character.MAX_RADIX);
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

    public List<File> listShardFiles(ShardRouting routing) {
        NodesStatsResponse nodeStatses = client().admin().cluster().prepareNodesStats(routing.currentNodeId()).setFs(true).get();

        assertThat(routing.toString(), nodeStatses.getNodes().length, equalTo(1));
        List<File> files = new ArrayList<>();
        for (FsStats.Info info : nodeStatses.getNodes()[0].getFs()) {
            String path = info.getPath();
            File file = new File(path, "indices/test/" + Integer.toString(routing.getId()) + "/index");
            files.addAll(Arrays.asList(file.listFiles()));
        }
        return files;
    }

    private void disableAllocation(String index) {
        client().admin().indices().prepareUpdateSettings(index).setSettings(ImmutableSettings.builder().put(
                "index.routing.allocation.enable", "none"
        )).get();
    }

    private void enableAllocation(String index) {
        client().admin().indices().prepareUpdateSettings(index).setSettings(ImmutableSettings.builder().put(
                "index.routing.allocation.enable", "all"
        )).get();
    }

    public static class NoMergePolicyProvider  extends AbstractMergePolicyProvider<MergePolicy> {

        @Inject
        public NoMergePolicyProvider(Store store) {
            super(store);
        }

        @Override
        public MergePolicy getMergePolicy() {
            return NoMergePolicy.INSTANCE;
        }

        @Override
        public void close() throws ElasticsearchException {
        }
    }

}
