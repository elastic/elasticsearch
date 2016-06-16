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

package org.elasticsearch.snapshots;

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntSet;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotStatus;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.MetaDataIndexStateService;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.zen.ZenDiscovery;
import org.elasticsearch.discovery.zen.elect.ElectMasterService;
import org.elasticsearch.index.store.IndexStore;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.indices.ttl.IndicesTTLService;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.rest.AbstractRestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.admin.cluster.repositories.get.RestGetRepositoriesAction;
import org.elasticsearch.rest.action.admin.cluster.state.RestClusterStateAction;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.TestCustomMetaData;
import org.elasticsearch.test.rest.FakeRestRequest;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertBlocked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertThrows;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 */
@ClusterScope(scope = Scope.TEST, numDataNodes = 0, transportClientRatio = 0)
@ESIntegTestCase.SuppressLocalMode // TODO only restorePersistentSettingsTest needs this maybe factor out?
public class DedicatedClusterSnapshotRestoreIT extends AbstractSnapshotIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(MockRepository.Plugin.class);
    }

    public void testRestorePersistentSettings() throws Exception {
        logger.info("--> start 2 nodes");
        Settings nodeSettings = Settings.builder()
                .put("discovery.type", "zen")
                .put(ZenDiscovery.PING_TIMEOUT_SETTING.getKey(), "200ms")
                .put("discovery.initial_state_timeout", "500ms")
                .build();
        internalCluster().startNode(nodeSettings);
        Client client = client();
        String secondNode = internalCluster().startNode(nodeSettings);
        logger.info("--> wait for the second node to join the cluster");
        assertThat(client.admin().cluster().prepareHealth().setWaitForNodes("2").get().isTimedOut(), equalTo(false));

        int random = randomIntBetween(10, 42);

        logger.info("--> set test persistent setting");
        client.admin().cluster().prepareUpdateSettings().setPersistentSettings(
                Settings.builder()
                        .put(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey(), 2)
                        .put(IndicesTTLService.INDICES_TTL_INTERVAL_SETTING.getKey(), random, TimeUnit.MINUTES))
                .execute().actionGet();

        assertThat(client.admin().cluster().prepareState().setRoutingTable(false).setNodes(false).execute().actionGet().getState()
                .getMetaData().persistentSettings().getAsTime(IndicesTTLService.INDICES_TTL_INTERVAL_SETTING.getKey(), TimeValue.timeValueMinutes(1)).millis(), equalTo(TimeValue.timeValueMinutes(random).millis()));
        assertThat(client.admin().cluster().prepareState().setRoutingTable(false).setNodes(false).execute().actionGet().getState()
                .getMetaData().persistentSettings().getAsInt(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey(), -1), equalTo(2));

        logger.info("--> create repository");
        PutRepositoryResponse putRepositoryResponse = client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.builder().put("location", randomRepoPath())).execute().actionGet();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));

        logger.info("--> start snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(true).execute().actionGet();
        assertThat(createSnapshotResponse.getSnapshotInfo().totalShards(), equalTo(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(0));
        assertThat(client.admin().cluster().prepareGetSnapshots("test-repo").setSnapshots("test-snap").execute().actionGet().getSnapshots().get(0).state(), equalTo(SnapshotState.SUCCESS));

        logger.info("--> clean the test persistent setting");
        client.admin().cluster().prepareUpdateSettings().setPersistentSettings(
                Settings.builder()
                        .put(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey(), 1)
                        .put(IndicesTTLService.INDICES_TTL_INTERVAL_SETTING.getKey(), TimeValue.timeValueMinutes(1)))
                .execute().actionGet();
        assertThat(client.admin().cluster().prepareState().setRoutingTable(false).setNodes(false).execute().actionGet().getState()
                .getMetaData().persistentSettings().getAsTime(IndicesTTLService.INDICES_TTL_INTERVAL_SETTING.getKey(), TimeValue.timeValueMinutes(1)).millis(), equalTo(TimeValue.timeValueMinutes(1).millis()));

        stopNode(secondNode);
        assertThat(client.admin().cluster().prepareHealth().setWaitForNodes("1").get().isTimedOut(), equalTo(false));

        logger.info("--> restore snapshot");
        try {
            client.admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap").setRestoreGlobalState(true).setWaitForCompletion(true).execute().actionGet();
            fail("can't restore minimum master nodes");
        } catch (IllegalArgumentException ex) {
            assertEquals("illegal value can't update [discovery.zen.minimum_master_nodes] from [1] to [2]", ex.getMessage());
            assertEquals("cannot set discovery.zen.minimum_master_nodes to more than the current master nodes count [1]", ex.getCause().getMessage());
        }
        logger.info("--> ensure that zen discovery minimum master nodes wasn't restored");
        assertThat(client.admin().cluster().prepareState().setRoutingTable(false).setNodes(false).execute().actionGet().getState()
                .getMetaData().persistentSettings().getAsInt(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey(), -1), not(equalTo(2)));
    }

    public void testRestoreCustomMetadata() throws Exception {
        Path tempDir = randomRepoPath();

        logger.info("--> start node");
        internalCluster().startNode();
        Client client = client();
        createIndex("test-idx");
        ensureYellow();
        logger.info("--> add custom persistent metadata");
        updateClusterState(new ClusterStateUpdater() {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                ClusterState.Builder builder = ClusterState.builder(currentState);
                MetaData.Builder metadataBuilder = MetaData.builder(currentState.metaData());
                metadataBuilder.putCustom(SnapshottableMetadata.TYPE, new SnapshottableMetadata("before_snapshot_s"));
                metadataBuilder.putCustom(NonSnapshottableMetadata.TYPE, new NonSnapshottableMetadata("before_snapshot_ns"));
                metadataBuilder.putCustom(SnapshottableGatewayMetadata.TYPE, new SnapshottableGatewayMetadata("before_snapshot_s_gw"));
                metadataBuilder.putCustom(NonSnapshottableGatewayMetadata.TYPE, new NonSnapshottableGatewayMetadata("before_snapshot_ns_gw"));
                metadataBuilder.putCustom(SnapshotableGatewayNoApiMetadata.TYPE, new SnapshotableGatewayNoApiMetadata("before_snapshot_s_gw_noapi"));
                builder.metaData(metadataBuilder);
                return builder.build();
            }
        });

        logger.info("--> create repository");
        PutRepositoryResponse putRepositoryResponse = client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.builder().put("location", tempDir)).execute().actionGet();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));

        logger.info("--> start snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(true).execute().actionGet();
        assertThat(createSnapshotResponse.getSnapshotInfo().totalShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(createSnapshotResponse.getSnapshotInfo().successfulShards()));
        assertThat(client.admin().cluster().prepareGetSnapshots("test-repo").setSnapshots("test-snap").execute().actionGet().getSnapshots().get(0).state(), equalTo(SnapshotState.SUCCESS));

        logger.info("--> change custom persistent metadata");
        updateClusterState(new ClusterStateUpdater() {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                ClusterState.Builder builder = ClusterState.builder(currentState);
                MetaData.Builder metadataBuilder = MetaData.builder(currentState.metaData());
                if (randomBoolean()) {
                    metadataBuilder.putCustom(SnapshottableMetadata.TYPE, new SnapshottableMetadata("after_snapshot_s"));
                } else {
                    metadataBuilder.removeCustom(SnapshottableMetadata.TYPE);
                }
                metadataBuilder.putCustom(NonSnapshottableMetadata.TYPE, new NonSnapshottableMetadata("after_snapshot_ns"));
                if (randomBoolean()) {
                    metadataBuilder.putCustom(SnapshottableGatewayMetadata.TYPE, new SnapshottableGatewayMetadata("after_snapshot_s_gw"));
                } else {
                    metadataBuilder.removeCustom(SnapshottableGatewayMetadata.TYPE);
                }
                metadataBuilder.putCustom(NonSnapshottableGatewayMetadata.TYPE, new NonSnapshottableGatewayMetadata("after_snapshot_ns_gw"));
                metadataBuilder.removeCustom(SnapshotableGatewayNoApiMetadata.TYPE);
                builder.metaData(metadataBuilder);
                return builder.build();
            }
        });

        logger.info("--> delete repository");
        assertAcked(client.admin().cluster().prepareDeleteRepository("test-repo"));

        logger.info("--> create repository");
        putRepositoryResponse = client.admin().cluster().preparePutRepository("test-repo-2")
                .setType("fs").setSettings(Settings.builder().put("location", tempDir)).execute().actionGet();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));

        logger.info("--> restore snapshot");
        client.admin().cluster().prepareRestoreSnapshot("test-repo-2", "test-snap").setRestoreGlobalState(true).setIndices("-*").setWaitForCompletion(true).execute().actionGet();

        logger.info("--> make sure old repository wasn't restored");
        assertThrows(client.admin().cluster().prepareGetRepositories("test-repo"), RepositoryMissingException.class);
        assertThat(client.admin().cluster().prepareGetRepositories("test-repo-2").get().repositories().size(), equalTo(1));

        logger.info("--> check that custom persistent metadata was restored");
        ClusterState clusterState = client.admin().cluster().prepareState().get().getState();
        logger.info("Cluster state: {}", clusterState);
        MetaData metaData = clusterState.getMetaData();
        assertThat(((SnapshottableMetadata) metaData.custom(SnapshottableMetadata.TYPE)).getData(), equalTo("before_snapshot_s"));
        assertThat(((NonSnapshottableMetadata) metaData.custom(NonSnapshottableMetadata.TYPE)).getData(), equalTo("after_snapshot_ns"));
        assertThat(((SnapshottableGatewayMetadata) metaData.custom(SnapshottableGatewayMetadata.TYPE)).getData(), equalTo("before_snapshot_s_gw"));
        assertThat(((NonSnapshottableGatewayMetadata) metaData.custom(NonSnapshottableGatewayMetadata.TYPE)).getData(), equalTo("after_snapshot_ns_gw"));

        logger.info("--> restart all nodes");
        internalCluster().fullRestart();
        ensureYellow();

        logger.info("--> check that gateway-persistent custom metadata survived full cluster restart");
        clusterState = client().admin().cluster().prepareState().get().getState();
        logger.info("Cluster state: {}", clusterState);
        metaData = clusterState.getMetaData();
        assertThat(metaData.custom(SnapshottableMetadata.TYPE), nullValue());
        assertThat(metaData.custom(NonSnapshottableMetadata.TYPE), nullValue());
        assertThat(((SnapshottableGatewayMetadata) metaData.custom(SnapshottableGatewayMetadata.TYPE)).getData(), equalTo("before_snapshot_s_gw"));
        assertThat(((NonSnapshottableGatewayMetadata) metaData.custom(NonSnapshottableGatewayMetadata.TYPE)).getData(), equalTo("after_snapshot_ns_gw"));
        // Shouldn't be returned as part of API response
        assertThat(metaData.custom(SnapshotableGatewayNoApiMetadata.TYPE), nullValue());
        // But should still be in state
        metaData = internalCluster().getInstance(ClusterService.class).state().metaData();
        assertThat(((SnapshotableGatewayNoApiMetadata) metaData.custom(SnapshotableGatewayNoApiMetadata.TYPE)).getData(), equalTo("before_snapshot_s_gw_noapi"));
    }

    private void updateClusterState(final ClusterStateUpdater updater) throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final ClusterService clusterService = internalCluster().getInstance(ClusterService.class);
        clusterService.submitStateUpdateTask("test", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                return updater.execute(currentState);
            }

            @Override
            public void onFailure(String source, @Nullable Throwable t) {
                countDownLatch.countDown();
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                countDownLatch.countDown();
            }
        });
        countDownLatch.await();
    }

    private static interface ClusterStateUpdater {
        public ClusterState execute(ClusterState currentState) throws Exception;
    }

    public void testSnapshotDuringNodeShutdown() throws Exception {
        logger.info("--> start 2 nodes");
        Client client = client();

        assertAcked(prepareCreate("test-idx", 2, Settings.builder().put("number_of_shards", 2).put("number_of_replicas", 0)));
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            index("test-idx", "doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();
        assertThat(client.prepareSearch("test-idx").setSize(0).get().getHits().totalHits(), equalTo(100L));

        logger.info("--> create repository");
        logger.info("--> creating repository");
        PutRepositoryResponse putRepositoryResponse = client.admin().cluster().preparePutRepository("test-repo")
                .setType("mock").setSettings(
                        Settings.builder()
                                .put("location", randomRepoPath())
                                .put("random", randomAsciiOfLength(10))
                                .put("wait_after_unblock", 200)
                ).get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));

        // Pick one node and block it
        String blockedNode = blockNodeWithIndex("test-repo", "test-idx");

        logger.info("--> snapshot");
        client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(false).setIndices("test-idx").get();

        logger.info("--> waiting for block to kick in");
        waitForBlock(blockedNode, "test-repo", TimeValue.timeValueSeconds(60));

        logger.info("--> execution was blocked on node [{}], shutting it down", blockedNode);
        unblockNode("test-repo", blockedNode);

        logger.info("--> stopping node [{}]", blockedNode);
        stopNode(blockedNode);
        logger.info("--> waiting for completion");
        SnapshotInfo snapshotInfo = waitForCompletion("test-repo", "test-snap", TimeValue.timeValueSeconds(60));
        logger.info("Number of failed shards [{}]", snapshotInfo.shardFailures().size());
        logger.info("--> done");
    }

    public void testSnapshotWithStuckNode() throws Exception {
        logger.info("--> start 2 nodes");
        ArrayList<String> nodes = new ArrayList<>();
        nodes.add(internalCluster().startNode());
        nodes.add(internalCluster().startNode());
        Client client = client();

        assertAcked(prepareCreate("test-idx", 2, Settings.builder().put("number_of_shards", 2).put("number_of_replicas", 0)));
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            index("test-idx", "doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();
        assertThat(client.prepareSearch("test-idx").setSize(0).get().getHits().totalHits(), equalTo(100L));

        logger.info("--> creating repository");
        Path repo = randomRepoPath();
        PutRepositoryResponse putRepositoryResponse = client.admin().cluster().preparePutRepository("test-repo")
                .setType("mock").setSettings(
                        Settings.builder()
                                .put("location", repo)
                                .put("random", randomAsciiOfLength(10))
                                .put("wait_after_unblock", 200)
                ).get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));

        // Pick one node and block it
        String blockedNode = blockNodeWithIndex("test-repo", "test-idx");
        // Remove it from the list of available nodes
        nodes.remove(blockedNode);

        int numberOfFilesBeforeSnapshot = numberOfFiles(repo);
        logger.info("--> snapshot");
        client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(false).setIndices("test-idx").get();

        logger.info("--> waiting for block to kick in");
        waitForBlock(blockedNode, "test-repo", TimeValue.timeValueSeconds(60));

        logger.info("--> execution was blocked on node [{}], aborting snapshot", blockedNode);

        ListenableActionFuture<DeleteSnapshotResponse> deleteSnapshotResponseFuture = internalCluster().client(nodes.get(0)).admin().cluster().prepareDeleteSnapshot("test-repo", "test-snap").execute();
        // Make sure that abort makes some progress
        Thread.sleep(100);
        unblockNode("test-repo", blockedNode);
        logger.info("--> stopping node [{}]", blockedNode);
        stopNode(blockedNode);
        try {
            DeleteSnapshotResponse deleteSnapshotResponse = deleteSnapshotResponseFuture.actionGet();
            assertThat(deleteSnapshotResponse.isAcknowledged(), equalTo(true));
        } catch (SnapshotMissingException ex) {
            // When master node is closed during this test, it sometime manages to delete the snapshot files before
            // completely stopping. In this case the retried delete snapshot operation on the new master can fail
            // with SnapshotMissingException
        }

        logger.info("--> making sure that snapshot no longer exists");
        assertThrows(client().admin().cluster().prepareGetSnapshots("test-repo").setSnapshots("test-snap").execute(), SnapshotMissingException.class);
        // Subtract index file from the count
        assertThat("not all files were deleted during snapshot cancellation", numberOfFilesBeforeSnapshot, equalTo(numberOfFiles(repo) - 1));
        logger.info("--> done");
    }

    public void testRestoreIndexWithMissingShards() throws Exception {
        logger.info("--> start 2 nodes");
        internalCluster().startNode();
        internalCluster().startNode();
        cluster().wipeIndices("_all");

        logger.info("--> create an index that will have some unallocated shards");
        assertAcked(prepareCreate("test-idx-some", 2, Settings.builder().put("number_of_shards", 6)
                .put("number_of_replicas", 0)));
        ensureGreen();

        logger.info("--> indexing some data into test-idx-some");
        for (int i = 0; i < 100; i++) {
            index("test-idx-some", "doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();
        assertThat(client().prepareSearch("test-idx-some").setSize(0).get().getHits().totalHits(), equalTo(100L));

        logger.info("--> shutdown one of the nodes");
        internalCluster().stopRandomDataNode();
        assertThat(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setTimeout("1m").setWaitForNodes("<2").execute().actionGet().isTimedOut(), equalTo(false));

        logger.info("--> create an index that will have all allocated shards");
        assertAcked(prepareCreate("test-idx-all", 1, Settings.builder().put("number_of_shards", 6)
                .put("number_of_replicas", 0)));
        ensureGreen("test-idx-all");

        logger.info("--> create an index that will be closed");
        assertAcked(prepareCreate("test-idx-closed", 1, Settings.builder().put("number_of_shards", 4).put("number_of_replicas", 0)));
        ensureGreen("test-idx-closed");

        logger.info("--> indexing some data into test-idx-all");
        for (int i = 0; i < 100; i++) {
            index("test-idx-all", "doc", Integer.toString(i), "foo", "bar" + i);
            index("test-idx-closed", "doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh("test-idx-closed", "test-idx-all"); // don't refresh test-idx-some it will take 30 sec until it times out...
        assertThat(client().prepareSearch("test-idx-all").setSize(0).get().getHits().totalHits(), equalTo(100L));
        assertAcked(client().admin().indices().prepareClose("test-idx-closed"));

        logger.info("--> create an index that will have no allocated shards");
        assertAcked(prepareCreate("test-idx-none", 1, Settings.builder().put("number_of_shards", 6)
                .put("index.routing.allocation.include.tag", "nowhere")
                .put("number_of_replicas", 0)));

        logger.info("--> create repository");
        logger.info("--> creating repository");
        PutRepositoryResponse putRepositoryResponse = client().admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.builder().put("location", randomRepoPath())).execute().actionGet();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));

        logger.info("--> start snapshot with default settings and closed index - should be blocked");
        assertBlocked(client().admin().cluster().prepareCreateSnapshot("test-repo", "test-snap-1")
                .setIndices("test-idx-all", "test-idx-none", "test-idx-some", "test-idx-closed")
                .setWaitForCompletion(true), MetaDataIndexStateService.INDEX_CLOSED_BLOCK);


        logger.info("--> start snapshot with default settings without a closed index - should fail");
        CreateSnapshotResponse createSnapshotResponse = client().admin().cluster().prepareCreateSnapshot("test-repo", "test-snap-1")
                .setIndices("test-idx-all", "test-idx-none", "test-idx-some")
                .setWaitForCompletion(true).execute().actionGet();
        assertThat(createSnapshotResponse.getSnapshotInfo().state(), equalTo(SnapshotState.FAILED));
        assertThat(createSnapshotResponse.getSnapshotInfo().reason(), containsString("Indices don't have primary shards"));

        if (randomBoolean()) {
            logger.info("checking snapshot completion using status");
            client().admin().cluster().prepareCreateSnapshot("test-repo", "test-snap-2")
                    .setIndices("test-idx-all", "test-idx-none", "test-idx-some")
                    .setWaitForCompletion(false).setPartial(true).execute().actionGet();
            assertBusy(new Runnable() {
                @Override
                public void run() {
                    SnapshotsStatusResponse snapshotsStatusResponse = client().admin().cluster().prepareSnapshotStatus("test-repo").setSnapshots("test-snap-2").get();
                    List<SnapshotStatus> snapshotStatuses = snapshotsStatusResponse.getSnapshots();
                    assertEquals(snapshotStatuses.size(), 1);
                    logger.trace("current snapshot status [{}]", snapshotStatuses.get(0));
                    assertTrue(snapshotStatuses.get(0).getState().completed());
                }
            }, 1, TimeUnit.MINUTES);
            SnapshotsStatusResponse snapshotsStatusResponse = client().admin().cluster().prepareSnapshotStatus("test-repo").setSnapshots("test-snap-2").get();
            List<SnapshotStatus> snapshotStatuses = snapshotsStatusResponse.getSnapshots();
            assertThat(snapshotStatuses.size(), equalTo(1));
            SnapshotStatus snapshotStatus = snapshotStatuses.get(0);
            logger.info("State: [{}], Reason: [{}]", createSnapshotResponse.getSnapshotInfo().state(), createSnapshotResponse.getSnapshotInfo().reason());
            assertThat(snapshotStatus.getShardsStats().getTotalShards(), equalTo(18));
            assertThat(snapshotStatus.getShardsStats().getDoneShards(), lessThan(12));
            assertThat(snapshotStatus.getShardsStats().getDoneShards(), greaterThan(6));

            // There is slight delay between snapshot being marked as completed in the cluster state and on the file system
            // After it was marked as completed in the cluster state - we need to check if it's completed on the file system as well
            assertBusy(new Runnable() {
                @Override
                public void run() {
                    GetSnapshotsResponse response = client().admin().cluster().prepareGetSnapshots("test-repo").setSnapshots("test-snap-2").get();
                    assertThat(response.getSnapshots().size(), equalTo(1));
                    SnapshotInfo snapshotInfo = response.getSnapshots().get(0);
                    assertTrue(snapshotInfo.state().completed());
                    assertEquals(SnapshotState.PARTIAL, snapshotInfo.state());
                }
            }, 1, TimeUnit.MINUTES);
        } else {
            logger.info("checking snapshot completion using wait_for_completion flag");
            createSnapshotResponse = client().admin().cluster().prepareCreateSnapshot("test-repo", "test-snap-2")
                    .setIndices("test-idx-all", "test-idx-none", "test-idx-some")
                    .setWaitForCompletion(true).setPartial(true).execute().actionGet();
            logger.info("State: [{}], Reason: [{}]", createSnapshotResponse.getSnapshotInfo().state(), createSnapshotResponse.getSnapshotInfo().reason());
            assertThat(createSnapshotResponse.getSnapshotInfo().totalShards(), equalTo(18));
            assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), lessThan(12));
            assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(6));
            assertThat(client().admin().cluster().prepareGetSnapshots("test-repo").setSnapshots("test-snap-2").execute().actionGet().getSnapshots().get(0).state(), equalTo(SnapshotState.PARTIAL));
        }

        assertAcked(client().admin().indices().prepareClose("test-idx-some", "test-idx-all").execute().actionGet());

        logger.info("--> restore incomplete snapshot - should fail");
        assertThrows(client().admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap-2").setRestoreGlobalState(false).setWaitForCompletion(true).execute(), SnapshotRestoreException.class);

        logger.info("--> restore snapshot for the index that was snapshotted completely");
        RestoreSnapshotResponse restoreSnapshotResponse = client().admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap-2").setRestoreGlobalState(false).setIndices("test-idx-all").setWaitForCompletion(true).execute().actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo(), notNullValue());
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), equalTo(6));
        assertThat(restoreSnapshotResponse.getRestoreInfo().successfulShards(), equalTo(6));
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));

        assertThat(client().prepareSearch("test-idx-all").setSize(0).get().getHits().totalHits(), equalTo(100L));

        logger.info("--> restore snapshot for the partial index");
        cluster().wipeIndices("test-idx-some");
        restoreSnapshotResponse = client().admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap-2")
                .setRestoreGlobalState(false).setIndices("test-idx-some").setPartial(true).setWaitForCompletion(true).get();
        assertThat(restoreSnapshotResponse.getRestoreInfo(), notNullValue());
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), equalTo(6));
        assertThat(restoreSnapshotResponse.getRestoreInfo().successfulShards(), allOf(greaterThan(0), lessThan(6)));
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), greaterThan(0));

        assertThat(client().prepareSearch("test-idx-some").setSize(0).get().getHits().totalHits(), allOf(greaterThan(0L), lessThan(100L)));

        logger.info("--> restore snapshot for the index that didn't have any shards snapshotted successfully");
        cluster().wipeIndices("test-idx-none");
        restoreSnapshotResponse = client().admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap-2")
                .setRestoreGlobalState(false).setIndices("test-idx-none").setPartial(true).setWaitForCompletion(true).get();
        assertThat(restoreSnapshotResponse.getRestoreInfo(), notNullValue());
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), equalTo(6));
        assertThat(restoreSnapshotResponse.getRestoreInfo().successfulShards(), equalTo(0));
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(6));

        assertThat(client().prepareSearch("test-idx-some").setSize(0).get().getHits().totalHits(), allOf(greaterThan(0L), lessThan(100L)));
    }

    public void testRestoreIndexWithShardsMissingInLocalGateway() throws Exception {
        logger.info("--> start 2 nodes");
        Settings nodeSettings = Settings.builder()
                .put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Rebalance.NONE)
                .build();

        internalCluster().startNode(nodeSettings);
        internalCluster().startNode(nodeSettings);
        cluster().wipeIndices("_all");

        logger.info("--> create repository");
        PutRepositoryResponse putRepositoryResponse = client().admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.builder().put("location", randomRepoPath())).execute().actionGet();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));
        int numberOfShards = 6;
        logger.info("--> create an index that will have some unallocated shards");
        assertAcked(prepareCreate("test-idx", 2, Settings.builder().put("number_of_shards", numberOfShards)
                .put("number_of_replicas", 0)));
        ensureGreen();

        logger.info("--> indexing some data into test-idx");
        for (int i = 0; i < 100; i++) {
            index("test-idx", "doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();
        assertThat(client().prepareSearch("test-idx").setSize(0).get().getHits().totalHits(), equalTo(100L));

        logger.info("--> start snapshot");
        assertThat(client().admin().cluster().prepareCreateSnapshot("test-repo", "test-snap-1").setIndices("test-idx").setWaitForCompletion(true).get().getSnapshotInfo().state(), equalTo(SnapshotState.SUCCESS));

        logger.info("--> close the index");
        assertAcked(client().admin().indices().prepareClose("test-idx"));

        logger.info("--> shutdown one of the nodes that should make half of the shards unavailable");
        internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {
            @Override
            public boolean clearData(String nodeName) {
                return true;
            }
        });

        assertThat(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setTimeout("1m").setWaitForNodes("2").execute().actionGet().isTimedOut(), equalTo(false));

        logger.info("--> restore index snapshot");
        assertThat(client().admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap-1").setRestoreGlobalState(false).setWaitForCompletion(true).get().getRestoreInfo().successfulShards(), equalTo(6));

        ensureGreen("test-idx");
        assertThat(client().prepareSearch("test-idx").setSize(0).get().getHits().totalHits(), equalTo(100L));

        IntSet reusedShards = new IntHashSet();
        for (RecoveryState recoveryState : client().admin().indices().prepareRecoveries("test-idx").get().shardRecoveryStates().get("test-idx")) {
            if (recoveryState.getIndex().reusedBytes() > 0) {
                reusedShards.add(recoveryState.getShardId().getId());
            }
        }
        logger.info("--> check that at least half of the shards had some reuse: [{}]", reusedShards);
        assertThat(reusedShards.size(), greaterThanOrEqualTo(numberOfShards / 2));
    }

    public void testRegistrationFailure() {
        logger.info("--> start first node");
        internalCluster().startNode();
        logger.info("--> start second node");
        // Make sure the first node is elected as master
        internalCluster().startNode(Settings.builder().put(Node.NODE_MASTER_SETTING.getKey(), false));
        // Register mock repositories
        for (int i = 0; i < 5; i++) {
            client().admin().cluster().preparePutRepository("test-repo" + i)
                    .setType("mock").setSettings(Settings.builder()
                    .put("location", randomRepoPath())).setVerify(false).get();
        }
        logger.info("--> make sure that properly setup repository can be registered on all nodes");
        client().admin().cluster().preparePutRepository("test-repo-0")
                .setType("fs").setSettings(Settings.builder()
                .put("location", randomRepoPath())).get();

    }

    public void testThatSensitiveRepositorySettingsAreNotExposed() throws Exception {
        Settings nodeSettings = Settings.builder().put().build();
        logger.info("--> start two nodes");
        internalCluster().startNodesAsync(2, nodeSettings).get();
        // Register mock repositories
        client().admin().cluster().preparePutRepository("test-repo")
                .setType("mock").setSettings(Settings.builder()
                        .put("location", randomRepoPath())
                        .put(MockRepository.Plugin.USERNAME_SETTING.getKey(), "notsecretusername")
                        .put(MockRepository.Plugin.PASSWORD_SETTING.getKey(), "verysecretpassword")
        ).get();

        RestGetRepositoriesAction getRepoAction = internalCluster().getInstance(RestGetRepositoriesAction.class);
        RestRequest getRepoRequest = new FakeRestRequest();
        getRepoRequest.params().put("repository", "test-repo");
        final CountDownLatch getRepoLatch = new CountDownLatch(1);
        final AtomicReference<AssertionError> getRepoError = new AtomicReference<>();
        getRepoAction.handleRequest(getRepoRequest, new AbstractRestChannel(getRepoRequest, true) {
            @Override
            public void sendResponse(RestResponse response) {
                try {
                    assertThat(response.content().toUtf8(), containsString("notsecretusername"));
                    assertThat(response.content().toUtf8(), not(containsString("verysecretpassword")));
                } catch (AssertionError ex) {
                    getRepoError.set(ex);
                }
                getRepoLatch.countDown();
            }
        });
        assertTrue(getRepoLatch.await(1, TimeUnit.SECONDS));
        if (getRepoError.get() != null) {
            throw getRepoError.get();
        }

        RestClusterStateAction clusterStateAction = internalCluster().getInstance(RestClusterStateAction.class);
        RestRequest clusterStateRequest = new FakeRestRequest();
        final CountDownLatch clusterStateLatch = new CountDownLatch(1);
        final AtomicReference<AssertionError> clusterStateError = new AtomicReference<>();
        clusterStateAction.handleRequest(clusterStateRequest, new AbstractRestChannel(clusterStateRequest, true) {
            @Override
            public void sendResponse(RestResponse response) {
                try {
                    assertThat(response.content().toUtf8(), containsString("notsecretusername"));
                    assertThat(response.content().toUtf8(), not(containsString("verysecretpassword")));
                } catch (AssertionError ex) {
                    clusterStateError.set(ex);
                }
                clusterStateLatch.countDown();
            }
        });
        assertTrue(clusterStateLatch.await(1, TimeUnit.SECONDS));
        if (clusterStateError.get() != null) {
            throw clusterStateError.get();
        }

    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/12621")
    public void testChaosSnapshot() throws Exception {
        final List<String> indices = new CopyOnWriteArrayList<>();
        Settings settings = Settings.builder().put("action.write_consistency", "one").build();
        int initialNodes = between(1, 3);
        logger.info("--> start {} nodes", initialNodes);
        for (int i = 0; i < initialNodes; i++) {
            internalCluster().startNode(settings);
        }

        logger.info("-->  creating repository");
        assertAcked(client().admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.builder()
                        .put("location", randomRepoPath())
                        .put("compress", randomBoolean())
                        .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));

        int initialIndices = between(1, 3);
        logger.info("--> create {} indices", initialIndices);
        for (int i = 0; i < initialIndices; i++) {
            createTestIndex("test-" + i);
            indices.add("test-" + i);
        }

        int asyncNodes = between(0, 5);
        logger.info("--> start {} additional nodes asynchronously", asyncNodes);
        InternalTestCluster.Async<List<String>> asyncNodesFuture = internalCluster().startNodesAsync(asyncNodes, settings);

        int asyncIndices = between(0, 10);
        logger.info("--> create {} additional indices asynchronously", asyncIndices);
        Thread[] asyncIndexThreads = new Thread[asyncIndices];
        for (int i = 0; i < asyncIndices; i++) {
            final int cur = i;
            asyncIndexThreads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    createTestIndex("test-async-" + cur);
                    indices.add("test-async-" + cur);

                }
            });
            asyncIndexThreads[i].start();
        }

        logger.info("--> snapshot");

        ListenableActionFuture<CreateSnapshotResponse> snapshotResponseFuture = client().admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(true).setIndices("test-*").setPartial(true).execute();

        long start = System.currentTimeMillis();
        // Produce chaos for 30 sec or until snapshot is done whatever comes first
        int randomIndices = 0;
        while (System.currentTimeMillis() - start < 30000 && !snapshotIsDone("test-repo", "test-snap")) {
            Thread.sleep(100);
            int chaosType = randomInt(10);
            if (chaosType < 4) {
                // Randomly delete an index
                if (indices.size() > 0) {
                    String index = indices.remove(randomInt(indices.size() - 1));
                    logger.info("--> deleting random index [{}]", index);
                    internalCluster().wipeIndices(index);
                }
            } else if (chaosType < 6) {
                // Randomly shutdown a node
                if (cluster().size() > 1) {
                    logger.info("--> shutting down random node");
                    internalCluster().stopRandomDataNode();
                }
            } else if (chaosType < 8) {
                // Randomly create an index
                String index = "test-rand-" + randomIndices;
                logger.info("--> creating random index [{}]", index);
                createTestIndex(index);
                randomIndices++;
            } else {
                // Take a break
                logger.info("--> noop");
            }
        }

        logger.info("--> waiting for async indices creation to finish");
        for (int i = 0; i < asyncIndices; i++) {
            asyncIndexThreads[i].join();
        }

        logger.info("--> update index settings to back to normal");
        assertAcked(client().admin().indices().prepareUpdateSettings("test-*").setSettings(Settings.builder()
                        .put(IndexStore.INDEX_STORE_THROTTLE_TYPE_SETTING.getKey(), "node")
        ));

        // Make sure that snapshot finished - doesn't matter if it failed or succeeded
        try {
            CreateSnapshotResponse snapshotResponse = snapshotResponseFuture.get();
            SnapshotInfo snapshotInfo = snapshotResponse.getSnapshotInfo();
            assertNotNull(snapshotInfo);
            logger.info("--> snapshot is done with state [{}], total shards [{}], successful shards [{}]", snapshotInfo.state(), snapshotInfo.totalShards(), snapshotInfo.successfulShards());
        } catch (Exception ex) {
            logger.info("--> snapshot didn't start properly", ex);
        }

        asyncNodesFuture.get();
        logger.info("--> done");
    }

    public void testMasterShutdownDuringSnapshot() throws Exception {
        Settings masterSettings = Settings.builder().put(Node.NODE_DATA_SETTING.getKey(), false).build();
        Settings dataSettings = Settings.builder().put(Node.NODE_MASTER_SETTING.getKey(), false).build();

        logger.info("-->  starting two master nodes and two data nodes");
        internalCluster().startNode(masterSettings);
        internalCluster().startNode(masterSettings);
        internalCluster().startNode(dataSettings);
        internalCluster().startNode(dataSettings);

        final Client client = client();

        logger.info("-->  creating repository");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.builder()
                        .put("location", randomRepoPath())
                        .put("compress", randomBoolean())
                        .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));

        assertAcked(prepareCreate("test-idx", 0, Settings.builder().put("number_of_shards", between(1, 20))
                .put("number_of_replicas", 0)));
        ensureGreen();

        logger.info("--> indexing some data");
        final int numdocs = randomIntBetween(10, 100);
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numdocs];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex("test-idx", "type1", Integer.toString(i)).setSource("field1", "bar " + i);
        }
        indexRandom(true, builders);
        flushAndRefresh();

        final int numberOfShards = getNumShards("test-idx").numPrimaries;
        logger.info("number of shards: {}", numberOfShards);

        final ClusterService clusterService = internalCluster().clusterService(internalCluster().getMasterName());
        BlockingClusterStateListener snapshotListener = new BlockingClusterStateListener(clusterService, "update_snapshot [", "update snapshot state", Priority.HIGH);
        try {
            clusterService.addFirst(snapshotListener);
            logger.info("--> snapshot");
            dataNodeClient().admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(false).setIndices("test-idx").get();

            // Await until some updates are in pending state.
            assertBusyPendingTasks("update snapshot state", 1);

            logger.info("--> stopping master node");
            internalCluster().stopCurrentMasterNode();

            logger.info("--> unblocking snapshot execution");
            snapshotListener.unblock();

        } finally {
            clusterService.remove(snapshotListener);
        }

        logger.info("--> wait until the snapshot is done");

        assertBusy(new Runnable() {
            @Override
            public void run() {
                GetSnapshotsResponse snapshotsStatusResponse = client().admin().cluster().prepareGetSnapshots("test-repo").setSnapshots("test-snap").get();
                SnapshotInfo snapshotInfo = snapshotsStatusResponse.getSnapshots().get(0);
                assertTrue(snapshotInfo.state().completed());
            }
        }, 1, TimeUnit.MINUTES);

        logger.info("--> verify that snapshot was succesful");

        GetSnapshotsResponse snapshotsStatusResponse = client().admin().cluster().prepareGetSnapshots("test-repo").setSnapshots("test-snap").get();
        SnapshotInfo snapshotInfo = snapshotsStatusResponse.getSnapshots().get(0);
        assertEquals(SnapshotState.SUCCESS, snapshotInfo.state());
        assertEquals(snapshotInfo.totalShards(), snapshotInfo.successfulShards());
        assertEquals(0, snapshotInfo.failedShards());
    }


    private boolean snapshotIsDone(String repository, String snapshot) {
        try {
            SnapshotsStatusResponse snapshotsStatusResponse = client().admin().cluster().prepareSnapshotStatus(repository).setSnapshots(snapshot).get();
            if (snapshotsStatusResponse.getSnapshots().isEmpty()) {
                return false;
            }
            for (SnapshotStatus snapshotStatus : snapshotsStatusResponse.getSnapshots()) {
                if (snapshotStatus.getState().completed()) {
                    return true;
                }
            }
            return false;
        } catch (SnapshotMissingException ex) {
            return false;
        }
    }

    private void createTestIndex(String name) {
        assertAcked(prepareCreate(name, 0, Settings.builder().put("number_of_shards", between(1, 6))
                .put("number_of_replicas", between(1, 6))));

        ensureYellow(name);

        logger.info("--> indexing some data into {}", name);
        for (int i = 0; i < between(10, 500); i++) {
            index(name, "doc", Integer.toString(i), "foo", "bar" + i);
        }

        assertAcked(client().admin().indices().prepareUpdateSettings(name).setSettings(Settings.builder()
                        .put(IndexStore.INDEX_STORE_THROTTLE_TYPE_SETTING.getKey(), "all")
                        .put(IndexStore.INDEX_STORE_THROTTLE_MAX_BYTES_PER_SEC_SETTING.getKey(), between(100, 50000))
        ));
    }

    static {
        MetaData.registerPrototype(SnapshottableMetadata.TYPE, SnapshottableMetadata.PROTO);
        MetaData.registerPrototype(NonSnapshottableMetadata.TYPE, NonSnapshottableMetadata.PROTO);
        MetaData.registerPrototype(SnapshottableGatewayMetadata.TYPE, SnapshottableGatewayMetadata.PROTO);
        MetaData.registerPrototype(NonSnapshottableGatewayMetadata.TYPE, NonSnapshottableGatewayMetadata.PROTO);
        MetaData.registerPrototype(SnapshotableGatewayNoApiMetadata.TYPE, SnapshotableGatewayNoApiMetadata.PROTO);
    }

    public static class SnapshottableMetadata extends TestCustomMetaData {
        public static final String TYPE = "test_snapshottable";

        public static final SnapshottableMetadata PROTO = new SnapshottableMetadata("");

        public SnapshottableMetadata(String data) {
            super(data);
        }

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        protected TestCustomMetaData newTestCustomMetaData(String data) {
            return new SnapshottableMetadata(data);
        }

        @Override
        public EnumSet<MetaData.XContentContext> context() {
            return MetaData.API_AND_SNAPSHOT;
        }
    }

    public static class NonSnapshottableMetadata extends TestCustomMetaData {
        public static final String TYPE = "test_non_snapshottable";

        public static final NonSnapshottableMetadata PROTO = new NonSnapshottableMetadata("");

        public NonSnapshottableMetadata(String data) {
            super(data);
        }

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        protected NonSnapshottableMetadata newTestCustomMetaData(String data) {
            return new NonSnapshottableMetadata(data);
        }

        @Override
        public EnumSet<MetaData.XContentContext> context() {
            return MetaData.API_ONLY;
        }
    }

    public static class SnapshottableGatewayMetadata extends TestCustomMetaData {
        public static final String TYPE = "test_snapshottable_gateway";

        public static final SnapshottableGatewayMetadata PROTO = new SnapshottableGatewayMetadata("");

        public SnapshottableGatewayMetadata(String data) {
            super(data);
        }

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        protected TestCustomMetaData newTestCustomMetaData(String data) {
            return new SnapshottableGatewayMetadata(data);
        }

        @Override
        public EnumSet<MetaData.XContentContext> context() {
            return EnumSet.of(MetaData.XContentContext.API, MetaData.XContentContext.SNAPSHOT, MetaData.XContentContext.GATEWAY);
        }
    }

    public static class NonSnapshottableGatewayMetadata extends TestCustomMetaData {
        public static final String TYPE = "test_non_snapshottable_gateway";

        public static final NonSnapshottableGatewayMetadata PROTO = new NonSnapshottableGatewayMetadata("");

        public NonSnapshottableGatewayMetadata(String data) {
            super(data);
        }

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        protected NonSnapshottableGatewayMetadata newTestCustomMetaData(String data) {
            return new NonSnapshottableGatewayMetadata(data);
        }

        @Override
        public EnumSet<MetaData.XContentContext> context() {
            return MetaData.API_AND_GATEWAY;
        }

    }

    public static class SnapshotableGatewayNoApiMetadata extends TestCustomMetaData {
        public static final String TYPE = "test_snapshottable_gateway_no_api";

        public static final SnapshotableGatewayNoApiMetadata PROTO = new SnapshotableGatewayNoApiMetadata("");

        public SnapshotableGatewayNoApiMetadata(String data) {
            super(data);
        }

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        protected SnapshotableGatewayNoApiMetadata newTestCustomMetaData(String data) {
            return new SnapshotableGatewayNoApiMetadata(data);
        }

        @Override
        public EnumSet<MetaData.XContentContext> context() {
            return EnumSet.of(MetaData.XContentContext.GATEWAY, MetaData.XContentContext.SNAPSHOT);
        }
    }

}
