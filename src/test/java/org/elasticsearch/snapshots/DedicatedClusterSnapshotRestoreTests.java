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

import com.carrotsearch.hppc.IntOpenHashSet;
import com.carrotsearch.hppc.IntSet;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotStatus;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse;
import org.elasticsearch.action.admin.indices.recovery.ShardRecoveryResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProcessedClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.discovery.zen.elect.ElectMasterService;
import org.elasticsearch.index.store.IndexStore;
import org.elasticsearch.indices.ttl.IndicesTTLService;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.snapshots.mockstore.MockRepositoryModule;
import org.elasticsearch.snapshots.mockstore.MockRepositoryPlugin;
import org.elasticsearch.test.InternalTestCluster;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Lists.newArrayList;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertThrows;
import static org.hamcrest.Matchers.*;

/**
 */
@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class DedicatedClusterSnapshotRestoreTests extends AbstractSnapshotTests {

    @Test
    public void restorePersistentSettingsTest() throws Exception {
        logger.info("--> start 2 nodes");
        Settings nodeSettings = settingsBuilder()
                .put("discovery.type", "zen")
                .put("discovery.zen.ping_timeout", "200ms")
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
                ImmutableSettings.settingsBuilder()
                        .put(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES, 2)
                        .put(IndicesTTLService.INDICES_TTL_INTERVAL, random, TimeUnit.MINUTES))
                .execute().actionGet();

        assertThat(client.admin().cluster().prepareState().setRoutingTable(false).setNodes(false).execute().actionGet().getState()
                .getMetaData().persistentSettings().getAsTime(IndicesTTLService.INDICES_TTL_INTERVAL, TimeValue.timeValueMinutes(1)).millis(), equalTo(TimeValue.timeValueMinutes(random).millis()));
        assertThat(client.admin().cluster().prepareState().setRoutingTable(false).setNodes(false).execute().actionGet().getState()
                .getMetaData().persistentSettings().getAsInt(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES, -1), equalTo(2));

        logger.info("--> create repository");
        PutRepositoryResponse putRepositoryResponse = client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(ImmutableSettings.settingsBuilder().put("location", createTempDir())).execute().actionGet();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));

        logger.info("--> start snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(true).execute().actionGet();
        assertThat(createSnapshotResponse.getSnapshotInfo().totalShards(), equalTo(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(0));
        assertThat(client.admin().cluster().prepareGetSnapshots("test-repo").setSnapshots("test-snap").execute().actionGet().getSnapshots().get(0).state(), equalTo(SnapshotState.SUCCESS));

        logger.info("--> clean the test persistent setting");
        client.admin().cluster().prepareUpdateSettings().setPersistentSettings(
                ImmutableSettings.settingsBuilder()
                        .put(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES, 1)
                        .put(IndicesTTLService.INDICES_TTL_INTERVAL, TimeValue.timeValueMinutes(1)))
                .execute().actionGet();
        assertThat(client.admin().cluster().prepareState().setRoutingTable(false).setNodes(false).execute().actionGet().getState()
                .getMetaData().persistentSettings().getAsTime(IndicesTTLService.INDICES_TTL_INTERVAL, TimeValue.timeValueMinutes(1)).millis(), equalTo(TimeValue.timeValueMinutes(1).millis()));

        stopNode(secondNode);
        assertThat(client.admin().cluster().prepareHealth().setWaitForNodes("1").get().isTimedOut(), equalTo(false));

        logger.info("--> restore snapshot");
        client.admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap").setRestoreGlobalState(true).setWaitForCompletion(true).execute().actionGet();
        assertThat(client.admin().cluster().prepareState().setRoutingTable(false).setNodes(false).execute().actionGet().getState()
                .getMetaData().persistentSettings().getAsTime(IndicesTTLService.INDICES_TTL_INTERVAL, TimeValue.timeValueMinutes(1)).millis(), equalTo(TimeValue.timeValueMinutes(random).millis()));

        logger.info("--> ensure that zen discovery minimum master nodes wasn't restored");
        assertThat(client.admin().cluster().prepareState().setRoutingTable(false).setNodes(false).execute().actionGet().getState()
                .getMetaData().persistentSettings().getAsInt(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES, -1), not(equalTo(2)));
    }

    @Test
    public void restoreCustomMetadata() throws Exception {
        Path tempDir = createTempDir();

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
                .setType("fs").setSettings(ImmutableSettings.settingsBuilder().put("location", tempDir)).execute().actionGet();
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
                .setType("fs").setSettings(ImmutableSettings.settingsBuilder().put("location", tempDir)).execute().actionGet();
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
        clusterService.submitStateUpdateTask("test", new ProcessedClusterStateUpdateTask() {
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

    @Test
    public void snapshotDuringNodeShutdownTest() throws Exception {
        logger.info("--> start 2 nodes");
        Client client = client();

        assertAcked(prepareCreate("test-idx", 2, settingsBuilder().put("number_of_shards", 2).put("number_of_replicas", 0)));
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            index("test-idx", "doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();
        assertThat(client.prepareCount("test-idx").get().getCount(), equalTo(100L));

        logger.info("--> create repository");
        logger.info("--> creating repository");
        PutRepositoryResponse putRepositoryResponse = client.admin().cluster().preparePutRepository("test-repo")
                .setType(MockRepositoryModule.class.getCanonicalName()).setSettings(
                        ImmutableSettings.settingsBuilder()
                                .put("location", createTempDir())
                                .put("random", randomAsciiOfLength(10))
                                .put("wait_after_unblock", 200)
                ).get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));

        // Pick one node and block it
        String blockedNode = blockNodeWithIndex("test-idx");

        logger.info("--> snapshot");
        client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(false).setIndices("test-idx").get();

        logger.info("--> waiting for block to kick in");
        waitForBlock(blockedNode, "test-repo", TimeValue.timeValueSeconds(60));

        logger.info("--> execution was blocked on node [{}], shutting it down", blockedNode);
        unblockNode(blockedNode);

        logger.info("--> stopping node", blockedNode);
        stopNode(blockedNode);
        logger.info("--> waiting for completion");
        SnapshotInfo snapshotInfo = waitForCompletion("test-repo", "test-snap", TimeValue.timeValueSeconds(60));
        logger.info("Number of failed shards [{}]", snapshotInfo.shardFailures().size());
        logger.info("--> done");
    }

    @Test
    public void snapshotWithStuckNodeTest() throws Exception {
        logger.info("--> start 2 nodes");
        ArrayList<String> nodes = newArrayList();
        nodes.add(internalCluster().startNode());
        nodes.add(internalCluster().startNode());
        Client client = client();

        assertAcked(prepareCreate("test-idx", 2, settingsBuilder().put("number_of_shards", 2).put("number_of_replicas", 0)));
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            index("test-idx", "doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();
        assertThat(client.prepareCount("test-idx").get().getCount(), equalTo(100L));

        logger.info("--> creating repository");
        Path repo = createTempDir();
        PutRepositoryResponse putRepositoryResponse = client.admin().cluster().preparePutRepository("test-repo")
                .setType(MockRepositoryModule.class.getCanonicalName()).setSettings(
                        ImmutableSettings.settingsBuilder()
                                .put("location", repo)
                                .put("random", randomAsciiOfLength(10))
                                .put("wait_after_unblock", 200)
                ).get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));

        // Pick one node and block it
        String blockedNode = blockNodeWithIndex("test-idx");
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
        unblockNode(blockedNode);
        logger.info("--> stopping node", blockedNode);
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

    @Test
    public void restoreIndexWithMissingShards() throws Exception {
        logger.info("--> start 2 nodes");
        internalCluster().startNode();
        internalCluster().startNode();
        cluster().wipeIndices("_all");

        logger.info("--> create an index that will have some unallocated shards");
        assertAcked(prepareCreate("test-idx-some", 2, settingsBuilder().put("number_of_shards", 6)
                .put("number_of_replicas", 0)));
        ensureGreen();

        logger.info("--> indexing some data into test-idx-some");
        for (int i = 0; i < 100; i++) {
            index("test-idx-some", "doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();
        assertThat(client().prepareCount("test-idx-some").get().getCount(), equalTo(100L));

        logger.info("--> shutdown one of the nodes");
        internalCluster().stopRandomDataNode();
        assertThat(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setTimeout("1m").setWaitForNodes("<2").execute().actionGet().isTimedOut(), equalTo(false));

        logger.info("--> create an index that will have all allocated shards");
        assertAcked(prepareCreate("test-idx-all", 1, settingsBuilder().put("number_of_shards", 6)
                .put("number_of_replicas", 0)));
        ensureGreen("test-idx-all");

        logger.info("--> create an index that will be closed");
        assertAcked(prepareCreate("test-idx-closed", 1, settingsBuilder().put("number_of_shards", 4).put("number_of_replicas", 0)));
        ensureGreen("test-idx-closed");

        logger.info("--> indexing some data into test-idx-all");
        for (int i = 0; i < 100; i++) {
            index("test-idx-all", "doc", Integer.toString(i), "foo", "bar" + i);
            index("test-idx-closed", "doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();
        assertThat(client().prepareCount("test-idx-all").get().getCount(), equalTo(100L));
        assertAcked(client().admin().indices().prepareClose("test-idx-closed"));

        logger.info("--> create an index that will have no allocated shards");
        assertAcked(prepareCreate("test-idx-none", 1, settingsBuilder().put("number_of_shards", 6)
                .put("index.routing.allocation.include.tag", "nowhere")
                .put("number_of_replicas", 0)));

        logger.info("--> create repository");
        logger.info("--> creating repository");
        PutRepositoryResponse putRepositoryResponse = client().admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(ImmutableSettings.settingsBuilder().put("location", createTempDir())).execute().actionGet();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));

        logger.info("--> start snapshot with default settings - should fail");
        CreateSnapshotResponse createSnapshotResponse = client().admin().cluster().prepareCreateSnapshot("test-repo", "test-snap-1")
                .setIndices("test-idx-all", "test-idx-none", "test-idx-some", "test-idx-closed")
                .setWaitForCompletion(true).execute().actionGet();
        assertThat(createSnapshotResponse.getSnapshotInfo().state(), equalTo(SnapshotState.FAILED));
        assertThat(createSnapshotResponse.getSnapshotInfo().reason(), containsString("Indices don't have primary shards"));
        assertThat(createSnapshotResponse.getSnapshotInfo().reason(), containsString("; Indices are closed [test-idx-closed]"));


        if (randomBoolean()) {
            logger.info("checking snapshot completion using status");
            client().admin().cluster().prepareCreateSnapshot("test-repo", "test-snap-2")
                    .setIndices("test-idx-all", "test-idx-none", "test-idx-some", "test-idx-closed")
                    .setWaitForCompletion(false).setPartial(true).execute().actionGet();
            awaitBusy(new Predicate<Object>() {
                @Override
                public boolean apply(Object o) {
                    SnapshotsStatusResponse snapshotsStatusResponse = client().admin().cluster().prepareSnapshotStatus("test-repo").setSnapshots("test-snap-2").get();
                    ImmutableList<SnapshotStatus> snapshotStatuses = snapshotsStatusResponse.getSnapshots();
                    if (snapshotStatuses.size() == 1) {
                        logger.trace("current snapshot status [{}]", snapshotStatuses.get(0));
                        return snapshotStatuses.get(0).getState().completed();
                    }
                    return false;
                }
            });
            SnapshotsStatusResponse snapshotsStatusResponse = client().admin().cluster().prepareSnapshotStatus("test-repo").setSnapshots("test-snap-2").get();
            ImmutableList<SnapshotStatus> snapshotStatuses = snapshotsStatusResponse.getSnapshots();
            assertThat(snapshotStatuses.size(), equalTo(1));
            SnapshotStatus snapshotStatus = snapshotStatuses.get(0);
            logger.info("State: [{}], Reason: [{}]", createSnapshotResponse.getSnapshotInfo().state(), createSnapshotResponse.getSnapshotInfo().reason());
            assertThat(snapshotStatus.getShardsStats().getTotalShards(), equalTo(22));
            assertThat(snapshotStatus.getShardsStats().getDoneShards(), lessThan(12));
            assertThat(snapshotStatus.getShardsStats().getDoneShards(), greaterThan(6));

            // There is slight delay between snapshot being marked as completed in the cluster state and on the file system
            // After it was marked as completed in the cluster state - we need to check if it's completed on the file system as well
            awaitBusy(new Predicate<Object>() {
                @Override
                public boolean apply(Object o) {
                    GetSnapshotsResponse response = client().admin().cluster().prepareGetSnapshots("test-repo").setSnapshots("test-snap-2").get();
                    assertThat(response.getSnapshots().size(), equalTo(1));
                    SnapshotInfo snapshotInfo = response.getSnapshots().get(0);
                    if (snapshotInfo.state().completed()) {
                        assertThat(snapshotInfo.state(), equalTo(SnapshotState.PARTIAL));
                        return true;
                    }
                    return false;
                }
            });
        } else {
            logger.info("checking snapshot completion using wait_for_completion flag");
            createSnapshotResponse = client().admin().cluster().prepareCreateSnapshot("test-repo", "test-snap-2")
                    .setIndices("test-idx-all", "test-idx-none", "test-idx-some", "test-idx-closed")
                    .setWaitForCompletion(true).setPartial(true).execute().actionGet();
            logger.info("State: [{}], Reason: [{}]", createSnapshotResponse.getSnapshotInfo().state(), createSnapshotResponse.getSnapshotInfo().reason());
            assertThat(createSnapshotResponse.getSnapshotInfo().totalShards(), equalTo(22));
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

        assertThat(client().prepareCount("test-idx-all").get().getCount(), equalTo(100L));

        logger.info("--> restore snapshot for the partial index");
        cluster().wipeIndices("test-idx-some");
        restoreSnapshotResponse = client().admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap-2")
                .setRestoreGlobalState(false).setIndices("test-idx-some").setPartial(true).setWaitForCompletion(true).get();
        assertThat(restoreSnapshotResponse.getRestoreInfo(), notNullValue());
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), equalTo(6));
        assertThat(restoreSnapshotResponse.getRestoreInfo().successfulShards(), allOf(greaterThan(0), lessThan(6)));
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), greaterThan(0));

        assertThat(client().prepareCount("test-idx-some").get().getCount(), allOf(greaterThan(0L), lessThan(100L)));

        logger.info("--> restore snapshot for the index that didn't have any shards snapshotted successfully");
        cluster().wipeIndices("test-idx-none");
        restoreSnapshotResponse = client().admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap-2")
                .setRestoreGlobalState(false).setIndices("test-idx-none").setPartial(true).setWaitForCompletion(true).get();
        assertThat(restoreSnapshotResponse.getRestoreInfo(), notNullValue());
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), equalTo(6));
        assertThat(restoreSnapshotResponse.getRestoreInfo().successfulShards(), equalTo(0));
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(6));

        assertThat(client().prepareCount("test-idx-some").get().getCount(), allOf(greaterThan(0L), lessThan(100L)));
    }

    @Test
    public void restoreIndexWithShardsMissingInLocalGateway() throws Exception {
        logger.info("--> start 2 nodes");
        internalCluster().startNode();
        internalCluster().startNode();
        cluster().wipeIndices("_all");

        logger.info("--> create repository");
        PutRepositoryResponse putRepositoryResponse = client().admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(ImmutableSettings.settingsBuilder().put("location", createTempDir())).execute().actionGet();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));
        int numberOfShards = 6;
        logger.info("--> create an index that will have some unallocated shards");
        assertAcked(prepareCreate("test-idx", 2, settingsBuilder().put("number_of_shards", numberOfShards)
                .put("number_of_replicas", 0)));
        ensureGreen();

        logger.info("--> indexing some data into test-idx");
        for (int i = 0; i < 100; i++) {
            index("test-idx", "doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();
        assertThat(client().prepareCount("test-idx").get().getCount(), equalTo(100L));

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
        assertThat(client().prepareCount("test-idx").get().getCount(), equalTo(100L));

        IntSet reusedShards = IntOpenHashSet.newInstance();
        for (ShardRecoveryResponse response : client().admin().indices().prepareRecoveries("test-idx").get().shardResponses().get("test-idx")) {
            if (response.recoveryState().getIndex().reusedBytes() > 0) {
                reusedShards.add(response.getShardId());
            }
        }
        logger.info("--> check that at least half of the shards had some reuse: [{}]", reusedShards);
        assertThat(reusedShards.size(), greaterThanOrEqualTo(numberOfShards / 2));
    }


    @Test
    public void registrationFailureTest() {
        logger.info("--> start first node");
        internalCluster().startNode(settingsBuilder().put("plugin.types", MockRepositoryPlugin.class.getName()));
        logger.info("--> start second node");
        // Make sure the first node is elected as master
        internalCluster().startNode(settingsBuilder().put("node.master", false));
        // Register mock repositories
        for (int i = 0; i < 5; i++) {
            client().admin().cluster().preparePutRepository("test-repo" + i)
                    .setType("mock").setSettings(ImmutableSettings.settingsBuilder()
                    .put("location", createTempDir())).setVerify(false).get();
        }
        logger.info("--> make sure that properly setup repository can be registered on all nodes");
        client().admin().cluster().preparePutRepository("test-repo-0")
                .setType("fs").setSettings(ImmutableSettings.settingsBuilder()
                .put("location", createTempDir())).get();

    }

    @Test
    @Ignore
    public void chaosSnapshotTest() throws Exception {
        final List<String> indices = new CopyOnWriteArrayList<>();
        Settings settings = settingsBuilder().put("action.write_consistency", "one").build();
        int initialNodes = between(1, 3);
        logger.info("--> start {} nodes", initialNodes);
        for (int i = 0; i < initialNodes; i++) {
            internalCluster().startNode(settings);
        }

        logger.info("-->  creating repository");
        assertAcked(client().admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(ImmutableSettings.settingsBuilder()
                        .put("location", createTempDir())
                        .put("compress", randomBoolean())
                        .put("chunk_size", randomIntBetween(100, 1000))));

        int initialIndices = between(1, 3);
        logger.info("--> create {} indices", initialIndices);
        for (int i = 0; i < initialIndices; i++) {
            createTestIndex("test-" + i);
            indices.add("test-" + i);
        }

        int asyncNodes = between(0, 5);
        logger.info("--> start {} additional nodes asynchronously", asyncNodes);
        ListenableFuture<List<String>> asyncNodesFuture = internalCluster().startNodesAsync(asyncNodes, settings);

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
        assertAcked(client().admin().indices().prepareUpdateSettings("test-*").setSettings(ImmutableSettings.builder()
                        .put(IndexStore.INDEX_STORE_THROTTLE_TYPE, "node")
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
        assertAcked(prepareCreate(name, 0, settingsBuilder().put("number_of_shards", between(1, 6))
                .put("number_of_replicas", between(1, 6))));

        ensureYellow(name);

        logger.info("--> indexing some data into {}", name);
        for (int i = 0; i < between(10, 500); i++) {
            index(name, "doc", Integer.toString(i), "foo", "bar" + i);
        }

        assertAcked(client().admin().indices().prepareUpdateSettings(name).setSettings(ImmutableSettings.builder()
                        .put(IndexStore.INDEX_STORE_THROTTLE_TYPE, "all")
                        .put(IndexStore.INDEX_STORE_THROTTLE_MAX_BYTES_PER_SEC, between(100, 50000))
        ));
    }

    public static abstract class TestCustomMetaData implements MetaData.Custom {
        private final String data;

        protected TestCustomMetaData(String data) {
            this.data = data;
        }

        public String getData() {
            return data;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TestCustomMetaData that = (TestCustomMetaData) o;

            if (!data.equals(that.data)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return data.hashCode();
        }

        public static abstract class TestCustomMetaDataFactory<T extends TestCustomMetaData> extends MetaData.Custom.Factory<T> {

            protected abstract TestCustomMetaData newTestCustomMetaData(String data);

            @Override
            public T readFrom(StreamInput in) throws IOException {
                return (T) newTestCustomMetaData(in.readString());
            }

            @Override
            public void writeTo(T metadata, StreamOutput out) throws IOException {
                out.writeString(metadata.getData());
            }

            @Override
            public T fromXContent(XContentParser parser) throws IOException {
                XContentParser.Token token;
                String data = null;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        String currentFieldName = parser.currentName();
                        if ("data".equals(currentFieldName)) {
                            if (parser.nextToken() != XContentParser.Token.VALUE_STRING) {
                                throw new ElasticsearchParseException("failed to parse snapshottable metadata, invalid data type");
                            }
                            data = parser.text();
                        } else {
                            throw new ElasticsearchParseException("failed to parse snapshottable metadata, unknown field [" + currentFieldName + "]");
                        }
                    } else {
                        throw new ElasticsearchParseException("failed to parse snapshottable metadata");
                    }
                }
                if (data == null) {
                    throw new ElasticsearchParseException("failed to parse snapshottable metadata, data not found");
                }
                return (T) newTestCustomMetaData(data);
            }

            @Override
            public void toXContent(T metadata, XContentBuilder builder, ToXContent.Params params) throws IOException {
                builder.field("data", metadata.getData());
            }
        }
    }

    static {
        MetaData.registerFactory(SnapshottableMetadata.TYPE, SnapshottableMetadata.FACTORY);
        MetaData.registerFactory(NonSnapshottableMetadata.TYPE, NonSnapshottableMetadata.FACTORY);
        MetaData.registerFactory(SnapshottableGatewayMetadata.TYPE, SnapshottableGatewayMetadata.FACTORY);
        MetaData.registerFactory(NonSnapshottableGatewayMetadata.TYPE, NonSnapshottableGatewayMetadata.FACTORY);
        MetaData.registerFactory(SnapshotableGatewayNoApiMetadata.TYPE, SnapshotableGatewayNoApiMetadata.FACTORY);
    }

    public static class SnapshottableMetadata extends TestCustomMetaData {
        public static final String TYPE = "test_snapshottable";

        public static final Factory FACTORY = new Factory();

        public SnapshottableMetadata(String data) {
            super(data);
        }

        private static class Factory extends TestCustomMetaDataFactory<SnapshottableMetadata> {

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
    }

    public static class NonSnapshottableMetadata extends TestCustomMetaData {
        public static final String TYPE = "test_non_snapshottable";

        public static final Factory FACTORY = new Factory();

        public NonSnapshottableMetadata(String data) {
            super(data);
        }

        private static class Factory extends TestCustomMetaDataFactory<NonSnapshottableMetadata> {

            @Override
            public String type() {
                return TYPE;
            }

            @Override
            protected NonSnapshottableMetadata newTestCustomMetaData(String data) {
                return new NonSnapshottableMetadata(data);
            }
        }
    }

    public static class SnapshottableGatewayMetadata extends TestCustomMetaData {
        public static final String TYPE = "test_snapshottable_gateway";

        public static final Factory FACTORY = new Factory();

        public SnapshottableGatewayMetadata(String data) {
            super(data);
        }

        private static class Factory extends TestCustomMetaDataFactory<SnapshottableGatewayMetadata> {

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
    }

    public static class NonSnapshottableGatewayMetadata extends TestCustomMetaData {
        public static final String TYPE = "test_non_snapshottable_gateway";

        public static final Factory FACTORY = new Factory();

        public NonSnapshottableGatewayMetadata(String data) {
            super(data);
        }

        private static class Factory extends TestCustomMetaDataFactory<NonSnapshottableGatewayMetadata> {

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
    }

    public static class SnapshotableGatewayNoApiMetadata extends TestCustomMetaData {
        public static final String TYPE = "test_snapshottable_gateway_no_api";

        public static final Factory FACTORY = new Factory();

        public SnapshotableGatewayNoApiMetadata(String data) {
            super(data);
        }

        private static class Factory extends TestCustomMetaDataFactory<SnapshotableGatewayNoApiMetadata> {

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

}
