/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntSet;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotStats;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotStatus;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.network.CloseableChannel;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.seqno.RetentionLeaseActions;
import org.elasticsearch.index.seqno.RetentionLeases;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.rest.AbstractRestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.admin.cluster.RestClusterStateAction;
import org.elasticsearch.rest.action.admin.cluster.RestGetRepositoriesAction;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.disruption.BusyMasterServiceDisruption;
import org.elasticsearch.test.disruption.ServiceDisruptionScheme;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportMessageListener;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.index.seqno.RetentionLeaseActions.RETAIN_ALL;
import static org.elasticsearch.test.NodeRoles.nonMasterNode;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFutureThrows;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class DedicatedClusterSnapshotRestoreIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockRepository.Plugin.class, MockTransportService.TestPlugin.class);
    }

    public void testSnapshotDuringNodeShutdown() throws Exception {
        assertAcked(prepareCreate("test-idx", 2, indexSettingsNoReplicas(2)));
        ensureGreen();

        indexRandomDocs("test-idx", 100);
        final Path repoPath = randomRepoPath();
        createRepository(
            "test-repo",
            "mock",
            Settings.builder().put("location", repoPath).put("random", randomAlphaOfLength(10)).put("wait_after_unblock", 200)
        );
        maybeInitWithOldSnapshotVersion("test-repo", repoPath);

        // Pick one node and block it
        String blockedNode = blockNodeWithIndex("test-repo", "test-idx");

        logger.info("--> snapshot");
        clusterAdmin().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(false).setIndices("test-idx").get();

        logger.info("--> waiting for block to kick in");
        waitForBlock(blockedNode, "test-repo");

        logger.info("--> execution was blocked on node [{}], shutting it down", blockedNode);
        unblockNode("test-repo", blockedNode);

        logger.info("--> stopping node [{}]", blockedNode);
        stopNode(blockedNode);
        logger.info("--> waiting for completion");
        awaitNoMoreRunningOperations();
        logger.info("Number of failed shards [{}]", getSnapshot("test-repo", "test-snap").shardFailures().size());
    }

    public void testSnapshotWithStuckNode() throws Exception {
        logger.info("--> start 2 nodes");
        List<String> nodes = internalCluster().startNodes(2);

        assertAcked(prepareCreate("test-idx", 2, indexSettingsNoReplicas(2)));
        ensureGreen();

        indexRandomDocs("test-idx", 100);

        Path repo = randomRepoPath();
        createRepository(
            "test-repo",
            "mock",
            Settings.builder().put("location", repo).put("random", randomAlphaOfLength(10)).put("wait_after_unblock", 200)
        );

        // Pick one node and block it
        String blockedNode = blockNodeWithIndex("test-repo", "test-idx");
        // Remove it from the list of available nodes
        nodes.remove(blockedNode);

        assertFileCount(repo, 0);
        logger.info("--> snapshot");
        clusterAdmin().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(false).setIndices("test-idx").get();

        logger.info("--> waiting for block to kick in");
        waitForBlock(blockedNode, "test-repo");

        logger.info("--> execution was blocked on node [{}], aborting snapshot", blockedNode);

        ActionFuture<AcknowledgedResponse> deleteSnapshotResponseFuture = internalCluster().client(nodes.get(0))
            .admin()
            .cluster()
            .prepareDeleteSnapshot("test-repo", "test-snap")
            .execute();
        // Make sure that abort makes some progress
        Thread.sleep(100);
        unblockNode("test-repo", blockedNode);
        logger.info("--> stopping node [{}]", blockedNode);
        stopNode(blockedNode);
        try {
            assertAcked(deleteSnapshotResponseFuture.actionGet());
        } catch (SnapshotMissingException ex) {
            // When master node is closed during this test, it sometime manages to delete the snapshot files before
            // completely stopping. In this case the retried delete snapshot operation on the new master can fail
            // with SnapshotMissingException
        }

        logger.info("--> making sure that snapshot no longer exists");
        expectThrows(
            SnapshotMissingException.class,
            () -> client().admin().cluster().prepareGetSnapshots("test-repo").setSnapshots("test-snap").execute().actionGet()
        );

        logger.info("--> Go through a loop of creating and deleting a snapshot to trigger repository cleanup");
        clusterAdmin().prepareCleanupRepository("test-repo").get();

        // Expect two files to remain in the repository:
        // (1) index-(N+1)
        // (2) index-latest
        assertFileCount(repo, 2);
        logger.info("--> done");
    }

    public void testRestoreIndexWithMissingShards() throws Exception {
        disableRepoConsistencyCheck("This test leaves behind a purposely broken repository");
        logger.info("--> start 2 nodes");
        internalCluster().startNodes(2);

        logger.info("--> create an index that will have some unallocated shards");
        assertAcked(prepareCreate("test-idx-some", 2, indexSettingsNoReplicas(6)));
        ensureGreen();

        indexRandomDocs("test-idx-some", 100);

        logger.info("--> shutdown one of the nodes");
        internalCluster().stopRandomDataNode();
        assertThat(
            clusterAdmin().prepareHealth()
                .setWaitForEvents(Priority.LANGUID)
                .setTimeout("1m")
                .setWaitForNodes("<2")
                .execute()
                .actionGet()
                .isTimedOut(),
            equalTo(false)
        );

        logger.info("--> create an index that will have all allocated shards");
        assertAcked(prepareCreate("test-idx-all", 1, indexSettingsNoReplicas(6)));
        ensureGreen("test-idx-all");

        logger.info("--> create an index that will be closed");
        assertAcked(prepareCreate("test-idx-closed", 1, indexSettingsNoReplicas(4)));
        indexRandomDocs("test-idx-all", 100);
        indexRandomDocs("test-idx-closed", 100);
        assertAcked(client().admin().indices().prepareClose("test-idx-closed"));

        logger.info("--> create an index that will have no allocated shards");
        assertAcked(
            prepareCreate("test-idx-none", 1, indexSettingsNoReplicas(6).put("index.routing.allocation.include.tag", "nowhere"))
                .setWaitForActiveShards(ActiveShardCount.NONE)
                .get()
        );
        assertTrue(indexExists("test-idx-none"));

        createRepository("test-repo", "fs");

        logger.info("--> start snapshot with default settings without a closed index - should fail");
        final SnapshotException sne = expectThrows(
            SnapshotException.class,
            () -> clusterAdmin().prepareCreateSnapshot("test-repo", "test-snap-1")
                .setIndices("test-idx-all", "test-idx-none", "test-idx-some", "test-idx-closed")
                .setWaitForCompletion(true)
                .execute()
                .actionGet()
        );
        assertThat(sne.getMessage(), containsString("Indices don't have primary shards"));

        if (randomBoolean()) {
            logger.info("checking snapshot completion using status");
            clusterAdmin().prepareCreateSnapshot("test-repo", "test-snap-2")
                .setIndices("test-idx-all", "test-idx-none", "test-idx-some", "test-idx-closed")
                .setWaitForCompletion(false)
                .setPartial(true)
                .execute()
                .actionGet();
            assertBusy(() -> {
                SnapshotsStatusResponse snapshotsStatusResponse = clusterAdmin().prepareSnapshotStatus("test-repo")
                    .setSnapshots("test-snap-2")
                    .get();
                List<SnapshotStatus> snapshotStatuses = snapshotsStatusResponse.getSnapshots();
                assertEquals(snapshotStatuses.size(), 1);
                logger.trace("current snapshot status [{}]", snapshotStatuses.get(0));
                assertTrue(snapshotStatuses.get(0).getState().completed());
            }, 1, TimeUnit.MINUTES);
            SnapshotsStatusResponse snapshotsStatusResponse = clusterAdmin().prepareSnapshotStatus("test-repo")
                .setSnapshots("test-snap-2")
                .get();
            List<SnapshotStatus> snapshotStatuses = snapshotsStatusResponse.getSnapshots();
            assertThat(snapshotStatuses.size(), equalTo(1));
            SnapshotStatus snapshotStatus = snapshotStatuses.get(0);

            assertThat(snapshotStatus.getShardsStats().getTotalShards(), equalTo(22));
            assertThat(snapshotStatus.getShardsStats().getDoneShards(), lessThan(16));
            assertThat(snapshotStatus.getShardsStats().getDoneShards(), greaterThan(10));

            // There is slight delay between snapshot being marked as completed in the cluster state and on the file system
            // After it was marked as completed in the cluster state - we need to check if it's completed on the file system as well
            assertBusy(() -> {
                SnapshotInfo snapshotInfo = getSnapshot("test-repo", "test-snap-2");
                assertTrue(snapshotInfo.state().completed());
                assertEquals(SnapshotState.PARTIAL, snapshotInfo.state());
            }, 1, TimeUnit.MINUTES);
        } else {
            logger.info("checking snapshot completion using wait_for_completion flag");
            final CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot("test-repo", "test-snap-2")
                .setIndices("test-idx-all", "test-idx-none", "test-idx-some", "test-idx-closed")
                .setWaitForCompletion(true)
                .setPartial(true)
                .execute()
                .actionGet();
            logger.info(
                "State: [{}], Reason: [{}]",
                createSnapshotResponse.getSnapshotInfo().state(),
                createSnapshotResponse.getSnapshotInfo().reason()
            );
            assertThat(createSnapshotResponse.getSnapshotInfo().totalShards(), equalTo(22));
            assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), lessThan(16));
            assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(10));
            assertThat(getSnapshot("test-repo", "test-snap-2").state(), equalTo(SnapshotState.PARTIAL));
        }

        assertAcked(client().admin().indices().prepareClose("test-idx-all"));

        logger.info("--> restore incomplete snapshot - should fail");
        assertFutureThrows(
            clusterAdmin().prepareRestoreSnapshot("test-repo", "test-snap-2")
                .setRestoreGlobalState(false)
                .setWaitForCompletion(true)
                .execute(),
            SnapshotRestoreException.class
        );

        logger.info("--> restore snapshot for the index that was snapshotted completely");
        RestoreSnapshotResponse restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot("test-repo", "test-snap-2")
            .setRestoreGlobalState(false)
            .setIndices("test-idx-all")
            .setWaitForCompletion(true)
            .execute()
            .actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo(), notNullValue());
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), equalTo(6));
        assertThat(restoreSnapshotResponse.getRestoreInfo().successfulShards(), equalTo(6));
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));
        assertDocCount("test-idx-all", 100L);

        logger.info("--> restore snapshot for the partial index");
        cluster().wipeIndices("test-idx-some");
        restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot("test-repo", "test-snap-2")
            .setRestoreGlobalState(false)
            .setIndices("test-idx-some")
            .setPartial(true)
            .setWaitForCompletion(true)
            .get();
        assertThat(restoreSnapshotResponse.getRestoreInfo(), notNullValue());
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), equalTo(6));
        assertThat(restoreSnapshotResponse.getRestoreInfo().successfulShards(), allOf(greaterThan(0), lessThan(6)));
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), greaterThan(0));
        assertThat(getCountForIndex("test-idx-some"), allOf(greaterThan(0L), lessThan(100L)));

        logger.info("--> restore snapshot for the index that didn't have any shards snapshotted successfully");
        cluster().wipeIndices("test-idx-none");
        restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot("test-repo", "test-snap-2")
            .setRestoreGlobalState(false)
            .setIndices("test-idx-none")
            .setPartial(true)
            .setWaitForCompletion(true)
            .get();
        assertThat(restoreSnapshotResponse.getRestoreInfo(), notNullValue());
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), equalTo(6));
        assertThat(restoreSnapshotResponse.getRestoreInfo().successfulShards(), equalTo(0));
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(6));
        assertThat(getCountForIndex("test-idx-some"), allOf(greaterThan(0L), lessThan(100L)));

        logger.info("--> restore snapshot for the closed index that was snapshotted completely");
        restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot("test-repo", "test-snap-2")
            .setRestoreGlobalState(false)
            .setIndices("test-idx-closed")
            .setWaitForCompletion(true)
            .execute()
            .actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo(), notNullValue());
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), equalTo(4));
        assertThat(restoreSnapshotResponse.getRestoreInfo().successfulShards(), equalTo(4));
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));
        assertDocCount("test-idx-closed", 100L);
    }

    public void testRestoreIndexWithShardsMissingInLocalGateway() throws Exception {
        logger.info("--> start 2 nodes");
        internalCluster().startNodes(2);

        createRepository("test-repo", "fs");

        int numberOfShards = 6;
        logger.info("--> create an index that will have some unallocated shards");
        assertAcked(prepareCreate("test-idx", 2, indexSettingsNoReplicas(numberOfShards)));
        ensureGreen();

        indexRandomDocs("test-idx", 100);

        logger.info("--> force merging down to a single segment to get a deterministic set of files");
        assertEquals(
            client().admin().indices().prepareForceMerge("test-idx").setMaxNumSegments(1).setFlush(true).get().getFailedShards(),
            0
        );

        createSnapshot("test-repo", "test-snap-1", Collections.singletonList("test-idx"));

        logger.info("--> close the index");
        assertAcked(client().admin().indices().prepareClose("test-idx"));

        logger.info("--> shutdown one of the nodes that should make half of the shards unavailable");
        internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {
            @Override
            public boolean clearData(String nodeName) {
                return true;
            }
        });

        assertThat(
            clusterAdmin().prepareHealth()
                .setWaitForEvents(Priority.LANGUID)
                .setTimeout("1m")
                .setWaitForNodes("2")
                .execute()
                .actionGet()
                .isTimedOut(),
            equalTo(false)
        );

        logger.info("--> restore index snapshot");
        assertThat(
            clusterAdmin().prepareRestoreSnapshot("test-repo", "test-snap-1")
                .setRestoreGlobalState(false)
                .setWaitForCompletion(true)
                .get()
                .getRestoreInfo()
                .successfulShards(),
            equalTo(6)
        );

        ensureGreen("test-idx");

        IntSet reusedShards = new IntHashSet();
        List<RecoveryState> recoveryStates = client().admin()
            .indices()
            .prepareRecoveries("test-idx")
            .get()
            .shardRecoveryStates()
            .get("test-idx");
        for (RecoveryState recoveryState : recoveryStates) {
            if (recoveryState.getIndex().reusedBytes() > 0) {
                reusedShards.add(recoveryState.getShardId().getId());
            }
        }
        logger.info("--> check that at least half of the shards had some reuse: [{}]", reusedShards);
        assertThat(reusedShards.size(), greaterThanOrEqualTo(numberOfShards / 2));
    }

    public void testRegistrationFailure() {
        disableRepoConsistencyCheck("This test does not create any data in the repository");
        logger.info("--> start first node");
        internalCluster().startNode();
        logger.info("--> start second node");
        // Make sure the first node is elected as master
        internalCluster().startNode(nonMasterNode());
        // Register mock repositories
        for (int i = 0; i < 5; i++) {
            clusterAdmin().preparePutRepository("test-repo" + i)
                .setType("mock")
                .setSettings(Settings.builder().put("location", randomRepoPath()))
                .setVerify(false)
                .get();
        }
        logger.info("--> make sure that properly setup repository can be registered on all nodes");
        clusterAdmin().preparePutRepository("test-repo-0")
            .setType("fs")
            .setSettings(Settings.builder().put("location", randomRepoPath()))
            .get();

    }

    public void testThatSensitiveRepositorySettingsAreNotExposed() throws Exception {
        disableRepoConsistencyCheck("This test does not create any data in the repository");
        logger.info("--> start two nodes");
        internalCluster().startNodes(2);
        createRepository(
            "test-repo",
            "mock",
            Settings.builder()
                .put("location", randomRepoPath())
                .put(MockRepository.Plugin.USERNAME_SETTING.getKey(), "notsecretusername")
                .put(MockRepository.Plugin.PASSWORD_SETTING.getKey(), "verysecretpassword")
        );

        NodeClient nodeClient = internalCluster().getInstance(NodeClient.class);
        RestGetRepositoriesAction getRepoAction = new RestGetRepositoriesAction(internalCluster().getInstance(SettingsFilter.class));
        RestRequest getRepoRequest = new FakeRestRequest();
        getRepoRequest.params().put("repository", "test-repo");
        final CountDownLatch getRepoLatch = new CountDownLatch(1);
        final AtomicReference<AssertionError> getRepoError = new AtomicReference<>();
        getRepoAction.handleRequest(getRepoRequest, new AbstractRestChannel(getRepoRequest, true) {
            @Override
            public void sendResponse(RestResponse response) {
                try {
                    assertThat(response.content().utf8ToString(), containsString("notsecretusername"));
                    assertThat(response.content().utf8ToString(), not(containsString("verysecretpassword")));
                } catch (AssertionError ex) {
                    getRepoError.set(ex);
                }
                getRepoLatch.countDown();
            }
        }, nodeClient);
        assertTrue(getRepoLatch.await(1, TimeUnit.SECONDS));
        if (getRepoError.get() != null) {
            throw getRepoError.get();
        }

        RestClusterStateAction clusterStateAction = new RestClusterStateAction(
            internalCluster().getInstance(SettingsFilter.class),
            internalCluster().getInstance(ThreadPool.class)
        );
        RestRequest clusterStateRequest = new FakeRestRequest();
        final CountDownLatch clusterStateLatch = new CountDownLatch(1);
        final AtomicReference<AssertionError> clusterStateError = new AtomicReference<>();
        clusterStateAction.handleRequest(clusterStateRequest, new AbstractRestChannel(clusterStateRequest, true) {
            @Override
            public void sendResponse(RestResponse response) {
                try {
                    assertThat(response.content().utf8ToString(), containsString("notsecretusername"));
                    assertThat(response.content().utf8ToString(), not(containsString("verysecretpassword")));
                } catch (AssertionError ex) {
                    clusterStateError.set(ex);
                } finally {
                    CloseableChannel.closeChannel(clusterStateRequest.getHttpChannel());
                }
                clusterStateLatch.countDown();
            }
        }, nodeClient);
        assertTrue(clusterStateLatch.await(1, TimeUnit.SECONDS));
        if (clusterStateError.get() != null) {
            throw clusterStateError.get();
        }
    }

    public void testMasterShutdownDuringSnapshot() throws Exception {
        logger.info("-->  starting two master nodes and two data nodes");
        internalCluster().startMasterOnlyNodes(2);
        internalCluster().startDataOnlyNodes(2);

        final Path repoPath = randomRepoPath();
        createRepository("test-repo", "fs", repoPath);
        maybeInitWithOldSnapshotVersion("test-repo", repoPath);

        assertAcked(prepareCreate("test-idx", 0, indexSettingsNoReplicas(between(1, 20))));
        ensureGreen();

        indexRandomDocs("test-idx", randomIntBetween(10, 100));

        final int numberOfShards = getNumShards("test-idx").numPrimaries;
        logger.info("number of shards: {}", numberOfShards);

        dataNodeClient().admin()
            .cluster()
            .prepareCreateSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(false)
            .setIndices("test-idx")
            .get();

        logger.info("--> stopping master node");
        internalCluster().stopCurrentMasterNode();

        logger.info("--> wait until the snapshot is done");

        assertBusy(() -> assertTrue(getSnapshot("test-repo", "test-snap").state().completed()), 1, TimeUnit.MINUTES);

        logger.info("--> verify that snapshot was successful");
        SnapshotInfo snapshotInfo = getSnapshot("test-repo", "test-snap");
        assertEquals(SnapshotState.SUCCESS, snapshotInfo.state());
        assertEquals(snapshotInfo.totalShards(), snapshotInfo.successfulShards());
        assertEquals(0, snapshotInfo.failedShards());
    }

    public void testMasterAndDataShutdownDuringSnapshot() throws Exception {
        logger.info("-->  starting three master nodes and two data nodes");
        internalCluster().startMasterOnlyNodes(3);
        internalCluster().startDataOnlyNodes(2);

        final Path repoPath = randomRepoPath();
        createRepository("test-repo", "mock", repoPath);
        maybeInitWithOldSnapshotVersion("test-repo", repoPath);

        assertAcked(prepareCreate("test-idx", 0, indexSettingsNoReplicas(between(1, 20))));
        ensureGreen();

        indexRandomDocs("test-idx", randomIntBetween(10, 100));

        final int numberOfShards = getNumShards("test-idx").numPrimaries;
        logger.info("number of shards: {}", numberOfShards);

        blockMasterFromFinalizingSnapshotOnSnapFile("test-repo");
        final String dataNode = blockNodeWithIndex("test-repo", "test-idx");

        dataNodeClient().admin()
            .cluster()
            .prepareCreateSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(false)
            .setIndices("test-idx")
            .get();

        stopNode(dataNode);
        internalCluster().stopCurrentMasterNode();

        logger.info("--> wait until the snapshot is done");

        assertBusy(() -> assertTrue(getSnapshot("test-repo", "test-snap").state().completed()), 1, TimeUnit.MINUTES);

        logger.info("--> verify that snapshot was partial");
        SnapshotInfo snapshotInfo = getSnapshot("test-repo", "test-snap");
        assertEquals(SnapshotState.PARTIAL, snapshotInfo.state());
        assertNotEquals(snapshotInfo.totalShards(), snapshotInfo.successfulShards());
        assertThat(snapshotInfo.failedShards(), greaterThan(0));
        for (SnapshotShardFailure failure : snapshotInfo.shardFailures()) {
            assertNotNull(failure.reason());
        }
    }

    /**
     * Tests that a shrunken index (created via the shrink APIs) and subsequently snapshotted
     * can be restored when the node the shrunken index was created on is no longer part of
     * the cluster.
     */
    public void testRestoreShrinkIndex() throws Exception {
        logger.info("-->  starting a master node and a data node");
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();

        final String repo = "test-repo";
        final String snapshot = "test-snap";
        final String sourceIdx = "test-idx";
        final String shrunkIdx = "test-idx-shrunk";

        createRepository(repo, "fs");

        assertAcked(prepareCreate(sourceIdx, 0, indexSettingsNoReplicas(between(2, 10))));
        ensureGreen();
        indexRandomDocs(sourceIdx, randomIntBetween(10, 100));

        logger.info("--> shrink the index");
        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings(sourceIdx)
                .setSettings(Settings.builder().put("index.blocks.write", true))
                .get()
        );
        assertAcked(client().admin().indices().prepareResizeIndex(sourceIdx, shrunkIdx).get());

        logger.info("--> snapshot the shrunk index");
        createSnapshot(repo, snapshot, Collections.singletonList(shrunkIdx));

        logger.info("--> delete index and stop the data node");
        assertAcked(client().admin().indices().prepareDelete(sourceIdx).get());
        assertAcked(client().admin().indices().prepareDelete(shrunkIdx).get());
        internalCluster().stopRandomDataNode();
        clusterAdmin().prepareHealth().setTimeout("30s").setWaitForNodes("1");

        logger.info("--> start a new data node");
        final Settings dataSettings = Settings.builder()
            .put(Node.NODE_NAME_SETTING.getKey(), randomAlphaOfLength(5))
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir()) // to get a new node id
            .build();
        internalCluster().startDataOnlyNode(dataSettings);
        clusterAdmin().prepareHealth().setTimeout("30s").setWaitForNodes("2");

        logger.info("--> restore the shrunk index and ensure all shards are allocated");
        RestoreSnapshotResponse restoreResponse = clusterAdmin().prepareRestoreSnapshot(repo, snapshot)
            .setWaitForCompletion(true)
            .setIndices(shrunkIdx)
            .get();
        assertEquals(restoreResponse.getRestoreInfo().totalShards(), restoreResponse.getRestoreInfo().successfulShards());
        ensureYellow();
    }

    public void testSnapshotWithDateMath() {
        final String repo = "repo";

        final IndexNameExpressionResolver nameExpressionResolver = TestIndexNameExpressionResolver.newInstance();
        final String snapshotName = "<snapshot-{now/d}>";

        logger.info("-->  creating repository");
        assertAcked(
            clusterAdmin().preparePutRepository(repo)
                .setType("fs")
                .setSettings(Settings.builder().put("location", randomRepoPath()).put("compress", randomBoolean()))
        );

        final String expression1 = nameExpressionResolver.resolveDateMathExpression(snapshotName);
        logger.info("-->  creating date math snapshot");
        createFullSnapshot(repo, snapshotName);
        // snapshot could be taken before or after a day rollover
        final String expression2 = nameExpressionResolver.resolveDateMathExpression(snapshotName);

        SnapshotsStatusResponse response = clusterAdmin().prepareSnapshotStatus(repo)
            .setSnapshots(Sets.newHashSet(expression1, expression2).toArray(Strings.EMPTY_ARRAY))
            .setIgnoreUnavailable(true)
            .get();
        List<SnapshotStatus> snapshots = response.getSnapshots();
        assertThat(snapshots, hasSize(1));
        assertThat(snapshots.get(0).getState().completed(), equalTo(true));
    }

    public void testSnapshotTotalAndIncrementalSizes() throws Exception {
        final String indexName = "test-blocks-1";
        final String repositoryName = "repo-" + indexName;
        final String snapshot0 = "snapshot-0";
        final String snapshot1 = "snapshot-1";

        createIndex(indexName);

        int docs = between(10, 100);
        for (int i = 0; i < docs; i++) {
            client().prepareIndex(indexName).setSource("test", "init").execute().actionGet();
        }

        final Path repoPath = randomRepoPath();
        createRepository(repositoryName, "fs", repoPath);
        createFullSnapshot(repositoryName, snapshot0);

        SnapshotsStatusResponse response = clusterAdmin().prepareSnapshotStatus(repositoryName).setSnapshots(snapshot0).get();

        List<SnapshotStatus> snapshots = response.getSnapshots();

        List<Path> snapshot0Files = scanSnapshotFolder(repoPath);
        assertThat(snapshots, hasSize(1));

        final int snapshot0FileCount = snapshot0Files.size();
        final long snapshot0FileSize = calculateTotalFilesSize(snapshot0Files);

        SnapshotStats stats = snapshots.get(0).getStats();

        final List<Path> snapshot0IndexMetaFiles = findRepoMetaBlobs(repoPath);
        assertThat(snapshot0IndexMetaFiles, hasSize(1)); // snapshotting a single index
        assertThat(stats.getTotalFileCount(), greaterThanOrEqualTo(snapshot0FileCount));
        assertThat(stats.getTotalSize(), greaterThanOrEqualTo(snapshot0FileSize));

        assertThat(stats.getIncrementalFileCount(), equalTo(stats.getTotalFileCount()));
        assertThat(stats.getIncrementalSize(), equalTo(stats.getTotalSize()));

        assertThat(stats.getIncrementalFileCount(), equalTo(stats.getProcessedFileCount()));
        assertThat(stats.getIncrementalSize(), equalTo(stats.getProcessedSize()));

        // add few docs - less than initially
        docs = between(1, 5);
        for (int i = 0; i < docs; i++) {
            client().prepareIndex(indexName).setSource("test", "test" + i).execute().actionGet();
        }

        // create another snapshot
        // total size has to grow and has to be equal to files on fs
        createFullSnapshot(repositoryName, snapshot1);

        // drop 1st one to avoid miscalculation as snapshot reuses some files of prev snapshot
        assertAcked(startDeleteSnapshot(repositoryName, snapshot0).get());

        response = clusterAdmin().prepareSnapshotStatus(repositoryName).setSnapshots(snapshot1).get();

        final List<Path> snapshot1Files = scanSnapshotFolder(repoPath);
        final List<Path> snapshot1IndexMetaFiles = findRepoMetaBlobs(repoPath);

        // The IndexMetadata did not change between snapshots, verify that no new redundant IndexMetaData was written to the repository
        assertThat(snapshot1IndexMetaFiles, is(snapshot0IndexMetaFiles));

        final int snapshot1FileCount = snapshot1Files.size();
        final long snapshot1FileSize = calculateTotalFilesSize(snapshot1Files);

        snapshots = response.getSnapshots();

        SnapshotStats anotherStats = snapshots.get(0).getStats();

        ArrayList<Path> snapshotFilesDiff = new ArrayList<>(snapshot1Files);
        snapshotFilesDiff.removeAll(snapshot0Files);

        assertThat(anotherStats.getIncrementalFileCount(), greaterThanOrEqualTo(snapshotFilesDiff.size()));
        assertThat(anotherStats.getIncrementalSize(), greaterThanOrEqualTo(calculateTotalFilesSize(snapshotFilesDiff)));

        assertThat(anotherStats.getIncrementalFileCount(), equalTo(anotherStats.getProcessedFileCount()));
        assertThat(anotherStats.getIncrementalSize(), equalTo(anotherStats.getProcessedSize()));

        assertThat(stats.getTotalSize(), lessThan(anotherStats.getTotalSize()));
        assertThat(stats.getTotalFileCount(), lessThan(anotherStats.getTotalFileCount()));

        assertThat(anotherStats.getTotalFileCount(), greaterThanOrEqualTo(snapshot1FileCount));
        assertThat(anotherStats.getTotalSize(), greaterThanOrEqualTo(snapshot1FileSize));
    }

    public void testDeduplicateIndexMetadata() throws Exception {
        final String indexName = "test-blocks-1";
        final String repositoryName = "repo-" + indexName;
        final String snapshot0 = "snapshot-0";
        final String snapshot1 = "snapshot-1";
        final String snapshot2 = "snapshot-2";

        createIndex(indexName);

        int docs = between(10, 100);
        for (int i = 0; i < docs; i++) {
            client().prepareIndex(indexName).setSource("test", "init").execute().actionGet();
        }

        final Path repoPath = randomRepoPath();
        createRepository(repositoryName, "fs", repoPath);
        createFullSnapshot(repositoryName, snapshot0);

        final List<Path> snapshot0IndexMetaFiles = findRepoMetaBlobs(repoPath);
        assertThat(snapshot0IndexMetaFiles, hasSize(1)); // snapshotting a single index

        docs = between(1, 5);
        for (int i = 0; i < docs; i++) {
            client().prepareIndex(indexName).setSource("test", "test" + i).execute().actionGet();
        }

        logger.info("--> restart random data node and add new data node to change index allocation");
        internalCluster().restartRandomDataNode();
        internalCluster().startDataOnlyNode();
        ensureGreen(indexName);

        createFullSnapshot(repositoryName, snapshot1);

        final List<Path> snapshot1IndexMetaFiles = findRepoMetaBlobs(repoPath);

        // The IndexMetadata did not change between snapshots, verify that no new redundant IndexMetaData was written to the repository
        assertThat(snapshot1IndexMetaFiles, is(snapshot0IndexMetaFiles));

        // index to some other field to trigger a change in index metadata
        for (int i = 0; i < docs; i++) {
            client().prepareIndex(indexName).setSource("new_field", "test" + i).execute().actionGet();
        }
        createFullSnapshot(repositoryName, snapshot2);

        final List<Path> snapshot2IndexMetaFiles = findRepoMetaBlobs(repoPath);
        assertThat(snapshot2IndexMetaFiles, hasSize(2)); // should have created one new metadata blob

        assertAcked(clusterAdmin().prepareDeleteSnapshot(repositoryName, snapshot0, snapshot1).get());
        final List<Path> snapshot3IndexMetaFiles = findRepoMetaBlobs(repoPath);
        assertThat(snapshot3IndexMetaFiles, hasSize(1)); // should have deleted the metadata blob referenced by the first two snapshots
    }

    public void testDataNodeRestartWithBusyMasterDuringSnapshot() throws Exception {
        logger.info("-->  starting a master node and two data nodes");
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        final Path repoPath = randomRepoPath();
        createRepository("test-repo", "mock", repoPath);
        maybeInitWithOldSnapshotVersion("test-repo", repoPath);

        assertAcked(prepareCreate("test-idx", 0, indexSettingsNoReplicas(5)));
        ensureGreen();
        indexRandomDocs("test-idx", randomIntBetween(50, 100));

        final String dataNode = blockNodeWithIndex("test-repo", "test-idx");
        logger.info("-->  snapshot");
        ServiceDisruptionScheme disruption = new BusyMasterServiceDisruption(random(), Priority.HIGH);
        setDisruptionScheme(disruption);
        client(internalCluster().getMasterName()).admin()
            .cluster()
            .prepareCreateSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(false)
            .setIndices("test-idx")
            .get();
        disruption.startDisrupting();
        logger.info("-->  restarting data node, which should cause primary shards to be failed");
        internalCluster().restartNode(dataNode, InternalTestCluster.EMPTY_CALLBACK);

        logger.info("-->  wait for shard snapshots to show as failed");
        assertBusy(
            () -> assertThat(
                clusterAdmin().prepareSnapshotStatus("test-repo")
                    .setSnapshots("test-snap")
                    .get()
                    .getSnapshots()
                    .get(0)
                    .getShardsStats()
                    .getFailedShards(),
                greaterThanOrEqualTo(1)
            ),
            60L,
            TimeUnit.SECONDS
        );

        unblockNode("test-repo", dataNode);
        disruption.stopDisrupting();
        // check that snapshot completes
        assertBusy(() -> {
            GetSnapshotsResponse snapshotsStatusResponse = clusterAdmin().prepareGetSnapshots("test-repo")
                .setSnapshots("test-snap")
                .setIgnoreUnavailable(true)
                .get();
            assertEquals(1, snapshotsStatusResponse.getSnapshots().size());
            SnapshotInfo snapshotInfo = snapshotsStatusResponse.getSnapshots().get(0);
            assertTrue(snapshotInfo.state().toString(), snapshotInfo.state().completed());
        }, 60L, TimeUnit.SECONDS);
    }

    public void testDataNodeRestartAfterShardSnapshotFailure() throws Exception {
        logger.info("-->  starting a master node and two data nodes");
        internalCluster().startMasterOnlyNode();
        final List<String> dataNodes = internalCluster().startDataOnlyNodes(2);
        final Path repoPath = randomRepoPath();
        createRepository("test-repo", "mock", repoPath);
        maybeInitWithOldSnapshotVersion("test-repo", repoPath);

        assertAcked(prepareCreate("test-idx", 0, indexSettingsNoReplicas(2)));
        ensureGreen();
        indexRandomDocs("test-idx", randomIntBetween(50, 100));

        blockAllDataNodes("test-repo");
        logger.info("-->  snapshot");
        client(internalCluster().getMasterName()).admin()
            .cluster()
            .prepareCreateSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(false)
            .setIndices("test-idx")
            .get();
        logger.info("-->  restarting first data node, which should cause the primary shard on it to be failed");
        internalCluster().restartNode(dataNodes.get(0), InternalTestCluster.EMPTY_CALLBACK);

        logger.info("-->  wait for shard snapshot of first primary to show as failed");
        assertBusy(
            () -> assertThat(
                clusterAdmin().prepareSnapshotStatus("test-repo")
                    .setSnapshots("test-snap")
                    .get()
                    .getSnapshots()
                    .get(0)
                    .getShardsStats()
                    .getFailedShards(),
                is(1)
            ),
            60L,
            TimeUnit.SECONDS
        );

        logger.info("-->  restarting second data node, which should cause the primary shard on it to be failed");
        internalCluster().restartNode(dataNodes.get(1), InternalTestCluster.EMPTY_CALLBACK);

        // check that snapshot completes with both failed shards being accounted for in the snapshot result
        assertBusy(() -> {
            GetSnapshotsResponse snapshotsStatusResponse = clusterAdmin().prepareGetSnapshots("test-repo")
                .setSnapshots("test-snap")
                .setIgnoreUnavailable(true)
                .get();
            assertEquals(1, snapshotsStatusResponse.getSnapshots().size());
            SnapshotInfo snapshotInfo = snapshotsStatusResponse.getSnapshots().get(0);
            assertTrue(snapshotInfo.state().toString(), snapshotInfo.state().completed());
            assertThat(snapshotInfo.totalShards(), is(2));
            assertThat(snapshotInfo.shardFailures(), hasSize(2));
        }, 60L, TimeUnit.SECONDS);
    }

    public void testRetentionLeasesClearedOnRestore() throws Exception {
        final String repoName = "test-repo-retention-leases";
        createRepository(repoName, "fs");

        final String indexName = "index-retention-leases";
        final int shardCount = randomIntBetween(1, 5);
        assertAcked(client().admin().indices().prepareCreate(indexName).setSettings(indexSettingsNoReplicas(shardCount)));
        final ShardId shardId = new ShardId(resolveIndex(indexName), randomIntBetween(0, shardCount - 1));

        final int snapshotDocCount = iterations(10, 1000);
        logger.debug("--> indexing {} docs into {}", snapshotDocCount, indexName);
        IndexRequestBuilder[] indexRequestBuilders = new IndexRequestBuilder[snapshotDocCount];
        for (int i = 0; i < snapshotDocCount; i++) {
            indexRequestBuilders[i] = client().prepareIndex(indexName).setSource("field", "value");
        }
        indexRandom(true, indexRequestBuilders);
        assertDocCount(indexName, snapshotDocCount);

        final String leaseId = randomAlphaOfLength(randomIntBetween(1, 10)).toLowerCase(Locale.ROOT);
        logger.debug("--> adding retention lease with id {} to {}", leaseId, shardId);
        client().execute(RetentionLeaseActions.Add.INSTANCE, new RetentionLeaseActions.AddRequest(shardId, leaseId, RETAIN_ALL, "test"))
            .actionGet();

        final ShardStats shardStats = Arrays.stream(client().admin().indices().prepareStats(indexName).get().getShards())
            .filter(s -> s.getShardRouting().shardId().equals(shardId))
            .findFirst()
            .get();
        final RetentionLeases retentionLeases = shardStats.getRetentionLeaseStats().retentionLeases();
        assertTrue(shardStats + ": " + retentionLeases, retentionLeases.contains(leaseId));

        final String snapshotName = "snapshot-retention-leases";
        createSnapshot(repoName, snapshotName, Collections.singletonList(indexName));

        if (randomBoolean()) {
            final int extraDocCount = iterations(10, 1000);
            logger.debug("--> indexing {} extra docs into {}", extraDocCount, indexName);
            indexRequestBuilders = new IndexRequestBuilder[extraDocCount];
            for (int i = 0; i < extraDocCount; i++) {
                indexRequestBuilders[i] = client().prepareIndex(indexName).setSource("field", "value");
            }
            indexRandom(true, indexRequestBuilders);
        }

        // Wait for green so the close does not fail in the edge case of coinciding with a shard recovery that hasn't fully synced yet
        ensureGreen();
        logger.debug("-->  close index {}", indexName);
        assertAcked(client().admin().indices().prepareClose(indexName));

        logger.debug("--> restore index {} from snapshot", indexName);
        RestoreSnapshotResponse restoreResponse = clusterAdmin().prepareRestoreSnapshot(repoName, snapshotName)
            .setWaitForCompletion(true)
            .get();
        assertThat(restoreResponse.getRestoreInfo().successfulShards(), equalTo(shardCount));
        assertThat(restoreResponse.getRestoreInfo().failedShards(), equalTo(0));

        ensureGreen();
        assertDocCount(indexName, snapshotDocCount);

        final RetentionLeases restoredRetentionLeases = Arrays.stream(client().admin().indices().prepareStats(indexName).get().getShards())
            .filter(s -> s.getShardRouting().shardId().equals(shardId))
            .findFirst()
            .get()
            .getRetentionLeaseStats()
            .retentionLeases();
        assertFalse(restoredRetentionLeases.toString() + " has no " + leaseId, restoredRetentionLeases.contains(leaseId));
    }

    public void testAbortWaitsOnDataNode() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNodeName = internalCluster().startDataOnlyNode();
        final String indexName = "test-index";
        createIndex(indexName);
        indexDoc(indexName, "some_id", "foo", "bar");

        final String otherDataNode = internalCluster().startDataOnlyNode();

        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        blockAllDataNodes(repoName);
        final String snapshotName = "test-snap";
        final ActionFuture<CreateSnapshotResponse> snapshotResponse = startFullSnapshot(repoName, snapshotName);
        waitForBlock(dataNodeName, repoName);

        final AtomicBoolean blocked = new AtomicBoolean(true);

        final TransportService transportService = internalCluster().getInstance(TransportService.class, otherDataNode);
        transportService.addMessageListener(new TransportMessageListener() {
            @Override
            public void onRequestSent(
                DiscoveryNode node,
                long requestId,
                String action,
                TransportRequest request,
                TransportRequestOptions finalOptions
            ) {
                if (blocked.get() && action.equals(SnapshotsService.UPDATE_SNAPSHOT_STATUS_ACTION_NAME)) {
                    throw new AssertionError("Node had no assigned shard snapshots so it shouldn't send out shard state updates");
                }
            }
        });

        logger.info("--> abort snapshot");
        final ActionFuture<AcknowledgedResponse> deleteResponse = startDeleteSnapshot(repoName, snapshotName);

        awaitClusterState(
            logger,
            otherDataNode,
            state -> state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY)
                .entries()
                .stream()
                .anyMatch(entry -> entry.state() == SnapshotsInProgress.State.ABORTED)
        );

        assertFalse("delete should not be able to finish until data node is unblocked", deleteResponse.isDone());
        blocked.set(false);
        unblockAllDataNodes(repoName);
        assertAcked(deleteResponse.get());
        assertThat(snapshotResponse.get().getSnapshotInfo().state(), is(SnapshotState.FAILED));
    }

    public void testPartialSnapshotAllShardsMissing() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "fs");
        createIndex("some-index");
        stopNode(dataNode);
        ensureStableCluster(1);
        final CreateSnapshotResponse createSnapshotResponse = startFullSnapshot(repoName, "test-snap", true).get();
        assertThat(createSnapshotResponse.getSnapshotInfo().state(), is(SnapshotState.PARTIAL));
    }

    public void testSnapshotDeleteRelocatingPrimaryIndex() throws Exception {
        internalCluster().startMasterOnlyNode();
        final List<String> dataNodes = internalCluster().startDataOnlyNodes(2);
        final String repoName = "test-repo";
        createRepository(repoName, "fs");

        // Create index on two nodes and make sure each node has a primary by setting no replicas
        final String indexName = "test-idx";
        assertAcked(prepareCreate(indexName, 2, indexSettingsNoReplicas(between(2, 10))));
        ensureGreen(indexName);
        indexRandomDocs(indexName, 100);

        // Drop all file chunk requests so that below relocation takes forever and we're guaranteed to run the snapshot in parallel to it
        for (String nodeName : dataNodes) {
            ((MockTransportService) internalCluster().getInstance(TransportService.class, nodeName)).addSendBehavior(
                (connection, requestId, action, request, options) -> {
                    if (PeerRecoveryTargetService.Actions.FILE_CHUNK.equals(action)) {
                        return;
                    }
                    connection.sendRequest(requestId, action, request, options);
                }
            );
        }

        logger.info("--> start relocations");
        allowNodes(indexName, 1);

        logger.info("--> wait for relocations to start");

        assertBusy(
            () -> assertThat(clusterAdmin().prepareHealth(indexName).execute().actionGet().getRelocatingShards(), greaterThan(0)),
            1L,
            TimeUnit.MINUTES
        );

        logger.info("--> snapshot");
        clusterAdmin().prepareCreateSnapshot(repoName, "test-snap")
            .setWaitForCompletion(false)
            .setPartial(true)
            .setIndices(indexName)
            .get();

        assertAcked(client().admin().indices().prepareDelete(indexName));

        awaitNoMoreRunningOperations();
        SnapshotInfo snapshotInfo = getSnapshot(repoName, "test-snap");
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
        logger.info("--> done");
    }

    public void testPartialSnapshotsDoNotRecordDeletedShardFailures() throws Exception {
        internalCluster().startMasterOnlyNodes(1);
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");

        final String firstIndex = "index-one";
        createIndexWithContent(firstIndex);
        final String secondIndex = "index-two";
        createIndexWithContent(secondIndex);

        final String snapshot = "snapshot-one";
        blockDataNode(repoName, dataNode);
        final ActionFuture<CreateSnapshotResponse> snapshotResponse = startFullSnapshot(repoName, snapshot, true);
        waitForBlock(dataNode, repoName);

        assertAcked(client().admin().indices().prepareDelete(firstIndex));

        unblockNode(repoName, dataNode);

        SnapshotInfo snapshotInfo = assertSuccessful(snapshotResponse);
        assertThat(snapshotInfo.shardFailures(), empty());
    }

    public void testDeleteIndexDuringSnapshot() throws Exception {
        final String indexName = "test-idx";
        assertAcked(prepareCreate(indexName, 1, indexSettingsNoReplicas(1)));
        ensureGreen();
        indexRandomDocs(indexName, 100);

        final String repoName = "test-repo";
        createRepository(repoName, "fs");

        final String firstSnapshotName = "test-snap";
        createSnapshot(repoName, firstSnapshotName, List.of(indexName));
        final int concurrentLoops = randomIntBetween(2, 5);
        final List<Future<Void>> futures = new ArrayList<>(concurrentLoops);
        for (int i = 0; i < concurrentLoops; i++) {
            final PlainActionFuture<Void> future = PlainActionFuture.newFuture();
            futures.add(future);
            startSnapshotDeleteLoop(repoName, indexName, "test-snap-" + i, future);
        }

        Thread.sleep(200);

        logger.info("--> delete index");
        assertAcked(admin().indices().prepareDelete(indexName));

        for (Future<Void> future : futures) {
            future.get();
        }

        logger.info("--> restore snapshot 1");
        clusterAdmin().prepareRestoreSnapshot(repoName, firstSnapshotName).get();
        ensureGreen(indexName);
    }

    // create and delete a snapshot of the given name and for the given single index in a loop until the index is removed from the cluster
    // at which point doneListener is resolved
    private void startSnapshotDeleteLoop(String repoName, String indexName, String snapshotName, ActionListener<Void> doneListener) {
        clusterAdmin().prepareCreateSnapshot(repoName, snapshotName)
            .setWaitForCompletion(true)
            .setPartial(true)
            .setIndices(indexName)
            .execute(new ActionListener<>() {
                @Override
                public void onResponse(CreateSnapshotResponse createSnapshotResponse) {
                    clusterAdmin().prepareDeleteSnapshot(repoName, snapshotName)
                        .execute(ActionTestUtils.assertNoFailureListener(acknowledgedResponse -> {
                            assertAcked(acknowledgedResponse);
                            startSnapshotDeleteLoop(repoName, indexName, snapshotName, doneListener);
                        }));
                }

                @Override
                public void onFailure(Exception e) {
                    assertThat(e, instanceOf(IndexNotFoundException.class));
                    doneListener.onResponse(null);
                }
            });
    }

    public void testGetReposWithWildcard() {
        internalCluster().startMasterOnlyNode();
        List<RepositoryMetadata> repositoryMetadata = client().admin().cluster().prepareGetRepositories("*").get().repositories();
        assertThat(repositoryMetadata, empty());
    }

    public void testConcurrentSnapshotAndRepoDelete() throws Exception {
        internalCluster().startMasterOnlyNodes(1);
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "fs");

        // create a few snapshots so deletes will run for a while
        final int snapshotCount = randomIntBetween(10, 25);
        final List<String> snapshotNames = createNSnapshots(repoName, snapshotCount);

        // concurrently trigger repository and snapshot deletes
        final List<ActionFuture<AcknowledgedResponse>> deleteFutures = new ArrayList<>(snapshotCount);
        final ActionFuture<AcknowledgedResponse> deleteRepoFuture = clusterAdmin().prepareDeleteRepository(repoName).execute();
        for (String snapshotName : snapshotNames) {
            deleteFutures.add(clusterAdmin().prepareDeleteSnapshot(repoName, snapshotName).execute());
        }

        try {
            assertAcked(deleteRepoFuture.actionGet());
        } catch (Exception e) {
            assertThat(
                e.getMessage(),
                containsString(
                    "trying to modify or unregister repository [test-repo] that is currently used (snapshot deletion is in progress)"
                )
            );
        }
        for (ActionFuture<AcknowledgedResponse> deleteFuture : deleteFutures) {
            try {
                assertAcked(deleteFuture.actionGet());
            } catch (RepositoryException e) {
                assertThat(
                    e.getMessage(),
                    either(containsString("[test-repo] repository is not in started state")).or(containsString("[test-repo] missing"))
                );
            }
        }
    }

    private long calculateTotalFilesSize(List<Path> files) {
        return files.stream().mapToLong(f -> {
            try {
                return Files.size(f);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }).sum();
    }

    private static List<Path> findRepoMetaBlobs(Path repoPath) throws IOException {
        List<Path> files = new ArrayList<>();
        forEachFileRecursively(repoPath.resolve("indices"), ((file, basicFileAttributes) -> {
            final String fileName = file.getFileName().toString();
            if (fileName.startsWith(BlobStoreRepository.METADATA_PREFIX) && fileName.endsWith(".dat")) {
                files.add(file);
            }
        }));
        return files;
    }

    private List<Path> scanSnapshotFolder(Path repoPath) throws IOException {
        List<Path> files = new ArrayList<>();
        forEachFileRecursively(repoPath.resolve("indices"), ((file, basicFileAttributes) -> {
            if (file.getFileName().toString().startsWith("__")) {
                files.add(file);
            }
        }));
        return files;
    }
}
