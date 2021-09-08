/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.discovery;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.SnapshotException;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotMissingException;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFutureThrows;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;

/**
 * Tests snapshot operations during disruptions.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SnapshotDisruptionIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class, MockRepository.Plugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(AbstractDisruptionTestCase.DEFAULT_SETTINGS)
            .build();
    }

    public void testDisruptionAfterFinalization() throws Exception {
        final String idxName = "test";
        internalCluster().startMasterOnlyNodes(3);
        final String dataNode = internalCluster().startDataOnlyNode();
        ensureStableCluster(4);

        createRandomIndex(idxName);

        createRepository("test-repo", "fs");

        final String masterNode1 = internalCluster().getMasterName();

        NetworkDisruption networkDisruption = isolateMasterDisruption(NetworkDisruption.UNRESPONSIVE);
        internalCluster().setDisruptionScheme(networkDisruption);

        ClusterService clusterService = internalCluster().clusterService(masterNode1);
        CountDownLatch disruptionStarted = new CountDownLatch(1);
        clusterService.addListener(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                SnapshotsInProgress snapshots = event.state().custom(SnapshotsInProgress.TYPE);
                if (snapshots != null && snapshots.entries().size() > 0) {
                    final SnapshotsInProgress.Entry snapshotEntry = snapshots.entries().get(0);
                    if (snapshotEntry.state() == SnapshotsInProgress.State.SUCCESS) {
                        final RepositoriesMetadata repoMeta =
                            event.state().metadata().custom(RepositoriesMetadata.TYPE);
                        final RepositoryMetadata metadata = repoMeta.repository("test-repo");
                        if (metadata.pendingGeneration() > snapshotEntry.repositoryStateId()) {
                            logger.info("--> starting disruption");
                            networkDisruption.startDisrupting();
                            clusterService.removeListener(this);
                            disruptionStarted.countDown();
                        }
                    }
                }
            }
        });

        final String snapshot = "test-snap";

        logger.info("--> starting snapshot");
        ActionFuture<CreateSnapshotResponse> future = client(masterNode1).admin().cluster()
            .prepareCreateSnapshot("test-repo", snapshot).setWaitForCompletion(true)
            .setIndices(idxName).execute();

        logger.info("--> waiting for disruption to start");
        assertTrue(disruptionStarted.await(1, TimeUnit.MINUTES));

        awaitNoMoreRunningOperations(dataNode);

        logger.info("--> verify that snapshot was successful or no longer exist");
        assertBusy(() -> {
            try {
                assertSnapshotExists("test-repo", snapshot);
            } catch (SnapshotMissingException exception) {
                logger.info("--> done verifying, snapshot doesn't exist");
            }
        }, 1, TimeUnit.MINUTES);

        logger.info("--> stopping disrupting");
        networkDisruption.stopDisrupting();
        ensureStableCluster(4, masterNode1);
        logger.info("--> done");

        try {
            future.get();
            fail("Should have failed because the node disconnected from the cluster during snapshot finalization");
        } catch (Exception ex) {
            final SnapshotException sne = (SnapshotException) ExceptionsHelper.unwrap(ex, SnapshotException.class);
            assertNotNull(sne);
            assertThat(
                sne.getMessage(), either(endsWith(" Failed to update cluster state during snapshot finalization"))
                            .or(endsWith(" no longer master")));
            assertThat(sne.getSnapshotName(), is(snapshot));
        }

        awaitNoMoreRunningOperations(dataNode);
    }

    public void testDisruptionAfterShardFinalization() throws Exception {
        final String idxName = "test";
        internalCluster().startMasterOnlyNodes(1);
        internalCluster().startDataOnlyNode();
        ensureStableCluster(2);
        createIndex(idxName);
        index(idxName, JsonXContent.contentBuilder().startObject().field("foo", "bar").endObject());

        final String repoName = "test-repo";
        createRepository(repoName, "mock");

        final String masterNode = internalCluster().getMasterName();

        blockAllDataNodes(repoName);

        final String snapshot = "test-snap";
        logger.info("--> starting snapshot");
        ActionFuture<CreateSnapshotResponse> future = client(masterNode).admin().cluster()
                .prepareCreateSnapshot(repoName, snapshot).setWaitForCompletion(true).execute();

        waitForBlockOnAnyDataNode(repoName);

        NetworkDisruption networkDisruption = isolateMasterDisruption(NetworkDisruption.DISCONNECT);
        internalCluster().setDisruptionScheme(networkDisruption);
        networkDisruption.startDisrupting();

        final CreateSnapshotResponse createSnapshotResponse = future.get();
        final SnapshotInfo snapshotInfo = createSnapshotResponse.getSnapshotInfo();
        assertThat(snapshotInfo.state(), is(SnapshotState.PARTIAL));

        logger.info("--> stopping disrupting");
        networkDisruption.stopDisrupting();
        unblockAllDataNodes(repoName);

        ensureStableCluster(2, masterNode);
        logger.info("--> done");

        logger.info("--> recreate the index with potentially different shard counts");
        client().admin().indices().prepareDelete(idxName).get();
        createIndex(idxName);
        index(idxName, JsonXContent.contentBuilder().startObject().field("foo", "bar").endObject());

        logger.info("--> run a snapshot that fails to finalize but succeeds on the data node");
        blockMasterFromFinalizingSnapshotOnIndexFile(repoName);
        final ActionFuture<CreateSnapshotResponse> snapshotFuture =
                client(masterNode).admin().cluster().prepareCreateSnapshot(repoName, "snapshot-2").setWaitForCompletion(true).execute();
        waitForBlock(masterNode, repoName);
        unblockNode(repoName, masterNode);
        assertFutureThrows(snapshotFuture, SnapshotException.class);

        logger.info("--> create a snapshot expected to be successful");
        final CreateSnapshotResponse successfulSnapshot =
                client(masterNode).admin().cluster().prepareCreateSnapshot(repoName, "snapshot-2").setWaitForCompletion(true).get();
        final SnapshotInfo successfulSnapshotInfo = successfulSnapshot.getSnapshotInfo();
        assertThat(successfulSnapshotInfo.state(), is(SnapshotState.SUCCESS));

        logger.info("--> making sure snapshot delete works out cleanly");
        assertAcked(client().admin().cluster().prepareDeleteSnapshot(repoName, "snapshot-2").get());
    }

    public void testMasterFailOverDuringShardSnapshots() throws Exception {
        internalCluster().startMasterOnlyNodes(3);
        final String dataNode = internalCluster().startDataOnlyNode();
        ensureStableCluster(4);
        final String repoName = "test-repo";
        createRepository(repoName, "mock");

        final String indexName = "index-one";
        createIndex(indexName);
        client().prepareIndex(indexName).setSource("foo", "bar").get();

        blockDataNode(repoName, dataNode);

        logger.info("--> create snapshot via master node client");
        final ActionFuture<CreateSnapshotResponse> snapshotResponse = internalCluster().masterClient().admin().cluster()
                .prepareCreateSnapshot(repoName, "test-snap").setWaitForCompletion(true).execute();

        waitForBlock(dataNode, repoName);

        final NetworkDisruption networkDisruption = isolateMasterDisruption(NetworkDisruption.DISCONNECT);
        internalCluster().setDisruptionScheme(networkDisruption);
        networkDisruption.startDisrupting();
        ensureStableCluster(3, dataNode);
        unblockNode(repoName, dataNode);

        networkDisruption.stopDisrupting();
        awaitNoMoreRunningOperations(dataNode);

        logger.info("--> make sure isolated master responds to snapshot request");
        final SnapshotException sne =
                expectThrows(SnapshotException.class, () -> snapshotResponse.actionGet(TimeValue.timeValueSeconds(30L)));
        assertThat(sne.getMessage(), endsWith("no longer master"));
    }

    private void assertSnapshotExists(String repository, String snapshot) {
        GetSnapshotsResponse snapshotsStatusResponse = dataNodeClient().admin().cluster().prepareGetSnapshots(repository)
                .setSnapshots(snapshot).get();
        SnapshotInfo snapshotInfo = snapshotsStatusResponse.getSnapshots().get(0);
        assertEquals(SnapshotState.SUCCESS, snapshotInfo.state());
        assertEquals(snapshotInfo.totalShards(), snapshotInfo.successfulShards());
        assertEquals(0, snapshotInfo.failedShards());
        logger.info("--> done verifying, snapshot exists");
    }

    private void createRandomIndex(String idxName) throws InterruptedException {
        assertAcked(prepareCreate(idxName, 0, indexSettingsNoReplicas(between(1, 5))));
        logger.info("--> indexing some data");
        final int numdocs = randomIntBetween(10, 100);
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numdocs];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex(idxName).setId(Integer.toString(i)).setSource("field1", "bar " + i);
        }
        indexRandom(true, builders);
    }
}
