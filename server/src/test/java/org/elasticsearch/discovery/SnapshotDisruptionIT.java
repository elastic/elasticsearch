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
package org.elasticsearch.discovery;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotMissingException;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Tests snapshot operations during disruptions.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, transportClientRatio = 0, autoMinMasterNodes = false)
@TestLogging("org.elasticsearch.snapshot:TRACE")
public class SnapshotDisruptionIT extends AbstractDisruptionTestCase {

    public void testDisruptionOnSnapshotInitialization() throws Exception {
        final Settings settings = Settings.builder()
            .put(DEFAULT_SETTINGS)
            .put(DiscoverySettings.COMMIT_TIMEOUT_SETTING.getKey(), "30s") // wait till cluster state is committed
            .build();
        final String idxName = "test";
        configureCluster(settings, 4, null, 2);
        final List<String> allMasterEligibleNodes = internalCluster().startMasterOnlyNodes(3);
        final String dataNode = internalCluster().startDataOnlyNode();
        ensureStableCluster(4);

        createRandomIndex(idxName);

        logger.info("-->  creating repository");
        assertAcked(client().admin().cluster().preparePutRepository("test-repo")
            .setType("fs").setSettings(Settings.builder()
                .put("location", randomRepoPath())
                .put("compress", randomBoolean())
                .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));

        // Writing incompatible snapshot can cause this test to fail due to a race condition in repo initialization
        // by the current master and the former master. It is not causing any issues in real life scenario, but
        // might make this test to fail. We are going to complete initialization of the snapshot to prevent this failures.
        logger.info("-->  initializing the repository");
        assertEquals(SnapshotState.SUCCESS, client().admin().cluster().prepareCreateSnapshot("test-repo", "test-snap-1")
            .setWaitForCompletion(true).setIncludeGlobalState(true).setIndices().get().getSnapshotInfo().state());

        final String masterNode1 = internalCluster().getMasterName();
        Set<String> otherNodes = new HashSet<>();
        otherNodes.addAll(allMasterEligibleNodes);
        otherNodes.remove(masterNode1);
        otherNodes.add(dataNode);

        NetworkDisruption networkDisruption =
            new NetworkDisruption(new NetworkDisruption.TwoPartitions(Collections.singleton(masterNode1), otherNodes),
                new NetworkDisruption.NetworkUnresponsive());
        internalCluster().setDisruptionScheme(networkDisruption);

        ClusterService clusterService = internalCluster().clusterService(masterNode1);
        CountDownLatch disruptionStarted = new CountDownLatch(1);
        clusterService.addListener(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                SnapshotsInProgress snapshots = event.state().custom(SnapshotsInProgress.TYPE);
                if (snapshots != null && snapshots.entries().size() > 0) {
                    if (snapshots.entries().get(0).state() == SnapshotsInProgress.State.INIT) {
                        // The snapshot started, we can start disruption so the INIT state will arrive to another master node
                        logger.info("--> starting disruption");
                        networkDisruption.startDisrupting();
                        clusterService.removeListener(this);
                        disruptionStarted.countDown();
                    }
                }
            }
        });

        logger.info("--> starting snapshot");
        ActionFuture<CreateSnapshotResponse> future = client(masterNode1).admin().cluster()
            .prepareCreateSnapshot("test-repo", "test-snap-2").setWaitForCompletion(false).setIndices(idxName).execute();

        logger.info("--> waiting for disruption to start");
        assertTrue(disruptionStarted.await(1, TimeUnit.MINUTES));

        logger.info("--> wait until the snapshot is done");
        assertBusy(() -> {
            SnapshotsInProgress snapshots = dataNodeClient().admin().cluster().prepareState().setLocal(true).get().getState()
                .custom(SnapshotsInProgress.TYPE);
            if (snapshots != null && snapshots.entries().size() > 0) {
                logger.info("Current snapshot state [{}]", snapshots.entries().get(0).state());
                fail("Snapshot is still running");
            } else {
                logger.info("Snapshot is no longer in the cluster state");
            }
        }, 1, TimeUnit.MINUTES);

        logger.info("--> verify that snapshot was successful or no longer exist");
        assertBusy(() -> {
            try {
                GetSnapshotsResponse snapshotsStatusResponse = dataNodeClient().admin().cluster().prepareGetSnapshots("test-repo")
                    .setSnapshots("test-snap-2").get();
                SnapshotInfo snapshotInfo = snapshotsStatusResponse.getSnapshots().get(0);
                assertEquals(SnapshotState.SUCCESS, snapshotInfo.state());
                assertEquals(snapshotInfo.totalShards(), snapshotInfo.successfulShards());
                assertEquals(0, snapshotInfo.failedShards());
                logger.info("--> done verifying");
            } catch (SnapshotMissingException exception) {
                logger.info("--> snapshot doesn't exist");
            }
        }, 1, TimeUnit.MINUTES);

        logger.info("--> stopping disrupting");
        networkDisruption.stopDisrupting();
        ensureStableCluster(4, masterNode1);
        logger.info("--> done");

        try {
            future.get();
        } catch (Exception ex) {
            logger.info("--> got exception from hanged master", ex);
            Throwable cause = ex.getCause();
            assertThat(cause, instanceOf(MasterNotDiscoveredException.class));
            cause = cause.getCause();
            assertThat(cause, instanceOf(Discovery.FailedToCommitClusterStateException.class));
        }
    }

    private void createRandomIndex(String idxName) throws ExecutionException, InterruptedException {
        assertAcked(prepareCreate(idxName, 0, Settings.builder().put("number_of_shards", between(1, 20))
            .put("number_of_replicas", 0)));
        logger.info("--> indexing some data");
        final int numdocs = randomIntBetween(10, 100);
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numdocs];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex(idxName, "type1", Integer.toString(i)).setSource("field1", "bar " + i);
        }
        indexRandom(true, builders);
    }
}
