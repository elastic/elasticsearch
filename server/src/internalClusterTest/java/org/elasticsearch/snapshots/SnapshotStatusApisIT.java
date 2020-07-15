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

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotIndexShardStage;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotIndexShardStatus;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotStats;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotStatus;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class SnapshotStatusApisIT extends AbstractSnapshotIntegTestCase {

    public void testStatusApiConsistency() {
        Client client = client();

        createRepository("test-repo", "fs");

        createIndex("test-idx-1", "test-idx-2", "test-idx-3");
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            indexDoc("test-idx-1", Integer.toString(i), "foo", "bar" + i);
            indexDoc("test-idx-2", Integer.toString(i), "foo", "baz" + i);
            indexDoc("test-idx-3", Integer.toString(i), "foo", "baz" + i);
        }
        refresh();

        createFullSnapshot("test-repo", "test-snap");

        List<SnapshotInfo> snapshotInfos =
            client.admin().cluster().prepareGetSnapshots("test-repo").get().getSnapshots("test-repo");
        assertThat(snapshotInfos.size(), equalTo(1));
        SnapshotInfo snapshotInfo = snapshotInfos.get(0);
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfo.version(), equalTo(Version.CURRENT));

        final List<SnapshotStatus> snapshotStatus = client.admin().cluster().snapshotsStatus(
            new SnapshotsStatusRequest("test-repo", new String[]{"test-snap"})).actionGet().getSnapshots();
        assertThat(snapshotStatus.size(), equalTo(1));
        final SnapshotStatus snStatus = snapshotStatus.get(0);
        assertEquals(snStatus.getStats().getStartTime(), snapshotInfo.startTime());
        assertEquals(snStatus.getStats().getTime(), snapshotInfo.endTime() - snapshotInfo.startTime());
    }

    public void testStatusAPICallInProgressSnapshot() throws Exception {
        Client client = client();

        createRepository("test-repo", "mock", Settings.builder().put("location", randomRepoPath()).put("block_on_data", true));

        createIndex("test-idx-1");
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            indexDoc("test-idx-1", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();

        logger.info("--> snapshot");
        ActionFuture<CreateSnapshotResponse> createSnapshotResponseActionFuture =
            client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(true).execute();

        logger.info("--> wait for data nodes to get blocked");
        waitForBlockOnAnyDataNode("test-repo", TimeValue.timeValueMinutes(1));

        assertBusy(() -> {
            try {
                assertEquals(SnapshotsInProgress.State.STARTED, client.admin().cluster().snapshotsStatus(
                        new SnapshotsStatusRequest("test-repo", new String[]{"test-snap"})).actionGet().getSnapshots().get(0)
                        .getState());
            } catch (SnapshotMissingException sme) {
                throw new AssertionError(sme);
            }
        }, 1L, TimeUnit.MINUTES);

        logger.info("--> unblock all data nodes");
        unblockAllDataNodes("test-repo");

        logger.info("--> wait for snapshot to finish");
        createSnapshotResponseActionFuture.actionGet();
    }

    public void testExceptionOnMissingSnapBlob() throws IOException {
        disableRepoConsistencyCheck("This test intentionally corrupts the repository");

        final Path repoPath = randomRepoPath();
        createRepository("test-repo", "fs", repoPath);

        final SnapshotInfo snapshotInfo = createFullSnapshot("test-repo", "test-snap");
        logger.info("--> delete snap-${uuid}.dat file for this snapshot to simulate concurrent delete");
        IOUtils.rm(repoPath.resolve(BlobStoreRepository.SNAPSHOT_PREFIX + snapshotInfo.snapshotId().getUUID() + ".dat"));

        GetSnapshotsResponse snapshotsResponse = client().admin().cluster()
            .getSnapshots(new GetSnapshotsRequest(new String[] {"test-repo"}, new String[] {"test-snap"})).actionGet();
        assertThat(snapshotsResponse.getFailedResponses().get("test-repo"), instanceOf(SnapshotMissingException.class));
    }

    public void testExceptionOnMissingShardLevelSnapBlob() throws IOException {
        disableRepoConsistencyCheck("This test intentionally corrupts the repository");

        final Path repoPath = randomRepoPath();
        createRepository("test-repo", "fs", repoPath);

        createIndex("test-idx-1");
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            indexDoc("test-idx-1", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();

        final SnapshotInfo snapshotInfo = createFullSnapshot("test-repo", "test-snap");

        logger.info("--> delete shard-level snap-${uuid}.dat file for one shard in this snapshot to simulate concurrent delete");
        final String indexRepoId = getRepositoryData("test-repo").resolveIndexId(snapshotInfo.indices().get(0)).getId();
        IOUtils.rm(repoPath.resolve("indices").resolve(indexRepoId).resolve("0").resolve(
            BlobStoreRepository.SNAPSHOT_PREFIX + snapshotInfo.snapshotId().getUUID() + ".dat"));

        expectThrows(SnapshotMissingException.class, () -> client().admin().cluster()
            .prepareSnapshotStatus("test-repo").setSnapshots("test-snap").execute().actionGet());
    }

    public void testGetSnapshotsWithoutIndices() {
        createRepository("test-repo", "fs");

        logger.info("--> snapshot");
        final SnapshotInfo snapshotInfo =
            client().admin().cluster().prepareCreateSnapshot("test-repo", "test-snap")
                .setIndices().setWaitForCompletion(true).get().getSnapshotInfo();

        assertThat(snapshotInfo.state(), is(SnapshotState.SUCCESS));
        assertThat(snapshotInfo.totalShards(), is(0));

        logger.info("--> verify that snapshot without index shows up in non-verbose listing");
        final List<SnapshotInfo> snapshotInfos =
            client().admin().cluster().prepareGetSnapshots("test-repo").setVerbose(false).get().getSnapshots("test-repo");
        assertThat(snapshotInfos, hasSize(1));
        final SnapshotInfo found = snapshotInfos.get(0);
        assertThat(found.snapshotId(), is(snapshotInfo.snapshotId()));
        assertThat(found.state(), is(SnapshotState.SUCCESS));
    }

    /**
     * Tests the following sequence of steps:
     * 1. Start snapshot of two shards (both located on separate data nodes).
     * 2. Have one of the shards snapshot completely and the other block
     * 3. Restart the data node that completed its shard snapshot
     * 4. Make sure that snapshot status APIs show correct file-counts and -sizes
     *
     * @throws Exception on failure
     */
    public void testCorrectCountsForDoneShards() throws Exception {
        final String indexOne = "index-1";
        final String indexTwo = "index-2";
        final List<String> dataNodes = internalCluster().startDataOnlyNodes(2);
        final String dataNodeOne = dataNodes.get(0);
        final String dataNodeTwo = dataNodes.get(1);

        createIndex(indexOne, singleShardOneNode(dataNodeOne));
        indexDoc(indexOne, "some_doc_id", "foo", "bar");
        createIndex(indexTwo, singleShardOneNode(dataNodeTwo));
        indexDoc(indexTwo, "some_doc_id", "foo", "bar");

        final String repoName = "test-repo";
        createRepository(repoName, "mock");

        blockDataNode(repoName, dataNodeOne);

        final String snapshotOne = "snap-1";
        // restarting a data node below so using a master client here
        final ActionFuture<CreateSnapshotResponse> responseSnapshotOne = internalCluster().masterClient().admin()
            .cluster().prepareCreateSnapshot(repoName, snapshotOne).setWaitForCompletion(true).execute();

        assertBusy(() -> {
            final SnapshotStatus snapshotStatusOne = getSnapshotStatus(repoName, snapshotOne);
            final SnapshotIndexShardStatus snapshotShardState = stateFirstShard(snapshotStatusOne, indexTwo);
            assertThat(snapshotShardState.getStage(), is(SnapshotIndexShardStage.DONE));
            assertThat(snapshotShardState.getStats().getTotalFileCount(), greaterThan(0));
            assertThat(snapshotShardState.getStats().getTotalSize(), greaterThan(0L));
        }, 30L, TimeUnit.SECONDS);

        final SnapshotStats snapshotShardStats =
            stateFirstShard(getSnapshotStatus(repoName, snapshotOne), indexTwo).getStats();
        final int totalFiles = snapshotShardStats.getTotalFileCount();
        final long totalFileSize = snapshotShardStats.getTotalSize();

        internalCluster().restartNode(dataNodeTwo);

        final SnapshotIndexShardStatus snapshotShardStateAfterNodeRestart =
            stateFirstShard(getSnapshotStatus(repoName, snapshotOne), indexTwo);
        assertThat(snapshotShardStateAfterNodeRestart.getStage(), is(SnapshotIndexShardStage.DONE));
        assertThat(snapshotShardStateAfterNodeRestart.getStats().getTotalFileCount(), equalTo(totalFiles));
        assertThat(snapshotShardStateAfterNodeRestart.getStats().getTotalSize(), equalTo(totalFileSize));

        unblockAllDataNodes(repoName);
        assertThat(responseSnapshotOne.get().getSnapshotInfo().state(), is(SnapshotState.SUCCESS));

        // indexing another document to the second index so it will do writes during the snapshot and we can block on those writes
        indexDoc(indexTwo, "some_other_doc_id", "foo", "other_bar");

        blockDataNode(repoName, dataNodeTwo);

        final String snapshotTwo = "snap-2";
        final ActionFuture<CreateSnapshotResponse> responseSnapshotTwo =
            client().admin().cluster().prepareCreateSnapshot(repoName, snapshotTwo).setWaitForCompletion(true).execute();

        waitForBlock(dataNodeTwo, repoName, TimeValue.timeValueSeconds(30L));

        assertBusy(() -> {
            final SnapshotStatus snapshotStatusOne = getSnapshotStatus(repoName, snapshotOne);
            final SnapshotStatus snapshotStatusTwo = getSnapshotStatus(repoName, snapshotTwo);
            final SnapshotIndexShardStatus snapshotShardStateOne = stateFirstShard(snapshotStatusOne, indexOne);
            final SnapshotIndexShardStatus snapshotShardStateTwo = stateFirstShard(snapshotStatusTwo, indexOne);
            assertThat(snapshotShardStateOne.getStage(), is(SnapshotIndexShardStage.DONE));
            assertThat(snapshotShardStateTwo.getStage(), is(SnapshotIndexShardStage.DONE));
            final int totalFilesShardOne = snapshotShardStateOne.getStats().getTotalFileCount();
            final long totalSizeShardOne = snapshotShardStateOne.getStats().getTotalSize();
            assertThat(totalFilesShardOne, greaterThan(0));
            assertThat(totalSizeShardOne, greaterThan(0L));
            assertThat(totalFilesShardOne, equalTo(snapshotShardStateTwo.getStats().getTotalFileCount()));
            assertThat(totalSizeShardOne, equalTo(snapshotShardStateTwo.getStats().getTotalSize()));
            assertThat(snapshotShardStateTwo.getStats().getIncrementalFileCount(), equalTo(0));
            assertThat(snapshotShardStateTwo.getStats().getIncrementalSize(), equalTo(0L));
        }, 30L, TimeUnit.SECONDS);

        unblockAllDataNodes(repoName);
        assertThat(responseSnapshotTwo.get().getSnapshotInfo().state(), is(SnapshotState.SUCCESS));
    }

    private static SnapshotIndexShardStatus stateFirstShard(SnapshotStatus snapshotStatus, String indexName) {
        return snapshotStatus.getIndices().get(indexName).getShards().get(0);
    }

    private static SnapshotStatus getSnapshotStatus(String repoName, String snapshotName) {
        try {
            return client().admin().cluster().prepareSnapshotStatus(repoName).setSnapshots(snapshotName)
                .get().getSnapshots().get(0);
        } catch (SnapshotMissingException e) {
            throw new AssertionError(e);
        }
    }

    private static Settings singleShardOneNode(String node) {
        return indexSettingsNoReplicas(1).put("index.routing.allocation.include._name", node).build();
    }
}
