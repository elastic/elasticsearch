/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.snapshots;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotIndexShardStage;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotIndexShardStatus;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotStats;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotStatus;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.oneOf;

public class SnapshotStatusApisIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.getKey(), 0) // We have tests that check by-timestamp order
            .put(LARGE_SNAPSHOT_POOL_SETTINGS) // we have #testGetSnapshotsWithSnapshotInProgress which needs many threads to ensure
            // its snapshot pool does not become fully blocked on data nodes when blocking on data files
            .build();
    }

    public void testStatusApiConsistency() throws Exception {
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

        List<SnapshotInfo> snapshotInfos = clusterAdmin().prepareGetSnapshots("test-repo").get().getSnapshots();
        assertThat(snapshotInfos.size(), equalTo(1));
        SnapshotInfo snapshotInfo = snapshotInfos.get(0);
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfo.version(), equalTo(IndexVersion.CURRENT));

        final List<SnapshotStatus> snapshotStatus = clusterAdmin().prepareSnapshotStatus("test-repo")
            .setSnapshots("test-snap")
            .get()
            .getSnapshots();
        assertThat(snapshotStatus.size(), equalTo(1));
        final SnapshotStatus snStatus = snapshotStatus.get(0);
        assertEquals(snStatus.getStats().getStartTime(), snapshotInfo.startTime());
        assertEquals(snStatus.getStats().getTime(), snapshotInfo.endTime() - snapshotInfo.startTime());
    }

    public void testStatusAPICallInProgressSnapshot() throws Exception {
        createRepository("test-repo", "mock", Settings.builder().put("location", randomRepoPath()).put("block_on_data", true));

        createIndex("test-idx-1");
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            indexDoc("test-idx-1", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();

        logger.info("--> snapshot");
        ActionFuture<CreateSnapshotResponse> createSnapshotResponseActionFuture = startFullSnapshot("test-repo", "test-snap");

        logger.info("--> wait for data nodes to get blocked");
        waitForBlockOnAnyDataNode("test-repo");
        awaitNumberOfSnapshotsInProgress(1);
        assertEquals(
            SnapshotsInProgress.State.STARTED,
            clusterAdmin().prepareSnapshotStatus("test-repo").setSnapshots("test-snap").get().getSnapshots().get(0).getState()
        );

        logger.info("--> unblock all data nodes");
        unblockAllDataNodes("test-repo");

        assertSuccessful(createSnapshotResponseActionFuture);
    }

    public void testExceptionOnMissingSnapBlob() throws IOException {
        disableRepoConsistencyCheck("This test intentionally corrupts the repository");

        final Path repoPath = randomRepoPath();
        createRepository("test-repo", "fs", repoPath);

        final SnapshotInfo snapshotInfo = createFullSnapshot("test-repo", "test-snap");
        logger.info("--> delete snap-${uuid}.dat file for this snapshot to simulate concurrent delete");
        IOUtils.rm(repoPath.resolve(BlobStoreRepository.SNAPSHOT_PREFIX + snapshotInfo.snapshotId().getUUID() + ".dat"));

        expectThrows(
            SnapshotMissingException.class,
            () -> clusterAdmin().prepareGetSnapshots("test-repo").setSnapshots("test-snap").execute().actionGet()
        );
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
        IOUtils.rm(
            repoPath.resolve("indices")
                .resolve(indexRepoId)
                .resolve("0")
                .resolve(BlobStoreRepository.SNAPSHOT_PREFIX + snapshotInfo.snapshotId().getUUID() + ".dat")
        );

        expectThrows(
            SnapshotMissingException.class,
            () -> clusterAdmin().prepareSnapshotStatus("test-repo").setSnapshots("test-snap").execute().actionGet()
        );
    }

    public void testGetSnapshotsWithoutIndices() throws Exception {
        createRepository("test-repo", "fs");

        logger.info("--> snapshot");
        final SnapshotInfo snapshotInfo = assertSuccessful(
            clusterAdmin().prepareCreateSnapshot("test-repo", "test-snap").setIndices().setWaitForCompletion(true).execute()
        );
        assertThat(snapshotInfo.totalShards(), is(0));

        logger.info("--> verify that snapshot without index shows up in non-verbose listing");
        final List<SnapshotInfo> snapshotInfos = clusterAdmin().prepareGetSnapshots("test-repo").setVerbose(false).get().getSnapshots();
        assertThat(snapshotInfos, hasSize(1));
        final SnapshotInfo found = snapshotInfos.get(0);
        assertThat(found.snapshotId(), is(snapshotInfo.snapshotId()));
        assertThat(found.state(), is(SnapshotState.SUCCESS));
        assertThat(found.indexSnapshotDetails(), anEmptyMap());
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
        final ActionFuture<CreateSnapshotResponse> responseSnapshotOne = internalCluster().masterClient()
            .admin()
            .cluster()
            .prepareCreateSnapshot(repoName, snapshotOne)
            .setWaitForCompletion(true)
            .execute();

        assertBusy(() -> {
            final SnapshotStatus snapshotStatusOne = getSnapshotStatus(repoName, snapshotOne);
            final SnapshotIndexShardStatus snapshotShardState = stateFirstShard(snapshotStatusOne, indexTwo);
            assertThat(snapshotShardState.getStage(), is(SnapshotIndexShardStage.DONE));
            assertThat(snapshotShardState.getStats().getTotalFileCount(), greaterThan(0));
            assertThat(snapshotShardState.getStats().getTotalSize(), greaterThan(0L));
        }, 30L, TimeUnit.SECONDS);

        final SnapshotStats snapshotShardStats = stateFirstShard(getSnapshotStatus(repoName, snapshotOne), indexTwo).getStats();
        final int totalFiles = snapshotShardStats.getTotalFileCount();
        final long totalFileSize = snapshotShardStats.getTotalSize();

        internalCluster().restartNode(dataNodeTwo);

        final SnapshotIndexShardStatus snapshotShardStateAfterNodeRestart = stateFirstShard(
            getSnapshotStatus(repoName, snapshotOne),
            indexTwo
        );
        assertThat(snapshotShardStateAfterNodeRestart.getStage(), is(SnapshotIndexShardStage.DONE));
        assertThat(snapshotShardStateAfterNodeRestart.getStats().getTotalFileCount(), equalTo(totalFiles));
        assertThat(snapshotShardStateAfterNodeRestart.getStats().getTotalSize(), equalTo(totalFileSize));

        unblockAllDataNodes(repoName);
        assertThat(responseSnapshotOne.get().getSnapshotInfo().state(), is(SnapshotState.SUCCESS));

        // indexing another document to the second index so it will do writes during the snapshot and we can block on those writes
        indexDoc(indexTwo, "some_other_doc_id", "foo", "other_bar");

        blockDataNode(repoName, dataNodeTwo);

        final String snapshotTwo = "snap-2";
        final ActionFuture<CreateSnapshotResponse> responseSnapshotTwo = clusterAdmin().prepareCreateSnapshot(repoName, snapshotTwo)
            .setWaitForCompletion(true)
            .execute();

        waitForBlock(dataNodeTwo, repoName);

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
        final SnapshotInfo snapshotInfo = responseSnapshotTwo.get().getSnapshotInfo();
        assertThat(snapshotInfo.state(), is(SnapshotState.SUCCESS));

        assertTrue(snapshotInfo.indexSnapshotDetails().toString(), snapshotInfo.indexSnapshotDetails().containsKey(indexOne));
        final SnapshotInfo.IndexSnapshotDetails indexSnapshotDetails = snapshotInfo.indexSnapshotDetails().get(indexOne);
        assertThat(indexSnapshotDetails.toString(), indexSnapshotDetails.getShardCount(), equalTo(1));
        assertThat(indexSnapshotDetails.toString(), indexSnapshotDetails.getMaxSegmentsPerShard(), greaterThanOrEqualTo(1));
        assertThat(indexSnapshotDetails.toString(), indexSnapshotDetails.getSize().getBytes(), greaterThan(0L));
    }

    public void testGetSnapshotsNoRepos() {
        ensureGreen();
        GetSnapshotsResponse getSnapshotsResponse = clusterAdmin().prepareGetSnapshots(new String[] { "_all" })
            .setSnapshots(randomFrom("_all", "*"))
            .get();

        assertTrue(getSnapshotsResponse.getSnapshots().isEmpty());
        assertTrue(getSnapshotsResponse.getFailures().isEmpty());
    }

    public void testGetSnapshotsMultipleRepos() throws Exception {
        final Client client = client();

        List<String> snapshotList = new ArrayList<>();
        List<String> repoList = new ArrayList<>();
        Map<String, List<String>> repo2SnapshotNames = new HashMap<>();

        logger.info("--> create an index and index some documents");
        final String indexName = "test-idx";
        createIndexWithRandomDocs(indexName, 10);
        final int numberOfShards = IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(
            client.admin().indices().prepareGetSettings(indexName).get().getIndexToSettings().get(indexName)
        );

        for (int repoIndex = 0; repoIndex < randomIntBetween(2, 5); repoIndex++) {
            final String repoName = "repo" + repoIndex;
            repoList.add(repoName);
            final Path repoPath = randomRepoPath();
            logger.info("--> create repository with name " + repoName);
            assertAcked(
                client.admin()
                    .cluster()
                    .preparePutRepository(repoName)
                    .setType("fs")
                    .setSettings(Settings.builder().put("location", repoPath).build())
            );
            List<String> snapshotNames = new ArrayList<>();
            repo2SnapshotNames.put(repoName, snapshotNames);

            for (int snapshotIndex = 0; snapshotIndex < randomIntBetween(2, 5); snapshotIndex++) {
                final String snapshotName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
                snapshotList.add(snapshotName);
                // Wait for at least 1ms to ensure that snapshots can be ordered by timestamp deterministically
                for (final ThreadPool threadPool : internalCluster().getInstances(ThreadPool.class)) {
                    final long startMillis = threadPool.absoluteTimeInMillis();
                    assertBusy(() -> assertThat(threadPool.absoluteTimeInMillis(), greaterThan(startMillis)));
                }
                logger.info("--> create snapshot with index {} and name {} in repository {}", snapshotIndex, snapshotName, repoName);
                CreateSnapshotResponse createSnapshotResponse = client.admin()
                    .cluster()
                    .prepareCreateSnapshot(repoName, snapshotName)
                    .setWaitForCompletion(true)
                    .setIndices(indexName)
                    .get();
                final SnapshotInfo snapshotInfo = createSnapshotResponse.getSnapshotInfo();
                assertThat(snapshotInfo.successfulShards(), greaterThan(0));
                assertTrue(snapshotInfo.indexSnapshotDetails().containsKey(indexName));
                final SnapshotInfo.IndexSnapshotDetails indexSnapshotDetails = snapshotInfo.indexSnapshotDetails().get(indexName);
                assertThat(indexSnapshotDetails.getShardCount(), equalTo(numberOfShards));
                assertThat(indexSnapshotDetails.getMaxSegmentsPerShard(), greaterThanOrEqualTo(1));
                assertThat(indexSnapshotDetails.getSize().getBytes(), greaterThan(0L));
                snapshotNames.add(snapshotName);
            }
        }

        logger.info("--> get and verify snapshots");
        GetSnapshotsResponse getSnapshotsResponse = client.admin()
            .cluster()
            .prepareGetSnapshots(randomFrom(new String[] { "_all" }, new String[] { "repo*" }, repoList.toArray(new String[0])))
            .setSnapshots(randomFrom("_all", "*"))
            .get();

        for (Map.Entry<String, List<String>> repo2Names : repo2SnapshotNames.entrySet()) {
            String repo = repo2Names.getKey();
            List<String> snapshotNames = repo2Names.getValue();
            List<SnapshotInfo> snapshots = getSnapshotsResponse.getSnapshots();
            assertEquals(
                snapshotNames,
                snapshots.stream().filter(s -> s.repository().equals(repo)).map(s -> s.snapshotId().getName()).toList()
            );
        }

        logger.info("--> specify all snapshot names with ignoreUnavailable=false");
        GetSnapshotsResponse getSnapshotsResponse2 = client.admin()
            .cluster()
            .prepareGetSnapshots(randomFrom("_all", "repo*"))
            .setIgnoreUnavailable(false)
            .setSnapshots(snapshotList.toArray(new String[0]))
            .get();

        for (String repo : repoList) {
            assertThat(getSnapshotsResponse2.getFailures().get(repo), instanceOf(SnapshotMissingException.class));
        }

        logger.info("--> specify all snapshot names with ignoreUnavailable=true");
        GetSnapshotsResponse getSnapshotsResponse3 = client.admin()
            .cluster()
            .prepareGetSnapshots(randomFrom("_all", "repo*"))
            .setIgnoreUnavailable(true)
            .setSnapshots(snapshotList.toArray(new String[0]))
            .get();

        for (Map.Entry<String, List<String>> repo2Names : repo2SnapshotNames.entrySet()) {
            String repo = repo2Names.getKey();
            List<String> snapshotNames = repo2Names.getValue();
            List<SnapshotInfo> snapshots = getSnapshotsResponse3.getSnapshots();
            assertEquals(
                snapshotNames,
                snapshots.stream().filter(s -> s.repository().equals(repo)).map(s -> s.snapshotId().getName()).toList()
            );
        }
    }

    public void testGetSnapshotsWithSnapshotInProgress() throws Exception {
        createRepository("test-repo", "mock", Settings.builder().put("location", randomRepoPath()).put("block_on_data", true));

        String indexName = "test-idx-1";
        createIndexWithContent(indexName, indexSettingsNoReplicas(randomIntBetween(2, 10)).build());
        ensureGreen();

        ActionFuture<CreateSnapshotResponse> createSnapshotResponseActionFuture = startFullSnapshot("test-repo", "test-snap");

        logger.info("--> wait for data nodes to get blocked");
        waitForBlockOnAnyDataNode("test-repo");
        awaitNumberOfSnapshotsInProgress(1);

        logger.info("--> wait for snapshots to get to a consistent state");
        awaitClusterState(state -> {
            SnapshotsInProgress snapshotsInProgress = state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
            Set<Snapshot> snapshots = snapshotsInProgress.asStream().map(SnapshotsInProgress.Entry::snapshot).collect(Collectors.toSet());
            if (snapshots.size() != 1) {
                return false;
            }
            var shards = snapshotsInProgress.snapshot(snapshots.iterator().next()).shards();
            long initShards = shards.values().stream().filter(v -> v.state() == SnapshotsInProgress.ShardState.INIT).count();
            long successShards = shards.entrySet()
                .stream()
                .filter(e -> e.getValue().state() == SnapshotsInProgress.ShardState.SUCCESS)
                .count();
            return successShards == shards.size() - 1 && initShards == 1;
        });

        GetSnapshotsResponse response1 = clusterAdmin().prepareGetSnapshots("test-repo")
            .setSnapshots("test-snap")
            .setIgnoreUnavailable(true)
            .get();
        List<SnapshotInfo> snapshotInfoList = response1.getSnapshots();
        assertEquals(1, snapshotInfoList.size());
        SnapshotInfo snapshotInfo = snapshotInfoList.get(0);
        assertEquals(SnapshotState.IN_PROGRESS, snapshotInfo.state());

        SnapshotStatus snapshotStatus = clusterAdmin().prepareSnapshotStatus().get().getSnapshots().get(0);
        assertThat(snapshotInfo.totalShards(), equalTo(snapshotStatus.getIndices().get(indexName).getShardsStats().getTotalShards()));
        assertThat(snapshotInfo.successfulShards(), equalTo(snapshotStatus.getIndices().get(indexName).getShardsStats().getDoneShards()));
        assertThat(snapshotInfo.shardFailures().size(), equalTo(0));

        String notExistedSnapshotName = "snapshot_not_exist";
        GetSnapshotsResponse response2 = clusterAdmin().prepareGetSnapshots("test-repo")
            .setSnapshots(notExistedSnapshotName)
            .setIgnoreUnavailable(true)
            .get();
        assertEquals(0, response2.getSnapshots().size());

        expectThrows(
            SnapshotMissingException.class,
            () -> clusterAdmin().prepareGetSnapshots("test-repo")
                .setSnapshots(notExistedSnapshotName)
                .setIgnoreUnavailable(false)
                .execute()
                .actionGet()
        );

        logger.info("--> unblock all data nodes");
        unblockAllDataNodes("test-repo");

        assertSuccessful(createSnapshotResponseActionFuture);
    }

    public void testSnapshotStatusOnFailedSnapshot() throws Exception {
        String repoName = "test-repo";
        createRepositoryNoVerify(repoName, "fs"); // mustn't load the repository data before we inject the broken snapshot
        final String snapshot = "test-snap-1";
        addBwCFailedSnapshot(repoName, snapshot, Collections.emptyMap());

        logger.info("--> creating good index");
        assertAcked(prepareCreate("test-idx-good").setSettings(indexSettingsNoReplicas(1)));
        ensureGreen();
        indexRandomDocs("test-idx-good", randomIntBetween(1, 5));

        final SnapshotsStatusResponse snapshotsStatusResponse = clusterAdmin().prepareSnapshotStatus(repoName).setSnapshots(snapshot).get();
        assertEquals(1, snapshotsStatusResponse.getSnapshots().size());
        assertEquals(SnapshotsInProgress.State.FAILED, snapshotsStatusResponse.getSnapshots().get(0).getState());
    }

    public void testGetSnapshotsRequest() throws Exception {
        final String repositoryName = "test-repo";
        final String indexName = "test-idx";
        final Client client = client();

        createRepository(
            repositoryName,
            "mock",
            Settings.builder()
                .put("location", randomRepoPath())
                .put("compress", false)
                .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
                .put("wait_after_unblock", 200)
        );

        logger.info("--> get snapshots on an empty repository");
        expectThrows(
            SnapshotMissingException.class,
            () -> client.admin().cluster().prepareGetSnapshots(repositoryName).addSnapshots("non-existent-snapshot").get()
        );
        // with ignore unavailable set to true, should not throw an exception
        GetSnapshotsResponse getSnapshotsResponse = client.admin()
            .cluster()
            .prepareGetSnapshots(repositoryName)
            .setIgnoreUnavailable(true)
            .addSnapshots("non-existent-snapshot")
            .get();
        assertThat(getSnapshotsResponse.getSnapshots().size(), equalTo(0));

        logger.info("--> creating an index and indexing documents");
        // Create index on 2 nodes and make sure each node has a primary by setting no replicas
        assertAcked(prepareCreate(indexName, 1, Settings.builder().put("number_of_replicas", 0)));
        ensureGreen();
        indexRandomDocs(indexName, 10);

        // make sure we return only the in-progress snapshot when taking the first snapshot on a clean repository
        // take initial snapshot with a block, making sure we only get 1 in-progress snapshot returned
        // block a node so the create snapshot operation can remain in progress
        final String initialBlockedNode = blockNodeWithIndex(repositoryName, indexName);
        client.admin()
            .cluster()
            .prepareCreateSnapshot(repositoryName, "snap-on-empty-repo")
            .setWaitForCompletion(false)
            .setIndices(indexName)
            .get();
        waitForBlock(initialBlockedNode, repositoryName); // wait for block to kick in
        getSnapshotsResponse = client.admin()
            .cluster()
            .prepareGetSnapshots("test-repo")
            .setSnapshots(randomFrom("_all", "_current", "snap-on-*", "*-on-empty-repo", "snap-on-empty-repo"))
            .get();
        assertEquals(1, getSnapshotsResponse.getSnapshots().size());
        assertEquals("snap-on-empty-repo", getSnapshotsResponse.getSnapshots().get(0).snapshotId().getName());
        unblockNode(repositoryName, initialBlockedNode); // unblock node
        startDeleteSnapshot(repositoryName, "snap-on-empty-repo").get();

        final int numSnapshots = randomIntBetween(1, 3) + 1;
        logger.info("--> take {} snapshot(s)", numSnapshots - 1);
        final String[] snapshotNames = new String[numSnapshots];
        for (int i = 0; i < numSnapshots - 1; i++) {
            final String snapshotName = randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
            CreateSnapshotResponse createSnapshotResponse = client.admin()
                .cluster()
                .prepareCreateSnapshot(repositoryName, snapshotName)
                .setWaitForCompletion(true)
                .setIndices(indexName)
                .get();
            assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
            snapshotNames[i] = snapshotName;
        }
        logger.info("--> take another snapshot to be in-progress");
        // add documents so there are data files to block on
        for (int i = 10; i < 20; i++) {
            indexDoc(indexName, Integer.toString(i), "foo", "bar" + i);
        }
        refresh();

        final String inProgressSnapshot = randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        snapshotNames[numSnapshots - 1] = inProgressSnapshot;
        // block a node so the create snapshot operation can remain in progress
        final String blockedNode = blockNodeWithIndex(repositoryName, indexName);
        client.admin()
            .cluster()
            .prepareCreateSnapshot(repositoryName, inProgressSnapshot)
            .setWaitForCompletion(false)
            .setIndices(indexName)
            .get();
        waitForBlock(blockedNode, repositoryName); // wait for block to kick in

        logger.info("--> get all snapshots with a current in-progress");
        // with ignore unavailable set to true, should not throw an exception
        final List<String> snapshotsToGet = new ArrayList<>();
        if (randomBoolean()) {
            // use _current plus the individual names of the finished snapshots
            snapshotsToGet.add("_current");
            for (int i = 0; i < numSnapshots - 1; i++) {
                snapshotsToGet.add(snapshotNames[i]);
            }
        } else {
            snapshotsToGet.add("_all");
        }
        getSnapshotsResponse = client.admin()
            .cluster()
            .prepareGetSnapshots(repositoryName)
            .setSnapshots(snapshotsToGet.toArray(Strings.EMPTY_ARRAY))
            .get();
        List<String> sortedNames = Arrays.asList(snapshotNames);
        Collections.sort(sortedNames);
        assertThat(getSnapshotsResponse.getSnapshots().size(), equalTo(numSnapshots));
        assertThat(getSnapshotsResponse.getSnapshots().stream().map(s -> s.snapshotId().getName()).sorted().toList(), equalTo(sortedNames));

        getSnapshotsResponse = client.admin().cluster().prepareGetSnapshots(repositoryName).addSnapshots(snapshotNames).get();
        sortedNames = Arrays.asList(snapshotNames);
        Collections.sort(sortedNames);
        assertThat(getSnapshotsResponse.getSnapshots().size(), equalTo(numSnapshots));
        assertThat(getSnapshotsResponse.getSnapshots().stream().map(s -> s.snapshotId().getName()).sorted().toList(), equalTo(sortedNames));

        logger.info("--> make sure duplicates are not returned in the response");
        String regexName = snapshotNames[randomIntBetween(0, numSnapshots - 1)];
        final int splitPos = regexName.length() / 2;
        final String firstRegex = regexName.substring(0, splitPos) + "*";
        final String secondRegex = "*" + regexName.substring(splitPos);
        getSnapshotsResponse = client.admin()
            .cluster()
            .prepareGetSnapshots(repositoryName)
            .addSnapshots(snapshotNames)
            .addSnapshots(firstRegex, secondRegex)
            .get();
        assertThat(getSnapshotsResponse.getSnapshots().size(), equalTo(numSnapshots));
        assertThat(getSnapshotsResponse.getSnapshots().stream().map(s -> s.snapshotId().getName()).sorted().toList(), equalTo(sortedNames));

        unblockNode(repositoryName, blockedNode); // unblock node
        awaitNoMoreRunningOperations();
    }

    public void testConcurrentCreateAndStatusAPICalls() throws Exception {
        final var indexNames = IntStream.range(0, between(1, 10)).mapToObj(i -> "test-idx-" + i).toList();
        indexNames.forEach(this::createIndexWithContent);
        final String repoName = "test-repo";
        createRepository(repoName, "fs");

        if (randomBoolean()) {
            // sometimes cause some deduplication
            createSnapshot(repoName, "initial_snapshot", List.of());
            for (final var indexName : indexNames) {
                if (randomBoolean()) {
                    indexDoc(indexName, "another_id", "baz", "quux");
                }
            }
        }

        final int snapshots = randomIntBetween(10, 20);
        final List<ActionFuture<SnapshotsStatusResponse>> statuses = new ArrayList<>(snapshots);
        final List<ActionFuture<GetSnapshotsResponse>> gets = new ArrayList<>(snapshots);
        final Client dataNodeClient = dataNodeClient();

        final var snapshotNames = IntStream.range(0, snapshots).mapToObj(i -> "test-snap-" + i).toArray(String[]::new);
        final var waitForCompletion = randomBoolean();
        final var createsListener = new PlainActionFuture<Void>();
        final var createsGroupedListener = new GroupedActionListener<CreateSnapshotResponse>(
            snapshotNames.length,
            createsListener.map(ignored -> null)
        );
        for (final var snapshotName : snapshotNames) {
            clusterAdmin().prepareCreateSnapshot(repoName, snapshotName)
                .setWaitForCompletion(waitForCompletion)
                .execute(createsGroupedListener);
        }
        createsListener.get(60, TimeUnit.SECONDS);

        // run enough parallel status requests to max out the SNAPSHOT_META threadpool
        final var metaThreadPoolSize = internalCluster().getCurrentMasterNodeInstance(ThreadPool.class)
            .info(ThreadPool.Names.SNAPSHOT_META)
            .getMax();
        for (int i = 0; i < metaThreadPoolSize * 2; i++) {
            statuses.add(dataNodeClient.admin().cluster().prepareSnapshotStatus(repoName).setSnapshots(snapshotNames).execute());
            gets.add(dataNodeClient.admin().cluster().prepareGetSnapshots(repoName).setSnapshots(snapshotNames).execute());
        }

        // ... and then some more status requests until all snapshots are done
        var masterClusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        assertBusy(() -> {
            final var stillRunning = masterClusterService.state()
                .custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY)
                .isEmpty() == false;
            statuses.add(dataNodeClient.admin().cluster().prepareSnapshotStatus(repoName).setSnapshots(snapshotNames).execute());
            gets.add(dataNodeClient.admin().cluster().prepareGetSnapshots(repoName).setSnapshots(snapshotNames).execute());
            assertFalse(stillRunning);
        }, 60, TimeUnit.SECONDS);

        for (ActionFuture<SnapshotsStatusResponse> status : statuses) {
            assertThat(status.get().getSnapshots(), hasSize(snapshots));
            for (SnapshotStatus snapshot : status.get().getSnapshots()) {
                assertThat(snapshot.getState(), allOf(not(SnapshotsInProgress.State.FAILED), not(SnapshotsInProgress.State.ABORTED)));
                for (final var shard : snapshot.getShards()) {
                    if (shard.getStage() == SnapshotIndexShardStage.DONE) {
                        assertEquals(shard.getStats().getIncrementalFileCount(), shard.getStats().getProcessedFileCount());
                        assertEquals(shard.getStats().getIncrementalSize(), shard.getStats().getProcessedSize());
                    }
                }
            }
        }
        for (ActionFuture<GetSnapshotsResponse> get : gets) {
            final List<SnapshotInfo> snapshotInfos = get.get().getSnapshots();
            assertThat(snapshotInfos, hasSize(snapshots));
            for (SnapshotInfo snapshotInfo : snapshotInfos) {
                assertThat(snapshotInfo.state(), oneOf(SnapshotState.IN_PROGRESS, SnapshotState.SUCCESS));
            }
        }
    }

    private static SnapshotIndexShardStatus stateFirstShard(SnapshotStatus snapshotStatus, String indexName) {
        return snapshotStatus.getIndices().get(indexName).getShards().get(0);
    }

    private static SnapshotStatus getSnapshotStatus(String repoName, String snapshotName) {
        try {
            return clusterAdmin().prepareSnapshotStatus(repoName).setSnapshots(snapshotName).get().getSnapshots().get(0);
        } catch (SnapshotMissingException e) {
            throw new AssertionError(e);
        }
    }

    private static Settings singleShardOneNode(String node) {
        return indexSettingsNoReplicas(1).put("index.routing.allocation.include._name", node).build();
    }
}
