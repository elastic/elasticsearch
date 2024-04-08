/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.get.shard.GetShardSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.get.shard.GetShardSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.shard.GetShardSnapshotResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.snapshots.mockstore.MockRepository;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.snapshots.SnapshotsService.NO_FEATURE_STATES_VALUE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.index.IndexVersionUtils.randomVersionBetween;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class IndexSnapshotsServiceIT extends AbstractSnapshotIntegTestCase {
    public void testGetShardSnapshotFromUnknownRepoReturnsAnError() throws Exception {
        boolean useMultipleUnknownRepositories = randomBoolean();
        List<String> repositories = useMultipleUnknownRepositories ? List.of("unknown", "unknown-2") : List.of("unknown");
        final ActionFuture<GetShardSnapshotResponse> responseFuture = getLatestSnapshotForShardFuture(repositories, "idx", 0, false);

        if (useMultipleUnknownRepositories) {
            GetShardSnapshotResponse response = responseFuture.get();
            assertThat(response.getLatestShardSnapshot().isPresent(), is(equalTo(false)));

            final Map<String, RepositoryException> failures = response.getRepositoryFailures();
            for (String repository : repositories) {
                RepositoryException repositoryException = failures.get(repository);
                assertThat(repositoryException, is(notNullValue()));
                assertThat(
                    repositoryException.getMessage(),
                    equalTo(Strings.format("[%s] Unable to find the latest snapshot for shard [[idx][0]]", repository))
                );
            }
        } else {
            expectThrows(RepositoryException.class, responseFuture);
        }

        disableRepoConsistencyCheck("This test checks an empty repository");
    }

    public void testGetShardSnapshotFromEmptyRepositoryReturnsEmptyResult() {
        final String fsRepoName = randomAlphaOfLength(10);
        createRepository(fsRepoName, FsRepository.TYPE);

        final Optional<ShardSnapshotInfo> indexShardSnapshotInfo = getLatestSnapshotForShard(fsRepoName, "test", 0);
        assertThat(indexShardSnapshotInfo.isEmpty(), equalTo(true));

        disableRepoConsistencyCheck("This test checks an empty repository");
    }

    public void testGetShardSnapshotFromUnknownIndexReturnsEmptyResult() {
        final String fsRepoName = randomAlphaOfLength(10);
        createRepository(fsRepoName, FsRepository.TYPE);

        createSnapshot(fsRepoName, "snap-1", Collections.emptyList());

        final Optional<ShardSnapshotInfo> indexShardSnapshotInfo = getLatestSnapshotForShard(fsRepoName, "test", 0);
        assertThat(indexShardSnapshotInfo.isEmpty(), equalTo(true));
    }

    public void testGetShardSnapshotFromUnknownShardReturnsEmptyResult() {
        final String fsRepoName = randomAlphaOfLength(10);
        final String indexName = "test-idx";

        createIndexWithContent(indexName);

        createRepository(fsRepoName, FsRepository.TYPE);
        createSnapshot(fsRepoName, "snap-1", Collections.singletonList(indexName));

        final Optional<ShardSnapshotInfo> indexShardSnapshotInfo = getLatestSnapshotForShard(fsRepoName, indexName, 100);
        assertThat(indexShardSnapshotInfo.isEmpty(), equalTo(true));
    }

    public void testGetShardSnapshotOnEmptyRepositoriesListThrowsAnError() {
        expectThrows(IllegalArgumentException.class, () -> getLatestSnapshotForShardFuture(Collections.emptyList(), "idx", 0, false));
    }

    public void testGetShardSnapshotReturnsTheLatestSuccessfulSnapshot() throws Exception {
        final String repoName = "repo-name";
        final Path repoPath = randomRepoPath();
        createRepository(repoName, FsRepository.TYPE, repoPath);

        final boolean useBwCFormat = randomBoolean();
        if (useBwCFormat) {
            final IndexVersion version = randomVersionBetween(random(), IndexVersions.V_7_5_0, IndexVersion.current());
            initWithSnapshotVersion(repoName, repoPath, version);
        }

        createSnapshot(repoName, "empty-snap", Collections.emptyList());

        final String indexName = "test";
        final String indexName2 = "test-2";
        List<String> indices = List.of(indexName, indexName2);
        createIndex(indexName, indexName2);
        SnapshotInfo lastSnapshot = null;
        String expectedIndexMetadataId = null;

        int numSnapshots = randomIntBetween(5, 25);
        for (int i = 0; i < numSnapshots; i++) {
            if (randomBoolean()) {
                indexRandomDocs(indexName, 5);
                indexRandomDocs(indexName2, 10);
            }
            final List<String> snapshotIndices = randomSubsetOf(indices);
            final SnapshotInfo snapshotInfo = createSnapshot(repoName, Strings.format("snap-%03d", i), snapshotIndices);
            if (snapshotInfo.indices().contains(indexName)) {
                lastSnapshot = snapshotInfo;
                ClusterStateResponse clusterStateResponse = admin().cluster().prepareState().get();
                IndexMetadata indexMetadata = clusterStateResponse.getState().metadata().index(indexName);
                expectedIndexMetadataId = IndexMetaDataGenerations.buildUniqueIdentifier(indexMetadata);
            }
        }

        if (useBwCFormat) {
            // Reload the RepositoryData so we don't use cached data that wasn't serialized
            assertAcked(clusterAdmin().prepareDeleteRepository(repoName).get());
            createRepository(repoName, "fs", repoPath);
        }

        final Optional<ShardSnapshotInfo> indexShardSnapshotInfoOpt = getLatestSnapshotForShard(repoName, indexName, 0);
        if (lastSnapshot == null) {
            assertThat(indexShardSnapshotInfoOpt.isPresent(), equalTo(false));
        } else {
            assertThat(indexShardSnapshotInfoOpt.isPresent(), equalTo(true));

            final ShardSnapshotInfo shardSnapshotInfo = indexShardSnapshotInfoOpt.get();

            assertThat(shardSnapshotInfo.getIndexMetadataIdentifier(), equalTo(expectedIndexMetadataId));

            final Snapshot snapshot = shardSnapshotInfo.getSnapshot();
            assertThat(snapshot, equalTo(lastSnapshot.snapshot()));
        }
    }

    public void testGetShardSnapshotWhileThereIsARunningSnapshot() throws Exception {
        final String fsRepoName = randomAlphaOfLength(10);
        createRepository(fsRepoName, "mock");

        createSnapshot(fsRepoName, "empty-snap", Collections.emptyList());

        final String indexName = "test-idx";
        createIndexWithContent(indexName);

        blockAllDataNodes(fsRepoName);

        final String snapshotName = "snap-1";
        final ActionFuture<CreateSnapshotResponse> snapshotFuture = clusterAdmin().prepareCreateSnapshot(fsRepoName, snapshotName)
            .setIndices(indexName)
            .setWaitForCompletion(true)
            .execute();

        waitForBlockOnAnyDataNode(fsRepoName);

        assertThat(getLatestSnapshotForShard(fsRepoName, indexName, 0).isEmpty(), equalTo(true));

        unblockAllDataNodes(fsRepoName);

        assertSuccessful(snapshotFuture);
    }

    public void testGetShardSnapshotFailureHandlingLetOtherRepositoriesRequestsMakeProgress() throws Exception {
        final String failingRepoName = randomAlphaOfLength(10);
        createRepository(failingRepoName, "mock");
        int repoCount = randomIntBetween(1, 10);
        List<String> workingRepoNames = new ArrayList<>();
        for (int i = 0; i < repoCount; i++) {
            final String repoName = randomAlphaOfLength(10);
            createRepository(repoName, "fs");
            workingRepoNames.add(repoName);
        }

        final String indexName = "test-idx";
        createIndexWithContent(indexName);

        int snapshotIdx = 0;
        Object[] args1 = new Object[] { snapshotIdx++ };
        createSnapshot(failingRepoName, Strings.format("snap-%03d", args1), Collections.singletonList(indexName));
        SnapshotInfo latestSnapshot = null;
        for (String workingRepoName : workingRepoNames) {
            Object[] args = new Object[] { snapshotIdx++ };
            String snapshot = Strings.format("snap-%03d", args);
            latestSnapshot = createSnapshot(workingRepoName, snapshot, Collections.singletonList(indexName));
        }

        final MockRepository repository = getRepositoryOnMaster(failingRepoName);
        if (randomBoolean()) {
            repository.setBlockAndFailOnReadIndexFiles();
        } else {
            repository.setBlockAndFailOnReadSnapFiles();
        }

        PlainActionFuture<GetShardSnapshotResponse> future = getLatestSnapshotForShardFuture(
            CollectionUtils.appendToCopy(workingRepoNames, failingRepoName),
            indexName,
            0
        );
        waitForBlock(internalCluster().getMasterName(), failingRepoName);
        repository.unblock();

        final GetShardSnapshotResponse response = future.actionGet();

        final Optional<RepositoryException> error = response.getFailureForRepository(failingRepoName);
        assertThat(error.isPresent(), is(equalTo(true)));
        assertThat(
            error.get().getMessage(),
            equalTo(Strings.format("[%s] Unable to find the latest snapshot for shard [[%s][0]]", failingRepoName, indexName))
        );

        for (String workingRepoName : workingRepoNames) {
            assertThat(response.getFailureForRepository(workingRepoName).isEmpty(), is(equalTo(true)));
        }

        Optional<ShardSnapshotInfo> shardSnapshotInfoOpt = response.getLatestShardSnapshot();

        assertThat(shardSnapshotInfoOpt.isPresent(), equalTo(true));
        ShardSnapshotInfo shardSnapshotInfo = shardSnapshotInfoOpt.get();
        assertThat(shardSnapshotInfo.getSnapshot(), equalTo(latestSnapshot.snapshot()));
        assertThat(shardSnapshotInfo.getRepository(), equalTo(latestSnapshot.repository()));
    }

    public void testGetShardSnapshotInMultipleRepositoriesReturnsTheLatestSnapshot() {
        int repoCount = randomIntBetween(2, 10);
        List<String> repositories = new ArrayList<>();
        for (int i = 0; i < repoCount; i++) {
            final String repoName = randomAlphaOfLength(10);
            createRepository(repoName, "fs");
            repositories.add(repoName);
        }

        final String indexName = "test-idx";
        createIndexWithContent(indexName);

        int snapshotIdx = 0;
        SnapshotInfo expectedLatestSnapshot = null;
        for (String repository : repositories) {
            Object[] args = new Object[] { snapshotIdx++ };
            String snapshot = Strings.format("snap-%03d", args);
            expectedLatestSnapshot = createSnapshot(repository, snapshot, Collections.singletonList(indexName));
        }

        GetShardSnapshotResponse response = getLatestSnapshotForShardFuture(repositories, indexName, 0).actionGet();

        assertThat(response.getRepositoryFailures(), is(anEmptyMap()));
        Optional<ShardSnapshotInfo> shardSnapshotInfoOpt = response.getLatestShardSnapshot();

        assertThat(shardSnapshotInfoOpt.isPresent(), equalTo(true));
        ShardSnapshotInfo shardSnapshotInfo = shardSnapshotInfoOpt.get();
        assertThat(shardSnapshotInfo.getSnapshot(), equalTo(expectedLatestSnapshot.snapshot()));
        assertThat(shardSnapshotInfo.getRepository(), equalTo(expectedLatestSnapshot.repository()));
    }

    public void testFailedSnapshotsAreNotReturned() throws Exception {
        final String indexName = "test";
        createIndexWithContent(indexName);

        final String repoName = "test-repo";
        createRepository(repoName, "mock");

        for (RepositoriesService repositoriesService : internalCluster().getDataNodeInstances(RepositoriesService.class)) {
            ((MockRepository) repositoriesService.repository(repoName)).setBlockAndFailOnWriteSnapFiles();
        }

        clusterAdmin().prepareCreateSnapshot(repoName, "snap")
            .setIndices(indexName)
            .setWaitForCompletion(false)
            .setFeatureStates(NO_FEATURE_STATES_VALUE)
            .get();

        waitForBlockOnAnyDataNode(repoName);

        for (RepositoriesService repositoriesService : internalCluster().getDataNodeInstances(RepositoriesService.class)) {
            ((MockRepository) repositoriesService.repository(repoName)).unblock();
        }

        assertBusy(() -> assertThat(getSnapshot(repoName, "snap").state(), equalTo(SnapshotState.PARTIAL)));

        Optional<ShardSnapshotInfo> shardSnapshotInfo = getLatestSnapshotForShard(repoName, indexName, 0);
        assertThat(shardSnapshotInfo.isEmpty(), equalTo(true));

        final SnapshotInfo snapshotInfo = createSnapshot(repoName, "snap-1", Collections.singletonList(indexName));

        Optional<ShardSnapshotInfo> latestSnapshotForShard = getLatestSnapshotForShard(repoName, indexName, 0);
        assertThat(latestSnapshotForShard.isPresent(), equalTo(true));
        assertThat(latestSnapshotForShard.get().getSnapshot(), equalTo(snapshotInfo.snapshot()));
        assertThat(latestSnapshotForShard.get().getRepository(), equalTo(snapshotInfo.repository()));
    }

    private Optional<ShardSnapshotInfo> getLatestSnapshotForShard(String repository, String indexName, int shard) {
        final GetShardSnapshotResponse response = getLatestSnapshotForShardFuture(Collections.singletonList(repository), indexName, shard)
            .actionGet();
        return response.getLatestShardSnapshot();
    }

    private PlainActionFuture<GetShardSnapshotResponse> getLatestSnapshotForShardFuture(
        List<String> repositories,
        String indexName,
        int shard
    ) {
        return getLatestSnapshotForShardFuture(repositories, indexName, shard, true);
    }

    private PlainActionFuture<GetShardSnapshotResponse> getLatestSnapshotForShardFuture(
        List<String> repositories,
        String indexName,
        int shard,
        boolean useAllRepositoriesRequest
    ) {
        ShardId shardId = new ShardId(new Index(indexName, "__na__"), shard);
        PlainActionFuture<GetShardSnapshotResponse> future = new PlainActionFuture<>();
        final GetShardSnapshotRequest request;
        if (useAllRepositoriesRequest && randomBoolean()) {
            request = GetShardSnapshotRequest.latestSnapshotInAllRepositories(shardId);
        } else {
            request = GetShardSnapshotRequest.latestSnapshotInRepositories(shardId, repositories);
        }

        client().execute(GetShardSnapshotAction.INSTANCE, request, future);
        return future;
    }
}
