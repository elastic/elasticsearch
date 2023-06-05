/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.recovery.plan;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshots;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardSnapshotInfo;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.SnapshotException;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class ShardSnapshotsServiceIT extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(FailingRepoPlugin.class);
    }

    public static class FailingRepoPlugin extends Plugin implements RepositoryPlugin {
        public static final String TYPE = "failingrepo";

        @Override
        public Map<String, Repository.Factory> getRepositories(
            Environment env,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            BigArrays bigArrays,
            RecoverySettings recoverySettings
        ) {
            return Collections.singletonMap(
                TYPE,
                metadata -> new FailingRepo(metadata, env, namedXContentRegistry, clusterService, bigArrays, recoverySettings)
            );
        }
    }

    public static class FailingRepo extends FsRepository {
        static final String FAIL_GET_REPOSITORY_DATA_SETTING_KEY = "fail_get_repository_data";
        static final String FAIL_LOAD_SHARD_SNAPSHOT_SETTING_KEY = "fail_load_shard_snapshot";
        static final String FAIL_LOAD_SHARD_SNAPSHOTS_SETTING_KEY = "fail_load_shard_snapshots";

        private final boolean failGetRepositoryData;
        private final boolean failLoadShardSnapshot;
        private final boolean failLoadShardSnapshots;

        public FailingRepo(
            RepositoryMetadata metadata,
            Environment environment,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            BigArrays bigArrays,
            RecoverySettings recoverySettings
        ) {
            super(metadata, environment, namedXContentRegistry, clusterService, bigArrays, recoverySettings);
            this.failGetRepositoryData = metadata.settings().getAsBoolean(FAIL_GET_REPOSITORY_DATA_SETTING_KEY, false);
            this.failLoadShardSnapshot = metadata.settings().getAsBoolean(FAIL_LOAD_SHARD_SNAPSHOT_SETTING_KEY, false);
            this.failLoadShardSnapshots = metadata.settings().getAsBoolean(FAIL_LOAD_SHARD_SNAPSHOTS_SETTING_KEY, false);
        }

        @Override
        public void getRepositoryData(ActionListener<RepositoryData> listener) {
            if (failGetRepositoryData) {
                listener.onFailure(new IOException("Failure getting repository data"));
                return;
            }
            super.getRepositoryData(listener);
        }

        @Override
        public BlobStoreIndexShardSnapshot loadShardSnapshot(BlobContainer shardContainer, SnapshotId snapshotId) {
            if (failLoadShardSnapshot) {
                throw new SnapshotException(
                    metadata.name(),
                    snapshotId,
                    "failed to read shard snapshot file for [" + shardContainer.path() + ']',
                    new FileNotFoundException("unable to find file")
                );
            }
            return super.loadShardSnapshot(shardContainer, snapshotId);
        }

        @Override
        public BlobStoreIndexShardSnapshots getBlobStoreIndexShardSnapshots(IndexId indexId, int shardId, ShardGeneration shardGen)
            throws IOException {
            if (failLoadShardSnapshots) {
                throw new FileNotFoundException("Failed to get blob store index shard snapshots");
            }
            return super.getBlobStoreIndexShardSnapshots(indexId, shardId, shardGen);
        }
    }

    public void testReturnsEmptyListWhenThereAreNotAvailableRepositories() throws Exception {
        String indexName = "test";
        createIndex(indexName, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).build());
        ShardId shardId = getShardIdForIndex(indexName);

        Optional<ShardSnapshot> shardSnapshot = getLatestShardSnapshot(shardId);
        assertThat(shardSnapshot.isPresent(), is(equalTo(false)));
    }

    public void testOnlyFetchesSnapshotFromEnabledRepositories() throws Exception {
        final String indexName = "test";
        createIndex(indexName, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).build());
        ShardId shardId = getShardIdForIndex(indexName);

        for (int i = 0; i < randomIntBetween(1, 50); i++) {
            index(indexName, Integer.toString(i), Collections.singletonMap("foo", "bar"));
        }

        String snapshotName = "snap";

        int numberOfNonEnabledRepos = randomIntBetween(1, 3);
        List<String> nonEnabledRepos = new ArrayList<>();
        for (int i = 0; i < numberOfNonEnabledRepos; i++) {
            String repositoryName = "non-enabled-repo-" + i;
            Path repoPath = randomRepoPath();
            createRepository(repositoryName, "fs", repoPath, false);
            createSnapshot(repositoryName, snapshotName, indexName);
            nonEnabledRepos.add(repositoryName);
        }

        int numberOfRecoveryEnabledRepositories = randomIntBetween(0, 4);
        List<String> recoveryEnabledRepos = new ArrayList<>();
        for (int i = 0; i < numberOfRecoveryEnabledRepositories; i++) {
            String repositoryName = "repo-" + i;
            createRepository(repositoryName, "fs", randomRepoPath(), true);
            recoveryEnabledRepos.add(repositoryName);
            createSnapshot(repositoryName, snapshotName, indexName);
        }

        Optional<ShardSnapshot> latestShardSnapshot = getLatestShardSnapshot(shardId);

        if (numberOfRecoveryEnabledRepositories == 0) {
            assertThat(latestShardSnapshot.isPresent(), is(equalTo(false)));
        } else {
            assertThat(latestShardSnapshot.isPresent(), is(equalTo(true)));

            ShardSnapshot shardSnapshotData = latestShardSnapshot.get();
            ShardSnapshotInfo shardSnapshotInfo = shardSnapshotData.getShardSnapshotInfo();
            assertThat(recoveryEnabledRepos.contains(shardSnapshotInfo.getRepository()), is(equalTo(true)));
            assertThat(nonEnabledRepos.contains(shardSnapshotInfo.getRepository()), is(equalTo(false)));

            assertThat(shardSnapshotData.getMetadataSnapshot().size(), is(greaterThan(0)));
            Version commitVersion = shardSnapshotData.getCommitVersion();
            assertThat(commitVersion, is(notNullValue()));
            assertThat(commitVersion, is(equalTo(Version.CURRENT)));
            final org.apache.lucene.util.Version commitLuceneVersion = shardSnapshotData.getCommitLuceneVersion();
            assertThat(commitLuceneVersion, is(notNullValue()));
            assertThat(commitLuceneVersion, is(equalTo(Version.CURRENT.luceneVersion())));

            assertThat(shardSnapshotInfo.getShardId(), is(equalTo(shardId)));
            assertThat(shardSnapshotInfo.getSnapshot().getSnapshotId().getName(), is(equalTo(snapshotName)));
        }
    }

    public void testFailingReposAreTreatedAsNonExistingShardSnapshots() throws Exception {
        final String indexName = "test";
        createIndex(indexName, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).build());
        ShardId shardId = getShardIdForIndex(indexName);

        for (int i = 0; i < randomIntBetween(1, 50); i++) {
            index(indexName, Integer.toString(i), Collections.singletonMap("foo", "bar"));
        }

        String snapshotName = "snap";

        int numberOfFailingRepos = randomIntBetween(1, 3);
        List<Tuple<String, Path>> failingRepos = new ArrayList<>();
        List<String> failingRepoNames = new ArrayList<>();
        for (int i = 0; i < numberOfFailingRepos; i++) {
            String repositoryName = "failing-repo-" + i;
            Path repoPath = randomRepoPath();
            createRepository(repositoryName, FailingRepoPlugin.TYPE, repoPath, true);
            createSnapshot(repositoryName, snapshotName, indexName);
            failingRepos.add(Tuple.tuple(repositoryName, repoPath));
            failingRepoNames.add(repositoryName);
        }

        int numberOfWorkingRepositories = randomIntBetween(0, 4);
        List<String> workingRepos = new ArrayList<>();
        for (int i = 0; i < numberOfWorkingRepositories; i++) {
            String repositoryName = "repo-" + i;
            createRepository(repositoryName, "fs", randomRepoPath(), true);
            workingRepos.add(repositoryName);
            createSnapshot(repositoryName, snapshotName, indexName);
        }

        for (Tuple<String, Path> failingRepo : failingRepos) {
            // Update repository settings to fail fetching the repository information at any stage
            String repoFailureType = randomFrom(
                FailingRepo.FAIL_GET_REPOSITORY_DATA_SETTING_KEY,
                FailingRepo.FAIL_LOAD_SHARD_SNAPSHOT_SETTING_KEY,
                FailingRepo.FAIL_LOAD_SHARD_SNAPSHOTS_SETTING_KEY
            );

            assertAcked(
                client().admin()
                    .cluster()
                    .preparePutRepository(failingRepo.v1())
                    .setType(FailingRepoPlugin.TYPE)
                    .setVerify(false)
                    .setSettings(Settings.builder().put(repoFailureType, true).put("location", failingRepo.v2()))
            );
        }

        Optional<ShardSnapshot> latestShardSnapshot = getLatestShardSnapshot(shardId);

        if (numberOfWorkingRepositories == 0) {
            assertThat(latestShardSnapshot.isPresent(), is(equalTo(false)));
        } else {
            assertThat(latestShardSnapshot.isPresent(), is(equalTo(true)));
            ShardSnapshot shardSnapshotData = latestShardSnapshot.get();
            ShardSnapshotInfo shardSnapshotInfo = shardSnapshotData.getShardSnapshotInfo();
            assertThat(workingRepos.contains(shardSnapshotInfo.getRepository()), is(equalTo(true)));
            assertThat(failingRepoNames.contains(shardSnapshotInfo.getRepository()), is(equalTo(false)));

            assertThat(shardSnapshotData.getMetadataSnapshot().size(), is(greaterThan(0)));

            assertThat(shardSnapshotInfo.getShardId(), is(equalTo(shardId)));
            assertThat(shardSnapshotInfo.getSnapshot().getSnapshotId().getName(), is(equalTo(snapshotName)));
        }
    }

    public void testFetchingInformationFromAnIncompatibleMasterNodeReturnsAnEmptyList() {
        String indexName = "test";
        createIndex(indexName, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).build());
        ShardId shardId = getShardIdForIndex(indexName);

        for (int i = 0; i < randomIntBetween(1, 50); i++) {
            index(indexName, Integer.toString(i), Collections.singletonMap("foo", "bar"));
        }

        String snapshotName = "snap";
        String repositoryName = "repo";
        createRepository(repositoryName, "fs", randomRepoPath(), true);
        createSnapshot(repositoryName, snapshotName, indexName);

        RepositoriesService repositoriesService = internalCluster().getAnyMasterNodeInstance(RepositoriesService.class);
        ThreadPool threadPool = internalCluster().getAnyMasterNodeInstance(ThreadPool.class);
        ClusterService clusterService = internalCluster().getAnyMasterNodeInstance(ClusterService.class);
        ShardSnapshotsService shardSnapshotsService = new ShardSnapshotsService(client(), repositoriesService, threadPool, clusterService) {
            @Override
            protected boolean masterSupportsFetchingLatestSnapshots() {
                return false;
            }
        };

        PlainActionFuture<Optional<ShardSnapshot>> latestSnapshots = PlainActionFuture.newFuture();
        shardSnapshotsService.fetchLatestSnapshotsForShard(shardId, latestSnapshots);
        assertThat(latestSnapshots.actionGet().isPresent(), is(equalTo(false)));
    }

    private Optional<ShardSnapshot> getLatestShardSnapshot(ShardId shardId) throws Exception {
        ShardSnapshotsService shardSnapshotsService = getShardSnapshotsService();

        PlainActionFuture<Optional<ShardSnapshot>> future = PlainActionFuture.newFuture();
        shardSnapshotsService.fetchLatestSnapshotsForShard(shardId, future);
        return future.get();
    }

    private ShardSnapshotsService getShardSnapshotsService() {
        RepositoriesService repositoriesService = internalCluster().getAnyMasterNodeInstance(RepositoriesService.class);
        ThreadPool threadPool = internalCluster().getAnyMasterNodeInstance(ThreadPool.class);
        ClusterService clusterService = internalCluster().getAnyMasterNodeInstance(ClusterService.class);
        return new ShardSnapshotsService(client(), repositoriesService, threadPool, clusterService);
    }

    private ShardId getShardIdForIndex(String indexName) {
        ClusterState state = clusterAdmin().prepareState().get().getState();
        return state.routingTable().index(indexName).shard(0).shardId();
    }

    private void createRepository(String repositoryName, String type, Path location, boolean recoveryEnabledRepo) {
        assertAcked(
            client().admin()
                .cluster()
                .preparePutRepository(repositoryName)
                .setType(type)
                .setVerify(false)
                .setSettings(
                    Settings.builder()
                        .put("location", location)
                        .put(BlobStoreRepository.USE_FOR_PEER_RECOVERY_SETTING.getKey(), recoveryEnabledRepo)
                )
        );
    }

    private void createSnapshot(String repoName, String snapshotName, String index) {
        clusterAdmin().prepareCreateSnapshot(repoName, snapshotName).setWaitForCompletion(true).setIndices(index).get();
    }
}
