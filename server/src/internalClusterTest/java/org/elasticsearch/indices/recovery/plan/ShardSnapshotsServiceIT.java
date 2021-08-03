/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.recovery.plan;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.core.Nullable;
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
import org.elasticsearch.repositories.ShardSnapshotInfo;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.SnapshotException;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(minNumDataNodes = 1)
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

        public FailingRepo(RepositoryMetadata metadata,
                    Environment environment,
                    NamedXContentRegistry namedXContentRegistry,
                    ClusterService clusterService,
                    BigArrays bigArrays,
                    RecoverySettings recoverySettings) {
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
        public BlobStoreIndexShardSnapshots getBlobStoreIndexShardSnapshots(IndexId indexId, int shardId, @Nullable String shardGen)
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

        List<ShardSnapshot> shardSnapshotData = getShardSnapshotShard(shardId);
        assertThat(shardSnapshotData, is(empty()));
    }

    public void testFetchFromSingleRepository() throws Exception {
        String indexName = "test";
        createIndex(indexName, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).build());
        ShardId shardId = getShardIdForIndex(indexName);

        for (int i = 0; i < randomIntBetween(1, 50); i++) {
            index(indexName, Integer.toString(i), Collections.singletonMap("foo", "bar"));
        }

        String snapshotName = "snap";
        final int numberOfRepos = randomIntBetween(1, 4);
        List<String> repositories = new ArrayList<>(numberOfRepos);
        for (int i = 0; i < numberOfRepos; i++) {
            String repositoryName = "repo-" + i;
            createRepository(repositoryName, "fs");
            repositories.add(repositoryName);
            createSnapshot(repositoryName, snapshotName, indexName);
        }

        String repositoryToFetch = randomFrom(repositories);
        ShardSnapshotsService shardSnapshotsService = getShardSnapshotsService();

        PlainActionFuture<List<ShardSnapshot>> future = PlainActionFuture.newFuture();
        shardSnapshotsService.fetchAvailableSnapshots(repositoryToFetch, shardId, future);
        List<ShardSnapshot> shardSnapshots = future.get();

        assertThat(shardSnapshots.size(), equalTo(1));
        ShardSnapshot shardSnapshot = shardSnapshots.get(0);
        assertThat(shardSnapshot.getRepository(), equalTo(repositoryToFetch));
        assertThat(shardSnapshot.getShardSnapshotInfo().getShardId(), equalTo(shardId));
        assertThat(shardSnapshot.getMetadataSnapshot().size(), greaterThan(0));
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
        List<String> failingRepos = new ArrayList<>();
        for (int i = 0; i < numberOfFailingRepos; i++) {
            String repositoryName = "failing-repo-" + i;
            createRepository(repositoryName, FailingRepoPlugin.TYPE);
            createSnapshot(repositoryName, snapshotName, indexName);
            failingRepos.add(repositoryName);
        }

        int numberOfWorkingRepositories = randomIntBetween(0, 4);
        List<String> workingRepos = new ArrayList<>();
        for (int i = 0; i < numberOfWorkingRepositories; i++) {
            String repositoryName = "repo-" + i;
            createRepository(repositoryName, "fs");
            workingRepos.add(repositoryName);
            createSnapshot(repositoryName, snapshotName, indexName);
        }

        for (String failingRepo : failingRepos) {
            // Update repository settings to fail fetching the repository information at any stage
            String repoFailureType =
                randomFrom(FailingRepo.FAIL_GET_REPOSITORY_DATA_SETTING_KEY,
                    FailingRepo.FAIL_LOAD_SHARD_SNAPSHOT_SETTING_KEY,
                    FailingRepo.FAIL_LOAD_SHARD_SNAPSHOTS_SETTING_KEY
                );
            createRepository(failingRepo, FailingRepoPlugin.TYPE, Settings.builder().put(repoFailureType, true).build());
        }

        List<ShardSnapshot> shardSnapshotDataForShard = getShardSnapshotShard(shardId);

        assertThat(shardSnapshotDataForShard.size(), is(equalTo(numberOfWorkingRepositories)));
        for (ShardSnapshot shardSnapshotData : shardSnapshotDataForShard) {
            assertThat(workingRepos.contains(shardSnapshotData.getRepository()), is(equalTo(true)));
            assertThat(shardSnapshotData.getMetadataSnapshot().size(), is(greaterThan(0)));

            ShardSnapshotInfo shardSnapshotInfo = shardSnapshotData.getShardSnapshotInfo();
            assertThat(shardSnapshotInfo.getShardId(), equalTo(shardId));
            assertThat(shardSnapshotInfo.getSnapshot().getSnapshotId().getName(), equalTo(snapshotName));
        }
    }

    public void testFetchFromNonExistingRepositoryReturnsAnError() {
        String indexName = "test";
        createIndex(indexName, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).build());
        ShardId shardId = getShardIdForIndex(indexName);

        String repositoryToFetch = "unknown";
        ShardSnapshotsService shardSnapshotsService = getShardSnapshotsService();

        PlainActionFuture<List<ShardSnapshot>> future = PlainActionFuture.newFuture();
        shardSnapshotsService.fetchAvailableSnapshots(repositoryToFetch, shardId, future);
        expectThrows(Exception.class, future::actionGet);
    }

    public void testInputValidations() {
        ShardSnapshotsService shardSnapshotsService = getShardSnapshotsService();
        expectThrows(IllegalArgumentException.class, () ->
            shardSnapshotsService.fetchAvailableSnapshots(randomFrom("", null), null, null));
        expectThrows(IllegalArgumentException.class, () ->
            shardSnapshotsService.fetchAvailableSnapshots("repo", null, null));
        expectThrows(IllegalArgumentException.class, () ->
            shardSnapshotsService.fetchAvailableSnapshotsInAllRepositories(null, null));
    }

    private List<ShardSnapshot> getShardSnapshotShard(ShardId shardId) throws Exception {
        ShardSnapshotsService shardSnapshotsService = getShardSnapshotsService();

        PlainActionFuture<List<ShardSnapshot>> future = PlainActionFuture.newFuture();
        shardSnapshotsService.fetchAvailableSnapshotsInAllRepositories(shardId, future);
        return future.get();
    }

    private ShardSnapshotsService getShardSnapshotsService() {
        RepositoriesService repositoriesService = internalCluster().getDataNodeInstance(RepositoriesService.class);
        ThreadPool threadPool = internalCluster().getDataNodeInstance(ThreadPool.class);
        return new ShardSnapshotsService(client(), repositoriesService, threadPool);
    }

    private ShardId getShardIdForIndex(String indexName) {
        ClusterState state = clusterAdmin().prepareState().get().getState();
        return state.routingTable().index(indexName).shard(0).shardId();
    }

    private void createRepository(String repositoryName, String type) {
        createRepository(repositoryName, type, Settings.EMPTY);
    }

    private void createRepository(String repositoryName, String type, Settings settings) {
        assertAcked(client().admin().cluster().preparePutRepository(repositoryName)
            .setType(type)
            .setVerify(false)
            .setSettings(Settings.builder().put(settings).put("location", randomRepoPath())));
    }

    private void createSnapshot(String repoName, String snapshotName, String index) {
        clusterAdmin()
            .prepareCreateSnapshot(repoName, snapshotName)
            .setWaitForCompletion(true)
            .setIndices(index)
            .get();
    }
}
