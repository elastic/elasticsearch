/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.recovery;

import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.blobstore.ESBlobStoreRepositoryIntegTestCase;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.searchablesnapshots.BaseSearchableSnapshotsIntegTestCase;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots;
import org.elasticsearch.xpack.searchablesnapshots.cache.full.CacheService;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

import static org.elasticsearch.index.IndexSettings.INDEX_SOFT_DELETES_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

@ESIntegTestCase.ClusterScope(numDataNodes = 1)
public class SearchableSnapshotRecoveryStateIntegrationTests extends BaseSearchableSnapshotsIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), TestRepositoryPlugin.class);
    }

    public void testRecoveryStateRecoveredBytesMatchPhysicalCacheState() throws Exception {
        final String fsRepoName = randomAlphaOfLength(10);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final String restoredIndexName = randomBoolean() ? indexName : randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final String snapshotName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);

        createRepository(fsRepoName, "fs");

        final Settings.Builder originalIndexSettings = Settings.builder();
        originalIndexSettings.put(INDEX_SOFT_DELETES_SETTING.getKey(), true);
        originalIndexSettings.put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1);

        createAndPopulateIndex(indexName, originalIndexSettings);

        final SnapshotInfo snapshotInfo = createFullSnapshot(fsRepoName, snapshotName);

        assertAcked(client().admin().indices().prepareDelete(indexName));

        mountSnapshot(fsRepoName, snapshotName, indexName, restoredIndexName, Settings.EMPTY);
        ensureGreen(restoredIndexName);

        final Index restoredIndex = client().admin()
            .cluster()
            .prepareState()
            .clear()
            .setMetadata(true)
            .get()
            .getState()
            .metadata()
            .index(restoredIndexName)
            .getIndex();

        assertExecutorIsIdle(SearchableSnapshots.CACHE_PREWARMING_THREAD_POOL_NAME);
        assertExecutorIsIdle(SearchableSnapshots.CACHE_FETCH_ASYNC_THREAD_POOL_NAME);

        RecoveryState recoveryState = getRecoveryState(restoredIndexName);

        assertThat(recoveryState.getStage(), equalTo(RecoveryState.Stage.DONE));

        long recoveredBytes = recoveryState.getIndex().recoveredBytes();
        long physicalCacheSize = getPhysicalCacheSize(restoredIndex, snapshotInfo.snapshotId().getUUID());

        assertThat("Physical cache size doesn't match with recovery state data", physicalCacheSize, equalTo(recoveredBytes));
        assertThat("Expected to recover 100% of files", recoveryState.getIndex().recoveredBytesPercent(), equalTo(100.0f));
        assertThat(recoveryState.getIndex().recoveredFromSnapshotBytes(), equalTo(recoveredBytes));
        assertThat(recoveryState.getIndex().recoveredBytes(), equalTo(recoveredBytes));
    }

    public void testFilesStoredInThePersistentCacheAreMarkedAsReusedInRecoveryState() throws Exception {
        final String fsRepoName = randomAlphaOfLength(10);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final String restoredIndexName = randomBoolean() ? indexName : randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final String snapshotName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);

        createRepository(fsRepoName, "test-fs");
        int numberOfShards = 1;

        assertAcked(
            prepareCreate(
                indexName,
                Settings.builder()
                    .put(INDEX_SOFT_DELETES_SETTING.getKey(), true)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
            )
        );
        ensureGreen(indexName);

        final int documentCount = randomIntBetween(1000, 3000);
        populateIndex(indexName, documentCount);

        final SnapshotInfo snapshotInfo = createFullSnapshot(fsRepoName, snapshotName);

        assertAcked(client().admin().indices().prepareDelete(indexName));

        mountSnapshot(fsRepoName, snapshotName, indexName, restoredIndexName, Settings.EMPTY);
        ensureGreen(restoredIndexName);
        assertBusy(() -> assertThat(getRecoveryState(restoredIndexName).getStage(), equalTo(RecoveryState.Stage.DONE)));

        for (CacheService cacheService : internalCluster().getDataNodeInstances(CacheService.class)) {
            cacheService.synchronizeCache();
        }

        internalCluster().restartRandomDataNode();
        ensureGreen(restoredIndexName);

        final Index restoredIndex = client().admin()
            .cluster()
            .prepareState()
            .clear()
            .setMetadata(true)
            .get()
            .getState()
            .metadata()
            .index(restoredIndexName)
            .getIndex();

        assertExecutorIsIdle(SearchableSnapshots.CACHE_PREWARMING_THREAD_POOL_NAME);
        assertExecutorIsIdle(SearchableSnapshots.CACHE_FETCH_ASYNC_THREAD_POOL_NAME);

        RecoveryState recoveryState = getRecoveryState(restoredIndexName);

        assertThat(recoveryState.getStage(), equalTo(RecoveryState.Stage.DONE));

        long recoveredBytes = recoveryState.getIndex().recoveredBytes();
        long physicalCacheSize = getPhysicalCacheSize(restoredIndex, snapshotInfo.snapshotId().getUUID());

        assertThat("Expected to reuse all data from the persistent cache but it didn't", 0L, equalTo(recoveredBytes));

        final Repository repository = internalCluster().getDataNodeInstance(RepositoriesService.class).repository(fsRepoName);
        assertThat(repository, instanceOf(BlobStoreRepository.class));
        final BlobStoreRepository blobStoreRepository = (BlobStoreRepository) repository;

        final RepositoryData repositoryData = ESBlobStoreRepositoryIntegTestCase.getRepositoryData(
            internalCluster().getCurrentMasterNodeInstance(RepositoriesService.class).repository(fsRepoName)
        );
        final IndexId indexId = repositoryData.resolveIndexId(indexName);
        long inMemoryCacheSize = 0;
        long expectedPhysicalCacheSize = 0;
        for (int shardId = 0; shardId < numberOfShards; shardId++) {
            final BlobStoreIndexShardSnapshot snapshot = blobStoreRepository.loadShardSnapshot(
                blobStoreRepository.shardContainer(indexId, shardId),
                snapshotInfo.snapshotId()
            );
            inMemoryCacheSize += snapshot.indexFiles()
                .stream()
                .filter(f -> f.metadata().hashEqualsContents())
                .mapToLong(BlobStoreIndexShardSnapshot.FileInfo::length)
                .sum();

            expectedPhysicalCacheSize += snapshot.indexFiles()
                .stream()
                .filter(f -> f.metadata().hashEqualsContents() == false)
                .mapToLong(BlobStoreIndexShardSnapshot.FileInfo::length)
                .sum();
        }

        assertThat(physicalCacheSize, equalTo(expectedPhysicalCacheSize));
        assertThat(physicalCacheSize + inMemoryCacheSize, equalTo(recoveryState.getIndex().reusedBytes()));
        assertThat("Expected to recover 100% of files", recoveryState.getIndex().recoveredBytesPercent(), equalTo(100.0f));
        assertThat(recoveryState.getIndex().recoveredFromSnapshotBytes(), equalTo(0L));

        for (RecoveryState.FileDetail fileDetail : recoveryState.getIndex().fileDetails()) {
            assertThat(fileDetail.name() + " wasn't mark as reused", fileDetail.reused(), equalTo(true));
        }
    }

    private RecoveryState getRecoveryState(String indexName) {
        final RecoveryResponse recoveryResponse = client().admin().indices().prepareRecoveries(indexName).get();
        Map<String, List<RecoveryState>> shardRecoveries = recoveryResponse.shardRecoveryStates();
        assertThat(shardRecoveries.containsKey(indexName), equalTo(true));
        List<RecoveryState> recoveryStates = shardRecoveries.get(indexName);
        assertThat(recoveryStates.size(), equalTo(1));
        return recoveryStates.get(0);
    }

    @SuppressForbidden(reason = "Uses FileSystem APIs")
    private long getPhysicalCacheSize(Index index, String snapshotUUID) throws Exception {
        final Collection<DiscoveryNode> dataNodes = getDiscoveryNodes().getDataNodes().values();

        assertThat(dataNodes.size(), equalTo(1));

        final String dataNode = dataNodes.iterator().next().getName();

        final IndexService indexService = internalCluster().getInstance(IndicesService.class, dataNode).indexService(index);
        final Path shardCachePath = CacheService.getShardCachePath(indexService.getShard(0).shardPath());

        long physicalCacheSize;
        try (Stream<Path> files = Files.list(shardCachePath.resolve(snapshotUUID))) {
            physicalCacheSize = files.map(Path::toFile).mapToLong(File::length).sum();
        }
        return physicalCacheSize;
    }

    /**
     * A fs repository plugin that allows using its methods from any thread
     */
    public static class TestRepositoryPlugin extends Plugin implements RepositoryPlugin {
        @Override
        public Map<String, Repository.Factory> getRepositories(
            Environment env,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            BigArrays bigArrays,
            RecoverySettings recoverySettings
        ) {
            return Collections.singletonMap(
                "test-fs",
                (metadata) -> new FsRepository(metadata, env, namedXContentRegistry, clusterService, bigArrays, recoverySettings) {
                    @Override
                    protected void assertSnapshotOrGenericThread() {
                        // ignore
                    }
                }
            );
        }
    }
}
