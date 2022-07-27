/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.cache.full;

import org.apache.lucene.document.Document;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.test.BackgroundIndexer;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.xpack.searchablesnapshots.BaseSearchableSnapshotsIntegTestCase;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX;
import static org.elasticsearch.index.IndexSettings.INDEX_SOFT_DELETES_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.searchablesnapshots.cache.full.PersistentCache.resolveCacheIndexFolder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class SearchableSnapshotsPersistentCacheIntegTests extends BaseSearchableSnapshotsIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // to make cache synchronization predictable
            .put(CacheService.SNAPSHOT_CACHE_SYNC_INTERVAL_SETTING.getKey(), TimeValue.timeValueHours(1L))
            .build();
    }

    public void testCacheSurviveRestart() throws Exception {
        final String fsRepoName = randomAlphaOfLength(10);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final String restoredIndexName = randomBoolean() ? indexName : randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final String snapshotName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);

        createRepository(fsRepoName, "fs");

        final Settings.Builder originalIndexSettings = Settings.builder()
            .put(INDEX_SOFT_DELETES_SETTING.getKey(), true)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1);
        createAndPopulateIndex(indexName, originalIndexSettings);

        final SnapshotInfo snapshotInfo = createFullSnapshot(fsRepoName, snapshotName);
        assertThat(snapshotInfo.successfulShards(), greaterThan(0));
        assertThat(snapshotInfo.successfulShards(), equalTo(snapshotInfo.totalShards()));
        assertAcked(client().admin().indices().prepareDelete(indexName));

        final DiscoveryNodes discoveryNodes = client().admin().cluster().prepareState().clear().setNodes(true).get().getState().nodes();
        final String dataNode = randomFrom(discoveryNodes.getDataNodes().values()).getName();

        mountSnapshot(
            fsRepoName,
            snapshotName,
            indexName,
            restoredIndexName,
            Settings.builder().put(INDEX_ROUTING_REQUIRE_GROUP_PREFIX + "._name", dataNode).build()
        );
        ensureGreen(restoredIndexName);

        assertExecutorIsIdle(SearchableSnapshots.CACHE_FETCH_ASYNC_THREAD_POOL_NAME);
        assertExecutorIsIdle(SearchableSnapshots.CACHE_PREWARMING_THREAD_POOL_NAME);

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

        final IndexService indexService = internalCluster().getInstance(IndicesService.class, dataNode).indexService(restoredIndex);
        final ShardPath shardPath = indexService.getShard(0).shardPath();
        final Path shardCachePath = CacheService.getShardCachePath(shardPath);

        assertTrue(Files.isDirectory(shardCachePath));
        final Set<Path> cacheFiles = new HashSet<>();
        try (DirectoryStream<Path> snapshotCacheStream = Files.newDirectoryStream(shardCachePath)) {
            for (final Path snapshotCachePath : snapshotCacheStream) {
                assertTrue(snapshotCachePath + " should be a directory", Files.isDirectory(snapshotCachePath));
                try (DirectoryStream<Path> cacheFileStream = Files.newDirectoryStream(snapshotCachePath)) {
                    for (final Path cacheFilePath : cacheFileStream) {
                        assertTrue(cacheFilePath + " should be a file", Files.isRegularFile(cacheFilePath));
                        cacheFiles.add(cacheFilePath);
                    }
                }
            }
        }
        assertFalse("no cache files found", cacheFiles.isEmpty());

        final CacheService cacheService = internalCluster().getInstance(CacheService.class, dataNode);
        cacheService.synchronizeCache();

        final PersistentCache persistentCache = cacheService.getPersistentCache();
        assertThat(persistentCache.getNumDocs(), equalTo((long) cacheFiles.size()));

        internalCluster().restartNode(dataNode, new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) {
                try {
                    assertTrue(Files.isDirectory(shardCachePath));

                    final Path persistentCacheIndexDir = resolveCacheIndexFolder(shardPath.getRootDataPath());
                    assertTrue(Files.isDirectory(persistentCacheIndexDir));

                    final Map<String, Document> documents = PersistentCache.loadDocuments(persistentCacheIndexDir);
                    assertThat(documents.size(), equalTo(cacheFiles.size()));

                    for (Path cacheFile : cacheFiles) {
                        final String cacheFileName = cacheFile.getFileName().toString();
                        assertTrue(cacheFileName + " should exist on disk", Files.isRegularFile(cacheFile));
                        assertThat(cacheFileName + " should exist in persistent cache index", documents.get(cacheFileName), notNullValue());
                    }
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
                return Settings.EMPTY;
            }
        });

        ensureGreen(restoredIndexName);

        assertExecutorIsIdle(SearchableSnapshots.CACHE_FETCH_ASYNC_THREAD_POOL_NAME);
        assertExecutorIsIdle(SearchableSnapshots.CACHE_PREWARMING_THREAD_POOL_NAME);

        final CacheService cacheServiceAfterRestart = internalCluster().getInstance(CacheService.class, dataNode);
        final PersistentCache persistentCacheAfterRestart = cacheServiceAfterRestart.getPersistentCache();

        cacheFiles.forEach(cacheFile -> assertTrue(cacheFile + " should have survived node restart", Files.exists(cacheFile)));
        assertThat("Cache files should be loaded in cache", persistentCacheAfterRestart.getNumDocs(), equalTo((long) cacheFiles.size()));

        assertAcked(client().admin().indices().prepareDelete(restoredIndexName));
        assertBusy(() -> cacheFiles.forEach(cacheFile -> assertFalse(cacheFile + " should have been cleaned up", Files.exists(cacheFile))));
        assertEmptyPersistentCacheOnDataNodes();
    }

    public void testPersistentCacheCleanUpAfterRelocation() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().ensureAtLeastNumDataNodes(3);
        ensureStableCluster(cluster().size());

        final String prefix = getTestName().toLowerCase(Locale.ROOT) + '-';

        final String fsRepoName = prefix + "repository";
        createRepository(fsRepoName, FsRepository.TYPE);

        final String indexName = prefix + "index";
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, randomIntBetween(0, 1))
                .put(INDEX_SOFT_DELETES_SETTING.getKey(), true)
                .build()
        );
        ensureGreen(indexName);

        final int numDocs = scaledRandomIntBetween(1_000, 5_000);
        try (BackgroundIndexer indexer = new BackgroundIndexer(indexName, "_doc", client(), numDocs)) {
            waitForDocs(numDocs, indexer);
            indexer.stopAndAwaitStopped();
        }
        refresh(indexName);

        final String snapshotName = prefix + "snapshot";
        createFullSnapshot(fsRepoName, snapshotName);

        assertAcked(client().admin().indices().prepareDelete(prefix + '*'));

        final String mountedIndexName = mountSnapshot(fsRepoName, snapshotName, indexName, Settings.EMPTY);

        assertExecutorIsIdle(SearchableSnapshots.CACHE_FETCH_ASYNC_THREAD_POOL_NAME);
        assertExecutorIsIdle(SearchableSnapshots.CACHE_PREWARMING_THREAD_POOL_NAME);
        waitForRelocation();

        RecoveryResponse recoveryResponse = client().admin().indices().prepareRecoveries(mountedIndexName).get();
        assertTrue(recoveryResponse.shardRecoveryStates().containsKey(mountedIndexName));
        assertTrue(
            recoveryResponse.shardRecoveryStates()
                .get(mountedIndexName)
                .stream()
                .allMatch(recoveryState -> recoveryState.getStage() == RecoveryState.Stage.DONE)
        );

        final ClusterStateResponse state = client().admin()
            .cluster()
            .prepareState()
            .clear()
            .setMetadata(true)
            .setIndices(mountedIndexName)
            .get();
        final Index mountedIndex = state.getState().metadata().index(mountedIndexName).getIndex();

        final Set<DiscoveryNode> dataNodes = new HashSet<>();
        for (DiscoveryNode node : getDiscoveryNodes()) {
            if (node.getRoles().stream().anyMatch(DiscoveryNodeRole::canContainData)) {
                IndicesService indicesService = internalCluster().getInstance(IndicesService.class, node.getName());
                if (indicesService.hasIndex(mountedIndex)) {
                    assertBusy(() -> {
                        CacheService cacheService = internalCluster().getInstance(CacheService.class, node.getName());
                        cacheService.synchronizeCache();

                        assertThat(cacheService.getPersistentCache().getNumDocs(), greaterThan(0L));
                        dataNodes.add(node);
                    });
                }
            }
        }

        final DiscoveryNode excludedDataNode = randomFrom(dataNodes);
        logger.info("--> relocating mounted index {} away from {}", mountedIndex, excludedDataNode);
        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings(mountedIndexName)
                .setSettings(Settings.builder().put(INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._id", excludedDataNode.getId()))
                .get()
        );

        ensureGreen(mountedIndexName);

        assertExecutorIsIdle(SearchableSnapshots.CACHE_FETCH_ASYNC_THREAD_POOL_NAME);
        assertExecutorIsIdle(SearchableSnapshots.CACHE_PREWARMING_THREAD_POOL_NAME);

        recoveryResponse = client().admin().indices().prepareRecoveries(mountedIndexName).get();
        assertTrue(recoveryResponse.shardRecoveryStates().containsKey(mountedIndexName));
        assertTrue(
            recoveryResponse.shardRecoveryStates()
                .get(mountedIndexName)
                .stream()
                .allMatch(recoveryState -> recoveryState.getStage() == RecoveryState.Stage.DONE)
        );

        assertBusy(() -> {
            for (DiscoveryNode dataNode : dataNodes) {
                CacheService cacheService = internalCluster().getInstance(CacheService.class, dataNode.getName());
                cacheService.synchronizeCache();

                assertThat(
                    cacheService.getPersistentCache().getNumDocs(),
                    dataNode.equals(excludedDataNode) ? equalTo(0L) : greaterThan(0L)
                );
            }
        }, 30L, TimeUnit.SECONDS);

        logger.info("--> deleting mounted index {}", mountedIndex);
        assertAcked(client().admin().indices().prepareDelete(mountedIndexName));
        assertEmptyPersistentCacheOnDataNodes();
    }

    private void assertEmptyPersistentCacheOnDataNodes() throws Exception {
        final Set<DiscoveryNode> dataNodes = new HashSet<>(getDiscoveryNodes().getDataNodes().values());
        logger.info("--> verifying persistent caches are empty on nodes... {}", dataNodes);
        try {
            assertBusy(() -> {
                for (DiscoveryNode node : org.elasticsearch.core.List.copyOf(dataNodes)) {
                    final CacheService cacheService = internalCluster().getInstance(CacheService.class, node.getName());
                    cacheService.synchronizeCache();
                    assertThat(cacheService.getPersistentCache().getNumDocs(), equalTo(0L));
                    logger.info("--> persistent cache is empty on node {}", node);
                    dataNodes.remove(node);
                }
            });
            logger.info("--> all persistent caches are empty");
        } catch (AssertionError ae) {
            logger.error("--> persistent caches not empty on nodes: {}", dataNodes);
            throw ae;
        }
    }
}
