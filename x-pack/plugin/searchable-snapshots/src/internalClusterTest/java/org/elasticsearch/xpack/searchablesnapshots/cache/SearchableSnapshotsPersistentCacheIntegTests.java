/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.searchablesnapshots.cache;

import org.apache.lucene.document.Document;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.xpack.searchablesnapshots.BaseSearchableSnapshotsIntegTestCase;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX;
import static org.elasticsearch.index.IndexSettings.INDEX_SOFT_DELETES_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.searchablesnapshots.cache.PersistentCache.resolveCacheIndexFolder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;

public class SearchableSnapshotsPersistentCacheIntegTests extends BaseSearchableSnapshotsIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            // ensure the cache is definitely used
            .put(CacheService.SNAPSHOT_CACHE_SIZE_SETTING.getKey(), new ByteSizeValue(1L, ByteSizeUnit.GB))
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
        final String dataNode = randomFrom(discoveryNodes.getDataNodes().values().toArray(DiscoveryNode.class)).getName();

        mountSnapshot(
            fsRepoName,
            snapshotName,
            indexName,
            restoredIndexName,
            Settings.builder().put(INDEX_ROUTING_REQUIRE_GROUP_PREFIX + "._name", dataNode).build()
        );
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

        final CacheService cacheServiceAfterRestart = internalCluster().getInstance(CacheService.class, dataNode);
        final PersistentCache persistentCacheAfterRestart = cacheServiceAfterRestart.getPersistentCache();
        ensureGreen(restoredIndexName);

        cacheFiles.forEach(cacheFile -> assertTrue(cacheFile + " should have survived node restart", Files.exists(cacheFile)));
        assertThat("Cache files should be loaded in cache", persistentCacheAfterRestart.getNumDocs(), equalTo((long) cacheFiles.size()));

        assertAcked(client().admin().indices().prepareDelete(restoredIndexName));

        assertBusy(() -> {
            cacheFiles.forEach(cacheFile -> assertFalse(cacheFile + " should have been cleaned up", Files.exists(cacheFile)));
            cacheServiceAfterRestart.synchronizeCache();
            assertThat(persistentCacheAfterRestart.getNumDocs(), equalTo(0L));
        });
    }
}
