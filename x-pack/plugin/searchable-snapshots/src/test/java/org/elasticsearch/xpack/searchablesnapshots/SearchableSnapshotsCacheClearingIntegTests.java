/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.searchablesnapshots;

import org.apache.lucene.mockfile.FilterFileSystemProvider;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.io.PathUtilsForTesting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotAction;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;
import org.elasticsearch.xpack.searchablesnapshots.cache.CacheService;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX;
import static org.elasticsearch.index.IndexSettings.INDEX_SOFT_DELETES_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class SearchableSnapshotsCacheClearingIntegTests extends BaseSearchableSnapshotsIntegTestCase {

    private static DeleteBlockingFileSystemProvider deleteBlockingFileSystemProvider;

    @BeforeClass
    public static void installDeleteBlockingFileSystemProvider() {
        FileSystem current = PathUtils.getDefaultFileSystem();
        deleteBlockingFileSystemProvider = new DeleteBlockingFileSystemProvider(current);
        PathUtilsForTesting.installMock(deleteBlockingFileSystemProvider.getFileSystem(null));
    }

    @AfterClass
    public static void removeDeleteBlockingFileSystemProvider() {
        PathUtilsForTesting.teardown();
    }

    void startBlockingDeletes() {
        deleteBlockingFileSystemProvider.injectFailures.set(true);
    }

    void stopBlockingDeletes() {
        deleteBlockingFileSystemProvider.injectFailures.set(false);
    }

    private static class DeleteBlockingFileSystemProvider extends FilterFileSystemProvider {

        AtomicBoolean injectFailures = new AtomicBoolean();

        DeleteBlockingFileSystemProvider(FileSystem inner) {
            super("deleteblocking://", inner);
        }

        @Override
        public boolean deleteIfExists(Path path) throws IOException {
            if (injectFailures.get()) {
                throw new IOException("blocked deletion of " + path);
            } else {
                return super.deleteIfExists(path);
            }
        }

    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            // ensure the cache is definitely used
            .put(CacheService.SNAPSHOT_CACHE_SIZE_SETTING.getKey(), new ByteSizeValue(1L, ByteSizeUnit.GB))
            .build();
    }

    public void testCacheDirectoriesRemovedOnStartup() throws Exception {
        final String fsRepoName = randomAlphaOfLength(10);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final String restoredIndexName = randomBoolean() ? indexName : randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final String snapshotName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);

        createRepo(fsRepoName);

        final Settings.Builder originalIndexSettings = Settings.builder()
            .put(INDEX_SOFT_DELETES_SETTING.getKey(), true)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1);
        createAndPopulateIndex(indexName, originalIndexSettings);

        CreateSnapshotResponse createSnapshotResponse = client().admin()
            .cluster()
            .prepareCreateSnapshot(fsRepoName, snapshotName)
            .setWaitForCompletion(true)
            .get();
        final SnapshotInfo snapshotInfo = createSnapshotResponse.getSnapshotInfo();
        assertThat(snapshotInfo.successfulShards(), greaterThan(0));
        assertThat(snapshotInfo.successfulShards(), equalTo(snapshotInfo.totalShards()));
        assertAcked(client().admin().indices().prepareDelete(indexName));

        final DiscoveryNodes discoveryNodes = client().admin().cluster().prepareState().clear().setNodes(true).get().getState().nodes();
        final String dataNode = randomFrom(discoveryNodes.getDataNodes().values().toArray(DiscoveryNode.class)).getName();

        final MountSearchableSnapshotRequest req = new MountSearchableSnapshotRequest(
            restoredIndexName,
            fsRepoName,
            snapshotName,
            indexName,
            Settings.builder().put(INDEX_ROUTING_REQUIRE_GROUP_PREFIX + "._name", dataNode).build(),
            Strings.EMPTY_ARRAY,
            true
        );

        final RestoreSnapshotResponse restoreSnapshotResponse = client().execute(MountSearchableSnapshotAction.INSTANCE, req).get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));
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
        final Path shardCachePath = CacheService.getShardCachePath(indexService.getShard(0).shardPath());
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

        startBlockingDeletes();
        internalCluster().restartNode(dataNode, new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) {
                assertTrue(Files.isDirectory(shardCachePath));
                for (Path cacheFile : cacheFiles) {
                    assertTrue(cacheFile + " should not have been cleaned up yet", Files.isRegularFile(cacheFile));
                }
                stopBlockingDeletes();
                return Settings.EMPTY;
            }
        });

        ensureGreen(restoredIndexName);

        for (Path cacheFile : cacheFiles) {
            assertFalse(cacheFile + " should have been cleaned up", Files.exists(cacheFile));
        }

        assertAcked(client().admin().indices().prepareDelete(restoredIndexName));
    }
}
