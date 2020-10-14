/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.apache.lucene.mockfile.FilterFileStore;
import org.apache.lucene.mockfile.FilterFileSystemProvider;
import org.apache.lucene.mockfile.FilterPath;
import org.apache.lucene.util.Constants;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider.Rebalance;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.io.PathUtilsForTesting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.monitor.fs.FsService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.RestoreInfo;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.junit.After;
import org.junit.Before;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;
import static org.elasticsearch.index.store.Store.INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DiskThresholdDeciderIT extends ESIntegTestCase {

    private static TestFileSystemProvider fileSystemProvider;

    private FileSystem defaultFileSystem;

    @Before
    public void installFilesystemProvider() {
        assertNull(defaultFileSystem);
        defaultFileSystem = PathUtils.getDefaultFileSystem();
        assertNull(fileSystemProvider);
        fileSystemProvider = new TestFileSystemProvider(defaultFileSystem, createTempDir());
        PathUtilsForTesting.installMock(fileSystemProvider.getFileSystem(null));
    }

    @After
    public void removeFilesystemProvider() {
        fileSystemProvider = null;
        assertNotNull(defaultFileSystem);
        PathUtilsForTesting.installMock(defaultFileSystem); // set the default filesystem back
        defaultFileSystem = null;
    }

    private static final long WATERMARK_BYTES = new ByteSizeValue(10, ByteSizeUnit.KB).getBytes();

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        final Path dataPath = fileSystemProvider.getRootDir().resolve("node-" + nodeOrdinal);
        try {
            Files.createDirectories(dataPath);
        } catch (IOException e) {
            throw new AssertionError("unexpected", e);
        }
        fileSystemProvider.addTrackedPath(dataPath);
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(Environment.PATH_DATA_SETTING.getKey(), dataPath)
                .put(FsService.ALWAYS_REFRESH_SETTING.getKey(), true)
                .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), WATERMARK_BYTES + "b")
                .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), WATERMARK_BYTES + "b")
                .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "0b")
                .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.getKey(), "0ms")
                .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(InternalSettingsPlugin.class);
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/62326")
    public void testHighWatermarkNotExceeded() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String dataNodeName = internalCluster().startDataOnlyNode();
        ensureStableCluster(3);

        final InternalClusterInfoService clusterInfoService
                = (InternalClusterInfoService) internalCluster().getMasterNodeInstance(ClusterInfoService.class);
        internalCluster().getMasterNodeInstance(ClusterService.class).addListener(event -> clusterInfoService.refresh());

        final String dataNode0Id = internalCluster().getInstance(NodeEnvironment.class, dataNodeName).nodeId();
        final Path dataNode0Path = internalCluster().getInstance(Environment.class, dataNodeName).dataFiles()[0];

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 6)
                .put(INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING.getKey(), "0ms")
                .build());
        final long minShardSize = createReasonableSizedShards(indexName);

        // reduce disk size of node 0 so that no shards fit below the high watermark, forcing all shards onto the other data node
        // (subtract the translog size since the disk threshold decider ignores this and may therefore move the shard back again)
        fileSystemProvider.getTestFileStore(dataNode0Path).setTotalSpace(minShardSize + WATERMARK_BYTES - 1L);
        refreshDiskUsage();
        assertBusy(() -> assertThat(getShardRoutings(dataNode0Id, indexName), empty()));

        // increase disk size of node 0 to allow just enough room for one shard, and check that it's rebalanced back
        fileSystemProvider.getTestFileStore(dataNode0Path).setTotalSpace(minShardSize + WATERMARK_BYTES + 1L);
        refreshDiskUsage();
        assertBusy(() -> assertThat(getShardRoutings(dataNode0Id, indexName), hasSize(1)));
    }

    public void testRestoreSnapshotAllocationDoesNotExceedWatermark() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String dataNodeName = internalCluster().startDataOnlyNode();
        ensureStableCluster(3);

        assertAcked(client().admin().cluster().preparePutRepository("repo")
            .setType(FsRepository.TYPE)
            .setSettings(Settings.builder()
                .put("location", randomRepoPath())
                .put("compress", randomBoolean())));

        final InternalClusterInfoService clusterInfoService
            = (InternalClusterInfoService) internalCluster().getMasterNodeInstance(ClusterInfoService.class);
        internalCluster().getMasterNodeInstance(ClusterService.class).addListener(event -> clusterInfoService.refresh());

        final String dataNode0Id = internalCluster().getInstance(NodeEnvironment.class, dataNodeName).nodeId();
        final Path dataNode0Path = internalCluster().getInstance(Environment.class, dataNodeName).dataFiles()[0];

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 6)
            .put(INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING.getKey(), "0ms")
            .build());
        final long minShardSize = createReasonableSizedShards(indexName);

        final CreateSnapshotResponse createSnapshotResponse = client().admin().cluster().prepareCreateSnapshot("repo", "snap")
            .setWaitForCompletion(true).get();
        final SnapshotInfo snapshotInfo = createSnapshotResponse.getSnapshotInfo();
        assertThat(snapshotInfo.successfulShards(), is(snapshotInfo.totalShards()));
        assertThat(snapshotInfo.state(), is(SnapshotState.SUCCESS));

        assertAcked(client().admin().indices().prepareDelete(indexName).get());

        // reduce disk size of node 0 so that no shards fit below the low watermark, forcing shards to be assigned to the other data node
        fileSystemProvider.getTestFileStore(dataNode0Path).setTotalSpace(minShardSize + WATERMARK_BYTES - 1L);
        refreshDiskUsage();

        assertAcked(client().admin().cluster().prepareUpdateSettings()
            .setTransientSettings(Settings.builder()
                .put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), Rebalance.NONE.toString())
                .build())
            .get());

        final RestoreSnapshotResponse restoreSnapshotResponse = client().admin().cluster().prepareRestoreSnapshot("repo", "snap")
            .setWaitForCompletion(true).get();
        final RestoreInfo restoreInfo = restoreSnapshotResponse.getRestoreInfo();
        assertThat(restoreInfo.successfulShards(), is(snapshotInfo.totalShards()));
        assertThat(restoreInfo.failedShards(), is(0));

        assertBusy(() -> assertThat(getShardRoutings(dataNode0Id, indexName), empty()));

        assertAcked(client().admin().cluster().prepareUpdateSettings()
            .setTransientSettings(Settings.builder()
                .putNull(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey())
                .build())
            .get());

        // increase disk size of node 0 to allow just enough room for one shard, and check that it's rebalanced back
        fileSystemProvider.getTestFileStore(dataNode0Path).setTotalSpace(minShardSize + WATERMARK_BYTES + 1L);
        refreshDiskUsage();
        assertBusy(() -> assertThat(getShardRoutings(dataNode0Id, indexName), hasSize(1)));
    }

    private Set<ShardRouting> getShardRoutings(final String nodeId, final String indexName) {
        final Set<ShardRouting> shardRoutings = new HashSet<>();
        for (IndexShardRoutingTable indexShardRoutingTable : client().admin().cluster().prepareState().clear().setRoutingTable(true)
                .get().getState().getRoutingTable().index(indexName)) {
            for (ShardRouting shard : indexShardRoutingTable.shards()) {
                assertThat(shard.state(), equalTo(ShardRoutingState.STARTED));
                if (shard.currentNodeId().equals(nodeId)) {
                    shardRoutings.add(shard);
                }
            }
        }
        return shardRoutings;
    }

    /**
     * Index documents until all the shards are at least WATERMARK_BYTES in size, and return the size of the smallest shard
     */
    private long createReasonableSizedShards(final String indexName) throws InterruptedException {
        while (true) {
            final IndexRequestBuilder[] indexRequestBuilders = new IndexRequestBuilder[scaledRandomIntBetween(100, 10000)];
            for (int i = 0; i < indexRequestBuilders.length; i++) {
                indexRequestBuilders[i] = client().prepareIndex(indexName).setSource("field", randomAlphaOfLength(10));
            }
            indexRandom(true, indexRequestBuilders);
            forceMerge();
            refresh();

            final ShardStats[] shardStatses = client().admin().indices().prepareStats(indexName)
                    .clear().setStore(true).setTranslog(true).get().getShards();
            final long[] shardSizes = new long[shardStatses.length];
            for (ShardStats shardStats : shardStatses) {
                shardSizes[shardStats.getShardRouting().id()] = shardStats.getStats().getStore().sizeInBytes();
            }

            final long minShardSize = Arrays.stream(shardSizes).min().orElseThrow(() -> new AssertionError("no shards"));
            if (minShardSize > WATERMARK_BYTES) {
                return minShardSize;
            }
        }
    }

    private void refreshDiskUsage() {
        assertFalse(client().admin().cluster().prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNoRelocatingShards(true)
            .setWaitForNoInitializingShards(true)
            .get()
            .isTimedOut());

        final ClusterInfoService clusterInfoService = internalCluster().getMasterNodeInstance(ClusterInfoService.class);
        ((InternalClusterInfoService) clusterInfoService).refresh();
        // if the nodes were all under the low watermark already (but unbalanced) then a change in the disk usage doesn't trigger a reroute
        // even though it's now possible to achieve better balance, so we have to do an explicit reroute. TODO fix this?
        if (StreamSupport.stream(clusterInfoService.getClusterInfo().getNodeMostAvailableDiskUsages().values().spliterator(), false)
            .allMatch(cur -> cur.value.getFreeBytes() > WATERMARK_BYTES)) {
            assertAcked(client().admin().cluster().prepareReroute());
        }

        assertFalse(client().admin().cluster().prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNoRelocatingShards(true)
            .setWaitForNoInitializingShards(true)
            .get()
            .isTimedOut());
    }

    private static class TestFileStore extends FilterFileStore {

        private final Path path;

        private volatile long totalSpace = -1;

        TestFileStore(FileStore delegate, String scheme, Path path) {
            super(delegate, scheme);
            this.path = path;
        }

        @Override
        public String name() {
            return "fake"; // Lucene's is-spinning-disk check expects the device name here
        }

        @Override
        public long getTotalSpace() throws IOException {
            final long totalSpace = this.totalSpace;
            if (totalSpace == -1) {
                return super.getTotalSpace();
            } else {
                return totalSpace;
            }
        }

        public void setTotalSpace(long totalSpace) {
            assertThat(totalSpace, anyOf(is(-1L), greaterThan(0L)));
            this.totalSpace = totalSpace;
        }

        @Override
        public long getUsableSpace() throws IOException {
            final long totalSpace = this.totalSpace;
            if (totalSpace == -1) {
                return super.getUsableSpace();
            } else {
                return Math.max(0L, totalSpace - getTotalFileSize(path));
            }
        }

        @Override
        public long getUnallocatedSpace() throws IOException {
            final long totalSpace = this.totalSpace;
            if (totalSpace == -1) {
                return super.getUnallocatedSpace();
            } else {
                return Math.max(0L, totalSpace - getTotalFileSize(path));
            }
        }

        private static long getTotalFileSize(Path path) throws IOException {
            if (Files.isRegularFile(path)) {
                try {
                    return Files.size(path);
                } catch (NoSuchFileException | FileNotFoundException e) {
                    // probably removed
                    return 0L;
                }
            } else if (path.getFileName().toString().equals("_state") || path.getFileName().toString().equals("translog")) {
                // ignore metadata and translog, since the disk threshold decider only cares about the store size
                return 0L;
            } else {
                try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(path)) {
                    long total = 0L;
                    for (Path subpath : directoryStream) {
                        total += getTotalFileSize(subpath);
                    }
                    return total;
                } catch (NotDirectoryException | NoSuchFileException | FileNotFoundException e) {
                    // probably removed
                    return 0L;
                }
            }
        }
    }

    private static class TestFileSystemProvider extends FilterFileSystemProvider {
        private final Map<Path, TestFileStore> trackedPaths = newConcurrentMap();
        private final Path rootDir;

        TestFileSystemProvider(FileSystem delegateInstance, Path rootDir) {
            super("diskthreshold://", delegateInstance);
            this.rootDir = new FilterPath(rootDir, fileSystem);
        }

        Path getRootDir() {
            return rootDir;
        }

        void addTrackedPath(Path path) {
            assertTrue(path + " starts with " + rootDir, path.startsWith(rootDir));
            final FileStore fileStore;
            try {
                fileStore = super.getFileStore(path);
            } catch (IOException e) {
                throw new AssertionError("unexpected", e);
            }
            assertNull(trackedPaths.put(path, new TestFileStore(fileStore, getScheme(), path)));
        }

        @Override
        public FileStore getFileStore(Path path) {
            return getTestFileStore(path);
        }

        TestFileStore getTestFileStore(Path path) {
            final TestFileStore fileStore = trackedPaths.get(path);
            if (fileStore != null) {
                return fileStore;
            }

            // On Linux, and only Linux, Lucene obtains a filestore for the index in order to determine whether it's on a spinning disk or
            // not so it can configure the merge scheduler accordingly
            assertTrue(path + " not tracked and not on Linux", Constants.LINUX);
            final Set<Path> containingPaths = trackedPaths.keySet().stream().filter(path::startsWith).collect(Collectors.toSet());
            assertThat(path + " not contained in a unique tracked path", containingPaths, hasSize(1));
            return trackedPaths.get(containingPaths.iterator().next());
        }

        void clearTrackedPaths() throws IOException {
            for (Path path : trackedPaths.keySet()) {
                IOUtils.rm(path);
            }
            trackedPaths.clear();
        }
    }
}
