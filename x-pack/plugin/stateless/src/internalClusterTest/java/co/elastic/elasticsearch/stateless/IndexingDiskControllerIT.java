/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless;

import co.elastic.elasticsearch.stateless.commits.VirtualBatchedCompoundCommit;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.mockfile.FilterFileStore;
import org.apache.lucene.tests.mockfile.FilterFileSystem;
import org.apache.lucene.tests.mockfile.FilterFileSystemProvider;
import org.apache.lucene.tests.mockfile.FilterPath;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.PathUtilsForTesting;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndexingMemoryController;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.PluginComponentBinding;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class IndexingDiskControllerIT extends AbstractStatelessIntegTestCase {

    private FileSystem defaultFileSystem;
    private FilterPath rootDir;
    private Map<String, ReleasableUsableSpaceFileStore> nodesDataPaths;

    /**
     * Installs a FilterFileSystemProvider that calls ReleasableUsableSpaceFileStore#releaseBytes(long) everytime a file is deleted from
     * the node data path. That way if the node's usable space has been modified for the test with
     * ReleasableUsableSpaceFileStore#resizeUsableSpace(java.lang.Long)) the FilterFileSystemProvider will release back the freed bytes on
     * file deletions.
     */
    @Before
    public void installFilesystemProvider() {
        assertNull(defaultFileSystem);
        defaultFileSystem = PathUtils.getDefaultFileSystem();
        var fileSystemProvider = new FilterFileSystemProvider("test://", defaultFileSystem) {
            @Override
            public FileStore getFileStore(Path path) throws IOException {
                var dataPath = nodesDataPaths.get(path.getFileName().toString());
                if (dataPath != null) {
                    return dataPath;
                }
                return super.getFileStore(path);
            }

            FilterFileSystem getDelegate() {
                return fileSystem;
            }

            @Override
            public void delete(Path path) throws IOException {
                if (nodesDataPaths != null && path.startsWith(rootDir)) {
                    var dataPath = nodesDataPaths.get(rootDir.relativize(path).getName(0).getFileName().toString());
                    if (dataPath != null) {
                        long size = Files.size(path);
                        super.delete(path);
                        dataPath.releaseBytes(size);
                        return;
                    }
                }
                super.delete(path);
            }
        };
        rootDir = new FilterPath(createTempDir(), fileSystemProvider.getDelegate());
        PathUtilsForTesting.installMock(fileSystemProvider.getFileSystem(null));
        assertNull(nodesDataPaths);
        nodesDataPaths = ConcurrentCollections.newConcurrentMap();
    }

    @After
    public void removeFilesystemProvider() {
        assertNotNull(defaultFileSystem);
        PathUtilsForTesting.installMock(defaultFileSystem);
        defaultFileSystem = null;
        nodesDataPaths.clear();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(Stateless.class);
        plugins.add(TestStateless.class);
        plugins.add(InternalSettingsPlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        var nodeDir = "node-" + nodeOrdinal;
        try {
            var dataPath = Files.createDirectories(rootDir.resolve(nodeDir));
            var dataFileStore = new ReleasableUsableSpaceFileStore(dataPath);
            if (nodesDataPaths.putIfAbsent(nodeDir, dataFileStore) != null) {
                throw new AssertionError("Path already exist: " + dataPath);
            }
            return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal, otherSettings))
                .put(Environment.PATH_DATA_SETTING.getKey(), dataPath)
                .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), false)
                .build();
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    @TestLogging(reason = "d", value = "co.elastic.elasticsearch.stateless.IndexingDiskController:TRACE")
    public void testAvailableDiskSpaceBelowLimit() throws Exception {
        final ByteSizeValue reservedDiskSpace = ByteSizeValue.ofMb(randomIntBetween(11, 100));
        startMasterOnlyNode();
        startIndexNode(
            Settings.builder()
                .put(IndexingDiskController.INDEXING_DISK_INTERVAL_TIME_SETTING.getKey(), TimeValue.timeValueHours(1L))
                .put(IndexingDiskController.INDEXING_DISK_RESERVED_BYTES_SETTING.getKey(), reservedDiskSpace)
                .put(IndexingMemoryController.INDEX_BUFFER_SIZE_SETTING.getKey(), ByteSizeValue.ofMb(10L))
                .build()
        );

        // create one or more empty indices
        final int nbIndices = randomIntBetween(1, 6);
        final Set<String> indices = new HashSet<>();
        for (int i = 0; i < nbIndices; i++) {
            var indexName = "index-" + i;
            createIndex(
                indexName,
                indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE)
                    .put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
                    .build()
            );
            indices.add(indexName);
        }
        flushAndRefresh("index-*");

        // block all new commits uploads so that files remain on disk
        blockCommitUploads();

        // index more docs in randomly chosen indices
        var largerIndices = randomNonEmptySubsetOf(indices);
        for (String largerIndex : largerIndices) {
            indexDocs(largerIndex, scaledRandomIntBetween(100, 1_000));
        }

        // retrieve shard by sizes on disk
        var shardsBySize = shardDiskUsages();

        // reduce the usable space so that the largest shard will be flushed and throttled
        long usableSpace = reservedDiskSpace.getBytes() + shardsBySize.stream()
            .mapToLong(IndexingDiskController.ShardDiskUsage::indexBufferRAMBytesUsed)
            .sum() - shardsBySize.get(0).totalSizeInBytes() + 1L;
        setUsableSpaceOnNode(usableSpace);

        final var indexDiskController = internalCluster().getDataNodeInstance(IndexingDiskController.class);
        setUsableSpaceOnNode(usableSpace);

        // largest shard should be throttled
        assertBusy(() -> {
            var shardsDiskUsages = shardDiskUsages();
            assertThat(
                "Available disk space (including Lucene buffer) should be below the reserved limit",
                indexDiskController.availableBytes() - shardsDiskUsages.stream()
                    .mapToLong(IndexingDiskController.ShardDiskUsage::indexBufferRAMBytesUsed)
                    .sum(),
                lessThanOrEqualTo(reservedDiskSpace.getBytes())
            );

            indexDiskController.runNow();
            for (int i = 0; i < nbIndices; i++) {
                var shard = shardsDiskUsages.get(i).shard();
                var shardIndexingStats = shard.indexingStats().getTotal();
                if (i == 0) {
                    assertThat("Shard " + shard.shardId() + " should be throttled", shardIndexingStats.isThrottled(), equalTo(true));
                } else {
                    assertThat("Shard " + shard.shardId() + " should not be throttled", shardIndexingStats.isThrottled(), equalTo(false));
                }
            }
        });

        unblockCommitUploads();

        assertBusy(() -> {
            indexDiskController.runNow();
            var shardsDiskUsages = shardDiskUsages();
            for (var shard : shardsDiskUsages) {
                assertThat(
                    "Shard " + shard.shard().shardId() + " should not be throttled",
                    shard.shard().indexingStats().getTotal().isThrottled(),
                    equalTo(false)
                );
            }
            assertThat(
                "Available disk space (including Lucene buffer) should be above the reserved limit",
                indexDiskController.availableBytes() - shardsDiskUsages.stream()
                    .mapToLong(IndexingDiskController.ShardDiskUsage::indexBufferRAMBytesUsed)
                    .sum(),
                greaterThan(reservedDiskSpace.getBytes())
            );
        });

        setUsableSpaceOnNode(null);
    }

    private void blockCommitUploads() {
        internalCluster().getDataNodeInstance(TestObjectStoreService.class).block();
    }

    private void unblockCommitUploads() {
        internalCluster().getDataNodeInstance(TestObjectStoreService.class).unblock();
    }

    private static List<IndexingDiskController.ShardDiskUsage> shardDiskUsages() {
        List<IndexingDiskController.ShardDiskUsage> list = new ArrayList<>();
        for (IndexService indexService : internalCluster().getDataNodeInstance(IndicesService.class)) {
            for (IndexShard indexShard : indexService) {
                list.add(IndexingDiskController.shardDiskUsage(indexShard));
            }
        }
        list.sort(IndexingDiskController.LARGEST_SHARD_FIRST_COMPARATOR);
        return List.copyOf(list);
    }

    private void setUsableSpaceOnNode(@Nullable Long value) throws IOException {
        var nodeEnv = internalCluster().getDataNodeInstance(Environment.class);
        assertThat(nodeEnv, notNullValue());
        var fileStore = nodesDataPaths.get(nodeEnv.dataFiles()[0].getFileName().toString());
        assertThat(fileStore, notNullValue());
        if (value != null) {
            logger.debug("--> setting usable space to {} bytes on data node");
        } else {
            logger.debug("--> resetting usable space on data node");
        }
        fileStore.resizeUsableSpace(value);
    }

    /**
     * A FileStore that allows to set the usable space and then release more bytes.
     */
    private class ReleasableUsableSpaceFileStore extends FilterFileStore {

        private volatile Long usableSpaceInBytes = null;

        ReleasableUsableSpaceFileStore(Path dataPath) throws IOException {
            super(dataPath.getFileSystem().provider().getFileStore(dataPath), dataPath.getFileSystem().provider().getScheme());
        }

        @Override
        public long getUsableSpace() throws IOException {
            var bytes = this.usableSpaceInBytes;
            if (bytes != null) {
                return bytes;
            }
            return super.getUsableSpace();
        }

        public synchronized void resizeUsableSpace(@Nullable Long bytes) throws IOException {
            if (bytes != null && bytes > getTotalSpace()) {
                throw new IllegalStateException("Resizing usable space to [" + bytes + "] exceed total space");
            }
            this.usableSpaceInBytes = bytes;
        }

        public synchronized void releaseBytes(long delta) throws IOException {
            if (usableSpaceInBytes != null) {
                resizeUsableSpace(usableSpaceInBytes + delta);
            }
        }
    }

    public static class TestStateless extends Stateless {

        public TestStateless(Settings settings) {
            super(settings);
        }

        @Override
        public Collection<Object> createComponents(PluginServices services) {
            Collection<Object> components = super.createComponents(services);
            // TODO remove this Guice magic once MockRepository can block commit files only
            var optional = components.stream().filter(o -> o instanceof TestObjectStoreService).findFirst();
            components.add(new PluginComponentBinding<>(ObjectStoreService.class, optional.get()));
            return components;
        }

        @Override
        protected ObjectStoreService createObjectStoreService(
            Settings settings,
            Supplier<RepositoriesService> repositoriesServiceSupplier,
            ThreadPool threadPool,
            ClusterService clusterService
        ) {
            return new TestObjectStoreService(settings, repositoriesServiceSupplier, threadPool, clusterService);
        }
    }

    /**
     * An ObjectStoreService implementation that allows to block all commits uploads.
     */
    public static class TestObjectStoreService extends ObjectStoreService {

        private final List<BlockedListener<Void>> blockedListeners = new ArrayList<>();
        private boolean blocked;

        @Inject
        public TestObjectStoreService(
            Settings settings,
            Supplier<RepositoriesService> supplier,
            ThreadPool threadPool,
            ClusterService clusterService
        ) {
            super(settings, supplier, threadPool, clusterService);
        }

        synchronized void block() {
            assert blocked == false;
            assert blockedListeners.isEmpty();
            blocked = true;
        }

        synchronized void unblock() {
            assert blocked;
            var it = blockedListeners.iterator();
            while (it.hasNext()) {
                var listener = it.next();
                listener.unblock();
                it.remove();
            }
            blocked = false;
        }

        @Override
        public void uploadBatchedCompoundCommitFile(
            long primaryTerm,
            Directory directory,
            long commitStartNanos,
            VirtualBatchedCompoundCommit pendingCommit,
            ActionListener<Void> listener
        ) {
            synchronized (this) {
                if (blocked) {
                    var wrappedListener = new BlockedListener<>(listener);
                    blockedListeners.add(wrappedListener);
                    listener = wrappedListener;
                }
            }

            super.uploadBatchedCompoundCommitFile(primaryTerm, directory, commitStartNanos, pendingCommit, listener);
        }
    }

    private static class BlockedListener<T> extends SubscribableListener<T> {

        private final ActionListener<T> delegate;

        private BlockedListener(ActionListener<T> delegate) {
            this.delegate = delegate;
        }

        void unblock() {
            addListener(delegate);
        }
    }
}
