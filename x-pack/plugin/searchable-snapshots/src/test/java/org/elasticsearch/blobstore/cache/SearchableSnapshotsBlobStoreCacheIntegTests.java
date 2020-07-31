/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.blobstore.cache;

import org.apache.lucene.index.IndexFileNames;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.support.FilterBlobContainer;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.searchablesnapshots.BaseSearchableSnapshotsIntegTestCase;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsConstants;
import org.elasticsearch.xpack.searchablesnapshots.cache.CacheService;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.BiConsumer;

import static org.elasticsearch.repositories.blobstore.BlobStoreRepository.INDEX_SHARD_SNAPSHOT_FORMAT;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsConstants.CACHE_FETCH_ASYNC_THREAD_POOL_NAME;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsConstants.CACHE_PREWARMING_THREAD_POOL_NAME;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsConstants.SNAPSHOT_BLOB_CACHE_INDEX;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class SearchableSnapshotsBlobStoreCacheIntegTests extends BaseSearchableSnapshotsIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final List<Class<? extends Plugin>> plugins = new ArrayList<>();
        plugins.add(WaitForSnapshotBlobCacheShardsActivePlugin.class);
        plugins.add(TrackingRepositoryPlugin.class);
        plugins.addAll(super.nodePlugins());
        return List.copyOf(plugins);
    }

    @Override
    protected int numberOfReplicas() {
        return 0;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            // Use a cache range size setting aligned with BufferedIndexInput's buffer size and BlobStoreCacheService's default size
            // TODO randomized this
            .put(
                CacheService.SNAPSHOT_CACHE_RANGE_SIZE_SETTING.getKey(),
                new ByteSizeValue(BlobStoreCacheService.DEFAULT_SIZE, ByteSizeUnit.BYTES)
            )
            .put(CacheService.SNAPSHOT_CACHE_SIZE_SETTING.getKey(), new ByteSizeValue(Long.MAX_VALUE, ByteSizeUnit.BYTES))
            .build();
    }

    public void testBlobStoreCache() throws Exception {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName);

        final List<IndexRequestBuilder> indexRequestBuilders = new ArrayList<>();
        for (int i = scaledRandomIntBetween(0, 10_000); i >= 0; i--) {
            indexRequestBuilders.add(client().prepareIndex(indexName).setSource("text", randomUnicodeOfLength(10), "num", i));
        }
        indexRandom(true, false, true, indexRequestBuilders);
        final long numberOfDocs = indexRequestBuilders.size();
        final NumShards numberOfShards = getNumShards(indexName);

        final ForceMergeResponse forceMergeResponse = client().admin().indices().prepareForceMerge(indexName).setMaxNumSegments(1).get();
        assertThat(forceMergeResponse.getSuccessfulShards(), equalTo(numberOfShards.totalNumShards));
        assertThat(forceMergeResponse.getFailedShards(), equalTo(0));

        final String repositoryName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final Path repositoryLocation = randomRepoPath();
        createFsRepository(repositoryName, repositoryLocation);

        final SnapshotId snapshot = createSnapshot(repositoryName, List.of(indexName));
        assertAcked(client().admin().indices().prepareDelete(indexName));

        // extract the list of blobs per shard from the snapshot directory on disk
        final Map<String, BlobStoreIndexShardSnapshot> blobsInSnapshot = blobsInSnapshot(repositoryLocation, snapshot.getUUID());
        assertThat("Failed to load all shard snapshot metadata files", blobsInSnapshot.size(), equalTo(numberOfShards.numPrimaries));

        // register a new repository that can track blob read operations
        assertAcked(client().admin().cluster().prepareDeleteRepository(repositoryName));
        createRepository(
            repositoryName,
            TrackingRepositoryPlugin.TRACKING,
            Settings.builder().put(FsRepository.LOCATION_SETTING.getKey(), repositoryLocation).build(),
            false
        );
        assertBusy(this::ensureClusterStateConsistency);

        expectThrows(
            IndexNotFoundException.class,
            ".snapshot-blob-cache system index should not be created yet",
            () -> systemClient().admin().indices().prepareGetIndex().addIndices(SNAPSHOT_BLOB_CACHE_INDEX).get()
        );

        final boolean usePrewarming = false; // TODO randomize this and adapt test

        logger.info("--> mount snapshot [{}] as an index for the first time", snapshot);
        final String restoredIndex = mountSnapshot(
            repositoryName,
            snapshot.getName(),
            indexName,
            Settings.builder()
                .put(SearchableSnapshots.SNAPSHOT_CACHE_ENABLED_SETTING.getKey(), true)
                .put(SearchableSnapshots.SNAPSHOT_CACHE_PREWARM_ENABLED_SETTING.getKey(), usePrewarming)
                .build()
        );
        ensureGreen(restoredIndex);
        ensureExecutorsAreIdle();

        logger.info("--> verifying cached documents in system index [{}]", SNAPSHOT_BLOB_CACHE_INDEX);
        assertCachedBlobsInSystemIndex(repositoryName, blobsInSnapshot);

        refreshSystemIndex();
        final long numberOfCachedBlobs = systemClient().prepareSearch(SNAPSHOT_BLOB_CACHE_INDEX).get().getHits().getTotalHits().value;

        ensureBlobStoreRepositoriesWithActiveShards(
            restoredIndex,
            (nodeId, blobStore) -> assertThat(
                "Blob read operations should have been executed on node [" + nodeId + ']',
                blobStore.numberOfReads(),
                greaterThan(0L)
            )
        );

        logger.info("--> verifying documents in index [{}]", restoredIndex);
        assertHitCount(client().prepareSearch(restoredIndex).setSize(0).setTrackTotalHits(true).get(), numberOfDocs);
        assertHitCount(
            client().prepareSearch(restoredIndex)
                .setQuery(QueryBuilders.rangeQuery("num").lte(numberOfDocs))
                .setSize(0)
                .setTrackTotalHits(true)
                .get(),
            numberOfDocs
        );
        assertHitCount(
            client().prepareSearch(restoredIndex)
                .setQuery(QueryBuilders.rangeQuery("num").gt(numberOfDocs + 1))
                .setSize(0)
                .setTrackTotalHits(true)
                .get(),
            0L
        );

        assertAcked(client().admin().indices().prepareDelete(restoredIndex));
        resetTrackedFiles();

        logger.info("--> mount snapshot [{}] as an index for the second time", snapshot);
        final String restoredAgainIndex = mountSnapshot(
            repositoryName,
            snapshot.getName(),
            indexName,
            Settings.builder()
                .put(SearchableSnapshots.SNAPSHOT_CACHE_ENABLED_SETTING.getKey(), true)
                .put(SearchableSnapshots.SNAPSHOT_CACHE_PREWARM_ENABLED_SETTING.getKey(), usePrewarming)
                .build()
        );
        ensureGreen(restoredAgainIndex);
        ensureExecutorsAreIdle();

        logger.info("--> verifying documents in index [{}]", restoredAgainIndex);
        assertHitCount(client().prepareSearch(restoredAgainIndex).setSize(0).setTrackTotalHits(true).get(), numberOfDocs);
        assertHitCount(
            client().prepareSearch(restoredAgainIndex)
                .setQuery(QueryBuilders.rangeQuery("num").lte(numberOfDocs))
                .setSize(0)
                .setTrackTotalHits(true)
                .get(),
            numberOfDocs
        );
        assertHitCount(
            client().prepareSearch(restoredAgainIndex)
                .setQuery(QueryBuilders.rangeQuery("num").gt(numberOfDocs + 1))
                .setSize(0)
                .setTrackTotalHits(true)
                .get(),
            0L
        );

        logger.info("--> verifying cached documents (again) in system index [{}]", SNAPSHOT_BLOB_CACHE_INDEX);
        assertCachedBlobsInSystemIndex(repositoryName, blobsInSnapshot);

        logger.info("--> verifying that no cached blobs were indexed in system index [{}]", SNAPSHOT_BLOB_CACHE_INDEX);
        refreshSystemIndex();
        assertHitCount(systemClient().prepareSearch(SNAPSHOT_BLOB_CACHE_INDEX).setSize(0).get(), numberOfCachedBlobs);

        logger.info("--> verifying blobs read from the repository");
        assertBlobsReadFromRemoteRepository(restoredAgainIndex, blobsInSnapshot);

        resetTrackedFiles();

        logger.info("--> restarting cluster");
        internalCluster().fullRestart(new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                return Settings.builder()
                    .put(super.onNodeStopped(nodeName))
                    .put(WaitForSnapshotBlobCacheShardsActivePlugin.ENABLED.getKey(), true)
                    .build();
            }
        });
        ensureGreen(restoredAgainIndex);
        ensureExecutorsAreIdle();

        logger.info("--> verifying documents in index [{}]", restoredAgainIndex);
        assertHitCount(client().prepareSearch(restoredAgainIndex).setSize(0).setTrackTotalHits(true).get(), numberOfDocs);
        assertHitCount(client().prepareSearch(restoredAgainIndex).setSize(0).setTrackTotalHits(true).get(), numberOfDocs);
        assertHitCount(
            client().prepareSearch(restoredAgainIndex)
                .setQuery(QueryBuilders.rangeQuery("num").lte(numberOfDocs))
                .setSize(0)
                .setTrackTotalHits(true)
                .get(),
            numberOfDocs
        );
        assertHitCount(
            client().prepareSearch(restoredAgainIndex)
                .setQuery(QueryBuilders.rangeQuery("num").gt(numberOfDocs + 1))
                .setSize(0)
                .setTrackTotalHits(true)
                .get(),
            0L
        );

        logger.info("--> verifying cached documents (after restart) in system index [{}]", SNAPSHOT_BLOB_CACHE_INDEX);
        assertCachedBlobsInSystemIndex(repositoryName, blobsInSnapshot);

        logger.info("--> verifying that no cached blobs were indexed in system index [{}] after restart", SNAPSHOT_BLOB_CACHE_INDEX);
        assertHitCount(systemClient().prepareSearch(SNAPSHOT_BLOB_CACHE_INDEX).setSize(0).get(), numberOfCachedBlobs);

        logger.info("--> verifying blobs read from the repository after restart");
        // Without the WaitForSnapshotBlobCacheShardsActivePlugin this would fail
        assertBlobsReadFromRemoteRepository(restoredAgainIndex, blobsInSnapshot);

        // TODO would be great to test when the index is frozen
    }

    /**
     * @return a {@link Client} that can be used to query the blob store cache system index
     */
    private Client systemClient() {
        return new OriginSettingClient(client(), ClientHelper.SEARCHABLE_SNAPSHOTS_ORIGIN);
    }

    private void refreshSystemIndex() {
        final RefreshResponse refreshResponse = systemClient().admin().indices().prepareRefresh(SNAPSHOT_BLOB_CACHE_INDEX).get();
        assertThat(refreshResponse.getSuccessfulShards(), greaterThan(0));
        assertThat(refreshResponse.getFailedShards(), equalTo(0));
    }

    /**
     * Reads a repository location on disk and extracts the list of blobs for each shards
     */
    private Map<String, BlobStoreIndexShardSnapshot> blobsInSnapshot(Path repositoryLocation, String snapshotId) throws IOException {
        final Map<String, BlobStoreIndexShardSnapshot> blobsPerShard = new HashMap<>();
        Files.walkFileTree(repositoryLocation.resolve("indices"), new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                final String fileName = file.getFileName().toString();
                if (fileName.equals("snap-" + snapshotId + ".dat")) {
                    blobsPerShard.put(
                        String.join(
                            "/",
                            snapshotId,
                            file.getParent().getParent().getFileName().toString(),
                            file.getParent().getFileName().toString()
                        ),
                        INDEX_SHARD_SNAPSHOT_FORMAT.deserialize(fileName, xContentRegistry(), Streams.readFully(Files.newInputStream(file)))
                    );
                }
                return FileVisitResult.CONTINUE;
            }
        });
        return Map.copyOf(blobsPerShard);
    }

    private void ensureExecutorsAreIdle() throws Exception {
        assertBusy(() -> {
            for (ThreadPool threadPool : internalCluster().getDataNodeInstances(ThreadPool.class)) {
                for (String threadPoolName : List.of(CACHE_FETCH_ASYNC_THREAD_POOL_NAME, CACHE_PREWARMING_THREAD_POOL_NAME)) {
                    final ThreadPoolExecutor executor = (ThreadPoolExecutor) threadPool.executor(threadPoolName);
                    assertThat(threadPoolName, executor.getQueue().size(), equalTo(0));
                    assertThat(threadPoolName, executor.getActiveCount(), equalTo(0));
                }
            }
        });
    }

    private void assertCachedBlobsInSystemIndex(final String repositoryName, final Map<String, BlobStoreIndexShardSnapshot> blobsInSnapshot)
        throws Exception {
        assertBusy(() -> {
            refreshSystemIndex();

            long numberOfCachedBlobs = 0L;
            for (Map.Entry<String, BlobStoreIndexShardSnapshot> blob : blobsInSnapshot.entrySet()) {
                for (BlobStoreIndexShardSnapshot.FileInfo fileInfo : blob.getValue().indexFiles()) {
                    if (fileInfo.name().startsWith("__") == false) {
                        continue;
                    }

                    final String path = String.join("/", repositoryName, blob.getKey(), fileInfo.physicalName());
                    if (fileInfo.length() <= BlobStoreCacheService.DEFAULT_SIZE * 2) {
                        // file has been fully cached
                        final GetResponse getResponse = systemClient().prepareGet(SNAPSHOT_BLOB_CACHE_INDEX, path + "/@0").get();
                        assertThat(
                            "Blob [" + fileInfo + "] should have been indexed in the blob cache system index as a single document",
                            getResponse.isExists(),
                            is(true)
                        );
                        final CachedBlob cachedBlob = CachedBlob.fromSource(getResponse.getSourceAsMap());
                        assertThat(cachedBlob.from(), equalTo(0L));
                        assertThat(cachedBlob.to(), equalTo(fileInfo.length()));
                        assertThat((long) cachedBlob.length(), equalTo(fileInfo.length()));
                        numberOfCachedBlobs += 1;

                    } else {
                        // first region of file has been cached
                        GetResponse getResponse = systemClient().prepareGet(SNAPSHOT_BLOB_CACHE_INDEX, path + "/@0").get();
                        assertThat(
                            "First region of blob [" + fileInfo + "] should have been indexed in the blob cache system index",
                            getResponse.isExists(),
                            is(true)
                        );

                        CachedBlob cachedBlob = CachedBlob.fromSource(getResponse.getSourceAsMap());
                        assertThat(cachedBlob.from(), equalTo(0L));
                        assertThat(cachedBlob.to(), equalTo((long) BlobStoreCacheService.DEFAULT_SIZE));
                        assertThat(cachedBlob.length(), equalTo(BlobStoreCacheService.DEFAULT_SIZE));
                        numberOfCachedBlobs += 1;
                    }
                }
            }

            refreshSystemIndex();
            assertHitCount(systemClient().prepareSearch(SNAPSHOT_BLOB_CACHE_INDEX).setSize(0).get(), numberOfCachedBlobs);
        });
    }

    private void assertBlobsReadFromRemoteRepository(
        final String indexName,
        final Map<String, BlobStoreIndexShardSnapshot> blobsInSnapshot
    ) {
        ensureBlobStoreRepositoriesWithActiveShards(indexName, (nodeId, blobStore) -> {
            for (Map.Entry<String, List<Tuple<Long, Long>>> blob : blobStore.blobs.entrySet()) {
                final String blobName = blob.getKey();

                if (blobName.endsWith(".dat") || blobName.equals("index-0")) {
                    // The snapshot metadata files are accessed when recovering from the snapshot during restore and do not benefit from
                    // the snapshot blob cache as the files are accessed outside of a searchable snapshot directory
                    assertThat(
                        blobName + " should be fully read from the beginning",
                        blob.getValue().stream().allMatch(read -> read.v1() == 0L),
                        is(true)
                    );
                    // TODO assert it is read til the end

                } else {
                    BlobStoreIndexShardSnapshot.FileInfo blobInfo = null;
                    for (BlobStoreIndexShardSnapshot blobs : blobsInSnapshot.values()) {
                        for (BlobStoreIndexShardSnapshot.FileInfo fileInfo : blobs.indexFiles()) {
                            for (int i = 0; i < fileInfo.numberOfParts(); i++) {
                                if (blobName.endsWith(fileInfo.partName(i))) {
                                    blobInfo = fileInfo;
                                    break;
                                }
                            }
                        }
                    }
                    assertThat("Unable to find blob " + blobName + " in the blobs on disk", blobInfo, notNullValue());

                    final String fileExtension = IndexFileNames.getExtension(blobInfo.physicalName());
                    assertThat(
                        "Only compound files can be read from the blob store after blob store cache is populated",
                        fileExtension,
                        equalTo("cfs")
                    );
                }
            }
        });
    }

    /**
     * Returns the {@link TrackingRepositoryPlugin} instance on a given node.
     */
    private TrackingRepositoryPlugin getTrackingRepositoryInstance(String node) {
        DiscoveryNode discoveryNode = clusterService().state().nodes().resolveNode(node);
        assertThat("Cannot find node " + node, discoveryNode, notNullValue());

        PluginsService pluginsService = internalCluster().getInstance(PluginsService.class, discoveryNode.getName());
        assertThat("Cannot find PluginsService on node " + node, pluginsService, notNullValue());

        List<TrackingRepositoryPlugin> trackingRepositoryPlugins = pluginsService.filterPlugins(TrackingRepositoryPlugin.class);
        assertThat("List of TrackingRepositoryPlugin is null on node " + node, trackingRepositoryPlugins, notNullValue());
        assertThat("List of TrackingRepositoryPlugin is empty on node " + node, trackingRepositoryPlugins, hasSize(1));

        TrackingRepositoryPlugin trackingRepositoryPlugin = trackingRepositoryPlugins.get(0);
        assertThat("TrackingRepositoryPlugin is null on node " + node, trackingRepositoryPlugin, notNullValue());
        return trackingRepositoryPlugin;
    }

    private void resetTrackedFiles() {
        for (String nodeName : internalCluster().getNodeNames()) {
            final TrackingRepositoryPlugin tracker = getTrackingRepositoryInstance(nodeName);
            tracker.reset();
            assertThat(tracker.numberOfReads(), equalTo(0L));
            assertThat(tracker.blobs.size(), equalTo(0));
        }
    }

    private void ensureBlobStoreRepositoriesWithActiveShards(String indexName, BiConsumer<String, TrackingRepositoryPlugin> consumer) {
        final ClusterState clusterState = clusterService().state();
        assertTrue(clusterState.metadata().hasIndex(indexName));
        assertTrue(SearchableSnapshotsConstants.isSearchableSnapshotStore(clusterState.metadata().index(indexName).getSettings()));
        final IndexRoutingTable indexRoutingTable = clusterState.routingTable().index(indexName);
        assertThat(indexRoutingTable, notNullValue());

        final ShardsIterator shardsIterator = indexRoutingTable.randomAllActiveShardsIt();
        assertThat(shardsIterator.size(), greaterThanOrEqualTo(getNumShards(indexName).numPrimaries));

        for (ShardRouting shardRouting : shardsIterator) {
            consumer.accept(shardRouting.currentNodeId(), getTrackingRepositoryInstance(shardRouting.currentNodeId()));
        }
    }

    /**
     * A plugin that allows to track the read  operations on blobs
     */
    public static class TrackingRepositoryPlugin extends Plugin implements RepositoryPlugin {

        static final String TRACKING = "tracking";

        private final Map<String, List<Tuple<Long, Long>>> blobs = new ConcurrentHashMap<>();

        long numberOfReads() {
            return blobs.values().stream().flatMap(Collection::stream).mapToLong(Tuple::v2).sum();
        }

        void reset() {
            blobs.clear();
        }

        @Override
        public Map<String, Repository.Factory> getRepositories(
            Environment env,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            RecoverySettings recoverySettings
        ) {
            return Collections.singletonMap(
                TRACKING,
                (metadata) -> new FsRepository(metadata, env, namedXContentRegistry, clusterService, recoverySettings) {

                    @Override
                    protected BlobStore createBlobStore() throws Exception {
                        final BlobStore delegate = super.createBlobStore();
                        return new BlobStore() {
                            @Override
                            public BlobContainer blobContainer(BlobPath path) {
                                return new TrackingFilesBlobContainer(delegate.blobContainer(path));
                            }

                            @Override
                            public void close() throws IOException {
                                delegate.close();
                            }
                        };
                    }
                }
            );
        }

        class TrackingFilesBlobContainer extends FilterBlobContainer {

            TrackingFilesBlobContainer(BlobContainer delegate) {
                super(delegate);
            }

            @Override
            public InputStream readBlob(String blobName) throws IOException {
                return new CountingInputStream(buildPath(blobName), 0L, super.readBlob(blobName));
            }

            @Override
            public InputStream readBlob(String blobName, long position, long length) throws IOException {
                return new CountingInputStream(buildPath(blobName), position, super.readBlob(blobName, position, length));
            }

            private String buildPath(String name) {
                return path().buildAsString() + name;
            }

            @Override
            protected BlobContainer wrapChild(BlobContainer child) {
                return new TrackingFilesBlobContainer(child);
            }
        }

        class CountingInputStream extends FilterInputStream {

            private final String name;
            private final long offset;

            long bytesRead = 0L;

            protected CountingInputStream(String name, long offset, InputStream in) {
                super(in);
                this.name = name;
                this.offset = offset;
            }

            @Override
            public int read() throws IOException {
                final int result = in.read();
                if (result == -1) {
                    return result;
                }
                bytesRead += 1L;
                return result;
            }

            @Override
            public int read(byte[] b, int offset, int len) throws IOException {
                final int result = in.read(b, offset, len);
                if (result == -1) {
                    return result;
                }
                bytesRead += len;
                return result;
            }

            @Override
            public void close() throws IOException {
                blobs.computeIfAbsent(name, n -> Collections.synchronizedList(new ArrayList<>())).add(Tuple.tuple(offset, bytesRead));
                super.close();
            }
        }
    }

    /**
     * This plugin declares an {@link AllocationDecider} that forces searchable snapshot shards to be allocated after
     * the primary shards of the snapshot blob cache index are started. This way we can ensure that searchable snapshot
     * shards can use the snapshot blob cache index after the cluster restarted.
     */
    public static class WaitForSnapshotBlobCacheShardsActivePlugin extends Plugin implements ClusterPlugin {

        public static Setting<Boolean> ENABLED = Setting.boolSetting(
            "wait_for_snapshot_blob_cache_shards_active.enabled",
            false,
            Setting.Property.NodeScope
        );

        @Override
        public List<Setting<?>> getSettings() {
            return List.of(ENABLED);
        }

        @Override
        public Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {
            if (ENABLED.get(settings) == false) {
                return List.of();
            }
            final String name = "wait_for_snapshot_blob_cache_shards_active";
            return List.of(new AllocationDecider() {

                @Override
                public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                    return canAllocate(shardRouting, allocation);
                }

                @Override
                public Decision canAllocate(ShardRouting shardRouting, RoutingAllocation allocation) {
                    final IndexMetadata indexMetadata = allocation.metadata().index(shardRouting.index());
                    if (SearchableSnapshotsConstants.isSearchableSnapshotStore(indexMetadata.getSettings()) == false) {
                        return allocation.decision(Decision.YES, name, "index is not a searchable snapshot shard - can allocate");
                    }
                    if (allocation.metadata().hasIndex(SNAPSHOT_BLOB_CACHE_INDEX) == false) {
                        return allocation.decision(Decision.YES, name, SNAPSHOT_BLOB_CACHE_INDEX + " is not created yet");
                    }
                    if (allocation.routingTable().hasIndex(SNAPSHOT_BLOB_CACHE_INDEX) == false) {
                        return allocation.decision(Decision.THROTTLE, name, SNAPSHOT_BLOB_CACHE_INDEX + " is not active yet");
                    }
                    final IndexRoutingTable indexRoutingTable = allocation.routingTable().index(SNAPSHOT_BLOB_CACHE_INDEX);
                    if (indexRoutingTable.allPrimaryShardsActive() == false) {
                        return allocation.decision(Decision.THROTTLE, name, SNAPSHOT_BLOB_CACHE_INDEX + " is not active yet");
                    }
                    return allocation.decision(Decision.YES, name, "primary shard for this replica is already active");
                }
            });
        }
    }
}
