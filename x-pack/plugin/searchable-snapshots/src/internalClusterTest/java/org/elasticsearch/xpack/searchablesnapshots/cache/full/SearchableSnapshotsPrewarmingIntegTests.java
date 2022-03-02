/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.cache.full;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.support.FilterBlobContainer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.blobstore.ESBlobStoreRepositoryIntegTestCase;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotAction;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;
import org.elasticsearch.xpack.searchablesnapshots.LocalStateSearchableSnapshots;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots;
import org.junit.After;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_CACHE_ENABLED_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_CACHE_PREWARM_ENABLED_SETTING;
import static org.hamcrest.Matchers.emptyCollectionOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@LuceneTestCase.SuppressFileSystems(value = "ExtrasFS") // we don't want extra dirs to be added to the shards snapshot_cache directories
public class SearchableSnapshotsPrewarmingIntegTests extends ESSingleNodeTestCase {

    private static final int MAX_NUMBER_OF_INDICES = 10;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(LocalStateSearchableSnapshots.class, TrackingRepositoryPlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put(super.nodeSettings())
            .put(LicenseService.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial")
            .put(SearchableSnapshots.CACHE_FETCH_ASYNC_THREAD_POOL_SETTING + ".max", randomIntBetween(1, 32))
            .put(SearchableSnapshots.CACHE_FETCH_ASYNC_THREAD_POOL_SETTING + ".keep_alive", "1s")
            .put(SearchableSnapshots.CACHE_PREWARMING_THREAD_POOL_SETTING + ".max", randomIntBetween(1, 32))
            .put(SearchableSnapshots.CACHE_PREWARMING_THREAD_POOL_SETTING + ".keep_alive", "1s")
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING.getKey(), MAX_NUMBER_OF_INDICES)
            .build();
    }

    public void testConcurrentPrewarming() throws Exception {
        final int nbIndices = randomIntBetween(1, MAX_NUMBER_OF_INDICES);
        logger.debug("--> creating [{}] indices", nbIndices);

        final Map<String, Integer> shardsPerIndex = new HashMap<>();
        for (int index = 0; index < nbIndices; index++) {
            final String indexName = "index-" + index;
            final int nbShards = randomIntBetween(1, 5);
            createIndex(
                indexName,
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, nbShards)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .build()
            );
            shardsPerIndex.put(indexName, nbShards);
        }

        logger.debug("--> indexing documents");
        final Map<String, Long> docsPerIndex = new HashMap<>();
        for (int index = 0; index < nbIndices; index++) {
            final String indexName = "index-" + index;

            final long nbDocs = scaledRandomIntBetween(0, 500);
            logger.debug("--> indexing [{}] documents in {}", nbDocs, indexName);

            if (nbDocs > 0) {
                final BulkRequestBuilder bulkRequest = client().prepareBulk();
                for (int i = 0; i < nbDocs; i++) {
                    bulkRequest.add(client().prepareIndex(indexName).setSource("foo", randomBoolean() ? "bar" : "baz"));
                }
                final BulkResponse bulkResponse = bulkRequest.get();
                assertThat(bulkResponse.status(), is(RestStatus.OK));
                assertThat(bulkResponse.hasFailures(), is(false));
            }
            docsPerIndex.put(indexName, nbDocs);
        }

        final Path repositoryPath = node().getEnvironment().resolveRepoFile(randomAlphaOfLength(10));
        final Settings.Builder repositorySettings = Settings.builder().put("location", repositoryPath);
        if (randomBoolean()) {
            repositorySettings.put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES);
        }

        logger.debug("--> registering repository");
        assertAcked(
            client().admin().cluster().preparePutRepository("repository").setType(FsRepository.TYPE).setSettings(repositorySettings.build())
        );

        logger.debug("--> snapshotting indices");
        final CreateSnapshotResponse createSnapshotResponse = client().admin()
            .cluster()
            .prepareCreateSnapshot("repository", "snapshot")
            .setIncludeGlobalState(false)
            .setIndices("index-*")
            .setWaitForCompletion(true)
            .get();

        final int totalShards = shardsPerIndex.values().stream().mapToInt(i -> i).sum();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(totalShards));
        assertThat(createSnapshotResponse.getSnapshotInfo().failedShards(), equalTo(0));

        ensureGreen("index-*");

        logger.debug("--> deleting indices");
        assertAcked(client().admin().indices().prepareDelete("index-*"));

        logger.debug("--> deleting repository");
        assertAcked(client().admin().cluster().prepareDeleteRepository("repository"));

        logger.debug("--> registering tracking repository");
        assertAcked(
            client().admin()
                .cluster()
                .preparePutRepository("repository")
                .setType("tracking")
                .setVerify(false)
                .setSettings(repositorySettings.build())
        );

        TrackingRepositoryPlugin tracker = getTrackingRepositoryPlugin();
        assertThat(tracker.totalFilesRead(), equalTo(0L));
        assertThat(tracker.totalBytesRead(), equalTo(0L));

        final Map<String, List<String>> exclusionsPerIndex = new HashMap<>();
        for (int index = 0; index < nbIndices; index++) {
            exclusionsPerIndex.put("index-" + index, randomSubsetOf(Arrays.asList("fdt", "fdx", "nvd", "dvd", "tip", "cfs", "dim")));
        }

        logger.debug("--> mounting indices");
        final Thread[] threads = new Thread[nbIndices];
        final AtomicArray<Throwable> throwables = new AtomicArray<>(nbIndices);
        final CountDownLatch latch = new CountDownLatch(1);

        for (int i = 0; i < threads.length; i++) {
            int threadId = i;
            final String indexName = "index-" + threadId;
            final Thread thread = new Thread(() -> {
                try {
                    latch.await();

                    final Settings restoredIndexSettings = Settings.builder()
                        .put(SNAPSHOT_CACHE_ENABLED_SETTING.getKey(), true)
                        .put(SNAPSHOT_CACHE_PREWARM_ENABLED_SETTING.getKey(), true)
                        .putList(SearchableSnapshots.SNAPSHOT_CACHE_EXCLUDED_FILE_TYPES_SETTING.getKey(), exclusionsPerIndex.get(indexName))
                        .build();

                    logger.info("--> mounting snapshot as index [{}] with settings {}", indexName, restoredIndexSettings);
                    final RestoreSnapshotResponse restoreSnapshotResponse = client().execute(
                        MountSearchableSnapshotAction.INSTANCE,
                        new MountSearchableSnapshotRequest(
                            indexName,
                            "repository",
                            "snapshot",
                            indexName,
                            restoredIndexSettings,
                            Strings.EMPTY_ARRAY,
                            true,
                            MountSearchableSnapshotRequest.Storage.FULL_COPY
                        )
                    ).get();

                    ensureGreen(indexName);

                    assertThat(restoreSnapshotResponse.status(), is(RestStatus.OK));
                    assertThat(restoreSnapshotResponse.getRestoreInfo().successfulShards(), equalTo(shardsPerIndex.get(indexName)));
                    assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));
                    assertHitCount(client().prepareSearch(indexName).setSize(0).setTrackTotalHits(true).get(), docsPerIndex.get(indexName));

                    final GetSettingsResponse getSettingsResponse = client().admin().indices().prepareGetSettings(indexName).get();
                    assertThat(getSettingsResponse.getSetting(indexName, SNAPSHOT_CACHE_ENABLED_SETTING.getKey()), equalTo("true"));
                    assertThat(getSettingsResponse.getSetting(indexName, SNAPSHOT_CACHE_PREWARM_ENABLED_SETTING.getKey()), equalTo("true"));

                } catch (Throwable t) {
                    logger.error(() -> new ParameterizedMessage("Fail to mount snapshot for index [{}]", indexName), t);
                    throwables.setOnce(threadId, t);
                }
            });
            threads[threadId] = thread;
            thread.start();
        }

        latch.countDown();
        for (Thread thread : threads) {
            thread.join();
        }

        assertThat("Failed to mount snapshot as indices", throwables.asList(), emptyCollectionOf(Throwable.class));

        logger.debug("--> waiting for background cache prewarming to");
        assertBusy(() -> {
            final ThreadPool threadPool = getInstanceFromNode(ThreadPool.class);
            assertThat(threadPool.info(SearchableSnapshots.CACHE_FETCH_ASYNC_THREAD_POOL_NAME).getQueueSize(), nullValue());
            assertThat(threadPool.info(SearchableSnapshots.CACHE_PREWARMING_THREAD_POOL_NAME).getQueueSize(), nullValue());
        });

        logger.debug("--> loading snapshot metadata");
        final Repository repository = getInstanceFromNode(RepositoriesService.class).repository("repository");
        assertThat(repository, instanceOf(BlobStoreRepository.class));
        final BlobStoreRepository blobStoreRepository = (BlobStoreRepository) repository;

        final RepositoryData repositoryData = ESBlobStoreRepositoryIntegTestCase.getRepositoryData(repository);
        final SnapshotId snapshotId = createSnapshotResponse.getSnapshotInfo().snapshotId();
        assertThat("Repository blobs tracking disabled", tracker.enabled.compareAndSet(true, false), is(true));

        logger.debug("--> checking prewarmed files");
        for (int index = 0; index < nbIndices; index++) {
            final String indexName = "index-" + index;

            final IndexId indexId = repositoryData.resolveIndexId(indexName);
            assertThat("Index id not found in snapshot for index " + indexName, indexId, notNullValue());

            for (int shard = 0; shard < shardsPerIndex.get(indexName); shard++) {
                logger.debug("--> loading shard snapshot metadata for index [{}][{}][{}]", indexName, indexId, shard);
                final BlobStoreIndexShardSnapshot snapshot = blobStoreRepository.loadShardSnapshot(
                    blobStoreRepository.shardContainer(indexId, shard),
                    snapshotId
                );

                final List<BlobStoreIndexShardSnapshot.FileInfo> expectedPrewarmedBlobs = snapshot.indexFiles()
                    .stream()
                    .filter(file -> file.metadata().hashEqualsContents() == false)
                    .filter(file -> exclusionsPerIndex.get(indexName).contains(IndexFileNames.getExtension(file.physicalName())) == false)
                    .collect(Collectors.toList());

                for (BlobStoreIndexShardSnapshot.FileInfo expectedPrewarmedBlob : expectedPrewarmedBlobs) {
                    for (int part = 0; part < expectedPrewarmedBlob.numberOfParts(); part++) {
                        final String blobName = expectedPrewarmedBlob.partName(part);
                        long actualBytesRead = tracker.totalBytesRead(blobName);
                        long expectedBytesRead = expectedPrewarmedBlob.partBytes(part);
                        assertThat("Blob [" + blobName + "] not fully warmed", actualBytesRead, greaterThanOrEqualTo(expectedBytesRead));
                    }
                }
            }
        }
    }

    @After
    public void resetTracker() {
        getTrackingRepositoryPlugin().clear();
    }

    private TrackingRepositoryPlugin getTrackingRepositoryPlugin() {
        for (RepositoryPlugin plugin : getInstanceFromNode(PluginsService.class).filterPlugins(RepositoryPlugin.class)) {
            if (plugin instanceof TrackingRepositoryPlugin) {
                return ((TrackingRepositoryPlugin) plugin);
            }
        }
        throw new IllegalStateException("tracking repository missing");
    }

    /**
     * A plugin that allows to track the read operations on blobs
     */
    public static class TrackingRepositoryPlugin extends Plugin implements RepositoryPlugin {

        private final ConcurrentHashMap<String, Long> files = new ConcurrentHashMap<>();
        private final AtomicBoolean enabled = new AtomicBoolean(true);

        void clear() {
            files.clear();
            enabled.set(true);
        }

        long totalFilesRead() {
            return files.size();
        }

        long totalBytesRead() {
            return files.values().stream().mapToLong(bytesRead -> bytesRead).sum();
        }

        @Nullable
        Long totalBytesRead(String name) {
            return files.getOrDefault(name, null);
        }

        @Override
        public Map<String, Repository.Factory> getRepositories(
            Environment env,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            BigArrays bigArrays,
            RecoverySettings recoverySettings
        ) {
            return Collections.singletonMap(
                "tracking",
                (metadata) -> new FsRepository(metadata, env, namedXContentRegistry, clusterService, bigArrays, recoverySettings) {

                    @Override
                    protected void assertSnapshotOrGenericThread() {
                        if (enabled.get()) {
                            super.assertSnapshotOrGenericThread();
                        }
                    }

                    @Override
                    protected BlobStore createBlobStore() throws Exception {
                        final BlobStore delegate = super.createBlobStore();
                        if (enabled.get()) {
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
                        return delegate;
                    }
                }
            );
        }

        class TrackingFilesBlobContainer extends FilterBlobContainer {

            TrackingFilesBlobContainer(BlobContainer delegate) {
                super(delegate);
            }

            @Override
            public InputStream readBlob(String blobName, long position, long length) throws IOException {
                return new FilterInputStream(super.readBlob(blobName, position, length)) {
                    long bytesRead = 0L;

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
                        files.merge(blobName, bytesRead, Math::addExact);
                        super.close();
                    }
                };
            }

            @Override
            protected BlobContainer wrapChild(BlobContainer child) {
                return new TrackingFilesBlobContainer(child);
            }
        }
    }
}
