/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.cache.full;

import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.support.FilterBlobContainer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.store.LuceneFilesExtensions;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesMetrics;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.CACHE_PREWARMING_THREAD_POOL_NAME;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_CACHE_ENABLED_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_CACHE_PREWARM_ENABLED_SETTING;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.emptyCollectionOf;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
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
            .put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial")
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
            createIndex(indexName, indexSettings(nbShards, 0).build());
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
                    bulkRequest.add(prepareIndex(indexName).setSource("foo", randomBoolean() ? "bar" : "baz"));
                }
                final BulkResponse bulkResponse = bulkRequest.get();
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
        assertAcked(clusterAdmin().preparePutRepository("repository").setType(FsRepository.TYPE).setSettings(repositorySettings.build()));

        logger.debug("--> snapshotting indices");
        final CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot("repository", "snapshot")
            .setIncludeGlobalState(false)
            .setIndices("index-*")
            .setWaitForCompletion(true)
            .get();

        final int totalShards = shardsPerIndex.values().stream().mapToInt(i -> i).sum();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(totalShards));
        assertThat(createSnapshotResponse.getSnapshotInfo().failedShards(), equalTo(0));

        ensureGreen("index-*");

        logger.debug("--> deleting indices");
        assertAcked(indicesAdmin().prepareDelete("index-*"));

        logger.debug("--> deleting repository");
        assertAcked(clusterAdmin().prepareDeleteRepository("repository"));

        logger.debug("--> registering tracking repository");
        assertAcked(
            clusterAdmin().preparePutRepository("repository").setType("tracking").setVerify(false).setSettings(repositorySettings.build())
        );

        TrackingRepositoryPlugin tracker = getTrackingRepositoryPlugin();
        assertThat(tracker.totalFilesRead(), equalTo(0L));
        assertThat(tracker.totalBytesRead(), equalTo(0L));

        final Map<String, List<String>> exclusionsPerIndex = new HashMap<>();
        for (int index = 0; index < nbIndices; index++) {
            exclusionsPerIndex.put(
                "index-" + index,
                randomSubsetOf(Stream.of(LuceneFilesExtensions.values()).map(LuceneFilesExtensions::getExtension).toList())
            );
        }

        logger.debug("--> mounting indices");
        final List<String> mountedIndices = new ArrayList<>(nbIndices);
        final Thread[] threads = new Thread[nbIndices];
        final AtomicArray<Throwable> throwables = new AtomicArray<>(nbIndices);
        final CountDownLatch startMounting = new CountDownLatch(1);

        for (int i = 0; i < threads.length; i++) {
            int threadId = i;
            final String indexName = "index-" + threadId;
            mountedIndices.add(indexName);
            final Thread thread = new Thread(() -> {
                try {
                    startMounting.await();

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
                    assertHitCount(client().prepareSearch(indexName).setSize(0).setTrackTotalHits(true), docsPerIndex.get(indexName));

                    final GetSettingsResponse getSettingsResponse = indicesAdmin().prepareGetSettings(indexName).get();
                    assertThat(getSettingsResponse.getSetting(indexName, SNAPSHOT_CACHE_ENABLED_SETTING.getKey()), equalTo("true"));
                    assertThat(getSettingsResponse.getSetting(indexName, SNAPSHOT_CACHE_PREWARM_ENABLED_SETTING.getKey()), equalTo("true"));

                } catch (Throwable t) {
                    logger.error(() -> "Fail to mount snapshot for index [" + indexName + "]", t);
                    throwables.setOnce(threadId, t);
                }
            });
            threads[threadId] = thread;
            thread.start();
        }

        // some indices are randomly removed before prewarming completes
        final Set<String> deletedIndicesDuringPrewarming = randomBoolean() ? Set.copyOf(randomSubsetOf(mountedIndices)) : Set.of();

        final CountDownLatch startPrewarmingLatch = new CountDownLatch(1);
        final var threadPool = getInstanceFromNode(ThreadPool.class);
        final int maxUploadTasks = threadPool.info(CACHE_PREWARMING_THREAD_POOL_NAME).getMax();
        final CountDownLatch maxUploadTasksCreated = new CountDownLatch(maxUploadTasks);
        for (int i = 0; i < maxUploadTasks; i++) {
            threadPool.executor(CACHE_PREWARMING_THREAD_POOL_NAME).execute(new AbstractRunnable() {

                @Override
                protected void doRun() throws Exception {
                    maxUploadTasksCreated.countDown();
                    startPrewarmingLatch.await();
                }

                @Override
                public void onFailure(Exception e) {
                    throw new AssertionError(e);
                }
            });
        }
        safeAwait(maxUploadTasksCreated);
        var prewarmingExecutor = threadPool.executor(CACHE_PREWARMING_THREAD_POOL_NAME);
        assertThat(prewarmingExecutor, instanceOf(ThreadPoolExecutor.class));
        assertThat(((ThreadPoolExecutor) prewarmingExecutor).getActiveCount(), equalTo(maxUploadTasks));

        startMounting.countDown();
        for (Thread thread : threads) {
            thread.join();
        }

        assertThat("Failed to mount snapshot as indices", throwables.asList(), emptyCollectionOf(Throwable.class));

        logger.debug("--> waiting for background cache to complete");
        assertBusy(() -> {
            if (threadPool.executor(SearchableSnapshots.CACHE_FETCH_ASYNC_THREAD_POOL_NAME) instanceof ThreadPoolExecutor executor) {
                assertThat(executor.getQueue().size(), equalTo(0));
                assertThat(executor.getActiveCount(), equalTo(0));
            }
        });

        if (deletedIndicesDuringPrewarming.isEmpty() == false) {
            Set<Index> deletedIndices = deletedIndicesDuringPrewarming.stream().map(this::resolveIndex).collect(Collectors.toSet());
            logger.debug("--> deleting indices [{}] before prewarming", deletedIndices);
            assertAcked(indicesAdmin().prepareDelete(deletedIndicesDuringPrewarming.toArray(String[]::new)));

            var indicesService = getInstanceFromNode(IndicesService.class);
            assertBusy(() -> deletedIndices.forEach(deletedIndex -> assertThat(indicesService.hasIndex(deletedIndex), is(false))));
        }

        startPrewarmingLatch.countDown();

        // wait for recovery to be DONE
        assertBusy(() -> {
            var recoveryResponse = indicesAdmin().prepareRecoveries(mountedIndices.toArray(String[]::new))
                .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN)
                .get();
            assertThat(
                recoveryResponse.shardRecoveryStates()
                    .values()
                    .stream()
                    .flatMap(Collection::stream)
                    .allMatch(recoveryState -> recoveryState.getStage().equals(RecoveryState.Stage.DONE)),
                is(true)
            );
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
                    .toList();

                if (deletedIndicesDuringPrewarming.contains(indexName) == false) {
                    for (BlobStoreIndexShardSnapshot.FileInfo blob : expectedPrewarmedBlobs) {
                        for (int part = 0; part < blob.numberOfParts(); part++) {
                            final String blobName = blob.partName(part);
                            try {
                                assertThat(
                                    "Blob [" + blobName + "][" + blob.physicalName() + "] not prewarmed",
                                    tracker.totalBytesRead(blobName),
                                    equalTo(blob.partBytes(part))
                                );
                            } catch (AssertionError ae) {
                                assertThat(
                                    "Only blobs from physical file with specific extensions are expected to be prewarmed over their sizes ["
                                        + blobName
                                        + "]["
                                        + blob.physicalName()
                                        + "] but got :"
                                        + ae,
                                    blob.physicalName(),
                                    anyOf(endsWith(".cfe"), endsWith(".cfs"))
                                );
                            }
                        }
                    }
                } else {
                    for (BlobStoreIndexShardSnapshot.FileInfo blob : expectedPrewarmedBlobs) {
                        for (int part = 0; part < blob.numberOfParts(); part++) {
                            final String blobName = blob.partName(part);
                            try {
                                assertThat(
                                    "Blob [" + blobName + "][" + blob.physicalName() + "] should not have been fully prewarmed",
                                    tracker.totalBytesRead(blobName),
                                    nullValue()
                                );
                            } catch (AssertionError ae) {
                                assertThat(
                                    "Only blobs from physical file with specific extensions are expected to be prewarmed over their sizes ["
                                        + blobName
                                        + "]["
                                        + blob.physicalName()
                                        + "] but got :"
                                        + ae,
                                    blob.physicalName(),
                                    anyOf(endsWith(".cfe"), endsWith(".cfs"))
                                );
                            }
                        }
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
        return getInstanceFromNode(PluginsService.class).filterPlugins(RepositoryPlugin.class)
            .filter(p -> p instanceof TrackingRepositoryPlugin)
            .map(p -> (TrackingRepositoryPlugin) p)
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("tracking repository missing"));
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
            RecoverySettings recoverySettings,
            RepositoriesMetrics repositoriesMetrics
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
                                public void deleteBlobsIgnoringIfNotExists(OperationPurpose purpose, Iterator<String> blobNames)
                                    throws IOException {
                                    delegate.deleteBlobsIgnoringIfNotExists(purpose, blobNames);
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
            public InputStream readBlob(OperationPurpose purpose, String blobName, long position, long length) throws IOException {
                return new FilterInputStream(super.readBlob(purpose, blobName, position, length)) {
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
