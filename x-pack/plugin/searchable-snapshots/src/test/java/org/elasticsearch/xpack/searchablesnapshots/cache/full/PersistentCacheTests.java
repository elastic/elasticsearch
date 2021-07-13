/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.cache.full;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.mockfile.FilterFileChannel;
import org.apache.lucene.mockfile.FilterFileSystemProvider;
import org.apache.lucene.mockfile.FilterPath;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.PathUtilsForTesting;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.xpack.searchablesnapshots.AbstractSearchableSnapshotsTestCase;
import org.elasticsearch.xpack.searchablesnapshots.cache.common.ByteRange;
import org.elasticsearch.xpack.searchablesnapshots.cache.common.CacheFile;
import org.elasticsearch.xpack.searchablesnapshots.cache.common.CacheKey;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.node.NodeRoleSettings.NODE_ROLES_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.cache.common.TestUtils.assertCacheFileEquals;
import static org.elasticsearch.xpack.searchablesnapshots.cache.common.TestUtils.randomPopulateAndReads;
import static org.elasticsearch.xpack.searchablesnapshots.cache.common.TestUtils.sumOfCompletedRangesLengths;
import static org.elasticsearch.xpack.searchablesnapshots.cache.full.PersistentCache.createCacheIndexWriter;
import static org.elasticsearch.xpack.searchablesnapshots.cache.full.PersistentCache.resolveCacheIndexFolder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class PersistentCacheTests extends AbstractSearchableSnapshotsTestCase {

    public void testCacheIndexWriter() throws Exception {
        final NodeEnvironment.NodePath nodePath = nodeEnvironment.nodePath();

        int docId = 0;
        final Map<String, Integer> liveDocs = new HashMap<>();
        final Set<String> deletedDocs = new HashSet<>();

        for (int iter = 0; iter < 20; iter++) {

            final Path snapshotCacheIndexDir = resolveCacheIndexFolder(nodePath);
            assertThat(Files.exists(snapshotCacheIndexDir), equalTo(iter > 0));

            // load existing documents from persistent cache index before each iteration
            final Map<String, Document> documents = PersistentCache.loadDocuments(nodeEnvironment);
            assertThat(documents.size(), equalTo(liveDocs.size()));

            try (PersistentCache.CacheIndexWriter writer = createCacheIndexWriter(nodePath)) {
                assertThat(writer.nodePath(), sameInstance(nodePath));

                // verify that existing documents are loaded
                for (Map.Entry<String, Integer> liveDoc : liveDocs.entrySet()) {
                    final Document document = documents.get(liveDoc.getKey());
                    assertThat("Document should be loaded", document, notNullValue());
                    final String iteration = document.get("update_iteration");
                    assertThat(iteration, equalTo(String.valueOf(liveDoc.getValue())));
                    writer.updateCacheFile(liveDoc.getKey(), document);
                }

                // verify that deleted documents are not loaded
                for (String deletedDoc : deletedDocs) {
                    final Document document = documents.get(deletedDoc);
                    assertThat("Document should not be loaded", document, nullValue());
                }

                // random updates of existing documents
                final Map<String, Integer> updatedDocs = new HashMap<>();
                for (String cacheId : randomSubsetOf(liveDocs.keySet())) {
                    final Document document = new Document();
                    document.add(new StringField("cache_id", cacheId, Field.Store.YES));
                    document.add(new StringField("update_iteration", String.valueOf(iter), Field.Store.YES));
                    writer.updateCacheFile(cacheId, document);

                    updatedDocs.put(cacheId, iter);
                }

                // create new random documents
                final Map<String, Integer> newDocs = new HashMap<>();
                for (int i = 0; i < between(1, 10); i++) {
                    final String cacheId = String.valueOf(docId++);
                    final Document document = new Document();
                    document.add(new StringField("cache_id", cacheId, Field.Store.YES));
                    document.add(new StringField("update_iteration", String.valueOf(iter), Field.Store.YES));
                    writer.updateCacheFile(cacheId, document);

                    newDocs.put(cacheId, iter);
                }

                // deletes random documents
                final Map<String, Integer> removedDocs = new HashMap<>();
                for (String cacheId : randomSubsetOf(Sets.union(liveDocs.keySet(), newDocs.keySet()))) {
                    writer.deleteCacheFile(cacheId);

                    removedDocs.put(cacheId, iter);
                }

                boolean commit = false;
                if (frequently()) {
                    writer.commit();
                    commit = true;
                }

                if (commit) {
                    liveDocs.putAll(updatedDocs);
                    liveDocs.putAll(newDocs);
                    for (String cacheId : removedDocs.keySet()) {
                        liveDocs.remove(cacheId);
                        deletedDocs.add(cacheId);
                    }
                }
            }
        }
    }

    public void testRepopulateCache() throws Exception {
        final CacheService cacheService = defaultCacheService();
        cacheService.setCacheSyncInterval(TimeValue.ZERO);
        cacheService.start();

        final List<CacheFile> cacheFiles = randomCacheFiles(cacheService);
        cacheService.synchronizeCache();

        final List<CacheFile> removedCacheFiles = randomSubsetOf(cacheFiles);
        for (CacheFile removedCacheFile : removedCacheFiles) {
            if (randomBoolean()) {
                // evict cache file from the cache
                cacheService.removeFromCache(removedCacheFile.getCacheKey());
            } else {
                IOUtils.rm(removedCacheFile.getFile());
            }
            cacheFiles.remove(removedCacheFile);
        }
        cacheService.stop();

        final CacheService newCacheService = defaultCacheService();
        newCacheService.start();
        for (CacheFile cacheFile : cacheFiles) {
            CacheFile newCacheFile = newCacheService.get(cacheFile.getCacheKey(), cacheFile.getLength(), cacheFile.getFile().getParent());
            assertThat(newCacheFile, notNullValue());
            assertThat(newCacheFile, not(sameInstance(cacheFile)));
            assertCacheFileEquals(newCacheFile, cacheFile);
        }
        newCacheService.stop();
    }

    public void testCleanUp() throws Exception {
        final List<Path> cacheFiles;
        try (CacheService cacheService = defaultCacheService()) {
            cacheService.start();
            cacheFiles = randomCacheFiles(cacheService).stream().map(CacheFile::getFile).collect(Collectors.toList());
            if (randomBoolean()) {
                cacheService.synchronizeCache();
            }
        }

        final Settings nodeSettings = Settings.builder()
            .put(
                NODE_ROLES_SETTING.getKey(),
                randomValueOtherThanMany(
                    r -> Objects.equals(r, DiscoveryNodeRole.VOTING_ONLY_NODE_ROLE) || r.canContainData(),
                    () -> randomFrom(DiscoveryNodeRole.roles())
                ).roleName()
            )
            .build();

        assertTrue(cacheFiles.stream().allMatch(Files::exists));
        PersistentCache.cleanUp(nodeSettings, nodeEnvironment);
        assertTrue(cacheFiles.stream().noneMatch(Files::exists));
    }

    public void testFSyncDoesNotAddDocumentsBackInPersistentCacheWhenShardIsEvicted() throws Exception {
        IOUtils.close(nodeEnvironment); // this test uses a specific filesystem to block fsync

        final FSyncBlockingFileSystemProvider fileSystem = setupFSyncBlockingFileSystemProvider();
        try {
            nodeEnvironment = newNodeEnvironment(
                Settings.builder()
                    .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath())
                    .put(Environment.PATH_DATA_SETTING.getKey(), createTempDir().toAbsolutePath())
                    .build()
            );

            try (
                PersistentCache persistentCache = new PersistentCache(nodeEnvironment);
                CacheService cacheService = new CacheService(Settings.EMPTY, clusterService, threadPool, persistentCache)
            ) {
                cacheService.setCacheSyncInterval(TimeValue.ZERO);
                cacheService.start();

                logger.debug("creating cache files");
                final List<CacheFile> cacheFiles = randomCacheFiles(cacheService);
                assertThat(cacheService.getCacheFilesEventsQueueSize(), equalTo((long) cacheFiles.size()));
                assertThat(cacheService.getNumberOfCacheFilesEvents(), equalTo((long) cacheFiles.size()));

                final CacheFile randomCacheFile = randomFrom(cacheFiles);
                final CacheKey cacheKey = randomCacheFile.getCacheKey();

                final SnapshotId snapshotId = new SnapshotId("_ignored_", cacheKey.getSnapshotUUID());
                if (randomBoolean()) {
                    final ByteRange absent = randomCacheFile.getAbsentRangeWithin(ByteRange.of(0L, randomCacheFile.getLength()));
                    if (absent != null) {
                        assertThat(
                            "Persistent cache should not contain any cached data",
                            persistentCache.getCacheSize(cacheKey.getShardId(), snapshotId),
                            equalTo(0L)
                        );

                        cacheService.synchronizeCache();

                        assertThat(cacheService.getCacheFilesEventsQueueSize(), equalTo(0L));
                        assertThat(cacheService.getNumberOfCacheFilesEvents(), equalTo(0L));

                        final long sizeInCache = persistentCache.getCacheSize(cacheKey.getShardId(), snapshotId);
                        final long sizeInCacheFile = sumOfCompletedRangesLengths(randomCacheFile);
                        assertThat(
                            "Persistent cache should contain cached data for at least 1 cache file",
                            sizeInCache,
                            greaterThanOrEqualTo(sizeInCacheFile)
                        );

                        final CacheFile.EvictionListener listener = evictedCacheFile -> {};
                        randomCacheFile.acquire(listener);
                        try {
                            SortedSet<ByteRange> ranges = null;
                            while (ranges == null || ranges.isEmpty()) {
                                ranges = randomPopulateAndReads(randomCacheFile);
                            }
                            assertTrue(cacheService.isCacheFileToSync(randomCacheFile));
                            assertThat(cacheService.getCacheFilesEventsQueueSize(), equalTo(1L));
                            assertThat(cacheService.getNumberOfCacheFilesEvents(), equalTo(1L));
                        } finally {
                            randomCacheFile.release(listener);
                        }

                        assertThat(
                            "Persistent cache should contain cached data for at least 1 cache file",
                            persistentCache.getCacheSize(cacheKey.getShardId(), snapshotId),
                            equalTo(sizeInCache)
                        );
                    }
                }

                final boolean fsyncFailure = randomBoolean();
                logger.debug("blocking fsync for cache file [{}] with failure [{}]", randomCacheFile.getFile(), fsyncFailure);
                fileSystem.blockFSyncForPath(randomCacheFile.getFile(), fsyncFailure);

                logger.debug("starting synchronization of cache files");
                final Thread fsyncThread = new Thread(cacheService::synchronizeCache);
                fsyncThread.start();

                logger.debug("waiting for synchronization of cache files to be blocked");
                fileSystem.waitForBlock();

                logger.debug("starting eviction of shard [{}]", cacheKey);
                cacheService.markShardAsEvictedInCache(cacheKey.getSnapshotUUID(), cacheKey.getSnapshotIndexName(), cacheKey.getShardId());

                logger.debug("waiting for shard eviction to be processed");
                cacheService.waitForCacheFilesEvictionIfNeeded(
                    cacheKey.getSnapshotUUID(),
                    cacheKey.getSnapshotIndexName(),
                    cacheKey.getShardId()
                );

                logger.debug("unblocking synchronization of cache files");
                fileSystem.unblock();
                fsyncThread.join();

                assertThat(
                    "Persistent cache should not report any cached data for the evicted shard",
                    persistentCache.getCacheSize(cacheKey.getShardId(), new SnapshotId("_ignored_", cacheKey.getSnapshotUUID())),
                    equalTo(0L)
                );

                logger.debug("triggering one more cache synchronization in case all cache files deletions were not processed before");
                cacheService.synchronizeCache();

                assertThat(
                    "Persistent cache should not report any cached data for the evicted shard (ignoring deleted files)",
                    persistentCache.getCacheSize(
                        cacheKey.getShardId(),
                        new SnapshotId("_ignored_", cacheKey.getSnapshotUUID()),
                        path -> true
                    ),
                    equalTo(0L)
                );

                assertThat(cacheService.getCacheFilesEventsQueueSize(), equalTo(0L));
                assertThat(cacheService.getNumberOfCacheFilesEvents(), equalTo(0L));
            }
        } finally {
            fileSystem.tearDown();
        }
    }

    public void testGetCacheSizeIgnoresDeletedCacheFiles() throws Exception {
        try (CacheService cacheService = defaultCacheService()) {
            cacheService.setCacheSyncInterval(TimeValue.ZERO);
            cacheService.start();

            final CacheFile cacheFile = randomFrom(randomCacheFiles(cacheService));
            final long sizeOfCacheFileInCache = sumOfCompletedRangesLengths(cacheFile);

            final Supplier<Long> cacheSizeSupplier = () -> cacheService.getPersistentCache()
                .getCacheSize(cacheFile.getCacheKey().getShardId(), new SnapshotId("_ignored_", cacheFile.getCacheKey().getSnapshotUUID()));
            assertThat(cacheSizeSupplier.get(), equalTo(0L));

            cacheService.synchronizeCache();

            final long sizeOfShardInCache = cacheSizeSupplier.get();
            assertThat(sizeOfShardInCache, greaterThanOrEqualTo(sizeOfCacheFileInCache));

            final CacheFile.EvictionListener listener = evictedCacheFile -> {};
            cacheFile.acquire(listener);
            try {
                cacheService.removeFromCache(cacheFile.getCacheKey());
                assertThat(cacheSizeSupplier.get(), equalTo(sizeOfShardInCache));
            } finally {
                cacheFile.release(listener);
            }
            assertThat(cacheSizeSupplier.get(), equalTo(sizeOfShardInCache - sizeOfCacheFileInCache));
        }
    }

    private static FSyncBlockingFileSystemProvider setupFSyncBlockingFileSystemProvider() {
        final FileSystem defaultFileSystem = PathUtils.getDefaultFileSystem();
        final FSyncBlockingFileSystemProvider provider = new FSyncBlockingFileSystemProvider(defaultFileSystem, createTempDir());
        PathUtilsForTesting.installMock(provider.getFileSystem(null));
        return provider;
    }

    /**
     * {@link FilterFileSystemProvider} that can block fsync for a specified {@link Path}.
     */
    public static class FSyncBlockingFileSystemProvider extends FilterFileSystemProvider {

        private final AtomicReference<Path> pathToBlock = new AtomicReference<>();
        private final AtomicBoolean failFSync = new AtomicBoolean();
        private final CountDownLatch blockingLatch = new CountDownLatch(1);
        private final CountDownLatch releasingLatch = new CountDownLatch(1);

        private final FileSystem delegateInstance;
        private final Path rootDir;

        public FSyncBlockingFileSystemProvider(FileSystem delegate, Path rootDir) {
            super("fsyncblocking://", delegate);
            this.rootDir = new FilterPath(rootDir, this.fileSystem);
            this.delegateInstance = delegate;
        }

        public Path resolve(String other) {
            return rootDir.resolve(other);
        }

        @Override
        public FileChannel newFileChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
            return new FilterFileChannel(super.newFileChannel(path, options, attrs)) {

                @Override
                public void force(boolean metaData) throws IOException {
                    final Path blockedPath = pathToBlock.get();
                    if (blockedPath == null || blockedPath.equals(path.toAbsolutePath()) == false) {
                        super.force(metaData);
                        return;
                    }
                    try {
                        blockingLatch.countDown();
                        releasingLatch.await();
                        if (failFSync.get()) {
                            throw new IOException("Simulated");
                        } else {
                            super.force(metaData);
                        }
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }
                }
            };
        }

        public void blockFSyncForPath(Path path, boolean failure) {
            pathToBlock.set(path.toAbsolutePath());
            failFSync.set(failure);
        }

        public void waitForBlock() {
            try {
                blockingLatch.await();
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
        }

        public void unblock() {
            releasingLatch.countDown();
        }

        public void tearDown() {
            PathUtilsForTesting.installMock(delegateInstance);
        }
    }
}
