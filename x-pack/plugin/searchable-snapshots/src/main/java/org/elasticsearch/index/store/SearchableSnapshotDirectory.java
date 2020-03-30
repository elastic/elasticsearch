/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.index.store;

import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.SingleInstanceLockFactory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.lucene.store.ByteArrayIndexInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.store.cache.CacheFile;
import org.elasticsearch.index.store.cache.CacheKey;
import org.elasticsearch.index.store.cache.CachedBlobContainerIndexInput;
import org.elasticsearch.index.store.direct.DirectBlobContainerIndexInput;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.xpack.searchablesnapshots.cache.CacheService;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_CACHE_ENABLED_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_CACHE_EXCLUDED_FILE_TYPES_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_INDEX_ID_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_REPOSITORY_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_SNAPSHOT_ID_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_SNAPSHOT_NAME_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_UNCACHED_CHUNK_SIZE_SETTING;

/**
 * Implementation of {@link Directory} that exposes files from a snapshot as a Lucene directory. Because snapshot are immutable this
 * implementation does not allow modification of the directory files and only supports {@link #listAll()}, {@link #fileLength(String)} and
 * {@link #openInput(String, IOContext)} methods.
 *
 * To create a {@link SearchableSnapshotDirectory} both the list of the snapshot files and a {@link BlobContainer} to read these files must
 * be provided. The definition of the snapshot files are provided using a {@link BlobStoreIndexShardSnapshot} object which contains the name
 * of the snapshot and all the files it contains along with their metadata. Because there is no one-to-one relationship between the original
 * shard files and what it stored in the snapshot the {@link BlobStoreIndexShardSnapshot} is used to map a physical file name as expected by
 * Lucene with the one (or the ones) corresponding blob(s) in the snapshot.
 */
public class SearchableSnapshotDirectory extends BaseDirectory {

    private final Supplier<BlobContainer> blobContainer;
    private final Supplier<BlobStoreIndexShardSnapshot> snapshot;
    private final SnapshotId snapshotId;
    private final IndexId indexId;
    private final ShardId shardId;
    private final LongSupplier statsCurrentTimeNanosSupplier;
    private final Map<String, IndexInputStats> stats;
    private final CacheService cacheService;
    private final boolean useCache;
    private final Set<String> excludedFileTypes;
    private final long uncachedChunkSize; // if <0 use BlobContainer#readBlobPreferredLength
    private final Path cacheDir;
    private final AtomicBoolean closed;

    public SearchableSnapshotDirectory(
        Supplier<BlobContainer> blobContainer,
        Supplier<BlobStoreIndexShardSnapshot> snapshot,
        SnapshotId snapshotId,
        IndexId indexId,
        ShardId shardId,
        Settings indexSettings,
        LongSupplier currentTimeNanosSupplier,
        CacheService cacheService,
        Path cacheDir
    ) {
        super(new SingleInstanceLockFactory());
        this.snapshot = Objects.requireNonNull(snapshot);
        this.blobContainer = Objects.requireNonNull(blobContainer);
        this.snapshotId = Objects.requireNonNull(snapshotId);
        this.indexId = Objects.requireNonNull(indexId);
        this.shardId = Objects.requireNonNull(shardId);
        this.stats = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();
        this.statsCurrentTimeNanosSupplier = Objects.requireNonNull(currentTimeNanosSupplier);
        this.cacheService = Objects.requireNonNull(cacheService);
        this.cacheDir = Objects.requireNonNull(cacheDir);
        this.closed = new AtomicBoolean(false);
        this.useCache = SNAPSHOT_CACHE_ENABLED_SETTING.get(indexSettings);
        this.excludedFileTypes = new HashSet<>(SNAPSHOT_CACHE_EXCLUDED_FILE_TYPES_SETTING.get(indexSettings));
        this.uncachedChunkSize = SNAPSHOT_UNCACHED_CHUNK_SIZE_SETTING.get(indexSettings).getBytes();
    }

    public BlobContainer blobContainer() {
        final BlobContainer blobContainer = this.blobContainer.get();
        assert blobContainer != null;
        return blobContainer;
    }

    public BlobStoreIndexShardSnapshot snapshot() {
        final BlobStoreIndexShardSnapshot snapshot = this.snapshot.get();
        assert snapshot != null;
        return snapshot;
    }

    public SnapshotId getSnapshotId() {
        return snapshotId;
    }

    public IndexId getIndexId() {
        return indexId;
    }

    public ShardId getShardId() {
        return shardId;
    }

    public Map<String, IndexInputStats> getStats() {
        return Collections.unmodifiableMap(stats);
    }

    @Nullable
    public IndexInputStats getStats(String fileName) {
        return stats.get(fileName);
    }

    public long statsCurrentTimeNanos() {
        return statsCurrentTimeNanosSupplier.getAsLong();
    }

    private BlobStoreIndexShardSnapshot.FileInfo fileInfo(final String name) throws FileNotFoundException {
        return snapshot().indexFiles()
            .stream()
            .filter(fileInfo -> fileInfo.physicalName().equals(name))
            .findFirst()
            .orElseThrow(() -> new FileNotFoundException(name));
    }

    @Override
    public final String[] listAll() {
        ensureOpen();
        return snapshot().indexFiles()
            .stream()
            .map(BlobStoreIndexShardSnapshot.FileInfo::physicalName)
            .sorted(String::compareTo)
            .toArray(String[]::new);
    }

    @Override
    public final long fileLength(final String name) throws IOException {
        ensureOpen();
        return fileInfo(name).length();
    }

    @Override
    public Set<String> getPendingDeletions() {
        throw unsupportedException();
    }

    @Override
    public void sync(Collection<String> names) {
        throw unsupportedException();
    }

    @Override
    public void syncMetaData() {
        throw unsupportedException();
    }

    @Override
    public void deleteFile(String name) {
        throw unsupportedException();
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) {
        throw unsupportedException();
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) {
        throw unsupportedException();
    }

    @Override
    public void rename(String source, String dest) {
        throw unsupportedException();
    }

    private static UnsupportedOperationException unsupportedException() {
        assert false : "this operation is not supported and should have not be called";
        return new UnsupportedOperationException("Searchable snapshot directory does not support this operation");
    }

    @Override
    public final void close() {
        if (closed.compareAndSet(false, true)) {
            isOpen = false;
            // Ideally we could let the cache evict/remove cached files by itself after the
            // directory has been closed.
            clearCache();
        }
    }

    public void clearCache() {
        cacheService.removeFromCache(cacheKey -> cacheKey.belongsTo(snapshotId, indexId, shardId));
    }

    protected IndexInputStats createIndexInputStats(final long fileLength) {
        return new IndexInputStats(fileLength);
    }

    public CacheKey createCacheKey(String fileName) {
        return new CacheKey(snapshotId, indexId, shardId, fileName);
    }

    public CacheFile getCacheFile(CacheKey cacheKey, long fileLength) throws Exception {
        return cacheService.get(cacheKey, fileLength, cacheDir);
    }

    @Override
    public IndexInput openInput(final String name, final IOContext context) throws IOException {
        ensureOpen();

        final BlobStoreIndexShardSnapshot.FileInfo fileInfo = fileInfo(name);
        if (fileInfo.metadata().hashEqualsContents()) {
            final BytesRef content = fileInfo.metadata().hash();
            return new ByteArrayIndexInput("ByteArrayIndexInput(" + name + ')', content.bytes, content.offset, content.length);
        }

        final IndexInputStats inputStats = stats.computeIfAbsent(name, n -> createIndexInputStats(fileInfo.length()));
        if (useCache && isExcludedFromCache(name) == false) {
            return new CachedBlobContainerIndexInput(this, fileInfo, context, inputStats);
        } else {
            return new DirectBlobContainerIndexInput(
                blobContainer(), fileInfo, context, getUncachedChunkSize(), BufferedIndexInput.BUFFER_SIZE);
        }
    }

    private long getUncachedChunkSize() {
        if (uncachedChunkSize < 0) {
            return blobContainer().readBlobPreferredLength();
        } else {
            return uncachedChunkSize;
        }
    }

    private boolean isExcludedFromCache(String name) {
        final String ext = IndexFileNames.getExtension(name);
        return ext != null && excludedFileTypes.contains(ext);
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "@" + snapshot().snapshot() + " lockFactory=" + lockFactory;
    }

    public static Directory create(RepositoriesService repositories,
                                   CacheService cache,
                                   IndexSettings indexSettings,
                                   ShardPath shardPath,
                                   LongSupplier currentTimeNanosSupplier) throws IOException {

        final Repository repository = repositories.repository(SNAPSHOT_REPOSITORY_SETTING.get(indexSettings.getSettings()));
        if (repository instanceof BlobStoreRepository == false) {
            throw new IllegalArgumentException("Repository [" + repository + "] is not searchable");
        }
        final BlobStoreRepository blobStoreRepository = (BlobStoreRepository) repository;

        final IndexId indexId = new IndexId(indexSettings.getIndex().getName(), SNAPSHOT_INDEX_ID_SETTING.get(indexSettings.getSettings()));
        final SnapshotId snapshotId = new SnapshotId(
            SNAPSHOT_SNAPSHOT_NAME_SETTING.get(indexSettings.getSettings()),
            SNAPSHOT_SNAPSHOT_ID_SETTING.get(indexSettings.getSettings())
        );

        final LazyInitializable<BlobContainer, RuntimeException> lazyBlobContainer
            = new LazyInitializable<>(() -> blobStoreRepository.shardContainer(indexId, shardPath.getShardId().id()));
        final LazyInitializable<BlobStoreIndexShardSnapshot, RuntimeException> lazySnapshot
            = new LazyInitializable<>(() -> blobStoreRepository.loadShardSnapshot(lazyBlobContainer.getOrCompute(), snapshotId));

        final Path cacheDir = shardPath.getDataPath().resolve("snapshots").resolve(snapshotId.getUUID());
        Files.createDirectories(cacheDir);

        return new InMemoryNoOpCommitDirectory(
            new SearchableSnapshotDirectory(
                lazyBlobContainer::getOrCompute,
                lazySnapshot::getOrCompute,
                snapshotId,
                indexId,
                shardPath.getShardId(),
                indexSettings.getSettings(),
                currentTimeNanosSupplier,
                cache,
                cacheDir
            )
        );
    }
}
