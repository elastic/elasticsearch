/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.searchablesnapshots.store;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.SingleInstanceLockFactory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.support.FilterBlobContainer;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.store.ByteArrayIndexInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ThrottledTaskRunner;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots;
import org.elasticsearch.xpack.searchablesnapshots.cache.blob.BlobStoreCacheService;
import org.elasticsearch.xpack.searchablesnapshots.cache.blob.CachedBlob;
import org.elasticsearch.xpack.searchablesnapshots.cache.common.CacheFile;
import org.elasticsearch.xpack.searchablesnapshots.cache.common.CacheKey;
import org.elasticsearch.xpack.searchablesnapshots.cache.full.CacheService;
import org.elasticsearch.xpack.searchablesnapshots.recovery.SearchableSnapshotRecoveryState;
import org.elasticsearch.xpack.searchablesnapshots.store.input.CachedBlobContainerIndexInput;
import org.elasticsearch.xpack.searchablesnapshots.store.input.ChecksumBlobContainerIndexInput;
import org.elasticsearch.xpack.searchablesnapshots.store.input.DirectBlobContainerIndexInput;
import org.elasticsearch.xpack.searchablesnapshots.store.input.FrozenIndexInput;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.core.TimeValue.timeValueNanos;
import static org.elasticsearch.index.IndexModule.INDEX_STORE_TYPE_SETTING;
import static org.elasticsearch.snapshots.SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOT_STORE_TYPE;
import static org.elasticsearch.snapshots.SearchableSnapshotsSettings.SNAPSHOT_PARTIAL_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_BLOB_CACHE_METADATA_FILES_MAX_LENGTH_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_CACHE_ENABLED_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_CACHE_EXCLUDED_FILE_TYPES_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_CACHE_PREWARM_ENABLED_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_INDEX_ID_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_INDEX_NAME_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_REPOSITORY_NAME_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_REPOSITORY_UUID_SETTING;
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

    private static final Logger logger = LogManager.getLogger(SearchableSnapshotDirectory.class);

    private final Supplier<BlobContainer> blobContainerSupplier;
    private final Supplier<BlobStoreIndexShardSnapshot> snapshotSupplier;
    private final BlobStoreCacheService blobStoreCacheService;
    private final String repository;
    private final SnapshotId snapshotId;
    private final IndexId indexId;
    private final ShardId shardId;
    private final LongSupplier statsCurrentTimeNanosSupplier;
    private final Map<String, IndexInputStats> stats;
    private final ThreadPool threadPool;
    private final CacheService cacheService;
    private final boolean useCache;
    private final boolean prewarmCache;
    private final Set<String> excludedFileTypes;
    private final long uncachedChunkSize; // if negative use BlobContainer#readBlobPreferredLength, see #getUncachedChunkSize()
    private final Path cacheDir;
    private final ShardPath shardPath;
    private final AtomicBoolean closed;
    private final boolean partial;
    private final SharedBlobCacheService<CacheKey> sharedBlobCacheService;
    private final ByteSizeValue blobStoreCacheMaxLength;

    // volatile fields are updated once under `this` lock, all together, iff loaded is not true.
    private volatile BlobStoreIndexShardSnapshot snapshot;
    private volatile BlobContainer blobContainer;
    private volatile boolean loaded;
    private volatile SearchableSnapshotRecoveryState recoveryState;

    public SearchableSnapshotDirectory(
        Supplier<BlobContainer> blobContainer,
        Supplier<BlobStoreIndexShardSnapshot> snapshot,
        BlobStoreCacheService blobStoreCacheService,
        String repository,
        SnapshotId snapshotId,
        IndexId indexId,
        ShardId shardId,
        Settings indexSettings,
        LongSupplier currentTimeNanosSupplier,
        CacheService cacheService,
        Path cacheDir,
        ShardPath shardPath,
        ThreadPool threadPool,
        SharedBlobCacheService<CacheKey> sharedBlobCacheService
    ) {
        super(new SingleInstanceLockFactory());
        this.snapshotSupplier = Objects.requireNonNull(snapshot);
        this.blobContainerSupplier = Objects.requireNonNull(blobContainer);
        this.blobStoreCacheService = Objects.requireNonNull(blobStoreCacheService);
        this.repository = Objects.requireNonNull(repository);
        this.snapshotId = Objects.requireNonNull(snapshotId);
        this.indexId = Objects.requireNonNull(indexId);
        this.shardId = Objects.requireNonNull(shardId);
        this.stats = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();
        this.statsCurrentTimeNanosSupplier = Objects.requireNonNull(currentTimeNanosSupplier);
        this.cacheService = Objects.requireNonNull(cacheService);
        this.cacheDir = Objects.requireNonNull(cacheDir);
        this.shardPath = Objects.requireNonNull(shardPath);
        this.closed = new AtomicBoolean(false);
        this.useCache = SNAPSHOT_CACHE_ENABLED_SETTING.get(indexSettings);
        this.partial = SNAPSHOT_PARTIAL_SETTING.get(indexSettings);
        this.prewarmCache = partial == false && useCache ? SNAPSHOT_CACHE_PREWARM_ENABLED_SETTING.get(indexSettings) : false;
        this.excludedFileTypes = new HashSet<>(SNAPSHOT_CACHE_EXCLUDED_FILE_TYPES_SETTING.get(indexSettings));
        this.uncachedChunkSize = SNAPSHOT_UNCACHED_CHUNK_SIZE_SETTING.get(indexSettings).getBytes();
        this.blobStoreCacheMaxLength = SNAPSHOT_BLOB_CACHE_METADATA_FILES_MAX_LENGTH_SETTING.get(indexSettings);
        this.threadPool = threadPool;
        this.loaded = false;
        this.sharedBlobCacheService = sharedBlobCacheService;
        assert invariant();
    }

    private synchronized boolean invariant() {
        assert loaded != (snapshot == null);
        assert loaded != (blobContainer == null);
        assert loaded != (recoveryState == null);
        return true;
    }

    /**
     * Loads the snapshot if and only if the snapshot is not loaded yet.
     *
     * @return true if the snapshot was loaded by executing this method, false otherwise
     */
    public boolean loadSnapshot(
        RecoveryState snapshotRecoveryState,
        Supplier<Boolean> cancelPreWarming,
        ActionListener<Void> preWarmListener
    ) {
        assert snapshotRecoveryState != null;
        assert snapshotRecoveryState instanceof SearchableSnapshotRecoveryState;
        assert snapshotRecoveryState.getRecoverySource().getType() == RecoverySource.Type.SNAPSHOT
            || snapshotRecoveryState.getRecoverySource().getType() == RecoverySource.Type.PEER
            : snapshotRecoveryState.getRecoverySource().getType();
        assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.GENERIC);
        // noinspection ConstantConditions in case assertions are disabled
        if (snapshotRecoveryState instanceof SearchableSnapshotRecoveryState == false) {
            throw new IllegalArgumentException("A SearchableSnapshotRecoveryState instance was expected");
        }
        boolean alreadyLoaded = this.loaded;
        if (alreadyLoaded == false) {
            synchronized (this) {
                alreadyLoaded = this.loaded;
                if (alreadyLoaded == false) {
                    this.blobContainer = blobContainerSupplier.get();
                    this.snapshot = snapshotSupplier.get();
                    this.loaded = true;
                    cleanExistingRegularShardFiles();
                    waitForPendingEvictions();
                    this.recoveryState = (SearchableSnapshotRecoveryState) snapshotRecoveryState;
                    prewarmCache(preWarmListener, cancelPreWarming);
                }
            }
        }
        assert invariant();
        return alreadyLoaded == false;
    }

    @Nullable
    public BlobContainer blobContainer() {
        final BlobContainer blobContainer = this.blobContainer;
        assert blobContainer != null;
        return blobContainer;
    }

    @Nullable
    public BlobStoreIndexShardSnapshot snapshot() {
        final BlobStoreIndexShardSnapshot snapshot = this.snapshot;
        assert snapshot != null;
        return snapshot;
    }

    private List<BlobStoreIndexShardSnapshot.FileInfo> files() {
        if (loaded == false) {
            return List.of();
        }
        final List<BlobStoreIndexShardSnapshot.FileInfo> files = snapshot().indexFiles();
        assert files != null;
        assert files.size() > 0;
        return files;
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

    // only used for tests
    @Nullable
    IndexInputStats getStats(String fileName) {
        return stats.get(getNonNullFileExt(fileName));
    }

    // only used in tests
    public void clearStats() {
        stats.clear();
    }

    private BlobStoreIndexShardSnapshot.FileInfo fileInfo(final String name) throws FileNotFoundException {
        return files().stream()
            .filter(fileInfo -> fileInfo.physicalName().equals(name))
            .findFirst()
            .orElseThrow(() -> new FileNotFoundException(name));
    }

    @Override
    public final String[] listAll() {
        ensureOpen();
        return files().stream().map(BlobStoreIndexShardSnapshot.FileInfo::physicalName).sorted(String::compareTo).toArray(String[]::new);
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
        }
    }

    public void clearCache(boolean clearCacheService, boolean clearFrozenCacheService) {
        for (BlobStoreIndexShardSnapshot.FileInfo file : files()) {
            final CacheKey cacheKey = createCacheKey(file.physicalName());
            if (clearCacheService) {
                cacheService.removeFromCache(cacheKey);
            }
            if (clearFrozenCacheService) {
                sharedBlobCacheService.removeFromCache(cacheKey);
            }
        }
    }

    protected IndexInputStats createIndexInputStats(long numFiles, long totalSize, long minSize, long maxSize) {
        return new IndexInputStats(numFiles, totalSize, minSize, maxSize, statsCurrentTimeNanosSupplier);
    }

    public CacheKey createCacheKey(String fileName) {
        return new CacheKey(snapshotId.getUUID(), indexId.getName(), shardId, fileName);
    }

    public CacheFile getCacheFile(CacheKey cacheKey, long fileLength) throws Exception {
        return cacheService.get(cacheKey, fileLength, cacheDir);
    }

    public Executor cacheFetchAsyncExecutor() {
        return threadPool.executor(SearchableSnapshots.CACHE_FETCH_ASYNC_THREAD_POOL_NAME);
    }

    public Executor prewarmExecutor() {
        return threadPool.executor(SearchableSnapshots.CACHE_PREWARMING_THREAD_POOL_NAME);
    }

    @Override
    public IndexInput openInput(final String name, final IOContext context) throws IOException {
        ensureOpen();

        final BlobStoreIndexShardSnapshot.FileInfo fileInfo = fileInfo(name);
        if (fileInfo.metadata().hashEqualsContents()) {
            final BytesRef content = fileInfo.metadata().hash();
            return new ByteArrayIndexInput("ByteArrayIndexInput(" + name + ')', content.bytes, content.offset, content.length);
        }
        if (context == Store.READONCE_CHECKSUM) {
            return ChecksumBlobContainerIndexInput.create(fileInfo.physicalName(), fileInfo.length(), fileInfo.checksum(), context);
        }

        final String ext = getNonNullFileExt(name);
        final IndexInputStats inputStats = stats.computeIfAbsent(ext, n -> {
            final IndexInputStats.Counter counter = new IndexInputStats.Counter();
            for (BlobStoreIndexShardSnapshot.FileInfo file : files()) {
                if (n.equals(getNonNullFileExt(file.physicalName()))) {
                    counter.add(file.length());
                }
            }
            return createIndexInputStats(counter.count(), counter.total(), counter.min(), counter.max());
        });
        if (useCache && isExcludedFromCache(name) == false) {
            if (partial) {
                return new FrozenIndexInput(
                    name,
                    this,
                    fileInfo,
                    context,
                    inputStats,
                    sharedBlobCacheService.getRangeSize(),
                    sharedBlobCacheService.getRecoveryRangeSize()
                );
            } else {
                return new CachedBlobContainerIndexInput(
                    name,
                    this,
                    fileInfo,
                    context,
                    inputStats,
                    cacheService.getRangeSize(),
                    cacheService.getRecoveryRangeSize()
                );
            }
        } else {
            return new DirectBlobContainerIndexInput(name, blobContainer(), fileInfo, context, inputStats, getUncachedChunkSize());
        }
    }

    static String getNonNullFileExt(String name) {
        final String ext = IndexFileNames.getExtension(name);
        return ext == null ? "" : ext;
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

    public boolean isRecoveryFinalized() {
        SearchableSnapshotRecoveryState recoveryState = this.recoveryState;
        if (recoveryState == null) return false;
        RecoveryState.Stage stage = recoveryState.getStage();
        return stage == RecoveryState.Stage.DONE || stage == RecoveryState.Stage.FINALIZE;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "(snapshotId=" + snapshotId + ", indexId=" + indexId + " shardId=" + shardId + ')';
    }

    private void cleanExistingRegularShardFiles() {
        try {
            IOUtils.rm(shardPath.resolveIndex(), shardPath.resolveTranslog());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Waits for the eviction of cache files associated with the current searchable snapshot shard to be processed in case a
     * previous instance of that same shard has been marked as evicted on this node.
     */
    private void waitForPendingEvictions() {
        assert Thread.holdsLock(this);
        cacheService.waitForCacheFilesEvictionIfNeeded(snapshotId.getUUID(), indexId.getName(), shardId);
    }

    private void prewarmCache(ActionListener<Void> listener, Supplier<Boolean> cancelPreWarming) {
        try (var completionListener = new RefCountingListener(listener.map(v -> {
            recoveryState.setPreWarmComplete();
            return v;
        }))) {
            if (prewarmCache == false || cancelPreWarming.get()) {
                return;
            }
            var prewarmTaskRunner = new ThrottledTaskRunner(
                "prewarm_task_runner" + shardId,
                threadPool.info(SearchableSnapshots.CACHE_PREWARMING_THREAD_POOL_NAME).getMax(),
                prewarmExecutor()
            );

            for (BlobStoreIndexShardSnapshot.FileInfo file : snapshot().indexFiles()) {
                if (cancelPreWarming.get()) {
                    return;
                }
                boolean hashEqualsContents = file.metadata().hashEqualsContents();
                if (hashEqualsContents || isExcludedFromCache(file.physicalName())) {
                    if (hashEqualsContents) {
                        recoveryState.getIndex().addFileDetail(file.physicalName(), file.length(), true);
                    } else {
                        recoveryState.ignoreFile(file.physicalName());
                    }
                    continue;
                }
                recoveryState.getIndex().addFileDetail(file.physicalName(), file.length(), false);
                try {
                    final IndexInput input = openInput(file.physicalName(), CachedBlobContainerIndexInput.CACHE_WARMING_CONTEXT);
                    assert input instanceof CachedBlobContainerIndexInput : "expected cached index input but got " + input.getClass();

                    final AtomicBoolean alreadyCached = new AtomicBoolean();
                    try (var fileListener = new RefCountingListener(ActionListener.runBefore(completionListener.acquire().map(v -> {
                        if (alreadyCached.get()) {
                            recoveryState.markIndexFileAsReused(file.physicalName());
                        } else {
                            recoveryState.getIndex().addRecoveredFromSnapshotBytesToFile(file.physicalName(), file.length());
                        }
                        return v;
                    }), () -> IOUtils.closeWhileHandlingException(input)))) {

                        if (input instanceof CachedBlobContainerIndexInput cachedIndexInput) {
                            if (cachedIndexInput.getPersistentCacheInitialLength() == file.length()) {
                                alreadyCached.set(true);
                                logger.trace(
                                    () -> format(
                                        "%s file [%s] is already available in cache (%d bytes)",
                                        shardId,
                                        file.physicalName(),
                                        file.length()
                                    )
                                );
                                continue;
                            }
                        }

                        for (int p = 0; p < file.numberOfParts(); p++) {
                            final int part = p;
                            prewarmTaskRunner.enqueueTask(fileListener.acquire().map(releasable -> {
                                try (releasable) {
                                    var fileName = file.physicalName();
                                    final long startTimeInNanos = statsCurrentTimeNanosSupplier.getAsLong();
                                    var prefetchedPartBytes = ((CachedBlobContainerIndexInput) input).prefetchPart(part, cancelPreWarming);
                                    if (prefetchedPartBytes > -1L) {
                                        logger.trace(
                                            () -> format(
                                                "%s part [%s/%s] of [%s] warmed in [%s] ms (%d bytes)",
                                                shardId,
                                                part + 1,
                                                file.numberOfParts(),
                                                fileName,
                                                timeValueNanos(statsCurrentTimeNanosSupplier.getAsLong() - startTimeInNanos).millis(),
                                                prefetchedPartBytes
                                            )
                                        );
                                    }
                                    return null;
                                }
                            }));
                        }
                    }
                } catch (Exception e) {
                    logger.warn(() -> format("%s unable to prewarm file [%s]", shardId, file.physicalName()), e);
                }
            }
        }
    }

    public static Directory create(
        RepositoriesService repositories,
        CacheService cache,
        IndexSettings indexSettings,
        ShardPath shardPath,
        LongSupplier currentTimeNanosSupplier,
        ThreadPool threadPool,
        BlobStoreCacheService blobStoreCacheService,
        SharedBlobCacheService<CacheKey> sharedBlobCacheService
    ) throws IOException {

        if (SNAPSHOT_REPOSITORY_NAME_SETTING.exists(indexSettings.getSettings()) == false
            || SNAPSHOT_INDEX_NAME_SETTING.exists(indexSettings.getSettings()) == false
            || SNAPSHOT_INDEX_ID_SETTING.exists(indexSettings.getSettings()) == false
            || SNAPSHOT_SNAPSHOT_NAME_SETTING.exists(indexSettings.getSettings()) == false
            || SNAPSHOT_SNAPSHOT_ID_SETTING.exists(indexSettings.getSettings()) == false) {

            throw new IllegalArgumentException(
                "directly setting ["
                    + INDEX_STORE_TYPE_SETTING.getKey()
                    + "] to ["
                    + SEARCHABLE_SNAPSHOT_STORE_TYPE
                    + "] is not permitted; use the mount snapshot API instead"
            );
        }

        if (indexSettings.hasCustomDataPath()) {
            // cache management requires the shard data path to be in a non-custom location
            throw new IllegalArgumentException(
                "setting ["
                    + IndexMetadata.INDEX_DATA_PATH_SETTING.getKey()
                    + "] is not permitted on searchable snapshots, but was ["
                    + IndexMetadata.INDEX_DATA_PATH_SETTING.get(indexSettings.getSettings())
                    + "]"
            );
        }

        Repository repository;
        final String repositoryName;
        if (SNAPSHOT_REPOSITORY_UUID_SETTING.exists(indexSettings.getSettings())) {
            repository = repositoryByUuid(
                repositories.getRepositories(),
                SNAPSHOT_REPOSITORY_UUID_SETTING.get(indexSettings.getSettings()),
                SNAPSHOT_REPOSITORY_NAME_SETTING.get(indexSettings.getSettings())
            );
            repositoryName = repository.getMetadata().name();
        } else {
            // repository containing pre-7.12 snapshots has no UUID so we assume it matches by name
            repositoryName = SNAPSHOT_REPOSITORY_NAME_SETTING.get(indexSettings.getSettings());
            repository = repositories.repository(repositoryName);
            assert repository.getMetadata().name().equals(repositoryName) : repository.getMetadata().name() + " vs " + repositoryName;
        }

        final BlobStoreRepository blobStoreRepository = SearchableSnapshots.getSearchableRepository(repository);

        final IndexId indexId = new IndexId(
            SNAPSHOT_INDEX_NAME_SETTING.get(indexSettings.getSettings()),
            SNAPSHOT_INDEX_ID_SETTING.get(indexSettings.getSettings())
        );
        final SnapshotId snapshotId = new SnapshotId(
            SNAPSHOT_SNAPSHOT_NAME_SETTING.get(indexSettings.getSettings()),
            SNAPSHOT_SNAPSHOT_ID_SETTING.get(indexSettings.getSettings())
        );

        final LazyInitializable<BlobContainer, RuntimeException> lazyBlobContainer = new LazyInitializable<>(
            () -> new RateLimitingBlobContainer(
                blobStoreRepository,
                blobStoreRepository.shardContainer(indexId, shardPath.getShardId().id())
            )
        );
        final LazyInitializable<BlobStoreIndexShardSnapshot, RuntimeException> lazySnapshot = new LazyInitializable<>(
            () -> blobStoreRepository.loadShardSnapshot(lazyBlobContainer.getOrCompute(), snapshotId)
        );

        final Path cacheDir = CacheService.getShardCachePath(shardPath).resolve(snapshotId.getUUID());
        Files.createDirectories(cacheDir);

        return new InMemoryNoOpCommitDirectory(
            new SearchableSnapshotDirectory(
                lazyBlobContainer::getOrCompute,
                lazySnapshot::getOrCompute,
                blobStoreCacheService,
                repositoryName,
                snapshotId,
                indexId,
                shardPath.getShardId(),
                indexSettings.getSettings(),
                currentTimeNanosSupplier,
                cache,
                cacheDir,
                shardPath,
                threadPool,
                sharedBlobCacheService
            )
        );
    }

    public static SearchableSnapshotDirectory unwrapDirectory(Directory dir) {
        while (dir != null) {
            if (dir instanceof SearchableSnapshotDirectory) {
                return (SearchableSnapshotDirectory) dir;
            } else if (dir instanceof InMemoryNoOpCommitDirectory) {
                dir = ((InMemoryNoOpCommitDirectory) dir).getRealDirectory();
            } else if (dir instanceof FilterDirectory) {
                dir = ((FilterDirectory) dir).getDelegate();
            } else {
                dir = null;
            }
        }
        return null;
    }

    public ByteRange getBlobCacheByteRange(String fileName, long fileLength) {
        return blobStoreCacheService.computeBlobCacheByteRange(shardId, fileName, fileLength, blobStoreCacheMaxLength);
    }

    public CachedBlob getCachedBlob(String name, ByteRange range) {
        return blobStoreCacheService.get(repository, snapshotId, indexId, shardId, name, range);
    }

    public void putCachedBlob(String name, ByteRange range, BytesReference content, ActionListener<Void> listener) {
        blobStoreCacheService.putAsync(
            repository,
            snapshotId,
            indexId,
            shardId,
            name,
            range,
            content,
            threadPool.absoluteTimeInMillis(),
            listener
        );
    }

    public SharedBlobCacheService<CacheKey>.CacheFile getFrozenCacheFile(String fileName, long length) {
        return sharedBlobCacheService.getCacheFile(createCacheKey(fileName), length);
    }

    private static Repository repositoryByUuid(Map<String, Repository> repositories, String repositoryUuid, String originalName) {
        for (Repository repository : repositories.values()) {
            if (repository.getMetadata().uuid().equals(repositoryUuid)) {
                return repository;
            }
        }
        throw new RepositoryMissingException("uuid [" + repositoryUuid + "], original name [" + originalName + "]");
    }

    /**
     * A {@link FilterBlobContainer} that uses {@link BlobStoreRepository#maybeRateLimitRestores(InputStream)} to limit the rate at which
     * blobs are read from the repository.
     */
    private static class RateLimitingBlobContainer extends FilterBlobContainer {

        private final BlobStoreRepository blobStoreRepository;

        RateLimitingBlobContainer(BlobStoreRepository blobStoreRepository, BlobContainer blobContainer) {
            super(blobContainer);
            this.blobStoreRepository = blobStoreRepository;
        }

        @Override
        protected BlobContainer wrapChild(BlobContainer child) {
            return new RateLimitingBlobContainer(blobStoreRepository, child);
        }

        @Override
        public InputStream readBlob(String blobName) throws IOException {
            return blobStoreRepository.maybeRateLimitRestores(super.readBlob(blobName));
        }

        @Override
        public InputStream readBlob(String blobName, long position, long length) throws IOException {
            return blobStoreRepository.maybeRateLimitRestores(super.readBlob(blobName, position, length));
        }
    }
}
