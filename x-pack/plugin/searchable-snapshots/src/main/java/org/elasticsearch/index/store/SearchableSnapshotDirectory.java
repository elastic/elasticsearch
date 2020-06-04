/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.index.store;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
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
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.common.CheckedRunnable;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.support.FilterBlobContainer;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.lucene.store.ByteArrayIndexInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.store.cache.CacheFile;
import org.elasticsearch.index.store.cache.CacheKey;
import org.elasticsearch.index.store.cache.CachedBlobContainerIndexInput;
import org.elasticsearch.index.store.checksum.ChecksumBlobContainerIndexInput;
import org.elasticsearch.index.store.direct.DirectBlobContainerIndexInput;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.searchablesnapshots.cache.CacheService;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static org.apache.lucene.store.BufferedIndexInput.bufferSize;
import static org.elasticsearch.index.IndexModule.INDEX_STORE_TYPE_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_CACHE_ENABLED_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_CACHE_EXCLUDED_FILE_TYPES_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_CACHE_PREWARM_ENABLED_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_INDEX_ID_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_REPOSITORY_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_SNAPSHOT_ID_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_SNAPSHOT_NAME_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_UNCACHED_CHUNK_SIZE_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsConstants.SEARCHABLE_SNAPSHOTS_THREAD_POOL_NAME;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsConstants.SNAPSHOT_DIRECTORY_FACTORY_KEY;

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
    private final AtomicBoolean closed;

    // volatile fields are updated once under `this` lock, all together, iff loaded is not true.
    private volatile BlobStoreIndexShardSnapshot snapshot;
    private volatile BlobContainer blobContainer;
    private volatile boolean loaded;

    public SearchableSnapshotDirectory(
        Supplier<BlobContainer> blobContainer,
        Supplier<BlobStoreIndexShardSnapshot> snapshot,
        SnapshotId snapshotId,
        IndexId indexId,
        ShardId shardId,
        Settings indexSettings,
        LongSupplier currentTimeNanosSupplier,
        CacheService cacheService,
        Path cacheDir,
        ThreadPool threadPool
    ) {
        super(new SingleInstanceLockFactory());
        this.snapshotSupplier = Objects.requireNonNull(snapshot);
        this.blobContainerSupplier = Objects.requireNonNull(blobContainer);
        this.snapshotId = Objects.requireNonNull(snapshotId);
        this.indexId = Objects.requireNonNull(indexId);
        this.shardId = Objects.requireNonNull(shardId);
        this.stats = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();
        this.statsCurrentTimeNanosSupplier = Objects.requireNonNull(currentTimeNanosSupplier);
        this.cacheService = Objects.requireNonNull(cacheService);
        this.cacheDir = Objects.requireNonNull(cacheDir);
        this.closed = new AtomicBoolean(false);
        this.useCache = SNAPSHOT_CACHE_ENABLED_SETTING.get(indexSettings);
        this.prewarmCache = useCache ? SNAPSHOT_CACHE_PREWARM_ENABLED_SETTING.get(indexSettings) : false;
        this.excludedFileTypes = new HashSet<>(SNAPSHOT_CACHE_EXCLUDED_FILE_TYPES_SETTING.get(indexSettings));
        this.uncachedChunkSize = SNAPSHOT_UNCACHED_CHUNK_SIZE_SETTING.get(indexSettings).getBytes();
        this.threadPool = threadPool;
        this.loaded = false;
        assert invariant();
    }

    private synchronized boolean invariant() {
        assert loaded != (snapshot == null);
        assert loaded != (blobContainer == null);
        return true;
    }

    protected final boolean assertCurrentThreadMayLoadSnapshot() {
        final String threadName = Thread.currentThread().getName();
        assert threadName.contains('[' + ThreadPool.Names.GENERIC + ']')
            // Unit tests access the blob store on the main test thread; simplest just to permit this rather than have them override this
            // method somehow.
            || threadName.startsWith("TEST-") : "current thread [" + Thread.currentThread() + "] may not load " + snapshotId;
        return true;
    }

    /**
     * Loads the snapshot if and only if it the snapshot is not loaded yet.
     *
     * @return true if the snapshot was loaded by executing this method, false otherwise
     */
    public boolean loadSnapshot() {
        assert assertCurrentThreadMayLoadSnapshot();
        boolean alreadyLoaded = this.loaded;
        if (alreadyLoaded == false) {
            synchronized (this) {
                alreadyLoaded = this.loaded;
                if (alreadyLoaded == false) {
                    this.blobContainer = blobContainerSupplier.get();
                    this.snapshot = snapshotSupplier.get();
                    this.loaded = true;
                    prewarmCache();
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

    @Nullable
    public IndexInputStats getStats(String fileName) {
        return stats.get(fileName);
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
            // Ideally we could let the cache evict/remove cached files by itself after the
            // directory has been closed.
            clearCache();
        }
    }

    public void clearCache() {
        cacheService.removeFromCache(cacheKey -> cacheKey.belongsTo(snapshotId, indexId, shardId));
    }

    protected IndexInputStats createIndexInputStats(final long fileLength) {
        return new IndexInputStats(fileLength, statsCurrentTimeNanosSupplier);
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
        if (context == Store.READONCE_CHECKSUM) {
            return ChecksumBlobContainerIndexInput.create(fileInfo.physicalName(), fileInfo.length(), fileInfo.checksum(), context);
        }

        final IndexInputStats inputStats = stats.computeIfAbsent(name, n -> createIndexInputStats(fileInfo.length()));
        if (useCache && isExcludedFromCache(name) == false) {
            return new CachedBlobContainerIndexInput(this, fileInfo, context, inputStats, cacheService.getRangeSize());
        } else {
            return new DirectBlobContainerIndexInput(
                blobContainer(),
                fileInfo,
                context,
                inputStats,
                getUncachedChunkSize(),
                bufferSize(context)
            );
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
        return this.getClass().getSimpleName() + "@snapshotId=" + snapshotId + " lockFactory=" + lockFactory;
    }

    private void prewarmCache() {
        if (prewarmCache) {
            final BlockingQueue<Tuple<ActionListener<Void>, CheckedRunnable<Exception>>> queue = new LinkedBlockingQueue<>();
            final Executor executor = threadPool.executor(SEARCHABLE_SNAPSHOTS_THREAD_POOL_NAME);

            for (BlobStoreIndexShardSnapshot.FileInfo file : snapshot().indexFiles()) {
                if (file.metadata().hashEqualsContents() || isExcludedFromCache(file.physicalName())) {
                    continue;
                }
                try {
                    final IndexInput input = openInput(file.physicalName(), CachedBlobContainerIndexInput.CACHE_WARMING_CONTEXT);
                    assert input instanceof CachedBlobContainerIndexInput : "expected cached index input but got " + input.getClass();

                    final int numberOfParts = Math.toIntExact(file.numberOfParts());
                    final GroupedActionListener<Void> listener = new GroupedActionListener<>(
                        ActionListener.wrap(voids -> input.close(), e -> IOUtils.closeWhileHandlingException(input)),
                        numberOfParts
                    );

                    for (int p = 0; p < numberOfParts; p++) {
                        final int part = p;
                        queue.add(Tuple.tuple(listener, () -> {
                            ensureOpen();

                            logger.trace("{} warming cache for [{}] part [{}/{}]", shardId, file.physicalName(), part + 1, numberOfParts);
                            final long startTimeInNanos = statsCurrentTimeNanosSupplier.getAsLong();
                            ((CachedBlobContainerIndexInput) input).prefetchPart(part);

                            logger.trace(
                                () -> new ParameterizedMessage(
                                    "{} part [{}/{}] of [{}] warmed in [{}] ms",
                                    shardId,
                                    part + 1,
                                    numberOfParts,
                                    file.physicalName(),
                                    TimeValue.timeValueNanos(statsCurrentTimeNanosSupplier.getAsLong() - startTimeInNanos).millis()
                                )
                            );
                        }));
                    }
                } catch (IOException e) {
                    logger.warn(() -> new ParameterizedMessage("{} unable to prewarm file [{}]", shardId, file.physicalName()), e);
                }
            }

            logger.debug("{} warming shard cache for [{}] files", shardId, queue.size());

            // Start as many workers as fit into the searchable snapshot pool at once at the most
            final int workers = Math.min(threadPool.info(SEARCHABLE_SNAPSHOTS_THREAD_POOL_NAME).getMax(), queue.size());
            for (int i = 0; i < workers; ++i) {
                prewarmNext(executor, queue);
            }
        }
    }

    private void prewarmNext(final Executor executor, final BlockingQueue<Tuple<ActionListener<Void>, CheckedRunnable<Exception>>> queue) {
        try {
            final Tuple<ActionListener<Void>, CheckedRunnable<Exception>> next = queue.poll(0L, TimeUnit.MILLISECONDS);
            if (next == null) {
                return;
            }
            executor.execute(ActionRunnable.run(ActionListener.runAfter(next.v1(), () -> prewarmNext(executor, queue)), next.v2()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn(() -> new ParameterizedMessage("{} prewarming worker has been interrupted", shardId), e);
        }
    }

    public static Directory create(
        RepositoriesService repositories,
        CacheService cache,
        IndexSettings indexSettings,
        ShardPath shardPath,
        LongSupplier currentTimeNanosSupplier,
        ThreadPool threadPool
    ) throws IOException {

        if (SNAPSHOT_REPOSITORY_SETTING.exists(indexSettings.getSettings()) == false
            || SNAPSHOT_INDEX_ID_SETTING.exists(indexSettings.getSettings()) == false
            || SNAPSHOT_SNAPSHOT_NAME_SETTING.exists(indexSettings.getSettings()) == false
            || SNAPSHOT_SNAPSHOT_ID_SETTING.exists(indexSettings.getSettings()) == false) {

            throw new IllegalArgumentException(
                "directly setting ["
                    + INDEX_STORE_TYPE_SETTING.getKey()
                    + "] to ["
                    + SNAPSHOT_DIRECTORY_FACTORY_KEY
                    + "] is not permitted; use the mount snapshot API instead"
            );
        }

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

        final LazyInitializable<BlobContainer, RuntimeException> lazyBlobContainer = new LazyInitializable<>(
            () -> new RateLimitingBlobContainer(
                blobStoreRepository,
                blobStoreRepository.shardContainer(indexId, shardPath.getShardId().id())
            )
        );
        final LazyInitializable<BlobStoreIndexShardSnapshot, RuntimeException> lazySnapshot = new LazyInitializable<>(
            () -> blobStoreRepository.loadShardSnapshot(lazyBlobContainer.getOrCompute(), snapshotId)
        );

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
                cacheDir,
                threadPool
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
