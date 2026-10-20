/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.lucene;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.SingleInstanceLockFactory;
import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.ByteSizeDirectory;
import org.elasticsearch.index.store.ImmutableDirectoryException;
import org.elasticsearch.index.store.PluggableDirectoryMetricsHolder;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.xpack.stateless.cache.StatelessSharedBlobCacheService;
import org.elasticsearch.xpack.stateless.cache.reader.CacheBlobReader;
import org.elasticsearch.xpack.stateless.cache.reader.CacheFileReader;
import org.elasticsearch.xpack.stateless.commits.BatchedCompoundCommit;
import org.elasticsearch.xpack.stateless.commits.BlobFile;
import org.elasticsearch.xpack.stateless.commits.BlobFileRanges;
import org.elasticsearch.xpack.stateless.commits.BlobLocation;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Lucene directory implementation that uses a block based cache for reading files stored in an object store
 */
public abstract class BlobStoreCacheDirectory extends ByteSizeDirectory {

    private static final Logger logger = LogManager.getLogger(BlobStoreCacheDirectory.class);

    protected final LongAdder totalBytesReadFromObjectStore;
    protected final LongAdder totalBytesWarmedFromObjectStore;

    protected final ShardId shardId;
    protected final StatelessSharedBlobCacheService cacheService;
    protected final AtomicReference<LongFunction<BlobContainer>> blobContainer = new AtomicReference<>();

    /**
     * Tracks the number of obsolete regions eviction requests that have not yet been processed
     */
    private final AtomicLong submittedObsoleteRegionsEvictionTasks = new AtomicLong();

    private final AtomicReference<String> corruptionMarker = new AtomicReference<>();
    private final LockFactory lockFactory = new SingleInstanceLockFactory();

    private final AtomicReference<Thread> updatingCommitThread = Assertions.ENABLED ? new AtomicReference<>() : null;// only used in asserts
    private final PluggableDirectoryMetricsHolder<BlobStoreCacheDirectoryMetrics> metricsHolder;

    BlobStoreCacheDirectory(StatelessSharedBlobCacheService cacheService, ShardId shardId) {
        this(cacheService, shardId, new LongAdder(), new LongAdder(), null);
    }

    protected BlobStoreCacheDirectory(
        StatelessSharedBlobCacheService cacheService,
        ShardId shardId,
        LongAdder totalBytesRead,
        LongAdder totalBytesWarmed,
        @Nullable LongFunction<BlobContainer> blobContainerFunction
    ) {
        super(EmptyDirectory.INSTANCE);
        this.cacheService = cacheService;
        this.shardId = shardId;
        this.totalBytesReadFromObjectStore = totalBytesRead;
        this.totalBytesWarmedFromObjectStore = totalBytesWarmed;
        this.metricsHolder = cacheService.metricsHolder();
        if (blobContainerFunction != null) {
            setBlobContainer(blobContainerFunction);
        }
    }

    @Override
    public Lock obtainLock(String name) throws IOException {
        return lockFactory.obtainLock(this, name);
    }

    public final void setBlobContainer(LongFunction<BlobContainer> blobContainer) {
        this.blobContainer.compareAndSet(null, blobContainer); // set once
    }

    /**
     * Returns the file metadata this directory currently publishes.
     * <p>
     * Callers must not mutate the returned map. Implementations either return an immutable map or a map that only
     * the directory itself mutates.
     */
    protected abstract Map<String, BlobFileRanges> getCurrentMetadata();

    public boolean containsFile(String name) {
        final var metadata = getCurrentMetadata();
        if (metadata.isEmpty()) {
            try {
                for (String s : super.listAll()) {
                    if (name.equals(s)) {
                        return true;
                    }
                }
            } catch (IOException e) {
                throw new AssertionError("never throws");
            }
        }
        return metadata.containsKey(name);
    }

    public boolean isMarkedAsCorrupted() {
        return corruptionMarker.get() != null;
    }

    /**
     * Schedules an async eviction of cache regions that are no longer referenced by the current metadata,
     * unless one is already running. Concurrent calls while a task is executing are coalesced into a single follow-up.
     */
    protected void maybeScheduleObsoleteRegionsEviction() {
        maybeScheduleObsoleteRegionsEviction(null);
    }

    /**
     * Same as {@link #maybeScheduleObsoleteRegionsEviction()}, but only the current metadata files that are still live are considered
     * as referenced.
     *
     * @param liveFilesSupplier supplies the files still in use, called when the eviction task executes
     */
    protected void maybeScheduleObsoleteRegionsEviction(@Nullable Supplier<Set<String>> liveFilesSupplier) {
        if (submittedObsoleteRegionsEvictionTasks.incrementAndGet() == 1) {
            submitObsoleteRegionsEviction(liveFilesSupplier);
        }
    }

    private void submitObsoleteRegionsEviction(@Nullable Supplier<Set<String>> liveFilesSupplier) {
        cacheService.submitAsyncEviction(() -> {
            final long regionsEvictionTasks = submittedObsoleteRegionsEvictionTasks.get();
            assert regionsEvictionTasks > 0 : regionsEvictionTasks;

            final Map<String, BlobFileRanges> metadata = getCurrentMetadata();
            final Set<String> liveFiles = liveFilesSupplier != null ? liveFilesSupplier.get() : null;

            final Map<Long, BitSet> activeRegionsByBccGen = new HashMap<>();
            long maxBccGeneration = -1L;

            for (var entry : metadata.entrySet()) {
                if (liveFiles != null && liveFiles.contains(entry.getKey()) == false) {
                    continue;
                }
                final var file = entry.getValue();
                long bccGen = file.getBatchedCompoundCommitTermAndGeneration().generation();
                int startRegion = cacheService.getRegion(file.fileOffset());
                int endRegion = cacheService.getEndingRegion(file.fileOffset() + file.fileLength());
                activeRegionsByBccGen.computeIfAbsent(bccGen, k -> new BitSet()).set(startRegion, endRegion + 1);
                maxBccGeneration = Math.max(maxBccGeneration, bccGen);

                if (file.hasReplicatedRanges()) {
                    file.forEachReplicatedRange(
                        (offset, length) -> activeRegionsByBccGen.computeIfAbsent(bccGen, k -> new BitSet())
                            .set(cacheService.getRegion(offset), cacheService.getEndingRegion(offset + length) + 1)
                    );
                }
            }

            final long maxBccGen = maxBccGeneration;
            cacheService.forceEvict(shardId, (key, region) -> {
                final String blobName = key.fileName();
                final long bccGeneration = BatchedCompoundCommit.parseGenerationFromBlobName(blobName);

                BitSet activeRegions = activeRegionsByBccGen.get(bccGeneration);
                if (activeRegions != null && activeRegions.get(region)) {
                    return false; // Region is active, keep it
                }

                if (bccGeneration < maxBccGen) {
                    logger.debug(
                        "{} evicting obsolete region [{}] of blob [{}] (bcc gen [{}] < max [{}])",
                        shardId,
                        region,
                        blobName,
                        bccGeneration,
                        maxBccGen
                    );
                    return true; // BCC is older and region is not active, evict
                }
                if (bccGeneration == maxBccGen) {
                    assert activeRegions != null : "activeRegions should not be null for maxBccGen";
                    int maxKnownRegion = activeRegions.length() - 1;
                    if (region <= maxKnownRegion) {
                        logger.debug(
                            "{} evicting obsolete region [{}] of blob [{}] (bcc gen [{}], max known region [{}])",
                            shardId,
                            region,
                            blobName,
                            bccGeneration,
                            maxKnownRegion
                        );
                        return true;
                    }
                    return false;
                }
                return false;
            });

            final long remainingTasks = submittedObsoleteRegionsEvictionTasks.addAndGet(-regionsEvictionTasks);
            assert remainingTasks >= 0 : remainingTasks;
            if (remainingTasks > 0) {
                submitObsoleteRegionsEviction(liveFilesSupplier);
            }
        });
    }

    /// For test usage only. Returns the number of obsolete-region eviction tasks scheduled by [#maybeScheduleObsoleteRegionsEviction]
    /// that have not yet completed. Draining this to zero lets a test wait out any in-flight eviction instead of racing it,
    /// so a "nothing was evicted" assertion can be made deterministically rather than against a not-yet-run async task.
    long pendingObsoleteRegionsEvictionTasks() {
        return submittedObsoleteRegionsEvictionTasks.get();
    }

    /**
     * For test usage only.
     */
    @Nullable
    BlobLocation getBlobLocation(String fileName) {
        var blobFileRanges = getCurrentMetadata().get(fileName);
        return blobFileRanges != null ? blobFileRanges.blobLocation() : null;
    }

    public Collection<BlobFileRanges> getBlobFileRanges() {
        return Collections.unmodifiableCollection(getCurrentMetadata().values());
    }

    /**
     * Returns position of the file in the surrounding blob
     */
    public long getPosition(String fileName, long pos, int length) {
        var blobFileRanges = getCurrentMetadata().get(fileName);
        return blobFileRanges != null ? blobFileRanges.getPosition(pos, length) : pos;
    }

    /**
     * Returns the raw data timestamp for a file: the owning compound commit's midpoint, or
     * {@link SharedBlobCacheService#UNKNOWN_TIMESTAMP} when the file is unknown or carries no timestamp range.
     */
    public long getTimestampMillis(String fileName) {
        var blobFileRanges = getCurrentMetadata().get(fileName);
        return blobFileRanges != null
            ? BlobFileRanges.midpointMillisOrUnknownForCache(blobFileRanges.timestampRange())
            : SharedBlobCacheService.UNKNOWN_TIMESTAMP;
    }

    /**
     * Resolves a raw compound-commit timestamp range into a stampable cache-region timestamp.
     */
    public long resolveRegionTimestampMillis(@Nullable StatelessCompoundCommit.TimestampFieldValueRange range) {
        return resolveRegionTimestampMillis(BlobFileRanges.midpointMillisOrUnknownForCache(range));
    }

    public long resolveRegionTimestampMillis(long rawMillis) {
        assert rawMillis >= SharedBlobCacheService.MINIMAL_CACHE_TIMESTAMP || rawMillis == SharedBlobCacheService.UNKNOWN_TIMESTAMP
            : "raw region timestamp must be epoch millis or UNKNOWN, got " + rawMillis;
        return rawMillis != SharedBlobCacheService.UNKNOWN_TIMESTAMP ? rawMillis : fallbackRegionTimestampMillis();
    }

    /**
     * Timestamp to stamp on a cache region whose content carries no known data timestamp.
     */
    protected long fallbackRegionTimestampMillis() {
        return SharedBlobCacheService.UNKNOWN_TIMESTAMP;
    }

    StatelessSharedBlobCacheService getCacheService() {
        return cacheService;
    }

    public long totalBytesReadFromObjectStore() {
        return totalBytesReadFromObjectStore.sum();
    }

    public long totalBytesWarmedFromObjectStore() {
        return totalBytesWarmedFromObjectStore.sum();
    }

    // TODO this method works because we never prune old commits files
    public OptionalLong getPrimaryTerm(String segmentsFileName) throws FileNotFoundException {
        final var blobFileRanges = getCurrentMetadata().get(segmentsFileName);
        if (blobFileRanges != null) {
            return OptionalLong.of(blobFileRanges.primaryTerm());
        }
        if (segmentsFileName.equals(EmptyDirectory.INSTANCE.getSegmentsFileName())) {
            return OptionalLong.empty();
        }
        var exception = new FileNotFoundException(segmentsFileName);
        assert false : exception;
        throw exception;
    }

    public boolean metadataContainsReplicatedRanges() {
        return getCurrentMetadata().values().stream().anyMatch(BlobFileRanges::hasReplicatedRanges);
    }

    protected boolean assertCompareAndSetUpdatingCommitThread(Thread current, Thread updated) {
        final Thread witness = updatingCommitThread.compareAndExchange(current, updated);
        assert witness == current
            : "Unable to set updating commit thread to ["
                + updated
                + "]: expected thread ["
                + current
                + "] to be the updating commit thread, but thread "
                + witness
                + " is already updating the commit of "
                + shardId;
        return true;
    }

    @Override
    public String[] listAll() throws IOException {
        final var current = getCurrentMetadata();
        final String[] list;
        if (current.isEmpty()) {
            list = super.listAll();
        } else {
            list = current.keySet().stream().sorted(String::compareTo).toArray(String[]::new);
        }
        if (isMarkedAsCorrupted()) {
            return Stream.concat(Stream.of(list), Stream.of(corruptionMarker.get())).sorted(String::compareTo).toArray(String[]::new);
        }
        return list;
    }

    @Override
    public void deleteFile(String name) {
        throw unsupportedException();
    }

    @Override
    public long fileLength(String name) throws IOException {
        final var current = getCurrentMetadata();
        if (current.isEmpty()) {
            return super.fileLength(name);
        }
        var blobFileRanges = current.get(name);
        if (blobFileRanges == null) {
            throw new FileNotFoundException(name);
        }
        return blobFileRanges.fileLength();
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) {
        if (name.startsWith(Store.CORRUPTED_MARKER_NAME_PREFIX)) {
            if (corruptionMarker.compareAndSet(null, name)) {
                throw new ImmutableDirectoryException(name);
            }
        }
        throw unsupportedException();
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) {
        throw unsupportedException();
    }

    @Override
    public void sync(Collection<String> names) {
        if (isMarkedAsCorrupted()) {
            return; // allows to sync after the corruption marker has been written
        }
        throw unsupportedException();
    }

    @Override
    public void syncMetaData() {
        throw unsupportedException();
    }

    @Override
    public void rename(String source, String dest) {
        throw unsupportedException();
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        final var current = getCurrentMetadata();
        if (current.isEmpty()) {
            return super.openInput(name, context);
        }
        final var blobFileRanges = current.get(name);
        if (blobFileRanges == null) {
            throw new FileNotFoundException(name);
        }
        return doOpenInput(name, context, blobFileRanges);
    }

    protected IndexInput doOpenInput(String name, IOContext context, BlobFileRanges blobFileRanges) {
        return doOpenInput(name, context, blobFileRanges, cacheService.getBlobCacheMetrics(), null);
    }

    protected IndexInput doOpenInput(String name, IOContext context, BlobFileRanges blobFileRanges, BlobCacheMetrics blobCacheMetrics) {
        return doOpenInput(name, context, blobFileRanges, blobCacheMetrics, null);
    }

    /**
     * Opens an {@link IndexInput} for reading an existing file.
     *
     * @param name the name of an existing file
     * @param context the IO context
     * @param blobFileRanges the {@link BlobFileRanges} associated to the file
     * @param blobCacheMetrics the {@link BlobCacheMetrics} to use for tracking cache metrics
     * @param releasable a {@link Releasable} to be released when the {@link IndexInput} is closed
     * @return an {@link IndexInput}
     */
    protected final IndexInput doOpenInput(
        String name,
        IOContext context,
        BlobFileRanges blobFileRanges,
        BlobCacheMetrics blobCacheMetrics,
        @Nullable Releasable releasable
    ) {
        var blobFile = blobFileRanges.blobLocation().blobFile();
        var reader = new CacheFileReader(
            getCacheFile(blobFileRanges),
            getCacheBlobReader(name, blobFile),
            blobFileRanges,
            blobCacheMetrics,
            cacheService.getThreadPool().relativeTimeInMillisSupplier(),
            cacheService.getRegionSize(),
            context,
            cacheService.hasSearchRole(),
            cacheService.isObjectStorePrefetchEnabled()
        );
        return new BlobCacheIndexInput(name, context, reader, releasable, blobFileRanges.fileLength(), blobFileRanges.fileOffset());
    }

    /**
     * @return the executor on which cache misses of this directory's reads claim their gaps, or {@code null} to claim them on the calling
     * thread
     */
    @Nullable
    protected Executor claimExecutor() {
        return null;
    }

    private SharedBlobCacheService<FileCacheKey>.CacheFile getCacheFile(BlobFileRanges blobFileRanges) {
        long timestampMillis = resolveRegionTimestampMillis(blobFileRanges.timestampRange());
        return cacheService.getCacheFile(
            new FileCacheKey(shardId, blobFileRanges.primaryTerm(), blobFileRanges.blobName()),
            // this length is a lower bound on the length of the blob, used to assert that the cache file does not try to read
            // data beyond the file boundary within the blob since we overload computeCacheFileRegionSize in
            // StatelessSharedBlobCacheService to fully utilize each region
            // it is also used for bounding the reads we do against indexing shard to ensure that we never read beyond the
            // blob length (with padding added).
            blobFileRanges.fileOffset() + blobFileRanges.fileLength(),
            // todo: time-source
            new CacheMissHandler(metricsHolder.singleThreaded(), System::nanoTime),
            timestampMillis,
            claimExecutor()
        );
    }

    /**
     * Returns a CacheBlobReader for reading a specific file from the blob store.
     *
     * @param fileName the name of the file to be read
     * @param blobFile the blob file
     * @return a CacheBlobReader for reading the specified file
     */
    protected abstract CacheBlobReader getCacheBlobReader(String fileName, BlobFile blobFile);

    /**
     * Returns a CacheBlobReader for reading a specific file from the blob store
     * for warming purposes.
     *
     * @param blobFile the blob file
     * @return a CacheBlobReader for reading the specified file
     */
    public abstract CacheBlobReader getCacheBlobReaderForWarming(BlobFile blobFile);

    @Override
    public void close() throws IOException {
        // do not close EmptyDirectory
        if (isMarkedAsCorrupted()) {
            forceEvict();
        }
    }

    @Override
    public Set<String> getPendingDeletions() {
        throw unsupportedException();
    }

    public void forceEvict() {
        final int n = cacheService.forceEvict(fileCacheKey -> shardId.equals(fileCacheKey.shardId()));
        logger.warn("[{}] force evicted [{}] blob cache entries for ShardId {}", this, n, shardId);
    }

    public final BlobContainer getBlobContainer(long primaryTerm) {
        var applier = blobContainer.get();
        assert applier != null : "blob container not set";
        return applier.apply(primaryTerm);
    }

    public ShardId getShardId() {
        return shardId;
    }

    @Override
    public long estimateSizeInBytes() {
        // size is 0 bytes since search directory has no files on disk
        return 0L;
    }

    /**
     * @return the size of the latest commit this directory publishes. This can differ from {@link #estimateSizeInBytes()},
     *         which is zero because these directories store no files locally.
     */
    @Override
    public abstract long estimateDataSetSizeInBytes();

    /**
     * @return the {@link BlobStoreCacheDirectory} to use for pre-warming, warming or reading BCC purpose.
     * Note: the bytes read using this instance are always added to {@link #totalBytesWarmedFromObjectStore}.
     */
    public abstract BlobStoreCacheDirectory createNewBlobStoreCacheDirectoryForWarming();

    /**
     * @param metadata           the file metadata to publish on the returned directory
     * @param dataSetSizeInBytes the data set size to publish on the returned directory
     * @return the {@link BlobStoreCacheDirectory} for a single BCC metadata read through cache, populated with {@code metadata}
     */
    public abstract BlobStoreCacheDirectory createPerBccMetadataReadDirectory(
        Map<String, BlobFileRanges> metadata,
        long dataSetSizeInBytes
    );

    private static UnsupportedOperationException unsupportedException() {
        assert false : "this operation is not supported and should have not be called";
        return new UnsupportedOperationException("stateless directory does not support this operation");
    }

    public static BlobStoreCacheDirectory unwrapDirectory(final Directory directory) {
        Directory dir = directory;
        while (dir != null) {
            if (dir instanceof BlobStoreCacheDirectory blobStoreCacheDirectory) {
                return blobStoreCacheDirectory;
            } else if (dir instanceof IndexDirectory indexDirectory) {
                return indexDirectory.getBlobStoreCacheDirectory();
            } else if (dir instanceof FilterDirectory) {
                dir = ((FilterDirectory) dir).getDelegate();
            } else {
                dir = null;
            }
        }
        var e = new IllegalStateException(directory.getClass() + " cannot be unwrapped as " + BlobStoreCacheDirectory.class);
        assert false : e;
        throw e;
    }

    private static class CacheMissHandler implements SharedBlobCacheService.CacheMissHandler {
        private final PluggableDirectoryMetricsHolder<BlobStoreCacheDirectoryMetrics> blobCacheMetrics;
        private final LongSupplier nanosSource;

        private CacheMissHandler(
            PluggableDirectoryMetricsHolder<BlobStoreCacheDirectoryMetrics> blobCacheMetrics,
            LongSupplier nanosSource
        ) {
            this.blobCacheMetrics = blobCacheMetrics;
            this.nanosSource = nanosSource;
        }

        @Override
        public Releasable record(long bytes) {
            long now = nanosSource.getAsLong();
            return () -> { blobCacheMetrics.instance().add(nanosSource.getAsLong() - now, bytes); };
        }

        @Override
        public SharedBlobCacheService.CacheMissHandler copy() {
            return new CacheMissHandler(blobCacheMetrics.singleThreaded(), nanosSource);
        }
    }
}
