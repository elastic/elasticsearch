/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.lucene;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.LuceneFilesExtensions;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.cache.StatelessSharedBlobCacheService;
import org.elasticsearch.xpack.stateless.cache.reader.CacheBlobReader;
import org.elasticsearch.xpack.stateless.cache.reader.CacheBlobReaderService;
import org.elasticsearch.xpack.stateless.cache.reader.MutableObjectStoreUploadTracker;
import org.elasticsearch.xpack.stateless.commits.BlobFile;
import org.elasticsearch.xpack.stateless.commits.BlobFileRanges;
import org.elasticsearch.xpack.stateless.commits.BlobLocation;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongFunction;

import static org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit.isGenerationalFile;

public class SearchDirectory extends BlobStoreCacheDirectory {
    private static final Logger logger = LogManager.getLogger(SearchDirectory.class);

    /// IndexVersion that introduced storing a `@timestamp` field value range in the compound commit header
    /// (buffered a few versions above the true introduction point to absorb promotion lag). Indices created before
    /// this version can contain compound commits with no recorded range purely because the field did not exist yet,
    /// not because the commit lacks `@timestamp` data.
    public static final IndexVersion TIMESTAMP_FIELD_VALUE_RANGE_INTRODUCED_VERSION = IndexVersions.PATTERN_TEXT_ARGS_IN_BINARY_DOC_VALUES;

    /// Conservative upper bound on the `@timestamp` of any commit written before [#TIMESTAMP_FIELD_VALUE_RANGE_INTRODUCED_VERSION].
    public static final long PRE_TIMESTAMP_FIELD_OLD_TAIL_MILLIS = Instant.parse("2025-12-01T00:00:00Z").toEpochMilli();

    private final CacheBlobReaderService cacheBlobReaderService;
    private final LongAdder totalBytesReadFromIndexing = new LongAdder();
    private final LongAdder totalBytesWarmedFromIndexing = new LongAdder();

    private final AtomicReference<StatelessCompoundCommit> currentCommit = new AtomicReference<>(null);
    private final MutableObjectStoreUploadTracker objectStoreUploadTracker;

    /**
     * Map of terms/generations that are currently in use by opened Lucene generational files.
     */
    private final Map<PrimaryTermAndGeneration, RefCounted> generationalFilesTermAndGens;

    /**
     * Term/generation of the latest updated commit if it contained at least one generational file.
     */
    private volatile Releasable lastAcquiredGenerationalFilesTermAndGen = null;

    /**
     * Tracks the number of obsolete regions eviction requests that have not yet been processed
     */
    private final AtomicLong submittedObsoleteRegionsEvictionTasks = new AtomicLong();

    private final boolean hasTimestampField;
    private final long fallbackRegionTimestampMillis;

    public SearchDirectory(
        StatelessSharedBlobCacheService cacheService,
        CacheBlobReaderService cacheBlobReaderService,
        MutableObjectStoreUploadTracker objectStoreUploadTracker,
        ShardId shardId,
        boolean hasTimestampField,
        IndexVersion creationVersion
    ) {
        super(cacheService, shardId);
        this.cacheBlobReaderService = cacheBlobReaderService;
        this.objectStoreUploadTracker = objectStoreUploadTracker;
        this.generationalFilesTermAndGens = new HashMap<>();
        this.hasTimestampField = hasTimestampField;
        this.fallbackRegionTimestampMillis = resolveFallbackRegionTimestampMillis(hasTimestampField, creationVersion);
    }

    private static long resolveFallbackRegionTimestampMillis(boolean hasTimestampField, IndexVersion creationVersion) {
        if (hasTimestampField == false) {
            return SharedBlobCacheService.UNKNOWN_TIMESTAMP;
        }
        // For a time-based index created after the introduction of timestamp range field, we treat all CCs without timestamp as having
        // a minimal timestamp. This should be a rare case, e.g. a soft-delete only commit.
        // For indices created before that, we could have CCs lacking timestamp range purely because the field did not exist yet. We
        // conservatively assign a timestamp to them
        return creationVersion.before(TIMESTAMP_FIELD_VALUE_RANGE_INTRODUCED_VERSION)
            ? PRE_TIMESTAMP_FIELD_OLD_TAIL_MILLIS
            : SharedBlobCacheService.MINIMAL_CACHE_TIMESTAMP;
    }

    /**
     * Whether BCC metadata reads on this shard should stamp {@link SharedBlobCacheService#BACKFILL_IN_PROGRESS_TIMESTAMP} and be backfilled
     * after parsing. Requires a time-based index and the metadata-read timestamp backfill setting to be enabled.
     */
    public boolean timestampBackfillEnabled() {
        return hasTimestampField && cacheService.isMetadataTimestampBackfillEnabled();
    }

    @Override
    public long fallbackRegionTimestampMillis() {
        return fallbackRegionTimestampMillis;
    }

    /**
     * Backfills the timestamps of every present sentinel region on this shard, using a single cache scan.
     * <p>
     * We support clearing orphaned sentinel regions to handle the following scenario:
     * A region is stamped with the BACKFILL_IN_PROGRESS_TIMESTAMP sentinel when a BCC/CC metadata is read. If that read fails, the region
     * is left in cache with this sentinel timestamp. Such failures close the shard, which normally demotes and unpins those regions so
     * eviction can reclaim them. However, if the shard reopens on the same node before that happens, the sentinel regions are pinned again
     * and linger. Thus, we clear the orphans.
     *
     * @param timestampByCacheKey timestamps for blobs read during this backfill pass, keyed by {@link FileCacheKey}. Values are floored to
     *                            {@link SharedBlobCacheService#MINIMAL_CACHE_TIMESTAMP};
     * @param clearOrphans        when {@code true}, unmatched {@link SharedBlobCacheService#BACKFILL_IN_PROGRESS_TIMESTAMP} regions are
     *                            stamped with {@link SharedBlobCacheService#MINIMAL_CACHE_TIMESTAMP};
     */
    public void backfillMetadataReadTimestamps(Map<FileCacheKey, Long> timestampByCacheKey, boolean clearOrphans) {
        if (clearOrphans == false && timestampByCacheKey.isEmpty()) {
            return;
        }
        final long startTime = System.nanoTime();
        cacheService.backfillRegionTimestamps(shardId, key -> {
            assert key.shardId().equals(shardId) : key.shardId() + " != " + shardId;
            Long timestampMillis = timestampByCacheKey.get(key);
            if (timestampMillis != null) {
                // If we don't know the timestamp, then we say region is not as important.
                // TODO: always come up with timestamp at the caller level, e.g., by getting the next/best available timestamp from
                // neighboring BCCs.
                // Note: that this floored fallback value is not backfilled later on.
                return Math.max(timestampMillis, SharedBlobCacheService.MINIMAL_CACHE_TIMESTAMP);
            }
            return clearOrphans ? SharedBlobCacheService.MINIMAL_CACHE_TIMESTAMP : null;
        });
        if (logger.isDebugEnabled()) {
            logger.debug(
                "{} backfilled [{}] timestamps (clearOrphans=[{}]) in [{}]",
                shardId,
                timestampByCacheKey.size(),
                clearOrphans,
                TimeValue.timeValueNanos(System.nanoTime() - startTime)
            );
        }
    }

    /**
     * Backfills the timestamps of every present sentinel region for each blob in {@code timestampByCacheKey}, using a single cache scan.
     */
    public void backfillMetadataReadTimestamps(Map<FileCacheKey, Long> timestampByCacheKey) {
        backfillMetadataReadTimestamps(timestampByCacheKey, false);
    }

    public void updateLatestUploadedBcc(PrimaryTermAndGeneration latestUploadedBccTermAndGen) {
        objectStoreUploadTracker.updateLatestUploadedBcc(latestUploadedBccTermAndGen);
    }

    public void updateLatestCommitInfo(PrimaryTermAndGeneration ccTermAndGen, String nodeId) {
        objectStoreUploadTracker.updateLatestCommitInfo(ccTermAndGen, nodeId);
    }

    public boolean isBccUploaded(PrimaryTermAndGeneration bccTermAndGen) {
        return objectStoreUploadTracker.getLatestUploadInfo(bccTermAndGen).isUploaded();
    }

    private Releasable acquireGenerationalFileTermAndGeneration(PrimaryTermAndGeneration termAndGen, String name) {
        synchronized (generationalFilesTermAndGens) {
            var refCounted = generationalFilesTermAndGens.get(termAndGen);
            if (refCounted == null || refCounted.tryIncRef() == false) {
                throw new IllegalStateException("Cannot acquire " + termAndGen + " for generational file [" + name + ']');
            }
            assert generationalFilesTermAndGens.isEmpty() == false;
            return refCounted::decRef;
        }
    }

    private Releasable addGenerationalFileTermAndGeneration(PrimaryTermAndGeneration termAndGen) {
        RefCounted refCounted;
        synchronized (generationalFilesTermAndGens) {
            refCounted = generationalFilesTermAndGens.get(termAndGen);
            if (refCounted == null) {
                refCounted = AbstractRefCounted.of(() -> removeGenerationalFileTermAndGeneration(termAndGen));
                generationalFilesTermAndGens.put(termAndGen, refCounted);
            } else {
                // when updating the commit we always acquire the BCC term/gen the commit is part of; if two commits are in the same BCC
                // the second update need to incRef the same BCC term/gen instead of creating a new AbstractRefCounted
                refCounted.incRef();
            }
        }
        return refCounted::decRef;
    }

    private void removeGenerationalFileTermAndGeneration(PrimaryTermAndGeneration termAndGen) {
        synchronized (generationalFilesTermAndGens) {
            var removed = generationalFilesTermAndGens.remove(termAndGen);
            assert removed != null : termAndGen;
            assert removed.hasReferences() == false : termAndGen;
        }
    }

    /**
     * @return the set of {@link PrimaryTermAndGeneration} used by opened Lucene generational files
     */
    public Set<PrimaryTermAndGeneration> getAcquiredGenerationalFileTermAndGenerations() {
        synchronized (generationalFilesTermAndGens) {
            return Set.copyOf(generationalFilesTermAndGens.keySet());
        }
    }

    @Override
    protected IndexInput doOpenInput(String name, IOContext context, BlobFileRanges blobFileRanges) {
        if (isGenerationalFile(name) == false) {
            return super.doOpenInput(name, context, blobFileRanges, cacheService.getBlobCacheMetrics());
        }
        var releasable = acquireGenerationalFileTermAndGeneration(blobFileRanges.getBatchedCompoundCommitTermAndGeneration(), name);
        return doOpenInput(name, context, blobFileRanges, cacheService.getBlobCacheMetrics(), releasable);
    }

    /**
     * Returns the set of file names currently known by this directory. These are files for which
     * {@link BlobFileRanges} have already been resolved and stored in the directory's metadata.
     * Callers can use this to avoid re-resolving referenced commits for files that are already known.
     */
    public Set<String> getKnownFileNames() {
        return currentMetadata.keySet();
    }

    /**
     * Moves the directory to a new commit by setting the newly valid map of files and their metadata.
     * The file metadata does NOT take advantage of the replicated headers and footers in the CC headers.
     *
     * @param newCommit map of file name to store metadata
     * @return true if this update advanced the commit tracked by this directory
     */
    public boolean updateCommit(StatelessCompoundCommit newCommit) {
        return updateCommit(newCommit, Map.of());
    }

    /**
     * Moves the directory to a new commit by setting the newly valid map of files and their metadata.
     * The provided file ranges override the commit's default file ranges for the matching file names.
     */
    public boolean updateCommit(StatelessCompoundCommit newCommit, Map<String, BlobFileRanges> commitFilesRangesOverride) {
        assert blobContainer.get() != null : shardId + " must have the blob container set before any commit update";

        final Map<String, BlobFileRanges> commitFileRanges = createIncomingFileRangesForCommit(newCommit, commitFilesRangesOverride);

        mergeMetadata(commitFileRanges, false);
        // TODO: Commits may not arrive in order. However, the maximum commit we have received is the commit of this directory since the
        // TODO: files always accumulate
        return currentCommit.accumulateAndGet(newCommit, (current, contender) -> {
            if (current == null) {
                return contender;
            } else if (current.generation() > contender.generation()) {
                return current;
            } else {
                return contender;
            }
        }).generation() == newCommit.generation();
    }

    /**
     * Builds a map of file names to {@link BlobFileRanges} for the given commit.
     *
     * If the ranges of a file exist in the {@code commitFilesRangesOverride}, the ranges take precedence over the
     * default ranges derived from the commit; otherwise, default ranges (with timestamp metadata
     * for internal files) are used.
     *
     * @param commit the commit whose files should be mapped to their blob storage locations and byte ranges
     * @param commitFilesRangesOverride custom byte ranges for specific files
     *                                  if provided, these override the commit's default ranges
     * @return a map of file name to {@link BlobFileRanges}, containing blob location and optional
     *         timestamp range for efficient filtering and remote reading
     */
    private static Map<String, BlobFileRanges> createIncomingFileRangesForCommit(
        final StatelessCompoundCommit commit,
        final Map<String, BlobFileRanges> commitFilesRangesOverride
    ) {
        final Map<String, BlobFileRanges> commitFileRanges = new HashMap<>();
        for (final var entry : commit.commitFiles().entrySet()) {
            final String fileName = entry.getKey();
            final BlobLocation blobLocation = entry.getValue();
            final BlobFileRanges override = commitFilesRangesOverride.get(fileName);

            if (override == null) {
                final var ts = commit.internalFiles().contains(fileName) ? commit.getTimestampFieldValueRange() : null;
                commitFileRanges.put(fileName, new BlobFileRanges(blobLocation, ts));
            } else {
                assert override.blobLocation().equals(blobLocation)
                    : "BlobFileRanges override for ["
                        + fileName
                        + "] must use the same blob location as the commit; override="
                        + override.blobLocation()
                        + ", commit="
                        + blobLocation;
                commitFileRanges.put(fileName, override);
            }
        }
        return commitFileRanges;
    }

    /**
     * Removes superfluous files
     * @param filesToRetain the files to retain
     */
    public void retainFiles(Set<String> filesToRetain) {
        if (filesToRetain.containsAll(currentMetadata.keySet()) == false) {
            assert assertCompareAndSetUpdatingCommitThread(null, Thread.currentThread());
            try {
                final var updated = new HashMap<>(currentMetadata);
                final var filesRemoved = updated.keySet().retainAll(filesToRetain);
                assert updated.keySet().containsAll(filesToRetain)
                    : "missing files [" + Sets.difference(filesToRetain, updated.keySet()) + "]";
                currentMetadata = Map.copyOf(updated);
                assert filesRemoved;

                if (filesRemoved && cacheService.isEvictObsoleteRegionsEnabled()) {
                    maybeScheduleObsoleteRegionsEviction();
                }
            } finally {
                assert assertCompareAndSetUpdatingCommitThread(Thread.currentThread(), null);
            }
        }
    }

    /**
     * Schedules an async eviction of cache regions that are no longer referenced by the current metadata,
     * unless one is already running. Concurrent calls while a task is executing are coalesced into a single follow-up.
     */
    private void maybeScheduleObsoleteRegionsEviction() {
        if (submittedObsoleteRegionsEvictionTasks.incrementAndGet() == 1) {
            submitObsoleteRegionsEviction();
        }
    }

    private void submitObsoleteRegionsEviction() {
        cacheService.submitAsyncEviction(() -> {
            final long regionsEvictionTasks = submittedObsoleteRegionsEvictionTasks.get();
            assert regionsEvictionTasks > 0 : regionsEvictionTasks;

            final Map<String, BlobFileRanges> metadata = currentMetadata;

            final Map<Long, BitSet> activeRegionsByBccGen = new HashMap<>();
            long maxBccGeneration = -1L;

            for (var file : metadata.values()) {
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
                final long bccGeneration = StatelessCompoundCommit.parseGenerationFromBlobName(blobName);

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
                submitObsoleteRegionsEviction();
            }
        });
    }

    /**
     * For test usage only.
     */
    @Override
    StatelessSharedBlobCacheService getCacheService() {
        return super.getCacheService();
    }

    /// For test usage only. Returns the number of obsolete-region eviction tasks scheduled by [#retainFiles] that have not yet
    /// completed. Draining this to zero lets a test wait out any in-flight [#submitObsoleteRegionsEviction] instead of racing it,
    /// so a "nothing was evicted" assertion can be made deterministically rather than against a not-yet-run async task.
    long pendingObsoleteRegionsEvictionTasks() {
        return submittedObsoleteRegionsEvictionTasks.get();
    }

    // TODO this method works because we never prune old commits files
    public OptionalLong getPrimaryTerm(String segmentsFileName) throws FileNotFoundException {
        final BlobLocation location = getBlobLocation(segmentsFileName);
        if (location != null) {
            return OptionalLong.of(location.primaryTerm());
        }
        if (segmentsFileName.equals(EmptyDirectory.INSTANCE.getSegmentsFileName())) {
            return OptionalLong.empty();
        }
        var exception = new FileNotFoundException(segmentsFileName);
        assert false : exception;
        throw exception;
    }

    public StatelessCompoundCommit getCurrentCommit() {
        // Only used to initialize the search engine
        return currentCommit.get();
    }

    /**
     * Returns a best-effort view of the {@link BlobFileRanges} for files belonging to the current commit only,
     * excluding files retained solely by open PIT readers or other older readers.
     *
     * <p>This method reads {@code currentCommit} and {@code currentMetadata} without synchronization.
     * Reading the commit first is deliberate: {@link #updateCommit} writes metadata before commit,
     * so the reverse read order guarantees that metadata contains at least all files referenced by
     * the snapshotted commit. A concurrent {@link #retainFiles} call could still remove files
     * belonging to the snapshotted commit from metadata if a newer commit has been processed and no
     * reader holds the old files, causing some entries to be missing from the result. The effect is
     * a transiently smaller cache-size estimation. In practice this is benign: only obsolete files
     * are affected, the estimation is per-shard, and the autoscaler applies a stabilization window
     * of 30 minutes or more before acting on scale-down signals. Callers use this for best-effort
     * cache sizing, not correctness-critical decisions.
     *
     * @return the file ranges for the current commit, or an empty collection if no commit has been received yet
     */
    public Collection<BlobFileRanges> getCurrentCommitBlobFileRanges() {
        final var commit = getCurrentCommit();
        final var metadata = currentMetadata;
        if (commit == null) {
            return List.of();
        }
        final var commitFileNames = commit.commitFiles().keySet();
        final var result = new ArrayList<BlobFileRanges>(commitFileNames.size());
        for (String fileName : commitFileNames) {
            final var blobFileRanges = metadata.get(fileName);
            if (blobFileRanges != null) {
                result.add(blobFileRanges);
            }
        }
        return Collections.unmodifiableList(result);
    }

    @Override
    public CacheBlobReader getCacheBlobReader(String fileName, BlobFile blobFile) {
        return getCacheBlobReader(
            fileName,
            blobFile,
            BlobCacheMetrics.CachePopulationReason.CacheMiss,
            cacheService.getShardReadThreadPoolExecutor(),
            false
        );
    }

    @Override
    public CacheBlobReader getCacheBlobReaderForWarming(BlobFile blobFile) {
        assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.GENERIC);
        return getCacheBlobReader(
            blobFile.blobName(),
            blobFile,
            BlobCacheMetrics.CachePopulationReason.Warming,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            true
        );
    }

    /**
     * Returns a CacheBlobReader for reading a specific file from the blob store
     * for search online prewarming purposes (i.e. as a result of an incoming
     * search request targeting this shard)
     * <p>
     * We allow creating this reader from any thread but the actual downloading of
     * bytes will happen on the stateless_prewarm pool.
     *
     * @param blobFile the blob file
     * @return a CacheBlobReader for reading the specified file
     */
    public CacheBlobReader getCacheBlobReaderForSearchOnlineWarming(BlobFile blobFile) {
        return getCacheBlobReader(
            blobFile.blobName(),
            blobFile,
            BlobCacheMetrics.CachePopulationReason.OnlinePrewarming,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            true
        );
    }

    /**
     * Returns a CacheBlobReader for reading a specific file from the blob store
     * for proactive commit prefetching purposes (i.e. triggered by commit notifications
     * to improve future search performance)
     * <p>
     * We allow creating this reader from any thread but the actual downloading of
     * bytes will happen on the stateless_prewarm pool.
     *
     * @param blobFile blob file
     * @return a CacheBlobReader for reading the specified file
     */
    public CacheBlobReader getCacheBlobReaderForPreFetching(BlobFile blobFile) {
        return getCacheBlobReader(
            blobFile.blobName(),
            blobFile,
            BlobCacheMetrics.CachePopulationReason.PreFetchingNewCommit,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            true
        );
    }

    private CacheBlobReader getCacheBlobReader(
        String fileName,
        BlobFile blobFile,
        BlobCacheMetrics.CachePopulationReason cachePopulationReason,
        Executor executor,
        boolean speculativeFill
    ) {
        return cacheBlobReaderService.getCacheBlobReader(
            shardId,
            this::getBlobContainer,
            blobFile,
            objectStoreUploadTracker,
            totalBytesWarmedFromObjectStore::add,
            totalBytesWarmedFromIndexing::add,
            cachePopulationReason,
            executor,
            fileName,
            speculativeFill
        );
    }

    @Override
    public BlobStoreCacheDirectory createNewBlobStoreCacheDirectoryForWarming() {
        assert false : "SearchDirectory does not support warming directory clones";
        throw new UnsupportedOperationException("SearchDirectory does not support warming directory clones");
    }

    /// Creates a metadata-read directory that stamps regions according to `timestampBackfillEnabled`.
    /// Caller should ensure that `backfillMetadataReadTimestamps` is called after the reads are done if backfill was enabled.
    public BlobStoreCacheDirectory createMetadataReadDirectory(boolean timestampBackfillEnabled) {
        return createNewInstance(blobContainer.get(), timestampBackfillEnabled);
    }

    /// Default implementation with timestamp backfill disabled. For timestamp enabled metadata reads an intermediate directory should be
    /// created with [#createMetadataReadDirectory(boolean)] and then another directory via [#createPerBccMetadataReadDirectory].
    @Override
    public BlobStoreCacheDirectory createPerBccMetadataReadDirectory() {
        return createMetadataReadDirectory(false);
    }

    private BlobStoreCacheDirectory createNewInstance(
        @Nullable LongFunction<BlobContainer> blobContainerFunction,
        boolean timestampBackfillEnabled
    ) {
        return new BlobStoreCacheDirectory(
            cacheService,
            shardId,
            totalBytesReadFromObjectStore,
            totalBytesWarmedFromObjectStore,
            blobContainerFunction
        ) {
            @Override
            protected long fallbackRegionTimestampMillis() {
                return timestampBackfillEnabled
                    ? SharedBlobCacheService.BACKFILL_IN_PROGRESS_TIMESTAMP
                    : SearchDirectory.this.fallbackRegionTimestampMillis();
            }

            @Override
            protected CacheBlobReader getCacheBlobReader(String fileName, BlobFile blobFile) {
                // feeds CacheFileReader (demand reads a warming thread blocks on): accounted as warming, but must bypass
                // the fill-memory budget to avoid queuing behind the speculative region fetches
                return SearchDirectory.this.getCacheBlobReader(
                    fileName,
                    blobFile,
                    BlobCacheMetrics.CachePopulationReason.Warming,
                    getCacheService().getShardReadThreadPoolExecutor(),
                    false
                );
            }

            @Override
            public CacheBlobReader getCacheBlobReaderForWarming(BlobFile blobFile) {
                return SearchDirectory.this.getCacheBlobReader(
                    blobFile.blobName(),
                    blobFile,
                    BlobCacheMetrics.CachePopulationReason.Warming,
                    getCacheService().getShardReadThreadPoolExecutor(),
                    true
                );
            }

            @Override
            public BlobStoreCacheDirectory createNewBlobStoreCacheDirectoryForWarming() {
                assert false : "SearchDirectory does not support warming directory clones";
                throw new UnsupportedOperationException("SearchDirectory does not support warming directory clones");
            }

            /// @return the [BlobStoreCacheDirectory] for a single BCC metadata read through cache that inherits parent's
            /// fallbackRegionTimestampMillis value.
            @Override
            public BlobStoreCacheDirectory createPerBccMetadataReadDirectory() {
                return SearchDirectory.this.createNewInstance(this::getBlobContainer, timestampBackfillEnabled);
            }
        };
    }

    public long totalBytesReadFromIndexing() {
        return totalBytesReadFromIndexing.sum();
    }

    public long totalBytesWarmedFromIndexing() {
        return totalBytesWarmedFromIndexing.sum();
    }

    /**
     * Returns the total number of bytes the search directory has read from both
     * the object store and the indexing tier for cache warming purposes.
     */
    public long totalBytesWarmed() {
        return totalBytesWarmedFromIndexing() + totalBytesWarmedFromObjectStore();
    }

    /**
     * For each blob file referenced by this directory, get the @{@link BlobFileRanges} representing the {@link LuceneFilesExtensions#SI}
     * file with the highest offset
     */
    public Collection<BlobFileRanges> getHighestOffsetSegmentInfos() {
        if (this.currentMetadata.isEmpty()) {
            return Set.of();
        }
        Map<String, BlobFileRanges> highestBlobRanges = new HashMap<>();
        for (Map.Entry<String, BlobFileRanges> entry : this.currentMetadata.entrySet()) {
            if (entry.getKey().endsWith(LuceneFilesExtensions.SI.getExtension()) == false) {
                continue;
            }

            var fileRange = entry.getValue();
            BlobFileRanges existing = highestBlobRanges.putIfAbsent(fileRange.blobName(), fileRange);
            if (existing != null && existing.fileOffset() < fileRange.fileOffset()) {
                highestBlobRanges.put(fileRange.blobName(), fileRange);
            }
        }
        return highestBlobRanges.values();
    }

    public static SearchDirectory unwrapDirectory(final Directory directory) {
        Directory dir = directory;
        while (dir != null) {
            if (dir instanceof SearchDirectory searchDirectory) {
                return searchDirectory;
            } else if (dir instanceof FilterDirectory) {
                dir = ((FilterDirectory) dir).getDelegate();
            } else {
                dir = null;
            }
        }
        var e = new IllegalStateException(directory.getClass() + " cannot be unwrapped as " + SearchDirectory.class);
        assert false : e;
        throw e;
    }

    @Override
    public void close() throws IOException {
        Releasables.close(lastAcquiredGenerationalFilesTermAndGen);
        if (Assertions.ENABLED) {
            synchronized (generationalFilesTermAndGens) {
                assert generationalFilesTermAndGens.isEmpty()
                    : "expect all inputs to be closed at the time the directory is closed but found that shard "
                        + shardId
                        + " has open generational files "
                        + generationalFilesTermAndGens.keySet();
            }
        }
        super.close();
    }

    /**
     * Get the current metadata for the specified files.
     * We e.g. use this during PIT context transfer between nodes in stateless.
     *
     * @param fileNames The names of the files for which to retrieve the metadata.
     */
    public Map<String, BlobFileRanges> getBlobFileRangesForFiles(final Collection<String> fileNames) {
        if (fileNames == null || fileNames.isEmpty()) {
            return Map.of();
        }
        final Map<String, BlobFileRanges> metadata = new HashMap<>(fileNames.size());
        for (String fileName : fileNames) {
            final BlobFileRanges blobFileRanges = currentMetadata.get(fileName);
            if (blobFileRanges != null) {
                metadata.put(fileName, blobFileRanges);
            }
        }
        assert fileNames.size() == metadata.size()
            : "We should find the blob file range for all the requested files but found filenames ["
                + fileNames
                + "] and metadata keys ["
                + metadata.keySet()
                + "]";
        return metadata;
    }

    /// Retrieves the [BlobFileRanges] metadata for a specific file by its name.
    ///
    /// @param fileName the name of the file for which to retrieve the metadata
    /// @return the [BlobFileRanges] associated with the specified file,
    ///         or `null` if no metadata is found for the given file name
    @Nullable
    public BlobFileRanges getBlobFileRangesForFile(String fileName) {
        return currentMetadata.get(fileName);
    }

    /**
     * Merge the incoming metadata into the current metadata.
     * This is used to merge file metadata from other PIT contexts coming from other nodes.
     *
     * @param incomingFileRanges the metadata to merge into the current metadata
     */
    public void mergePITReaderMetadata(final Map<String, BlobFileRanges> incomingFileRanges) {
        mergeMetadata(incomingFileRanges, true);
    }

    private void mergeMetadata(final Map<String, BlobFileRanges> incomingFileRanges, final boolean pitContextRelocationTransfer) {
        assert assertCompareAndSetUpdatingCommitThread(null, Thread.currentThread());

        var previousGenerationalFilesTermAndGen = this.lastAcquiredGenerationalFilesTermAndGen;
        try {
            final var reconciledMetadata = new HashMap<>(currentMetadata);
            PrimaryTermAndGeneration generationalFilesTermAndGen = null;
            long commitSize = 0L;
            for (var entry : incomingFileRanges.entrySet()) {
                final String fileName = entry.getKey();
                final var reconciledRanges = reconcileBlobFileRanges(fileName, reconciledMetadata.get(fileName), entry.getValue());
                if (isGenerationalFile(fileName)) {
                    // blob locations for generational files are not updated: we pin the file to the first blob location that we know about.
                    // we expect generational files to be opened when the reader is refreshed and picks up the generational files for the
                    // first time and never reopened them after that (as segment core readers are handed over between refreshed reader
                    // instances).
                    reconciledMetadata.putIfAbsent(fileName, reconciledRanges);
                    if (generationalFilesTermAndGen == null) {
                        generationalFilesTermAndGen = reconciledRanges.blobLocation().getBatchedCompoundCommitTermAndGeneration();
                    }
                    assert reconciledRanges.blobLocation().getBatchedCompoundCommitTermAndGeneration().equals(generationalFilesTermAndGen)
                        : "All generational files in an incoming commit batch must belong to the same BCC, but "
                            + fileName
                            + " belongs to BCC "
                            + reconciledRanges.blobLocation().getBatchedCompoundCommitTermAndGeneration()
                            + " which differs from "
                            + generationalFilesTermAndGen
                            + " (established by a preceding generational file in this batch)";
                } else {
                    reconciledMetadata.put(fileName, reconciledRanges);
                }
                commitSize += reconciledRanges.blobLocation().fileLength();
            }
            // If we have generational file(s) in the new commit, we create a ref counted instance that holds the term/generation of the
            // batched compound commit so that it can be reported as used to the indexing shard in new commit responses. The ref counted
            // instance will be decRef on the next commit update or when the directory is closed. Any generational file opened between two
            // commits update should incRef the instance to indicate that the BCC term/generation is in use and decRef it once the file is
            // closed. When fully decRefed, the BCC term/gen is removed from the set of used generations.
            if (generationalFilesTermAndGen != null) {
                var releasable = addGenerationalFileTermAndGeneration(generationalFilesTermAndGen);
                // use releaseOnce to decRef only once, either on commit update or directory close
                this.lastAcquiredGenerationalFilesTermAndGen = Releasables.releaseOnce(releasable);
            } else if (pitContextRelocationTransfer) {
                // commit has no generational files, and we're opening a PIT reader during relocation,
                // in that case we don't want to decRef the current generational files term/gen until a
                // new commit notification arrives and mutates it accordingly
                previousGenerationalFilesTermAndGen = null;
            } else {
                // commit has no generational files, and we're not opening a PIT reader during relocation
                this.lastAcquiredGenerationalFilesTermAndGen = null;
            }
            currentMetadata = Map.copyOf(reconciledMetadata);
            if (pitContextRelocationTransfer == false) {
                currentDataSetSizeInBytes = commitSize;
            }
        } finally {
            try {
                Releasables.close(previousGenerationalFilesTermAndGen);
            } finally {
                assert assertCompareAndSetUpdatingCommitThread(Thread.currentThread(), null);
            }
        }
    }

    private static BlobFileRanges reconcileBlobFileRanges(String fileName, BlobFileRanges existingRanges, BlobFileRanges incomingRanges) {
        if (existingRanges == null) {
            return incomingRanges;
        }
        if (existingRanges.blobLocation().equals(incomingRanges.blobLocation()) == false) {
            assert isGenerationalFile(fileName)
                : "A non-generational file ["
                    + fileName
                    + "] has unexpectedly changed blob location from "
                    + existingRanges.blobLocation()
                    + " to "
                    + incomingRanges.blobLocation();
            return incomingRanges;
        }
        return existingRanges.reconcileWith(incomingRanges);
    }
}
