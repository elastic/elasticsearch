/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache.reader;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.IOContext;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyUploadedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.blobcache.BlobCacheMetrics.PrefetchResult;
import org.elasticsearch.blobcache.BlobCacheUtils;
import org.elasticsearch.blobcache.CachePopulationSource;
import org.elasticsearch.blobcache.common.ByteBufferReference;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Streams;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.stateless.StatelessPlugin;
import org.elasticsearch.xpack.stateless.cache.StatelessSharedBlobCacheService;
import org.elasticsearch.xpack.stateless.commits.BlobFileRanges;
import org.elasticsearch.xpack.stateless.lucene.BlobCacheIndexInput;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
import java.util.function.LongSupplier;

import static org.elasticsearch.blobcache.BlobCacheMetrics.CACHE_POPULATION_SOURCE_ATTRIBUTE_KEY;
import static org.elasticsearch.blobcache.shared.SharedBytes.MAX_BYTES_PER_WRITE;
import static org.elasticsearch.threadpool.ThreadPool.Names.SEARCH;
import static org.elasticsearch.xpack.stateless.StatelessPlugin.GET_VIRTUAL_BATCHED_COMPOUND_COMMIT_CHUNK_THREAD_POOL;

/**
 * Used by {@link BlobCacheIndexInput} to read data from the cache using a given {@link StatelessSharedBlobCacheService.CacheFile} instance.
 * When bytes are not cached, the reader uses the provided {@link CacheBlobReader} to fetch data from different sources.
 *
 * <p> Supports applying {@code madvise(MADV_RANDOM)} to cache regions that exclusively contain a data from a single file,
 * e.g. vector data, to disable wasteful kernel read-ahead. For compound files, only interior regions fully within a
 * sub-file's byte range receive {@code MADV_RANDOM}; boundary regions shared with adjacent files fall back to {@code MADV_NORMAL}.
 */
public class CacheFileReader {

    public static final FeatureFlag OBJECT_STORE_PREFETCH_FEATURE_FLAG = new FeatureFlag("stateless_object_store_prefetch");

    static final int MAX_PREFETCH_ALREADY_UPLOADED_RETRIES = 2;

    private static final Logger logger = LogManager.getLogger(CacheFileReader.class);

    private static final Map<String, Object> BLOB_POPULATION_SOURCE_ATTRIBUTES = Map.of(
        CACHE_POPULATION_SOURCE_ATTRIBUTE_KEY,
        CachePopulationSource.BlobStore.name()
    );
    private static final Map<String, Object> PEER_POPULATION_SOURCE_ATTRIBUTES = Map.of(
        CACHE_POPULATION_SOURCE_ATTRIBUTE_KEY,
        CachePopulationSource.Peer.name()
    );

    // On post-6.4 Linux kernels, MADV_RANDOM causes pages to not be marked as accessed,
    // leading to aggressive eviction under MGLRU even without memory pressure.
    // Enabled on snapshot builds for benchmarking; disabled in production.
    // Override with -Des.blob_cache_madvise_random_feature_flag_enabled=true|false.
    static final FeatureFlag MADVISE_RANDOM_FEATURE_FLAG = new FeatureFlag("blob_cache_madvise_random");

    private final StatelessSharedBlobCacheService.CacheFile cacheFile;
    private final CacheBlobReader cacheBlobReader;
    private final BlobFileRanges blobFileRanges;
    private final BlobCacheMetrics blobCacheMetrics;
    private final LongSupplier relativeTimeInMillisSupplier;
    private final int regionSize;

    // The madvise advice this file would like applied (MADV_RANDOM or MADV_NORMAL).
    // Actual advice may differ per read — see adviceForRange().
    private final int desiredMAdvice;

    // The byte range [exclusiveStart, exclusiveEnd) within the blob where cache regions
    // are guaranteed to contain only this file's data. Reads within this range receive
    // desiredAdvice; reads outside it fall back to MADV_NORMAL.
    // For top-level files: [0, Long.MAX_VALUE). For compound sub-files: the interior
    // region-aligned range, excluding boundary regions shared with adjacent files.
    private final long exclusiveStart;
    private final long exclusiveEnd;
    private final boolean hasSearchRole;

    public CacheFileReader(
        StatelessSharedBlobCacheService.CacheFile cacheFile,
        CacheBlobReader cacheBlobReader,
        BlobFileRanges blobFileRanges,
        BlobCacheMetrics blobCacheMetrics,
        LongSupplier relativeTimeInMillisSupplier
    ) {
        this(
            cacheFile,
            cacheBlobReader,
            blobFileRanges,
            blobCacheMetrics,
            relativeTimeInMillisSupplier,
            0,
            SharedBytes.MADV_NORMAL,
            0,
            0,
            false
        );
    }

    /**
     * Creates a reader for a top-level file opened via {@code BlobStoreCacheDirectory.openInput}.
     * Top-level files exclusively own their blob, so all cache regions contain only this file's data.
     * If the IOContext contains {@link DataAccessHint#RANDOM} and the feature flag is enabled,
     * {@code MADV_RANDOM} will be applied to all regions.
     */
    public CacheFileReader(
        StatelessSharedBlobCacheService.CacheFile cacheFile,
        CacheBlobReader cacheBlobReader,
        BlobFileRanges blobFileRanges,
        BlobCacheMetrics blobCacheMetrics,
        LongSupplier relativeTimeInMillisSupplier,
        int regionSize,
        IOContext context,
        boolean hasSearchRole
    ) {
        this(
            cacheFile,
            cacheBlobReader,
            blobFileRanges,
            blobCacheMetrics,
            relativeTimeInMillisSupplier,
            regionSize,
            contextToAdvice(context, hasSearchRole),
            0,
            Long.MAX_VALUE,
            hasSearchRole
        );
    }

    private CacheFileReader(
        StatelessSharedBlobCacheService.CacheFile cacheFile,
        CacheBlobReader cacheBlobReader,
        BlobFileRanges blobFileRanges,
        BlobCacheMetrics blobCacheMetrics,
        LongSupplier relativeTimeInMillisSupplier,
        int regionSize,
        int desiredAdvice,
        long exclusiveStart,
        long exclusiveEnd,
        boolean hasSearchRole
    ) {
        this.cacheFile = Objects.requireNonNull(cacheFile);
        this.cacheBlobReader = Objects.requireNonNull(cacheBlobReader);
        this.blobFileRanges = Objects.requireNonNull(blobFileRanges);
        this.blobCacheMetrics = blobCacheMetrics;
        this.relativeTimeInMillisSupplier = relativeTimeInMillisSupplier;
        this.regionSize = regionSize;
        this.desiredMAdvice = desiredAdvice;
        this.exclusiveStart = exclusiveStart;
        this.exclusiveEnd = exclusiveEnd;
        this.hasSearchRole = hasSearchRole;
    }

    /**
     * @return a new instance that is a copy of the current instance
     */
    public CacheFileReader copy() {
        return new CacheFileReader(
            cacheFile.copy(),
            cacheBlobReader,
            blobFileRanges,
            blobCacheMetrics,
            relativeTimeInMillisSupplier,
            regionSize,
            desiredMAdvice,
            exclusiveStart,
            exclusiveEnd,
            hasSearchRole
        );
    }

    /**
     * Returns a copy of this reader for a sub-file within a compound ({@code .cfs}) blob.
     * Computes the range of cache regions that are exclusively occupied by the sub-file:
     * only those interior regions will receive {@code MADV_RANDOM}. Boundary regions that
     * contain data from adjacent sub-files fall back to {@code MADV_NORMAL}.
     *
     * @param context the IOContext for the sub-file (may contain {@link DataAccessHint#RANDOM})
     * @param subFileOffset the sub-file's absolute byte offset within the blob
     * @param subFileLength the sub-file's length in bytes
     */
    public CacheFileReader copyWithContext(IOContext context, long subFileOffset, long subFileLength) {
        int advice = contextToAdvice(context, hasSearchRole);
        long exclStart;
        long exclEnd;
        if (advice == SharedBytes.MADV_RANDOM && regionSize > 0) {
            exclStart = roundUpToRegion(subFileOffset, regionSize);
            exclEnd = roundDownToRegion(subFileOffset + subFileLength, regionSize);
        } else {
            exclStart = 0;
            exclEnd = 0;
        }
        return new CacheFileReader(
            cacheFile.copy(),
            cacheBlobReader,
            blobFileRanges,
            blobCacheMetrics,
            relativeTimeInMillisSupplier,
            regionSize,
            advice,
            exclStart,
            exclEnd,
            hasSearchRole
        );
    }

    /**
     * Maps Lucene's {@link DataAccessHint} to the corresponding {@code madvise} advice.
     * Returns {@code MADV_RANDOM} only when the node has the search role, the feature flag
     * is enabled, and the context contains {@link DataAccessHint#RANDOM}. On non-search nodes
     * (e.g. during indexing or merge), sequential read-ahead is preserved.
     */
    static int contextToAdvice(IOContext context, boolean hasSearchRole) {
        if (hasSearchRole && MADVISE_RANDOM_FEATURE_FLAG.isEnabled() && context.hints().contains(DataAccessHint.RANDOM)) {
            return SharedBytes.MADV_RANDOM;
        }
        return SharedBytes.MADV_NORMAL;
    }

    /**
     * Returns the effective advice for a read that will populate the given byte range in the blob.
     * Returns {@link SharedBytes#MADV_RANDOM} only if the entire range falls within the exclusive
     * region of this file (i.e. regions not shared with other files in a compound blob).
     */
    int adviceForRange(ByteRange rangeToWrite) {
        if (desiredMAdvice == SharedBytes.MADV_NORMAL) {
            return SharedBytes.MADV_NORMAL;
        }
        if (rangeToWrite.start() >= exclusiveStart && rangeToWrite.end() <= exclusiveEnd) {
            return SharedBytes.MADV_RANDOM;
        }
        return SharedBytes.MADV_NORMAL;
    }

    // visible for testing
    int getDesiredMAdvice() {
        return desiredMAdvice;
    }

    // visible for testing
    long getExclusiveStart() {
        return exclusiveStart;
    }

    // visible for testing
    long getExclusiveEnd() {
        return exclusiveEnd;
    }

    static long roundUpToRegion(long offset, int regionSize) {
        return ((offset + regionSize - 1) / regionSize) * regionSize;
    }

    static long roundDownToRegion(long offset, int regionSize) {
        return (offset / regionSize) * regionSize;
    }

    /**
     * Attempts to prefetch byte(s) from the local cache using the fast path.
     *
     * <p>If the data is not in the cache and {@link #OBJECT_STORE_PREFETCH_FEATURE_FLAG} is enabled,
     * schedules an asynchronous download from the object store so that subsequent reads may find the
     * data already cached. Otherwise this method is best-effort and non-blocking, only succeeding
     * when the data is already present in the local cache.</p>
     *
     * @param offset the starting offset to prefetch from
     * @param length the number of bytes to prefetch
     * @return {@code true} if the fast-path prefetch succeeded,
     *         {@code false} otherwise (async download may have been scheduled)
     * @throws IOException if an I/O error occurs
     */
    public final boolean tryPrefetch(long offset, long length) throws IOException {
        if (OBJECT_STORE_PREFETCH_FEATURE_FLAG.isEnabled() == false) {
            return cacheFile.tryPrefetch(offset, length);
        }
        final long blobLength = cacheFile.getLength();
        // return when there is nothing to prefetch
        if (offset < 0 || offset >= blobLength || length <= 0) {
            return false;
        }
        final long remainingFileLength = blobLength - offset;
        final long clampedLength = Math.min(length, remainingFileLength);
        if (cacheFile.tryPrefetch(offset, clampedLength)) {
            blobCacheMetrics.recordPrefetch(PrefetchResult.AlreadyCached);
            return true;
        }
        final int intLength = clampedLength < Integer.MAX_VALUE ? Math.toIntExact(clampedLength) : Integer.MAX_VALUE;
        // same ranges cannot be passed to populate, as write range may extend beyond actually file length,
        // however read range must stay within file length
        final ByteRange rangeToRead = ByteRange.of(offset, offset + clampedLength);
        populateForPrefetch(offset, intLength, remainingFileLength, rangeToRead, 0, ActionListener.wrap(v -> {
            blobCacheMetrics.recordPrefetch(PrefetchResult.Fetched);
        }, e -> {
            blobCacheMetrics.recordPrefetch(PrefetchResult.Failed);
            logger.debug(() -> "async prefetch failed for [" + cacheFile.getCacheKey() + "]", e);
        }));
        return false;
    }

    /**
     * Populates the cache for an async prefetch, but retries in case of {@link ResourceAlreadyUploadedException}.
     * Such a failure means the batched compound commit was uploaded to the object store while the fetch to the
     * indexing node was in flight
     */
    private void populateForPrefetch(
        long offset,
        int intLength,
        long remainingFileLength,
        ByteRange rangeToRead,
        int attempt,
        ActionListener<Integer> listener
    ) {
        final ByteRange rangeToWrite = cacheBlobReader.getRange(offset, intLength, remainingFileLength);
        cacheFile.populate(rangeToWrite, rangeToRead, (channel, channelPos, relativePos, len) -> {
            channel.prefetch(channelPos, len);
            return len;
        },
            new SequentialRangeMissingHandler(
                "lucene-prefetch",
                cacheFile.getCacheKey().fileName(),
                rangeToWrite,
                cacheBlobReader,
                () -> writeBuffer.get().clear(),
                bytesCopied -> {},
                StatelessPlugin.SHARD_READ_THREAD_POOL,
                StatelessPlugin.FILL_VIRTUAL_BATCHED_COMPOUND_COMMIT_CACHE_THREAD_POOL
            ),
            "lucene-prefetch:" + cacheFile.getCacheKey().fileName(),
            listener.delegateResponse((l, e) -> {
                if (attempt < MAX_PREFETCH_ALREADY_UPLOADED_RETRIES
                    && ExceptionsHelper.unwrap(e, ResourceAlreadyUploadedException.class) != null) {
                    logger.debug(
                        () -> "prefetch for [" + cacheFile.getCacheKey() + "] already uploaded, retrying with attempt " + attempt,
                        e
                    );
                    populateForPrefetch(offset, intLength, remainingFileLength, rangeToRead, attempt + 1, l);
                } else {
                    l.onFailure(e);
                }
            })
        );
    }

    /**
     * Attempts to read byte(s) from the cache using the fast path.
     *
     * @param b the {@link ByteBuffer} to write bytes into
     * @param position the starting position to read from
     * @return true is reading using the fast path succeeded, false otherwise
     * @throws IOException if an I/O error occurs
     */
    public final boolean tryRead(ByteBuffer b, long position) throws IOException {
        if (desiredMAdvice == SharedBytes.MADV_NORMAL) {
            return cacheFile.tryRead(b, position);
        }
        final long regionStart = (position / regionSize) * regionSize;
        final int advice = adviceForRange(ByteRange.of(regionStart, regionStart + regionSize));
        return cacheFile.tryRead(b, position, advice);
    }

    /**
     * If a direct memory segment view is available for the given range, passes it
     * to {@code action} and returns {@code true}. Otherwise returns
     * {@code false} without invoking the action.
     *
     * @param offset the byte offset within the file
     * @param length the number of bytes requested
     * @param action the action to perform with the memory segment
     * @return {@code true} if a segment was available and the action was invoked
     */
    public final boolean withMemorySegmentSlice(long offset, int length, CheckedConsumer<MemorySegment, IOException> action)
        throws IOException {
        if (desiredMAdvice == SharedBytes.MADV_NORMAL) {
            return cacheFile.withMemorySegmentSlice(offset, length, action);
        }
        final long regionStart = (offset / regionSize) * regionSize;
        final int advice = adviceForRange(ByteRange.of(regionStart, regionStart + regionSize));
        return cacheFile.withMemorySegmentSlice(offset, length, action, advice);
    }

    public final boolean withMemorySegmentSlices(
        long[] offsets,
        int length,
        int count,
        CheckedConsumer<MemorySegment[], IOException> action
    ) throws IOException {
        if (desiredMAdvice == SharedBytes.MADV_NORMAL) {
            return cacheFile.withMemorySegmentSlices(offsets, length, count, action);
        }
        // For top-level files the entire range is exclusive, so a single advice applies.
        // For compound sub-files, individual regions could differ, but the bulk path is
        // only used for vector data which is always in a top-level .vec file.
        return cacheFile.withMemorySegmentSlices(offsets, length, count, action, desiredMAdvice);
    }

    /**
     * Reads byte(s) from the cache, potentially fetching the data from a remote source if the data are not present in cache (slow path).
     *
     * @param initiator     the caller (used for debug logging)
     * @param b             the {@link ByteBuffer} to write bytes into
     * @param position      the starting position to read from
     * @param length        the length of bytes to read
     * @param endOfInput    the length of the {@link BlobCacheIndexInput} that triggers the read (used for assertions)
     * @param resourceDescription the underlying {@link BlobCacheIndexInput} resource description
     * @throws Exception    if an error occurs
     */
    public void read(Object initiator, ByteBuffer b, long position, int length, long endOfInput, String resourceDescription)
        throws Exception {
        // the executor can be null if the calling thread is not an {@link org.elasticsearch.common.util.concurrent.EsExecutors.EsThread}
        String executorName = EsExecutors.executorName(Thread.currentThread());

        if (executorName != null
            && (executorName.equals(SEARCH) || executorName.equals(GET_VIRTUAL_BATCHED_COMPOUND_COMMIT_CHUNK_THREAD_POOL))) {
            long start = relativeTimeInMillisSupplier.getAsLong();
            doRead(initiator, b, blobFileRanges.getPosition(position, length), length, endOfInput, resourceDescription);
            blobCacheMetrics.getSearchOriginDownloadTime()
                .record(
                    relativeTimeInMillisSupplier.getAsLong() - start,
                    executorName.equals(SEARCH) ? BLOB_POPULATION_SOURCE_ATTRIBUTES : PEER_POPULATION_SOURCE_ATTRIBUTES
                );
        } else {
            doRead(initiator, b, blobFileRanges.getPosition(position, length), length, endOfInput, resourceDescription);
        }
    }

    private void doRead(Object initiator, ByteBuffer b, long position, int length, long endOfInput, String resourceDescription)
        throws Exception {
        // Semaphore that, when all permits are acquired, ensures that async callbacks (such as those used by readCacheFile) are not
        // accessing the byte buffer anymore that was passed to doReadInternal
        // In particular, it's important to acquire all permits before adapting the ByteBuffer's offset
        final ByteBufferReference byteBufferReference = new ByteBufferReference(b);
        try {
            // Compute the range of bytes of the blob to fetch and to write to the cache.
            //
            // The range represents one or more full regions to fetch. It can also be larger (in both directions) than the file opened by
            // the current BlobCacheIndexInput instance. The range can also be larger than the real length of the blob in the object store.
            // This is OK, we rely on the object store to return as many bytes as possible without failing.

            // we use the length from `cacheFile` since this allows reading beyond the slice'd portion of the file, important for
            // reading beyond individual files inside CFS.
            long remainingFileLength = cacheFile.getLength() - position;
            assert remainingFileLength >= 0 : remainingFileLength;
            assert length <= remainingFileLength : length + " > " + remainingFileLength;
            assert remainingFileLength >= endOfInput - position
                : "cache file length smaller than file length " + cacheFile.getLength() + " < " + endOfInput;
            final ByteRange rangeToWrite = cacheBlobReader.getRange(position, length, remainingFileLength);

            assert rangeToWrite.start() <= position && position + length <= rangeToWrite.end()
                : "[" + position + "-" + (position + length) + "] vs " + rangeToWrite;
            final ByteRange rangeToRead = ByteRange.of(position, position + length);

            int bytesRead = 0;
            try {
                // Can be executed on different thread pool depending on whether we read from
                // the ObjectStoreCacheBlobReader (SHARD_READ pool) or the IndexingShardCacheBlobReader (VBCC pool)
                SharedBlobCacheService.RangeMissingHandler rangeMissingHandler = new SequentialRangeMissingHandler(
                    initiator,
                    cacheFile.getCacheKey().fileName(),
                    rangeToWrite,
                    cacheBlobReader,
                    () -> writeBuffer.get().clear(),
                    bytesCopied -> {},
                    StatelessPlugin.SHARD_READ_THREAD_POOL,
                    StatelessPlugin.FILL_VIRTUAL_BATCHED_COMPOUND_COMMIT_CACHE_THREAD_POOL
                );
                // Determine madvise advice for this read. For compound sub-files, only ranges
                // that fall entirely within the exclusive interior get MADV_RANDOM; boundary
                // ranges that may share a region with adjacent files get MADV_NORMAL.
                final int advice = adviceForRange(rangeToWrite);
                bytesRead = cacheFile.populateAndRead(rangeToWrite, rangeToRead, (channel, channelPos, relativePos, len) -> {
                    // Apply madvise so the kernel uses the correct access pattern for this region.
                    // Covers both already-resident regions (warmed by prefetch/prewarm services)
                    // and freshly-filled regions. The call is idempotent — skipped when unchanged.
                    channel.madvise(advice);
                    logger.trace(
                        "{}: reading cached [{}][{}-{}]",
                        initiator.toString(),
                        cacheFile.getCacheKey().fileName(),
                        rangeToRead.start(),
                        rangeToRead.start() + len
                    );
                    return SharedBytes.readCacheFile(channel, channelPos, relativePos, len, byteBufferReference);
                }, rangeMissingHandler, resourceDescription);
                byteBufferReference.finish(bytesRead);
            } catch (Exception e) {
                if (e instanceof AlreadyClosedException || e.getCause() instanceof AlreadyClosedException) {
                    assert bytesRead == 0 : "expecting bytes read to be 0 but got: " + bytesRead + " for " + cacheFile.getCacheKey();
                    int len = length - bytesRead;
                    // TODO ideally we would make it async, but it should be safe
                    // since the future is created on the shard read thread pool or GET_VIRTUAL_BATCHED_COMPOUND_COMMIT_CHUNK_THREAD_POOL.
                    // ObjectStoreCacheBlobReader is completed on the same thread and before actually waiting on the future, and
                    // IndexingShardCacheBlobReader should be completed on the FILL_VIRTUAL_BATCHED_COMPOUND_COMMIT_CACHE_THREAD_POOL
                    var readFuture = new PlainActionFuture<Integer>();
                    cacheBlobReader.getRangeInputStream(position, len, readFuture.map(in -> {
                        try (in) {
                            final int read = Streams.read(in, b, len);
                            if (read == -1) {
                                BlobCacheUtils.throwEOF(position, len);
                            }
                            return read;
                        }
                    }));
                    bytesRead += cacheFile.recordWait(len, readFuture);
                    blobCacheMetrics.recordBypassRead();
                } else {
                    throw e;
                }
            }
            assert bytesRead == length : bytesRead + " vs " + length;
        } finally {
            byteBufferReference.finish(0);
        }
    }

    // package-private for testing
    StatelessSharedBlobCacheService.CacheFile getCacheFile() {
        return cacheFile;
    }

    @Override
    public String toString() {
        return cacheFile.toString();
    }

    private static final ThreadLocal<ByteBuffer> writeBuffer = ThreadLocal.withInitial(
        () -> ByteBuffer.allocateDirect(MAX_BYTES_PER_WRITE)
    );
}
