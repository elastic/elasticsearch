/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.cache;

import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.commits.BlobFile;
import co.elastic.elasticsearch.stateless.commits.BlobLocation;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;
import co.elastic.elasticsearch.stateless.lucene.FileCacheKey;
import co.elastic.elasticsearch.stateless.utils.IndexingShardRecoveryComparator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.util.concurrent.ThrottledTaskRunner;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.LuceneFilesExtensions;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.threadpool.ThreadPool;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static co.elastic.elasticsearch.stateless.lucene.SearchDirectory.unwrapDirectory;
import static org.elasticsearch.blobcache.common.BlobCacheBufferedIndexInput.BUFFER_SIZE;
import static org.elasticsearch.blobcache.shared.SharedBytes.MAX_BYTES_PER_WRITE;
import static org.elasticsearch.core.Strings.format;

public class SharedBlobCacheWarmingService {

    private static final Logger logger = LogManager.getLogger(SharedBlobCacheWarmingService.class);

    private final StatelessSharedBlobCacheService cacheService;
    private final ThreadPool threadPool;
    private final ThrottledTaskRunner throttledTaskRunner;

    public SharedBlobCacheWarmingService(StatelessSharedBlobCacheService cacheService, ThreadPool threadPool) {
        this.cacheService = cacheService;
        this.threadPool = threadPool;
        this.throttledTaskRunner = new ThrottledTaskRunner(
            "prewarming-cache",
            // use half of the SHARD_READ_THREAD_POOL at max to leave room for regular concurrent reads to complete
            Math.max(1, threadPool.info(Stateless.SHARD_READ_THREAD_POOL).getMax() / 2),
            threadPool.generic()
        );
    }

    /**
     * Warms the cache to optimize cache hits during the recovery of an indexing or search shard. The warming happens concurrently
     * with the recovery and doesn't block it.
     *
     * <p>
     * This method uses the list of files of the recovered commit to identify which region(s) of the compound commit blob are likely to be
     * accessed first. It then tries to fetch every region to write them in cache. Note that regions are fetched completely, ie not only the
     * parts required for accessing one or more files. If the cache is under contention then one or more regions may be skipped and not
     * warmed up. If a region is pending to be written to cache by another thread, the warmer skips the region and starts warming the next
     * one without waiting for the region to be available in cache.
     * </p>
     *
     * @param indexShard the shard to warm in cache
     * @param commit the commit to be recovered
     */
    public void warmCacheForShardRecovery(IndexShard indexShard, StatelessCompoundCommit commit) {
        final long started = threadPool.rawRelativeTimeInMillis();
        logger.debug("{} warming", indexShard.shardId());
        warmCache(indexShard, commit, Map.Entry.comparingByKey(new IndexingShardRecoveryComparator()), ActionListener.running(() -> {
            final long finished = threadPool.rawRelativeTimeInMillis();
            logger.debug("{} warming completed in {} ms", indexShard.shardId(), finished - started);
        }));
    }

    protected void warmCache(
        IndexShard indexShard,
        StatelessCompoundCommit commit,
        Comparator<Map.Entry<String, BlobLocation>> comparator,
        ActionListener<Void> listener
    ) {
        final Store store = indexShard.store();
        if (store.isClosing() || store.tryIncRef() == false) {
            listener.onFailure(new AlreadyClosedException("Failed to warm cache for " + indexShard + ", store is closing"));
            return;
        }
        try (var refs = new RefCountingListener(ActionListener.runAfter(listener, store::decRef))) {
            // ordered list of files with their blob locations to warm up in cache
            var commitFiles = commit.commitFiles().entrySet().stream().sorted(comparator).toList();

            // used to deduplicate regions of blob files to avoid warming the same region twice
            final Set<BlobRegion> regions = Sets.newHashSetWithExpectedSize(commitFiles.size());
            for (var entry : commitFiles) {
                if (store.isClosing()) {
                    break;
                }
                var fileName = entry.getKey();
                var blobLocation = entry.getValue();
                addRegions(regions, indexShard.shardId(), fileName, blobLocation);
            }

            // Maybe avoid enqueing all regions needed for a recovery at once? It means that a recovery of a large shard may
            // keep busy the runner for a long time while the regions to warm for other shards are waiting to be processed in
            // the queue.

            // TODO the time to reach the first byte is the main latency, so we could combine contiguous regions in a single
            // warming task and benefit from the object store high throughput to fill more than one regions.

            for (BlobRegion region : regions) {
                throttledTaskRunner.enqueueTask(new CacheRegionWarmingTask(region.blob(), region.region(), indexShard, refs.acquire()));
            }
        }
    }

    private record BlobRegion(BlobFile blob, int region) {}

    /**
     * Finds the regions of the compound commit blob that must be warmed up for the given file.
     * <p>
     * The regions to warm are:
     * - the region containing the file header (but the size of the header is unknown so 1024 bytes will be requested)
     * - the region containing the file footer (usually 16 bytes)
     * If the file is a Lucene metadata file or is less than 1024 bytes then it is fully requested to compute the region(s).
     * </p>
     * @param fileName the name of the Lucene file
     * @param blobLocation the blob location of the Lucene file
     */
    private void addRegions(Set<BlobRegion> regions, ShardId shardId, String fileName, BlobLocation blobLocation) {
        if (shouldFullyWarmUp(fileName)) {
            // compute the range of bytes in the compound commit blob corresponding to the full file
            var range = ByteRange.of(blobLocation.offset(), blobLocation.offset() + blobLocation.fileLength());
            findRegions(range).forEach(region -> {
                var added = regions.add(new BlobRegion(blobLocation.blobFile(), region));
                logDetectedRegion(shardId, added, region, fileName, blobLocation, range, "content");
            });
            return;
        }

        // prewarm the header of the file (header have no fixed length so we prewarm BUFFER_SIZE at least)
        var header = ByteRange.of(blobLocation.offset(), blobLocation.offset() + Math.min(BUFFER_SIZE, blobLocation.fileLength()));
        findRegions(header).forEach(region -> {
            var added = regions.add(new BlobRegion(blobLocation.blobFile(), region));
            logDetectedRegion(shardId, added, region, fileName, blobLocation, header, "header");
        });

        // prewarm the footer of the file
        if (blobLocation.fileLength() > BUFFER_SIZE) {
            var footer = ByteRange.of(
                blobLocation.offset() + blobLocation.fileLength() - CodecUtil.footerLength(),
                blobLocation.offset() + blobLocation.fileLength()
            );
            findRegions(footer).forEach(region -> {
                var added = regions.add(new BlobRegion(blobLocation.blobFile(), region));
                logDetectedRegion(shardId, added, region, fileName, blobLocation, footer, "footer");
            });
        }
    }

    private static void logDetectedRegion(
        ShardId shardId,
        boolean added,
        int regions,
        String fileName,
        BlobLocation blobLocation,
        ByteRange range,
        String type
    ) {
        logger.trace(
            () -> format(
                "%s: %s warming region %s of blob [%s][%d-%d] for file [name=%s, length=%d] %s",
                shardId,
                added ? "scheduling" : "already scheduled",
                regions,
                blobLocation.blobFile().blobName(),
                range.start(),
                range.end(),
                fileName,
                blobLocation.fileLength(),
                type
            )
        );
    }

    private static boolean shouldFullyWarmUp(String fileName) {
        var extension = LuceneFilesExtensions.fromFile(fileName);
        return extension == null // segments_N are fully warmed up in cache
            || extension.isMetadata() // metadata files
            || StatelessCommitService.isGenerationalFile(fileName); // generational files
    }

    private static final ThreadLocal<ByteBuffer> writeBuffer = ThreadLocal.withInitial(() -> {
        assert ThreadPool.assertCurrentThreadPool(Stateless.SHARD_READ_THREAD_POOL);
        return ByteBuffer.allocateDirect(MAX_BYTES_PER_WRITE);
    });

    /**
     * Returns the list of regions, as integers, that are required to read a given byte range.
     *
     * @param range a range of bytes that can cover more than one region
     * @return a list of region ids
     */
    private IntStream findRegions(ByteRange range) {
        final int regionSize = cacheService.getRegionSize();
        int startRegion = (int) (range.start() / regionSize);
        int endRegion = (int) ((range.end() - (range.end() % regionSize == 0 ? 1 : 0)) / regionSize);
        if (startRegion != endRegion) {
            return IntStream.rangeClosed(startRegion, endRegion);
        }
        return IntStream.of(startRegion);
    }

    /**
     * Fetch and write in cache a given region of a compound commit blob file.
     */
    private class CacheRegionWarmingTask implements ActionListener<Releasable> {

        private final BlobFile blobFile;
        private final int blobRegion;
        private final IndexShard indexShard;
        private final ActionListener<Void> listener;

        CacheRegionWarmingTask(BlobFile blobFile, int blobRegion, IndexShard indexShard, ActionListener<Void> listener) {
            this.blobFile = blobFile;
            this.blobRegion = blobRegion;
            this.indexShard = indexShard;
            this.listener = listener;
        }

        private boolean shouldWarmRegion() {
            return indexShard.store().isClosing() == false && indexShard.state() == IndexShardState.RECOVERING;
        }

        @Override
        public void onResponse(Releasable releasable) {
            boolean success = false;
            try {
                if (shouldWarmRegion()) {
                    cacheService.maybeFetchRegion(
                        new FileCacheKey(indexShard.shardId(), blobFile.primaryTerm(), blobFile.blobName()),
                        blobRegion,
                        // this length is not used since we overload computeCacheFileRegionSize in StatelessSharedBlobCacheService to
                        // fully utilize each region. So we just pass it with a value that cover the current region.
                        (long) (blobRegion + 1) * cacheService.getRegionSize(),
                        (channel, channelPos, relativePos, length, progressUpdater) -> {
                            long position = (long) blobRegion * cacheService.getRegionSize() + relativePos;
                            var blobContainer = unwrapDirectory(indexShard.store().directory()).getBlobContainer(blobFile.primaryTerm());
                            try (var in = blobContainer.readBlob(OperationPurpose.INDICES, blobFile.blobName(), position, length)) {
                                assert ThreadPool.assertCurrentThreadPool(Stateless.SHARD_READ_THREAD_POOL);
                                var bytesCopied = SharedBytes.copyToCacheFileAligned(
                                    channel,
                                    in,
                                    channelPos,
                                    progressUpdater,
                                    writeBuffer.get().clear()
                                );
                                if (bytesCopied < length) {
                                    // TODO we should remove this and allow gap completion in SparseFileTracker even if progress < range end
                                    progressUpdater.accept(length);
                                }
                            }
                        },
                        ActionListener.releaseAfter(listener.map(warmed -> {
                            if (warmed) {
                                logger.trace("{} warmed region [{}] of [{}] in cache", indexShard.shardId(), blobRegion, blobFile);
                            } else {
                                logger.trace("{} skipped warming region [{}] of [{}] in cache", indexShard.shardId(), blobRegion, blobFile);
                            }
                            return null;
                        }), releasable)
                    );
                    success = true;
                } else {
                    listener.onResponse(null);
                }
            } finally {
                if (success == false) {
                    releasable.close();
                }
            }
        }

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }
    }
}
