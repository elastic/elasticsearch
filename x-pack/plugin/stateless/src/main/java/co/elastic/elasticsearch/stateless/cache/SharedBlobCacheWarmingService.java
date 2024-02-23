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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
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

    private final SharedBlobCacheService<FileCacheKey> cacheService;
    private final ThreadPool threadPool;
    private final ThrottledTaskRunner throttledTaskRunner;

    public SharedBlobCacheWarmingService(SharedBlobCacheService<FileCacheKey> cacheService, ThreadPool threadPool) {
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
     * Warms the cache to optimize cache hits during the recovery of an indexing shard.
     * <p>
     * This method uses the list of files of the recovered commit to identify which region(s) of the compound commit blob are likely to be
     * accessed first. It then tries to fetch every region to write them in cache. Note that regions are fetched completely, ie not only the
     * parts required for accessing one or more files. If the cache is under contention then one or more regions may be skipped and not
     * warmed up. If a region is pending to be written to cache by another thread, the warmer skips the region and starts warming the next
     * one without waiting for the region to be available in cache.
     * </p>
     *
     * @param indexShard the indexing shard
     * @param commit the commit to be recovered
     */
    public void warmCacheForIndexingShardRecovery(IndexShard indexShard, StatelessCompoundCommit commit) {
        final long started = threadPool.rawRelativeTimeInMillis();
        logger.debug("{} warming", indexShard.shardId());
        warmCache(indexShard, commit, new IndexingShardRecoveryComparator(), ActionListener.running(() -> {
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
                        blobFile.blobLength(),
                        (channel, channelPos, relativePos, length, progressUpdater) -> {
                            long position = (long) blobRegion * cacheService.getRegionSize() + relativePos;
                            var blobContainer = unwrapDirectory(indexShard.store().directory()).getBlobContainer(blobFile.primaryTerm());
                            try (var in = blobContainer.readBlob(OperationPurpose.INDICES, blobFile.blobName(), position, length)) {
                                assert ThreadPool.assertCurrentThreadPool(Stateless.SHARD_READ_THREAD_POOL);
                                SharedBytes.copyToCacheFileAligned(
                                    channel,
                                    in,
                                    channelPos,
                                    relativePos,
                                    length,
                                    progressUpdater,
                                    writeBuffer.get().clear()
                                );
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

    /**
     * Order commit files in an optimized order for indexing shard recoveries
     */
    private static class IndexingShardRecoveryComparator implements Comparator<Map.Entry<String, BlobLocation>> {

        @Override
        public int compare(Map.Entry<String, BlobLocation> first, Map.Entry<String, BlobLocation> second) {
            final String fileName1 = first.getKey();
            final String fileName2 = second.getKey();

            // The segment_N file is usually the first file Lucene reads, so it is always prewarmed first.
            boolean segments1 = fileName1.startsWith(IndexFileNames.SEGMENTS);
            boolean segments2 = fileName2.startsWith(IndexFileNames.SEGMENTS);
            var compare = Boolean.compare(segments2, segments1);
            if (compare != 0) {
                return compare;
            }

            // Lucene then usually reads segment core info files (.si), so we prioritize them over other type of files.
            var si = LuceneFilesExtensions.SI.getExtension();
            boolean segmentInfo1 = IndexFileNames.matchesExtension(fileName1, si);
            boolean segmentInfo2 = IndexFileNames.matchesExtension(fileName2, si);
            compare = Boolean.compare(segmentInfo2, segmentInfo1);
            if (compare != 0) {
                return compare;
            }

            final var blobLocation1 = first.getValue();
            final var blobLocation2 = second.getValue();

            // Special case of two .si files: we sort them by blob locations (most recent first) and then by offsets within same blob
            // location.
            if (segmentInfo1 && segmentInfo2) {
                compare = Long.compare(blobLocation2.compoundFileGeneration(), blobLocation1.compoundFileGeneration());
                if (compare == 0) {
                    return Long.compare(blobLocation1.offset(), blobLocation2.offset());
                }
                return compare;
            }

            // Lucene usually reads generational files when opening the IndexWriter
            var isGenerationalFile1 = StatelessCommitService.isGenerationalFile(fileName1);
            var isGenerationalFile2 = StatelessCommitService.isGenerationalFile(fileName2);
            compare = Boolean.compare(isGenerationalFile2, isGenerationalFile1);
            if (compare != 0) {
                return compare;
            }

            // Lucene loads a global field map when initializing the IndexWriter, so we want to prewarm .fnm files before other type of
            // files.
            var fnm = LuceneFilesExtensions.FNM.getExtension();
            boolean fields1 = IndexFileNames.matchesExtension(fileName1, fnm);
            boolean fields2 = IndexFileNames.matchesExtension(fileName2, fnm);
            compare = Boolean.compare(fields2, fields1);
            if (compare != 0) {
                return compare;
            }

            // Special case of two .fnm files: we sort them by blob locations (most recent first) and then by offsets within same blob
            // location.
            if (fields1 && fields2) {
                compare = Long.compare(blobLocation2.compoundFileGeneration(), blobLocation1.compoundFileGeneration());
                if (compare == 0) {
                    return Long.compare(blobLocation1.offset(), blobLocation2.offset());
                }
                return compare;
            }

            // Lucene usually parses segment core info files (.si) in the order they are serialized in the segment_N file. We don't have
            // this exact order today (but we could add it this information in the compound commit blob in the future) so we use the segment
            // names (parsed as longs) to order them.
            var segmentName1 = IndexFileNames.parseGeneration(fileName1);
            var segmentName2 = IndexFileNames.parseGeneration(fileName2);
            compare = Long.compare(segmentName1, segmentName2);
            if (compare != 0) {
                return compare;
            }

            // Sort files belonging to the same segment core are sorted in a pre-defined order (see #getExtensionOrder)
            var extension1 = getExtensionOrder(fileName1);
            var extension2 = getExtensionOrder(fileName2);
            return Integer.compare(extension1, extension2);
        }
    }

    private static int getExtensionOrder(String fileName) {
        var ext = LuceneFilesExtensions.fromFile(fileName);
        assert ext != null : fileName;
        // basically the order in which files are accessed when SegmentCoreReaders and SegmentReader are instantiated
        return switch (ext) {
            case SI -> 0;
            case FNM -> 1;
            case CFE, CFS -> 2;
            case BFM, BFI, DOC, POS, PAY, CMP, LKP, TMD, TIM, TIP -> 3;
            case NVM, NVD -> 4;
            case FDM, FDT, FDX -> 5;
            case TVM, TVD, TVX, TVF -> 6;
            case KDM, KDI, KDD, DIM, DII -> 7;
            case VEC, VEX, VEM, VEMF, VEMQ, VEQ -> 8;
            case LIV -> 9;
            case DVM, DVD -> 10;
            default -> Integer.MAX_VALUE;
        };
    }
}
