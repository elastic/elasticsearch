/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.docvalues;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.blobcache.BlobCacheUtils;
import org.elasticsearch.blobcache.common.ByteBufferReference;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.blobcache.shared.DefaultEvictionPolicy;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * A local disk cache for the byte ranges of Parquet files, so that a query that reads a column chunk again does not fetch it again
 * from the object store. It is a thin layer on the {@link SharedBlobCacheService} of the {@code blob-cache} plugin, which owns the
 * file on disk, the regions, the eviction and the settings ({@code xpack.searchable.snapshot.shared_cache.*}). A reader asks for a
 * range of a file; the regions of the range that are not cached are filled with one ranged read of the {@link ParquetObject}.
 *
 * <p>The cache has its own settings ({@code xpack.parquet.cache.*}) and its own file, so it can live next to the frozen cache of
 * searchable snapshots on the same node.
 */
public final class ParquetBlobCache implements Releasable {

    /** Cache entries of one file. Files are assumed to be immutable, so the location and the length identify the content. */
    record CacheKey(String location, long length) implements SharedBlobCacheService.KeyBase {

        /** The cache evicts and accounts per shard, and these files do not belong to one. */
        private static final ShardId NO_SHARD = new ShardId("_parquet", "_na_", 0);

        @Override
        public ShardId shardId() {
            return NO_SHARD;
        }
    }

    /**
     * The size of the cache on disk, {@code 0} for no cache. The node needs this much free space in its data path when it starts, and it
     * must have one data path.
     */
    public static final Setting<ByteSizeValue> CACHE_SIZE = Setting.byteSizeSetting(
        "xpack.parquet.cache.size",
        ByteSizeValue.ZERO,
        Setting.Property.NodeScope
    );

    /** The size of a region of the cache. A chunk of a column uses part of a region, so small regions hold more chunks. */
    public static final Setting<ByteSizeValue> CACHE_REGION_SIZE = Setting.byteSizeSetting(
        "xpack.parquet.cache.region_size",
        ByteSizeValue.ofMb(16),
        Setting.Property.NodeScope
    );

    public static List<Setting<?>> settings() {
        return List.of(CACHE_SIZE, CACHE_REGION_SIZE);
    }

    /**
     * The name of our cache file in the data path. It differs from the name that the cache of searchable snapshots uses
     * ({@code shared_snapshot_cache}), because the file is not locked: a node with both caches would let them overwrite each other's
     * regions, and the cache of searchable snapshots deletes its file when its size is zero.
     */
    static final String CACHE_FILE_NAME = "parquet_cache";

    /** The size of the buffer that moves the bytes of a range into the cache, a multiple of the page size of the cache file. */
    private static final int COPY_BUFFER_SIZE = 1 << 20;

    private final SharedBlobCacheService<CacheKey> cache;

    /**
     * @param settings the settings of the node: only {@link #CACHE_SIZE} and {@link #CACHE_REGION_SIZE} are read, and they are given to
     *                 the shared cache under its own setting names, so the cache of searchable snapshots keeps its own size
     * @throws RuntimeException if the cache cannot be created, for example when the data path has not enough free space
     */
    public ParquetBlobCache(NodeEnvironment environment, Settings settings, ThreadPool threadPool, BlobCacheMetrics metrics) {
        ByteSizeValue regionSize = CACHE_REGION_SIZE.get(settings);
        Settings shared = Settings.builder()
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), CACHE_SIZE.get(settings).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), regionSize.getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING.getKey(), regionSize.getStringRep())
            .build();
        // the fetches run on the generic pool, because the pool of searchable snapshots may not exist on this node
        this.cache = new SharedBlobCacheService<>(
            environment,
            shared,
            threadPool,
            threadPool.generic(),
            metrics,
            System::nanoTime,
            new DefaultEvictionPolicy<>(),
            CACHE_FILE_NAME
        );
    }

    /** Whether the settings ask for a cache. */
    public static boolean isEnabled(Settings settings) {
        return CACHE_SIZE.get(settings).getBytes() > 0;
    }

    /**
     * Reads exactly {@code length} bytes of the file starting at {@code position} into {@code destination} at {@code destinationOffset},
     * from the cache where it can, and otherwise by a ranged read of the file that also fills the cache. This blocks until the bytes are
     * there, the same as a read of the file does.
     */
    public void read(ParquetObject object, long position, byte[] destination, int destinationOffset, int length) throws IOException {
        long fileLength = object.length();
        SharedBlobCacheService<CacheKey>.CacheFile file = cache.getCacheFile(
            new CacheKey(object.location(), fileLength),
            fileLength,
            SharedBlobCacheService.CacheMissHandler.NOOP,
            SharedBlobCacheService.UNKNOWN_TIMESTAMP
        );
        ByteRange rangeToRead = ByteRange.of(position, position + length);
        // Fetch what the reader needs, widened only to whole pages, because the cache file takes page-aligned writes only (an unaligned
        // range fails an assertion on the fetch thread, and the readers that wait for it never get an answer). Rounding up to the range
        // size of the cache instead would fetch the neighbours of a column chunk, which belong to other columns of the same row group, and
        // on the ClickBench file that wrote about ten times the bytes that a scan uses.
        ByteRange rangeToWrite = ByteRange.of(
            BlobCacheUtils.roundDownToAlignedSize(position, SharedBytes.PAGE_SIZE),
            Math.min(BlobCacheUtils.roundUpToAlignedSize(position + length, SharedBytes.PAGE_SIZE), fileLength)
        );
        ByteBufferReference target = new ByteBufferReference(ByteBuffer.wrap(destination, destinationOffset, length));
        try {
            int read = file.populateAndRead(
                rangeToWrite,
                rangeToRead,
                (channel, channelPos, relativePos, len) -> SharedBytes.readCacheFile(channel, channelPos, relativePos, len, target),
                (channel, channelPos, streamFactory, relativePos, len, progressUpdater, listener) -> ActionListener.completeWith(
                    listener,
                    () -> {
                        byte[] bytes = object.readRange(rangeToWrite.start() + relativePos, len);
                        SharedBytes.copyToCacheFileAligned(
                            channel,
                            new ByteArrayInputStream(bytes),
                            channelPos,
                            relativePos,
                            len,
                            progressUpdater,
                            ByteBuffer.allocateDirect(COPY_BUFFER_SIZE)
                        );
                        return null;
                    }
                ),
                object.location()
            );
            assert read == length : read + " vs " + length;
            target.finish(read);
        } catch (IOException | RuntimeException e) {
            stopWriting(target);
            throw e;
        } catch (Exception e) {
            stopWriting(target);
            throw new IOException("failed to read [" + length + "] bytes at [" + position + "] of [" + object.location() + "]", e);
        }
    }

    /** Makes sure that no callback of a failed read writes into the result any more. */
    private static void stopWriting(ByteBufferReference target) {
        try {
            target.finish(0);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            // finish only waits for the permits of the callbacks, and the read has failed already, so there is nothing more to report
            assert false : e;
        }
    }

    /** What the cache has done so far. */
    public SharedBlobCacheService.Stats stats() {
        return cache.getStats();
    }

    @Override
    public void close() {
        cache.close();
    }
}
