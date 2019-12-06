/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots.cache;

import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.xpack.searchablesnapshots.FilterSearchableSnapshotShard;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotShard;
import org.elasticsearch.xpack.searchablesnapshots.cache.CacheService.CacheKey;
import org.elasticsearch.xpack.searchablesnapshots.cache.CacheService.CacheValue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;

public class CachedSearchableSnapshotShard extends FilterSearchableSnapshotShard {

    private final CacheService cacheService;
    private final int segmentSize;

    public CachedSearchableSnapshotShard(final SearchableSnapshotShard in, final CacheService cacheService) {
        super(in);
        this.cacheService = Objects.requireNonNull(cacheService);
        this.segmentSize = cacheService.getSegmentSize();
    }

    private CacheKey computeCacheKey(final String name, final long position) {
        return new CacheKey(getContext(), name, position / segmentSize);
    }

    @Override
    public Map<String, FileInfo> listSnapshotFiles() throws IOException {
        final CacheKey cacheKey = computeCacheKey("listSnapshotFiles(" + getContext() + ")", 0L);
        try {
            ListOfFilesCacheValue cachedListOfFiles = (ListOfFilesCacheValue) cacheService.computeIfAbsent(cacheKey, key ->
                new ListOfFilesCacheValue(super.listSnapshotFiles()));
            return Map.copyOf(cachedListOfFiles.files);
        } catch (Exception e) {
            throw new IOException("Failed to list searchable snapshot shard files from cache", e);
        }
    }

    @Override
    public ByteBuffer readSnapshotFile(final String name, long position, int length) throws IOException {
        final int segmentSize = cacheService.getSegmentSize();

        //
        //    .-------------------------.
        //    |    PARENTAL ADVISORY    |
        //    |                         |
        //    |     over-simplistic     |
        //    |     implementation      |
        //    |                         |
        //    '-------------------------'
        //
        try {
            final ByteBuffer buffer = ByteBuffer.allocate(length);

            int read = 0;
            while (length > 0) {
                final CacheKey cacheKey = computeCacheKey(name, position);

                FileSegmentCacheValue cachedFileSegment = (FileSegmentCacheValue) cacheService.computeIfAbsent(cacheKey, key ->
                    new FileSegmentCacheValue(super.readSnapshotFile(name, key.segment() * segmentSize, segmentSize)));

                // always duplicate ByteBuffer as it may come from cache
                final ByteBuffer segmentBuffer = cachedFileSegment.buffer.duplicate();

                // adjust reading position within the segment file
                final int segmentPosition = (int) (position % segmentSize);
                segmentBuffer.position(segmentPosition);

                // adjust length of bytes to read from the segment file
                final int limit = Math.min(length, segmentBuffer.limit() - segmentPosition);
                segmentBuffer.limit(segmentPosition + limit);
                buffer.put(segmentBuffer);

                position += limit;
                length -= limit;
                read += limit;
            }
            return buffer.limit(read).position(0);
        } catch (Exception e) {
            throw new IOException("Failed to retrieve the list of snapshot files", e);
        }
    }

    /**
     * Cached value for the list of files
     */
    private class ListOfFilesCacheValue extends CacheValue {

        private final Map<String, FileInfo> files;

        ListOfFilesCacheValue(final Map<String, FileInfo> files) {
            this.files = Objects.requireNonNull(files);
        }

        @Override
        long ramBytesUsed() {
            return 0L;
        }
    }

    /**
     * Cached value for a segment of a file
     */
    private class FileSegmentCacheValue extends CacheValue {
        private final ByteBuffer buffer;


        FileSegmentCacheValue(final ByteBuffer buffer) {
            this.buffer = buffer;
        }

        @Override
        long ramBytesUsed() {
            return this.buffer.capacity();
        }
    }
}
