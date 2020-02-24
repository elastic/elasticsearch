/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots.cache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.SnapshotId;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

/**
 * {@link CacheDirectory} uses a {@link CacheService} to cache Lucene files provided by another {@link Directory}.
 */
public class CacheDirectory extends FilterDirectory {

    private static final Logger logger = LogManager.getLogger(CacheDirectory.class);
    private static final int COPY_BUFFER_SIZE = 8192;

    private final Map<String, IndexInputStats> stats;
    private final CacheService cacheService;
    private final SnapshotId snapshotId;
    private final IndexId indexId;
    private final ShardId shardId;
    private final Path cacheDir;
    private final LongSupplier currentTimeNanosSupplier;

    public CacheDirectory(Directory in, CacheService cacheService, Path cacheDir, SnapshotId snapshotId, IndexId indexId, ShardId shardId,
                          LongSupplier currentTimeNanosSupplier)
        throws IOException {
        super(in);
        this.stats = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();
        this.cacheService = Objects.requireNonNull(cacheService);
        this.cacheDir = Files.createDirectories(cacheDir);
        this.snapshotId = Objects.requireNonNull(snapshotId);
        this.indexId = Objects.requireNonNull(indexId);
        this.shardId = Objects.requireNonNull(shardId);
        this.currentTimeNanosSupplier = Objects.requireNonNull(currentTimeNanosSupplier);
    }

    private CacheKey createCacheKey(String fileName) {
        return new CacheKey(snapshotId, indexId, shardId, fileName);
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

    // pkg private for tests
    @Nullable IndexInputStats getStats(String name) {
        return stats.get(name);
    }

    public void close() throws IOException {
        super.close();
        // Ideally we could let the cache evict/remove cached files by itself after the
        // directory has been closed.
        cacheService.removeFromCache(cacheKey -> cacheKey.belongsTo(snapshotId, indexId, shardId));
    }

    @Override
    public IndexInput openInput(final String name, final IOContext context) throws IOException {
        ensureOpen();
        final long fileLength = fileLength(name);
        return new CacheBufferedIndexInput(name, fileLength, context, stats.computeIfAbsent(name, n -> new IndexInputStats(fileLength)));
    }

    private class CacheFileReference implements CacheFile.EvictionListener {

        private final long fileLength;
        private final CacheKey cacheKey;
        private final AtomicReference<CacheFile> cacheFile = new AtomicReference<>(); // null if evicted or not yet acquired

        private CacheFileReference(String fileName, long fileLength) {
            this.cacheKey = createCacheKey(fileName);
            this.fileLength = fileLength;
        }

        @Nullable
        CacheFile get() throws Exception {
            CacheFile currentCacheFile = cacheFile.get();
            if (currentCacheFile != null) {
                return currentCacheFile;
            }

            final CacheFile newCacheFile = cacheService.get(cacheKey, fileLength, cacheDir);
            synchronized (this) {
                currentCacheFile = cacheFile.get();
                if (currentCacheFile != null) {
                    return currentCacheFile;
                }
                if (newCacheFile.acquire(this)) {
                    final CacheFile previousCacheFile = cacheFile.getAndSet(newCacheFile);
                    assert previousCacheFile == null;
                    return newCacheFile;
                }
            }
            return null;
        }

        String getFileName() {
            return cacheKey.getFileName();
        }

        @Override
        public void onEviction(final CacheFile evictedCacheFile) {
            synchronized (this) {
                if (cacheFile.compareAndSet(evictedCacheFile, null)) {
                    evictedCacheFile.release(this);
                }
            }
        }

        void releaseOnClose() {
            synchronized (this) {
                final CacheFile currentCacheFile = cacheFile.getAndSet(null);
                if (currentCacheFile != null) {
                    currentCacheFile.release(this);
                }
            }
        }

        @Override
        public String toString() {
            return "CacheFileReference{" +
                "cacheKey='" + cacheKey + '\'' +
                ", fileLength=" + fileLength +
                ", cacheDir=" + cacheDir +
                ", acquired=" + (cacheFile.get() != null) +
                '}';
        }
    }

    public class CacheBufferedIndexInput extends BufferedIndexInput {

        private final IOContext ioContext;
        private final long offset;
        private final long end;
        private final CacheFileReference cacheFileReference;
        private final IndexInputStats stats;

        // the following are only mutable so they can be adjusted after cloning
        private AtomicBoolean closed;
        private boolean isClone;

        // last read position is kept around in order to detect (non)contiguous reads for stats
        private long lastReadPosition;

        CacheBufferedIndexInput(String fileName, long fileLength, IOContext ioContext, IndexInputStats stats) {
            this(new CacheFileReference(fileName, fileLength), ioContext, stats,
                "CachedBufferedIndexInput(" + fileName + ")", 0L, fileLength, false);
            stats.incrementOpenCount();
        }

        private CacheBufferedIndexInput(CacheFileReference cacheFileReference, IOContext ioContext, IndexInputStats stats,
                                        String desc, long offset, long length, boolean isClone) {
            super(desc, ioContext);
            this.ioContext = ioContext;
            this.offset = offset;
            this.cacheFileReference = cacheFileReference;
            this.stats = stats;
            this.end = offset + length;
            this.closed = new AtomicBoolean(false);
            this.isClone = isClone;
        }

        @Override
        public long length() {
            return end - offset;
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                if (isClone == false) {
                    stats.incrementCloseCount();
                    cacheFileReference.releaseOnClose();
                }
            }
        }

        @Override
        protected void readInternal(final byte[] buffer, final int offset, final int length) throws IOException {
            final long position = getFilePointer() + this.offset;

            int totalBytesRead = 0;
            while (totalBytesRead < length) {
                final long pos = position + totalBytesRead;
                final int off = offset + totalBytesRead;
                final int len = length - totalBytesRead;

                int bytesRead = 0;
                try {
                    final CacheFile cacheFile = cacheFileReference.get();
                    if (cacheFile == null) {
                        throw new AlreadyClosedException("Failed to acquire a non-evicted cache file");
                    }

                    try (ReleasableLock ignored = cacheFile.fileLock()) {
                        bytesRead = cacheFile.fetchRange(pos,
                            (start, end) -> readCacheFile(cacheFile.getChannel(), end, pos, buffer, off, len),
                            (start, end) -> writeCacheFile(cacheFile.getChannel(), start, end))
                            .get();
                    }
                } catch (final Exception e) {
                    if (e instanceof AlreadyClosedException || (e.getCause() != null && e.getCause() instanceof AlreadyClosedException)) {
                        try {
                            // cache file was evicted during the range fetching, read bytes directly from source
                            bytesRead = readDirectly(pos, pos + len, buffer, off);
                            continue;
                        } catch (Exception inner) {
                            e.addSuppressed(inner);
                        }
                    }
                    throw new IOException("Fail to read data from cache", e);

                } finally {
                    totalBytesRead += bytesRead;
                }
            }
            assert totalBytesRead == length : "partial read operation, read [" + totalBytesRead + "] bytes of [" + length + "]";
            stats.incrementBytesRead(lastReadPosition, position, totalBytesRead);
            lastReadPosition = position + totalBytesRead;
        }

        int readCacheFile(FileChannel fc, long end, long position, byte[] buffer, int offset, long length) throws IOException {
            assert assertFileChannelOpen(fc);
            int bytesRead = Channels.readFromFileChannel(fc, position, buffer, offset, Math.toIntExact(Math.min(length, end - position)));
            stats.addCachedBytesRead(bytesRead);
            return bytesRead;
        }

        @SuppressForbidden(reason = "Use positional writes on purpose")
        void writeCacheFile(FileChannel fc, long start, long end) throws IOException {
            assert assertFileChannelOpen(fc);
            final byte[] copyBuffer = new byte[Math.toIntExact(Math.min(COPY_BUFFER_SIZE, end - start))];
            logger.trace(() -> new ParameterizedMessage("writing range [{}-{}] to cache file [{}]", start, end, cacheFileReference));

            int bytesCopied = 0;
            try (IndexInput input = in.openInput(cacheFileReference.getFileName(), ioContext)) {
                stats.incrementInnerOpenCount();
                final long startTimeNanos = currentTimeNanosSupplier.getAsLong();
                if (start > 0) {
                    input.seek(start);
                }
                long remaining = end - start;
                while (remaining > 0) {
                    final int size = (remaining < copyBuffer.length) ? Math.toIntExact(remaining) : copyBuffer.length;
                    input.readBytes(copyBuffer, 0, size);
                    fc.write(ByteBuffer.wrap(copyBuffer, 0, size), start + bytesCopied);
                    bytesCopied += size;
                    remaining -= size;
                }
                final long endTimeNanos = currentTimeNanosSupplier.getAsLong();
                stats.addCachedBytesWritten(bytesCopied, endTimeNanos - startTimeNanos);
            }
        }

        @Override
        protected void seekInternal(long pos) throws IOException {
            if (pos > length()) {
                throw new EOFException("Reading past end of file [position=" + pos + ", length=" + length() + "] for " + toString());
            } else if (pos < 0L) {
                throw new IOException("Seeking to negative position [" + pos + "] for " + toString());
            }
            stats.incrementSeeks(getFilePointer(), pos);
        }

        @Override
        public CacheBufferedIndexInput clone() {
            final CacheBufferedIndexInput clone = (CacheBufferedIndexInput) super.clone();
            clone.closed = new AtomicBoolean(false);
            clone.isClone = true;
            return clone;
        }

        @Override
        public IndexInput slice(String sliceDescription, long offset, long length) {
            if (offset < 0 || length < 0 || offset + length > this.length()) {
                throw new IllegalArgumentException("slice() " + sliceDescription + " out of bounds: offset=" + offset
                    + ",length=" + length + ",fileLength=" + this.length() + ": " + this);
            }
            return new CacheBufferedIndexInput(cacheFileReference, ioContext, stats,
                getFullSliceDescription(sliceDescription), this.offset + offset, length, true);
        }

        @Override
        public String toString() {
            return "CacheBufferedIndexInput{" +
                "cacheFileReference=" + cacheFileReference +
                ", offset=" + offset +
                ", end=" + end +
                ", length=" + length() +
                ", position=" + getFilePointer() +
                '}';
        }

        private int readDirectly(long start, long end, byte[] buffer, int offset) throws IOException {
            final byte[] copyBuffer = new byte[Math.toIntExact(Math.min(COPY_BUFFER_SIZE, end - start))];
            logger.trace(() ->
                new ParameterizedMessage("direct reading of range [{}-{}] for cache file [{}]", start, end, cacheFileReference));

            int bytesCopied = 0;
            try (IndexInput input = in.openInput(cacheFileReference.getFileName(), ioContext)) {
                stats.incrementInnerOpenCount();
                final long startTimeNanos = currentTimeNanosSupplier.getAsLong();
                if (start > 0) {
                    input.seek(start);
                }
                long remaining = end - start;
                while (remaining > 0) {
                    final int len = (remaining < copyBuffer.length) ? (int) remaining : copyBuffer.length;
                    input.readBytes(copyBuffer, 0, len);
                    System.arraycopy(copyBuffer, 0, buffer, offset + bytesCopied, len);
                    bytesCopied += len;
                    remaining -= len;
                }
                final long endTimeNanos = currentTimeNanosSupplier.getAsLong();
                stats.addDirectBytesRead(bytesCopied, endTimeNanos - startTimeNanos);
            }
            return bytesCopied;
        }
    }

    private static boolean assertFileChannelOpen(FileChannel fileChannel) {
        assert fileChannel != null;
        assert fileChannel.isOpen();
        return true;
    }
}
