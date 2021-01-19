/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.searchablesnapshots.cache;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.AbstractRefCounted;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.KeyedLock;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.cache.CacheKey;
import org.elasticsearch.index.store.cache.SparseFileTracker;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

public class SearchableSnapshotsLFUCache {

    private final ConcurrentLinkedQueue<CacheFileChunk>[] items;
    private final ConcurrentHashMap<Chunk, CacheFileChunk> keyMapping;

    private final LongSupplier currentTimeSupplier;

    public static final long BYTES_16_MB = ByteSizeValue.ofMb(16).getBytes();

    private final KeyedLock<CacheKey> keyedLock = new KeyedLock<>();

    private final SharedBytes sharedBytes;

    private final long regionSize;

    private final ConcurrentLinkedQueue<Integer> freeRegions = new ConcurrentLinkedQueue<>();

    // creates an LFU cache that can hold size items
    public SearchableSnapshotsLFUCache(int numRegions, long regionSize, int maxFreq, LongSupplier currentTimeSupplier) throws IOException {
        this.currentTimeSupplier = currentTimeSupplier;
        items = new ConcurrentLinkedQueue[maxFreq];
        for (int i = 0; i < maxFreq; i++) {
            items[i] = new ConcurrentLinkedQueue<>();
        }
        keyMapping = new ConcurrentHashMap<>();
        for (int i = 0; i < numRegions; i++) {
            freeRegions.add(i);
        }
        this.regionSize = regionSize;
        sharedBytes = new SharedBytes(numRegions, regionSize, Files.createTempFile("cache", "snap"));
    }

    private int getAccessBucket(long position) {
        return Math.toIntExact(position / regionSize);
    }

    private int getAccessBucketEnd(long position) {
        if (position % regionSize == 0L) {
            return getAccessBucket(position - 1);
        }
        return getAccessBucket(position);
    }

    private long getBucketRelativePosition(long position) {
        return position % regionSize;
    }

    private long getBucketStart(int bucket) {
        return bucket * regionSize;
    }

    private Tuple<Long, Long> mapSubRangeToBucket(Tuple<Long, Long> range, int bucket) {
        final long bucketStart = bucket * regionSize;
        final long bucketEnd = (bucket + 1) * regionSize;
        if (range.v1() >= bucketEnd || range.v2() <= bucketStart) {
            return Tuple.tuple(0L, 0L);
        }
        final long rangeStart = Math.max(bucketStart, range.v1());
        final long rangeEnd = Math.min(bucketEnd, range.v2());
        if (rangeStart >= rangeEnd) {
            return Tuple.tuple(0L, 0L);
        }
        return Tuple.tuple(getBucketRelativePosition(rangeStart),
            rangeEnd == bucketEnd ? regionSize : getBucketRelativePosition(rangeEnd));
    }

    private long getBucketSize(long fileLength, int bucket) {
        if (bucket * regionSize == fileLength) {
            return 0;
        }
        int maxBucket = getAccessBucket(fileLength - 1);
        if (bucket == maxBucket) {
            return fileLength % regionSize;
        } else {
            return regionSize;
        }
    }

    public CacheFileChunk get(CacheKey cacheKey, long fileLength, int accessBucket) {
        final long chunkLength = getBucketSize(fileLength, accessBucket);
        try (Releasable ignore = keyedLock.acquire(cacheKey)) {
            final CacheFileChunk chunk = keyMapping.computeIfAbsent(
                new Chunk(cacheKey, accessBucket), key -> new CacheFileChunk(accessBucket, chunkLength));
            final long now = currentTimeSupplier.getAsLong();
            if (chunk.freq > 0 && now - chunk.lastAccessed > 60_000) {
                // increase freq
                chunk.freq++;
                // TODO: move up one list, i.e., unlink at this level and relink one up
                chunk.lastAccessed = now;
            } else if (chunk.freq == 0) {
                // new item
                chunk.freq++;
                final Integer freeSlot = freeRegions.poll();
                if (freeSlot != null) {
                    // no need to evict an item, just add
                    items[0].add(chunk);
                    chunk.sharedBytesPos = freeSlot;
                } else {
                    assert false : "should never evict for now";
                    final CacheFileChunk evictedChunk = possiblyEvictItem(chunk.freq);
                    if (evictedChunk != null) {
                        items[0].add(chunk);
                        chunk.sharedBytesPos = evictedChunk.sharedBytesPos;
                    } else {
                        throw new ResourceNotFoundException("no eviction target found");
                    }
                }
            }
            return chunk;
        }
    }

    // searches for item that can be evicted that has at most the given freq

    @Nullable
    private CacheFileChunk possiblyEvictItem(int maxFreq) {
        for (int i = 0; i < maxFreq; i++) {
            final ConcurrentLinkedQueue<CacheFileChunk> deque = items[i];
            for (Iterator<CacheFileChunk> it = deque.iterator(); it.hasNext(); ) {
                final CacheFileChunk node = it.next();
                assert node.freq <= maxFreq;
                if (node.tryEvict()) {
                    // remove from list but keep node in keyMap
                    it.remove();
                    return node;
                }
            }
        }
        return null;
    }

    // periodically scan and update freq
    public void applyDecay() {

    }

    public void removeUnavailableFiles(ShardId shardId) {
        // full scan on keyMap and items to remove any entries that mention this shard
        // add freed entries to freeRegions deque again
    }

    private static class Chunk {
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Chunk chunk = (Chunk) o;
            return part == chunk.part && file.equals(chunk.file);
        }

        @Override
        public int hashCode() {
            return Objects.hash(file, part);
        }

        Chunk(CacheKey file, int part) {
            this.file = file;
            this.part = part;
        }

        final CacheKey file;
        final int part;
    }

    static class SharedBytes {

        private static final StandardOpenOption[] OPEN_OPTIONS = new StandardOpenOption[] {
            StandardOpenOption.READ,
            StandardOpenOption.WRITE,
            StandardOpenOption.CREATE
        };

        final int numRegions;
        final long regionSize;

        private final FileChannel fileChannel;

        SharedBytes(int numRegions, long regionSize, Path file) throws IOException {
            this.numRegions = numRegions;
            this.regionSize = regionSize;
            this.fileChannel = FileChannel.open(file, OPEN_OPTIONS);
            fileChannel.position(numRegions * regionSize);
            fileChannel.write(ByteBuffer.allocate(1));
        }

        FileChannel getFileChannel(int sharedBytesPos) {
            return fileChannel;
            /*return new FileChannel() {
                @Override
                public int read(ByteBuffer dst) throws IOException {
                    throw new UnsupportedOperationException();
                }

                @Override
                public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
                    checkOffsets(offset, length);
                    return fileChannel.read(dsts, offset, length);
                }

                @Override
                public int write(ByteBuffer src) throws IOException {
                    throw new UnsupportedOperationException();
                }

                @Override
                public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
                    checkOffsets(offset, length);
                    return fileChannel.write(srcs, offset, length);
                }

                @Override
                public long position() throws IOException {
                    return fileChannel.position();
                }

                @Override
                public FileChannel position(long newPosition) throws IOException {
                    checkOffsets(newPosition, 0);
                    return fileChannel.position(newPosition);
                }

                @Override
                public long size() throws IOException {
                    return fileChannel.size();
                }

                @Override
                public FileChannel truncate(long size) throws IOException {
                    assert false;
                    throw new UnsupportedOperationException();
                }

                @Override
                public void force(boolean metaData) throws IOException {
                    fileChannel.force(metaData);
                }

                @Override
                public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
                    checkOffsets(position, count);
                    return fileChannel.transferTo(position, count, target);
                }

                @Override
                public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
                    checkOffsets(position, count);
                    return fileChannel.transferFrom(src, position, count);
                }

                @Override
                public int read(ByteBuffer dst, long position) throws IOException {
                    checkOffsets(position, dst.remaining());
                    return fileChannel.read(dst, position);
                }

                @Override
                public int write(ByteBuffer src, long position) throws IOException {
                    checkOffsets(position, src.remaining());
                    return fileChannel.write(src, position);
                }

                private void checkOffsets(long position, long length) {
                    long pageStart = getPhysicalOffset(sharedBytesPos);
                    long pageEnd = pageStart + regionSize;
                    if (position < getPhysicalOffset(sharedBytesPos) ||
                        position > pageEnd ||
                        position + length > pageEnd) {
                        assert false;
                        throw new IllegalArgumentException("bad access");
                    }
                }

                @Override
                public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException {
                    assert false;
                    throw new UnsupportedOperationException();
                }

                @Override
                public FileLock lock(long position, long size, boolean shared) throws IOException {
                    assert false;
                    throw new UnsupportedOperationException();
                }

                @Override
                public FileLock tryLock(long position, long size, boolean shared) throws IOException {
                    assert false;
                    throw new UnsupportedOperationException();
                }

                @Override
                protected void implCloseChannel() throws IOException {
                    fileChannel.close();
                }
            };*/
        }

        long getPhysicalOffset(long chunkPosition) {
            return chunkPosition * regionSize;
        }
    }

    class CacheFileChunk extends AbstractRefCounted {
        int freq = 0;
        long lastAccessed = currentTimeSupplier.getAsLong();

        volatile int sharedBytesPos;
        final long accessBucket;
        final SparseFileTracker tracker;

        CacheFileChunk(int accessBucket, long chunkLength) {
            super("CacheFileChunk");
            this.accessBucket = accessBucket;
            tracker = new SparseFileTracker("file", chunkLength);
        }

        public long physicalStartOffset() {
            return sharedBytes.getPhysicalOffset(sharedBytesPos);
        }

        // If true this file has been evicted from the cache and should not be used any more
        private final AtomicBoolean evicted = new AtomicBoolean(false);
        private volatile boolean closed = false;

        // tries to evict this chunk. If not all resources are cleaned up right away return false
        public boolean tryEvict() {
            if (evicted.compareAndSet(false, true)) {
                // TODO: do some more stuff here
                decRef();
            }
            return closed;
        }

        public boolean closed() {
            return closed;
        }

        @Override
        protected void closeInternal() {
            // now actually free the region associated with this chunk
            closed = true;
        }

        private void ensureOpen() {
            if (evicted.get()) {
                throwAlreadyEvicted();
            }
        }

        private void throwAlreadyEvicted() {
            throw new AlreadyClosedException("File chunk is evicted");
        }

        public CompletableFuture<Integer> populateAndRead(
            final Tuple<Long, Long> rangeToWrite,
            final Tuple<Long, Long> rangeToRead,
            final RangeAvailableHandler reader,
            final RangeMissingHandler writer,
            final Executor executor
        ) {
            final CompletableFuture<Integer> future = new CompletableFuture<>();
            Releasable decrementRef = null;
            try {
                ensureOpen();
                incRef();
                decrementRef = Releasables.releaseOnce(this::decRef);
                final FileChannel fileChannel = sharedBytes.getFileChannel(sharedBytesPos);
                final List<SparseFileTracker.Gap> gaps = tracker.waitForRange(
                    rangeToWrite,
                    rangeToRead,
                    rangeListener(rangeToRead, reader, future, fileChannel, decrementRef)
                );

                for (SparseFileTracker.Gap gap : gaps) {
                    executor.execute(new AbstractRunnable() {

                        @Override
                        protected void doRun() throws Exception {
                            if (CacheFileChunk.this.tryIncRef() == false) {
                                assert false : "expected a non-closed channel reference";
                                throw new AlreadyClosedException("Cache file channel has been released and closed");
                            }
                            try {
                                ensureOpen();
                                final long start = gap.start();
                                writer.fillCacheRange(fileChannel, physicalStartOffset() + gap.start(), gap.start(),
                                    gap.end() - gap.start(), progress -> gap.onProgress(start + progress));
                            } finally {
                                decRef();
                            }
                            gap.onCompletion();
                        }

                        @Override
                        public void onFailure(Exception e) {
                            gap.onFailure(e);
                        }
                    });
                }
            } catch (Exception e) {
                releaseAndFail(future, decrementRef, e);
            }
            return future;
        }

        @Nullable
        public CompletableFuture<Integer> readIfAvailableOrPending(final Tuple<Long, Long> rangeToRead,
                                                                   final RangeAvailableHandler reader) {
            final CompletableFuture<Integer> future = new CompletableFuture<>();
            Releasable decrementRef = null;
            try {
                ensureOpen();
                incRef();
                decrementRef = Releasables.releaseOnce(this::decRef);
                final FileChannel fileChannel = sharedBytes.getFileChannel(sharedBytesPos);
                if (tracker.waitForRangeIfPending(rangeToRead, rangeListener(rangeToRead, reader, future, fileChannel, decrementRef))) {
                    return future;
                } else {
                    decrementRef.close();
                    return null;
                }
            } catch (Exception e) {
                releaseAndFail(future, decrementRef, e);
                return future;
            }
        }

        private ActionListener<Void> rangeListener(
            Tuple<Long, Long> rangeToRead,
            RangeAvailableHandler reader,
            CompletableFuture<Integer> future,
            FileChannel fileChannel,
            Releasable releasable
        ) {
            return ActionListener.runAfter(ActionListener.wrap(success -> {
                final long physicalStartOffset = physicalStartOffset();
                final int read = reader.onRangeAvailable(fileChannel,
                    physicalStartOffset + rangeToRead.v1(), rangeToRead.v1(), rangeToRead.v2() - rangeToRead.v1());
                assert read == rangeToRead.v2() - rangeToRead.v1() : "partial read ["
                    + read
                    + "] does not match the range to read ["
                    + rangeToRead.v2()
                    + '-'
                    + rangeToRead.v1()
                    + ']';
                future.complete(read);
            }, future::completeExceptionally), releasable::close);
        }

        private void releaseAndFail(CompletableFuture<Integer> future, Releasable decrementRef, Exception e) {
            try {
                Releasables.close(decrementRef);
            } catch (Exception ex) {
                e.addSuppressed(ex);
            }
            future.completeExceptionally(e);
        }
    }

    public class SharedCacheFile {

        private final CacheKey cacheKey;
        private final long length;

        public SharedCacheFile(CacheKey cacheKey, long length) {
            this.cacheKey = cacheKey;
            this.length = length;
        }

        public Future<Integer> populateAndRead(
            final Tuple<Long, Long> rangeToWrite,
            final Tuple<Long, Long> rangeToRead,
            final RangeAvailableHandler reader,
            final RangeMissingHandler writer,
            final Executor executor
        ) {
            CompletableFuture<Integer> combinedFut = null;
            final long writeStart = rangeToWrite.v1();
            final long readStart = rangeToRead.v1();
            for (int i = getAccessBucket(rangeToWrite.v1()); i <= getAccessBucketEnd(rangeToWrite.v2()); i++) {
                final int bucket = i;
                final Tuple<Long, Long> subRangeToWrite = mapSubRangeToBucket(rangeToWrite, i);
                final Tuple<Long, Long> subRangeToRead = mapSubRangeToBucket(rangeToRead, i);
                CacheFileChunk chunk = get(cacheKey, length, i);

                final CompletableFuture<Integer> fut = chunk.populateAndRead(subRangeToWrite, subRangeToRead,
                    new RangeAvailableHandler() {
                        @Override
                        public int onRangeAvailable(FileChannel channel, long channelPos, long relativePos, long length)
                            throws IOException {
                            final long distanceToStart = bucket == getAccessBucket(readStart) ?
                                relativePos - getBucketRelativePosition(readStart) :
                                getBucketStart(bucket) + relativePos - readStart;
                            return reader.onRangeAvailable(channel, channelPos, distanceToStart, length);
                        }
                    },
                    new RangeMissingHandler() {
                        @Override
                        public void fillCacheRange(FileChannel channel, long channelPos, long relativePos, long length,
                                                   Consumer<Long> progressUpdater) throws IOException {
                            final long distanceToStart = bucket == getAccessBucket(writeStart) ?
                                relativePos - getBucketRelativePosition(writeStart) :
                                getBucketStart(bucket) + relativePos - writeStart;
                            writer.fillCacheRange(channel, channelPos, distanceToStart, length, progressUpdater);
                        }
                }, executor);
                assert fut != null;
                if (combinedFut == null) {
                    combinedFut = fut;
                } else {
                    combinedFut = combinedFut.thenCombine(fut, Math::addExact);
                }

            }
            return combinedFut;
        }


        @Nullable
        public Future<Integer> readIfAvailableOrPending(final Tuple<Long, Long> rangeToRead, final RangeAvailableHandler reader) {
            CompletableFuture<Integer> combinedFut = null;
            final long start = rangeToRead.v1();
            for (int i = getAccessBucket(rangeToRead.v1()); i <= getAccessBucketEnd(rangeToRead.v2()); i++) {
                final int bucket = i;
                final Tuple<Long, Long> subRangeToRead = mapSubRangeToBucket(rangeToRead, i);
                /*if (subRangeToRead.v1() == subRangeToRead.v2()) {
                    // nothing to do
                    continue;
                }*/
                final CacheFileChunk chunk = get(cacheKey, length, bucket);
                final CompletableFuture<Integer> fut = chunk.readIfAvailableOrPending(subRangeToRead, new RangeAvailableHandler() {
                    @Override
                    public int onRangeAvailable(FileChannel channel, long channelPos, long relativePos, long length) throws IOException {
                        final long distanceToStart = bucket == getAccessBucket(start) ? relativePos - getBucketRelativePosition(start) :
                            getBucketStart(bucket) + relativePos - start;
                        return reader.onRangeAvailable(channel, channelPos, distanceToStart, length);
                    }
                });
                if (fut == null) {
                    return null;
                }
                if (combinedFut == null) {
                    combinedFut = fut;
                } else {
                    combinedFut = combinedFut.thenCombine(fut, Math::addExact);
                }
            }
            return combinedFut;
        }

        @Override
        public String toString() {
            return "SharedCacheFile{" +
                "cacheKey=" + cacheKey +
                ", length=" + length +
                '}';
        }
    }

    public SharedCacheFile getSharedCacheFile(CacheKey cacheKey, long length) {
        return new SharedCacheFile(cacheKey, length);
    }


    @FunctionalInterface
    public interface RangeAvailableHandler {
        // caller that wants to read from x should instead do a positional read from x + adaptedOffset
        // adaptedOffset can be negative or positive
        // caller should also only read up to size, further bytes will be offered by another call to this method
        int onRangeAvailable(FileChannel channel, long channelPos, long relativePos, long length) throws IOException;
    }

    @FunctionalInterface
    public interface RangeMissingHandler {
        void fillCacheRange(FileChannel channel, long channelPos, long relativePos, long length, Consumer<Long> progressUpdater)
            throws IOException;
    }
}
