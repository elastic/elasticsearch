/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.searchablesnapshots.cache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.Assertions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractAsyncTask;
import org.elasticsearch.common.util.concurrent.AbstractRefCounted;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.KeyedLock;
import org.elasticsearch.index.store.cache.CacheKey;
import org.elasticsearch.index.store.cache.SparseFileTracker;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

public class SearchableSnapshotsLFUCache {

    private static final String SETTINGS_PREFIX = "xpack.searchable.snapshot.shared-cache.";

    public static final Setting<ByteSizeValue> SNAPSHOT_CACHE_SIZE_SETTING = Setting.byteSizeSetting(
        SETTINGS_PREFIX + "size",
        ByteSizeValue.ZERO,
        Setting.Property.NodeScope
    );

    public static final Setting<ByteSizeValue> SNAPSHOT_CACHE_REGION_SIZE_SETTING = Setting.byteSizeSetting(
        SETTINGS_PREFIX + "region-size",
        ByteSizeValue.ofMb(16),
        Setting.Property.NodeScope
    );

    public static final TimeValue MIN_SNAPSHOT_CACHE_DECAY_INTERVAL = TimeValue.timeValueSeconds(1L);
    public static final Setting<TimeValue> SNAPSHOT_CACHE_DECAY_INTERVAL_SETTING = Setting.timeSetting(
        SETTINGS_PREFIX + "decay.interval",
        TimeValue.timeValueSeconds(60L),                        // default
        MIN_SNAPSHOT_CACHE_DECAY_INTERVAL,                      // min
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<Integer> SNAPSHOT_CACHE_MAX_FREQ_SETTING = Setting.intSetting(
        SETTINGS_PREFIX + "maxfreq",
        100,                       // default
        1,                            // min
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> SNAPSHOT_CACHE_MIN_TIME_DELTA_SETTING = Setting.timeSetting(
        SETTINGS_PREFIX + "min-time-delta",
        TimeValue.timeValueSeconds(60L),                        // default
        TimeValue.timeValueSeconds(0L),                         // min
        Setting.Property.NodeScope
    );

    private static final Logger logger = LogManager.getLogger(SearchableSnapshotsLFUCache.class);

    private final ConcurrentHashMap<ChunkKey, Entry<CacheFileChunk>> keyMapping;

    private final LongSupplier currentTimeSupplier;

    private final KeyedLock<CacheKey> keyedLock = new KeyedLock<>();

    private final SharedBytes sharedBytes;
    private final long regionSize;

    private final ConcurrentLinkedQueue<Integer> freeRegions = new ConcurrentLinkedQueue<>();
    private final Entry<CacheFileChunk>[] freqs;
    private final int maxFreq;
    private final long minTimeDelta;

    // to assert exclusive access of regions
    private final AtomicReference<CacheFileChunk>[] regionOwners;

    // creates an LFU cache that can hold size items
    public SearchableSnapshotsLFUCache(Settings settings, ThreadPool threadPool) throws IOException {
        this.currentTimeSupplier = threadPool::relativeTimeInMillis;
        final long cacheSize = SNAPSHOT_CACHE_SIZE_SETTING.get(settings).getBytes();
        final long regionSize = SNAPSHOT_CACHE_REGION_SIZE_SETTING.get(settings).getBytes();
        final int numRegions = Math.toIntExact(cacheSize / regionSize);
        keyMapping = new ConcurrentHashMap<>();
        if (Assertions.ENABLED) {
            regionOwners = new AtomicReference[numRegions];
            for (int i = 0; i < numRegions; i++) {
                regionOwners[i] = new AtomicReference<>();
            }
        } else {
            regionOwners = null;
        }
        for (int i = 0; i < numRegions; i++) {
            freeRegions.add(i);
        }
        this.regionSize = regionSize;
        this.maxFreq = SNAPSHOT_CACHE_MAX_FREQ_SETTING.get(settings);
        this.minTimeDelta = SNAPSHOT_CACHE_MIN_TIME_DELTA_SETTING.get(settings).millis();
        freqs = new Entry[maxFreq];
        sharedBytes = new SharedBytes(numRegions, regionSize, Files.createTempFile("cache", "snap"));
        new CacheDecayTask(threadPool, SNAPSHOT_CACHE_DECAY_INTERVAL_SETTING.get(settings)).rescheduleIfNecessary();
    }

    private int getBucket(long position) {
        return Math.toIntExact(position / regionSize);
    }

    private long getBucketRelativePosition(long position) {
        return position % regionSize;
    }

    private long getBucketStart(int bucket) {
        return bucket * regionSize;
    }

    private int getEndingBucket(long position) {
        if (position % regionSize == 0L) {
            return getBucket(position - 1);
        }
        return getBucket(position);
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
        int maxBucket = getBucket(fileLength - 1);
        if (bucket == maxBucket) {
            return fileLength % regionSize;
        } else {
            return regionSize;
        }
    }

    public CacheFileChunk get(CacheKey cacheKey, long fileLength, int bucket) {
        final long chunkLength = getBucketSize(fileLength, bucket);
        try (Releasable ignore = keyedLock.acquire(cacheKey)) {
            final ChunkKey chunkKey = new ChunkKey(cacheKey, bucket);
            final long now = currentTimeSupplier.getAsLong();
            final Entry<CacheFileChunk> entry = keyMapping.computeIfAbsent(chunkKey,
                key -> new Entry<>(new CacheFileChunk(chunkKey, bucket, chunkLength), now));
            if (entry.chunk.sharedBytesPos == -1) {
                // new item
                assert entry.freq == 0;
                assert entry.prev == null;
                assert entry.next == null;
                final Integer freeSlot = freeRegions.poll();
                if (freeSlot != null) {
                    // no need to evict an item, just add
                    entry.chunk.sharedBytesPos = freeSlot;
                    assert regionOwners[freeSlot].compareAndSet(null, entry.chunk);
                    synchronized(this) {
                        pushEntryToBack(entry);
                    }
                } else {
                    // need to evict something
                    synchronized(this) {
                        maybeEvict();
                    }
                    final Integer freeSlotRetry = freeRegions.poll();
                    if (freeSlotRetry != null) {
                        entry.chunk.sharedBytesPos = freeSlotRetry;
                        assert regionOwners[freeSlotRetry].compareAndSet(null, entry.chunk);
                        synchronized(this) {
                            pushEntryToBack(entry);
                        }
                    } else {
                        boolean removed = keyMapping.remove(chunkKey, entry);
                        assert removed;
                        throw new AlreadyClosedException("no free region found");
                    }
                }
            } else {
                // check if we need to promote item
                synchronized(this) {
                    if (now - entry.lastAccessed > minTimeDelta && entry.freq + 1 < maxFreq) {
                        // TODO: take lock here
                        unlink(entry);
                        entry.freq++;
                        pushEntryToBack(entry);
                    }
                }
            }
            return entry.chunk;
        }
    }

    public void onClose(CacheFileChunk chunk) {
        assert regionOwners[chunk.sharedBytesPos].compareAndSet(chunk, null);
        freeRegions.add(chunk.sharedBytesPos);
    }

    private synchronized boolean invariant(final Entry<CacheFileChunk> e, boolean present) {
        boolean found = false;
        for (int i = 0; i < maxFreq; i++) {
            assert freqs[i] == null || freqs[i].prev != null;
            assert freqs[i] == null || freqs[i].prev != freqs[i] || freqs[i].next == null;
            assert freqs[i] == null || freqs[i].prev.next == null;
            for (Entry<CacheFileChunk> entry = freqs[i]; entry != null; entry = entry.next) {
                assert entry.next == null || entry.next.prev == entry;
                assert entry.prev != null;
                assert entry.prev.next == null || entry.prev.next == entry;
                assert entry.freq == i;
                assert entry.chunk.evicted.get() == false;
                if (entry == e) {
                    found = true;
                }
            }
            for (Entry<CacheFileChunk> entry = freqs[i]; entry != null && entry.prev != freqs[i]; entry = entry.prev) {
                assert entry.next == null || entry.next.prev == entry;
                assert entry.prev != null;
                assert entry.prev.next == null || entry.prev.next == entry;
                assert entry.freq == i;
                assert entry.chunk.evicted.get() == false;
                if (entry == e) {
                    found = true;
                }
            }
        }
        assert found == present;
        return true;
    }

    private void maybeEvict() {
        assert Thread.holdsLock(this);
        for (int i = 0; i < maxFreq; i++) {
            for (Entry<CacheFileChunk> entry = freqs[i]; entry != null; entry = entry.next) {
                unlink(entry);
                keyMapping.remove(entry.chunk.chunkKey, entry);
                boolean evicted = entry.chunk.tryEvict();
                assert evicted;
                return;
            }
        }
    }

    private void pushEntryToBack(final Entry<CacheFileChunk> entry) {
        assert Thread.holdsLock(this);
        assert invariant(entry, false);
        assert entry.prev == null;
        assert entry.next == null;
        final Entry<CacheFileChunk> currFront = freqs[entry.freq];
        if (currFront == null) {
            freqs[entry.freq] = entry;
            entry.prev = entry;
            entry.next = null;
        } else {
            assert currFront.freq == entry.freq;
            final Entry<CacheFileChunk> last = currFront.prev;
            currFront.prev = entry;
            last.next = entry;
            entry.prev = last;
            entry.next = null;
        }
        assert freqs[entry.freq].prev == entry;
        assert freqs[entry.freq].prev.next == null;
        assert entry.prev != null;
        assert entry.prev.next == null || entry.prev.next == entry;
        assert entry.next == null;
        assert invariant(entry, true);
    }

    private void unlink(final Entry<CacheFileChunk> entry) {
        assert Thread.holdsLock(this);
        assert invariant(entry, true);
        assert entry.prev != null;
        final Entry<CacheFileChunk> currFront = freqs[entry.freq];
        assert currFront != null;
        if (currFront == entry) {
            freqs[entry.freq] = entry.next;
            if (entry.next != null) {
                assert entry.prev != entry;
                entry.next.prev = entry.prev;
            }
        } else {
            if (entry.next != null) {
                entry.next.prev = entry.prev;
            }
            entry.prev.next = entry.next;
            if (currFront.prev == entry) {
                currFront.prev = entry.prev;
            }
        }
        entry.next = null;
        entry.prev = null;
        assert invariant(entry, false);
    }

    private void computeDecay() {
        synchronized (this) {
            long now = currentTimeSupplier.getAsLong();
            for (int i = 0; i < maxFreq; i++) {
                for (Entry<CacheFileChunk> entry = freqs[i]; entry != null; entry = entry.next) {
                    if (now - entry.lastAccessed > 2 * minTimeDelta) {
                        if (entry.freq > 0) {
                            unlink(entry);
                            entry.freq--;
                            pushEntryToBack(entry);
                        }
                    }
                }
            }
        }
    }

    class CacheDecayTask extends AbstractAsyncTask {

        CacheDecayTask(ThreadPool threadPool, TimeValue interval) {
            super(logger, Objects.requireNonNull(threadPool), Objects.requireNonNull(interval), true);
        }

        @Override
        protected boolean mustReschedule() {
            return true;
        }

        @Override
        public void runInternal() {
            computeDecay();
        }

        @Override
        protected String getThreadPool() {
            return ThreadPool.Names.GENERIC;
        }

        @Override
        public String toString() {
            return "cache_synchronization_task";
        }
    }

    private static class ChunkKey {
        ChunkKey(CacheKey file, int part) {
            this.file = file;
            this.part = part;
        }

        final CacheKey file;
        final int part;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ChunkKey chunkKey = (ChunkKey) o;
            return part == chunkKey.part && file.equals(chunkKey.file);
        }

        @Override
        public int hashCode() {
            return Objects.hash(file, part);
        }

        @Override
        public String toString() {
            return "Chunk{" +
                "file=" + file +
                ", part=" + part +
                '}';
        }
    }

    static class Entry<T> {
        final T chunk;
        Entry<T> prev;
        Entry<T> next;
        int freq;
        long lastAccessed;

        Entry(T chunk, long lastAccessed) {
            this.chunk = chunk;
            this.lastAccessed = lastAccessed;
        }
    }

    class CacheFileChunk extends AbstractRefCounted {
        final ChunkKey chunkKey;
        final SparseFileTracker tracker;
        volatile int sharedBytesPos = -1;

        CacheFileChunk(ChunkKey chunkKey, int bucket, long chunkLength) {
            super("CacheFileChunk");
            this.chunkKey = chunkKey;
            tracker = new SparseFileTracker("file", chunkLength);
        }

        public long physicalStartOffset() {
            return sharedBytes.getPhysicalOffset(sharedBytesPos);
        }

        public long physicalEndOffset() {
            return sharedBytes.getPhysicalOffset(sharedBytesPos + 1);
        }

        // If true this file has been evicted from the cache and should not be used any more
        private final AtomicBoolean evicted = new AtomicBoolean(false);

        // tries to evict this chunk. If not all resources are cleaned up right away return false
        public boolean tryEvict() {
            if (evicted.compareAndSet(false, true)) {
                logger.trace("evicted {} with channel pos {}", chunkKey, physicalStartOffset());
                decRef();
                return true;
            }
            return false;
        }

        @Override
        protected void closeInternal() {
            // now actually free the region associated with this chunk
            onClose(this);
            logger.trace("closed {} with channel pos {}", chunkKey, physicalStartOffset());
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
                ensureOpen();
                Releasable finalDecrementRef = decrementRef;
                future.handle((integer, throwable) -> {
                    finalDecrementRef.close();
                    return null;
                });
                final FileChannel fileChannel = sharedBytes.getFileChannel(sharedBytesPos);
                final List<SparseFileTracker.Gap> gaps = tracker.waitForRange(
                    rangeToWrite,
                    rangeToRead,
                    rangeListener(rangeToRead, reader, future, fileChannel)
                );

                for (SparseFileTracker.Gap gap : gaps) {
                    executor.execute(new AbstractRunnable() {

                        @Override
                        protected void doRun() throws Exception {
                            if (CacheFileChunk.this.tryIncRef() == false) {
                                //assert false : "expected a non-closed channel reference";
                                throw new AlreadyClosedException("Cache file channel has been released and closed");
                            }
                            try {
                                ensureOpen();
                                final long start = gap.start();
                                assert regionOwners[sharedBytesPos].get() == CacheFileChunk.this;
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
                ensureOpen();
                Releasable finalDecrementRef = decrementRef;
                future.handle((integer, throwable) -> {
                    finalDecrementRef.close();
                    return null;
                });
                final FileChannel fileChannel = sharedBytes.getFileChannel(sharedBytesPos);
                if (tracker.waitForRangeIfPending(rangeToRead, rangeListener(rangeToRead, reader, future, fileChannel))) {
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
            FileChannel fileChannel
        ) {
            return ActionListener.wrap(success -> {
                final long physicalStartOffset = physicalStartOffset();
                assert regionOwners[sharedBytesPos].get() == CacheFileChunk.this;
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
            }, future::completeExceptionally);
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
            for (int i = getBucket(rangeToWrite.v1()); i <= getEndingBucket(rangeToWrite.v2()); i++) {
                final int bucket = i;
                final Tuple<Long, Long> subRangeToWrite = mapSubRangeToBucket(rangeToWrite, i);
                final Tuple<Long, Long> subRangeToRead = mapSubRangeToBucket(rangeToRead, i);
                CacheFileChunk chunk = get(cacheKey, length, i);
                chunk.ensureOpen();

                final CompletableFuture<Integer> fut = chunk.populateAndRead(subRangeToWrite, subRangeToRead,
                    new RangeAvailableHandler() {
                        @Override
                        public int onRangeAvailable(FileChannel channel, long channelPos, long relativePos, long length)
                            throws IOException {
                            final long distanceToStart = bucket == getBucket(readStart) ?
                                relativePos - getBucketRelativePosition(readStart) :
                                getBucketStart(bucket) + relativePos - readStart;
                            assert regionOwners[chunk.sharedBytesPos].get() == chunk;
                            assert channelPos >= chunk.physicalStartOffset() && channelPos + length <= chunk.physicalEndOffset();
                            return reader.onRangeAvailable(channel, channelPos, distanceToStart, length);
                        }
                    },
                    new RangeMissingHandler() {
                        @Override
                        public void fillCacheRange(FileChannel channel, long channelPos, long relativePos, long length,
                                                   Consumer<Long> progressUpdater) throws IOException {
                            final long distanceToStart = bucket == getBucket(writeStart) ?
                                relativePos - getBucketRelativePosition(writeStart) :
                                getBucketStart(bucket) + relativePos - writeStart;
                            assert regionOwners[chunk.sharedBytesPos].get() == chunk;
                            assert channelPos >= chunk.physicalStartOffset() && channelPos + length <= chunk.physicalEndOffset();
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
            for (int i = getBucket(rangeToRead.v1()); i <= getEndingBucket(rangeToRead.v2()); i++) {
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
                        final long distanceToStart = bucket == getBucket(start) ? relativePos - getBucketRelativePosition(start) :
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
        // caller that wants to read from x should instead do a positional read from x + relativePos
        // caller should also only read up to length, further bytes will be offered by another call to this method
        int onRangeAvailable(FileChannel channel, long channelPos, long relativePos, long length) throws IOException;
    }

    @FunctionalInterface
    public interface RangeMissingHandler {
        void fillCacheRange(FileChannel channel, long channelPos, long relativePos, long length, Consumer<Long> progressUpdater)
            throws IOException;
    }
}
