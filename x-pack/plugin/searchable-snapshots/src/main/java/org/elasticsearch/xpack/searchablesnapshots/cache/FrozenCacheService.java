/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.cache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.Assertions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.common.Nullable;
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
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.cache.CacheKey;
import org.elasticsearch.index.store.cache.SparseFileTracker;
import org.elasticsearch.snapshots.SharedCacheConfiguration;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

import static org.elasticsearch.snapshots.SnapshotsService.FROZEN_CACHE_RECOVERY_RANGE_SIZE_SETTING;
import static org.elasticsearch.snapshots.SnapshotsService.SHARED_CACHE_RANGE_SIZE_SETTING;
import static org.elasticsearch.snapshots.SnapshotsService.SHARED_CACHE_SETTINGS_PREFIX;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsUtils.toIntBytes;

public class FrozenCacheService implements Releasable {

    public static final TimeValue MIN_SNAPSHOT_CACHE_DECAY_INTERVAL = TimeValue.timeValueSeconds(1L);
    public static final Setting<TimeValue> SNAPSHOT_CACHE_DECAY_INTERVAL_SETTING = Setting.timeSetting(
        SHARED_CACHE_SETTINGS_PREFIX + "decay.interval",
        TimeValue.timeValueSeconds(60L),                        // default
        MIN_SNAPSHOT_CACHE_DECAY_INTERVAL,                      // min
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<Integer> SNAPSHOT_CACHE_MAX_FREQ_SETTING = Setting.intSetting(
        SHARED_CACHE_SETTINGS_PREFIX + "max_freq",
        100,                       // default
        1,                            // min
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> SNAPSHOT_CACHE_MIN_TIME_DELTA_SETTING = Setting.timeSetting(
        SHARED_CACHE_SETTINGS_PREFIX + "min_time_delta",
        TimeValue.timeValueSeconds(60L),                        // default
        TimeValue.timeValueSeconds(0L),                         // min
        Setting.Property.NodeScope
    );

    private static final Logger logger = LogManager.getLogger(FrozenCacheService.class);

    private final ConcurrentHashMap<RegionKey, Entry<CacheFileRegion>> keyMapping;

    private final LongSupplier currentTimeSupplier;

    private final KeyedLock<CacheKey> keyedLock = new KeyedLock<>();

    private final SharedBytes sharedBytes;

    private final ByteSizeValue rangeSize;
    private final ByteSizeValue recoveryRangeSize;

    private final ConcurrentLinkedQueue<Integer> freeLargeRegions = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<Integer> freeSmallRegions = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<Integer> freeTinyRegions = new ConcurrentLinkedQueue<>();

    private final Entry<CacheFileRegion>[] regionFreqs;
    private final Entry<CacheFileRegion>[] smallRegionFreqs;
    private final Entry<CacheFileRegion>[] tinyRegionFreqs;

    private final int maxFreq;
    private final long minTimeDelta;

    private final AtomicReference<CacheFileRegion>[] regionOwners; // to assert exclusive access of regions

    private final CacheDecayTask decayTask;

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public FrozenCacheService(Environment environment, ThreadPool threadPool) {
        this.currentTimeSupplier = threadPool::relativeTimeInMillis;
        final Settings settings = environment.settings();
        try {
            sharedBytes = new SharedBytes(new SharedCacheConfiguration(settings), environment);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        final int numRegions = sharedBytes.sharedCacheConfiguration.numLargeRegions();
        final int numTinyRegions = sharedBytes.sharedCacheConfiguration.numTinyRegions();
        final int numSmallRegions = sharedBytes.sharedCacheConfiguration.numSmallRegions();
        keyMapping = new ConcurrentHashMap<>();
        if (Assertions.ENABLED) {
            regionOwners = new AtomicReference[numRegions + numSmallRegions + numTinyRegions];
            for (int i = 0; i < numRegions + numSmallRegions + numTinyRegions; i++) {
                regionOwners[i] = new AtomicReference<>();
            }
        } else {
            regionOwners = null;
        }
        for (int i = 0; i < numRegions; i++) {
            freeLargeRegions.add(i);
        }
        for (int i = 0; i < numSmallRegions; i++) {
            freeSmallRegions.add(numRegions + i);
        }
        for (int i = 0; i < numTinyRegions; i++) {
            freeTinyRegions.add(numRegions + numSmallRegions + i);
        }
        this.maxFreq = SNAPSHOT_CACHE_MAX_FREQ_SETTING.get(settings);
        this.minTimeDelta = SNAPSHOT_CACHE_MIN_TIME_DELTA_SETTING.get(settings).millis();
        regionFreqs = new Entry[maxFreq];
        smallRegionFreqs = new Entry[maxFreq];
        tinyRegionFreqs = new Entry[maxFreq];
        decayTask = new CacheDecayTask(threadPool, SNAPSHOT_CACHE_DECAY_INTERVAL_SETTING.get(settings));
        decayTask.rescheduleIfNecessary();
        this.rangeSize = SHARED_CACHE_RANGE_SIZE_SETTING.get(settings);
        this.recoveryRangeSize = FROZEN_CACHE_RECOVERY_RANGE_SIZE_SETTING.get(settings);
    }

    public int getRangeSize() {
        return toIntBytes(rangeSize.getBytes());
    }

    public int getRecoveryRangeSize() {
        return toIntBytes(recoveryRangeSize.getBytes());
    }

    private ByteRange mapSubRangeToRegion(ByteRange range, int region, long fileLength, long headerCacheLength, long footerCacheLength) {
        if (region == 0 && range.end() <= headerCacheLength) {
            return range;
        }
        final long regionStart = sharedBytes.sharedCacheConfiguration.getRegionStart(
            region,
            fileLength,
            headerCacheLength,
            footerCacheLength
        );
        final long regionEnd;
        if (footerCacheLength > 0 && fileLength - regionStart <= footerCacheLength) {
            regionEnd = fileLength;
        } else {
            regionEnd = regionStart + sharedBytes.sharedCacheConfiguration.getRegionSize(
                fileLength,
                region,
                headerCacheLength,
                footerCacheLength
            );
        }
        final ByteRange regionRange = ByteRange.of(regionStart, regionEnd);
        if (range.hasOverlap(regionRange) == false) {
            return ByteRange.EMPTY;
        }
        final ByteRange overlap = regionRange.overlap(range);
        final long start = overlap.start() - regionStart;
        return ByteRange.of(start, start + overlap.length());
    }

    public CacheFileRegion get(CacheKey cacheKey, long fileLength, int region, long headerCacheLength, long footerCacheLength) {
        final long regionSize = sharedBytes.sharedCacheConfiguration.getRegionSize(
            fileLength,
            region,
            headerCacheLength,
            footerCacheLength
        );
        try (Releasable ignore = keyedLock.acquire(cacheKey)) {
            final RegionKey regionKey = new RegionKey(cacheKey, region);
            final long now = currentTimeSupplier.getAsLong();
            final Entry<CacheFileRegion> entry = keyMapping.computeIfAbsent(
                regionKey,
                key -> new Entry<>(new CacheFileRegion(regionKey, regionSize), now)
            );
            if (entry.chunk.sharedPageIndex == -1) {
                // new item
                assert entry.freq == 0;
                assert entry.prev == null;
                assert entry.next == null;
                final SharedCacheConfiguration.RegionType regionType = sharedBytes.sharedCacheConfiguration.regionType(
                    region,
                    fileLength,
                    headerCacheLength,
                    footerCacheLength
                );
                final Integer freeSlot = tryPollFreeSlot(regionType);
                if (freeSlot != null) {
                    // no need to evict an item, just add
                    acquireSlotForEntry(entry, freeSlot);
                } else {
                    // need to evict something
                    synchronized (this) {
                        maybeEvict(regionType);
                    }
                    final Integer freeSlotRetry = tryPollFreeSlot(regionType);
                    if (freeSlotRetry != null) {
                        acquireSlotForEntry(entry, freeSlotRetry);
                    } else {
                        boolean removed = keyMapping.remove(regionKey, entry);
                        assert removed;
                        throw new AlreadyClosedException("no free region found");
                    }
                }
            } else {
                // check if we need to promote item
                synchronized (this) {
                    if (now - entry.lastAccessed >= minTimeDelta && entry.freq + 1 < maxFreq) {
                        unlink(entry);
                        entry.freq++;
                        entry.lastAccessed = now;
                        pushEntryToBack(entry);
                    }
                }
            }
            return entry.chunk;
        }
    }

    private Integer tryPollFreeSlot(SharedCacheConfiguration.RegionType regionType) {
        switch (regionType) {
            case SMALL:
                return freeSmallRegions.poll();
            case TINY:
                return freeTinyRegions.poll();
            default:
                return freeLargeRegions.poll();
        }
    }

    private void acquireSlotForEntry(Entry<CacheFileRegion> entry, int freeSlot) {
        entry.chunk.sharedPageIndex = freeSlot;
        assert regionOwners[freeSlot].compareAndSet(null, entry.chunk);
        synchronized (this) {
            pushEntryToBack(entry);
        }
    }

    public void onClose(CacheFileRegion chunk) {
        assert regionOwners[chunk.sharedPageIndex].compareAndSet(chunk, null);
        switch (sharedBytes.sharedCacheConfiguration.sharedRegionType(chunk.sharedPageIndex)) {
            case SMALL:
                freeSmallRegions.add(chunk.sharedPageIndex);
                break;
            case TINY:
                freeTinyRegions.add(chunk.sharedPageIndex);
                break;
            default:
                freeLargeRegions.add(chunk.sharedPageIndex);
        }
    }

    // used by tests
    int freeLargeRegionCount() {
        return freeLargeRegions.size();
    }

    // used by tests
    int freeSmallRegionCount() {
        return freeSmallRegions.size();
    }

    // used by tests
    int freeTinyRegionCount() {
        return freeTinyRegions.size();
    }

    private synchronized boolean invariant(final Entry<CacheFileRegion> e, boolean present) {
        boolean found = false;
        final Entry<CacheFileRegion>[] freqs = getFrequencies(
            sharedBytes.sharedCacheConfiguration.sharedRegionType(e.chunk.sharedPageIndex)
        );
        for (int i = 0; i < maxFreq; i++) {
            assert freqs[i] == null || freqs[i].prev != null;
            assert freqs[i] == null || freqs[i].prev != freqs[i] || freqs[i].next == null;
            assert freqs[i] == null || freqs[i].prev.next == null;
            for (Entry<CacheFileRegion> entry = freqs[i]; entry != null; entry = entry.next) {
                assert entry.next == null || entry.next.prev == entry;
                assert entry.prev != null;
                assert entry.prev.next == null || entry.prev.next == entry;
                assert entry.freq == i;
                if (entry == e) {
                    found = true;
                }
            }
            for (Entry<CacheFileRegion> entry = freqs[i]; entry != null && entry.prev != freqs[i]; entry = entry.prev) {
                assert entry.next == null || entry.next.prev == entry;
                assert entry.prev != null;
                assert entry.prev.next == null || entry.prev.next == entry;
                assert entry.freq == i;
                if (entry == e) {
                    found = true;
                }
            }
        }
        assert found == present;
        return true;
    }

    private void maybeEvict(SharedCacheConfiguration.RegionType size) {
        assert Thread.holdsLock(this);
        final Entry<CacheFileRegion>[] freqs = getFrequencies(size);
        for (int i = 0; i < maxFreq; i++) {
            for (Entry<CacheFileRegion> entry = freqs[i]; entry != null; entry = entry.next) {
                boolean evicted = entry.chunk.tryEvict();
                if (evicted) {
                    unlink(entry);
                    keyMapping.remove(entry.chunk.regionKey, entry);
                    return;
                }
            }
        }
    }

    private void pushEntryToBack(final Entry<CacheFileRegion> entry) {
        assert Thread.holdsLock(this);
        assert invariant(entry, false);
        assert entry.prev == null;
        assert entry.next == null;
        final Entry<CacheFileRegion>[] freqs = getFrequencies(
            sharedBytes.sharedCacheConfiguration.sharedRegionType(entry.chunk.sharedPageIndex)
        );
        final Entry<CacheFileRegion> currFront = freqs[entry.freq];
        if (currFront == null) {
            freqs[entry.freq] = entry;
            entry.prev = entry;
        } else {
            assert currFront.freq == entry.freq;
            final Entry<CacheFileRegion> last = currFront.prev;
            currFront.prev = entry;
            last.next = entry;
            entry.prev = last;
        }
        entry.next = null;
        assert freqs[entry.freq].prev == entry;
        assert freqs[entry.freq].prev.next == null;
        assert entry.prev != null;
        assert entry.prev.next == null || entry.prev.next == entry;
        assert entry.next == null;
        assert invariant(entry, true);
    }

    private void unlink(final Entry<CacheFileRegion> entry) {
        assert Thread.holdsLock(this);
        assert invariant(entry, true);
        assert entry.prev != null;
        final Entry<CacheFileRegion>[] freqs = getFrequencies(
            sharedBytes.sharedCacheConfiguration.sharedRegionType(entry.chunk.sharedPageIndex)
        );
        final Entry<CacheFileRegion> currFront = freqs[entry.freq];
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

    private Entry<CacheFileRegion>[] getFrequencies(SharedCacheConfiguration.RegionType regionType) {
        final Entry<CacheFileRegion>[] freqs;
        switch (regionType) {
            case LARGE:
                freqs = regionFreqs;
                break;
            case SMALL:
                freqs = smallRegionFreqs;
                break;
            default:
                freqs = tinyRegionFreqs;
                break;
        }
        return freqs;
    }

    private void computeDecay() {
        synchronized (this) {
            long now = currentTimeSupplier.getAsLong();
            doComputeDecay(now, regionFreqs);
            doComputeDecay(now, smallRegionFreqs);
            doComputeDecay(now, tinyRegionFreqs);
        }
    }

    private void doComputeDecay(long now, Entry<CacheFileRegion>[] freqs) {
        assert Thread.holdsLock(this);
        for (int i = 0; i < maxFreq; i++) {
            for (Entry<CacheFileRegion> entry = freqs[i]; entry != null; entry = entry.next) {
                if (now - entry.lastAccessed >= 2 * minTimeDelta) {
                    if (entry.freq > 0) {
                        unlink(entry);
                        entry.freq--;
                        pushEntryToBack(entry);
                    }
                }
            }
        }
    }

    public void removeFromCache(CacheKey cacheKey) {
        forceEvict(cacheKey::equals);
    }

    public void markShardAsEvictedInCache(String snapshotUUID, String snapshotIndexName, ShardId shardId) {
        forceEvict(
            k -> shardId.equals(k.getShardId())
                && snapshotIndexName.equals(k.getSnapshotIndexName())
                && snapshotUUID.equals(k.getSnapshotUUID())
        );
    }

    private void forceEvict(Predicate<CacheKey> cacheKeyPredicate) {
        final List<Entry<CacheFileRegion>> matchingEntries = new ArrayList<>();
        keyMapping.forEach((key, value) -> {
            if (cacheKeyPredicate.test(key.file)) {
                matchingEntries.add(value);
            }
        });
        if (matchingEntries.isEmpty() == false) {
            synchronized (this) {
                for (Entry<CacheFileRegion> entry : matchingEntries) {
                    boolean evicted = entry.chunk.forceEvict();
                    if (evicted) {
                        unlink(entry);
                        keyMapping.remove(entry.chunk.regionKey, entry);
                    }
                }
            }
        }
    }

    // used by tests
    int getFreq(CacheFileRegion cacheFileRegion) {
        return keyMapping.get(cacheFileRegion.regionKey).freq;
    }

    @Override
    public void close() {
        sharedBytes.decRef();
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
            return "frozen_cache_decay_task";
        }
    }

    private static class RegionKey {
        RegionKey(CacheKey file, int region) {
            this.file = file;
            this.region = region;
        }

        final CacheKey file;
        final int region;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RegionKey regionKey = (RegionKey) o;
            return region == regionKey.region && file.equals(regionKey.file);
        }

        @Override
        public int hashCode() {
            return Objects.hash(file, region);
        }

        @Override
        public String toString() {
            return "Chunk{" + "file=" + file + ", region=" + region + '}';
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

    class CacheFileRegion extends AbstractRefCounted {
        final RegionKey regionKey;
        final SparseFileTracker tracker;
        volatile int sharedPageIndex = -1;

        CacheFileRegion(RegionKey regionKey, long regionSize) {
            super("CacheFileRegion");
            this.regionKey = regionKey;
            assert regionSize > 0L;
            tracker = new SparseFileTracker("file", regionSize);
        }

        public long physicalStartOffset() {
            return sharedBytes.getPhysicalOffset(sharedPageIndex);
        }

        // If true this file region has been evicted from the cache and should not be used any more
        private final AtomicBoolean evicted = new AtomicBoolean(false);

        // tries to evict this chunk if noone is holding onto its resources anymore
        public boolean tryEvict() {
            if (refCount() <= 1 && evicted.compareAndSet(false, true)) {
                logger.trace("evicted {} with channel offset {}", regionKey, physicalStartOffset());
                decRef();
                return true;
            }
            return false;
        }

        public boolean forceEvict() {
            if (evicted.compareAndSet(false, true)) {
                logger.trace("force evicted {} with channel offset {}", regionKey, physicalStartOffset());
                decRef();
                return true;
            }
            return false;
        }

        public boolean isEvicted() {
            return evicted.get();
        }

        public boolean isReleased() {
            return isEvicted() && refCount() == 0;
        }

        @Override
        protected void closeInternal() {
            // now actually free the region associated with this chunk
            onClose(this);
            logger.trace("closed {} with channel offset {}", regionKey, physicalStartOffset());
        }

        private void ensureOpen() {
            if (evicted.get()) {
                throwAlreadyEvicted();
            }
        }

        private void throwAlreadyEvicted() {
            throw new AlreadyClosedException("File chunk is evicted");
        }

        @Nullable
        private StepListener<Integer> readIfAvailableOrPending(
            final ByteRange rangeToRead,
            final long regionRelativeStart,
            final RangeAvailableHandler reader
        ) {
            final StepListener<Integer> listener = new StepListener<>();
            Releasable decrementRef = null;
            try {
                ensureOpen();
                incRef();
                decrementRef = Releasables.releaseOnce(this::decRef);
                ensureOpen();
                final Releasable finalDecrementRef = decrementRef;
                listener.whenComplete(integer -> finalDecrementRef.close(), throwable -> finalDecrementRef.close());
                final SharedBytes.IO fileChannel = sharedBytes.getFileChannel(sharedPageIndex);
                listener.whenComplete(integer -> fileChannel.decRef(), e -> fileChannel.decRef());
                if (tracker.waitForRangeIfPending(
                    rangeToRead,
                    rangeListener(rangeToRead, regionRelativeStart, reader, listener, fileChannel)
                )) {
                    return listener;
                } else {
                    IOUtils.close(decrementRef, fileChannel::decRef);
                    return null;
                }
            } catch (Exception e) {
                releaseAndFail(listener, decrementRef, e);
                return listener;
            }
        }

        private ActionListener<Void> rangeListener(
            ByteRange rangeToRead,
            long regionRelativeStart,
            RangeAvailableHandler reader,
            ActionListener<Integer> listener,
            SharedBytes.IO fileChannel
        ) {
            final long relativePos = rangeToRead.start() + regionRelativeStart;
            return ActionListener.wrap(v -> {
                assert regionOwners[sharedPageIndex].get() == CacheFileRegion.this;
                // logger.info("--> read [{}] [{}] [{}]", rangeToRead, relativePos, fileChannel.size());
                final int read = reader.onRangeAvailable(fileChannel, rangeToRead.start(), relativePos, rangeToRead.length());
                assert read == rangeToRead.length() : "partial read ["
                    + read
                    + "] does not match the range to read ["
                    + rangeToRead.end()
                    + '-'
                    + rangeToRead.start()
                    + ']';
                listener.onResponse(read);
            }, listener::onFailure);
        }

        private void releaseAndFail(ActionListener<Integer> listener, Releasable decrementRef, Exception e) {
            try {
                Releasables.close(decrementRef);
            } catch (Exception ex) {
                e.addSuppressed(ex);
            }
            listener.onFailure(e);
        }

        @Override
        protected void alreadyClosed() {
            throwAlreadyEvicted();
        }
    }

    public class FrozenCacheFile {

        private final CacheKey cacheKey;
        private final long fileSize;
        private final long headerCacheLength;
        private final long footerCacheLength;

        private FrozenCacheFile(CacheKey cacheKey, long fileSize, ByteRange cacheRange, ByteRange sliceFooterByteRange) {
            this.cacheKey = cacheKey;
            this.fileSize = fileSize;
            this.headerCacheLength = SharedCacheConfiguration.effectiveHeaderCacheRange(cacheRange.length());
            this.footerCacheLength = sharedBytes.sharedCacheConfiguration.effectiveFooterCacheRange(sliceFooterByteRange.length());
        }

        public long getLength() {
            return fileSize;
        }

        public CacheKey getCacheKey() {
            return cacheKey;
        }

        /**
         * Populate frozen cache with the requested {@code rangeToWrite} end resolve given {@link RangeAvailableHandler} with the requested
         * {@code rangeToRead}.
         *
         * @param rangeToWrite range to populate cache with
         * @param rangeToRead  range to pass to {@code reader} once available
         * @param reader       reader to consumer read bytes
         * @param writer       writer that populates cache with requested {@code rangeToWrite}
         * @param executor     executor to run IO operations on
         * @return             listener that resolves with the number of bytes read
         */
        public StepListener<Integer> populateAndRead(
            final ByteRange rangeToWrite,
            final ByteRange rangeToRead,
            final RangeAvailableHandler reader,
            final RangeMissingHandler writer,
            final Executor executor
        ) {
            assert rangeToWrite.length() > 0 : "should not try to populate and read empty range";
            StepListener<Integer> stepListener = null;
            final int lastRegion = sharedBytes.sharedCacheConfiguration.getRegion(
                rangeToWrite.end() - 1,
                fileSize,
                headerCacheLength,
                footerCacheLength
            );
            final int firstRegion = sharedBytes.sharedCacheConfiguration.getRegion(
                rangeToWrite.start(),
                fileSize,
                headerCacheLength,
                footerCacheLength
            );
            final List<CacheFileRegion> regions = new ArrayList<>();
            final List<List<SparseFileTracker.Gap>> gapsList = new ArrayList<>();
            final List<SharedBytes.IO> channels = new ArrayList<>();
            long writeStart = -1L;
            long writeEnd = -1L;
            int firstWriteRegion = -1;
            for (int region = firstRegion; region <= lastRegion; region++) {
                final ByteRange subRangeToRead = mapSubRangeToRegion(rangeToRead, region, fileSize, headerCacheLength, footerCacheLength);
                final ByteRange subRangeToWrite = mapSubRangeToRegion(rangeToWrite, region, fileSize, headerCacheLength, footerCacheLength);
                assert subRangeToRead.length() > 0 || subRangeToWrite.length() > 0
                    : "Either read or write region must be non-empty but saw [" + subRangeToRead + "][" + subRangeToWrite + "]";

                final long regionStart = sharedBytes.sharedCacheConfiguration.getRegionStart(
                    region,
                    fileSize,
                    headerCacheLength,
                    footerCacheLength
                );
                final long readRangeRelativeStart = regionStart - rangeToRead.start();
                final CacheFileRegion fileRegion = get(cacheKey, fileSize, region, headerCacheLength, footerCacheLength);
                final StepListener<Integer> listener = new StepListener<>();
                Releasable decrementRef = null;
                try {
                    fileRegion.ensureOpen();
                    decrementRef = Releasables.releaseOnce(fileRegion::decRef);
                    final Releasable finalReleasable = decrementRef;
                    fileRegion.ensureOpen();
                    fileRegion.incRef();
                    listener.whenComplete(integer -> finalReleasable.close(), throwable -> finalReleasable.close());
                    final SharedBytes.IO fileChannel = sharedBytes.getFileChannel(fileRegion.sharedPageIndex);
                    fileChannel.incRef();
                    listener.whenComplete(integer -> fileChannel.decRef(), e -> fileChannel.decRef());
                    final List<SparseFileTracker.Gap> gaps = fileRegion.tracker.waitForRange(
                        subRangeToWrite,
                        subRangeToRead,
                        fileRegion.rangeListener(subRangeToRead, readRangeRelativeStart, reader, listener, fileChannel)
                    );
                    if (stepListener == null) {
                        stepListener = listener;
                    } else {
                        stepListener = stepListener.thenCombine(listener, Math::addExact);
                    }
                    if (gaps.isEmpty() == false) {
                        gapsList.add(gaps);
                        if (writeStart < 0) {
                            firstWriteRegion = region;
                            if (regionStart < rangeToWrite.start()) {
                                writeStart = gaps.get(0).start();
                            } else {
                                writeStart = 0;
                            }
                        }
                        channels.add(fileChannel);
                        writeEnd = regionStart + gaps.get(gaps.size() - 1).end();
                        regions.add(fileRegion);
                    } else if (gapsList.isEmpty() == false) {
                        gapsList.add(gaps);
                        fileChannel.decRef();
                        channels.add(null);
                        regions.add(fileRegion);
                    } else {
                        fileChannel.decRef();
                    }
                } catch (Exception e) {
                    fileRegion.releaseAndFail(listener, decrementRef, e);
                    return listener;
                }
            }
            if (gapsList.isEmpty() == false) {
                assert writeStart >= 0;
                final long startingRegionStart = sharedBytes.sharedCacheConfiguration.getRegionStart(
                    firstWriteRegion,
                    fileSize,
                    headerCacheLength,
                    footerCacheLength
                );
                final long startingOffset = writeStart;
                final long length = writeEnd - (startingRegionStart + writeStart);
                executor.execute(new AbstractRunnable() {
                    @Override
                    protected void doRun() throws Exception {
                        for (int i = 0; i < regions.size(); i++) {
                            if (regions.get(i).tryIncRef() == false) {
                                for (int j = 0; j < i; j++) {
                                    regions.get(j).decRef();
                                }
                                throw new AlreadyClosedException("Cache file channel has been released and closed");
                            }
                        }
                        try {
                            for (CacheFileRegion region : regions) {
                                region.ensureOpen();
                            }
                            final SharedBytes.IO[] ios = new SharedBytes.IO[channels.size()];
                            for (int i = 0; i < channels.size(); i++) {
                                SharedBytes.IO io = channels.get(i);
                                final CacheFileRegion region = regions.get(i);
                                if (io == null) {
                                    ios[i] = new SharedBytes.IO() {
                                        @Override
                                        public int read(ByteBuffer dst, long position) throws IOException {
                                            throw new AssertionError("nope");
                                        }

                                        @Override
                                        public void write(ByteBuffer src, long position) throws IOException {
                                            final int remaining = Math.toIntExact(size() - position);
                                            src.position(Math.min(src.position() + remaining, src.limit()));
                                        }

                                        @Override
                                        public long size() {
                                            return region.tracker.getLength();
                                        }

                                        @Override
                                        public void incRef() {

                                        }

                                        @Override
                                        public boolean tryIncRef() {
                                            return true;
                                        }

                                        @Override
                                        public boolean decRef() {
                                            return false;
                                        }
                                    };
                                } else if (region.tracker.getLength() != io.size()) {
                                    ios[i] = new SharedBytes.IO() {
                                        @Override
                                        public int read(ByteBuffer dst, long position) throws IOException {
                                            assert region.refCount() > 0;
                                            return io.read(dst, position);
                                        }

                                        @Override
                                        public void write(ByteBuffer src, long position) throws IOException {
                                            assert region.refCount() > 0;
                                            io.write(src, position);
                                        }

                                        @Override
                                        public long size() {
                                            assert region.refCount() > 0;
                                            return region.tracker.getLength();
                                        }

                                        @Override
                                        public void incRef() {
                                            assert region.refCount() > 0;
                                            io.incRef();
                                        }

                                        @Override
                                        public boolean tryIncRef() {
                                            return io.tryIncRef();
                                        }

                                        @Override
                                        public boolean decRef() {
                                            return io.decRef();
                                        }
                                    };
                                } else {
                                    ios[i] = io;
                                }
                            }
                            final long relativeWritePos = startingRegionStart + startingOffset - rangeToWrite.start();
                            writer.fillCacheRange(ios, startingOffset, relativeWritePos, length, progress -> {
                                // logger.info("--> read to [{}]", progress);
                            });
                            for (int i = 0; i < gapsList.size(); i++) {
                                List<SparseFileTracker.Gap> gaps = gapsList.get(i);
                                for (SparseFileTracker.Gap gap : gaps) {
                                    gap.onProgress(gap.end());
                                    gap.onCompletion();
                                }
                                final SharedBytes.IO c = channels.get(i);
                                if (c != null) {
                                    c.decRef();
                                }
                            }
                        } finally {
                            for (CacheFileRegion region : regions) {
                                region.decRef();
                            }
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        for (List<SparseFileTracker.Gap> gaps : gapsList) {
                            for (SparseFileTracker.Gap gap : gaps) {
                                gap.onFailure(e);
                            }
                        }
                    }
                });
            }
            return stepListener;
        }

        @Nullable
        public StepListener<Integer> readIfAvailableOrPending(final ByteRange rangeToRead, final RangeAvailableHandler reader) {
            assert rangeToRead.length() > 0 : "read range must not be empty";
            StepListener<Integer> stepListener = null;
            final long start = rangeToRead.start();
            final int lastRegion = sharedBytes.sharedCacheConfiguration.getRegion(
                rangeToRead.end() - 1,
                fileSize,
                headerCacheLength,
                footerCacheLength
            );
            for (int region = sharedBytes.sharedCacheConfiguration.getRegion(
                start,
                fileSize,
                headerCacheLength,
                footerCacheLength
            ); region <= lastRegion; region++) {
                final CacheFileRegion fileRegion = get(cacheKey, fileSize, region, headerCacheLength, footerCacheLength);
                final ByteRange subRangeToRead = mapSubRangeToRegion(rangeToRead, region, fileSize, headerCacheLength, footerCacheLength);
                final long regionRelativeStart = sharedBytes.sharedCacheConfiguration.getRegionStart(
                    region,
                    fileSize,
                    headerCacheLength,
                    footerCacheLength
                ) - start;
                final StepListener<Integer> lis = fileRegion.readIfAvailableOrPending(subRangeToRead, regionRelativeStart, reader);
                if (lis == null) {
                    return null;
                }
                if (stepListener == null) {
                    stepListener = lis;
                } else {
                    stepListener = stepListener.thenCombine(lis, Math::addExact);
                }
            }
            return stepListener;
        }

        @Override
        public String toString() {
            return "FrozenCacheFile{" + "cacheKey=" + cacheKey + ", length=" + fileSize + '}';
        }
    }

    public FrozenCacheFile getFrozenCacheFile(CacheKey cacheKey, long length, ByteRange cacheRange, ByteRange sliceFooterByteRange) {
        return new FrozenCacheFile(cacheKey, length, cacheRange, sliceFooterByteRange);
    }

    @FunctionalInterface
    public interface RangeAvailableHandler {

        /**
         * Signal that a given range of bytes has become available in the cache at a given channel position.
         *
         * @param channel     channel to cached bytes
         * @param channelPos  position relative to the start of {@code channel} that bytes became available at
         * @param relativePos position in the backing file that became available
         * @param length      number of bytes that became available
         * @return number of bytes that were actually read from the cache range as a result of becoming available
         */
        int onRangeAvailable(SharedBytes.IO channel, long channelPos, long relativePos, long length) throws IOException;
    }

    @FunctionalInterface
    public interface RangeMissingHandler {

        /**
         * Fills given shared bytes channel instance with the requested number of bytes at the given {@code channelPos}.
         *
         * @param channels           channels to write bytes to be cached to
         * @param channelRelativePos position relative to the start of the first channel in {@code channels} to write to
         *                           subsequent channels in the array will be written to from index 0
         * @param relativePos        position on the cached file that should be written to the channel at the given position
         * @param length             number of bytes to read and cache
         * @param progressUpdater    progress updater that is called with the number of bytes already written to the cache channel by the
         *                           implementation during cache writes
         */
        void fillCacheRange(
            SharedBytes.IO[] channels,
            long channelRelativePos,
            long relativePos,
            long length,
            Consumer<Long> progressUpdater
        ) throws IOException;
    }
}
