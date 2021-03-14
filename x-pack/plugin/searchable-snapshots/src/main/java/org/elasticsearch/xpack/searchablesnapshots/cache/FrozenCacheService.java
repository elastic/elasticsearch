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
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.unit.ByteSizeUnit;
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
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.DataTier;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsUtils.toIntBytes;

public class FrozenCacheService implements Releasable {

    public static final ByteSizeValue MIN_SNAPSHOT_CACHE_RANGE_SIZE = new ByteSizeValue(4, ByteSizeUnit.KB);
    public static final ByteSizeValue MAX_SNAPSHOT_CACHE_RANGE_SIZE = new ByteSizeValue(Integer.MAX_VALUE, ByteSizeUnit.BYTES);

    public static final String SHARED_CACHE_SETTINGS_PREFIX = "xpack.searchable.snapshot.shared_cache.";

    public static final Setting<ByteSizeValue> SHARED_CACHE_RANGE_SIZE_SETTING = Setting.byteSizeSetting(
        SHARED_CACHE_SETTINGS_PREFIX + "range_size",
        ByteSizeValue.ofMb(16),                                 // default
        Setting.Property.NodeScope
    );

    public static final Setting<ByteSizeValue> SNAPSHOT_CACHE_REGION_SIZE_SETTING = Setting.byteSizeSetting(
        SHARED_CACHE_SETTINGS_PREFIX + "region_size",
        SHARED_CACHE_RANGE_SIZE_SETTING,
        Setting.Property.NodeScope
    );

    public static final Setting<ByteSizeValue> SNAPSHOT_CACHE_SIZE_SETTING = new Setting<>(
        SHARED_CACHE_SETTINGS_PREFIX + "size",
        ByteSizeValue.ZERO.getStringRep(),
        s -> ByteSizeValue.parseBytesSizeValue(s, SHARED_CACHE_SETTINGS_PREFIX + "size"),
        new Setting.Validator<ByteSizeValue>() {

            @Override
            public void validate(final ByteSizeValue value) {

            }

            @Override
            public void validate(final ByteSizeValue value, final Map<Setting<?>, Object> settings) {
                if (value.getBytes() == -1) {
                    throw new SettingsException("setting [{}] must be non-negative", SHARED_CACHE_SETTINGS_PREFIX + "size");
                }
                if (value.getBytes() > 0) {
                    @SuppressWarnings("unchecked")
                    final List<DiscoveryNodeRole> roles = (List<DiscoveryNodeRole>) settings.get(NodeRoleSettings.NODE_ROLES_SETTING);
                    if (DataTier.isFrozenNode(Set.of(roles.toArray(DiscoveryNodeRole[]::new))) == false) {
                        deprecationLogger.deprecate(
                            DeprecationCategory.SETTINGS,
                            "shared_cache",
                            "setting [{}] to be positive [{}] on node without the data_frozen role is deprecated, roles are [{}]",
                            SHARED_CACHE_SETTINGS_PREFIX + "size",
                            value.getStringRep(),
                            roles.stream().map(DiscoveryNodeRole::roleName).collect(Collectors.joining(","))
                        );
                    }
                }
            }

            @Override
            public Iterator<Setting<?>> settings() {
                final List<Setting<?>> settings = List.of(NodeRoleSettings.NODE_ROLES_SETTING);
                return settings.iterator();
            }

        },
        Setting.Property.NodeScope
    );

    public static final Setting<ByteSizeValue> FROZEN_CACHE_RECOVERY_RANGE_SIZE_SETTING = Setting.byteSizeSetting(
        SHARED_CACHE_SETTINGS_PREFIX + "recovery_range_size",
        new ByteSizeValue(128, ByteSizeUnit.KB),                // default
        MIN_SNAPSHOT_CACHE_RANGE_SIZE,                          // min
        MAX_SNAPSHOT_CACHE_RANGE_SIZE,                          // max
        Setting.Property.NodeScope
    );

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
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(FrozenCacheService.class);

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

    /**
     * Returns a {@link CacheFileRegion} instance for the given file parameters. The returned instance is reference must have its
     * reference count decremented by one once no longer used.
     *
     * @param cacheKey          cache key under which the region is tracked
     * @param fileLength        overall length of the file to cache
     * @param region            file region number to cache
     * @param headerCacheLength length of the separate header cache region
     * @param footerCacheLength length of the separate footer cache region
     * @return cache file region
     */
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
            final Entry<CacheFileRegion> entry = keyMapping.compute(regionKey, (key, en) -> {
                // If there isn't an entry for the given key, the existing entry has been evicted already or if its reference count can't
                // be incremented, create a new entry and increment its count by one to account for the caller. Callers of this method do
                // not start at reference count `1` but start at `2` since the initial reference is used by the eviction logic and
                // decremented only when this region is evicted from the cache.
                if (en == null || en.chunk.isEvicted() || en.chunk.tryIncRef() == false) {
                    en = new Entry<>(new CacheFileRegion(regionKey, regionSize), now);
                    en.chunk.incRef();
                }
                return en;
            });
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
                        entry.chunk.decRef();
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

        @Override
        protected void closeInternal() {
            // now actually free the region associated with this chunk
            onClose(this);
            logger.trace("closed {} with channel offset {}", regionKey, physicalStartOffset());
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
            final Releasable decrementRef = Releasables.releaseOnce(this::decRef);
            try {
                listener.whenComplete(integer -> decrementRef.close(), throwable -> decrementRef.close());
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
            final int lastRegion = regionForPosition(rangeToWrite.end() - 1);
            final int firstRegion = regionForPosition(rangeToWrite.start());
            final List<List<SparseFileTracker.Gap>> gapsList = new ArrayList<>();
            final List<SharedBytes.IO> channels = new ArrayList<>();
            final List<CacheFileRegion> regions = new ArrayList<>();
            long writeStartInRegion = -1L;
            long writeEnd = -1L;
            int lastRegionToWrite = -1;
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
                final CacheFileRegion fileRegion;
                try {
                    fileRegion = get(cacheKey, fileSize, region, headerCacheLength, footerCacheLength);
                } catch (Exception e) {
                    failGaps(e, gapsList);
                    releaseCacheResources(channels, regions);
                    throw e;
                }
                final StepListener<Integer> listener = new StepListener<>();
                // first make sure the region has not been concurrently evicted
                final SharedBytes.IO fileChannel = sharedBytes.getFileChannel(fileRegion.sharedPageIndex);
                final Releasable decrementRef = Releasables.releaseOnce(() -> Releasables.close(fileChannel::decRef, fileRegion::decRef));
                try {
                    // as we are potentially going to write to the region and its IO we acquire another reference for both the region
                    // and the IO for writing
                    fileChannel.incRef();
                    fileRegion.incRef();

                    // release the references for the reading that we acquired above when the read listener completes
                    listener.whenComplete(ignored -> decrementRef.close(), ignored -> decrementRef.close());

                    // find out what gaps we must fill for the given region
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
                        // we do have gaps to fill for this region so we track them as well as the writable IO that should be used for the
                        // region
                        gapsList.add(new ArrayList<>(gaps));
                        if (writeStartInRegion < 0) {
                            // negative region-relative position for write means this is the first region we write to, we track its number
                            // and the position relative to its start that we begin writing at
                            writeStartInRegion = gaps.get(0).start();
                        }
                        // since we have gaps to fill for the current region we track the writable IO to write to
                        channels.add(fileChannel);
                        regions.add(fileRegion);

                        // we will at least have to write to the last gap's end, so for now this is the upper bound of the write
                        writeEnd = regionStart + gaps.get(gaps.size() - 1).end();
                        // track the highest region number we will execute a write to
                        lastRegionToWrite = region;
                    } else {
                        // there were no gaps to fill for the current region, no need to keep a reference for it and its IO
                        fileChannel.decRef();
                        fileRegion.decRef();
                        if (gapsList.isEmpty() == false) {
                            gapsList.add(null);
                            channels.add(null);
                            regions.add(null);
                        }
                    }
                } catch (Exception e) {
                    fileRegion.releaseAndFail(listener, decrementRef, e);
                    return listener;
                }
            }

            final int numberOfRegionsToIterate = lastRegionToWrite >= 0 ? lastRegionToWrite - regions.get(0).regionKey.region + 1 : 0;

            // If there are no gaps to fill in the write range to be done just break out, the read side listener is already resolved in
            // this case
            if (numberOfRegionsToIterate == 0) {
                return stepListener;
            }

            // we do have gaps to fill so we should have tracked a write start relative to the first write region
            assert writeStartInRegion >= 0;
            // compute the offset of the first byte to write
            final long writeStart = sharedBytes.sharedCacheConfiguration.getRegionStart(
                regions.get(0).regionKey.region,
                fileSize,
                headerCacheLength,
                footerCacheLength
            ) + writeStartInRegion;
            final long length = writeEnd - writeStart;

            // collect region sizes and writable shared file region IOs into separate arrays
            final long[] lengths = new long[numberOfRegionsToIterate];

            for (int i = 0; i < numberOfRegionsToIterate; i++) {
                lengths[i] = sharedBytes.sharedCacheConfiguration.getRegionSize(
                    fileSize,
                    regions.get(0).regionKey.region + i,
                    headerCacheLength,
                    footerCacheLength
                );
            }
            final List<SharedBytes.IO> ios = channels.subList(0, numberOfRegionsToIterate);
            final SharedCacheFillContext handler = new SharedCacheFillContext(writeStartInRegion, ios, regions, lengths, gapsList, length);
            final long relativeWritePos = writeStart - rangeToWrite.start();
            executor.execute(new AbstractRunnable() {
                @Override
                protected void doRun() throws Exception {
                    writer.fillCacheRange(handler, relativeWritePos, length);
                }

                @Override
                public void onFailure(Exception e) {
                    failGaps(e, gapsList);
                }

                @Override
                public void onAfter() {
                    releaseCacheResources(ios, regions);
                }
            });
            return stepListener;
        }

        @Nullable
        public StepListener<Integer> readIfAvailableOrPending(final ByteRange rangeToRead, final RangeAvailableHandler reader) {
            assert rangeToRead.length() > 0 : "read range must not be empty";
            StepListener<Integer> stepListener = null;
            final long start = rangeToRead.start();
            final int lastRegion = regionForPosition(rangeToRead.end() - 1);
            for (int region = regionForPosition(start); region <= lastRegion; region++) {
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

        private int regionForPosition(long position) {
            return sharedBytes.sharedCacheConfiguration.getRegion(position, fileSize, headerCacheLength, footerCacheLength);
        }

        @Override
        public String toString() {
            return "FrozenCacheFile{" + "cacheKey=" + cacheKey + ", length=" + fileSize + '}';
        }
    }

    private static void releaseCacheResources(List<SharedBytes.IO> channels, List<CacheFileRegion> regions) {
        for (SharedBytes.IO channel : channels) {
            if (channel != null) {
                channel.decRef();
            }
        }
        for (CacheFileRegion r : regions) {
            if (r != null) {
                r.decRef();
            }
        }
    }

    private static void failGaps(Exception e, List<List<SparseFileTracker.Gap>> gapsList) {
        for (List<SparseFileTracker.Gap> gaps : gapsList) {
            if (gaps != null) {
                for (SparseFileTracker.Gap gap : gaps) {
                    gap.onFailure(e);
                }
            }
        }
    }

    public FrozenCacheFile getFrozenCacheFile(CacheKey cacheKey, long length, ByteRange cacheRange, ByteRange sliceFooterByteRange) {
        return new FrozenCacheFile(cacheKey, length, cacheRange, sliceFooterByteRange);
    }

    /**
     * Consumer of {@link ByteBuffer} that is used by the {@link RangeMissingHandler}.
     */
    private final class SharedCacheFillContext implements CheckedConsumer<ByteBuffer, IOException> {

        private final long startingOffset;

        /**
         * Regions to be written to or to be skipped if {@code null}
         */
        private final List<SharedBytes.IO> ios;

        /**
         * Cache file region instances for those regions that should be written into and {@code null} entries for regions that should just
         * be skipped.
         */
        private final List<CacheFileRegion> regions;

        /**
         * Length of each region
         */
        private final long[] lengths;

        /**
         * List of list of gaps per region to fill. Can contain {@code null} entries for regions that need no writes
         */
        private final List<List<SparseFileTracker.Gap>> gapsList;

        /**
         * Number of bytes to write overall.
         */
        private final long length;

        /**
         * Number of bytes already written through this instance
         */
        private long written = 0L;

        /**
         * Index of the current region in #ios
         */
        private int regionIndex = 0;

        /**
         * Offset at which the currently written to region starts
         */
        private long regionStart = 0L;

        SharedCacheFillContext(
            long startingOffset,
            List<SharedBytes.IO> ios,
            List<CacheFileRegion> regions,
            long[] lengths,
            List<List<SparseFileTracker.Gap>> gapsList,
            long length
        ) {
            this.startingOffset = startingOffset;
            this.ios = ios;
            this.regions = regions;
            this.lengths = lengths;
            this.gapsList = gapsList;
            this.length = length;
        }

        @Override
        public void accept(ByteBuffer bb) throws IOException {
            long offsetInRegion = startingOffset + written - regionStart;
            for (int i = regionIndex; i < ios.size() && bb.hasRemaining(); i++) {
                if (offsetInRegion == 0) {
                    // if we end up right at the region boundary we move the region index and region start accordingly
                    regionStart = written + startingOffset;
                    regionIndex = i;
                }

                // track how many bytes we had remaining in the buffer before we do any writing or skipping on the buffer
                final int remainingBefore = bb.remaining();

                final SharedBytes.IO fileRegionIO = ios.get(i);
                if (fileRegionIO == null) {
                    // we don't have a region for this index which means we just skip the bytes in this region
                    // TODO: this makes sense for blob stores with high seek latency and cost but for (N)FS and maybe HDFS
                    // repositories it might not be optimal to read redundant bytes only to avoid a seek
                    bb.position(Math.min(bb.position() + Math.toIntExact(lengths[i] - offsetInRegion), bb.limit()));
                } else if (offsetInRegion + bb.remaining() > lengths[i]) {
                    // we have a region to write to but writing the remaining bytes in the buffer to it would exceed the capacity of
                    // the current region so we limit the buffer accordingly and adjust the write limit back afterwards
                    final int oldLimit = bb.limit();
                    final int remainingInPage = Math.toIntExact(lengths[i] - offsetInRegion);
                    if (bb.remaining() > remainingInPage) {
                        bb.limit(bb.position() + remainingInPage);
                    }
                    fileRegionIO.write(bb, offsetInRegion);
                    bb.limit(oldLimit);
                } else {
                    // we have a region to write to and the buffer fits into it fully
                    fileRegionIO.write(bb, offsetInRegion);
                }

                // calculate how many bytes we were able to write or skip
                final int progressInThisStep = remainingBefore - bb.remaining();

                written += progressInThisStep;

                // we filled the region at index i so we can resolve all gaps in it that might now be fully filled
                List<SparseFileTracker.Gap> gaps = gapsList.get(i);
                if (gaps != null) {
                    final long maxInRegionPositionCompleted = offsetInRegion + progressInThisStep;
                    for (Iterator<SparseFileTracker.Gap> iterator = gaps.iterator(); iterator.hasNext();) {
                        SparseFileTracker.Gap gap = iterator.next();
                        final long gapEnd = gap.end();
                        if (gapEnd <= maxInRegionPositionCompleted) {
                            gap.onProgress(gapEnd);
                            gap.onCompletion();
                            iterator.remove();
                        }
                    }
                    if (gaps.isEmpty()) {
                        gapsList.set(i, null);
                        assert fileRegionIO != null : "we had gaps for this region so we also had a physical write region";
                        ios.set(i, null).decRef();
                        regions.set(i, null).decRef();
                    }
                } else {
                    assert fileRegionIO == null
                        : "we don't have a gap to resolve for this region so we should not have an open file region to write either";
                }
                assert written <= length : "wrote beyond the requested range by writing [" + written + "]";
                offsetInRegion = 0;
            }

            assert assertGapsResolvedOnDone();
            assert bb.hasRemaining() == false : "we should always consume the full buffer on each call";
        }

        private boolean assertGapsResolvedOnDone() {
            if (written == length) {
                for (List<SparseFileTracker.Gap> gaps : gapsList) {
                    assert gaps == null : "All gaps should be resolved after writing out all bytes";
                }
            }
            return true;
        }
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
         * @param cacheWriter        consumer that writes the bytes in the consumed buffer to the cache
         * @param relativePos        position in the cached file that should be written to the channel at the given position
         * @param length             number of bytes to read and cache
         */
        void fillCacheRange(CheckedConsumer<ByteBuffer, IOException> cacheWriter, long relativePos, long length) throws IOException;
    }
}
