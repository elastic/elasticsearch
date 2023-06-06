/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache.shared;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.blobcache.BlobCacheUtils;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.blobcache.common.SparseFileTracker;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.RelativeByteSizeValue;
import org.elasticsearch.common.util.concurrent.AbstractAsyncTask;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.monitor.fs.FsProbe;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongConsumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class SharedBlobCacheService<KeyType> implements Releasable {

    private static final String SHARED_CACHE_SETTINGS_PREFIX = "xpack.searchable.snapshot.shared_cache.";

    public static final Setting<ByteSizeValue> SHARED_CACHE_RANGE_SIZE_SETTING = new Setting<>(
        SHARED_CACHE_SETTINGS_PREFIX + "range_size",
        ByteSizeValue.ofMb(16).getStringRep(),
        s -> ByteSizeValue.parseBytesSizeValue(s, SHARED_CACHE_SETTINGS_PREFIX + "range_size"),
        getPositivePageSizeAlignedByteSizeValueValidator(SHARED_CACHE_SETTINGS_PREFIX + "range_size"),
        Setting.Property.NodeScope
    );

    public static final Setting<ByteSizeValue> SHARED_CACHE_RECOVERY_RANGE_SIZE_SETTING = new Setting<>(
        SHARED_CACHE_SETTINGS_PREFIX + "recovery_range_size",
        ByteSizeValue.ofKb(128L).getStringRep(),
        s -> ByteSizeValue.parseBytesSizeValue(s, SHARED_CACHE_SETTINGS_PREFIX + "recovery_range_size"),
        getPositivePageSizeAlignedByteSizeValueValidator(SHARED_CACHE_SETTINGS_PREFIX + "recovery_range_size"),
        Setting.Property.NodeScope
    );

    public static final Setting<ByteSizeValue> SHARED_CACHE_REGION_SIZE_SETTING = new Setting<>(
        SHARED_CACHE_SETTINGS_PREFIX + "region_size",
        SHARED_CACHE_RANGE_SIZE_SETTING,
        s -> ByteSizeValue.parseBytesSizeValue(s, SHARED_CACHE_SETTINGS_PREFIX + "region_size"),
        getPositivePageSizeAlignedByteSizeValueValidator(SHARED_CACHE_SETTINGS_PREFIX + "region_size"),
        Setting.Property.NodeScope
    );

    private static Setting.Validator<ByteSizeValue> getPageSizeAlignedByteSizeValueValidator(String settingName) {
        return value -> {
            if (value.getBytes() == -1) {
                throw new SettingsException("setting [{}] must be non-negative", settingName);
            }
            if (value.getBytes() % SharedBytes.PAGE_SIZE != 0L) {
                throw new SettingsException("setting [{}] must be multiple of {}", settingName, SharedBytes.PAGE_SIZE);
            }
        };
    }

    private static Setting.Validator<ByteSizeValue> getPositivePageSizeAlignedByteSizeValueValidator(String settingName) {
        return value -> {
            if (value.getBytes() <= 0L) {
                throw new SettingsException("setting [{}] must be greater than zero", settingName);
            }
            getPageSizeAlignedByteSizeValueValidator(settingName).validate(value);
        };
    }

    public static final Setting<RelativeByteSizeValue> SHARED_CACHE_SIZE_SETTING = new Setting<>(
        new Setting.SimpleKey(SHARED_CACHE_SETTINGS_PREFIX + "size"),
        (settings) -> {
            if (DiscoveryNode.isDedicatedFrozenNode(settings) || isSearchOrIndexingNode(settings)) {
                return "90%";
            } else {
                return ByteSizeValue.ZERO.getStringRep();
            }
        },
        s -> RelativeByteSizeValue.parseRelativeByteSizeValue(s, SHARED_CACHE_SETTINGS_PREFIX + "size"),
        new Setting.Validator<>() {

            @Override
            public void validate(final RelativeByteSizeValue value) {

            }

            @Override
            public void validate(final RelativeByteSizeValue value, final Map<Setting<?>, Object> settings) {
                if (value.isAbsolute() && value.getAbsolute().getBytes() == -1) {
                    throw new SettingsException("setting [{}] must be non-negative", SHARED_CACHE_SETTINGS_PREFIX + "size");
                }
                if (value.isNonZeroSize()) {
                    @SuppressWarnings("unchecked")
                    final List<DiscoveryNodeRole> roles = (List<DiscoveryNodeRole>) settings.get(NodeRoleSettings.NODE_ROLES_SETTING);
                    final var rolesSet = Set.copyOf(roles);
                    if (DataTier.isFrozenNode(rolesSet) == false
                        && rolesSet.contains(DiscoveryNodeRole.SEARCH_ROLE) == false
                        && rolesSet.contains(DiscoveryNodeRole.INDEX_ROLE) == false) {
                        throw new SettingsException(
                            "Setting [{}] to be positive [{}] is only permitted on nodes with the data_frozen, search, or indexing role."
                                + " Roles are [{}]",
                            SHARED_CACHE_SETTINGS_PREFIX + "size",
                            value.getStringRep(),
                            roles.stream().map(DiscoveryNodeRole::roleName).collect(Collectors.joining(","))
                        );
                    }

                    @SuppressWarnings("unchecked")
                    final List<String> dataPaths = (List<String>) settings.get(Environment.PATH_DATA_SETTING);
                    if (dataPaths.size() > 1) {
                        throw new SettingsException(
                            "setting [{}={}] is not permitted on nodes with multiple data paths [{}]",
                            SHARED_CACHE_SIZE_SETTING.getKey(),
                            value.getStringRep(),
                            String.join(",", dataPaths)
                        );
                    }
                }
            }

            @Override
            public Iterator<Setting<?>> settings() {
                final List<Setting<?>> settings = List.of(NodeRoleSettings.NODE_ROLES_SETTING, Environment.PATH_DATA_SETTING);
                return settings.iterator();
            }

        },
        Setting.Property.NodeScope
    );

    private static boolean isSearchOrIndexingNode(Settings settings) {
        return DiscoveryNode.hasRole(settings, DiscoveryNodeRole.SEARCH_ROLE)
            || DiscoveryNode.hasRole(settings, DiscoveryNodeRole.INDEX_ROLE);
    }

    public static final Setting<ByteSizeValue> SHARED_CACHE_SIZE_MAX_HEADROOM_SETTING = new Setting<>(
        new Setting.SimpleKey(SHARED_CACHE_SETTINGS_PREFIX + "size.max_headroom"),
        (settings) -> {
            if (SHARED_CACHE_SIZE_SETTING.exists(settings) == false
                && (DiscoveryNode.isDedicatedFrozenNode(settings) || isSearchOrIndexingNode(settings))) {
                return "100GB";
            }

            return "-1";
        },
        (s) -> ByteSizeValue.parseBytesSizeValue(s, SHARED_CACHE_SETTINGS_PREFIX + "size.max_headroom"),
        new Setting.Validator<>() {
            private final Collection<Setting<?>> dependencies = List.of(SHARED_CACHE_SIZE_SETTING);

            @Override
            public Iterator<Setting<?>> settings() {
                return dependencies.iterator();
            }

            @Override
            public void validate(ByteSizeValue value) {
                // ignore
            }

            @Override
            public void validate(ByteSizeValue value, Map<Setting<?>, Object> settings, boolean isPresent) {
                if (isPresent && value.getBytes() != -1) {
                    RelativeByteSizeValue sizeValue = (RelativeByteSizeValue) settings.get(SHARED_CACHE_SIZE_SETTING);
                    if (sizeValue.isAbsolute()) {
                        throw new SettingsException(
                            "setting [{}] cannot be specified for absolute [{}={}]",
                            SHARED_CACHE_SIZE_MAX_HEADROOM_SETTING.getKey(),
                            SHARED_CACHE_SIZE_SETTING.getKey(),
                            sizeValue.getStringRep()
                        );
                    }
                }
            }
        },
        Setting.Property.NodeScope
    );

    public static final TimeValue MIN_SHARED_CACHE_DECAY_INTERVAL = TimeValue.timeValueSeconds(1L);
    public static final Setting<TimeValue> SHARED_CACHE_DECAY_INTERVAL_SETTING = Setting.timeSetting(
        SHARED_CACHE_SETTINGS_PREFIX + "decay.interval",
        TimeValue.timeValueSeconds(60L),                        // default
        MIN_SHARED_CACHE_DECAY_INTERVAL,                      // min
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<Integer> SHARED_CACHE_MAX_FREQ_SETTING = Setting.intSetting(
        SHARED_CACHE_SETTINGS_PREFIX + "max_freq",
        100,                       // default
        1,                            // min
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> SHARED_CACHE_MIN_TIME_DELTA_SETTING = Setting.timeSetting(
        SHARED_CACHE_SETTINGS_PREFIX + "min_time_delta",
        TimeValue.timeValueSeconds(60L),                        // default
        TimeValue.timeValueSeconds(0L),                         // min
        Setting.Property.NodeScope
    );

    private static final Logger logger = LogManager.getLogger(SharedBlobCacheService.class);

    private final ConcurrentHashMap<RegionKey<KeyType>, Entry<CacheFileRegion>> keyMapping;
    private final ThreadPool threadPool;

    private final SharedBytes sharedBytes;
    private final long cacheSize;
    private final long regionSize;
    private final ByteSizeValue rangeSize;
    private final ByteSizeValue recoveryRangeSize;

    private final int numRegions;
    private final ConcurrentLinkedQueue<Integer> freeRegions = new ConcurrentLinkedQueue<>();
    private final Entry<CacheFileRegion>[] freqs;
    private final int maxFreq;
    private final long minTimeDelta;

    private final AtomicReference<CacheFileRegion>[] regionOwners; // to assert exclusive access of regions

    private final CacheDecayTask decayTask;

    private final LongAdder writeCount = new LongAdder();
    private final LongAdder writeBytes = new LongAdder();

    private final LongAdder readCount = new LongAdder();
    private final LongAdder readBytes = new LongAdder();

    private final LongAdder evictCount = new LongAdder();

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public SharedBlobCacheService(NodeEnvironment environment, Settings settings, ThreadPool threadPool) {
        this.threadPool = threadPool;
        long totalFsSize;
        try {
            totalFsSize = FsProbe.getTotal(Environment.getFileStore(environment.nodeDataPaths()[0]));
        } catch (IOException e) {
            throw new IllegalStateException("unable to probe size of filesystem [" + environment.nodeDataPaths()[0] + "]");
        }
        this.cacheSize = calculateCacheSize(settings, totalFsSize);
        final long regionSize = SHARED_CACHE_REGION_SIZE_SETTING.get(settings).getBytes();
        this.numRegions = Math.toIntExact(cacheSize / regionSize);
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
        assert regionSize > 0L;
        this.maxFreq = SHARED_CACHE_MAX_FREQ_SETTING.get(settings);
        this.minTimeDelta = SHARED_CACHE_MIN_TIME_DELTA_SETTING.get(settings).millis();
        freqs = new Entry[maxFreq];
        try {
            sharedBytes = new SharedBytes(numRegions, regionSize, environment, writeBytes::add, readBytes::add);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        decayTask = new CacheDecayTask(threadPool, SHARED_CACHE_DECAY_INTERVAL_SETTING.get(settings));
        decayTask.rescheduleIfNecessary();
        this.rangeSize = SHARED_CACHE_RANGE_SIZE_SETTING.get(settings);
        this.recoveryRangeSize = SHARED_CACHE_RECOVERY_RANGE_SIZE_SETTING.get(settings);
    }

    public static long calculateCacheSize(Settings settings, long totalFsSize) {
        return SHARED_CACHE_SIZE_SETTING.get(settings)
            .calculateValue(ByteSizeValue.ofBytes(totalFsSize), SHARED_CACHE_SIZE_MAX_HEADROOM_SETTING.get(settings))
            .getBytes();
    }

    public int getRangeSize() {
        return BlobCacheUtils.toIntBytes(rangeSize.getBytes());
    }

    public int getRecoveryRangeSize() {
        return BlobCacheUtils.toIntBytes(recoveryRangeSize.getBytes());
    }

    private int getRegion(long position) {
        return Math.toIntExact(position / regionSize);
    }

    private long getRegionRelativePosition(long position) {
        return position % regionSize;
    }

    private long getRegionStart(int region) {
        return region * regionSize;
    }

    private long getRegionEnd(int region) {
        return (region + 1) * regionSize;
    }

    private int getEndingRegion(long position) {
        assert position > 0L;
        if (position % regionSize == 0L) {
            return getRegion(position - 1);
        }
        return getRegion(position);
    }

    private ByteRange mapSubRangeToRegion(ByteRange range, int region) {
        final long regionStart = getRegionStart(region);
        final long regionEnd = getRegionEnd(region);
        if (range.start() >= regionEnd || range.end() <= regionStart) {
            return ByteRange.EMPTY;
        }
        final long rangeStart = Math.max(regionStart, range.start());
        final long rangeEnd = Math.min(regionEnd, range.end());
        if (rangeStart >= rangeEnd) {
            return ByteRange.EMPTY;
        }
        return ByteRange.of(
            getRegionRelativePosition(rangeStart),
            rangeEnd == regionEnd ? regionSize : getRegionRelativePosition(rangeEnd)
        );
    }

    private long getRegionSize(long fileLength, int region) {
        assert fileLength > 0;
        final int maxRegion = getEndingRegion(fileLength);
        assert region >= 0 && region <= maxRegion : region + " - " + maxRegion;
        final long effectiveRegionSize;
        if (region == maxRegion && (region + 1) * regionSize != fileLength) {
            assert getRegionRelativePosition(fileLength) != 0L;
            effectiveRegionSize = getRegionRelativePosition(fileLength);
        } else {
            effectiveRegionSize = regionSize;
        }
        assert getRegionStart(region) + effectiveRegionSize <= fileLength;
        return effectiveRegionSize;
    }

    public CacheFileRegion get(KeyType cacheKey, long fileLength, int region) {
        final long effectiveRegionSize = getRegionSize(fileLength, region);
        final RegionKey<KeyType> regionKey = new RegionKey<>(cacheKey, region);
        final long now = threadPool.relativeTimeInMillis();
        final Entry<CacheFileRegion> entry = keyMapping.computeIfAbsent(
            regionKey,
            key -> new Entry<>(new CacheFileRegion(key, effectiveRegionSize), now)
        );
        // sharedBytesPos is volatile, double locking is fine, as long as we assign it last.
        if (entry.chunk.sharedBytesPos == -1) {
            synchronized (entry.chunk) {
                if (entry.chunk.sharedBytesPos == -1) {
                    if (keyMapping.get(regionKey) != entry) {
                        throw new AlreadyClosedException("no free region found (contender)");
                    }
                    // new item
                    assert entry.freq == 0;
                    assert entry.prev == null;
                    assert entry.next == null;
                    final Integer freeSlot = freeRegions.poll();
                    if (freeSlot != null) {
                        // no need to evict an item, just add
                        assignToSlot(entry, freeSlot);
                    } else {
                        // need to evict something
                        synchronized (this) {
                            maybeEvict();
                        }
                        final Integer freeSlotRetry = freeRegions.poll();
                        if (freeSlotRetry != null) {
                            assignToSlot(entry, freeSlotRetry);
                        } else {
                            boolean removed = keyMapping.remove(regionKey, entry);
                            assert removed;
                            throw new AlreadyClosedException("no free region found");
                        }
                    }

                    return entry.chunk;
                }
            }
        }
        assertChunkActiveOrEvicted(entry);

        // existing item, check if we need to promote item
        synchronized (this) {
            if (now - entry.lastAccessed >= minTimeDelta && entry.freq + 1 < maxFreq && entry.chunk.isEvicted() == false) {
                unlink(entry);
                entry.freq++;
                entry.lastAccessed = now;
                pushEntryToBack(entry);
            }
        }

        return entry.chunk;
    }

    private void assignToSlot(Entry<CacheFileRegion> entry, int freeSlot) {
        assert regionOwners[freeSlot].compareAndSet(null, entry.chunk);
        synchronized (this) {
            if (entry.chunk.isEvicted()) {
                assert regionOwners[freeSlot].compareAndSet(entry.chunk, null);
                freeRegions.add(freeSlot);
                keyMapping.remove(entry.chunk.regionKey, entry);
                throw new AlreadyClosedException("evicted during free region allocation");
            }
            pushEntryToBack(entry);
            // assign sharedBytesPos only when chunk is ready for use. Under lock to avoid concurrent tryEvict.
            entry.chunk.sharedBytesPos = freeSlot;
        }
    }

    private void assertChunkActiveOrEvicted(Entry<CacheFileRegion> entry) {
        if (Assertions.ENABLED) {
            synchronized (this) {
                // assert linked (or evicted)
                assert entry.prev != null || entry.chunk.isEvicted();

            }
        }
        assert regionOwners[entry.chunk.sharedBytesPos].get() == entry.chunk || entry.chunk.isEvicted();
    }

    public void onClose(CacheFileRegion chunk) {
        // we held the "this" lock when this was evicted, hence if sharedBytesPos is not filled in, chunk will never be registered.
        if (chunk.sharedBytesPos != -1) {
            assert regionOwners[chunk.sharedBytesPos].compareAndSet(chunk, null);
            freeRegions.add(chunk.sharedBytesPos);
        }
    }

    // used by tests
    int freeRegionCount() {
        return freeRegions.size();
    }

    public Stats getStats() {
        return new Stats(
            numRegions,
            cacheSize,
            regionSize,
            evictCount.sum(),
            writeCount.sum(),
            writeBytes.sum(),
            readCount.sum(),
            readBytes.sum()
        );
    }

    private synchronized boolean invariant(final Entry<CacheFileRegion> e, boolean present) {
        boolean found = false;
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

    private void maybeEvict() {
        assert Thread.holdsLock(this);
        for (int i = 0; i < maxFreq; i++) {
            for (Entry<CacheFileRegion> entry = freqs[i]; entry != null; entry = entry.next) {
                boolean evicted = entry.chunk.tryEvict();
                if (evicted && entry.chunk.sharedBytesPos != -1) {
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
        final Entry<CacheFileRegion> currFront = freqs[entry.freq];
        if (currFront == null) {
            freqs[entry.freq] = entry;
            entry.prev = entry;
            entry.next = null;
        } else {
            assert currFront.freq == entry.freq;
            final Entry<CacheFileRegion> last = currFront.prev;
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

    private void unlink(final Entry<CacheFileRegion> entry) {
        assert Thread.holdsLock(this);
        assert invariant(entry, true);
        assert entry.prev != null;
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

    private void computeDecay() {
        synchronized (this) {
            long now = threadPool.relativeTimeInMillis();
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
    }

    public void removeFromCache(KeyType cacheKey) {
        forceEvict(cacheKey::equals);
    }

    public void forceEvict(Predicate<KeyType> cacheKeyPredicate) {
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
                    if (evicted && entry.chunk.sharedBytesPos != -1) {
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
        decayTask.close();
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
            return "shared_cache_decay_task";
        }
    }

    private record RegionKey<KeyType>(KeyType file, int region) {
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
        final RegionKey<KeyType> regionKey;
        final SparseFileTracker tracker;
        volatile int sharedBytesPos = -1;

        CacheFileRegion(RegionKey<KeyType> regionKey, long regionSize) {
            this.regionKey = regionKey;
            assert regionSize > 0L;
            tracker = new SparseFileTracker("file", regionSize);
        }

        public long physicalStartOffset() {
            return sharedBytes.getPhysicalOffset(sharedBytesPos);
        }

        public long physicalEndOffset() {
            return sharedBytes.getPhysicalOffset(sharedBytesPos + 1);
        }

        // If true this file region has been evicted from the cache and should not be used any more
        private final AtomicBoolean evicted = new AtomicBoolean(false);

        // tries to evict this chunk if noone is holding onto its resources anymore
        // visible for tests.
        boolean tryEvict() {
            assert Thread.holdsLock(SharedBlobCacheService.this) : "must hold lock when evicting";
            if (refCount() <= 1 && evicted.compareAndSet(false, true)) {
                logger.trace("evicted {} with channel offset {}", regionKey, physicalStartOffset());
                evictCount.increment();
                decRef();
                return true;
            }
            return false;
        }

        public boolean forceEvict() {
            assert Thread.holdsLock(SharedBlobCacheService.this) : "must hold lock when evicting";
            if (evicted.compareAndSet(false, true)) {
                logger.trace("force evicted {} with channel offset {}", regionKey, physicalStartOffset());
                evictCount.increment();
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

        private void ensureOpen() {
            if (evicted.get()) {
                throwAlreadyEvicted();
            }
        }

        private static void throwAlreadyEvicted() {
            throw new AlreadyClosedException("File chunk is evicted");
        }

        void populateAndRead(
            final ByteRange rangeToWrite,
            final ByteRange rangeToRead,
            final RangeAvailableHandler reader,
            final RangeMissingHandler writer,
            final Executor executor,
            final ActionListener<Integer> listener
        ) {
            assert rangeToRead.length() > 0;
            final Releasable[] resources = new Releasable[2];
            try {
                ensureOpen();
                incRef();
                resources[1] = Releasables.releaseOnce(this::decRef);

                ensureOpen();
                final SharedBytes.IO fileChannel = sharedBytes.getFileChannel(sharedBytesPos);
                resources[0] = Releasables.releaseOnce(fileChannel);

                final ActionListener<Void> rangeListener = rangeListener(
                    rangeToRead,
                    reader,
                    ActionListener.runBefore(listener, () -> Releasables.close(resources)),
                    fileChannel
                );
                final List<SparseFileTracker.Gap> gaps = tracker.waitForRange(rangeToWrite, rangeToRead, rangeListener);

                if (gaps.isEmpty() == false) {
                    fillGaps(writer, executor, fileChannel, gaps);
                }
            } catch (Exception e) {
                releaseAndFail(listener, Releasables.wrap(resources), e);
            }
        }

        private void fillGaps(RangeMissingHandler writer, Executor executor, SharedBytes.IO fileChannel, List<SparseFileTracker.Gap> gaps) {
            for (SparseFileTracker.Gap gap : gaps) {
                executor.execute(new AbstractRunnable() {

                    @Override
                    protected void doRun() throws Exception {
                        if (CacheFileRegion.this.tryIncRef() == false) {
                            throw new AlreadyClosedException("Cache file channel has been released and closed");
                        }
                        try {
                            ensureOpen();
                            final long start = gap.start();
                            assert regionOwners[sharedBytesPos].get() == CacheFileRegion.this;
                            writer.fillCacheRange(
                                fileChannel,
                                physicalStartOffset() + gap.start(),
                                gap.start(),
                                gap.end() - gap.start(),
                                progress -> gap.onProgress(start + progress)
                            );
                            writeCount.increment();
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
        }

        private ActionListener<Void> rangeListener(
            ByteRange rangeToRead,
            RangeAvailableHandler reader,
            ActionListener<Integer> listener,
            SharedBytes.IO fileChannel
        ) {
            return listener.delegateFailureAndWrap((delegate, success) -> {
                final long physicalStartOffset = physicalStartOffset();
                assert regionOwners[sharedBytesPos].get() == CacheFileRegion.this;
                final int read = reader.onRangeAvailable(
                    fileChannel,
                    physicalStartOffset + rangeToRead.start(),
                    rangeToRead.start(),
                    rangeToRead.length()
                );
                assert read == rangeToRead.length()
                    : "partial read ["
                        + read
                        + "] does not match the range to read ["
                        + rangeToRead.end()
                        + '-'
                        + rangeToRead.start()
                        + ']';
                readCount.increment();
                delegate.onResponse(read);
            });
        }

        private static void releaseAndFail(ActionListener<Integer> listener, Releasable decrementRef, Exception e) {
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

    public class CacheFile {

        private final KeyType cacheKey;
        private final long length;

        private CacheFile(KeyType cacheKey, long length) {
            this.cacheKey = cacheKey;
            this.length = length;
        }

        public long getLength() {
            return length;
        }

        public KeyType getCacheKey() {
            return cacheKey;
        }

        public int populateAndRead(
            final ByteRange rangeToWrite,
            final ByteRange rangeToRead,
            final RangeAvailableHandler reader,
            final RangeMissingHandler writer,
            final String executor
        ) throws Exception {
            if (rangeToRead.length() == 0L) {
                // nothing to read, skip
                return 0;
            }
            final var exec = threadPool.executor(executor);
            final int startRegion = getRegion(rangeToWrite.start());
            final int endRegion = getEndingRegion(rangeToWrite.end());
            if (startRegion == endRegion) {
                return readSingleRegion(rangeToWrite, rangeToRead, reader, writer, exec, startRegion);
            }
            return readMultiRegions(rangeToWrite, rangeToRead, reader, writer, exec, startRegion, endRegion);
        }

        private int readSingleRegion(
            ByteRange rangeToWrite,
            ByteRange rangeToRead,
            RangeAvailableHandler reader,
            RangeMissingHandler writer,
            Executor executor,
            int region
        ) throws InterruptedException, ExecutionException {
            final PlainActionFuture<Integer> readFuture = PlainActionFuture.newFuture();
            final CacheFileRegion fileRegion = get(cacheKey, length, region);
            final long regionStart = getRegionStart(region);
            fileRegion.populateAndRead(
                mapSubRangeToRegion(rangeToWrite, region),
                mapSubRangeToRegion(rangeToRead, region),
                readerWithOffset(reader, fileRegion, rangeToRead.start() - regionStart),
                writerWithOffset(writer, fileRegion, rangeToWrite.start() - regionStart),
                executor,
                readFuture
            );
            return readFuture.get();
        }

        private int readMultiRegions(
            ByteRange rangeToWrite,
            ByteRange rangeToRead,
            RangeAvailableHandler reader,
            RangeMissingHandler writer,
            Executor executor,
            int startRegion,
            int endRegion
        ) throws InterruptedException, ExecutionException {
            final PlainActionFuture<Void> readsComplete = new PlainActionFuture<>();
            final AtomicInteger bytesRead = new AtomicInteger();
            try (var listeners = new RefCountingListener(1, readsComplete)) {
                for (int region = startRegion; region <= endRegion; region++) {
                    final ByteRange subRangeToRead = mapSubRangeToRegion(rangeToRead, region);
                    if (subRangeToRead.length() == 0L) {
                        // nothing to read, skip
                        continue;
                    }
                    final CacheFileRegion fileRegion = get(cacheKey, length, region);
                    final long regionStart = getRegionStart(region);
                    fileRegion.populateAndRead(
                        mapSubRangeToRegion(rangeToWrite, region),
                        subRangeToRead,
                        readerWithOffset(reader, fileRegion, rangeToRead.start() - regionStart),
                        writerWithOffset(writer, fileRegion, rangeToWrite.start() - regionStart),
                        executor,
                        listeners.acquire(i -> bytesRead.updateAndGet(j -> Math.addExact(i, j)))
                    );
                }
            }
            readsComplete.get();
            return bytesRead.get();
        }

        private RangeMissingHandler writerWithOffset(RangeMissingHandler writer, CacheFileRegion fileRegion, long writeOffset) {
            final RangeMissingHandler adjustedWriter;
            if (writeOffset == 0) {
                // no need to allocate a new capturing lambda if the offset isn't adjusted
                adjustedWriter = writer;
            } else {
                adjustedWriter = (channel, channelPos, relativePos, len, progressUpdater) -> writer.fillCacheRange(
                    channel,
                    channelPos,
                    relativePos - writeOffset,
                    len,
                    progressUpdater
                );
            }
            if (Assertions.ENABLED) {
                return (channel, channelPos, relativePos, len, progressUpdater) -> {
                    assert assertValidRegionAndLength(fileRegion, channelPos, len);
                    adjustedWriter.fillCacheRange(channel, channelPos, relativePos, len, progressUpdater);
                };
            }
            return adjustedWriter;
        }

        private RangeAvailableHandler readerWithOffset(RangeAvailableHandler reader, CacheFileRegion fileRegion, long readOffset) {
            final RangeAvailableHandler adjustedReader = (channel, channelPos, relativePos, len) -> reader.onRangeAvailable(
                channel,
                channelPos,
                relativePos - readOffset,
                len
            );
            if (Assertions.ENABLED) {
                return (channel, channelPos, relativePos, len) -> {
                    assert assertValidRegionAndLength(fileRegion, channelPos, len);
                    return adjustedReader.onRangeAvailable(channel, channelPos, relativePos, len);
                };
            }
            return adjustedReader;
        }

        private boolean assertValidRegionAndLength(CacheFileRegion fileRegion, long channelPos, long len) {
            assert regionOwners[fileRegion.sharedBytesPos].get() == fileRegion;
            assert channelPos >= fileRegion.physicalStartOffset() && channelPos + len <= fileRegion.physicalEndOffset();
            return true;
        }

        @Override
        public String toString() {
            return "SharedCacheFile{" + "cacheKey=" + cacheKey + ", length=" + length + '}';
        }
    }

    public CacheFile getCacheFile(KeyType cacheKey, long length) {
        return new CacheFile(cacheKey, length);
    }

    @FunctionalInterface
    public interface RangeAvailableHandler {
        // caller that wants to read from x should instead do a positional read from x + relativePos
        // caller should also only read up to length, further bytes will be offered by another call to this method
        int onRangeAvailable(SharedBytes.IO channel, long channelPos, long relativePos, long length) throws IOException;
    }

    @FunctionalInterface
    public interface RangeMissingHandler {
        void fillCacheRange(SharedBytes.IO channel, long channelPos, long relativePos, long length, LongConsumer progressUpdater)
            throws IOException;
    }

    public record Stats(
        int numberOfRegions,
        long size,
        long regionSize,
        long evictCount,
        long writeCount,
        long writeBytes,
        long readCount,
        long readBytes
    ) {
        public static final Stats EMPTY = new Stats(0, 0L, 0L, 0L, 0L, 0L, 0L, 0L);
    }
}
