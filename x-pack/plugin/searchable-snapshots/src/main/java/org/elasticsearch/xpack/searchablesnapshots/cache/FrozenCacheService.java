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
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
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
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.cache.CacheKey;
import org.elasticsearch.index.store.cache.SparseFileTracker;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.DataTier;

import java.io.IOException;
import java.io.UncheckedIOException;
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
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsUtils.toIntBytes;

public class FrozenCacheService implements Releasable {

    public static final ByteSizeValue MIN_SNAPSHOT_CACHE_RANGE_SIZE = new ByteSizeValue(4, ByteSizeUnit.KB);
    public static final ByteSizeValue MAX_SNAPSHOT_CACHE_RANGE_SIZE = new ByteSizeValue(Integer.MAX_VALUE, ByteSizeUnit.BYTES);

    private static final String SHARED_CACHE_SETTINGS_PREFIX = "xpack.searchable.snapshot.shared_cache.";

    public static final Setting<ByteSizeValue> SHARED_CACHE_RANGE_SIZE_SETTING = Setting.byteSizeSetting(
        SHARED_CACHE_SETTINGS_PREFIX + "range_size",
        ByteSizeValue.ofMb(16),                                 // default
        Setting.Property.NodeScope
    );

    // TODO: require range size to be multiple of 4kb
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
    private final long regionSize;
    private final ByteSizeValue rangeSize;
    private final ByteSizeValue recoveryRangeSize;

    private final ConcurrentLinkedQueue<Integer> freeRegions = new ConcurrentLinkedQueue<>();
    private final Entry<CacheFileRegion>[] freqs;
    private final int maxFreq;
    private final long minTimeDelta;

    private final AtomicReference<CacheFileRegion>[] regionOwners; // to assert exclusive access of regions

    private final CacheDecayTask decayTask;

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public FrozenCacheService(NodeEnvironment environment, Settings settings, ThreadPool threadPool) {
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
        assert regionSize > 0L;
        this.maxFreq = SNAPSHOT_CACHE_MAX_FREQ_SETTING.get(settings);
        this.minTimeDelta = SNAPSHOT_CACHE_MIN_TIME_DELTA_SETTING.get(settings).millis();
        freqs = new Entry[maxFreq];
        try {
            sharedBytes = new SharedBytes(numRegions, regionSize, environment);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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

    public CacheFileRegion get(CacheKey cacheKey, long fileLength, int region) {
        final long regionSize = getRegionSize(fileLength, region);
        try (Releasable ignore = keyedLock.acquire(cacheKey)) {
            final RegionKey regionKey = new RegionKey(cacheKey, region);
            final long now = currentTimeSupplier.getAsLong();
            final Entry<CacheFileRegion> entry = keyMapping.computeIfAbsent(
                regionKey,
                key -> new Entry<>(new CacheFileRegion(regionKey, regionSize), now)
            );
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
                    synchronized (this) {
                        pushEntryToBack(entry);
                    }
                } else {
                    // need to evict something
                    synchronized (this) {
                        maybeEvict();
                    }
                    final Integer freeSlotRetry = freeRegions.poll();
                    if (freeSlotRetry != null) {
                        entry.chunk.sharedBytesPos = freeSlotRetry;
                        assert regionOwners[freeSlotRetry].compareAndSet(null, entry.chunk);
                        synchronized (this) {
                            pushEntryToBack(entry);
                        }
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

    public void onClose(CacheFileRegion chunk) {
        assert regionOwners[chunk.sharedBytesPos].compareAndSet(chunk, null);
        freeRegions.add(chunk.sharedBytesPos);
    }

    // used by tests
    int freeRegionCount() {
        return freeRegions.size();
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
            long now = currentTimeSupplier.getAsLong();
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
        volatile int sharedBytesPos = -1;

        CacheFileRegion(RegionKey regionKey, long regionSize) {
            super("CacheFileRegion");
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

        StepListener<Integer> populateAndRead(
            final ByteRange rangeToWrite,
            final ByteRange rangeToRead,
            final RangeAvailableHandler reader,
            final RangeMissingHandler writer,
            final Executor executor
        ) {
            assert rangeToRead.length() > 0;
            final StepListener<Integer> listener = new StepListener<>();
            Releasable decrementRef = null;
            try {
                ensureOpen();
                incRef();
                decrementRef = Releasables.releaseOnce(this::decRef);
                ensureOpen();
                Releasable finalDecrementRef = decrementRef;
                listener.whenComplete(integer -> finalDecrementRef.close(), throwable -> finalDecrementRef.close());
                final SharedBytes.IO fileChannel = sharedBytes.getFileChannel(sharedBytesPos);
                listener.whenComplete(integer -> fileChannel.decRef(), e -> fileChannel.decRef());
                final ActionListener<Void> rangeListener = rangeListener(rangeToRead, reader, listener, fileChannel);
                final List<SparseFileTracker.Gap> gaps = tracker.waitForRange(rangeToWrite, rangeToRead, rangeListener);

                for (SparseFileTracker.Gap gap : gaps) {
                    executor.execute(new AbstractRunnable() {

                        @Override
                        protected void doRun() throws Exception {
                            if (CacheFileRegion.this.tryIncRef() == false) {
                                // assert false : "expected a non-closed channel reference";
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
                releaseAndFail(listener, decrementRef, e);
            }
            return listener;
        }

        private ActionListener<Void> rangeListener(
            ByteRange rangeToRead,
            RangeAvailableHandler reader,
            ActionListener<Integer> listener,
            SharedBytes.IO fileChannel
        ) {
            return ActionListener.wrap(success -> {
                final long physicalStartOffset = physicalStartOffset();
                assert regionOwners[sharedBytesPos].get() == CacheFileRegion.this;
                final int read = reader.onRangeAvailable(
                    fileChannel,
                    physicalStartOffset + rangeToRead.start(),
                    rangeToRead.start(),
                    rangeToRead.length()
                );
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
        private final long length;

        private FrozenCacheFile(CacheKey cacheKey, long length) {
            this.cacheKey = cacheKey;
            this.length = length;
        }

        public long getLength() {
            return length;
        }

        public CacheKey getCacheKey() {
            return cacheKey;
        }

        public StepListener<Integer> populateAndRead(
            final ByteRange rangeToWrite,
            final ByteRange rangeToRead,
            final RangeAvailableHandler reader,
            final RangeMissingHandler writer,
            final Executor executor
        ) {
            StepListener<Integer> stepListener = null;
            final long writeStart = rangeToWrite.start();
            final long readStart = rangeToRead.start();
            for (int region = getRegion(rangeToWrite.start()); region <= getEndingRegion(rangeToWrite.end()); region++) {
                final ByteRange subRangeToWrite = mapSubRangeToRegion(rangeToWrite, region);
                final ByteRange subRangeToRead = mapSubRangeToRegion(rangeToRead, region);
                if (subRangeToRead.length() == 0L) {
                    // nothing to read, skip
                    if (stepListener == null) {
                        stepListener = new StepListener<>();
                        stepListener.onResponse(0);
                    }
                    continue;
                }
                final CacheFileRegion fileRegion = get(cacheKey, length, region);
                final long regionStart = getRegionStart(region);
                final long writeOffset = writeStart - regionStart;
                final long readOffset = readStart - regionStart;
                final StepListener<Integer> lis = fileRegion.populateAndRead(
                    subRangeToWrite,
                    subRangeToRead,
                    (channel, channelPos, relativePos, length) -> {
                        assert regionOwners[fileRegion.sharedBytesPos].get() == fileRegion;
                        assert channelPos >= fileRegion.physicalStartOffset() && channelPos + length <= fileRegion.physicalEndOffset();
                        return reader.onRangeAvailable(channel, channelPos, relativePos - readOffset, length);
                    },
                    (channel, channelPos, relativePos, length, progressUpdater) -> {
                        assert regionOwners[fileRegion.sharedBytesPos].get() == fileRegion;
                        assert channelPos >= fileRegion.physicalStartOffset() && channelPos + length <= fileRegion.physicalEndOffset();
                        writer.fillCacheRange(channel, channelPos, relativePos - writeOffset, length, progressUpdater);
                    },
                    executor
                );
                assert lis != null;
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
            return "FrozenCacheFile{" + "cacheKey=" + cacheKey + ", length=" + length + '}';
        }
    }

    public FrozenCacheFile getFrozenCacheFile(CacheKey cacheKey, long length) {
        return new FrozenCacheFile(cacheKey, length);
    }

    @FunctionalInterface
    public interface RangeAvailableHandler {
        // caller that wants to read from x should instead do a positional read from x + relativePos
        // caller should also only read up to length, further bytes will be offered by another call to this method
        int onRangeAvailable(SharedBytes.IO channel, long channelPos, long relativePos, long length) throws IOException;
    }

    @FunctionalInterface
    public interface RangeMissingHandler {
        void fillCacheRange(SharedBytes.IO channel, long channelPos, long relativePos, long length, Consumer<Long> progressUpdater)
            throws IOException;
    }
}
