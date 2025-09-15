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
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.blobcache.BlobCacheMetrics;
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
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ThrottledTaskRunner;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.monitor.fs.FsProbe;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.IntConsumer;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * A caching layer on a local node to minimize network roundtrips to the remote blob store.
 */
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

    public static final Setting<Integer> SHARED_CACHE_CONCURRENT_EVICTIONS_SETTING = Setting.intSetting(
        SHARED_CACHE_SETTINGS_PREFIX + "concurrent_evictions",
        5,
        1,
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

    public static final Setting<Boolean> SHARED_CACHE_MMAP = Setting.boolSetting(
        SHARED_CACHE_SETTINGS_PREFIX + "mmap",
        false,
        Setting.Property.NodeScope
    );

    public static final Setting<Boolean> SHARED_CACHE_COUNT_READS = Setting.boolSetting(
        SHARED_CACHE_SETTINGS_PREFIX + "count_reads",
        true,
        Setting.Property.NodeScope
    );

    // used in tests
    void computeDecay() {
        if (cache instanceof LFUCache lfuCache) {
            lfuCache.computeDecay();
        }
    }

    // used in tests
    void maybeScheduleDecayAndNewEpoch() {
        if (cache instanceof LFUCache lfuCache) {
            lfuCache.maybeScheduleDecayAndNewEpoch(lfuCache.epoch.get());
        }
    }

    // used in tests
    long epoch() {
        return ((LFUCache) cache).epoch.get();
    }

    private interface Cache<K, T> extends Releasable {
        CacheEntry<T> get(K cacheKey, long fileLength, int region);

        int forceEvict(Predicate<K> cacheKeyPredicate);

        void forceEvictAsync(Predicate<K> cacheKey);
    }

    private abstract static class CacheEntry<T> {
        final T chunk;

        private CacheEntry(T chunk) {
            this.chunk = chunk;
        }

        abstract void touch();
    }

    private static final Logger logger = LogManager.getLogger(SharedBlobCacheService.class);

    private final ThreadPool threadPool;

    // executor to run reading from the blobstore on
    private final Executor ioExecutor;

    private final SharedBytes sharedBytes;
    private final long cacheSize;
    private final int regionSize;
    private final int rangeSize;
    private final int recoveryRangeSize;

    private final int numRegions;
    private final ConcurrentLinkedQueue<SharedBytes.IO> freeRegions = new ConcurrentLinkedQueue<>();

    private final Cache<KeyType, CacheFileRegion<KeyType>> cache;

    private final ConcurrentHashMap<SharedBytes.IO, CacheFileRegion<KeyType>> regionOwners; // to assert exclusive access of regions

    private final LongAdder writeCount = new LongAdder();
    private final LongAdder writeBytes = new LongAdder();

    private final LongAdder readCount = new LongAdder();
    private final LongAdder readBytes = new LongAdder();

    private final LongAdder evictCount = new LongAdder();

    private final BlobCacheMetrics blobCacheMetrics;

    private final Runnable evictIncrementer;

    private final LongSupplier relativeTimeInNanosSupplier;
    private final ThrottledTaskRunner asyncEvictionsRunner;

    public SharedBlobCacheService(
        NodeEnvironment environment,
        Settings settings,
        ThreadPool threadPool,
        Executor ioExecutor,
        BlobCacheMetrics blobCacheMetrics
    ) {
        this(environment, settings, threadPool, ioExecutor, blobCacheMetrics, System::nanoTime);
    }

    public SharedBlobCacheService(
        NodeEnvironment environment,
        Settings settings,
        ThreadPool threadPool,
        Executor ioExecutor,
        BlobCacheMetrics blobCacheMetrics,
        LongSupplier relativeTimeInNanosSupplier
    ) {
        this.threadPool = threadPool;
        this.ioExecutor = ioExecutor;
        long totalFsSize;
        try {
            totalFsSize = FsProbe.getTotal(Environment.getFileStore(environment.nodeDataPaths()[0]));
        } catch (IOException e) {
            throw new IllegalStateException("unable to probe size of filesystem [" + environment.nodeDataPaths()[0] + "]");
        }
        this.cacheSize = calculateCacheSize(settings, totalFsSize);
        final int regionSize = Math.toIntExact(SHARED_CACHE_REGION_SIZE_SETTING.get(settings).getBytes());
        this.numRegions = Math.toIntExact(cacheSize / regionSize);
        if (Assertions.ENABLED) {
            regionOwners = new ConcurrentHashMap<>();
        } else {
            regionOwners = null;
        }
        this.regionSize = regionSize;
        assert regionSize > 0L;
        this.cache = new LFUCache(settings);
        try {
            sharedBytes = new SharedBytes(
                numRegions,
                regionSize,
                environment,
                writeBytes::add,
                SHARED_CACHE_COUNT_READS.get(settings) ? readBytes::add : ignored -> {},
                SHARED_CACHE_MMAP.get(settings)
            );
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        for (int i = 0; i < numRegions; i++) {
            freeRegions.add(sharedBytes.getFileChannel(i));
        }

        this.rangeSize = BlobCacheUtils.toIntBytes(SHARED_CACHE_RANGE_SIZE_SETTING.get(settings).getBytes());
        this.recoveryRangeSize = BlobCacheUtils.toIntBytes(SHARED_CACHE_RECOVERY_RANGE_SIZE_SETTING.get(settings).getBytes());

        this.blobCacheMetrics = blobCacheMetrics;
        this.evictIncrementer = blobCacheMetrics.getEvictedCountNonZeroFrequency()::increment;
        this.relativeTimeInNanosSupplier = relativeTimeInNanosSupplier;
        this.asyncEvictionsRunner = new ThrottledTaskRunner(
            "shared_blob_cache_evictions",
            SHARED_CACHE_CONCURRENT_EVICTIONS_SETTING.get(settings),
            threadPool.generic()
        );
    }

    public static long calculateCacheSize(Settings settings, long totalFsSize) {
        return SHARED_CACHE_SIZE_SETTING.get(settings)
            .calculateValue(ByteSizeValue.ofBytes(totalFsSize), SHARED_CACHE_SIZE_MAX_HEADROOM_SETTING.get(settings))
            .getBytes();
    }

    public BlobCacheMetrics getBlobCacheMetrics() {
        return blobCacheMetrics;
    }

    public int getRangeSize() {
        return rangeSize;
    }

    public int getRecoveryRangeSize() {
        return recoveryRangeSize;
    }

    protected int getRegion(long position) {
        return (int) (position / regionSize);
    }

    protected int getRegionRelativePosition(long position) {
        return (int) (position % regionSize);
    }

    protected long getRegionStart(int region) {
        return (long) region * regionSize;
    }

    protected long getRegionEnd(int region) {
        return (long) (region + 1) * regionSize;
    }

    protected int getEndingRegion(long position) {
        return getRegion(position - (position % regionSize == 0 ? 1 : 0));
    }

    protected ByteRange mapSubRangeToRegion(ByteRange range, int region) {
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

    /**
     * Compute the size of a cache file region.
     *
     * @param fileLength the length of the file/blob to cache
     * @param region the region number
     * @return a size in bytes of the cache file region
     */
    protected int computeCacheFileRegionSize(long fileLength, int region) {
        assert fileLength > 0;
        final int maxRegion = getEndingRegion(fileLength);
        assert region >= 0 && region <= maxRegion : region + " - " + maxRegion;
        final int effectiveRegionSize;
        if (region == maxRegion && (long) (region + 1) * regionSize != fileLength) {
            assert getRegionRelativePosition(fileLength) != 0L;
            effectiveRegionSize = getRegionRelativePosition(fileLength);
        } else {
            effectiveRegionSize = regionSize;
        }
        assert getRegionStart(region) + effectiveRegionSize <= fileLength;
        return effectiveRegionSize;
    }

    public int getRegionSize() {
        return regionSize;
    }

    CacheFileRegion<KeyType> get(KeyType cacheKey, long fileLength, int region) {
        return cache.get(cacheKey, fileLength, region).chunk;
    }

    /**
     * Fetch and cache the full blob for the given cache entry from the remote repository if there
     * are enough free pages in the cache to do so.
     * <p>
     * This method returns as soon as the download tasks are instantiated, but the tasks themselves
     * are run on the bulk executor.
     * <p>
     * If an exception is thrown from the writer then the cache entry being downloaded is freed
     * and unlinked
     *
     * @param cacheKey      the key to fetch data for
     * @param length        the length of the blob to fetch
     * @param writer        a writer that handles writing of newly downloaded data to the shared cache
     * @param fetchExecutor an executor to use for reading from the blob store
     * @param listener      listener that is called once all downloading has finished
     * @return {@code true} if there were enough free pages to start downloading the full entry
     */
    public boolean maybeFetchFullEntry(
        KeyType cacheKey,
        long length,
        RangeMissingHandler writer,
        Executor fetchExecutor,
        ActionListener<Void> listener
    ) {
        int finalRegion = getEndingRegion(length);
        // TODO freeRegionCount uses freeRegions.size() which is is NOT a constant-time operation. Can we do better?
        if (freeRegionCount() < finalRegion) {
            // Not enough room to download a full file without evicting existing data, so abort
            listener.onResponse(null);
            return false;
        }
        long regionLength = regionSize;
        try (RefCountingListener refCountingListener = new RefCountingListener(listener)) {
            for (int region = 0; region <= finalRegion; region++) {
                if (region == finalRegion) {
                    regionLength = length - getRegionStart(region);
                }
                ByteRange rangeToWrite = ByteRange.of(0, regionLength);
                if (rangeToWrite.isEmpty()) {
                    return true;
                }
                final ActionListener<Integer> regionListener = refCountingListener.acquire(ignored -> {});
                final CacheFileRegion<KeyType> entry;
                try {
                    entry = get(cacheKey, length, region);
                } catch (AlreadyClosedException e) {
                    // failed to grab a cache page because some other operation concurrently acquired some
                    regionListener.onResponse(0);
                    return false;
                }
                // set read range == write range so the listener completes only once all the bytes have been downloaded
                entry.populateAndRead(
                    rangeToWrite,
                    rangeToWrite,
                    (channel, pos, relativePos, len) -> Math.toIntExact(len),
                    writer,
                    fetchExecutor,
                    regionListener.delegateResponse((l, e) -> {
                        if (e instanceof AlreadyClosedException) {
                            l.onResponse(0);
                        } else {
                            l.onFailure(e);
                        }
                    })
                );
            }
        }
        return true;
    }

    /**
     * Fetch and write in cache a region of a blob if there are enough free pages in the cache to do so.
     * <p>
     * This method returns as soon as the download tasks are instantiated, but the tasks themselves
     * are run on the bulk executor.
     * <p>
     * If an exception is thrown from the writer then the cache entry being downloaded is freed
     * and unlinked
     *
     * @param cacheKey      the key to fetch data for
     * @param region        the region of the blob to fetch
     * @param blobLength    the length of the blob from which the region is fetched (used to compute the size of the ending region)
     * @param writer        a writer that handles writing of newly downloaded data to the shared cache
     * @param fetchExecutor an executor to use for reading from the blob store
     * @param listener      a listener that is completed with {@code true} if the current thread triggered the fetching of the region, in
     *                      which case the data is available in cache. The listener is completed with {@code false} in every other cases: if
     *                      the region to write is already available in cache, if the region is pending fetching via another thread or if
     *                      there is not enough free pages to fetch the region.
     */
    public void maybeFetchRegion(
        final KeyType cacheKey,
        final int region,
        final long blobLength,
        final RangeMissingHandler writer,
        final Executor fetchExecutor,
        final ActionListener<Boolean> listener
    ) {
        if (freeRegions.isEmpty() && maybeEvictLeastUsed() == false) {
            // no free page available and no old enough unused region to be evicted
            logger.info("No free regions, skipping loading region [{}]", region);
            listener.onResponse(false);
            return;
        }
        try {
            ByteRange regionRange = ByteRange.of(0, computeCacheFileRegionSize(blobLength, region));
            if (regionRange.isEmpty()) {
                listener.onResponse(false);
                return;
            }
            final CacheFileRegion<KeyType> entry = get(cacheKey, blobLength, region);
            entry.populate(regionRange, writer, fetchExecutor, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Fetch and write in cache a range within a blob region if there is at least a free page in the cache to do so.
     * <p>
     * This method returns as soon as the download tasks are instantiated, but the tasks themselves
     * are run on the bulk executor.
     * <p>
     * If an exception is thrown from the writer then the cache entry being downloaded is freed
     * and unlinked
     *
     * @param cacheKey      the key to fetch data for
     * @param region        the region of the blob
     * @param range         the range of the blob to fetch
     * @param blobLength    the length of the blob from which the region is fetched (used to compute the size of the ending region)
     * @param writer        a writer that handles writing of newly downloaded data to the shared cache
     * @param fetchExecutor an executor to use for reading from the blob store
     * @param listener      a listener that is completed with {@code true} if the current thread triggered the fetching of the range, in
     *                      which case the data is available in cache. The listener is completed with {@code false} in every other cases: if
     *                      the range to write is already available in cache, if the range is pending fetching via another thread or if
     *                      there is not enough free pages to fetch the range.
     */
    public void maybeFetchRange(
        final KeyType cacheKey,
        final int region,
        final ByteRange range,
        final long blobLength,
        final RangeMissingHandler writer,
        final Executor fetchExecutor,
        final ActionListener<Boolean> listener
    ) {
        if (freeRegions.isEmpty() && maybeEvictLeastUsed() == false) {
            // no free page available and no old enough unused region to be evicted
            logger.info("No free regions, skipping loading region [{}]", region);
            listener.onResponse(false);
            return;
        }
        try {
            var regionRange = mapSubRangeToRegion(range, region);
            if (regionRange.isEmpty()) {
                listener.onResponse(false);
                return;
            }
            final CacheFileRegion<KeyType> entry = get(cacheKey, blobLength, region);
            entry.populate(
                regionRange,
                writerWithOffset(writer, Math.toIntExact(range.start() - getRegionStart(region))),
                fetchExecutor,
                listener
            );
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private RangeMissingHandler writerWithOffset(RangeMissingHandler writer, int writeOffset) {
        if (writeOffset == 0) {
            // no need to allocate a new capturing lambda if the offset isn't adjusted
            return writer;
        }
        return (channel, channelPos, streamFactory, relativePos, len, progressUpdater, completionListener) -> writer.fillCacheRange(
            channel,
            channelPos,
            streamFactory,
            relativePos - writeOffset,
            len,
            progressUpdater,
            completionListener
        );
    }

    // used by tests
    boolean maybeEvictLeastUsed() {
        if (cache instanceof LFUCache lfuCache) {
            return lfuCache.maybeEvictLeastUsed();
        }
        return false;
    }

    private static void throwAlreadyClosed(String message) {
        throw new AlreadyClosedException(message);
    }

    /**
     * NOTE: Method is package private mostly to allow checking the number of fee regions in tests.
     * However, it is also used by {@link SharedBlobCacheService#maybeFetchFullEntry} but we should try
     * to move away from that because calling "size" on a ConcurrentLinkedQueue is not a constant time operation.
     */
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

    public void removeFromCache(KeyType cacheKey) {
        forceEvict(cacheKey::equals);
    }

    /**
     * Evicts entries from the cache that match the given predicate.
     *
     * @param cacheKeyPredicate
     * @return The number of entries evicted from the keyMapping.
     */
    public int forceEvict(Predicate<KeyType> cacheKeyPredicate) {
        return cache.forceEvict(cacheKeyPredicate);

    }

    /**
     * Evict entries from the cache that match the given predicate asynchronously
     *
     * @param cacheKeyPredicate
     */
    public void forceEvictAsync(Predicate<KeyType> cacheKeyPredicate) {
        cache.forceEvictAsync(cacheKeyPredicate);
    }

    // used by tests
    int getFreq(CacheFileRegion<KeyType> cacheFileRegion) {
        if (cache instanceof LFUCache lfuCache) {
            return lfuCache.getFreq(cacheFileRegion);
        }
        return -1;
    }

    @Override
    public void close() {
        sharedBytes.decRef();
    }

    private record RegionKey<KeyType>(KeyType file, int region) {
        @Override
        public String toString() {
            return "Chunk{" + "file=" + file + ", region=" + region + '}';
        }
    }

    /**
     * This class models a reference counted object that also tracks a flag for eviction of an instance.
     * It is only inherited by CacheFileRegion to enable the use of a static var handle in on a non-static inner class.
     * As long as the flag in {@link #evicted} is not set the instance's contents can be trusted. As soon as the flag is set, the contents
     * of the instance can not be trusted. Thus, each read operation from a file region should be followed by a call to {@link #isEvicted()}
     * to ensure that whatever bytes have been read are still valid.
     * The reference count is used by write operations to a region on top of the eviction flag. Every write operation must first increment
     * the reference count, then write to the region and then decrement it again. Only when the reference count reaches zero, will the
     * region by moved to the {@link #freeRegions} list and becomes available for allocation again.
     */
    private abstract static class EvictableRefCounted extends AbstractRefCounted {
        protected static final VarHandle VH_EVICTED_FIELD;

        static {
            try {
                VH_EVICTED_FIELD = MethodHandles.lookup()
                    .in(EvictableRefCounted.class)
                    .findVarHandle(EvictableRefCounted.class, "evicted", int.class);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }

        // If != 0 this file region has been evicted from the cache and should not be used anymore
        // implemented using a var handle instead of an atomic boolean to save space and indirection
        @SuppressWarnings("FieldMayBeFinal") // updated via VH_EVICTED_FIELD (and _only_ via VH_EVICTED_FIELD)
        private volatile int evicted = 0;

        /**
         * @return true if the instance was evicted by this invocation, false if it was already evicted
         */
        protected final boolean evict() {
            return VH_EVICTED_FIELD.compareAndSet(this, 0, 1);
        }

        /**
         * @return true if this instance has been evicted and its contents can not be trusted any longer
         */
        public final boolean isEvicted() {
            return evicted != 0;
        }
    }

    protected boolean assertOffsetsWithinFileLength(long offset, long length, long fileLength) {
        assert offset >= 0L;
        assert length > 0L;
        assert fileLength > 0L;
        assert offset + length <= fileLength
            : "accessing ["
                + length
                + "] bytes at offset ["
                + offset
                + "] in cache file ["
                + this
                + "] would be beyond file length ["
                + fileLength
                + ']';
        return true;
    }

    /**
     * While this class has incRef and tryIncRef methods, incRefEnsureOpen and tryIncrefEnsureOpen should
     * always be used, ensuring the right ordering between incRef/tryIncRef and ensureOpen
     * (see {@link SharedBlobCacheService.LFUCache#maybeEvictAndTakeForFrequency(Runnable, int)})
     */
    static class CacheFileRegion<KeyType> extends EvictableRefCounted {

        private static final VarHandle VH_IO = findIOVarHandle();

        private static VarHandle findIOVarHandle() {
            try {
                return MethodHandles.lookup().in(CacheFileRegion.class).findVarHandle(CacheFileRegion.class, "io", SharedBytes.IO.class);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }

        final SharedBlobCacheService<KeyType> blobCacheService;

        final RegionKey<KeyType> regionKey;
        final SparseFileTracker tracker;
        // io can be null when not init'ed or after evict/take
        // io does not need volatile access on the read path, since it goes from null to a single value (and then possbily back to null).
        // "cache.get" never returns a `CacheFileRegion` without checking the value is non-null (with a volatile read, ensuring the value is
        // visible in that thread).
        // We assume any IndexInput passing among threads is done with proper happens-before semantics (otherwise they'd themselves break).
        // In general, assertions should use `nonVolatileIO` (when they can) to access this over `volatileIO` to avoid memory visibility
        // side effects
        private SharedBytes.IO io = null;

        CacheFileRegion(SharedBlobCacheService<KeyType> blobCacheService, RegionKey<KeyType> regionKey, int regionSize) {
            this.blobCacheService = blobCacheService;
            this.regionKey = regionKey;
            assert regionSize > 0;
            // NOTE we use a constant string for description to avoid consume extra heap space
            tracker = new SparseFileTracker("file", regionSize);
        }

        // only used for logging
        private long physicalStartOffset() {
            var ioRef = nonVolatileIO();
            return ioRef == null ? -1L : (long) regionKey.region * blobCacheService.regionSize;
        }

        public boolean tryIncRefEnsureOpen() {
            if (tryIncRef()) {
                ensureOpenOrDecRef();
                return true;
            }

            return false;
        }

        public void incRefEnsureOpen() {
            incRef();
            ensureOpenOrDecRef();
        }

        private void ensureOpenOrDecRef() {
            if (isEvicted()) {
                decRef();
                throwAlreadyEvicted();
            }
        }

        // tries to evict this chunk if noone is holding onto its resources anymore
        // visible for tests.
        boolean tryEvict() {
            assert Thread.holdsLock(blobCacheService) : "must hold lock when evicting";
            if (refCount() <= 1 && evict()) {
                logger.trace("evicted {} with channel offset {}", regionKey, physicalStartOffset());
                blobCacheService.evictCount.increment();
                decRef();
                return true;
            }
            return false;
        }

        boolean tryEvictNoDecRef() {
            assert Thread.holdsLock(blobCacheService) : "must hold lock when evicting";
            if (refCount() <= 1 && evict()) {
                logger.trace("evicted and take {} with channel offset {}", regionKey, physicalStartOffset());
                blobCacheService.evictCount.increment();
                return true;
            }

            return false;
        }

        public boolean forceEvict() {
            assert Thread.holdsLock(blobCacheService) : "must hold lock when evicting";
            if (evict()) {
                logger.trace("force evicted {} with channel offset {}", regionKey, physicalStartOffset());
                blobCacheService.evictCount.increment();
                decRef();
                return true;
            }
            return false;
        }

        @Override
        protected void closeInternal() {
            // now actually free the region associated with this chunk
            // we held the "this" lock when this was evicted, hence if io is not filled in, chunk will never be registered.
            SharedBytes.IO io = volatileIO();
            if (io != null) {
                assert blobCacheService.regionOwners.remove(io) == this;
                blobCacheService.freeRegions.add(io);
            }
            logger.trace("closed {} with channel offset {}", regionKey, physicalStartOffset());
        }

        private static void throwAlreadyEvicted() {
            throwAlreadyClosed("File chunk is evicted");
        }

        private SharedBytes.IO volatileIO() {
            return (SharedBytes.IO) VH_IO.getVolatile(this);
        }

        private void volatileIO(SharedBytes.IO io) {
            VH_IO.setVolatile(this, io);
        }

        private SharedBytes.IO nonVolatileIO() {
            return io;
        }

        // for use in tests *only*
        SharedBytes.IO testOnlyNonVolatileIO() {
            return io;
        }

        /**
         * Optimistically try to read from the region
         * @return true if successful, i.e., not evicted and data available, false if evicted
         */
        boolean tryRead(ByteBuffer buf, long offset) throws IOException {
            SharedBytes.IO ioRef = nonVolatileIO();
            if (ioRef != null) {
                int readBytes = ioRef.read(buf, blobCacheService.getRegionRelativePosition(offset));
                if (isEvicted()) {
                    buf.position(buf.position() - readBytes);
                    return false;
                }
                return true;
            } else {
                // taken by someone else
                return false;
            }
        }

        /**
         * Populates a range in cache if the range is not available nor pending to be available in cache.
         *
         * @param rangeToWrite the range of bytes to populate
         * @param writer a writer that handles writing of newly downloaded data to the shared cache
         * @param executor the executor used to download and to write new dat
         * @param listener a listener that is completed with {@code true} if the current thread triggered the download and write of the
         *                 range, in which case the listener is completed once writing is done. The listener is completed with {@code false}
         *                 if the range to write is already available in cache or if another thread will download and write the range, in
         *                 which cases the listener is completed immediately.
         */
        void populate(
            final ByteRange rangeToWrite,
            final RangeMissingHandler writer,
            final Executor executor,
            final ActionListener<Boolean> listener
        ) {
            try {
                incRefEnsureOpen();
                try (RefCountingRunnable refs = new RefCountingRunnable(CacheFileRegion.this::decRef)) {
                    final List<SparseFileTracker.Gap> gaps = tracker.waitForRange(
                        rangeToWrite,
                        rangeToWrite,
                        Assertions.ENABLED ? ActionListener.releaseAfter(ActionListener.running(() -> {
                            assert blobCacheService.regionOwners.get(nonVolatileIO()) == this;
                        }), refs.acquire()) : refs.acquireListener()
                    );
                    if (gaps.isEmpty()) {
                        listener.onResponse(false);
                        return;
                    }
                    final SourceInputStreamFactory streamFactory = writer.sharedInputStreamFactory(gaps);
                    logger.trace(
                        () -> Strings.format(
                            "fill gaps %s %s shared input stream factory",
                            gaps,
                            streamFactory == null ? "without" : "with"
                        )
                    );
                    if (streamFactory == null) {
                        try (var parallelGapsListener = new RefCountingListener(listener.map(unused -> true))) {
                            for (SparseFileTracker.Gap gap : gaps) {
                                executor.execute(
                                    fillGapRunnable(
                                        gap,
                                        writer,
                                        null,
                                        ActionListener.releaseAfter(parallelGapsListener.acquire(), refs.acquire())
                                    )
                                );
                            }
                        }
                    } else {
                        try (
                            var sequentialGapsListener = new RefCountingListener(
                                ActionListener.runBefore(listener.map(unused -> true), streamFactory::close)
                            )
                        ) {
                            final List<Runnable> gapFillingTasks = gaps.stream()
                                .map(
                                    gap -> fillGapRunnable(
                                        gap,
                                        writer,
                                        streamFactory,
                                        ActionListener.releaseAfter(sequentialGapsListener.acquire(), refs.acquire())
                                    )
                                )
                                .toList();
                            executor.execute(() -> {
                                // Fill the gaps in order. If a gap fails to fill for whatever reason, the task for filling the next
                                // gap will still be executed.
                                gapFillingTasks.forEach(Runnable::run);
                            });
                        }
                    }
                }
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }

        void populateAndRead(
            final ByteRange rangeToWrite,
            final ByteRange rangeToRead,
            final RangeAvailableHandler reader,
            final RangeMissingHandler writer,
            final Executor executor,
            final ActionListener<Integer> listener
        ) {
            try {
                incRefEnsureOpen();
                try (RefCountingRunnable refs = new RefCountingRunnable(CacheFileRegion.this::decRef)) {
                    final List<SparseFileTracker.Gap> gaps = tracker.waitForRange(
                        rangeToWrite,
                        rangeToRead,
                        ActionListener.releaseAfter(listener, refs.acquire()).delegateFailureAndWrap((l, success) -> {
                            var ioRef = nonVolatileIO();
                            assert blobCacheService.regionOwners.get(ioRef) == this;
                            final int start = Math.toIntExact(rangeToRead.start());
                            final int read = reader.onRangeAvailable(ioRef, start, start, Math.toIntExact(rangeToRead.length()));
                            assert read == rangeToRead.length()
                                : "partial read ["
                                    + read
                                    + "] does not match the range to read ["
                                    + rangeToRead.end()
                                    + '-'
                                    + rangeToRead.start()
                                    + ']';
                            blobCacheService.readCount.increment();
                            l.onResponse(read);
                        })
                    );

                    if (gaps.isEmpty() == false) {
                        final SourceInputStreamFactory streamFactory = writer.sharedInputStreamFactory(gaps);
                        logger.trace(
                            () -> Strings.format(
                                "fill gaps %s %s shared input stream factory",
                                gaps,
                                streamFactory == null ? "without" : "with"
                            )
                        );
                        if (streamFactory == null) {
                            for (SparseFileTracker.Gap gap : gaps) {
                                executor.execute(fillGapRunnable(gap, writer, null, refs.acquireListener()));
                            }
                        } else {
                            var gapFillingListener = refs.acquireListener();
                            try (var gfRefs = new RefCountingRunnable(ActionRunnable.run(gapFillingListener, streamFactory::close))) {
                                final List<Runnable> gapFillingTasks = gaps.stream()
                                    .map(gap -> fillGapRunnable(gap, writer, streamFactory, gfRefs.acquireListener()))
                                    .toList();
                                executor.execute(() -> {
                                    // Fill the gaps in order. If a gap fails to fill for whatever reason, the task for filling the next
                                    // gap will still be executed.
                                    gapFillingTasks.forEach(Runnable::run);
                                });
                            }
                        }
                    }
                }
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }

        private Runnable fillGapRunnable(
            SparseFileTracker.Gap gap,
            RangeMissingHandler writer,
            @Nullable SourceInputStreamFactory streamFactory,
            ActionListener<Void> listener
        ) {
            return () -> ActionListener.run(listener, l -> {
                var ioRef = nonVolatileIO();
                assert blobCacheService.regionOwners.get(ioRef) == CacheFileRegion.this;
                assert CacheFileRegion.this.hasReferences() : CacheFileRegion.this;
                int start = Math.toIntExact(gap.start());
                writer.fillCacheRange(
                    ioRef,
                    start,
                    streamFactory,
                    start,
                    Math.toIntExact(gap.end() - start),
                    progress -> gap.onProgress(start + progress),
                    l.<Void>map(unused -> {
                        assert blobCacheService.regionOwners.get(ioRef) == CacheFileRegion.this;
                        assert CacheFileRegion.this.hasReferences() : CacheFileRegion.this;
                        blobCacheService.writeCount.increment();
                        gap.onCompletion();
                        return null;
                    }).delegateResponse((delegate, e) -> failGapAndListener(gap, delegate, e))
                );
            });
        }

        private static void failGapAndListener(SparseFileTracker.Gap gap, ActionListener<?> listener, Exception e) {
            try {
                gap.onFailure(e);
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

        private CacheEntry<CacheFileRegion<KeyType>> lastAccessedRegion;

        private CacheFile(KeyType cacheKey, long length) {
            this.cacheKey = cacheKey;
            this.length = length;
        }

        public CacheFile copy() {
            return new CacheFile(cacheKey, length);
        }

        public long getLength() {
            return length;
        }

        public KeyType getCacheKey() {
            return cacheKey;
        }

        public boolean tryRead(ByteBuffer buf, long offset) throws IOException {
            assert assertOffsetsWithinFileLength(offset, buf.remaining(), length);
            final int startRegion = getRegion(offset);
            final long end = offset + buf.remaining();
            final int endRegion = getEndingRegion(end);
            if (startRegion != endRegion) {
                return false;
            }
            var fileRegion = lastAccessedRegion;
            if (fileRegion != null && fileRegion.chunk.regionKey.region == startRegion) {
                // existing item, check if we need to promote item
                fileRegion.touch();

            } else {
                fileRegion = cache.get(cacheKey, length, startRegion);
            }
            final var region = fileRegion.chunk;
            if (region.tracker.checkAvailable(end - getRegionStart(startRegion)) == false) {
                return false;
            }
            boolean res = region.tryRead(buf, offset);
            lastAccessedRegion = res ? fileRegion : null;
            return res;
        }

        public int populateAndRead(
            final ByteRange rangeToWrite,
            final ByteRange rangeToRead,
            final RangeAvailableHandler reader,
            final RangeMissingHandler writer
        ) throws Exception {
            // some cache files can grow after being created, so rangeToWrite can be larger than the initial {@code length}
            assert rangeToWrite.start() >= 0 : rangeToWrite;
            assert assertOffsetsWithinFileLength(rangeToRead.start(), rangeToRead.length(), length);
            // We are interested in the total time that the system spends when fetching a result (including time spent queuing), so we start
            // our measurement here.
            final long startTime = relativeTimeInNanosSupplier.getAsLong();
            RangeMissingHandler writerInstrumentationDecorator = new DelegatingRangeMissingHandler(writer) {
                @Override
                public void fillCacheRange(
                    SharedBytes.IO channel,
                    int channelPos,
                    SourceInputStreamFactory streamFactory,
                    int relativePos,
                    int length,
                    IntConsumer progressUpdater,
                    ActionListener<Void> completionListener
                ) throws IOException {
                    writer.fillCacheRange(
                        channel,
                        channelPos,
                        streamFactory,
                        relativePos,
                        length,
                        progressUpdater,
                        completionListener.map(unused -> {
                            var elapsedTime = TimeUnit.NANOSECONDS.toMillis(relativeTimeInNanosSupplier.getAsLong() - startTime);
                            blobCacheMetrics.getCacheMissLoadTimes().record(elapsedTime);
                            blobCacheMetrics.getCacheMissCounter().increment();
                            return null;
                        })
                    );
                }
            };
            if (rangeToRead.isEmpty()) {
                // nothing to read, skip
                return 0;
            }
            final int startRegion = getRegion(rangeToWrite.start());
            final int endRegion = getEndingRegion(rangeToWrite.end());
            if (startRegion == endRegion) {
                return readSingleRegion(rangeToWrite, rangeToRead, reader, writerInstrumentationDecorator, startRegion);
            }
            return readMultiRegions(rangeToWrite, rangeToRead, reader, writerInstrumentationDecorator, startRegion, endRegion);
        }

        private int readSingleRegion(
            ByteRange rangeToWrite,
            ByteRange rangeToRead,
            RangeAvailableHandler reader,
            RangeMissingHandler writer,
            int region
        ) throws InterruptedException, ExecutionException {
            final PlainActionFuture<Integer> readFuture = new PlainActionFuture<>();
            final CacheFileRegion<KeyType> fileRegion = get(cacheKey, length, region);
            final long regionStart = getRegionStart(region);
            fileRegion.populateAndRead(
                mapSubRangeToRegion(rangeToWrite, region),
                mapSubRangeToRegion(rangeToRead, region),
                readerWithOffset(reader, fileRegion, Math.toIntExact(rangeToRead.start() - regionStart)),
                writerWithOffset(writer, fileRegion, Math.toIntExact(rangeToWrite.start() - regionStart)),
                ioExecutor,
                readFuture
            );
            return readFuture.get();
        }

        private int readMultiRegions(
            ByteRange rangeToWrite,
            ByteRange rangeToRead,
            RangeAvailableHandler reader,
            RangeMissingHandler writer,
            int startRegion,
            int endRegion
        ) throws InterruptedException, ExecutionException {
            final PlainActionFuture<Void> readsComplete = new PlainActionFuture<>();
            final AtomicInteger bytesRead = new AtomicInteger();
            try (var listeners = new RefCountingListener(1, readsComplete)) {
                for (int region = startRegion; region <= endRegion; region++) {
                    final ByteRange subRangeToRead = mapSubRangeToRegion(rangeToRead, region);
                    if (subRangeToRead.isEmpty()) {
                        // nothing to read, skip
                        continue;
                    }
                    ActionListener<Integer> listener = listeners.acquire(i -> bytesRead.updateAndGet(j -> Math.addExact(i, j)));
                    try {
                        final CacheFileRegion<KeyType> fileRegion = get(cacheKey, length, region);
                        final long regionStart = getRegionStart(region);
                        fileRegion.populateAndRead(
                            mapSubRangeToRegion(rangeToWrite, region),
                            subRangeToRead,
                            readerWithOffset(reader, fileRegion, Math.toIntExact(rangeToRead.start() - regionStart)),
                            writerWithOffset(writer, fileRegion, Math.toIntExact(rangeToWrite.start() - regionStart)),
                            ioExecutor,
                            listener
                        );
                    } catch (Exception e) {
                        assert e instanceof AlreadyClosedException : e;
                        listener.onFailure(e);
                    }
                }
            }
            readsComplete.get();
            return bytesRead.get();
        }

        private RangeMissingHandler writerWithOffset(RangeMissingHandler writer, CacheFileRegion<KeyType> fileRegion, int writeOffset) {
            final RangeMissingHandler adjustedWriter;
            if (writeOffset == 0) {
                // no need to allocate a new capturing lambda if the offset isn't adjusted
                adjustedWriter = writer;
            } else {
                adjustedWriter = new DelegatingRangeMissingHandler(writer) {
                    @Override
                    public void fillCacheRange(
                        SharedBytes.IO channel,
                        int channelPos,
                        SourceInputStreamFactory streamFactory,
                        int relativePos,
                        int len,
                        IntConsumer progressUpdater,
                        ActionListener<Void> completionListener
                    ) throws IOException {
                        delegate.fillCacheRange(
                            channel,
                            channelPos,
                            streamFactory,
                            relativePos - writeOffset,
                            len,
                            progressUpdater,
                            completionListener
                        );
                    }
                };
            }
            if (Assertions.ENABLED) {
                return new DelegatingRangeMissingHandler(adjustedWriter) {
                    @Override
                    public void fillCacheRange(
                        SharedBytes.IO channel,
                        int channelPos,
                        SourceInputStreamFactory streamFactory,
                        int relativePos,
                        int len,
                        IntConsumer progressUpdater,
                        ActionListener<Void> completionListener
                    ) throws IOException {
                        assert assertValidRegionAndLength(fileRegion, channelPos, len);
                        delegate.fillCacheRange(
                            channel,
                            channelPos,
                            streamFactory,
                            relativePos,
                            len,
                            progressUpdater,
                            Assertions.ENABLED ? ActionListener.runBefore(completionListener, () -> {
                                assert regionOwners.get(fileRegion.nonVolatileIO()) == fileRegion
                                    : "File chunk [" + fileRegion.regionKey + "] no longer owns IO [" + fileRegion.nonVolatileIO() + "]";
                            }) : completionListener
                        );
                    }
                };

            }
            return adjustedWriter;
        }

        private RangeAvailableHandler readerWithOffset(RangeAvailableHandler reader, CacheFileRegion<KeyType> fileRegion, int readOffset) {
            final RangeAvailableHandler adjustedReader = (channel, channelPos, relativePos, len) -> reader.onRangeAvailable(
                channel,
                channelPos,
                relativePos - readOffset,
                len
            );
            if (Assertions.ENABLED) {
                return (channel, channelPos, relativePos, len) -> {
                    assert assertValidRegionAndLength(fileRegion, channelPos, len);
                    final int bytesRead = adjustedReader.onRangeAvailable(channel, channelPos, relativePos, len);
                    assert regionOwners.get(fileRegion.nonVolatileIO()) == fileRegion
                        : "File chunk [" + fileRegion.regionKey + "] no longer owns IO [" + fileRegion.nonVolatileIO() + "]";
                    return bytesRead;
                };
            }
            return adjustedReader;
        }

        private boolean assertValidRegionAndLength(CacheFileRegion<KeyType> fileRegion, int channelPos, int len) {
            assert fileRegion.nonVolatileIO() != null;
            assert fileRegion.hasReferences();
            assert regionOwners.get(fileRegion.nonVolatileIO()) == fileRegion;
            assert channelPos >= 0 && channelPos + len <= regionSize;
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
        /**
         * Callback method used to read data from the cache. The target is typically captured by the callback implementation.
         *
         * A caller should only read up to length, further bytes will be offered by another call to this method
         *
         * @param channel is the cache region to read from
         * @param channelPos a position in the channel (cache file) to read from
         * @param relativePos a position in the target buffer to store bytes and pass to the caller
         * @param length of the blob that can be read (must not be exceeded)
         * @return number of bytes read
         * @throws IOException on failure
         */
        int onRangeAvailable(SharedBytes.IO channel, int channelPos, int relativePos, int length) throws IOException;
    }

    @FunctionalInterface
    public interface RangeMissingHandler {
        /**
         * Attempt to get a shared {@link SourceInputStreamFactory} for the given list of Gaps so that all of them
         * can be filled from the input stream created from the factory. If a factory is returned, the gaps must be
         * filled sequentially by calling {@link #fillCacheRange} in order with the factory. If {@code null} is returned,
         * each invocation of {@link #fillCacheRange} creates its own input stream and can therefore be executed in parallel.
         * @param gaps The list of gaps to be filled by fetching from source storage and writing into the cache.
         * @return A factory object to be shared by all gaps filling process, or {@code null} if each gap filling should create
         * its own input stream.
         */
        @Nullable
        default SourceInputStreamFactory sharedInputStreamFactory(List<SparseFileTracker.Gap> gaps) {
            return null;
        }

        /**
         * Callback method used to fetch data (usually from a remote storage) and write it in the cache.
         *
         * @param channel is the cache region to write to
         * @param channelPos a position in the channel (cache file) to write to
         * @param streamFactory factory to get the input stream positioned at the given value for the remote storage.
         *                      This is useful for sharing the same stream across multiple calls to this method.
         *                      If it is {@code null}, the method should open input stream on its own.
         * @param relativePos the relative position in the remote storage to read from
         * @param length of data to fetch
         * @param progressUpdater consumer to invoke with the number of copied bytes as they are written in cache.
         *                        This is used to notify waiting readers that data become available in cache.
         * @param completionListener listener that has to be called when the callback method completes
         */
        void fillCacheRange(
            SharedBytes.IO channel,
            int channelPos,
            @Nullable SourceInputStreamFactory streamFactory,
            int relativePos,
            int length,
            IntConsumer progressUpdater,
            ActionListener<Void> completionListener
        ) throws IOException;
    }

    /**
     * Factory to create the input stream for reading data from the remote storage as the source for filling local cache regions.
     */
    public interface SourceInputStreamFactory extends Releasable {

        /**
         * Create the input stream at the specified position.
         * @param relativePos the relative position in the remote storage to read from.
         * @param listener listener for the input stream ready to be read from.
         */
        void create(int relativePos, ActionListener<InputStream> listener) throws IOException;
    }

    private abstract static class DelegatingRangeMissingHandler implements RangeMissingHandler {
        protected final RangeMissingHandler delegate;

        protected DelegatingRangeMissingHandler(RangeMissingHandler delegate) {
            this.delegate = delegate;
        }

        @Override
        public SourceInputStreamFactory sharedInputStreamFactory(List<SparseFileTracker.Gap> gaps) {
            return delegate.sharedInputStreamFactory(gaps);
        }

        @Override
        public void fillCacheRange(
            SharedBytes.IO channel,
            int channelPos,
            SourceInputStreamFactory streamFactory,
            int relativePos,
            int length,
            IntConsumer progressUpdater,
            ActionListener<Void> completionListener
        ) throws IOException {
            delegate.fillCacheRange(channel, channelPos, streamFactory, relativePos, length, progressUpdater, completionListener);
        }
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

    private class LFUCache implements Cache<KeyType, CacheFileRegion<KeyType>> {

        class LFUCacheEntry extends CacheEntry<CacheFileRegion<KeyType>> {
            LFUCacheEntry prev;
            LFUCacheEntry next;
            int freq;
            volatile long lastAccessedEpoch;

            LFUCacheEntry(CacheFileRegion<KeyType> chunk, long lastAccessed) {
                super(chunk);
                this.lastAccessedEpoch = lastAccessed;
                // todo: consider whether freq=1 is still right for new entries.
                // it could risk decaying to level 0 right after and thus potentially be evicted
                // if the freq 1 LRU chain was short.
                // seems ok for now, since if it were to get evicted soon, the decays done would ensure we have more level 1
                // entries eventually and thus such an entry would (after some decays) be able to survive in the cache.
                this.freq = 1;
            }

            void touch() {
                long now = epoch.get();
                if (now > lastAccessedEpoch) {
                    maybePromote(now, this);
                }
            }
        }

        private final ConcurrentHashMap<RegionKey<KeyType>, LFUCacheEntry> keyMapping = new ConcurrentHashMap<>();
        private final LFUCacheEntry[] freqs;
        private final int maxFreq;
        private final DecayAndNewEpochTask decayAndNewEpochTask;

        private final AtomicLong epoch = new AtomicLong();

        @SuppressWarnings("unchecked")
        LFUCache(Settings settings) {
            this.maxFreq = SHARED_CACHE_MAX_FREQ_SETTING.get(settings);
            freqs = (LFUCacheEntry[]) Array.newInstance(LFUCacheEntry.class, maxFreq);
            decayAndNewEpochTask = new DecayAndNewEpochTask(threadPool.generic());
        }

        @Override
        public void close() {
            decayAndNewEpochTask.close();
        }

        int getFreq(CacheFileRegion<KeyType> cacheFileRegion) {
            return keyMapping.get(cacheFileRegion.regionKey).freq;
        }

        @Override
        public LFUCacheEntry get(KeyType cacheKey, long fileLength, int region) {
            final RegionKey<KeyType> regionKey = new RegionKey<>(cacheKey, region);
            final long now = epoch.get();
            // try to just get from the map on the fast-path to save instantiating the capturing lambda needed on the slow path
            // if we did not find an entry
            var entry = keyMapping.get(regionKey);
            if (entry == null) {
                final int effectiveRegionSize = computeCacheFileRegionSize(fileLength, region);
                entry = keyMapping.computeIfAbsent(
                    regionKey,
                    key -> new LFUCacheEntry(new CacheFileRegion<KeyType>(SharedBlobCacheService.this, key, effectiveRegionSize), now)
                );
            }
            // checks using volatile, double locking is fine, as long as we assign io last.
            if (entry.chunk.volatileIO() == null) {
                synchronized (entry.chunk) {
                    if (entry.chunk.volatileIO() == null && entry.chunk.isEvicted() == false) {
                        return initChunk(entry);
                    }
                }
            }
            assert assertChunkActiveOrEvicted(entry);

            // existing item, check if we need to promote item
            if (now > entry.lastAccessedEpoch) {
                maybePromote(now, entry);
            }

            return entry;
        }

        @Override
        public int forceEvict(Predicate<KeyType> cacheKeyPredicate) {
            final List<LFUCacheEntry> matchingEntries = new ArrayList<>();
            keyMapping.forEach((key, value) -> {
                if (cacheKeyPredicate.test(key.file)) {
                    matchingEntries.add(value);
                }
            });
            var evictedCount = 0;
            var nonZeroFrequencyEvictedCount = 0;
            if (matchingEntries.isEmpty() == false) {
                synchronized (SharedBlobCacheService.this) {
                    for (LFUCacheEntry entry : matchingEntries) {
                        int frequency = entry.freq;
                        boolean evicted = entry.chunk.forceEvict();
                        if (evicted && entry.chunk.volatileIO() != null) {
                            unlink(entry);
                            keyMapping.remove(entry.chunk.regionKey, entry);
                            evictedCount++;
                            if (frequency > 0) {
                                nonZeroFrequencyEvictedCount++;
                            }
                        }
                    }
                }
            }
            blobCacheMetrics.getEvictedCountNonZeroFrequency().incrementBy(nonZeroFrequencyEvictedCount);
            return evictedCount;
        }

        @Override
        public void forceEvictAsync(Predicate<KeyType> cacheKeyPredicate) {
            asyncEvictionsRunner.enqueueTask(new ActionListener<>() {
                @Override
                public void onResponse(Releasable releasable) {
                    try (releasable) {
                        forceEvict(cacheKeyPredicate);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    // should be impossible, GENERIC pool doesn't reject anything
                    final String message = "unexpected failure evicting from shared blob cache";
                    logger.error(message, e);
                    assert false : new AssertionError(message, e);
                }
            });
        }

        private LFUCacheEntry initChunk(LFUCacheEntry entry) {
            assert Thread.holdsLock(entry.chunk);
            RegionKey<KeyType> regionKey = entry.chunk.regionKey;
            if (keyMapping.get(regionKey) != entry) {
                throwAlreadyClosed("no free region found (contender)");
            }
            // new item
            assert entry.freq == 1;
            assert entry.prev == null;
            assert entry.next == null;
            final SharedBytes.IO freeSlot = freeRegions.poll();
            if (freeSlot != null) {
                // no need to evict an item, just add
                assignToSlot(entry, freeSlot);
            } else {
                // need to evict something
                SharedBytes.IO io;
                synchronized (SharedBlobCacheService.this) {
                    io = maybeEvictAndTake(evictIncrementer);
                }
                if (io == null) {
                    io = freeRegions.poll();
                }
                if (io != null) {
                    assignToSlot(entry, io);
                } else {
                    boolean removed = keyMapping.remove(regionKey, entry);
                    assert removed;
                    throwAlreadyClosed("no free region found");
                }
            }

            return entry;
        }

        private void assignToSlot(LFUCacheEntry entry, SharedBytes.IO freeSlot) {
            assert regionOwners.put(freeSlot, entry.chunk) == null;
            synchronized (SharedBlobCacheService.this) {
                if (entry.chunk.isEvicted()) {
                    assert regionOwners.remove(freeSlot) == entry.chunk;
                    freeRegions.add(freeSlot);
                    keyMapping.remove(entry.chunk.regionKey, entry);
                    throwAlreadyClosed("evicted during free region allocation");
                }
                pushEntryToBack(entry);
                // assign io only when chunk is ready for use. Under lock to avoid concurrent tryEvict.
                entry.chunk.volatileIO(freeSlot);
            }
        }

        private void pushEntryToBack(final LFUCacheEntry entry) {
            assert Thread.holdsLock(SharedBlobCacheService.this);
            assert invariant(entry, false);
            assert entry.prev == null;
            assert entry.next == null;
            final LFUCacheEntry currFront = freqs[entry.freq];
            if (currFront == null) {
                freqs[entry.freq] = entry;
                entry.prev = entry;
                entry.next = null;
            } else {
                assert currFront.freq == entry.freq;
                final LFUCacheEntry last = currFront.prev;
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

        private synchronized boolean invariant(final LFUCacheEntry e, boolean present) {
            boolean found = false;
            for (int i = 0; i < maxFreq; i++) {
                assert freqs[i] == null || freqs[i].prev != null;
                assert freqs[i] == null || freqs[i].prev != freqs[i] || freqs[i].next == null;
                assert freqs[i] == null || freqs[i].prev.next == null;
                for (LFUCacheEntry entry = freqs[i]; entry != null; entry = entry.next) {
                    assert entry.next == null || entry.next.prev == entry;
                    assert entry.prev != null;
                    assert entry.prev.next == null || entry.prev.next == entry;
                    assert entry.freq == i;
                    if (entry == e) {
                        found = true;
                    }
                }
                for (LFUCacheEntry entry = freqs[i]; entry != null && entry.prev != freqs[i]; entry = entry.prev) {
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

        private boolean assertChunkActiveOrEvicted(LFUCacheEntry entry) {
            synchronized (SharedBlobCacheService.this) {
                // assert linked (or evicted)
                assert entry.prev != null || entry.chunk.isEvicted();

            }
            SharedBytes.IO io = entry.chunk.nonVolatileIO();
            assert io != null || entry.chunk.isEvicted();
            assert io == null || regionOwners.get(io) == entry.chunk || entry.chunk.isEvicted();
            return true;
        }

        private void maybePromote(long epoch, LFUCacheEntry entry) {
            synchronized (SharedBlobCacheService.this) {
                if (epoch > entry.lastAccessedEpoch && entry.freq < maxFreq - 1 && entry.chunk.isEvicted() == false) {
                    unlink(entry);
                    // go 2 up per epoch, allowing us to decay 1 every epoch.
                    entry.freq = Math.min(entry.freq + 2, maxFreq - 1);
                    entry.lastAccessedEpoch = epoch;
                    pushEntryToBack(entry);
                }
            }
        }

        private void unlink(final LFUCacheEntry entry) {
            assert Thread.holdsLock(SharedBlobCacheService.this);
            assert invariant(entry, true);
            assert entry.prev != null;
            final LFUCacheEntry currFront = freqs[entry.freq];
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

        private void appendLevel1ToLevel0() {
            assert Thread.holdsLock(SharedBlobCacheService.this);
            var front0 = freqs[0];
            var front1 = freqs[1];
            if (front0 == null) {
                freqs[0] = front1;
                freqs[1] = null;
                decrementFreqList(front1);
                assert front1 == null || invariant(front1, true);
            } else if (front1 != null) {
                var back0 = front0.prev;
                var back1 = front1.prev;
                assert invariant(front0, true);
                assert invariant(front1, true);
                assert invariant(back0, true);
                assert invariant(back1, true);

                decrementFreqList(front1);

                front0.prev = back1;
                back0.next = front1;
                front1.prev = back0;
                assert back1.next == null;

                freqs[1] = null;

                assert invariant(front0, true);
                assert invariant(front1, true);
                assert invariant(back0, true);
                assert invariant(back1, true);
            }
        }

        private void decrementFreqList(LFUCacheEntry entry) {
            while (entry != null) {
                entry.freq--;
                entry = entry.next;
            }
        }

        /**
         * Cycles through the {@link LFUCacheEntry} from 0 to max frequency and
         * tries to evict a chunk if no one is holding onto its resources anymore.
         *
         * Also regularly polls for free regions and thus might steal one in case any become available.
         *
         * @return a now free IO region or null if none available.
         */
        private SharedBytes.IO maybeEvictAndTake(Runnable evictedNotification) {
            assert Thread.holdsLock(SharedBlobCacheService.this);
            long currentEpoch = epoch.get(); // must be captured before attempting to evict a freq 0
            SharedBytes.IO freq0 = maybeEvictAndTakeForFrequency(evictedNotification, 0);
            if (freqs[0] == null) {
                // no frequency 0 entries, let us switch epoch and decay so we get some for next time.
                maybeScheduleDecayAndNewEpoch(currentEpoch);
            }
            if (freq0 != null) {
                return freq0;
            }
            for (int currentFreq = 1; currentFreq < maxFreq; currentFreq++) {
                // recheck this per freq in case we raced an eviction with an incref'er.
                SharedBytes.IO freeRegion = freeRegions.poll();
                if (freeRegion != null) {
                    return freeRegion;
                }
                SharedBytes.IO taken = maybeEvictAndTakeForFrequency(evictedNotification, currentFreq);
                if (taken != null) {
                    return taken;
                }
            }
            // give up
            return null;
        }

        private SharedBytes.IO maybeEvictAndTakeForFrequency(Runnable evictedNotification, int currentFreq) {
            for (LFUCacheEntry entry = freqs[currentFreq]; entry != null; entry = entry.next) {
                boolean evicted = entry.chunk.tryEvictNoDecRef();
                if (evicted) {
                    try {
                        SharedBytes.IO ioRef = entry.chunk.volatileIO();
                        if (ioRef != null) {
                            try {
                                if (entry.chunk.refCount() == 1) {
                                    // we own that one refcount (since we CAS'ed evicted to 1)
                                    // grab io, rely on incref'ers also checking evicted field.
                                    entry.chunk.volatileIO(null);
                                    assert regionOwners.remove(ioRef) == entry.chunk;
                                    return ioRef;
                                }
                            } finally {
                                unlink(entry);
                                keyMapping.remove(entry.chunk.regionKey, entry);
                            }
                        }
                    } finally {
                        entry.chunk.decRef();
                        if (currentFreq > 0) {
                            evictedNotification.run();
                        }
                    }
                }
            }
            return null;
        }

        /**
         * Check if a new epoch is needed based on the input. The input epoch should be captured
         * before the determination that a new epoch is needed is done.
         * @param currentEpoch the epoch to check against if a new epoch is needed
         */
        private void maybeScheduleDecayAndNewEpoch(long currentEpoch) {
            decayAndNewEpochTask.spawnIfNotRunning(currentEpoch);
        }

        /**
         * This method tries to evict the least used {@link LFUCacheEntry}. Only entries with the lowest possible frequency are considered
         * for eviction.
         *
         * @return true if an entry was evicted, false otherwise.
         */
        public boolean maybeEvictLeastUsed() {
            synchronized (SharedBlobCacheService.this) {
                for (LFUCacheEntry entry = freqs[0]; entry != null; entry = entry.next) {
                    boolean evicted = entry.chunk.tryEvict();
                    if (evicted && entry.chunk.volatileIO() != null) {
                        unlink(entry);
                        keyMapping.remove(entry.chunk.regionKey, entry);
                        return true;
                    }
                }
            }
            return false;
        }

        private void computeDecay() {
            long now = threadPool.rawRelativeTimeInMillis();
            long afterLock;
            long end;
            synchronized (SharedBlobCacheService.this) {
                afterLock = threadPool.rawRelativeTimeInMillis();
                appendLevel1ToLevel0();
                for (int i = 2; i < maxFreq; i++) {
                    assert freqs[i - 1] == null;
                    freqs[i - 1] = freqs[i];
                    freqs[i] = null;
                    decrementFreqList(freqs[i - 1]);
                    assert freqs[i - 1] == null || invariant(freqs[i - 1], true);
                }
            }
            end = threadPool.rawRelativeTimeInMillis();
            logger.debug("Decay took {} ms (acquire lock: {} ms)", end - now, afterLock - now);
        }

        class DecayAndNewEpochTask extends AbstractRunnable {

            private final Executor executor;
            private final AtomicLong pendingEpoch = new AtomicLong();
            private volatile boolean isClosed;

            DecayAndNewEpochTask(Executor executor) {
                this.executor = executor;
            }

            @Override
            protected void doRun() throws Exception {
                if (isClosed == false) {
                    computeDecay();
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("failed to run cache decay task", e);
            }

            @Override
            public void onAfter() {
                assert pendingEpoch.get() == epoch.get() + 1;
                epoch.incrementAndGet();
            }

            @Override
            public void onRejection(Exception e) {
                assert false : e;
                logger.error("unexpected rejection", e);
                epoch.incrementAndGet();
            }

            @Override
            public String toString() {
                return "shared_cache_decay_task";
            }

            public void spawnIfNotRunning(long currentEpoch) {
                if (isClosed == false && pendingEpoch.compareAndSet(currentEpoch, currentEpoch + 1)) {
                    executor.execute(this);
                }
            }

            public void close() {
                this.isClosed = true;
            }
        }
    }
}
