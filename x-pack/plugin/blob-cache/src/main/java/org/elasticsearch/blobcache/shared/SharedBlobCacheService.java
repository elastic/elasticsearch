/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache.shared;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexFileNames;
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
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThrottledTaskRunner;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.DirectAccessInput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.LuceneFilesExtensions;
import org.elasticsearch.monitor.fs.FsProbe;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiPredicate;
import java.util.function.IntConsumer;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.blobcache.BlobCacheMetrics.ES_EXECUTOR_ATTRIBUTE_KEY;
import static org.elasticsearch.blobcache.BlobCacheMetrics.EvictionScanMode.AllFrequencies;
import static org.elasticsearch.blobcache.BlobCacheMetrics.EvictionScanMode.LowestFrequency;
import static org.elasticsearch.blobcache.BlobCacheMetrics.EvictionScanOutcome.Evicted;
import static org.elasticsearch.blobcache.BlobCacheMetrics.EvictionScanOutcome.Free;
import static org.elasticsearch.blobcache.BlobCacheMetrics.EvictionScanOutcome.None;
import static org.elasticsearch.blobcache.BlobCacheMetrics.LUCENE_FILE_EXTENSION_ATTRIBUTE_KEY;
import static org.elasticsearch.blobcache.BlobCacheMetrics.LockAcquireSite.CacheMissEviction;
import static org.elasticsearch.blobcache.BlobCacheMetrics.LockAcquireSite.Decay;
import static org.elasticsearch.blobcache.BlobCacheMetrics.LockAcquireSite.Demote;
import static org.elasticsearch.blobcache.BlobCacheMetrics.LockAcquireSite.ForceEvict;
import static org.elasticsearch.blobcache.BlobCacheMetrics.LockAcquireSite.LowestFrequencyEviction;
import static org.elasticsearch.blobcache.BlobCacheMetrics.LockAcquireSite.Promote;
import static org.elasticsearch.blobcache.BlobCacheMetrics.LockAcquireSite.SlotAssignment;
import static org.elasticsearch.blobcache.BlobCacheMetrics.NON_ES_EXECUTOR_TO_RECORD;
import static org.elasticsearch.blobcache.BlobCacheMetrics.NON_LUCENE_EXTENSION_TO_RECORD;

/**
 * A caching layer on a local node to minimize network roundtrips to the remote blob store.
 */
public class SharedBlobCacheService<KeyType extends SharedBlobCacheService.KeyBase> implements Releasable {

    public interface KeyBase {
        ShardId shardId();
    }

    /**
     * Sentinel used when the data timestamp for a cache region is unknown or unavailable. It is a plain {@code long} at the cache layer:
     * the cache assigns no semantic meaning to it beyond "unknown".
     */
    public static final long UNKNOWN_TIMESTAMP = -1L;

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
        SHARED_CACHE_SETTINGS_PREFIX + "size",
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
        SHARED_CACHE_SETTINGS_PREFIX + "size.max_headroom",
        settings -> {
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

    public static final Setting<Integer> SHARED_CACHE_INITIAL_DECAYS_SETTING = Setting.intSetting(
        SHARED_CACHE_SETTINGS_PREFIX + "initial_decays",
        4,
        0,
        Setting.Property.NodeScope
    );

    /**
     * Fraction of the total number of regions. When the number of entries at frequency 0 falls below
     * {@code max(1, numRegions * this ratio)}, a decay and new epoch is scheduled.
     */
    public static final Setting<Float> SHARED_CACHE_DECAY_FREQ0_THRESHOLD_RATIO_SETTING = Setting.floatSetting(
        SHARED_CACHE_SETTINGS_PREFIX + "decay.freq0_threshold_ratio",
        0.05f,
        0f,
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
        CacheEntry<T> get(K cacheKey, long fileLength, int region, long timestampMillis);

        /// Returns the entry for the provided `cacheKey` and `region` if it exists and is fully initialized
        /// (i.e. its IO slot has been assigned), or `null` otherwise.
        ///
        /// Unlike [#get], this method will not allocate a new region slot if the entry does not exist.
        @Nullable
        CacheEntry<T> getIfPresent(K cacheKey, int region);

        int forceEvict(Predicate<K> cacheKeyPredicate);

        void forceEvictAsync(Predicate<K> cacheKey);

        int forceEvict(ShardId shard, Predicate<K> cacheKeyPredicate);

        int forceEvict(ShardId shard, BiPredicate<K, Integer> regionPredicate);

        int demoteAll(ShardId shard);
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

    private final LongAdder readBytes = new LongAdder();

    private final LongAdder evictCount = new LongAdder();

    private final BlobCacheMetrics blobCacheMetrics;

    private final Runnable evictIncrementer;

    private final LongSupplier relativeNanosProvider;
    private final ThrottledTaskRunner asyncEvictionsRunner;

    public SharedBlobCacheService(
        NodeEnvironment environment,
        Settings settings,
        ThreadPool threadPool,
        Executor ioExecutor,
        BlobCacheMetrics blobCacheMetrics
    ) {
        this(environment, settings, threadPool, ioExecutor, blobCacheMetrics, System::nanoTime, new DefaultEvictionPolicy<>());
    }

    public SharedBlobCacheService(
        NodeEnvironment environment,
        Settings settings,
        ThreadPool threadPool,
        Executor ioExecutor,
        BlobCacheMetrics blobCacheMetrics,
        LongSupplier relativeTimeInNanosSupplier
    ) {
        this(environment, settings, threadPool, ioExecutor, blobCacheMetrics, relativeTimeInNanosSupplier, new DefaultEvictionPolicy<>());
    }

    public SharedBlobCacheService(
        NodeEnvironment environment,
        Settings settings,
        ThreadPool threadPool,
        Executor ioExecutor,
        BlobCacheMetrics blobCacheMetrics,
        EvictionPolicy<KeyType> evictionPolicy
    ) {
        this(environment, settings, threadPool, ioExecutor, blobCacheMetrics, System::nanoTime, evictionPolicy);
    }

    public SharedBlobCacheService(
        NodeEnvironment environment,
        Settings settings,
        ThreadPool threadPool,
        Executor ioExecutor,
        BlobCacheMetrics blobCacheMetrics,
        LongSupplier relativeTimeInNanosSupplier,
        EvictionPolicy<KeyType> evictionPolicy
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
        this.cache = new LFUCache(settings, evictionPolicy);
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
        this.relativeNanosProvider = relativeTimeInNanosSupplier;
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

    public ThreadPool getThreadPool() {
        return threadPool;
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
        return get(cacheKey, fileLength, region, UNKNOWN_TIMESTAMP);
    }

    CacheFileRegion<KeyType> get(KeyType cacheKey, long fileLength, int region, long timestampMillis) {
        return cache.get(cacheKey, fileLength, region, timestampMillis).chunk;
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
        maybeFetchRegion(cacheKey, region, blobLength, writer, fetchExecutor, UNKNOWN_TIMESTAMP, listener);
    }

    public void maybeFetchRegion(
        final KeyType cacheKey,
        final int region,
        final long blobLength,
        final RangeMissingHandler writer,
        final Executor fetchExecutor,
        final long timestampMillis,
        final ActionListener<Boolean> listener
    ) {
        fetchRegion(cacheKey, region, blobLength, writer, fetchExecutor, false, timestampMillis, listener);
    }

    /**
     * Fetch and write in cache a region of a blob.
     * <p>
     * If {@code force} is {@code true} and no free regions remain, an existing region will be evicted to make room.
     * </p>
     *
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
     * @param force         flag indicating whether the cache should free an occupied region to accommodate the requested
     *                      region when none are free.
     * @param listener      a listener that is completed with {@code true} if the current thread triggered the fetching of the region, in
     *                      which case the data is available in cache. The listener is completed with {@code false} in every other cases: if
     *                      the region to write is already available in cache, if the region is pending fetching via another thread or if
     *                      there is not enough free pages to fetch the region.
     */
    public void fetchRegion(
        final KeyType cacheKey,
        final int region,
        final long blobLength,
        final RangeMissingHandler writer,
        final Executor fetchExecutor,
        final boolean force,
        final ActionListener<Boolean> listener
    ) {
        fetchRegion(cacheKey, region, blobLength, writer, fetchExecutor, force, UNKNOWN_TIMESTAMP, listener);
    }

    public void fetchRegion(
        final KeyType cacheKey,
        final int region,
        final long blobLength,
        final RangeMissingHandler writer,
        final Executor fetchExecutor,
        final boolean force,
        final long timestampMillis,
        final ActionListener<Boolean> listener
    ) {
        if (force == false && freeRegions.isEmpty()) {
            var incoming = new CacheFileRegion<>(
                this,
                new RegionKey<>(cacheKey, region),
                computeCacheFileRegionSize(blobLength, region),
                timestampMillis
            );
            if (maybeEvictLeastUsed(incoming) == false) {
                // no free page available and no old enough unused region to be evicted
                logger.info("No free regions, skipping loading region [{}]", region);
                listener.onResponse(false);
                return;
            }
        }
        try {
            ByteRange regionRange = ByteRange.of(0, computeCacheFileRegionSize(blobLength, region));
            if (regionRange.isEmpty()) {
                listener.onResponse(false);
                return;
            }
            final CacheFileRegion<KeyType> entry = get(cacheKey, blobLength, region, timestampMillis);
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
        maybeFetchRange(cacheKey, region, range, blobLength, writer, fetchExecutor, UNKNOWN_TIMESTAMP, listener);
    }

    public void maybeFetchRange(
        final KeyType cacheKey,
        final int region,
        final ByteRange range,
        final long blobLength,
        final RangeMissingHandler writer,
        final Executor fetchExecutor,
        final long timestampMillis,
        final ActionListener<Boolean> listener
    ) {
        fetchRange(cacheKey, region, range, blobLength, writer, fetchExecutor, false, timestampMillis, listener);
    }

    /**
     * Fetch and write in cache a range within a blob region.
     * <p>
     * If {@code force} is {@code true} and no free regions remain, an existing region will be evicted to make room.
     * </p>
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
     * @param force         flag indicating whether the cache should free an occupied region to accommodate the requested
     *                      range when none are free.
     * @param listener      a listener that is completed with {@code true} if the current thread triggered the fetching of the range, in
     *                      which case the data is available in cache. The listener is completed with {@code false} in every other cases: if
     *                      the range to write is already available in cache, if the range is pending fetching via another thread or if
     *                      there is not enough free pages to fetch the range.
     */
    public void fetchRange(
        final KeyType cacheKey,
        final int region,
        final ByteRange range,
        final long blobLength,
        final RangeMissingHandler writer,
        final Executor fetchExecutor,
        final boolean force,
        final ActionListener<Boolean> listener
    ) {
        fetchRange(cacheKey, region, range, blobLength, writer, fetchExecutor, force, UNKNOWN_TIMESTAMP, listener);
    }

    public void fetchRange(
        final KeyType cacheKey,
        final int region,
        final ByteRange range,
        final long blobLength,
        final RangeMissingHandler writer,
        final Executor fetchExecutor,
        final boolean force,
        final long timestampMillis,
        final ActionListener<Boolean> listener
    ) {
        if (force == false && freeRegions.isEmpty()) {
            var incoming = new CacheFileRegion<>(
                this,
                new RegionKey<>(cacheKey, region),
                computeCacheFileRegionSize(blobLength, region),
                timestampMillis
            );
            if (maybeEvictLeastUsed(incoming) == false) {
                // no free page available and no old enough unused region to be evicted
                logger.debug("No free regions, skipping loading region [{}]", region);
                listener.onResponse(false);
                return;
            }
        }
        try {
            var regionRange = mapSubRangeToRegion(range, region);
            if (regionRange.isEmpty()) {
                listener.onResponse(false);
                return;
            }
            final CacheFileRegion<KeyType> entry = get(cacheKey, blobLength, region, timestampMillis);
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

    /**
     * Wraps the given {@link RangeMissingHandler} to adjust the position of the data read from the blob store
     * (NB: the relativePos parameter in
     * {@link RangeMissingHandler#fillCacheRange(SharedBytes.IO, int, SourceInputStreamFactory, int, int, IntConsumer, ActionListener)})
     * relative to the beginning of the region we're reading from.
     *
     * This is useful so that we can read the input stream we open for reading from the blob store
     * from the beginning (i.e. position 0 <b>in the input stream</b>).
     *
     * For example, if we want to read 2000 bytes the blob store starting at position 1000, the writer here will
     * adjust the relative position we read to be 0, the offset being 1000, and the input stream we open to
     * read from the blob store will start streaming from position 1000 (but we adjusted the relative read position
     * to 0 so we consume the input stream from the beginning).
     */
    private RangeMissingHandler writerWithOffset(RangeMissingHandler writer, int writeOffset) {
        if (writeOffset == 0) {
            // no need to allocate a new capturing lambda if the offset isn't adjusted
            return writer;
        }

        return new RangeMissingHandler() {
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
                    relativePos - writeOffset,
                    length,
                    progressUpdater,
                    completionListener
                );
            }

            @Override
            public SourceInputStreamFactory sharedInputStreamFactory(List<SparseFileTracker.Gap> gaps) {
                return writer.sharedInputStreamFactory(gaps);
            }
        };
    }

    // used by tests
    boolean maybeEvictLeastUsed(KeyType cacheKey, long length, int region) {
        if (cache instanceof LFUCache lfuCache) {
            var incoming = new CacheFileRegion<>(
                this,
                new RegionKey<>(cacheKey, region),
                computeCacheFileRegionSize(length, region),
                UNKNOWN_TIMESTAMP
            );
            return lfuCache.maybeEvictLeastUsed(incoming);
        }
        return false;
    }

    private boolean maybeEvictLeastUsed(final CacheFileRegion<KeyType> incoming) {
        if (cache instanceof LFUCache lfuCache) {
            return lfuCache.maybeEvictLeastUsed(incoming);
        }
        return false;
    }

    // used by tests
    public long countCachedRegions(Predicate<KeyType> predicate) {
        if (cache instanceof LFUCache lfuCache) {
            return lfuCache.countCachedRegions(predicate);
        }
        throw new UnsupportedOperationException("cache is not an LFUCache");
    }

    // used by tests
    public long countCachedRegions(ShardId shardId, BiPredicate<KeyType, Integer> regionPredicate) {
        if (cache instanceof LFUCache lfuCache) {
            return lfuCache.countCachedRegions(shardId, regionPredicate);
        }
        throw new UnsupportedOperationException("cache is not an LFUCache");
    }

    private static void throwAlreadyClosed(String message) {
        throw new AlreadyClosedException(message);
    }

    /**
     * NOTE: Method is package private mostly to allow checking the number of fee regions in tests.
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
            blobCacheMetrics.readCount(),
            readBytes.sum(),
            blobCacheMetrics.missCount()
        );
    }

    public long getCacheSize() {
        return cacheSize;
    }

    public void removeFromCache(KeyType cacheKey) {
        forceEvict(cacheKey.shardId(), cacheKey::equals);
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

    public int forceEvict(ShardId shard, Predicate<KeyType> cacheKeyPredicate) {
        return cache.forceEvict(shard, cacheKeyPredicate);
    }

    public int forceEvict(ShardId shard, BiPredicate<KeyType, Integer> regionPredicate) {
        return cache.forceEvict(shard, regionPredicate);
    }

    /**
     * Evict entries from the cache that match the given predicate asynchronously
     *
     * @param cacheKeyPredicate
     */
    public void forceEvictAsync(Predicate<KeyType> cacheKeyPredicate) {
        cache.forceEvictAsync(cacheKeyPredicate);
    }

    /**
     * Demotes all active cache regions for the given shard to frequency 0.
     * Demoted entries are inserted at the front of the frequency-0 list so they are evicted before
     * other frequency-0 entries. {@code lastAccessedEpoch} is set to {@code -1} so demoted entries
     * are promoted again if they are accessed due to shard relocating back after demotion.
     *
     * @return the number of regions demoted
     */
    public int demoteAll(ShardId shard) {
        return cache.demoteAll(shard);
    }

    /**
     * Schedules an asynchronous demotion of all active cache regions for the given shard to frequency 0.
     * The predicate is evaluated when the task runs; demotion is skipped when it returns {@code false}.
     */
    public void demoteAllAsync(ShardId shard, Predicate<ShardId> shouldDemote) {
        asyncEvictionsRunner.enqueueTask(new ActionListener<>() {
            @Override
            public void onResponse(Releasable releasable) {
                try (releasable) {
                    if (shouldDemote.test(shard)) {
                        cache.demoteAll(shard);
                    }
                }
            }

            @Override
            public void onFailure(Exception e) {
                final String message = "unexpected failure in async demotion task for shard [" + shard + "]";
                logger.error(message, e);
                assert false : new AssertionError(message, e);
            }
        });
    }

    /**
     * Submits a task to be executed asynchronously on the cache eviction thread pool,
     * respecting the same throttling as other eviction tasks.
     */
    public void submitAsyncEviction(Runnable task) {
        asyncEvictionsRunner.enqueueTask(new ActionListener<>() {
            @Override
            public void onResponse(Releasable releasable) {
                try (releasable) {
                    task.run();
                }
            }

            @Override
            public void onFailure(Exception e) {
                final String message = "unexpected failure in async eviction task";
                logger.error(message, e);
                assert false : new AssertionError(message, e);
            }
        });
    }

    // used by tests
    Map<Integer, Integer> countCachedRegionsByFreq(Predicate<KeyType> predicate) {
        return countCachedRegionsByFreq(predicate, false);
    }

    // used by tests
    Map<Integer, Integer> countCachedRegionsByFreq(Predicate<KeyType> predicate, boolean includeEvicted) {
        if (cache instanceof LFUCache lfuCache) {
            return lfuCache.countCachedRegionsByFreq(predicate, includeEvicted);
        }
        throw new UnsupportedOperationException("cache is not an LFUCache");
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
     * (see LFUCache#maybeEvictAndTakeForFrequency)
     */
    static class CacheFileRegion<KeyType extends KeyBase> extends EvictableRefCounted implements CacheRegion<KeyType> {

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
        // Representative data timestamp (epoch millis) of the content in this region, or UNKNOWN_TIMESTAMP when unknown.
        private final long timestampMillis;
        // io can be null when not init'ed or after evict/take
        // io does not need volatile access on the read path, since it goes from null to a single value (and then possbily back to null).
        // "cache.get" never returns a `CacheFileRegion` without checking the value is non-null (with a volatile read, ensuring the value is
        // visible in that thread).
        // We assume any IndexInput passing among threads is done with proper happens-before semantics (otherwise they'd themselves break).
        // In general, assertions should use `nonVolatileIO` (when they can) to access this over `volatileIO` to avoid memory visibility
        // side effects
        private SharedBytes.IO io = null;

        CacheFileRegion(
            SharedBlobCacheService<KeyType> blobCacheService,
            RegionKey<KeyType> regionKey,
            int regionSize,
            long timestampMillis
        ) {
            this.blobCacheService = blobCacheService;
            this.regionKey = regionKey;
            assert timestampMillis > 0L || timestampMillis == UNKNOWN_TIMESTAMP : timestampMillis;
            this.timestampMillis = timestampMillis;
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
                blobCacheService.blobCacheMetrics.getTotalEvictedCount().increment();
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
                blobCacheService.blobCacheMetrics.getTotalEvictedCount().increment();
                return true;
            }

            return false;
        }

        public boolean forceEvict() {
            assert Thread.holdsLock(blobCacheService) : "must hold lock when evicting";
            if (evict()) {
                logger.trace("force evicted {} with channel offset {}", regionKey, physicalStartOffset());
                blobCacheService.evictCount.increment();
                blobCacheService.blobCacheMetrics.getTotalEvictedCount().increment();
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

        @Override
        public KeyType key() {
            return regionKey.file();
        }

        @Override
        public long timestampMillis() {
            return timestampMillis;
        }

        /**
         * Optimistically try to load the data from the region into main memory using madvise system call.
         * @return true if successful, i.e., not evicted and data available, false if evicted or mmap is not used underneath.
         */
        boolean tryPrefetch(long offset, long length) throws IOException {
            SharedBytes.IO ioRef = nonVolatileIO();
            if (ioRef != null) {
                ioRef.prefetch(blobCacheService.getRegionRelativePosition(offset), length);
                if (isEvicted()) {
                    return false;
                }
                return true;
            } else {
                // taken by someone else
                return false;
            }
        }

        /**
         * Optimistically try to read from the region
         * @return true if successful, i.e., not evicted and data available, false if evicted
         */
        boolean tryRead(ByteBuffer buf, long offset) throws IOException {
            return tryRead(buf, offset, SharedBytes.MADV_NORMAL);
        }

        /**
         * Optimistically try to read from the region, applying the given madvise advice.
         * @return true if successful, i.e., not evicted and data available, false if evicted
         */
        boolean tryRead(ByteBuffer buf, long offset, int advice) throws IOException {
            SharedBytes.IO ioRef = nonVolatileIO();
            if (ioRef != null) {
                ioRef.madvise(advice);
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
         * If a direct memory segment slice is available for the given range,
         * passes it to {@code action} within a ref-counted scope (preventing
         * eviction) and returns {@code true}. Returns {@code false} without
         * invoking the action when not available (not mmap'd, evicted, etc.).
         */
        boolean withMemorySegmentSlice(long offset, int length, CheckedConsumer<MemorySegment, IOException> action) throws IOException {
            return withMemorySegmentSlice(offset, length, action, SharedBytes.MADV_NORMAL);
        }

        boolean withMemorySegmentSlice(long offset, int length, CheckedConsumer<MemorySegment, IOException> action, int advice)
            throws IOException {
            SharedBytes.IO ioRef = nonVolatileIO();
            if (ioRef != null && tryIncRef()) {
                try {
                    ioRef.madvise(advice);
                    MemorySegment slice = ioRef.memorySegmentSlice(blobCacheService.getRegionRelativePosition(offset), length);
                    if (slice != null && isEvicted() == false) {
                        action.accept(slice);
                        return true;
                    }
                } finally {
                    decRef();
                }
            }
            return false;
        }

        /**
         * Populates a range in cache if the range is not available nor pending to be available in cache.
         *
         * @param rangeToWrite the range of bytes to populate
         * @param writer a writer that handles writing of newly downloaded data to the shared cache
         * @param executor the executor used to download and to write new data
         * @param listener a listener that is completed with {@code true} if the current thread triggered the download and write of the
         *                 range, in which case the listener is completed once writing is done. The listener is completed with {@code false}
         *                 if the range to write is already available in cache or if another thread will download and write the range. The
         *                 listener may be invoked on the current thread, and executor thread or another executor thread used by another
         *                 caller.
         */
        void populate(
            final ByteRange rangeToWrite,
            final RangeMissingHandler writer,
            final Executor executor,
            final ActionListener<Boolean> listener
        ) {
            if (rangeToWrite.isEmpty()) {
                listener.onResponse(false);
                return;
            }
            try {
                incRefEnsureOpen();
                try (RefCountingRunnable refs = new RefCountingRunnable(CacheFileRegion.this::decRef)) {
                    final var gapsOpt = tracker.waitForRange(
                        rangeToWrite,
                        rangeToWrite,
                        Assertions.ENABLED ? ActionListener.releaseAfter(ActionListener.running(() -> {
                            assert blobCacheService.regionOwners.get(nonVolatileIO()) == this;
                        }), refs.acquire()) : refs.acquireListener()
                    );
                    if (gapsOpt.isEmpty()) {
                        listener.onResponse(false);
                        return;
                    }
                    executor.execute(new AbstractRunnable() {
                        private final Releasable dispatchRef = refs.acquire();

                        @Override
                        protected void doRun() {
                            final List<SparseFileTracker.Gap> gaps = gapsOpt.get().claim();
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
                            final ActionListener<Void> gapsDoneListener = streamFactory != null
                                ? ActionListener.releaseBefore(streamFactory, listener.map(unused -> true))
                                : listener.map(unused -> true);
                            try (var gapsListener = new RefCountingListener(gapsDoneListener)) {
                                // Use current thread to fill the gaps in order
                                for (SparseFileTracker.Gap gap : gaps) {
                                    fillGapRunnable(
                                        gap,
                                        writer,
                                        streamFactory,
                                        ActionListener.releaseAfter(gapsListener.acquire(), refs.acquire())
                                    ).run();
                                }
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            listener.onFailure(e);
                        }

                        @Override
                        public void onAfter() {
                            dispatchRef.close();
                        }
                    });
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
                            blobCacheService.blobCacheMetrics.recordRead();
                            l.onResponse(read);
                        })
                    ).map(SparseFileTracker.Gaps::claim).orElse(List.of());

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

    public interface CacheMissHandler {
        CacheMissHandler NOOP = new CacheMissHandler() {
            @Override
            public Releasable record(long bytes) {
                return Releasables.wrap();
            }

            @Override
            public CacheMissHandler copy() {
                return this;
            }
        };

        /**
         * @param bytes number of bytes needed
         * @return Releasable that is invoked when data is available.
         */
        Releasable record(long bytes);

        CacheMissHandler copy();
    }

    public class CacheFile {

        private final KeyType cacheKey;
        private final long length;
        private final CacheMissHandler cacheMissMetricHandler;
        private final long timestampMillis;
        private CacheEntry<CacheFileRegion<KeyType>> lastAccessedRegion;

        private CacheFile(KeyType cacheKey, long length, CacheMissHandler cacheMissMetricHandler, long timestampMillis) {
            this.cacheKey = cacheKey;
            this.length = length;
            this.cacheMissMetricHandler = cacheMissMetricHandler;
            this.timestampMillis = timestampMillis;
        }

        public CacheFile copy() {
            return new CacheFile(cacheKey, length, cacheMissMetricHandler.copy(), timestampMillis);
        }

        public long getLength() {
            return length;
        }

        public KeyType getCacheKey() {
            return cacheKey;
        }

        public boolean tryPrefetch(long offset, long length) throws IOException {
            assert assertOffsetsWithinFileLength(offset, length, this.length);
            final int startRegion = getRegion(offset);
            final long end = offset + length;
            final int endRegion = getEndingRegion(end);
            final var rangeToRead = ByteRange.of(offset, offset + length);
            for (int region = startRegion; region <= endRegion; region++) {
                final ByteRange subRangeToRead = mapSubRangeToRegion(rangeToRead, region);
                if (subRangeToRead.isEmpty()) {
                    // nothing to read, skip
                    continue;
                }
                final CacheEntry<CacheFileRegion<KeyType>> fileRegion = cache.getIfPresent(cacheKey, region);
                if (fileRegion == null) {
                    continue;
                }
                final var chunk = fileRegion.chunk;
                if (chunk.tracker.checkAvailable(subRangeToRead.length()) == false) {
                    continue;
                }
                if (chunk.tryPrefetch(subRangeToRead.start(), subRangeToRead.length())) {
                    length -= subRangeToRead.length();
                }
            }
            return length == 0;
        }

        public boolean tryRead(ByteBuffer buf, long offset) throws IOException {
            return tryRead(buf, offset, SharedBytes.MADV_NORMAL);
        }

        public boolean tryRead(ByteBuffer buf, long offset, int advice) throws IOException {
            assert assertOffsetsWithinFileLength(offset, buf.remaining(), length);
            final int startRegion = getRegion(offset);
            final long end = offset + buf.remaining();
            final int endRegion = getEndingRegion(end);
            if (startRegion != endRegion) {
                return false;
            }
            var fileRegion = lastAccessedRegion;
            boolean incrementReads = false;
            if (fileRegion != null && fileRegion.chunk.regionKey.region == startRegion) {
                // existing item, check if we need to promote item
                fileRegion.touch();
            } else {
                fileRegion = cache.getIfPresent(cacheKey, startRegion);
                if (fileRegion == null) {
                    return false;
                }
                incrementReads = true;
            }
            final var region = fileRegion.chunk;
            if (region.tracker.checkAvailable(end - getRegionStart(startRegion)) == false) {
                return false;
            }
            boolean res = region.tryRead(buf, offset, advice);
            lastAccessedRegion = res ? fileRegion : null;
            if (res && incrementReads) {
                blobCacheMetrics.recordRead();
                // todo: should we add to readBytes? readBytes.add(end - offset);
            }
            return res;
        }

        /**
         * If a direct memory segment view is available for the given range, passes it
         * to {@code action} and returns {@code true}. Otherwise, returns
         * {@code false} without invoking the action.
         */
        public boolean withMemorySegmentSlice(long offset, int length, CheckedConsumer<MemorySegment, IOException> action)
            throws IOException {
            return withMemorySegmentSlice(offset, length, action, SharedBytes.MADV_NORMAL);
        }

        public boolean withMemorySegmentSlice(long offset, int length, CheckedConsumer<MemorySegment, IOException> action, int advice)
            throws IOException {
            assert assertOffsetsWithinFileLength(offset, length, this.length);
            final int startRegion = getRegion(offset);
            final long end = offset + length;
            final int endRegion = getEndingRegion(end);
            if (startRegion != endRegion) {
                return false;
            }
            CacheEntry<CacheFileRegion<KeyType>> fileRegion = lastAccessedRegion;
            if (fileRegion != null && fileRegion.chunk.regionKey.region == startRegion) {
                fileRegion.touch();
            } else {
                fileRegion = cache.getIfPresent(cacheKey, startRegion);
                if (fileRegion == null) {
                    return false;
                }
            }
            final var region = fileRegion.chunk;
            if (region.tracker.checkAvailable(end - getRegionStart(startRegion)) == false) {
                return false;
            }
            boolean result = region.withMemorySegmentSlice(offset, length, action, advice);
            if (result) {
                lastAccessedRegion = fileRegion;
            }
            return result;
        }

        /**
         * Bulk variant of {@link #withMemorySegmentSlice}. Resolves {@code count} byte ranges to
         * memory segments, holding ref-counts on all distinct regions to prevent eviction,
         * then invokes the action. Each individual range must fit within a single region.
         */
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public boolean withMemorySegmentSlices(long[] offsets, int length, int count, CheckedConsumer<MemorySegment[], IOException> action)
            throws IOException {
            return withMemorySegmentSlices(offsets, length, count, action, SharedBytes.MADV_NORMAL);
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        public boolean withMemorySegmentSlices(
            long[] offsets,
            int length,
            int count,
            CheckedConsumer<MemorySegment[], IOException> action,
            int advice
        ) throws IOException {
            if (DirectAccessInput.checkSlicesArgs(offsets, count)) {
                return false;
            }
            final CacheFileRegion<KeyType>[] held = new CacheFileRegion[count];
            int heldCount = 0;
            final MemorySegment[] results = new MemorySegment[count];
            try {
                for (int i = 0; i < count; i++) {
                    final long offset = offsets[i];
                    assert assertOffsetsWithinFileLength(offset, length, this.length);
                    final int regionIdx = getRegion(offset);
                    if (regionIdx != getEndingRegion(offset + length)) {
                        return false;
                    }

                    final var entry = cache.getIfPresent(cacheKey, regionIdx);
                    if (entry == null) {
                        return false;
                    }
                    final var region = entry.chunk;

                    final long regionEnd = offset + length - getRegionStart(regionIdx);
                    if (region.tracker.checkAvailable(regionEnd) == false) {
                        return false;
                    }

                    SharedBytes.IO ioRef = region.nonVolatileIO();
                    if (ioRef == null) {
                        return false;
                    }

                    if (notAlreadyHeld(region, held, heldCount)) {
                        if (region.tryIncRef() == false) {
                            return false;
                        }
                        held[heldCount++] = region;
                        ioRef.madvise(advice);
                    }

                    results[i] = ioRef.memorySegmentSlice(getRegionRelativePosition(offset), length);
                    if (results[i] == null) {
                        return false;
                    }
                }
                for (int i = 0; i < heldCount; i++) {
                    if (held[i].isEvicted()) {
                        return false;
                    }
                }
                action.accept(results);
                return true;
            } finally {
                for (int i = 0; i < heldCount; i++) {
                    held[i].decRef();
                }
            }
        }

        private static boolean notAlreadyHeld(CacheFileRegion<?> region, CacheFileRegion<?>[] held, int heldCount) {
            for (int i = 0; i < heldCount; i++) {
                if (held[i] == region) {
                    return false;
                }
            }
            return true;
        }

        public int populateAndRead(
            final ByteRange rangeToWrite,
            final ByteRange rangeToRead,
            final RangeAvailableHandler reader,
            final RangeMissingHandler writer,
            String resourceDescription
        ) throws Exception {
            final PlainActionFuture<Integer> future = new PlainActionFuture<>();
            final long absentBytes = populate(rangeToWrite, rangeToRead, reader, writer, resourceDescription, future);
            if (future.isDone() == false && absentBytes > 0) {
                return recordWait(absentBytes, future);
            }
            return future.get();
        }

        /**
         * Asynchronous, listener-based primitive that backs {@link #populateAndRead}. Ensures {@code rangeToWrite}
         * is populated in the cache (downloading missing sub-ranges via {@code writer} on the I/O executor) and
         * invokes {@code reader} per region when its sub-range of {@code rangeToRead} becomes available. The
         * {@code listener} is completed with the total number of bytes that the {@code reader} reported.
         * <p>
         * Returns as soon as the per-region work has been dispatched. Sync callers should use
         * {@link #populateAndRead}; fire-and-forget callers (e.g. an {@code madvise(WILLNEED)}-style prefetch that
         * just calls {@link SharedBytes.IO#prefetch(long, long)} from the {@code reader}) pass a no-op / logging
         * listener.
         *
         * @return a snapshot of how many bytes inside {@code rangeToRead} were still missing from the cache at
         *         dispatch time. Used by the sync wrapper to drive {@link #recordWait}; async callers can ignore.
         */
        public long populate(
            final ByteRange rangeToWrite,
            final ByteRange rangeToRead,
            final RangeAvailableHandler reader,
            final RangeMissingHandler writer,
            String resourceDescription,
            ActionListener<Integer> listener
        ) {
            // some cache files can grow after being created, so rangeToWrite can be larger than the initial {@code length}
            assert rangeToWrite.start() >= 0 : rangeToWrite;
            assert assertOffsetsWithinFileLength(rangeToRead.start(), rangeToRead.length(), length);
            if (rangeToRead.isEmpty()) {
                listener.onResponse(0);
                return 0L;
            }
            // We are interested in the total time that the system spends when fetching a result (including time spent queuing), so we start
            // our measurement here.
            final long startTime = relativeNanosProvider.getAsLong();
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
                    String blobFileExtension = getFileExtension(resourceDescription);
                    String executorName = EsExecutors.executorName(Thread.currentThread());
                    writer.fillCacheRange(
                        channel,
                        channelPos,
                        streamFactory,
                        relativePos,
                        length,
                        progressUpdater,
                        completionListener.map(unused -> {
                            var elapsedTime = TimeUnit.NANOSECONDS.toMillis(relativeNanosProvider.getAsLong() - startTime);
                            blobCacheMetrics.getCacheMissLoadTimes().record(elapsedTime);
                            blobCacheMetrics.getCacheMissCounter()
                                .incrementBy(
                                    1L,
                                    Map.of(
                                        LUCENE_FILE_EXTENSION_ATTRIBUTE_KEY,
                                        blobFileExtension,
                                        ES_EXECUTOR_ATTRIBUTE_KEY,
                                        executorName != null ? executorName : NON_ES_EXECUTOR_TO_RECORD
                                    )
                                );
                            return null;
                        })
                    );
                }
            };
            final int startRegion = getRegion(rangeToWrite.start());
            final int endRegion = getEndingRegion(rangeToWrite.end());
            if (startRegion == endRegion) {
                return readSingleRegion(rangeToWrite, rangeToRead, reader, writerInstrumentationDecorator, startRegion, listener);
            }
            return readMultiRegions(rangeToWrite, rangeToRead, reader, writerInstrumentationDecorator, startRegion, endRegion, listener);
        }

        private long readSingleRegion(
            ByteRange rangeToWrite,
            ByteRange rangeToRead,
            RangeAvailableHandler reader,
            RangeMissingHandler writer,
            int region,
            ActionListener<Integer> listener
        ) {
            final CacheFileRegion<KeyType> fileRegion;
            try {
                fileRegion = get(cacheKey, length, region, timestampMillis);
            } catch (Exception e) {
                assert e instanceof AlreadyClosedException : e;
                listener.onFailure(e);
                return 0L;
            }
            final long regionStart = getRegionStart(region);
            ByteRange regionRangeToRead = mapSubRangeToRegion(rangeToRead, region);
            fileRegion.populateAndRead(
                mapSubRangeToRegion(rangeToWrite, region),
                regionRangeToRead,
                readerWithOffset(reader, fileRegion, Math.toIntExact(rangeToRead.start() - regionStart)),
                metricRecordingWriter(writerWithOffset(writer, fileRegion, Math.toIntExact(rangeToWrite.start() - regionStart))),
                ioExecutor,
                listener
            );
            return fileRegion.tracker.getAbsentBytesWithin(regionRangeToRead);
        }

        private long readMultiRegions(
            ByteRange rangeToWrite,
            ByteRange rangeToRead,
            RangeAvailableHandler reader,
            RangeMissingHandler writer,
            int startRegion,
            int endRegion,
            ActionListener<Integer> listener
        ) {
            final AtomicInteger bytesRead = new AtomicInteger();
            final List<CacheFileRegion<KeyType>> regions = new ArrayList<>(endRegion - startRegion);
            try (var listeners = new RefCountingListener(1, listener.map(v -> bytesRead.get()))) {
                for (int region = startRegion; region <= endRegion; region++) {
                    final ByteRange subRangeToRead = mapSubRangeToRegion(rangeToRead, region);
                    if (subRangeToRead.isEmpty()) {
                        // nothing to read, skip
                        continue;
                    }
                    ActionListener<Integer> regionListener = listeners.acquire(i -> bytesRead.updateAndGet(j -> Math.addExact(i, j)));
                    try {
                        final CacheFileRegion<KeyType> fileRegion = get(cacheKey, length, region, timestampMillis);
                        regions.add(fileRegion);
                        final long regionStart = getRegionStart(region);
                        fileRegion.populateAndRead(
                            mapSubRangeToRegion(rangeToWrite, region),
                            subRangeToRead,
                            readerWithOffset(reader, fileRegion, Math.toIntExact(rangeToRead.start() - regionStart)),
                            metricRecordingWriter(
                                writerWithOffset(writer, fileRegion, Math.toIntExact(rangeToWrite.start() - regionStart))
                            ),
                            ioExecutor,
                            regionListener
                        );
                    } catch (Exception e) {
                        assert e instanceof AlreadyClosedException : e;
                        regionListener.onFailure(e);
                    }
                }
            }
            return regions.stream()
                .mapToLong(fr -> fr.tracker.getAbsentBytesWithin(mapSubRangeToRegion(rangeToRead, fr.regionKey.region())))
                .sum();
        }

        /**
         * Record a wait with the give number of bytes, with duration of the wait for the future given. This method will
         * wait for the `future` to complete.
         * @param bytes The bytes to record
         * @param future the future to wait for.
         * @return the result of the future.get()
         */
        public <T> T recordWait(long bytes, PlainActionFuture<T> future) throws InterruptedException, ExecutionException {
            try (var dummy = cacheMissMetricHandler.record(bytes)) {
                return future.get();
            }
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

        private RangeMissingHandler metricRecordingWriter(RangeMissingHandler writer) {
            return new DelegatingRangeMissingHandler(writer) {
                @Override
                public SourceInputStreamFactory sharedInputStreamFactory(List<SparseFileTracker.Gap> gaps) {
                    blobCacheMetrics.recordMiss();
                    return super.sharedInputStreamFactory(gaps);
                }
            };
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

    public CacheFile getCacheFile(KeyType cacheKey, long length, CacheMissHandler cacheMissHandler) {
        return getCacheFile(cacheKey, length, cacheMissHandler, UNKNOWN_TIMESTAMP);
    }

    public CacheFile getCacheFile(KeyType cacheKey, long length, CacheMissHandler cacheMissHandler, long timestampMillis) {
        return new CacheFile(cacheKey, length, cacheMissHandler, timestampMillis);
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
        long readBytes,
        // miss-count not exposed in REST API for now
        long missCount
    ) {
        public static final Stats EMPTY = new Stats(0, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L);
    }

    private class LFUCache implements Cache<KeyType, CacheFileRegion<KeyType>> {

        /**
         * Per-frequency level: holds the count of entries and the head of the doubly-linked list for that level.
         */
        private class FreqLevel {
            int count;
            LFUCacheEntry head;
        }

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

        private final KeyMapping<ShardId, RegionKey<KeyType>, LFUCacheEntry> keyMapping = new KeyMapping<>();
        private final FreqLevel[] freqs;
        private final int maxFreq;
        private final int freq0DecayScheduleThreshold;
        private final DecayAndNewEpochTask decayAndNewEpochTask;

        private final AtomicLong epoch = new AtomicLong();
        private final AtomicLong initialFreeRegions = new AtomicLong();
        private final long initialDecayPollCount;

        private final EvictionPolicy<KeyType> evictionPolicy;

        @SuppressWarnings("unchecked")
        LFUCache(Settings settings, EvictionPolicy<KeyType> evictionPolicy) {
            this.maxFreq = SHARED_CACHE_MAX_FREQ_SETTING.get(settings);
            this.freqs = (FreqLevel[]) Array.newInstance(FreqLevel.class, maxFreq);
            for (int i = 0; i < maxFreq; i++) {
                freqs[i] = new FreqLevel();
            }
            float thresholdRatio = SHARED_CACHE_DECAY_FREQ0_THRESHOLD_RATIO_SETTING.get(settings);
            this.freq0DecayScheduleThreshold = Math.max(1, (int) (numRegions * thresholdRatio));
            this.decayAndNewEpochTask = new DecayAndNewEpochTask(threadPool.generic());
            int initialDecays = SHARED_CACHE_INITIAL_DECAYS_SETTING.get(settings);
            if (initialDecays > 0) {
                initialDecayPollCount = Math.max(numRegions / initialDecays, 1);
                initialFreeRegions.set(numRegions);
            } else {
                initialDecayPollCount = 0;
            }
            // If EvictionPolicy requires access to FreqLevel[] then we could use some factory here to pass down the freqs array while
            // instantiating the EvictionPolicy, eg. this.evictionPolicy = evictionPolicyFactory.create(FreqLevel[], maxFreq);
            this.evictionPolicy = Objects.requireNonNull(evictionPolicy);
        }

        @Override
        public void close() {
            decayAndNewEpochTask.close();
        }

        // used by tests
        int getFreq(CacheFileRegion<KeyType> cacheFileRegion) {
            return keyMapping.get(cacheFileRegion.regionKey.file().shardId(), cacheFileRegion.regionKey).freq;
        }

        @Override
        public LFUCacheEntry get(KeyType cacheKey, long fileLength, int region, long timestampMillis) {
            final var regionKey = new RegionKey<>(cacheKey, region);
            final long now = epoch.get();
            // try to just get from the map on the fast-path to save instantiating the capturing lambda needed on the slow path
            // if we did not find an entry
            var entry = keyMapping.get(cacheKey.shardId(), regionKey);
            if (entry == null) {
                final int effectiveRegionSize = computeCacheFileRegionSize(fileLength, region);
                entry = keyMapping.computeIfAbsent(
                    cacheKey.shardId(),
                    regionKey,
                    key -> new LFUCacheEntry(
                        new CacheFileRegion<KeyType>(SharedBlobCacheService.this, key, effectiveRegionSize, timestampMillis),
                        now
                    )
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

            // existing item, check if we need to promote it
            if (now > entry.lastAccessedEpoch) {
                maybePromote(now, entry);
            }

            return entry;
        }

        @Override
        @Nullable
        public LFUCacheEntry getIfPresent(KeyType cacheKey, int region) {
            final var regionKey = new RegionKey<>(cacheKey, region);
            final long now = epoch.get();
            var entry = keyMapping.get(cacheKey.shardId(), regionKey);
            if (entry == null) {
                return null;
            }
            // If the IO slot has not been assigned yet, the entry is still being initialized.
            // Treat it as absent.
            if (entry.chunk.volatileIO() == null) {
                return null;
            }
            assert assertChunkActiveOrEvicted(entry);

            // existing item, check if we need to promote it
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
            return forceEvictEntries(null, matchingEntries);
        }

        private boolean removeKeyMappingForEntry(LFUCacheEntry entry) {
            return keyMapping.remove(entry.chunk.regionKey.file().shardId(), entry.chunk.regionKey, entry);
        }

        private void unlinkAndRemoveForEviction(LFUCacheEntry entry) {
            assert Thread.holdsLock(SharedBlobCacheService.this);
            unlink(entry);
            removeKeyMappingForEntry(entry);
            evictionPolicy.onEvicted(entry.chunk);
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

        @Override
        public int forceEvict(ShardId shard, Predicate<KeyType> cacheKeyPredicate) {
            final List<LFUCacheEntry> matchingEntries = new ArrayList<>();
            keyMapping.forEach(shard, (key, entry) -> {
                if (cacheKeyPredicate.test(key.file)) {
                    matchingEntries.add(entry);
                }
            });
            return forceEvictEntries(shard, matchingEntries);
        }

        @Override
        public int forceEvict(ShardId shard, BiPredicate<KeyType, Integer> regionPredicate) {
            final List<LFUCacheEntry> matchingEntries = new ArrayList<>();
            keyMapping.forEach(shard, (key, entry) -> {
                if (regionPredicate.test(key.file, key.region)) {
                    matchingEntries.add(entry);
                }
            });
            return forceEvictEntries(shard, matchingEntries);
        }

        private int forceEvictEntries(@Nullable final ShardId shardId, final List<LFUCacheEntry> matchingEntries) {
            assert matchingEntries != null;

            var evictedCount = 0;
            var nonZeroFrequencyEvictedCount = 0;
            if (matchingEntries.isEmpty() == false) {
                final long afterLockNanoTime;
                final long beforeLockNanoTime = relativeNanosProvider.getAsLong();
                synchronized (SharedBlobCacheService.this) {
                    afterLockNanoTime = relativeNanosProvider.getAsLong();
                    for (LFUCacheEntry entry : matchingEntries) {
                        int frequency = entry.freq;
                        boolean evicted = entry.chunk.forceEvict();
                        if (evicted && entry.chunk.volatileIO() != null) {
                            assert shardId == null || shardId.equals(entry.chunk.regionKey.file.shardId())
                                : shardId + " != " + entry.chunk.regionKey.file.shardId();
                            unlinkAndRemoveForEviction(entry);
                            evictedCount++;
                            if (frequency > 0) {
                                nonZeroFrequencyEvictedCount++;
                            }
                        }
                    }
                }
                blobCacheMetrics.recordLockAcquire(afterLockNanoTime - beforeLockNanoTime, ForceEvict);
            }
            blobCacheMetrics.getEvictedCountNonZeroFrequency().incrementBy(nonZeroFrequencyEvictedCount);
            return evictedCount;
        }

        @Override
        public int demoteAll(ShardId shard) {
            final List<LFUCacheEntry> matchingEntries = new ArrayList<>();
            keyMapping.forEach(shard, (key, entry) -> matchingEntries.add(entry));

            var demotedCount = 0;
            if (matchingEntries.isEmpty() == false) {
                final long afterLockNanoTime;
                final long beforeLockNanoTime = relativeNanosProvider.getAsLong();
                synchronized (SharedBlobCacheService.this) {
                    afterLockNanoTime = relativeNanosProvider.getAsLong();
                    for (LFUCacheEntry entry : matchingEntries) {
                        if (entry.freq == 0 || entry.chunk.isEvicted() || entry.chunk.volatileIO() == null) {
                            continue;
                        }
                        unlink(entry);
                        entry.freq = 0;
                        entry.lastAccessedEpoch = -1;
                        pushEntryToFront(entry);
                        demotedCount++;
                    }
                }
                blobCacheMetrics.recordLockAcquire(afterLockNanoTime - beforeLockNanoTime, Demote);
            }
            if (demotedCount > 0) {
                logger.debug("{} demoted [{}] cache regions to frequency 0", shard, demotedCount);
            }
            return demotedCount;
        }

        private LFUCacheEntry initChunk(LFUCacheEntry entry) {
            assert Thread.holdsLock(entry.chunk);
            RegionKey<KeyType> regionKey = entry.chunk.regionKey;
            if (keyMapping.get(regionKey.file().shardId(), regionKey) != entry) {
                throwAlreadyClosed("no free region found (contender)");
            }
            // new item
            assert entry.freq == 1;
            assert entry.prev == null;
            assert entry.next == null;
            final SharedBytes.IO freeSlot = pollFreeRegionAndMaybeDecay();
            if (freeSlot != null) {
                // no need to evict an item, just add
                assignToSlot(entry, freeSlot);
            } else {
                // need to evict something
                SharedBytes.IO io;
                final long afterLockNanoTime;
                final long beforeLockNanoTime = relativeNanosProvider.getAsLong();
                synchronized (SharedBlobCacheService.this) {
                    afterLockNanoTime = relativeNanosProvider.getAsLong();
                    io = maybeEvictAndTake(entry, evictIncrementer);
                }
                blobCacheMetrics.recordLockAcquire(afterLockNanoTime - beforeLockNanoTime, CacheMissEviction);
                if (io == null) {
                    io = freeRegions.poll();
                }
                if (io != null) {
                    assignToSlot(entry, io);
                } else {
                    boolean removed = removeKeyMappingForEntry(entry);
                    assert removed;
                    throwAlreadyClosed("no free region found");
                }
            }
            return entry;
        }

        private SharedBytes.IO pollFreeRegionAndMaybeDecay() {
            if (initialFreeRegions.getOpaque() > 0) {
                if (initialFreeRegions.decrementAndGet() % initialDecayPollCount == 0) {
                    maybeScheduleDecayAndNewEpoch(epoch.get());
                }
            }
            return freeRegions.poll();
        }

        private void assignToSlot(LFUCacheEntry entry, SharedBytes.IO freeSlot) {
            assert regionOwners.put(freeSlot, entry.chunk) == null;
            final long afterLockNanoTime;
            final long beforeLockNanoTime = relativeNanosProvider.getAsLong();
            synchronized (SharedBlobCacheService.this) {
                afterLockNanoTime = relativeNanosProvider.getAsLong();
                if (entry.chunk.isEvicted()) {
                    assert regionOwners.remove(freeSlot) == entry.chunk;
                    freeRegions.add(freeSlot);
                    removeKeyMappingForEntry(entry);
                    // record before throwing so we still sample
                    blobCacheMetrics.recordLockAcquire(afterLockNanoTime - beforeLockNanoTime, SlotAssignment);
                    throwAlreadyClosed("evicted during free region allocation");
                }
                pushEntryToBack(entry);
                // assign io only when chunk is ready for use. Under lock to avoid concurrent tryEvict.
                entry.chunk.volatileIO(freeSlot);
                evictionPolicy.onCached(entry.chunk);
            }
            blobCacheMetrics.recordLockAcquire(afterLockNanoTime - beforeLockNanoTime, SlotAssignment);
        }

        private void pushEntryToBack(final LFUCacheEntry entry) {
            pushEntry(entry, false);
        }

        private void pushEntryToFront(final LFUCacheEntry entry) {
            pushEntry(entry, true);
        }

        private void pushEntry(final LFUCacheEntry entry, boolean toFront) {
            assert Thread.holdsLock(SharedBlobCacheService.this);
            assert invariant(entry, false);
            assert entry.prev == null;
            assert entry.next == null;
            final FreqLevel level = freqs[entry.freq];
            assert level != null : entry.freq;
            final LFUCacheEntry currFront = level.head;
            if (currFront == null) {
                level.head = entry;
                entry.prev = entry;
                entry.next = null;
            } else {
                assert currFront.freq == entry.freq;
                if (toFront) {
                    entry.next = currFront;
                    entry.prev = currFront.prev;
                    currFront.prev = entry;
                    level.head = entry;
                } else {
                    final LFUCacheEntry last = currFront.prev;
                    currFront.prev = entry;
                    last.next = entry;
                    entry.prev = last;
                    entry.next = null;
                }
            }
            level.count++;
            assert toFront == false || freqs[entry.freq].head == entry;
            assert toFront || freqs[entry.freq].head.prev == entry;
            assert freqs[entry.freq].head.prev != null;
            assert entry.prev != null;
            assert entry.prev.next == null || entry.prev.next == entry;
            assert toFront || entry.next == null;
            assert invariant(entry, true);
        }

        private synchronized boolean invariant(final LFUCacheEntry e, boolean present) {
            boolean found = false;
            for (int i = 0; i < maxFreq; i++) {
                FreqLevel level = freqs[i];
                assert level.head == null || level.head.prev != null;
                assert level.head == null || level.head.prev != level.head || level.head.next == null;
                assert level.head == null || level.head.prev.next == null;
                for (LFUCacheEntry entry = level.head; entry != null; entry = entry.next) {
                    assert entry.next == null || entry.next.prev == entry;
                    assert entry.prev != null;
                    assert entry.prev.next == null || entry.prev.next == entry;
                    assert entry.freq == i;
                    if (entry == e) {
                        found = true;
                    }
                }
                for (LFUCacheEntry entry = level.head; entry != null && entry.prev != level.head; entry = entry.prev) {
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
            final long afterLockNanoTime;
            final long beforeLockNanoTime = relativeNanosProvider.getAsLong();
            synchronized (SharedBlobCacheService.this) {
                afterLockNanoTime = relativeNanosProvider.getAsLong();
                if (epoch > entry.lastAccessedEpoch && entry.freq < maxFreq - 1 && entry.chunk.isEvicted() == false) {
                    unlink(entry);
                    // go 2 up per epoch, allowing us to decay 1 every epoch.
                    entry.freq = Math.min(entry.freq + 2, maxFreq - 1);
                    entry.lastAccessedEpoch = epoch;
                    pushEntryToBack(entry);
                }
            }
            blobCacheMetrics.recordLockAcquire(afterLockNanoTime - beforeLockNanoTime, Promote);
        }

        private void unlink(final LFUCacheEntry entry) {
            assert Thread.holdsLock(SharedBlobCacheService.this);
            assert invariant(entry, true);
            assert entry.prev != null;
            final FreqLevel level = freqs[entry.freq];
            final LFUCacheEntry currFront = level.head;
            assert currFront != null;
            if (currFront == entry) {
                level.head = entry.next;
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
            level.count--;
            assert level.count >= 0 && level.count <= numRegions;
            entry.next = null;
            entry.prev = null;
            assert invariant(entry, false);
        }

        private void appendLevel1ToLevel0() {
            assert Thread.holdsLock(SharedBlobCacheService.this);
            FreqLevel level0 = freqs[0];
            FreqLevel level1 = freqs[1];
            var front0 = level0.head;
            var front1 = level1.head;
            if (front0 == null) {
                level0.head = front1;
                level0.count = level1.count;
                level1.head = null;
                level1.count = 0;
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

                level0.count += level1.count;
                level1.head = null;
                level1.count = 0;

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
        private SharedBytes.IO maybeEvictAndTake(final LFUCacheEntry incoming, final Runnable evictedNotification) {
            assert Thread.holdsLock(SharedBlobCacheService.this);
            final long startNanos = relativeNanosProvider.getAsLong();
            final int[] entriesScanned = new int[1];
            final long currentEpoch = epoch.get(); // must be captured before attempting to evict a freq 0
            final Predicate<CacheRegion<KeyType>> canEvict = evictionPolicy.createPredicate(incoming.chunk);
            SharedBytes.IO result = maybeEvictAndTakeForFrequency(incoming, evictedNotification, 0, entriesScanned, canEvict);
            if (freqs[0].count < freq0DecayScheduleThreshold && freeRegions.isEmpty()) {
                maybeScheduleDecayAndNewEpoch(currentEpoch);
            }
            if (result != null) {
                logAndMetricEvictionScan(relativeNanosProvider.getAsLong() - startNanos, AllFrequencies, Evicted, entriesScanned[0]);
                return result;
            }
            for (int currentFreq = 1; currentFreq < maxFreq; currentFreq++) {
                // recheck this per freq in case we raced an eviction with an incref'er.
                SharedBytes.IO freeRegion = freeRegions.poll();
                if (freeRegion != null) {
                    logAndMetricEvictionScan(relativeNanosProvider.getAsLong() - startNanos, AllFrequencies, Free, entriesScanned[0]);
                    return freeRegion;
                }
                result = maybeEvictAndTakeForFrequency(incoming, evictedNotification, currentFreq, entriesScanned, canEvict);
                if (result != null) {
                    logAndMetricEvictionScan(relativeNanosProvider.getAsLong() - startNanos, AllFrequencies, Evicted, entriesScanned[0]);
                    return result;
                }
            }
            logAndMetricEvictionScan(relativeNanosProvider.getAsLong() - startNanos, AllFrequencies, None, entriesScanned[0]);
            return null;
        }

        private void logAndMetricEvictionScan(
            final long elapsedNanos,
            final BlobCacheMetrics.EvictionScanMode evictionScanMode,
            final BlobCacheMetrics.EvictionScanOutcome outcome,
            final int entriesScanned
        ) {
            blobCacheMetrics.recordEvictionScan(elapsedNanos, entriesScanned, evictionScanMode, outcome);
            if (logger.isTraceEnabled()) {
                logger.trace(
                    "Eviction scan ({}) took {} across {} entries ({})",
                    evictionScanMode,
                    TimeValue.timeValueNanos(elapsedNanos),
                    entriesScanned,
                    outcome
                );
            }
        }

        private SharedBytes.IO maybeEvictAndTakeForFrequency(
            final LFUCacheEntry incoming,
            final Runnable evictedNotification,
            final int freq,
            final int[] entriesScanned,
            final Predicate<CacheRegion<KeyType>> canEvict
        ) {
            assert entriesScanned.length == 1 && entriesScanned[0] >= 0;
            for (LFUCacheEntry entry = freqs[freq].head; entry != null; entry = entry.next) {
                entriesScanned[0]++;
                if (canEvict.test(entry.chunk) == false) {
                    continue;
                }

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
                                unlinkAndRemoveForEviction(entry);
                            }
                        }
                    } finally {
                        entry.chunk.decRef();
                        if (freq > 0) {
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

        // used by tests
        long countCachedRegions(Predicate<KeyType> predicate) {
            return keyMapping.countMatchingKey2s(regionKey -> predicate.test(regionKey.file()));
        }

        // used by tests
        long countCachedRegions(ShardId shardId, BiPredicate<KeyType, Integer> regionPredicate) {
            final long[] count = new long[1];
            keyMapping.forEach(shardId, (key, entry) -> {
                if (regionPredicate.test(key.file(), key.region())) {
                    count[0]++;
                }
            });
            return count[0];
        }

        // used by tests
        Map<Integer, Integer> countCachedRegionsByFreq(Predicate<KeyType> predicate, boolean includeEvicted) {
            final Map<Integer, Integer> freqs = new HashMap<>();
            keyMapping.forEach((regionKey, entry) -> {
                if (predicate.test(regionKey.file()) && (includeEvicted || entry.chunk.isEvicted() == false)) {
                    freqs.merge(entry.freq, 1, Integer::sum);
                }
            });
            return Map.copyOf(freqs);
        }

        /**
         * This method tries to evict the least used {@link LFUCacheEntry}. Only entries with the lowest possible frequency are considered
         * for eviction.
         *
         * @return true if an entry was evicted, false otherwise.
         */
        private boolean maybeEvictLeastUsed(final CacheFileRegion<KeyType> incoming) {
            final long afterLockAndBeforeScanningNanoTime;
            int entriesScanned = 0;
            boolean found = false;
            final long beforeLockNanoTime = relativeNanosProvider.getAsLong();
            synchronized (SharedBlobCacheService.this) {
                afterLockAndBeforeScanningNanoTime = relativeNanosProvider.getAsLong();
                final Predicate<CacheRegion<KeyType>> canEvict = evictionPolicy.createPredicate(incoming);
                for (LFUCacheEntry entry = freqs[0].head; entry != null; entry = entry.next) {
                    entriesScanned++;
                    if (canEvict.test(entry.chunk) == false) {
                        continue;
                    }

                    boolean evicted = entry.chunk.tryEvict();
                    if (evicted && entry.chunk.volatileIO() != null) {
                        unlinkAndRemoveForEviction(entry);
                        found = true;
                        break;
                    }
                }
            }
            blobCacheMetrics.recordLockAcquire(afterLockAndBeforeScanningNanoTime - beforeLockNanoTime, LowestFrequencyEviction);
            logAndMetricEvictionScan(
                relativeNanosProvider.getAsLong() - afterLockAndBeforeScanningNanoTime,
                LowestFrequency,
                found ? Evicted : None,
                entriesScanned
            );
            return found;
        }

        private void computeDecay() {
            long now = threadPool.rawRelativeTimeInMillis();
            long afterLock;
            long end;
            final long afterLockNanoTime;
            final long beforeLockNanoTime = relativeNanosProvider.getAsLong();
            synchronized (SharedBlobCacheService.this) {
                afterLockNanoTime = relativeNanosProvider.getAsLong();
                afterLock = threadPool.rawRelativeTimeInMillis();
                appendLevel1ToLevel0();
                for (int i = 2; i < maxFreq; i++) {
                    assert freqs[i - 1].head == null;
                    freqs[i - 1].head = freqs[i].head;
                    freqs[i - 1].count = freqs[i].count;
                    freqs[i].head = null;
                    freqs[i].count = 0;
                    decrementFreqList(freqs[i - 1].head);
                    assert freqs[i - 1].head == null || invariant(freqs[i - 1].head, true);
                }
            }
            blobCacheMetrics.recordLockAcquire(afterLockNanoTime - beforeLockNanoTime, Decay);
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
                blobCacheMetrics.recordEpochChange();
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

    private static String getFileExtension(String resourceDescription) {
        // TODO: consider introspecting resourceDescription for compound files
        if (resourceDescription.endsWith(LuceneFilesExtensions.CFS.getExtension())) {
            return LuceneFilesExtensions.CFS.getExtension();
        }
        String extension = IndexFileNames.getExtension(resourceDescription);
        if (LuceneFilesExtensions.isLuceneExtension(extension)) {
            return extension;
        } else {
            return NON_LUCENE_EXTENSION_TO_RECORD;
        }
    }
}
