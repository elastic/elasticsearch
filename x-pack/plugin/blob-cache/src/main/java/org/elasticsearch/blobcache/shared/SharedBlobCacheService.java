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
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.IntConsumer;
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

    private interface Cache<K, T> extends Releasable {
        CacheEntry<T> get(K cacheKey, long fileLength, int region);

        int forceEvict(Predicate<K> cacheKeyPredicate);
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

    // executor to run bulk reading from the blobstore on
    private final Executor bulkIOExecutor;

    private final SharedBytes sharedBytes;
    private final long cacheSize;
    private final int regionSize;
    private final ByteSizeValue rangeSize;
    private final ByteSizeValue recoveryRangeSize;

    private final int numRegions;
    private final ConcurrentLinkedQueue<SharedBytes.IO> freeRegions = new ConcurrentLinkedQueue<>();

    private final Cache<KeyType, CacheFileRegion> cache;

    private final ConcurrentHashMap<SharedBytes.IO, CacheFileRegion> regionOwners; // to assert exclusive access of regions

    private final LongAdder writeCount = new LongAdder();
    private final LongAdder writeBytes = new LongAdder();

    private final LongAdder readCount = new LongAdder();
    private final LongAdder readBytes = new LongAdder();

    private final LongAdder evictCount = new LongAdder();

    private final BlobCacheMetrics blobCacheMetrics;

    public SharedBlobCacheService(
        NodeEnvironment environment,
        Settings settings,
        ThreadPool threadPool,
        String ioExecutor,
        BlobCacheMetrics blobCacheMetrics
    ) {
        this(environment, settings, threadPool, ioExecutor, ioExecutor, blobCacheMetrics);
    }

    public SharedBlobCacheService(
        NodeEnvironment environment,
        Settings settings,
        ThreadPool threadPool,
        String ioExecutor,
        String bulkExecutor,
        BlobCacheMetrics blobCacheMetrics
    ) {
        this.threadPool = threadPool;
        this.ioExecutor = threadPool.executor(ioExecutor);
        this.bulkIOExecutor = threadPool.executor(bulkExecutor);
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

        this.rangeSize = SHARED_CACHE_RANGE_SIZE_SETTING.get(settings);
        this.recoveryRangeSize = SHARED_CACHE_RECOVERY_RANGE_SIZE_SETTING.get(settings);

        this.blobCacheMetrics = blobCacheMetrics;
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
        return (int) (position / regionSize);
    }

    private int getRegionRelativePosition(long position) {
        return (int) (position % regionSize);
    }

    private long getRegionStart(int region) {
        return (long) region * regionSize;
    }

    private long getRegionEnd(int region) {
        return (long) (region + 1) * regionSize;
    }

    private int getEndingRegion(long position) {
        return getRegion(position - (position % regionSize == 0 ? 1 : 0));
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

    private int getRegionSize(long fileLength, int region) {
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

    CacheFileRegion get(KeyType cacheKey, long fileLength, int region) {
        return cache.get(cacheKey, fileLength, region).chunk;
    }

    /**
     * Fetch and cache the full blob for the given cache entry from the remote repository if there
     * are enough free pages in the cache to do so.
     *
     * This method returns as soon as the download tasks are instantiated, but the tasks themselves
     * are run on the bulk executor.
     *
     * If an exception is thrown from the writer then the cache entry being downloaded is freed
     * and unlinked
     *
     * @param cacheKey  the key to fetch data for
     * @param length    the length of the blob to fetch
     * @param writer    a writer that handles writing of newly downloaded data to the shared cache
     * @param listener  listener that is called once all downloading has finished
     *
     * @return {@code true} if there were enough free pages to start downloading the full entry
     */
    public boolean maybeFetchFullEntry(KeyType cacheKey, long length, RangeMissingHandler writer, ActionListener<Void> listener) {
        int finalRegion = getEndingRegion(length);
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
                final CacheFileRegion entry;
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
                    bulkIOExecutor,
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

    private static void throwAlreadyClosed(String message) {
        throw new AlreadyClosedException(message);
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

    // used by tests
    int getFreq(CacheFileRegion cacheFileRegion) {
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

    class CacheFileRegion extends EvictableRefCounted {

        final RegionKey<KeyType> regionKey;
        final SparseFileTracker tracker;
        volatile SharedBytes.IO io = null;

        CacheFileRegion(RegionKey<KeyType> regionKey, int regionSize) {
            this.regionKey = regionKey;
            assert regionSize > 0;
            tracker = new SparseFileTracker("file", regionSize);
        }

        public long physicalStartOffset() {
            var ioRef = io;
            return ioRef == null ? -1L : (long) regionKey.region * regionSize;
        }

        // tries to evict this chunk if noone is holding onto its resources anymore
        // visible for tests.
        boolean tryEvict() {
            assert Thread.holdsLock(SharedBlobCacheService.this) : "must hold lock when evicting";
            if (refCount() <= 1 && evict()) {
                logger.trace("evicted {} with channel offset {}", regionKey, physicalStartOffset());
                evictCount.increment();
                decRef();
                return true;
            }
            return false;
        }

        public boolean forceEvict() {
            assert Thread.holdsLock(SharedBlobCacheService.this) : "must hold lock when evicting";
            if (evict()) {
                logger.trace("force evicted {} with channel offset {}", regionKey, physicalStartOffset());
                evictCount.increment();
                decRef();
                return true;
            }
            return false;
        }

        @Override
        protected void closeInternal() {
            // now actually free the region associated with this chunk
            // we held the "this" lock when this was evicted, hence if io is not filled in, chunk will never be registered.
            if (io != null) {
                assert regionOwners.remove(io) == this;
                freeRegions.add(io);
            }
            logger.trace("closed {} with channel offset {}", regionKey, physicalStartOffset());
        }

        private void ensureOpen() {
            if (isEvicted()) {
                throwAlreadyEvicted();
            }
        }

        private static void throwAlreadyEvicted() {
            throwAlreadyClosed("File chunk is evicted");
        }

        boolean tryRead(ByteBuffer buf, long offset) throws IOException {
            int readBytes = io.read(buf, getRegionRelativePosition(offset));
            if (isEvicted()) {
                buf.position(buf.position() - readBytes);
                return false;
            }
            return true;
        }

        void populateAndRead(
            final ByteRange rangeToWrite,
            final ByteRange rangeToRead,
            final RangeAvailableHandler reader,
            final RangeMissingHandler writer,
            final Executor executor,
            final ActionListener<Integer> listener
        ) {
            Releasable resource = null;
            try {
                incRef();
                resource = Releasables.releaseOnce(this::decRef);
                ensureOpen();
                final List<SparseFileTracker.Gap> gaps = tracker.waitForRange(
                    rangeToWrite,
                    rangeToRead,
                    ActionListener.runBefore(listener, resource::close).delegateFailureAndWrap((l, success) -> {
                        var ioRef = io;
                        assert regionOwners.get(ioRef) == this;
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
                        readCount.increment();
                        l.onResponse(read);
                    })
                );

                if (gaps.isEmpty() == false) {
                    fillGaps(executor, writer, gaps);
                }
            } catch (Exception e) {
                releaseAndFail(listener, resource, e);
            }
        }

        private void fillGaps(Executor executor, RangeMissingHandler writer, List<SparseFileTracker.Gap> gaps) {
            for (SparseFileTracker.Gap gap : gaps) {
                executor.execute(new AbstractRunnable() {

                    @Override
                    protected void doRun() throws Exception {
                        assert CacheFileRegion.this.hasReferences();
                        ensureOpen();
                        final int start = Math.toIntExact(gap.start());
                        var ioRef = io;
                        assert regionOwners.get(ioRef) == CacheFileRegion.this;
                        writer.fillCacheRange(
                            ioRef,
                            start,
                            start,
                            Math.toIntExact(gap.end() - start),
                            progress -> gap.onProgress(start + progress)
                        );
                        writeCount.increment();

                        gap.onCompletion();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        gap.onFailure(e);
                    }
                });
            }
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

        private CacheEntry<CacheFileRegion> lastAccessedRegion;

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
            // We are interested in the total time that the system spends when fetching a result (including time spent queuing), so we start
            // our measurement here.
            final long startTime = threadPool.relativeTimeInMillis();
            RangeMissingHandler writerInstrumentationDecorator = (
                SharedBytes.IO channel,
                int channelPos,
                int relativePos,
                int length,
                IntConsumer progressUpdater) -> {
                writer.fillCacheRange(channel, channelPos, relativePos, length, progressUpdater);
                var elapsedTime = threadPool.relativeTimeInMillis() - startTime;
                SharedBlobCacheService.this.blobCacheMetrics.getCacheMissLoadTimes().record(elapsedTime);
                SharedBlobCacheService.this.blobCacheMetrics.getCacheMissCounter().increment();
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
            final CacheFileRegion fileRegion = get(cacheKey, length, region);
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
                        final CacheFileRegion fileRegion = get(cacheKey, length, region);
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

        private RangeMissingHandler writerWithOffset(RangeMissingHandler writer, CacheFileRegion fileRegion, int writeOffset) {
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

        private RangeAvailableHandler readerWithOffset(RangeAvailableHandler reader, CacheFileRegion fileRegion, int readOffset) {
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

        private boolean assertValidRegionAndLength(CacheFileRegion fileRegion, int channelPos, int len) {
            assert regionOwners.get(fileRegion.io) == fileRegion;
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
        // caller that wants to read from x should instead do a positional read from x + relativePos
        // caller should also only read up to length, further bytes will be offered by another call to this method
        int onRangeAvailable(SharedBytes.IO channel, int channelPos, int relativePos, int length) throws IOException;
    }

    @FunctionalInterface
    public interface RangeMissingHandler {
        void fillCacheRange(SharedBytes.IO channel, int channelPos, int relativePos, int length, IntConsumer progressUpdater)
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

    private class LFUCache implements Cache<KeyType, CacheFileRegion> {

        class LFUCacheEntry extends CacheEntry<CacheFileRegion> {
            LFUCacheEntry prev;
            LFUCacheEntry next;
            int freq;
            volatile long lastAccessed;

            LFUCacheEntry(CacheFileRegion chunk, long lastAccessed) {
                super(chunk);
                this.lastAccessed = lastAccessed;
            }

            void touch() {
                long now = threadPool.relativeTimeInMillis();
                if (now - lastAccessed >= minTimeDelta) {
                    maybePromote(now, this);
                }
            }
        }

        private final ConcurrentHashMap<RegionKey<KeyType>, LFUCacheEntry> keyMapping = new ConcurrentHashMap<>();
        private final LFUCacheEntry[] freqs;
        private final int maxFreq;
        private final long minTimeDelta;
        private final CacheDecayTask decayTask;

        @SuppressWarnings("unchecked")
        LFUCache(Settings settings) {
            this.maxFreq = SHARED_CACHE_MAX_FREQ_SETTING.get(settings);
            this.minTimeDelta = SHARED_CACHE_MIN_TIME_DELTA_SETTING.get(settings).millis();
            freqs = (LFUCacheEntry[]) Array.newInstance(LFUCacheEntry.class, maxFreq);
            decayTask = new CacheDecayTask(threadPool, threadPool.generic(), SHARED_CACHE_DECAY_INTERVAL_SETTING.get(settings));
            decayTask.rescheduleIfNecessary();
        }

        @Override
        public void close() {
            decayTask.close();
        }

        int getFreq(CacheFileRegion cacheFileRegion) {
            return keyMapping.get(cacheFileRegion.regionKey).freq;
        }

        @Override
        public LFUCacheEntry get(KeyType cacheKey, long fileLength, int region) {
            final RegionKey<KeyType> regionKey = new RegionKey<>(cacheKey, region);
            final long now = threadPool.relativeTimeInMillis();
            // try to just get from the map on the fast-path to save instantiating the capturing lambda needed on the slow path
            // if we did not find an entry
            var entry = keyMapping.get(regionKey);
            if (entry == null) {
                final int effectiveRegionSize = getRegionSize(fileLength, region);
                entry = keyMapping.computeIfAbsent(regionKey, key -> new LFUCacheEntry(new CacheFileRegion(key, effectiveRegionSize), now));
            }
            // io is volatile, double locking is fine, as long as we assign it last.
            if (entry.chunk.io == null) {
                synchronized (entry.chunk) {
                    if (entry.chunk.io == null) {
                        return initChunk(entry);
                    }
                }
            }
            assert assertChunkActiveOrEvicted(entry);

            // existing item, check if we need to promote item
            if (now - entry.lastAccessed >= minTimeDelta) {
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
                        if (evicted && entry.chunk.io != null) {
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

        private LFUCacheEntry initChunk(LFUCacheEntry entry) {
            assert Thread.holdsLock(entry.chunk);
            RegionKey<KeyType> regionKey = entry.chunk.regionKey;
            if (keyMapping.get(regionKey) != entry) {
                throwAlreadyClosed("no free region found (contender)");
            }
            // new item
            assert entry.freq == 0;
            assert entry.prev == null;
            assert entry.next == null;
            final SharedBytes.IO freeSlot = freeRegions.poll();
            if (freeSlot != null) {
                // no need to evict an item, just add
                assignToSlot(entry, freeSlot);
            } else {
                // need to evict something
                int frequency;
                synchronized (SharedBlobCacheService.this) {
                    frequency = maybeEvict();
                }
                if (frequency > 0) {
                    blobCacheMetrics.getEvictedCountNonZeroFrequency().increment();
                }
                final SharedBytes.IO freeSlotRetry = freeRegions.poll();
                if (freeSlotRetry != null) {
                    assignToSlot(entry, freeSlotRetry);
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
                entry.chunk.io = freeSlot;
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
            assert regionOwners.get(entry.chunk.io) == entry.chunk || entry.chunk.isEvicted();
            return true;
        }

        private void maybePromote(long now, LFUCacheEntry entry) {
            synchronized (SharedBlobCacheService.this) {
                if (now - entry.lastAccessed >= minTimeDelta && entry.freq + 1 < maxFreq && entry.chunk.isEvicted() == false) {
                    unlink(entry);
                    entry.freq++;
                    entry.lastAccessed = now;
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

        /**
         * Cycles through the {@link LFUCacheEntry} from 0 to max frequency and
         * tries to evict a chunk if no one is holding onto its resources anymore
         *
         * @return the frequency of the evicted entry as integer or -1 if no entry was evicted from cache
         */
        private int maybeEvict() {
            assert Thread.holdsLock(SharedBlobCacheService.this);
            for (int currentFreq = 0; currentFreq < maxFreq; currentFreq++) {
                for (LFUCacheEntry entry = freqs[currentFreq]; entry != null; entry = entry.next) {
                    boolean evicted = entry.chunk.tryEvict();
                    if (evicted && entry.chunk.io != null) {
                        unlink(entry);
                        keyMapping.remove(entry.chunk.regionKey, entry);
                        return currentFreq;
                    }
                }
            }
            return -1;
        }

        private void computeDecay() {
            synchronized (SharedBlobCacheService.this) {
                long now = threadPool.relativeTimeInMillis();
                for (int i = 0; i < maxFreq; i++) {
                    for (LFUCacheEntry entry = freqs[i]; entry != null; entry = entry.next) {
                        if (entry.freq > 0 && now - entry.lastAccessed >= 2 * minTimeDelta) {
                            unlink(entry);
                            entry.freq--;
                            pushEntryToBack(entry);
                        }
                    }
                }
            }
        }

        class CacheDecayTask extends AbstractAsyncTask {

            CacheDecayTask(ThreadPool threadPool, Executor executor, TimeValue interval) {
                super(logger, Objects.requireNonNull(threadPool), executor, Objects.requireNonNull(interval), true);
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
            public String toString() {
                return "shared_cache_decay_task";
            }
        }
    }
}
