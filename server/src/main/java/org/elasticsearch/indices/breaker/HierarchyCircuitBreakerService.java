/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.breaker;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.breaker.ChildMemoryCircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.monitor.jvm.GcNames;
import org.elasticsearch.monitor.jvm.JvmInfo;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.LongSupplier;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.indices.breaker.BreakerSettings.CIRCUIT_BREAKER_LIMIT_SETTING;
import static org.elasticsearch.indices.breaker.BreakerSettings.CIRCUIT_BREAKER_OVERHEAD_SETTING;

/**
 * CircuitBreakerService that attempts to redistribute space between breakers
 * if tripped
 */
public class HierarchyCircuitBreakerService extends CircuitBreakerService {
    private static final Logger logger = LogManager.getLogger(HierarchyCircuitBreakerService.class);

    private static final String CHILD_LOGGER_PREFIX = "org.elasticsearch.indices.breaker.";

    private static final MemoryMXBean MEMORY_MX_BEAN = ManagementFactory.getMemoryMXBean();

    private final Map<String, CircuitBreaker> breakers;

    public static final Setting<Boolean> USE_REAL_MEMORY_USAGE_SETTING = Setting.boolSetting(
        "indices.breaker.total.use_real_memory",
        true,
        Property.Dynamic,
        Property.NodeScope
    );

    public static final Setting<ByteSizeValue> TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING = Setting.memorySizeSetting(
        "indices.breaker.total.limit",
        settings -> {
            if (USE_REAL_MEMORY_USAGE_SETTING.get(settings)) {
                return "95%";
            } else {
                return "70%";
            }
        },
        Property.Dynamic,
        Property.NodeScope
    );

    public static final Setting<ByteSizeValue> FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING = Setting.memorySizeSetting(
        "indices.breaker.fielddata.limit",
        "40%",
        Property.Dynamic,
        Property.NodeScope
    );
    public static final Setting<Double> FIELDDATA_CIRCUIT_BREAKER_OVERHEAD_SETTING = Setting.doubleSetting(
        "indices.breaker.fielddata.overhead",
        1.03d,
        0.0d,
        Property.Dynamic,
        Property.NodeScope
    );
    public static final Setting<CircuitBreaker.Type> FIELDDATA_CIRCUIT_BREAKER_TYPE_SETTING = new Setting<>(
        "indices.breaker.fielddata.type",
        "memory",
        CircuitBreaker.Type::parseValue,
        Property.NodeScope
    );

    public static final Setting<ByteSizeValue> REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING = Setting.memorySizeSetting(
        "indices.breaker.request.limit",
        "60%",
        Property.Dynamic,
        Property.NodeScope
    );
    public static final Setting<Double> REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING = Setting.doubleSetting(
        "indices.breaker.request.overhead",
        1.0d,
        0.0d,
        Property.Dynamic,
        Property.NodeScope
    );
    public static final Setting<CircuitBreaker.Type> REQUEST_CIRCUIT_BREAKER_TYPE_SETTING = new Setting<>(
        "indices.breaker.request.type",
        "memory",
        CircuitBreaker.Type::parseValue,
        Property.NodeScope
    );

    public static final Setting<ByteSizeValue> IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_LIMIT_SETTING = Setting.memorySizeSetting(
        "network.breaker.inflight_requests.limit",
        "100%",
        Property.Dynamic,
        Property.NodeScope
    );
    public static final Setting<Double> IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_OVERHEAD_SETTING = Setting.doubleSetting(
        "network.breaker.inflight_requests.overhead",
        2.0d,
        0.0d,
        Property.Dynamic,
        Property.NodeScope
    );
    public static final Setting<CircuitBreaker.Type> IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_TYPE_SETTING = new Setting<>(
        "network.breaker.inflight_requests.type",
        "memory",
        CircuitBreaker.Type::parseValue,
        Property.NodeScope
    );

    private volatile boolean trackRealMemoryUsage;
    private volatile BreakerSettings parentSettings;

    // Tripped count for when redistribution was attempted but wasn't successful
    private final AtomicLong parentTripCount = new AtomicLong(0);

    private final Function<Boolean, OverLimitStrategy> overLimitStrategyFactory;
    private volatile OverLimitStrategy overLimitStrategy;

    public HierarchyCircuitBreakerService(Settings settings, List<BreakerSettings> customBreakers, ClusterSettings clusterSettings) {
        this(settings, customBreakers, clusterSettings, HierarchyCircuitBreakerService::createOverLimitStrategy);
    }

    HierarchyCircuitBreakerService(
        Settings settings,
        List<BreakerSettings> customBreakers,
        ClusterSettings clusterSettings,
        Function<Boolean, OverLimitStrategy> overLimitStrategyFactory
    ) {
        super();
        HashMap<String, CircuitBreaker> childCircuitBreakers = new HashMap<>();
        childCircuitBreakers.put(
            CircuitBreaker.FIELDDATA,
            validateAndCreateBreaker(
                new BreakerSettings(
                    CircuitBreaker.FIELDDATA,
                    FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING.get(settings).getBytes(),
                    FIELDDATA_CIRCUIT_BREAKER_OVERHEAD_SETTING.get(settings),
                    FIELDDATA_CIRCUIT_BREAKER_TYPE_SETTING.get(settings),
                    CircuitBreaker.Durability.PERMANENT
                )
            )
        );
        childCircuitBreakers.put(
            CircuitBreaker.IN_FLIGHT_REQUESTS,
            validateAndCreateBreaker(
                new BreakerSettings(
                    CircuitBreaker.IN_FLIGHT_REQUESTS,
                    IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_LIMIT_SETTING.get(settings).getBytes(),
                    IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_OVERHEAD_SETTING.get(settings),
                    IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_TYPE_SETTING.get(settings),
                    CircuitBreaker.Durability.TRANSIENT
                )
            )
        );
        childCircuitBreakers.put(
            CircuitBreaker.REQUEST,
            validateAndCreateBreaker(
                new BreakerSettings(
                    CircuitBreaker.REQUEST,
                    REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.get(settings).getBytes(),
                    REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING.get(settings),
                    REQUEST_CIRCUIT_BREAKER_TYPE_SETTING.get(settings),
                    CircuitBreaker.Durability.TRANSIENT
                )
            )
        );
        for (BreakerSettings breakerSettings : customBreakers) {
            if (childCircuitBreakers.containsKey(breakerSettings.getName())) {
                throw new IllegalArgumentException(
                    "More than one circuit breaker with the name ["
                        + breakerSettings.getName()
                        + "] exists. Circuit breaker names must be unique"
                );
            }
            childCircuitBreakers.put(breakerSettings.getName(), validateAndCreateBreaker(breakerSettings));
        }
        this.breakers = Map.copyOf(childCircuitBreakers);
        this.parentSettings = new BreakerSettings(
            CircuitBreaker.PARENT,
            TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING.get(settings).getBytes(),
            1.0,
            CircuitBreaker.Type.PARENT,
            null
        );
        logger.trace(() -> format("parent circuit breaker with settings %s", this.parentSettings));

        this.trackRealMemoryUsage = USE_REAL_MEMORY_USAGE_SETTING.get(settings);

        clusterSettings.addSettingsUpdateConsumer(
            TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING,
            this::setTotalCircuitBreakerLimit,
            HierarchyCircuitBreakerService::validateTotalCircuitBreakerLimit
        );
        clusterSettings.addSettingsUpdateConsumer(
            FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING,
            FIELDDATA_CIRCUIT_BREAKER_OVERHEAD_SETTING,
            (limit, overhead) -> updateCircuitBreakerSettings(CircuitBreaker.FIELDDATA, limit, overhead)
        );
        clusterSettings.addSettingsUpdateConsumer(
            IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_LIMIT_SETTING,
            IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_OVERHEAD_SETTING,
            (limit, overhead) -> updateCircuitBreakerSettings(CircuitBreaker.IN_FLIGHT_REQUESTS, limit, overhead)
        );
        clusterSettings.addSettingsUpdateConsumer(
            REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING,
            REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING,
            (limit, overhead) -> updateCircuitBreakerSettings(CircuitBreaker.REQUEST, limit, overhead)
        );
        clusterSettings.addAffixUpdateConsumer(
            CIRCUIT_BREAKER_LIMIT_SETTING,
            CIRCUIT_BREAKER_OVERHEAD_SETTING,
            (name, updatedValues) -> updateCircuitBreakerSettings(name, updatedValues.v1(), updatedValues.v2()),
            (s, t) -> {}
        );
        clusterSettings.addSettingsUpdateConsumer(USE_REAL_MEMORY_USAGE_SETTING, this::updateUseRealMemorySetting);

        this.overLimitStrategyFactory = overLimitStrategyFactory;
        this.overLimitStrategy = overLimitStrategyFactory.apply(this.trackRealMemoryUsage);
    }

    private void updateCircuitBreakerSettings(String name, ByteSizeValue newLimit, Double newOverhead) {
        CircuitBreaker childBreaker = breakers.get(name);
        if (childBreaker != null) {
            childBreaker.setLimitAndOverhead(newLimit.getBytes(), newOverhead);
            logger.info("Updated limit {} and overhead {} for {}", newLimit.getStringRep(), newOverhead, name);
        }
    }

    private static void validateTotalCircuitBreakerLimit(ByteSizeValue byteSizeValue) {
        BreakerSettings newParentSettings = new BreakerSettings(
            CircuitBreaker.PARENT,
            byteSizeValue.getBytes(),
            1.0,
            CircuitBreaker.Type.PARENT,
            null
        );
        validateSettings(new BreakerSettings[] { newParentSettings });
    }

    private void setTotalCircuitBreakerLimit(ByteSizeValue byteSizeValue) {
        this.parentSettings = new BreakerSettings(CircuitBreaker.PARENT, byteSizeValue.getBytes(), 1.0, CircuitBreaker.Type.PARENT, null);
    }

    public void updateUseRealMemorySetting(boolean trackRealMemoryUsage) {
        this.trackRealMemoryUsage = trackRealMemoryUsage;

        this.overLimitStrategy = overLimitStrategyFactory.apply(this.trackRealMemoryUsage);
    }

    public boolean isTrackRealMemoryUsage() {
        return trackRealMemoryUsage;
    }

    public OverLimitStrategy getOverLimitStrategy() {
        return overLimitStrategy;
    }

    /**
     * Validate that child settings are valid
     */
    public static void validateSettings(BreakerSettings[] childrenSettings) throws IllegalStateException {
        for (BreakerSettings childSettings : childrenSettings) {
            // If the child is disabled, ignore it
            if (childSettings.getLimit() == -1) {
                continue;
            }

            if (childSettings.getOverhead() < 0) {
                throw new IllegalStateException("Child breaker overhead " + childSettings + " must be non-negative");
            }
        }
    }

    @Override
    public CircuitBreaker getBreaker(String name) {
        return this.breakers.get(name);
    }

    @Override
    public AllCircuitBreakerStats stats() {
        List<CircuitBreakerStats> allStats = new ArrayList<>(this.breakers.size());
        // Gather the "estimated" count for the parent breaker by adding the
        // estimations for each individual breaker
        for (CircuitBreaker breaker : this.breakers.values()) {
            allStats.add(stats(breaker.getName()));
        }
        // Manually add the parent breaker settings since they aren't part of the breaker map
        allStats.add(
            new CircuitBreakerStats(CircuitBreaker.PARENT, parentSettings.getLimit(), memoryUsed(0L).totalUsage, 1.0, parentTripCount.get())
        );
        return new AllCircuitBreakerStats(allStats.toArray(CircuitBreakerStats[]::new));
    }

    @Override
    public CircuitBreakerStats stats(String name) {
        CircuitBreaker breaker = this.breakers.get(name);
        return new CircuitBreakerStats(
            breaker.getName(),
            breaker.getLimit(),
            breaker.getUsed(),
            breaker.getOverhead(),
            breaker.getTrippedCount()
        );
    }

    static class MemoryUsage {
        final long baseUsage;
        final long totalUsage;
        final long transientChildUsage;
        final long permanentChildUsage;

        MemoryUsage(final long baseUsage, final long totalUsage, final long transientChildUsage, final long permanentChildUsage) {
            this.baseUsage = baseUsage;
            this.totalUsage = totalUsage;
            this.transientChildUsage = transientChildUsage;
            this.permanentChildUsage = permanentChildUsage;
        }
    }

    private MemoryUsage memoryUsed(long newBytesReserved) {
        long transientUsage = 0;
        long permanentUsage = 0;

        for (CircuitBreaker breaker : this.breakers.values()) {
            long breakerUsed = (long) (breaker.getUsed() * breaker.getOverhead());
            if (breaker.getDurability() == CircuitBreaker.Durability.TRANSIENT) {
                transientUsage += breakerUsed;
            } else if (breaker.getDurability() == CircuitBreaker.Durability.PERMANENT) {
                permanentUsage += breakerUsed;
            }
        }
        if (this.trackRealMemoryUsage) {
            final long current = currentMemoryUsage();
            return new MemoryUsage(current, current + newBytesReserved, transientUsage, permanentUsage);
        } else {
            long parentEstimated = transientUsage + permanentUsage;
            return new MemoryUsage(parentEstimated, parentEstimated, transientUsage, permanentUsage);
        }
    }

    // package private to allow overriding it in tests
    long currentMemoryUsage() {
        return realMemoryUsage();
    }

    static long realMemoryUsage() {
        try {
            return MEMORY_MX_BEAN.getHeapMemoryUsage().getUsed();
        } catch (IllegalArgumentException ex) {
            // This exception can happen (rarely) due to a race condition in the JVM when determining usage of memory pools. We do not want
            // to fail requests because of this and thus return zero memory usage in this case. While we could also return the most
            // recently determined memory usage, we would overestimate memory usage immediately after a garbage collection event.
            assert ex.getMessage().matches("committed = \\d+ should be < max = \\d+");
            logger.info("Cannot determine current memory usage due to JDK-8207200.", ex);
            return 0;
        }
    }

    public long getParentLimit() {
        return this.parentSettings.getLimit();
    }

    /**
     * Checks whether the parent breaker has been tripped
     */
    public void checkParentLimit(long newBytesReserved, String label) throws CircuitBreakingException {
        final MemoryUsage memoryUsed = memoryUsed(newBytesReserved);
        long parentLimit = this.parentSettings.getLimit();
        if (memoryUsed.totalUsage > parentLimit && overLimitStrategy.overLimit(memoryUsed).totalUsage > parentLimit) {
            this.parentTripCount.incrementAndGet();
            final String messageString = buildParentTripMessage(
                newBytesReserved,
                label,
                memoryUsed,
                parentLimit,
                this.trackRealMemoryUsage,
                this.breakers
            );
            // derive durability of a tripped parent breaker depending on whether the majority of memory tracked by
            // child circuit breakers is categorized as transient or permanent.
            CircuitBreaker.Durability durability = memoryUsed.transientChildUsage >= memoryUsed.permanentChildUsage
                ? CircuitBreaker.Durability.TRANSIENT
                : CircuitBreaker.Durability.PERMANENT;
            logger.debug(() -> format("%s", messageString));
            throw new CircuitBreakingException(messageString, memoryUsed.totalUsage, parentLimit, durability);
        }
    }

    // exposed for tests
    static String buildParentTripMessage(
        long newBytesReserved,
        String label,
        MemoryUsage memoryUsed,
        long parentLimit,
        boolean trackRealMemoryUsage,
        Map<String, CircuitBreaker> breakers
    ) {
        final var message = new StringBuilder();
        message.append("[parent] Data too large, data for [");
        message.append(label);
        message.append("] would be [");
        appendBytesSafe(message, memoryUsed.totalUsage);
        message.append("], which is larger than the limit of [");
        appendBytesSafe(message, parentLimit);
        message.append("]");
        if (trackRealMemoryUsage) {
            final long realUsage = memoryUsed.baseUsage;
            message.append(", real usage: [");
            appendBytesSafe(message, realUsage);
            message.append("], new bytes reserved: [");
            appendBytesSafe(message, newBytesReserved);
            message.append("]");
        }
        message.append(", usages [");
        breakers.forEach(new BiConsumer<>() {
            private boolean first = true;

            @Override
            public void accept(String key, CircuitBreaker breaker) {
                if (first) {
                    first = false;
                } else {
                    message.append(", ");
                }
                message.append(key).append("=");
                appendBytesSafe(message, (long) (breaker.getUsed() * breaker.getOverhead()));
            }
        });
        message.append("]");
        return message.toString();
    }

    static void appendBytesSafe(StringBuilder stringBuilder, long bytes) {
        stringBuilder.append(bytes);
        if (-1L <= bytes) {
            stringBuilder.append("/");
            stringBuilder.append(ByteSizeValue.ofBytes(bytes));
        } else {
            // Something's definitely wrong, maybe a breaker was freed twice? Still, we're just creating an exception message here, so we
            // should keep going if we're running in production.
            logger.error("negative value in circuit breaker: {}", stringBuilder);
            assert permitNegativeValues : stringBuilder.toString();
        }
    }

    private CircuitBreaker validateAndCreateBreaker(BreakerSettings breakerSettings) {
        // Validate the settings
        validateSettings(new BreakerSettings[] { breakerSettings });
        return breakerSettings.getType() == CircuitBreaker.Type.NOOP
            ? new NoopCircuitBreaker(breakerSettings.getName())
            : new ChildMemoryCircuitBreaker(
                breakerSettings,
                LogManager.getLogger(CHILD_LOGGER_PREFIX + breakerSettings.getName()),
                this,
                breakerSettings.getName()
            );
    }

    static OverLimitStrategy createOverLimitStrategy(boolean trackRealMemoryUsage) {
        JvmInfo jvmInfo = JvmInfo.jvmInfo();
        if (trackRealMemoryUsage && jvmInfo.useG1GC().equals("true")
        // messing with GC is "dangerous" so we apply an escape hatch. Not intended to be used.
            && Booleans.parseBoolean(System.getProperty("es.real_memory_circuit_breaker.g1_over_limit_strategy.enabled"), true)) {
            TimeValue lockTimeout = TimeValue.timeValueMillis(
                Integer.parseInt(System.getProperty("es.real_memory_circuit_breaker.g1_over_limit_strategy.lock_timeout_ms", "500"))
            );
            // hardcode interval, do not want any tuning of it outside code changes.
            return new G1OverLimitStrategy(
                jvmInfo,
                HierarchyCircuitBreakerService::realMemoryUsage,
                createYoungGcCountSupplier(),
                System::currentTimeMillis,
                5000,
                lockTimeout
            );
        } else {
            return memoryUsed -> memoryUsed;
        }
    }

    static LongSupplier createYoungGcCountSupplier() {
        List<GarbageCollectorMXBean> youngBeans = ManagementFactory.getGarbageCollectorMXBeans()
            .stream()
            .filter(mxBean -> GcNames.getByGcName(mxBean.getName(), mxBean.getName()).equals(GcNames.YOUNG))
            .toList();
        assert youngBeans.size() == 1;
        assert youngBeans.get(0).getCollectionCount() != -1 : "G1 must support getting collection count";

        if (youngBeans.size() == 1) {
            return youngBeans.get(0)::getCollectionCount;
        } else {
            logger.warn("Unable to find young generation collector, G1 over limit strategy might be impacted [{}]", youngBeans);
            return () -> -1;
        }
    }

    interface OverLimitStrategy {
        MemoryUsage overLimit(MemoryUsage memoryUsed);
    }

    static class G1OverLimitStrategy implements OverLimitStrategy {
        private final long g1RegionSize;
        private final LongSupplier currentMemoryUsageSupplier;
        private final LongSupplier gcCountSupplier;
        private final LongSupplier timeSupplier;
        private final TimeValue lockTimeout;
        private final long maxHeap;

        private long lastCheckTime = Long.MIN_VALUE;
        private final long minimumInterval;

        private long blackHole;
        private final ReleasableLock lock = new ReleasableLock(new ReentrantLock());

        G1OverLimitStrategy(
            JvmInfo jvmInfo,
            LongSupplier currentMemoryUsageSupplier,
            LongSupplier gcCountSupplier,
            LongSupplier timeSupplier,
            long minimumInterval,
            TimeValue lockTimeout
        ) {
            this.lockTimeout = lockTimeout;
            assert minimumInterval > 0;
            this.currentMemoryUsageSupplier = currentMemoryUsageSupplier;
            this.gcCountSupplier = gcCountSupplier;
            this.timeSupplier = timeSupplier;
            this.minimumInterval = minimumInterval;
            this.maxHeap = jvmInfo.getMem().getHeapMax().getBytes();
            long g1RegionSize = jvmInfo.getG1RegionSize();
            if (g1RegionSize <= 0) {
                this.g1RegionSize = fallbackRegionSize(jvmInfo);
            } else {
                this.g1RegionSize = g1RegionSize;
            }
        }

        static long fallbackRegionSize(JvmInfo jvmInfo) {
            // mimick JDK calculation based on JDK 14 source:
            // https://hg.openjdk.java.net/jdk/jdk14/file/6c954123ee8d/src/hotspot/share/gc/g1/heapRegion.cpp#l65
            // notice that newer JDKs will have a slight variant only considering max-heap:
            // https://hg.openjdk.java.net/jdk/jdk/file/e7d0ec2d06e8/src/hotspot/share/gc/g1/heapRegion.cpp#l67
            // based on this JDK "bug":
            // https://bugs.openjdk.java.net/browse/JDK-8241670
            long averageHeapSize = (jvmInfo.getMem().getHeapMax().getBytes() + JvmInfo.jvmInfo().getMem().getHeapMax().getBytes()) / 2;
            long regionSize = Long.highestOneBit(averageHeapSize / 2048);
            if (regionSize < ByteSizeUnit.MB.toBytes(1)) {
                regionSize = ByteSizeUnit.MB.toBytes(1);
            } else if (regionSize > ByteSizeUnit.MB.toBytes(32)) {
                regionSize = ByteSizeUnit.MB.toBytes(32);
            }
            return regionSize;
        }

        @Override
        public MemoryUsage overLimit(MemoryUsage memoryUsed) {
            boolean leader = false;
            int allocationIndex = 0;
            long allocationDuration = 0;
            try (ReleasableLock locked = lock.tryAcquire(lockTimeout)) {
                if (locked != null) {
                    long begin = timeSupplier.getAsLong();
                    leader = begin >= lastCheckTime + minimumInterval;
                    overLimitTriggered(leader);
                    if (leader) {
                        long initialCollectionCount = gcCountSupplier.getAsLong();
                        logger.info("attempting to trigger G1GC due to high heap usage [{}]", memoryUsed.baseUsage);
                        long localBlackHole = 0;
                        // number of allocations, corresponding to (approximately) number of free regions + 1
                        int allocationCount = Math.toIntExact((maxHeap - memoryUsed.baseUsage) / g1RegionSize + 1);
                        // allocations of half-region size becomes single humongous alloc, thus taking up a full region.
                        int allocationSize = (int) (g1RegionSize >> 1);
                        long maxUsageObserved = memoryUsed.baseUsage;
                        for (; allocationIndex < allocationCount; ++allocationIndex) {
                            long current = currentMemoryUsageSupplier.getAsLong();
                            if (current >= maxUsageObserved) {
                                maxUsageObserved = current;
                            } else {
                                // we observed a memory drop, so some GC must have occurred
                                break;
                            }
                            if (initialCollectionCount != gcCountSupplier.getAsLong()) {
                                break;
                            }
                            localBlackHole += new byte[allocationSize].hashCode();
                        }

                        blackHole += localBlackHole;
                        logger.trace("black hole [{}]", blackHole);

                        long now = timeSupplier.getAsLong();
                        this.lastCheckTime = now;
                        allocationDuration = now - begin;
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                // fallthrough
            }

            final long current = currentMemoryUsageSupplier.getAsLong();
            if (current < memoryUsed.baseUsage) {
                if (leader) {
                    logger.info(
                        "GC did bring memory usage down, before [{}], after [{}], allocations [{}], duration [{}]",
                        memoryUsed.baseUsage,
                        current,
                        allocationIndex,
                        allocationDuration
                    );
                }
                return new MemoryUsage(
                    current,
                    memoryUsed.totalUsage - memoryUsed.baseUsage + current,
                    memoryUsed.transientChildUsage,
                    memoryUsed.permanentChildUsage
                );
            } else {
                if (leader) {
                    logger.info(
                        "GC did not bring memory usage down, before [{}], after [{}], allocations [{}], duration [{}]",
                        memoryUsed.baseUsage,
                        current,
                        allocationIndex,
                        allocationDuration
                    );
                }
                // prefer original measurement when reporting if heap usage was not brought down.
                return memoryUsed;
            }
        }

        void overLimitTriggered(boolean leader) {
            // for tests to override.
        }

        TimeValue getLockTimeout() {
            return lockTimeout;
        }
    }

    // exposed for testing
    static boolean permitNegativeValues = false;
}
