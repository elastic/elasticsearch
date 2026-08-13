/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.breaker;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.breaker.ChildMemoryCircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreaker.Durability;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.MemorySizeValue;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * A {@link CircuitBreakerService} dedicated to off-heap (native) memory allocations. This service is
 * completely independent of the heap-denominated parent breaker in
 * {@link HierarchyCircuitBreakerService}: breakers registered here never consult the heap parent and
 * are never included in its estimated total, so heap and native budgets are expressed in the same
 * units as their respective limits.
 *
 * <p>Currently contains a single breaker, {@link CircuitBreaker#NATIVE_MEMORY}, which tracks Arrow
 * allocations via {@code BlockFactory}. Future consumers (Panama arenas, direct
 * {@code ByteBuffer}s) will be added as additional children once their accounting hooks are in place.
 *
 * <p>When a {@link ThreadPool} is supplied (production nodes), a {@link NativeMemoryCgroupBackstop}
 * is started alongside the accounting-based limit. The backstop periodically reads real cgroup
 * memory usage and refuses new allocations when usage exceeds
 * {@link NativeMemoryCgroupBackstop#HIGH_WATERMARK_SETTING}. This catches native consumers —
 * compression libraries, mmap, ML native processes, etc. — that are never charged to the circuit
 * breaker. On ML-enabled nodes the ML native process consumes memory from the same cgroup; its
 * budget is invisible to the accounting limit, but the backstop's cgroup reads see the combined
 * pressure and will gate Arrow allocations accordingly. Operators can additionally set an explicit
 * byte limit (rather than the default 50% ratio) to shrink the Arrow budget in proportion to ML's
 * expected footprint.
 */
public class NativeMemoryCircuitBreakerService extends CircuitBreakerService implements Closeable {

    private static final Logger logger = LogManager.getLogger(NativeMemoryCircuitBreakerService.class);
    private static final String CHILD_LOGGER_PREFIX = "org.elasticsearch.indices.breaker.";

    /**
     * The default limit — 50% of the native memory base (cgroup budget when in a container,
     * {@code -XX:MaxDirectMemorySize} otherwise). A percentage rather than an absolute value because
     * the right size depends on the deployment; 50% leaves room for other native consumers that are
     * not yet metered here.
     *
     * <p><b>ML co-location caveat:</b> when the machine learning plugin is enabled, its native
     * process runs inside the same cgroup and consumes memory from the same cgroup limit. That
     * budget is not deducted automatically because {@code server} cannot depend on x-pack/ml
     * settings. On nodes where ML jobs are expected to run alongside ES|QL workloads, set an
     * explicit byte value rather than a percentage to prevent the two budgets from overlapping
     * under the cgroup limit.
     *
     * @see org.elasticsearch.common.unit.NativeMemoryLimitCalculator#nativeMemoryBase()
     */
    public static final Setting<ByteSizeValue> NATIVE_MEMORY_CIRCUIT_BREAKER_LIMIT_SETTING = new Setting<>(
        "indices.breaker.native_memory.limit",
        "50%",
        s -> MemorySizeValue.parseBytesSizeValueOrDirectMemoryRatio(s, "indices.breaker.native_memory.limit"),
        Property.Dynamic,
        Property.NodeScope
    );

    public static final Setting<Double> NATIVE_MEMORY_CIRCUIT_BREAKER_OVERHEAD_SETTING = Setting.doubleSetting(
        "indices.breaker.native_memory.overhead",
        1.0d,
        0.0d,
        Property.Dynamic,
        Property.NodeScope
    );

    public static final Setting<CircuitBreaker.Type> NATIVE_MEMORY_CIRCUIT_BREAKER_TYPE_SETTING = new Setting<>(
        "indices.breaker.native_memory.type",
        "memory",
        CircuitBreaker.Type::parseValue,
        Property.NodeScope
    );

    private final CircuitBreaker nativeMemoryBreaker;
    private final NativeMemoryCgroupBackstop backstop;

    @SuppressWarnings("this-escape")
    public NativeMemoryCircuitBreakerService(
        CircuitBreakerMetrics metrics,
        Settings settings,
        ClusterSettings clusterSettings,
        ThreadPool threadPool
    ) {
        super();
        BreakerSettings breakerSettings = new BreakerSettings(
            CircuitBreaker.NATIVE_MEMORY,
            NATIVE_MEMORY_CIRCUIT_BREAKER_LIMIT_SETTING.get(settings).getBytes(),
            NATIVE_MEMORY_CIRCUIT_BREAKER_OVERHEAD_SETTING.get(settings),
            NATIVE_MEMORY_CIRCUIT_BREAKER_TYPE_SETTING.get(settings),
            CircuitBreaker.Durability.TRANSIENT
        );
        CircuitBreaker inner = createInnerBreaker(metrics, breakerSettings);
        logger.trace("created NativeMemoryCircuitBreakerService with settings {}", breakerSettings);

        clusterSettings.addSettingsUpdateConsumer(
            NATIVE_MEMORY_CIRCUIT_BREAKER_LIMIT_SETTING,
            NATIVE_MEMORY_CIRCUIT_BREAKER_OVERHEAD_SETTING,
            (limit, overhead) -> {
                inner.setLimitAndOverhead(limit.getBytes(), overhead);
                logger.info("Updated limit {} and overhead {} for {}", limit.getStringRep(), overhead, CircuitBreaker.NATIVE_MEMORY);
            }
        );

        // The backstop is only meaningful when the inner breaker actually enforces limits.
        // Skip it when type=noop so the noop setting genuinely disables all native-memory gating.
        if (threadPool != null && breakerSettings.getType() != CircuitBreaker.Type.NOOP) {
            this.backstop = new NativeMemoryCgroupBackstop(settings, clusterSettings, threadPool);
            this.backstop.start();
            this.nativeMemoryBreaker = new BackstopCircuitBreaker(inner);
        } else {
            this.backstop = null;
            this.nativeMemoryBreaker = inner;
        }

    }

    /** Constructor for tests that do not need the cgroup backstop. */
    public NativeMemoryCircuitBreakerService(CircuitBreakerMetrics metrics, Settings settings, ClusterSettings clusterSettings) {
        this(metrics, settings, clusterSettings, null);
    }

    private static CircuitBreaker createInnerBreaker(CircuitBreakerMetrics metrics, BreakerSettings breakerSettings) {
        HierarchyCircuitBreakerService.validateSettings(new BreakerSettings[] { breakerSettings });
        if (breakerSettings.getType() == CircuitBreaker.Type.NOOP) {
            return new NoopCircuitBreaker(breakerSettings.getName());
        }
        // null parent: native breakers are self-limiting and must not consult the heap parent
        return new ChildMemoryCircuitBreaker(
            metrics,
            breakerSettings,
            LogManager.getLogger(CHILD_LOGGER_PREFIX + breakerSettings.getName()),
            null,
            breakerSettings.getName()
        );
    }

    @Override
    public void close() throws IOException {
        if (backstop != null) {
            backstop.close();
        }
    }

    Collection<LongWithAttributes> collectMemoryLimits() {
        List<LongWithAttributes> out = new ArrayList<>(1);
        out.add(
            new LongWithAttributes(
                nativeMemoryBreaker.getLimit(),
                Map.of(ChildMemoryCircuitBreaker.BREAKER_METRIC_TYPE_ATTRIBUTE, nativeMemoryBreaker.getName())
            )
        );
        return out;
    }

    Collection<LongWithAttributes> collectMemoryEstimates() {
        List<LongWithAttributes> out = new ArrayList<>(1);
        long estimated = (long) (nativeMemoryBreaker.getUsed() * nativeMemoryBreaker.getOverhead());
        out.add(
            new LongWithAttributes(
                estimated,
                Map.of(ChildMemoryCircuitBreaker.BREAKER_METRIC_TYPE_ATTRIBUTE, nativeMemoryBreaker.getName())
            )
        );
        return out;
    }

    @Override
    public CircuitBreaker getBreaker(String name) {
        if (CircuitBreaker.NATIVE_MEMORY.equals(name)) {
            return nativeMemoryBreaker;
        }
        return null;
    }

    @Override
    public AllCircuitBreakerStats stats() {
        return new AllCircuitBreakerStats(new CircuitBreakerStats[] { stats(CircuitBreaker.NATIVE_MEMORY) });
    }

    @Override
    public CircuitBreakerStats stats(String name) {
        if (CircuitBreaker.NATIVE_MEMORY.equals(name) == false) {
            return null;
        }
        return new CircuitBreakerStats(
            nativeMemoryBreaker.getName(),
            nativeMemoryBreaker.getLimit(),
            nativeMemoryBreaker.getUsed(),
            nativeMemoryBreaker.getOverhead(),
            nativeMemoryBreaker.getTrippedCount()
        );
    }

    /**
     * Wraps the inner {@link CircuitBreaker} to add a cgroup memory backstop check before
     * any allocation. When {@link NativeMemoryCgroupBackstop#isRefusing()} is {@code true},
     * {@link #addEstimateBytesAndMaybeBreak} calls {@link #circuitBreak} on the delegate first
     * (so the trip counter and metrics are updated) then throws a {@link CircuitBreakingException}
     * with the backstop-specific reason, without touching the accounting-based used/limit.
     *
     * <p>{@link #addWithoutBreaking} is intentionally NOT gated by the backstop — callers that must
     * adjust accounting without risking a rejection (e.g. releasing previously allocated memory) can
     * always do so safely.
     */
    private final class BackstopCircuitBreaker implements CircuitBreaker {

        private final CircuitBreaker delegate;

        BackstopCircuitBreaker(CircuitBreaker delegate) {
            this.delegate = delegate;
        }

        @Override
        public void circuitBreak(String fieldName, long bytesNeeded) {
            delegate.circuitBreak(fieldName, bytesNeeded);
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
            if (backstop.isRefusing()) {
                try {
                    // circuitBreak() increments the trip counter and updates metrics before throwing;
                    // we catch its exception and replace it with the backstop-specific message below.
                    delegate.circuitBreak(label, bytes);
                } catch (CircuitBreakingException ignored) {
                    // intentional: trip count was incremented; fall through to throw backstop message
                }
                throw new CircuitBreakingException(
                    "["
                        + NATIVE_MEMORY
                        + "] cgroup memory usage is above the high-watermark threshold ("
                        + backstop.getHighWatermark()
                        + "%)",
                    bytes,
                    delegate.getLimit(),
                    Durability.TRANSIENT
                );
            }
            delegate.addEstimateBytesAndMaybeBreak(bytes, label);
        }

        @Override
        public void addWithoutBreaking(long bytes) {
            delegate.addWithoutBreaking(bytes);
        }

        @Override
        public long getUsed() {
            return delegate.getUsed();
        }

        @Override
        public long getLimit() {
            return delegate.getLimit();
        }

        @Override
        public double getOverhead() {
            return delegate.getOverhead();
        }

        @Override
        public long getTrippedCount() {
            return delegate.getTrippedCount();
        }

        @Override
        public String getName() {
            return delegate.getName();
        }

        @Override
        public Durability getDurability() {
            return delegate.getDurability();
        }

        @Override
        public void setLimitAndOverhead(long limit, double overhead) {
            delegate.setLimitAndOverhead(limit, overhead);
        }
    }
}
