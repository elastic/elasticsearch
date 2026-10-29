/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.telemetry.metric.LongAsyncGauge;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.datasources.spi.AdmissionGate;
import org.elasticsearch.xpack.esql.datasources.spi.AdmissionTracker;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;

/**
 * Warns when an external-source admission gate has been stalled for {@link #DEFAULT_STALL}.
 * Stall is per-gate: {@link AdmissionGate.StallPolicy#HOLDERS} (permits, budget, segmentators)
 * skips while holders exist; {@link AdmissionGate.StallPolicy#GRANT_AGE} (byte budget) keys
 * on waiters with no grant, ignoring holders. WARN is rate-limited by
 * {@link #DEFAULT_QUIET}.
 * Byte-gate rescue uses {@link #DEFAULT_RESCUE}, not the stall or quiet windows: one FIFO
 * grant per rescue window, then the normal grant loop. Rescue WARN is a possible stall, not
 * a user error.
 * <p>
 * Inspect runs on {@code GENERIC} via {@link ThreadPool#scheduleWithFixedDelay} as a
 * force-execution task: the timer thread only enqueues the check, so a stuck scheduler
 * thread does not run the graph walk. Direct ({@code Runnable::run}) waiters run on inspect
 * like any other releaser; waiters that passed an I/O executor keep it. Every tick logs
 * a DEBUG dump of the byte budget. Queue depth, oldest-wait, and rescue counters share the
 * {@code es.esql.datasources.admission.*} namespace so a later query-slot admission surface
 * can join the same series. Oldest-wait stays on the APM gauge because phone-home counters
 * sum across nodes.
 */
final class AdmissionStallWatchdog implements AdmissionTracker, Closeable {

    private static final Logger logger = LogManager.getLogger(AdmissionStallWatchdog.class);

    static final TimeValue DEFAULT_INTERVAL = TimeValue.timeValueSeconds(5);
    static final TimeValue DEFAULT_STALL = TimeValue.timeValueSeconds(15);
    static final TimeValue DEFAULT_RESCUE = TimeValue.timeValueSeconds(5);
    static final TimeValue DEFAULT_QUIET = TimeValue.timeValueSeconds(30);

    static final String WAITERS_CURRENT = "es.esql.datasources.admission.waiters.current";
    static final String HOLDERS_CURRENT = "es.esql.datasources.admission.holders.current";
    static final String OLDEST_WAIT_MILLIS = "es.esql.datasources.admission.oldest_wait_millis.current";
    static final String RESCUES_TOTAL = "es.esql.datasources.admission.rescues.total";
    static final String REGRANTS_TOTAL = "es.esql.datasources.admission.regrants.total";
    static final String GATE_ATTRIBUTE = "es_datasource_admission_gate";

    private static final int LABEL_CAP = 8;

    private final ConcurrentHashMap<String, GateWaitState> waits = new ConcurrentHashMap<>();
    private final CopyOnWriteArrayList<AdmissionGate> gates = new CopyOnWriteArrayList<>();
    private final LongSupplier nanoTime;
    private final long stallNanos;
    private final long rescueNanos;
    private final long quietNanos;
    @Nullable
    private final Scheduler.Cancellable cancellable;
    private final LongAsyncGauge waitersGauge;
    private final LongAsyncGauge holdersGauge;
    private final LongAsyncGauge oldestWaitGauge;
    private final LongCounter rescuesCounter;
    private final LongCounter regrantsCounter;
    private final LongAdder rescues = new LongAdder();
    private final LongAdder regrants = new LongAdder();
    private final AtomicBoolean rescueEnabled = new AtomicBoolean(true);
    private final AtomicBoolean closed = new AtomicBoolean();

    AdmissionStallWatchdog(ThreadPool threadPool, MeterRegistry meters) {
        this(threadPool, meters, DEFAULT_INTERVAL, DEFAULT_STALL, DEFAULT_RESCUE, DEFAULT_QUIET, System::nanoTime, threadPool.generic());
    }

    /**
     * @param threadPool {@code null} disables scheduling (unit tests call {@link #inspect} directly)
     * @param inspectExecutor where each check runs; production passes {@code threadPool.generic()}
     */
    AdmissionStallWatchdog(
        @Nullable ThreadPool threadPool,
        MeterRegistry meters,
        TimeValue interval,
        TimeValue stall,
        TimeValue quiet,
        LongSupplier nanoTime,
        Executor inspectExecutor
    ) {
        this(threadPool, meters, interval, stall, DEFAULT_RESCUE, quiet, nanoTime, inspectExecutor);
    }

    AdmissionStallWatchdog(
        @Nullable ThreadPool threadPool,
        MeterRegistry meters,
        TimeValue interval,
        TimeValue stall,
        TimeValue rescue,
        TimeValue quiet,
        LongSupplier nanoTime,
        Executor inspectExecutor
    ) {
        this.nanoTime = nanoTime;
        this.stallNanos = stall.nanos();
        this.rescueNanos = rescue.nanos();
        this.quietNanos = quiet.nanos();
        MeterRegistry registry = meters != null ? meters : MeterRegistry.NOOP;
        this.waitersGauge = registry.registerLongsAsyncGauge(
            WAITERS_CURRENT,
            "External-source admission waiters currently parked, dimensioned by gate",
            "1",
            this::waiterObservations
        );
        this.holdersGauge = registry.registerLongsAsyncGauge(
            HOLDERS_CURRENT,
            "External-source admission units currently held, dimensioned by gate",
            "1",
            this::holderObservations
        );
        this.oldestWaitGauge = registry.registerLongsAsyncGauge(
            OLDEST_WAIT_MILLIS,
            "Oldest external-source admission wait currently parked, dimensioned by gate",
            "ms",
            this::oldestWaitObservations
        );
        this.rescuesCounter = registry.registerLongCounter(
            RESCUES_TOTAL,
            "Over-cap byte-budget rescue grants issued because a FIFO head stalled",
            "1"
        );
        this.regrantsCounter = registry.registerLongCounter(
            REGRANTS_TOTAL,
            "Within-cap byte-budget grants issued because a FIFO head was stuck after a lost wakeup",
            "1"
        );
        if (threadPool != null && interval.nanos() > 0L) {
            this.cancellable = threadPool.scheduleWithFixedDelay(new AbstractRunnable() {
                @Override
                protected void doRun() {
                    inspect();
                }

                @Override
                public void onFailure(Exception e) {
                    logger.debug("admission stall inspect failed", e);
                }

                @Override
                public boolean isForceExecution() {
                    return true;
                }
            }, interval, inspectExecutor);
        } else {
            this.cancellable = null;
        }
    }

    @Override
    public Wait waitStarted(String gate, String waiter) {
        GateWaitState state = waits.computeIfAbsent(gate, g -> new GateWaitState());
        WaitImpl wait = new WaitImpl(waiter == null ? "" : waiter, nanoTime.getAsLong(), state);
        state.outstanding.add(wait);
        return wait;
    }

    @Override
    public void register(AdmissionGate gate) {
        if (gate != null) {
            gates.add(gate);
        }
    }

    void setRescueEnabled(boolean enabled) {
        rescueEnabled.set(enabled);
    }

    long rescueCount() {
        return rescues.sum();
    }

    long regrantCount() {
        return regrants.sum();
    }

    void inspect() {
        if (closed.get()) {
            return;
        }
        long now = nanoTime.getAsLong();
        dumpByteBudget();
        AdmissionGate bytes = probe(AdmissionTracker.GATE_BYTES);
        if (bytes != null) {
            try {
                bytes.failCancelledWaiters();
            } catch (Exception e) {
                logger.warn("failCancelledWaiters failed", e);
            }
        }
        StringBuilder graph = null;
        long oldestStalled = 0L;
        for (Map.Entry<String, GateWaitState> entry : waits.entrySet()) {
            GateWaitState state = entry.getValue();
            int waiterCount = state.outstanding.size();
            if (waiterCount == 0) {
                continue;
            }
            AdmissionGate probe = probe(entry.getKey());
            long oldestWait = now - state.oldestStartNanos();
            long lastGrant = state.lastGrantNanos.get();
            long sinceGrant = lastGrant == 0L ? oldestWait : now - lastGrant;
            if (isStalled(probe, waiterCount, oldestWait, sinceGrant)) {
                long lastWarn = state.lastWarnNanos.get();
                if (lastWarn == 0L || now - lastWarn >= quietNanos) {
                    if (state.lastWarnNanos.compareAndSet(lastWarn, now)) {
                        if (graph == null) {
                            graph = new StringBuilder();
                        } else {
                            graph.append("; ");
                        }
                        graph.append(describe(entry.getKey(), state, waiterCount, oldestWait, sinceGrant, lastGrant == 0L));
                        if (oldestWait > oldestStalled) {
                            oldestStalled = oldestWait;
                        }
                    }
                }
            }
            try {
                maybeRescue(probe, state, now, oldestWait, sinceGrant, lastGrant == 0L);
            } catch (Exception e) {
                logger.warn("external-source admission rescue failed: {}", probe == null ? entry.getKey() : probe.name(), e);
            }
        }
        if (graph != null) {
            logger.warn("possible admission stall: [{}ms]: {}", TimeUnit.NANOSECONDS.toMillis(oldestStalled), graph);
        }
    }

    private boolean isStalled(AdmissionGate probe, int waiterCount, long oldestWaitNanos, long sinceGrantNanos) {
        if (waiterCount == 0) {
            return false;
        }
        AdmissionGate.StallPolicy policy = probe == null ? AdmissionGate.StallPolicy.HOLDERS : probe.stallPolicy();
        return switch (policy) {
            case HOLDERS -> {
                int holders = probe == null ? 0 : Math.max(0, probe.holders());
                yield holders == 0 && oldestWaitNanos >= stallNanos;
            }
            // Head must have waited the stall window, and no grant may have landed in that window.
            // lastGrant alone would over-cap a waiter that arrived after 15s of idle occupancy.
            case GRANT_AGE -> oldestWaitNanos >= stallNanos && sinceGrantNanos >= stallNanos;
        };
    }

    private void maybeRescue(
        AdmissionGate probe,
        GateWaitState state,
        long now,
        long oldestWaitNanos,
        long sinceGrantNanos,
        boolean neverGranted
    ) {
        if (rescueEnabled.get() == false || probe == null) {
            return;
        }
        if (probe.stallPolicy() != AdmissionGate.StallPolicy.GRANT_AGE) {
            return;
        }
        if (oldestWaitNanos < rescueNanos || sinceGrantNanos < rescueNanos) {
            return;
        }
        long lastRescue = state.lastRescueNanos.get();
        if (lastRescue != 0L && now - lastRescue < rescueNanos) {
            return;
        }
        String graph = describe(probe.name(), state, state.outstanding.size(), oldestWaitNanos, sinceGrantNanos, neverGranted);
        AdmissionGate.RescueResult result = probe.rescueHead(null);
        if (result == AdmissionGate.RescueResult.NONE) {
            return;
        }
        state.lastRescueNanos.set(now);
        switch (result) {
            case OVER_CAP -> {
                rescues.increment();
                rescuesCounter.increment();
                logger.warn("external-source admission rescue: granted FIFO head over the byte cap: {} {}", probe.holderSummary(), graph);
            }
            case REGRANT -> {
                regrants.increment();
                regrantsCounter.increment();
                logger.warn("external-source admission rescue: lost-wakeup regrant of FIFO head: {} {}", probe.holderSummary(), graph);
            }
            case NONE -> throw new AssertionError("NONE already returned");
        }
    }

    private void dumpByteBudget() {
        if (logger.isDebugEnabled() == false) {
            return;
        }
        AdmissionGate bytes = probe(AdmissionTracker.GATE_BYTES);
        if (bytes == null) {
            return;
        }
        GateWaitState state = waits.get(AdmissionTracker.GATE_BYTES);
        int waiterCount = state == null ? 0 : state.outstanding.size();
        logger.debug("external-source byte budget: {} waiters=[{}]", bytes.holderSummary(), waiterCount);
    }

    List<GateStats> stats() {
        long now = nanoTime.getAsLong();
        Map<String, AdmissionGate> probes = new HashMap<>();
        for (AdmissionGate gate : gates) {
            probes.putIfAbsent(gate.name(), gate);
        }
        List<GateStats> out = new ArrayList<>();
        for (Map.Entry<String, GateWaitState> entry : waits.entrySet()) {
            GateWaitState state = entry.getValue();
            AdmissionGate probe = probes.remove(entry.getKey());
            out.add(statsOf(entry.getKey(), state, probe, now));
        }
        for (AdmissionGate probe : probes.values()) {
            out.add(new GateStats(probe.name(), 0, Math.max(0, probe.holders()), 0L, -1L));
        }
        return out;
    }

    private static GateStats statsOf(String name, GateWaitState state, @Nullable AdmissionGate probe, long now) {
        int waiterCount = state.outstanding.size();
        long oldestWaitMillis = 0L;
        if (waiterCount > 0) {
            oldestWaitMillis = TimeUnit.NANOSECONDS.toMillis(now - state.oldestStartNanos());
        }
        long lastGrant = state.lastGrantNanos.get();
        long sinceGrant = lastGrant == 0L ? -1L : TimeUnit.NANOSECONDS.toMillis(now - lastGrant);
        int holders = probe == null ? 0 : Math.max(0, probe.holders());
        return new GateStats(name, waiterCount, holders, oldestWaitMillis, sinceGrant);
    }

    private String describe(
        String gate,
        GateWaitState state,
        int waiterCount,
        long oldestWaitNanos,
        long sinceGrantNanos,
        boolean neverGranted
    ) {
        StringBuilder sb = new StringBuilder(gate);
        sb.append("{waiters=").append(waiterCount);
        sb.append(" oldest=").append(TimeUnit.NANOSECONDS.toMillis(oldestWaitNanos)).append("ms");
        if (neverGranted) {
            sb.append(" lastGrant=never");
        } else {
            sb.append(" lastGrant=").append(TimeUnit.NANOSECONDS.toMillis(sinceGrantNanos)).append("ms ago");
        }
        AdmissionGate probe = probe(gate);
        if (probe != null) {
            sb.append(" holders=").append(probe.holders());
            String summary = probe.holderSummary();
            if (summary.isEmpty() == false) {
                sb.append(" [").append(summary).append(']');
            }
        }
        sb.append(" waiters=[");
        int n = 0;
        for (WaitImpl wait : state.outstanding) {
            if (n == LABEL_CAP) {
                sb.append("...");
                break;
            }
            if (n > 0) {
                sb.append(',');
            }
            sb.append(wait.waiter);
            n++;
        }
        sb.append("]}");
        return sb.toString();
    }

    @Nullable
    private AdmissionGate probe(String name) {
        for (AdmissionGate gate : gates) {
            if (name.equals(gate.name())) {
                return gate;
            }
        }
        return null;
    }

    private Collection<LongWithAttributes> waiterObservations() {
        return observations(true, false);
    }

    private Collection<LongWithAttributes> holderObservations() {
        return observations(false, false);
    }

    private Collection<LongWithAttributes> oldestWaitObservations() {
        return observations(false, true);
    }

    private Collection<LongWithAttributes> observations(boolean waiters, boolean oldestWait) {
        try {
            List<LongWithAttributes> out = new ArrayList<>();
            for (GateStats stat : stats()) {
                long value;
                if (waiters) {
                    value = stat.waiters();
                } else if (oldestWait) {
                    value = stat.oldestWaitMillis();
                } else {
                    value = stat.holders();
                }
                out.add(new LongWithAttributes(value, Map.of(GATE_ATTRIBUTE, stat.name())));
            }
            return out;
        } catch (Exception e) {
            logger.trace("telemetry: admission stall gauge failed", e);
            return List.of();
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true) == false) {
            return;
        }
        if (cancellable != null) {
            cancellable.cancel();
        }
        waitersGauge.close();
        holdersGauge.close();
        oldestWaitGauge.close();
    }

    /** {@code millisSinceGrant} is time since the last grant, not since a byte release. */
    record GateStats(String name, int waiters, int holders, long oldestWaitMillis, long millisSinceGrant) {}

    private static final class GateWaitState {
        private final Set<WaitImpl> outstanding = ConcurrentHashMap.newKeySet();
        private final AtomicLong lastGrantNanos = new AtomicLong();
        private final AtomicLong lastWarnNanos = new AtomicLong();
        private final AtomicLong lastRescueNanos = new AtomicLong();

        long oldestStartNanos() {
            long oldest = Long.MAX_VALUE;
            for (WaitImpl wait : outstanding) {
                if (wait.startedNanos < oldest) {
                    oldest = wait.startedNanos;
                }
            }
            return oldest;
        }
    }

    private final class WaitImpl implements Wait {
        private final String waiter;
        private final long startedNanos;
        private final GateWaitState state;
        private final AtomicBoolean done = new AtomicBoolean();

        private WaitImpl(String waiter, long startedNanos, GateWaitState state) {
            this.waiter = waiter;
            this.startedNanos = startedNanos;
            this.state = state;
        }

        @Override
        public void granted() {
            if (done.compareAndSet(false, true) == false) {
                return;
            }
            state.outstanding.remove(this);
            state.lastGrantNanos.set(nanoTime.getAsLong());
        }

        @Override
        public void finished() {
            if (done.compareAndSet(false, true) == false) {
                return;
            }
            state.outstanding.remove(this);
        }
    }
}
