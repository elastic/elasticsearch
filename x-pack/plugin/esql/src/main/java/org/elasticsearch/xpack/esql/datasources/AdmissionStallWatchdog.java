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
import java.util.function.LongSupplier;

/**
 * Warns when an external-source admission gate has waiters and no holders for
 * {@link #DEFAULT_STALL}. Holders that keep a unit for a whole stream (permits, segmentators)
 * are healthy saturation: {@code lastGrant} is telemetry, not a stall signal. Per-query
 * starvation on a live gate is out of scope.
 * <p>
 * Inspect runs on {@code GENERIC} via {@link ThreadPool#scheduleWithFixedDelay} as a
 * force-execution task: the timer thread only enqueues the check, so a stuck scheduler
 * thread does not run the graph walk. Queue depth and oldest-wait gauges share the
 * {@code es.esql.datasources.admission.*} namespace so a later query-slot admission surface
 * can join the same series. Oldest-wait stays on the APM gauge because phone-home counters
 * sum across nodes.
 */
final class AdmissionStallWatchdog implements AdmissionTracker, Closeable {

    private static final Logger logger = LogManager.getLogger(AdmissionStallWatchdog.class);

    static final TimeValue DEFAULT_INTERVAL = TimeValue.timeValueSeconds(5);
    static final TimeValue DEFAULT_STALL = TimeValue.timeValueSeconds(15);
    static final TimeValue DEFAULT_QUIET = TimeValue.timeValueSeconds(30);

    static final String WAITERS_CURRENT = "es.esql.datasources.admission.waiters.current";
    static final String HOLDERS_CURRENT = "es.esql.datasources.admission.holders.current";
    static final String OLDEST_WAIT_MILLIS = "es.esql.datasources.admission.oldest_wait_millis.current";
    static final String GATE_ATTRIBUTE = "es_datasource_admission_gate";

    private static final int LABEL_CAP = 8;

    private final ConcurrentHashMap<String, GateWaitState> waits = new ConcurrentHashMap<>();
    private final CopyOnWriteArrayList<AdmissionGate> gates = new CopyOnWriteArrayList<>();
    private final LongSupplier nanoTime;
    private final long stallNanos;
    private final long quietNanos;
    @Nullable
    private final Scheduler.Cancellable cancellable;
    private final LongAsyncGauge waitersGauge;
    private final LongAsyncGauge holdersGauge;
    private final LongAsyncGauge oldestWaitGauge;
    private final AtomicBoolean closed = new AtomicBoolean();

    AdmissionStallWatchdog(ThreadPool threadPool, MeterRegistry meters) {
        this(threadPool, meters, DEFAULT_INTERVAL, DEFAULT_STALL, DEFAULT_QUIET, System::nanoTime, threadPool.generic());
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
        this.nanoTime = nanoTime;
        this.stallNanos = stall.nanos();
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

    void inspect() {
        if (closed.get()) {
            return;
        }
        long now = nanoTime.getAsLong();
        StringBuilder graph = null;
        long oldestStalled = 0L;
        for (Map.Entry<String, GateWaitState> entry : waits.entrySet()) {
            GateWaitState state = entry.getValue();
            int waiterCount = state.outstanding.size();
            if (waiterCount == 0) {
                continue;
            }
            AdmissionGate probe = probe(entry.getKey());
            int holders = probe == null ? 0 : Math.max(0, probe.holders());
            if (holders > 0) {
                continue;
            }
            long oldestWait = now - state.oldestStartNanos();
            if (oldestWait < stallNanos) {
                continue;
            }
            long lastGrant = state.lastGrantNanos.get();
            long sinceGrant = lastGrant == 0L ? oldestWait : now - lastGrant;
            long lastWarn = state.lastWarnNanos.get();
            if (lastWarn != 0L && now - lastWarn < quietNanos) {
                continue;
            }
            if (state.lastWarnNanos.compareAndSet(lastWarn, now) == false) {
                continue;
            }
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
        if (graph != null) {
            logger.warn(
                "external-source admission stall: waiters with no holders for [{}ms]: {}",
                TimeUnit.NANOSECONDS.toMillis(oldestStalled),
                graph
            );
        }
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

    record GateStats(String name, int waiters, int holders, long oldestWaitMillis, long millisSinceGrant) {}

    private static final class GateWaitState {
        private final Set<WaitImpl> outstanding = ConcurrentHashMap.newKeySet();
        private final AtomicLong lastGrantNanos = new AtomicLong();
        private final AtomicLong lastWarnNanos = new AtomicLong();

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
