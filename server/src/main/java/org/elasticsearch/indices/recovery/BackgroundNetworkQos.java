/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.apache.lucene.store.RateLimiter;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.PrioritizedThrottledTaskRunner;
import org.elasticsearch.common.util.concurrent.TaskExecutionTimeTrackingEsThreadPoolExecutor;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.snapshots.blobstore.RateLimitingInputStream;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.monitor.network.NetworkProbe;
import org.elasticsearch.monitor.os.CgroupV2Probe;
import org.elasticsearch.repositories.blobstore.ShardSnapshotTaskRunner;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

import static org.elasticsearch.core.Strings.format;

/**
 * Node-level control of background snapshot work, behind two independent switches.
 * <ul>
 *     <li>{@link #BACKGROUND_QOS_ENABLED_SETTING}: snapshots go through an ingress and an egress limiter instead of the recovery limiter's
 *     network term. Every second each limiter's rate is set to the node's network share minus the measured foreground traffic and some
 *     headroom, never below today's network-only rate. Foreground is node traffic from {@link NetworkProbe} minus the bytes that passed
 *     through the limiter. Only active when the node bandwidth settings are set. Restores are not affected.</li>
 *     <li>{@link #ADAPTIVE_UPLOAD_CONCURRENCY_ENABLED_SETTING}: the number of concurrent shard snapshot uploads, which run on their own
 *     thread pool, is adjusted every few seconds by an {@link UploadConcurrencyController}, up to
 *     {@link #UPLOAD_CONCURRENCY_MAX_SETTING}. It reads CPU contention directly (cgroup pressure stall information and throttling, and
 *     the wait in the write queue) because uploads also use CPU outside their threads, and upload errors. Off, uploads run on the
 *     snapshot pool at today's concurrency.</li>
 * </ul>
 * All repositories share this node's upload task runner so that a single controller sets the node's upload concurrency.
 */
public class BackgroundNetworkQos extends AbstractLifecycleComponent {

    private static final Logger logger = LogManager.getLogger(BackgroundNetworkQos.class);

    public static final Setting<Boolean> BACKGROUND_QOS_ENABLED_SETTING = Setting.boolSetting(
        "indices.recovery.background_qos.enabled",
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Boolean> ADAPTIVE_UPLOAD_CONCURRENCY_ENABLED_SETTING = Setting.boolSetting(
        "indices.recovery.adaptive_upload_concurrency.enabled",
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * The most shard snapshot uploads the node runs at once, if the node is big enough: the ceiling is the lower of this and what the
     * node's size allows, see {@link ThreadPool.Names#SNAPSHOT_UPLOAD}. Never below today's concurrency.
     */
    public static final Setting<Integer> UPLOAD_CONCURRENCY_MAX_SETTING = Setting.intSetting(
        "indices.recovery.upload_concurrency.max",
        20,
        1,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /** Today's minimum recovery rate, see {@link RecoverySettings}. */
    static final long MIN_FLOOR_BYTES_PER_SEC = ByteSizeValue.of(40, ByteSizeUnit.MB).getBytes();
    /** Today's fraction of the bandwidth given to recoveries, see {@link RecoverySettings}. */
    static final double FLOOR_FRACTION = 0.4;
    /** Share left unused to absorb foreground bursts within a tick. */
    static final double HEADROOM_FRACTION = 0.1;
    /** The most the rate rises per tick, as a fraction of the share. Falls are applied at once. */
    static final double MAX_STEP_UP_FRACTION = 0.1;

    /** How often the limiter rates are adjusted. */
    static final TimeValue TICK_INTERVAL = TimeValue.timeValueSeconds(1);
    /** How many ticks make an interval of the upload concurrency controller. */
    static final int UPLOAD_CONCURRENCY_INTERVAL_TICKS = 5;
    /** How many ticks between status lines in the log. */
    static final int LOG_INTERVAL_TICKS = 10;
    /** The status line is only logged while background work ran in this time. */
    static final long LOG_ACTIVE_WINDOW_NANOS = TimeUnit.SECONDS.toNanos(30);

    /**
     * Total time and number of tasks of the node's write queue, from which the mean wait in the queue over an interval follows.
     */
    record QueueLatency(long totalNanos, long tasks) {}

    /**
     * Where the node's measurements come from. Each supplier returns {@code null} if the measurement is not available.
     */
    record Probes(
        Supplier<NetworkProbe.NetworkStats> network,
        Supplier<CgroupV2Probe.CpuPressure> cpuPressure,
        Supplier<CgroupV2Probe.CpuThrottling> cpuThrottling,
        Supplier<QueueLatency> writeQueue
    ) {
        static Probes forNode(ThreadPool threadPool) {
            final NetworkProbe networkProbe = NetworkProbe.getInstance();
            final CgroupV2Probe cgroupProbe = CgroupV2Probe.getInstance();
            return new Probes(networkProbe::getNetworkStats, cgroupProbe::getCpuPressure, cgroupProbe::getCpuThrottling, () -> {
                if (threadPool.executor(ThreadPool.Names.WRITE) instanceof TaskExecutionTimeTrackingEsThreadPoolExecutor write) {
                    return new QueueLatency(write.getTotalQueueLatencyNanos(), write.getTotalStartedTasks());
                }
                return null;
            });
        }
    }

    private final ThreadPool threadPool;
    private final RecoverySettings recoverySettings;
    private final Probes probes;
    private final LongSupplier nanoTimeSupplier;

    private final Resource networkIn;
    private final Resource networkOut;
    private final List<Resource> resources;

    private final Executor uploadExecutor;
    private final PrioritizedThrottledTaskRunner<ShardSnapshotTaskRunner.SnapshotTask> uploadTaskRunner;
    private final UploadConcurrencyController uploadConcurrencyController;
    private final LongAdder uploadBytes = new LongAdder();
    private final LongAdder uploadPauseNanos = new LongAdder();
    private final LongAdder uploadReadErrors = new LongAdder();
    private final LongAdder uploadWriteErrors = new LongAdder();

    private volatile boolean backgroundQosEnabled;
    private volatile boolean adaptiveUploadConcurrencyEnabled;
    private volatile int uploadConcurrencyMax;
    private final int nodeUploadConcurrencyCeiling;

    @Nullable
    private volatile Scheduler.Cancellable scheduledTick;

    // state below is only accessed by the periodic tick
    private long lastTickNanos;
    private long tickCount;
    private boolean adaptiveUploadConcurrencyActive;
    private long intervalStartNanos;
    private long intervalStartUploadBytes;
    private long intervalStartUploadPauseNanos;
    private long intervalStartReadErrors;
    private long intervalStartWriteErrors;
    @Nullable
    private CgroupV2Probe.CpuPressure intervalStartCpuPressure;
    @Nullable
    private CgroupV2Probe.CpuThrottling intervalStartCpuThrottling;
    @Nullable
    private QueueLatency intervalStartWriteQueue;
    private UploadConcurrencyController.Signals lastSignals;
    private long readErrorsSinceLog;
    private long writeErrorsSinceLog;
    private long lastUploadBytes;
    private long lastActiveNanos;
    private boolean everActive;

    public BackgroundNetworkQos(ClusterSettings clusterSettings, ThreadPool threadPool, RecoverySettings recoverySettings) {
        this(clusterSettings, threadPool, recoverySettings, Probes.forNode(threadPool), System::nanoTime);
    }

    BackgroundNetworkQos(
        ClusterSettings clusterSettings,
        ThreadPool threadPool,
        RecoverySettings recoverySettings,
        Probes probes,
        LongSupplier nanoTimeSupplier
    ) {
        this.threadPool = threadPool;
        this.recoverySettings = recoverySettings;
        this.probes = probes;
        this.nanoTimeSupplier = nanoTimeSupplier;
        this.lastTickNanos = nanoTimeSupplier.getAsLong();
        this.intervalStartNanos = lastTickNanos;
        // same capacity in both network directions until we know whether the node's share applies per direction or combined
        final long networkCapacity = recoverySettings.nodeBandwidthSettingsExist()
            ? Math.max(recoverySettings.getAvailableNetworkBandwidth().getBytes(), 0L)
            : 0L;
        this.networkIn = new Resource("net in", networkCapacity, stats -> stats == null ? -1L : stats.receiveBytes());
        this.networkOut = new Resource("net out", networkCapacity, stats -> stats == null ? -1L : stats.transmitBytes());
        this.resources = List.of(networkIn, networkOut);

        // today's concurrency, also the target while adaptive upload concurrency is off
        final int floor = threadPool.info(ThreadPool.Names.SNAPSHOT).getMax();
        this.nodeUploadConcurrencyCeiling = Math.max(floor, threadPool.info(ThreadPool.Names.SNAPSHOT_UPLOAD).getMax());
        this.uploadConcurrencyController = new UploadConcurrencyController(floor, nodeUploadConcurrencyCeiling);
        final Executor snapshotExecutor = threadPool.executor(ThreadPool.Names.SNAPSHOT);
        final Executor snapshotUploadExecutor = threadPool.executor(ThreadPool.Names.SNAPSHOT_UPLOAD);
        // chosen for each task, so that switching off sends new uploads back to the snapshot pool without waiting for anything
        this.uploadExecutor = command -> (adaptiveUploadConcurrencyEnabled ? snapshotUploadExecutor : snapshotExecutor).execute(command);
        this.uploadTaskRunner = new PrioritizedThrottledTaskRunner<>(ShardSnapshotTaskRunner.TASK_RUNNER_NAME, floor, uploadExecutor);

        clusterSettings.initializeAndWatch(UPLOAD_CONCURRENCY_MAX_SETTING, max -> this.uploadConcurrencyMax = max);
        uploadConcurrencyController.setCeiling(Math.min(uploadConcurrencyMax, nodeUploadConcurrencyCeiling));
        clusterSettings.initializeAndWatch(BACKGROUND_QOS_ENABLED_SETTING, enabled -> this.backgroundQosEnabled = enabled);
        clusterSettings.initializeAndWatch(ADAPTIVE_UPLOAD_CONCURRENCY_ENABLED_SETTING, enabled -> {
            this.adaptiveUploadConcurrencyEnabled = enabled;
            if (enabled == false) {
                // do not leave the adaptive target in place until the next interval
                setUploadConcurrency(uploadConcurrencyController.getFloor());
            }
        });
    }

    /**
     * Whether snapshots should use {@link #getIngressLimiter()} and {@link #getEgressLimiter()}.
     */
    public boolean isBackgroundQosEnabled() {
        return backgroundQosEnabled && networkIn.capacityBytesPerSec > 0L;
    }

    public RateLimiter getIngressLimiter() {
        return networkIn.limiter;
    }

    public RateLimiter getEgressLimiter() {
        return networkOut.limiter;
    }

    /**
     * The node-level runner for shard snapshot tasks, shared by all repositories.
     */
    public PrioritizedThrottledTaskRunner<ShardSnapshotTaskRunner.SnapshotTask> getUploadTaskRunner() {
        return uploadTaskRunner;
    }

    /**
     * The executor of the upload task runner: the {@link ThreadPool.Names#SNAPSHOT_UPLOAD} pool while adaptive upload concurrency is
     * on, the {@link ThreadPool.Names#SNAPSHOT} pool otherwise.
     */
    Executor getUploadExecutor() {
        return uploadExecutor;
    }

    /**
     * Wraps a snapshot upload stream to count the uploaded bytes, an input to the upload concurrency controller.
     */
    public InputStream countUploadBytes(InputStream stream) {
        return new FilterInputStream(stream) {
            @Override
            public int read() throws IOException {
                final int b = super.read();
                if (b != -1) {
                    uploadBytes.increment();
                }
                return b;
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                final int n = super.read(b, off, len);
                if (n > 0) {
                    uploadBytes.add(n);
                }
                return n;
            }
        };
    }

    /**
     * Wraps a snapshot upload throttle listener to count the time uploads spend paused in any rate limiter, an input to the upload
     * concurrency controller.
     */
    public RateLimitingInputStream.Listener wrapUploadThrottleListener(RateLimitingInputStream.Listener listener) {
        return nanos -> {
            uploadPauseNanos.add(nanos);
            listener.onPause(nanos);
        };
    }

    /**
     * Counts a failed upload that failed reading the source, which is shared with foreground work.
     */
    public void onUploadReadError() {
        uploadReadErrors.increment();
    }

    /**
     * Counts a failed upload that failed writing to the repository.
     */
    public void onUploadWriteError() {
        uploadWriteErrors.increment();
    }

    // package-private for tests
    int getUploadConcurrencyCeiling() {
        return uploadConcurrencyController.getCeiling();
    }

    /**
     * The number of failed uploads so far that failed reading the source.
     */
    public long getUploadReadErrors() {
        return uploadReadErrors.sum();
    }

    /**
     * The number of failed uploads so far that failed writing to the repository.
     */
    public long getUploadWriteErrors() {
        return uploadWriteErrors.sum();
    }

    @Override
    protected void doStart() {
        lastTickNanos = nanoTimeSupplier.getAsLong();
        intervalStartNanos = lastTickNanos;
        logger.info(
            "background qos measurements: network [{}], cpu.pressure [{}], cpu.stat throttling [{}], write queue [{}]",
            availability(probes.network()),
            availability(probes.cpuPressure()),
            availability(probes.cpuThrottling()),
            availability(probes.writeQueue())
        );
        scheduledTick = threadPool.scheduleWithFixedDelay(this::tick, TICK_INTERVAL, threadPool.generic());
    }

    private static String availability(Supplier<?> probe) {
        return probe.get() == null ? "unavailable" : "available";
    }

    @Override
    protected void doStop() {
        final Scheduler.Cancellable cancellable = scheduledTick;
        if (cancellable != null) {
            cancellable.cancel();
        }
    }

    @Override
    protected void doClose() {}

    // package-private for tests
    void tick() {
        try {
            final long now = nanoTimeSupplier.getAsLong();
            final long elapsedNanos = now - lastTickNanos;
            lastTickNanos = now;
            tickCount++;

            final NetworkProbe.NetworkStats networkStats = probes.network().get();
            final boolean qosEnabled = isBackgroundQosEnabled();
            boolean backgroundActive = false;
            for (Resource resource : resources) {
                resource.update(networkStats, elapsedNanos, qosEnabled);
                backgroundActive |= resource.lastBackgroundBytesPerSec > 0;
            }

            final long uploadBytesNow = uploadBytes.sum();
            if (backgroundActive || uploadBytesNow != lastUploadBytes) {
                lastActiveNanos = now;
                everActive = true;
            }
            lastUploadBytes = uploadBytesNow;

            if (tickCount % UPLOAD_CONCURRENCY_INTERVAL_TICKS == 0) {
                updateUploadConcurrency(now, uploadBytesNow);
            }
            if (tickCount % LOG_INTERVAL_TICKS == 0 && everActive && now - lastActiveNanos <= LOG_ACTIVE_WINDOW_NANOS) {
                logStatus(qosEnabled);
            }
        } catch (Exception e) {
            logger.warn("background network qos update failed", e);
            assert false : e;
        }
    }

    private void updateUploadConcurrency(long now, long uploadBytesNow) {
        final long uploadPauseNanosNow = uploadPauseNanos.sum();
        final long readErrorsNow = uploadReadErrors.sum();
        final long writeErrorsNow = uploadWriteErrors.sum();
        final CgroupV2Probe.CpuPressure cpuPressureNow = probes.cpuPressure().get();
        final CgroupV2Probe.CpuThrottling cpuThrottlingNow = probes.cpuThrottling().get();
        final QueueLatency writeQueueNow = probes.writeQueue().get();

        final long intervalNanos = now - intervalStartNanos;
        final long bytes = uploadBytesNow - intervalStartUploadBytes;
        final long pauseNanos = uploadPauseNanosNow - intervalStartUploadPauseNanos;
        final UploadConcurrencyController.Signals signals = new UploadConcurrencyController.Signals(
            uploadTaskRunner.queueSize(),
            uploadTaskRunner.runningTasks(),
            intervalNanos > 0L ? bytes * (double) TimeUnit.SECONDS.toNanos(1) / intervalNanos : 0.0,
            pauseNanos,
            intervalNanos,
            cpuPressure(intervalStartCpuPressure, cpuPressureNow, intervalNanos),
            throttledMicros(intervalStartCpuThrottling, cpuThrottlingNow),
            writeQueueWaitMillis(intervalStartWriteQueue, writeQueueNow),
            readErrorsNow - intervalStartReadErrors,
            writeErrorsNow - intervalStartWriteErrors
        );
        readErrorsSinceLog += signals.readErrors();
        writeErrorsSinceLog += signals.uploadErrors();
        lastSignals = signals;

        intervalStartNanos = now;
        intervalStartUploadBytes = uploadBytesNow;
        intervalStartUploadPauseNanos = uploadPauseNanosNow;
        intervalStartReadErrors = readErrorsNow;
        intervalStartWriteErrors = writeErrorsNow;
        intervalStartCpuPressure = cpuPressureNow;
        intervalStartCpuThrottling = cpuThrottlingNow;
        intervalStartWriteQueue = writeQueueNow;

        if (adaptiveUploadConcurrencyEnabled) {
            adaptiveUploadConcurrencyActive = true;
            if (intervalNanos <= 0L) {
                return;
            }
            // the setting may have changed
            uploadConcurrencyController.setCeiling(Math.min(uploadConcurrencyMax, nodeUploadConcurrencyCeiling));
            setUploadConcurrency(uploadConcurrencyController.onInterval(signals).target());
        } else {
            if (adaptiveUploadConcurrencyActive) {
                adaptiveUploadConcurrencyActive = false;
                uploadConcurrencyController.reset();
            }
            setUploadConcurrency(uploadConcurrencyController.getFloor());
        }
    }

    static OptionalDouble cpuPressure(
        @Nullable CgroupV2Probe.CpuPressure before,
        @Nullable CgroupV2Probe.CpuPressure after,
        long intervalNanos
    ) {
        if (before == null || after == null || after.someTotalMicros() < before.someTotalMicros() || intervalNanos <= 0L) {
            return OptionalDouble.empty();
        }
        return OptionalDouble.of(
            (after.someTotalMicros() - before.someTotalMicros()) * (double) TimeUnit.MICROSECONDS.toNanos(1) / intervalNanos
        );
    }

    static OptionalLong throttledMicros(@Nullable CgroupV2Probe.CpuThrottling before, @Nullable CgroupV2Probe.CpuThrottling after) {
        if (before == null || after == null || after.throttledMicros() < before.throttledMicros()) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(after.throttledMicros() - before.throttledMicros());
    }

    static OptionalDouble writeQueueWaitMillis(@Nullable QueueLatency before, @Nullable QueueLatency after) {
        if (before == null || after == null || after.tasks() < before.tasks() || after.totalNanos() < before.totalNanos()) {
            return OptionalDouble.empty();
        }
        final long tasks = after.tasks() - before.tasks();
        if (tasks == 0L) {
            return OptionalDouble.of(0.0);
        }
        return OptionalDouble.of((after.totalNanos() - before.totalNanos()) / (double) tasks / TimeUnit.MILLISECONDS.toNanos(1));
    }

    private void setUploadConcurrency(int target) {
        final int current = uploadTaskRunner.getMaxRunningTasks();
        if (current != target) {
            logger.debug("changing snapshot upload concurrency from [{}] to [{}]", current, target);
            uploadTaskRunner.setMaxRunningTasks(target);
        }
    }

    private void logStatus(boolean qosEnabled) {
        final UploadConcurrencyController.Decision decision = uploadConcurrencyController.getLastDecision();
        final UploadConcurrencyController.Signals signals = lastSignals;
        logger.info(
            "background network qos [{}]; {}; {}; cpu pressure [{}] throttled [{}us] write queue wait [{}ms]; "
                + "upload errors since last log read [{}] upload [{}]; uploads adaptive [{}] target [{}] running [{}] queued [{}] "
                + "ceiling [{}] last decision [{}: {}]",
            qosEnabled ? "on" : "off",
            networkIn.describeAndResetPause(),
            networkOut.describeAndResetPause(),
            signals == null || signals.cpuPressure().isEmpty() ? "unknown" : format("%.3f", signals.cpuPressure().getAsDouble()),
            signals == null || signals.throttledMicros().isEmpty() ? "unknown" : signals.throttledMicros().getAsLong(),
            signals == null || signals.writeQueueWaitMillis().isEmpty()
                ? "unknown"
                : format("%.1f", signals.writeQueueWaitMillis().getAsDouble()),
            readErrorsSinceLog,
            writeErrorsSinceLog,
            adaptiveUploadConcurrencyActive ? "on" : "off",
            uploadTaskRunner.getMaxRunningTasks(),
            uploadTaskRunner.runningTasks(),
            uploadTaskRunner.queueSize(),
            uploadConcurrencyController.getCeiling(),
            decision.action(),
            decision.reason()
        );
        readErrorsSinceLog = 0L;
        writeErrorsSinceLog = 0L;
    }

    /**
     * The rate floor: today's recovery rate, capped at the capacity.
     */
    static long computeFloor(long capacityBytesPerSec) {
        if (capacityBytesPerSec <= 0L) {
            return MIN_FLOOR_BYTES_PER_SEC;
        }
        return Math.min(capacityBytesPerSec, Math.max(MIN_FLOOR_BYTES_PER_SEC, Math.round(capacityBytesPerSec * FLOOR_FRACTION)));
    }

    /**
     * Computes the next background rate for one resource: the capacity left over by foreground use, minus some headroom, between the
     * floor and the capacity. Falls apply at once, rises are limited per tick.
     *
     * @param currentRate               the current rate in bytes per second
     * @param capacityBytesPerSec       the node's capacity for this resource
     * @param usageBytesPerSec          all use of this resource over the last tick, or negative if unknown
     * @param backgroundBytesPerSec     background use of this resource over the last tick
     */
    static long computeRate(long currentRate, long capacityBytesPerSec, long usageBytesPerSec, long backgroundBytesPerSec) {
        final long floor = computeFloor(capacityBytesPerSec);
        if (usageBytesPerSec < 0L) {
            return floor;
        }
        final long foreground = Math.max(0L, usageBytesPerSec - backgroundBytesPerSec);
        final long headroom = Math.round(capacityBytesPerSec * HEADROOM_FRACTION);
        final long target = Math.clamp(capacityBytesPerSec - foreground - headroom, floor, capacityBytesPerSec);
        if (target <= currentRate) {
            return target;
        }
        return Math.min(target, currentRate + Math.round(capacityBytesPerSec * MAX_STEP_UP_FRACTION));
    }

    private static long perSecond(long delta, long elapsedNanos) {
        return Math.round(delta * (double) TimeUnit.SECONDS.toNanos(1) / elapsedNanos);
    }

    private static double toMBPerSec(long bytesPerSec) {
        return bytesPerSec / (double) ByteSizeUnit.MB.toBytes(1);
    }

    /**
     * Limiter and measurements for one resource. Only the limiter is accessed outside the periodic tick.
     */
    private static class Resource {
        private final String name;
        private final long capacityBytesPerSec;
        private final ToLongFunction<NetworkProbe.NetworkStats> usageBytes;
        private final CountingRateLimiter limiter;
        private final long floorBytesPerSec;

        private long rateBytesPerSec;
        private long lastUsageBytes = -1L;
        private long lastBackgroundBytes;
        private long lastPauseNanos;
        private long pauseNanosSinceLog;
        private long lastUsageBytesPerSec = -1L;
        private long lastBackgroundBytesPerSec;

        /**
         * @param usageBytes  the cumulative bytes the whole pod used of this resource from the stats (which may be null), or negative if
         *                    unknown
         */
        Resource(String name, long capacityBytesPerSec, ToLongFunction<NetworkProbe.NetworkStats> usageBytes) {
            this.name = name;
            this.capacityBytesPerSec = capacityBytesPerSec;
            this.usageBytes = usageBytes;
            this.floorBytesPerSec = computeFloor(capacityBytesPerSec);
            this.rateBytesPerSec = floorBytesPerSec;
            this.limiter = new CountingRateLimiter(toMBPerSec(floorBytesPerSec));
        }

        void update(@Nullable NetworkProbe.NetworkStats stats, long elapsedNanos, boolean qosEnabled) {
            final long usage = usageBytes.applyAsLong(stats);
            final long backgroundBytes = limiter.getBytes();
            final long pauseNanos = limiter.getPauseNanos();
            if (elapsedNanos > 0L) {
                lastBackgroundBytesPerSec = perSecond(backgroundBytes - lastBackgroundBytes, elapsedNanos);
                // counters may reset, e.g. if an interface goes away; treat that as unknown
                lastUsageBytesPerSec = usage >= 0L && lastUsageBytes >= 0L && usage >= lastUsageBytes
                    ? perSecond(usage - lastUsageBytes, elapsedNanos)
                    : -1L;
            }
            pauseNanosSinceLog += pauseNanos - lastPauseNanos;
            lastUsageBytes = usage;
            lastBackgroundBytes = backgroundBytes;
            lastPauseNanos = pauseNanos;

            // when off, go back to the floor so that switching on starts from today's rate
            final long newRate = qosEnabled
                ? computeRate(rateBytesPerSec, capacityBytesPerSec, lastUsageBytesPerSec, lastBackgroundBytesPerSec)
                : floorBytesPerSec;
            if (newRate != rateBytesPerSec) {
                rateBytesPerSec = newRate;
                limiter.setMBPerSec(toMBPerSec(newRate));
            }
        }

        String describeAndResetPause() {
            final String description = format(
                "%s capacity [%s/s] usage [%s/s] bg [%s/s] fg [%s/s] budget [%s/s] pause [%dms]",
                name,
                ByteSizeValue.ofBytes(capacityBytesPerSec),
                lastUsageBytesPerSec < 0L ? "unknown" : ByteSizeValue.ofBytes(lastUsageBytesPerSec),
                ByteSizeValue.ofBytes(lastBackgroundBytesPerSec),
                lastUsageBytesPerSec < 0L
                    ? "unknown"
                    : ByteSizeValue.ofBytes(Math.max(0L, lastUsageBytesPerSec - lastBackgroundBytesPerSec)),
                ByteSizeValue.ofBytes(rateBytesPerSec),
                TimeUnit.NANOSECONDS.toMillis(pauseNanosSinceLog)
            );
            pauseNanosSinceLog = 0L;
            return description;
        }
    }
}
