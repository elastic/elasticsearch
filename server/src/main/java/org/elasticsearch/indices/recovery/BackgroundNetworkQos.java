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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.snapshots.blobstore.RateLimitingInputStream;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.monitor.network.NetworkProbe;
import org.elasticsearch.monitor.os.OsProbe;
import org.elasticsearch.monitor.process.ProcessProbe;
import org.elasticsearch.repositories.blobstore.ShardSnapshotTaskRunner;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static org.elasticsearch.core.Strings.format;

/**
 * Node-level control of background (snapshot and restore) network use, behind two independent switches.
 * <ul>
 *     <li>{@link #BACKGROUND_QOS_ENABLED_SETTING}: snapshots and restores go through an ingress and an egress limiter instead of the
 *     recovery limiter's network term. Every second each limiter's rate is set to the node's network share minus the measured foreground
 *     traffic and some headroom, never below today's network-only rate. Foreground is node traffic from {@link NetworkProbe} minus the
 *     bytes that passed through the limiter. Only active when the node bandwidth settings are set.</li>
 *     <li>{@link #ADAPTIVE_UPLOAD_CONCURRENCY_ENABLED_SETTING}: the number of concurrent shard snapshot uploads is adjusted every few
 *     seconds by an {@link UploadConcurrencyController}. Off, it stays at today's value.</li>
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

    /** Today's minimum recovery rate, see {@link RecoverySettings}. */
    static final long MIN_FLOOR_BYTES_PER_SEC = ByteSizeValue.of(40, ByteSizeUnit.MB).getBytes();
    /** Today's fraction of the bandwidth given to recoveries, see {@link RecoverySettings}. */
    static final double FLOOR_FRACTION = 0.4;
    /** Share left unused to absorb foreground bursts within a tick. */
    static final double HEADROOM_FRACTION = 0.1;
    /** The most the rate rises per tick, as a fraction of the share. Falls are applied at once. */
    static final double MAX_STEP_UP_FRACTION = 0.1;

    static final TimeValue TICK_INTERVAL = TimeValue.timeValueSeconds(1);
    static final int UPLOAD_CONCURRENCY_INTERVAL_TICKS = 5;
    static final int LOG_INTERVAL_TICKS = 10;
    static final long LOG_ACTIVE_WINDOW_NANOS = TimeUnit.SECONDS.toNanos(30);

    private final ThreadPool threadPool;
    private final RecoverySettings recoverySettings;
    private final Supplier<NetworkProbe.NetworkStats> networkStatsSupplier;
    private final LongSupplier nanoTimeSupplier;
    private final IntSupplier cpuPercentSupplier;

    private final long shareBytesPerSec;
    private final Direction ingress;
    private final Direction egress;

    private final PrioritizedThrottledTaskRunner<ShardSnapshotTaskRunner.SnapshotTask> uploadTaskRunner;
    private final UploadConcurrencyController uploadConcurrencyController;
    private final LongAdder uploadBytes = new LongAdder();
    private final LongAdder uploadPauseNanos = new LongAdder();

    private volatile boolean backgroundQosEnabled;
    private volatile boolean adaptiveUploadConcurrencyEnabled;

    @Nullable
    private volatile Scheduler.Cancellable scheduledTick;

    // state below is only accessed by the periodic tick
    private long lastTickNanos;
    private long tickCount;
    private boolean adaptiveUploadConcurrencyActive;
    private long intervalStartNanos;
    private long intervalStartUploadBytes;
    private long intervalStartUploadPauseNanos;
    private long lastUploadBytes;
    private long lastActiveNanos;
    private boolean everActive;

    public BackgroundNetworkQos(ClusterSettings clusterSettings, ThreadPool threadPool, RecoverySettings recoverySettings) {
        this(
            clusterSettings,
            threadPool,
            recoverySettings,
            NetworkProbe.getInstance()::getNetworkStats,
            System::nanoTime,
            ProcessProbe::getProcessCpuPercent,
            OsProbe.getInstance().getTotalPhysicalMemorySize()
        );
    }

    BackgroundNetworkQos(
        ClusterSettings clusterSettings,
        ThreadPool threadPool,
        RecoverySettings recoverySettings,
        Supplier<NetworkProbe.NetworkStats> networkStatsSupplier,
        LongSupplier nanoTimeSupplier,
        IntSupplier cpuPercentSupplier,
        long totalMemoryBytes
    ) {
        this.threadPool = threadPool;
        this.recoverySettings = recoverySettings;
        this.networkStatsSupplier = networkStatsSupplier;
        this.nanoTimeSupplier = nanoTimeSupplier;
        this.cpuPercentSupplier = cpuPercentSupplier;
        this.lastTickNanos = nanoTimeSupplier.getAsLong();
        this.intervalStartNanos = lastTickNanos;
        // same value per direction until we know whether the node's network share applies per direction or combined
        this.shareBytesPerSec = recoverySettings.nodeBandwidthSettingsExist()
            ? Math.max(recoverySettings.getAvailableNetworkBandwidth().getBytes(), 0L)
            : 0L;
        this.ingress = new Direction("ingress");
        this.egress = new Direction("egress");

        // today's concurrency, also the target while adaptive upload concurrency is off
        final int floor = ThreadPool.getDefaultSnapshotConcurrency(threadPool);
        final int ceiling = Math.max(
            floor,
            Math.min(ThreadPool.getSnapshotUploadConcurrencyCeiling(totalMemoryBytes), threadPool.info(ThreadPool.Names.SNAPSHOT).getMax())
        );
        this.uploadConcurrencyController = new UploadConcurrencyController(floor, ceiling);
        this.uploadTaskRunner = new PrioritizedThrottledTaskRunner<>(
            ShardSnapshotTaskRunner.TASK_RUNNER_NAME,
            floor,
            threadPool.executor(ThreadPool.Names.SNAPSHOT)
        );

        clusterSettings.initializeAndWatch(BACKGROUND_QOS_ENABLED_SETTING, enabled -> this.backgroundQosEnabled = enabled);
        clusterSettings.initializeAndWatch(
            ADAPTIVE_UPLOAD_CONCURRENCY_ENABLED_SETTING,
            enabled -> this.adaptiveUploadConcurrencyEnabled = enabled
        );
    }

    /**
     * Whether snapshots and restores should use {@link #getIngressLimiter()} and {@link #getEgressLimiter()}.
     */
    public boolean isBackgroundQosEnabled() {
        return backgroundQosEnabled && shareBytesPerSec > 0L;
    }

    public RateLimiter getIngressLimiter() {
        return ingress.limiter;
    }

    public RateLimiter getEgressLimiter() {
        return egress.limiter;
    }

    /**
     * The node-level runner for shard snapshot tasks, shared by all repositories.
     */
    public PrioritizedThrottledTaskRunner<ShardSnapshotTaskRunner.SnapshotTask> getUploadTaskRunner() {
        return uploadTaskRunner;
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

    @Override
    protected void doStart() {
        lastTickNanos = nanoTimeSupplier.getAsLong();
        intervalStartNanos = lastTickNanos;
        scheduledTick = threadPool.scheduleWithFixedDelay(this::tick, TICK_INTERVAL, threadPool.generic());
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

            final NetworkProbe.NetworkStats networkStats = networkStatsSupplier.get();
            final boolean qosEnabled = isBackgroundQosEnabled();
            ingress.update(networkStats == null ? -1L : networkStats.receiveBytes(), elapsedNanos, qosEnabled);
            egress.update(networkStats == null ? -1L : networkStats.transmitBytes(), elapsedNanos, qosEnabled);

            final long uploadBytesNow = uploadBytes.sum();
            if (ingress.lastBackgroundBytesPerSec > 0 || egress.lastBackgroundBytesPerSec > 0 || uploadBytesNow != lastUploadBytes) {
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
        final long intervalNanos = now - intervalStartNanos;
        final long bytes = uploadBytesNow - intervalStartUploadBytes;
        final long pauseNanos = uploadPauseNanosNow - intervalStartUploadPauseNanos;
        intervalStartNanos = now;
        intervalStartUploadBytes = uploadBytesNow;
        intervalStartUploadPauseNanos = uploadPauseNanosNow;

        if (adaptiveUploadConcurrencyEnabled) {
            adaptiveUploadConcurrencyActive = true;
            if (intervalNanos <= 0L) {
                return;
            }
            final UploadConcurrencyController.Decision decision = uploadConcurrencyController.onInterval(
                uploadTaskRunner.queueSize(),
                uploadTaskRunner.runningTasks(),
                bytes * (double) TimeUnit.SECONDS.toNanos(1) / intervalNanos,
                pauseNanos,
                intervalNanos,
                cpuPercentSupplier.getAsInt()
            );
            setUploadConcurrency(decision.target());
        } else {
            if (adaptiveUploadConcurrencyActive) {
                adaptiveUploadConcurrencyActive = false;
                uploadConcurrencyController.reset();
            }
            setUploadConcurrency(uploadConcurrencyController.getFloor());
        }
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
        logger.info(
            "background network qos [{}], share [{}/s]; {}; {}; uploads adaptive [{}] target [{}] running [{}] queued [{}] ceiling [{}] "
                + "last decision [{}: {}]",
            qosEnabled ? "on" : "off",
            ByteSizeValue.ofBytes(shareBytesPerSec),
            ingress.describeAndResetPause(),
            egress.describeAndResetPause(),
            adaptiveUploadConcurrencyActive ? "on" : "off",
            uploadTaskRunner.getMaxRunningTasks(),
            uploadTaskRunner.runningTasks(),
            uploadTaskRunner.queueSize(),
            uploadConcurrencyController.getCeiling(),
            decision.action(),
            decision.reason()
        );
    }

    /**
     * The rate floor: today's network-only recovery rate, capped at the share.
     */
    static long computeFloor(long shareBytesPerSec) {
        if (shareBytesPerSec <= 0L) {
            return MIN_FLOOR_BYTES_PER_SEC;
        }
        return Math.min(shareBytesPerSec, Math.max(MIN_FLOOR_BYTES_PER_SEC, Math.round(shareBytesPerSec * FLOOR_FRACTION)));
    }

    /**
     * Computes the next background rate for one direction: the share left over by foreground traffic, minus some headroom, between the
     * floor and the share. Falls apply at once, rises are limited per tick.
     *
     * @param currentRate               the current rate in bytes per second
     * @param shareBytesPerSec          the node's network share for this direction
     * @param nodeBytesPerSec           all node traffic in this direction over the last tick, or negative if unknown
     * @param backgroundBytesPerSec     background traffic in this direction over the last tick
     */
    static long computeRate(long currentRate, long shareBytesPerSec, long nodeBytesPerSec, long backgroundBytesPerSec) {
        final long floor = computeFloor(shareBytesPerSec);
        if (nodeBytesPerSec < 0L) {
            return floor;
        }
        final long foreground = Math.max(0L, nodeBytesPerSec - backgroundBytesPerSec);
        final long headroom = Math.round(shareBytesPerSec * HEADROOM_FRACTION);
        final long target = Math.clamp(shareBytesPerSec - foreground - headroom, floor, shareBytesPerSec);
        if (target <= currentRate) {
            return target;
        }
        return Math.min(target, currentRate + Math.round(shareBytesPerSec * MAX_STEP_UP_FRACTION));
    }

    private static long perSecond(long delta, long elapsedNanos) {
        return Math.round(delta * (double) TimeUnit.SECONDS.toNanos(1) / elapsedNanos);
    }

    private static double toMBPerSec(long bytesPerSec) {
        return bytesPerSec / (double) ByteSizeUnit.MB.toBytes(1);
    }

    /**
     * Limiter and measurements for one direction. Only the limiter is accessed outside the periodic tick.
     */
    private class Direction {
        private final String name;
        private final CountingRateLimiter limiter;
        private final long floorBytesPerSec;

        private long rateBytesPerSec;
        private long lastNodeBytes = -1L;
        private long lastBackgroundBytes;
        private long lastPauseNanos;
        private long pauseNanosSinceLog;
        private long lastNodeBytesPerSec = -1L;
        private long lastBackgroundBytesPerSec;

        Direction(String name) {
            this.name = name;
            this.floorBytesPerSec = computeFloor(shareBytesPerSec);
            this.rateBytesPerSec = floorBytesPerSec;
            this.limiter = new CountingRateLimiter(toMBPerSec(floorBytesPerSec));
        }

        void update(long nodeBytes, long elapsedNanos, boolean qosEnabled) {
            final long backgroundBytes = limiter.getBytes();
            final long pauseNanos = limiter.getPauseNanos();
            if (elapsedNanos > 0L) {
                lastBackgroundBytesPerSec = perSecond(backgroundBytes - lastBackgroundBytes, elapsedNanos);
                // counters may reset, e.g. if an interface goes away; treat that as unknown
                lastNodeBytesPerSec = nodeBytes >= 0L && lastNodeBytes >= 0L && nodeBytes >= lastNodeBytes
                    ? perSecond(nodeBytes - lastNodeBytes, elapsedNanos)
                    : -1L;
            }
            pauseNanosSinceLog += pauseNanos - lastPauseNanos;
            lastNodeBytes = nodeBytes;
            lastBackgroundBytes = backgroundBytes;
            lastPauseNanos = pauseNanos;

            // when off, go back to the floor so that switching on starts from today's rate
            final long newRate = qosEnabled
                ? computeRate(rateBytesPerSec, shareBytesPerSec, lastNodeBytesPerSec, lastBackgroundBytesPerSec)
                : floorBytesPerSec;
            if (newRate != rateBytesPerSec) {
                rateBytesPerSec = newRate;
                limiter.setMBPerSec(toMBPerSec(newRate));
            }
        }

        String describeAndResetPause() {
            final String description = format(
                "%s fg [%s/s] bg [%s/s] rate [%s/s] pause [%dms]",
                name,
                lastNodeBytesPerSec < 0L ? "unknown" : ByteSizeValue.ofBytes(Math.max(0L, lastNodeBytesPerSec - lastBackgroundBytesPerSec)),
                ByteSizeValue.ofBytes(lastBackgroundBytesPerSec),
                ByteSizeValue.ofBytes(rateBytesPerSec),
                TimeUnit.NANOSECONDS.toMillis(pauseNanosSinceLog)
            );
            pauseNanosSinceLog = 0L;
            return description;
        }
    }
}
