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
import org.elasticsearch.common.util.concurrent.AbstractThrottledTaskRunner;
import org.elasticsearch.common.util.concurrent.PrioritizedThrottledTaskRunner;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.snapshots.blobstore.RateLimitingInputStream;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.monitor.network.NetworkProbe;
import org.elasticsearch.monitor.os.CgroupV2Probe;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
 *     {@link #UPLOAD_CONCURRENCY_MAX_SETTING}. It reads CPU contention directly (cgroup pressure stall information and throttling)
 *     because uploads also use CPU outside their threads, and upload errors. Off, uploads run on the snapshot pool at today's
 *     concurrency.</li>
 * </ul>
 * Every repository always has its own task runner and queue for shard snapshot tasks, as it has always had. Adaptive upload concurrency
 * only adds a limit shared by all of them, a node-wide budget of tasks that may run at once, which a repository's runner asks for before
 * it starts a task and gives back when the task finishes. The runners that find the budget used up wait in a queue, and budget that is
 * given back is offered to them in the order in which they started waiting, so it is first come first served between repositories,
 * each getting what its runner can use; earliest deadline first, which the completion targets of the repositories would allow, is a
 * later design decision. While adaptive upload concurrency is off the budget is not consulted, so what runs is exactly what the repositories' own runners allow;
 * the tasks are still counted, which is all it costs, so that the budget is right when the switch is turned on while snapshots run.
 * While both switches are off nothing else here does anything: no measurements are read, no bytes are counted and nothing is scheduled.
 * Background QoS only applies on stateless nodes, where snapshots read from the object store; on other nodes they read local disk.
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
     * node's size allows (10 per 2GiB of node memory), see {@link ThreadPool.Names#SNAPSHOT_UPLOAD}, which is also how far this can be
     * raised at runtime. Never below today's concurrency.
     * <p>
     * Before raising the default above 30, the ceiling must also respect the connection limit of the object store client (50 by default
     * for the client used for backups), keeping a share of the connections for foreground work: every upload holds a connection while
     * it runs, and uploads waiting for a connection would only look like a slow object store.
     */
    public static final Setting<Integer> UPLOAD_CONCURRENCY_MAX_SETTING = Setting.intSetting(
        "indices.recovery.upload_concurrency.max",
        30,
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
     * Where the node's measurements come from. Each supplier returns {@code null} if the measurement is not available.
     */
    record Probes(
        Supplier<NetworkProbe.NetworkStats> network,
        Supplier<CgroupV2Probe.CpuPressure> cpuPressure,
        Supplier<CgroupV2Probe.CpuThrottling> cpuThrottling
    ) {
        static Probes forNode() {
            final NetworkProbe networkProbe = NetworkProbe.getInstance();
            final CgroupV2Probe cgroupProbe = CgroupV2Probe.getInstance();
            return new Probes(networkProbe::getNetworkStats, cgroupProbe::getCpuPressure, cgroupProbe::getCpuThrottling);
        }
    }

    private final ThreadPool threadPool;
    private final RecoverySettings recoverySettings;
    private final Probes probes;
    private final LongSupplier nanoTimeSupplier;
    private final boolean stateless;

    private final Resource networkIn;
    private final Resource networkOut;
    private final List<Resource> resources;

    // The runners that found the budget used up, in the order in which they did, as what to call to make them try again. Guarded by itself.
    private final Set<Runnable> waitingUploadTaskRunners = new LinkedHashSet<>();
    private final AtomicBoolean waitersOfferScheduled = new AtomicBoolean();
    // The runner that is being offered budget on this thread
    private final ThreadLocal<Runnable> offeredTo = new ThreadLocal<>();
    // The task runners of the repositories on this node, see registerUploadTaskRunner
    private final Set<PrioritizedThrottledTaskRunner<?>> uploadTaskRunners = ConcurrentHashMap.newKeySet();
    // The tasks the runners have started and not finished, counted whether or not the budget is consulted, so that it is right at once
    // when the switch is turned on while snapshots run
    private final AtomicInteger runningUploadTasks = new AtomicInteger();
    private final UploadConcurrencyController uploadConcurrencyController;
    private final LongAdder uploadBytes = new LongAdder();
    private final LongAdder uploadPauseNanos = new LongAdder();
    private final LongAdder uploadReadErrors = new LongAdder();
    private final LongAdder uploadWriteErrors = new LongAdder();

    private volatile boolean backgroundQosEnabled;
    private volatile boolean adaptiveUploadConcurrencyEnabled;
    private volatile int uploadConcurrencyMax;
    private final int nodeUploadConcurrencyCeiling;
    // The node-wide budget: how many tasks the runners may run at once while adaptive upload concurrency is on, set by the controller
    private volatile int uploadBudget;
    // How many tasks a repository's runner runs at once: today's concurrency when adaptive is off, else the ceiling, as the budget is
    // what limits them then
    private volatile int uploadTaskRunnerCap;
    private final int todaysUploadConcurrency;

    // Everything below is guarded by the lock, which the periodic tick holds and so does a change of a switch.
    private final Object lock = new Object();
    private boolean started;
    // the periodic tick, which exists only while it has something to do
    @Nullable
    private Scheduler.Cancellable scheduledTick;
    private boolean loggedProbes;
    private long tickCount;
    private long lastActiveNanos;
    private boolean everActive;
    private long lastUploadBytes;
    // whether the network measurements, or the controller's interval measurements, have a start to compare with
    private boolean trackingNetwork;
    private boolean trackingInterval;
    private long lastNetworkTickNanos;
    private long intervalStartNanos;
    private long intervalStartUploadBytes;
    private long intervalStartUploadPauseNanos;
    private long intervalStartReadErrors;
    private long intervalStartWriteErrors;
    @Nullable
    private CgroupV2Probe.CpuPressure intervalStartCpuPressure;
    @Nullable
    private CgroupV2Probe.CpuThrottling intervalStartCpuThrottling;
    private UploadConcurrencyController.Signals lastSignals;
    private long readErrorsSinceLog;
    private long writeErrorsSinceLog;

    /**
     * @param stateless whether this is a stateless node, the only kind that snapshots read from the object store, which the network
     *                  limiters are for
     */
    @SuppressWarnings("this-escape")
    public BackgroundNetworkQos(
        ClusterSettings clusterSettings,
        ThreadPool threadPool,
        RecoverySettings recoverySettings,
        boolean stateless
    ) {
        this(clusterSettings, threadPool, recoverySettings, stateless, Probes.forNode(), System::nanoTime);
    }

    @SuppressWarnings("this-escape")
    BackgroundNetworkQos(
        ClusterSettings clusterSettings,
        ThreadPool threadPool,
        RecoverySettings recoverySettings,
        boolean stateless,
        Probes probes,
        LongSupplier nanoTimeSupplier
    ) {
        this.threadPool = threadPool;
        this.recoverySettings = recoverySettings;
        this.stateless = stateless;
        this.probes = probes;
        this.nanoTimeSupplier = nanoTimeSupplier;
        // same capacity in both network directions until we know whether the node's share applies per direction or combined
        final long networkCapacity = recoverySettings.nodeBandwidthSettingsExist()
            ? Math.max(recoverySettings.getAvailableNetworkBandwidth().getBytes(), 0L)
            : 0L;
        // a snapshot reads each byte it uploads from the object store, so every background byte is both ingress and egress
        this.networkIn = new Resource("net in", networkCapacity, uploadBytes::sum, stats -> stats == null ? -1L : stats.receiveBytes());
        this.networkOut = new Resource("net out", networkCapacity, uploadBytes::sum, stats -> stats == null ? -1L : stats.transmitBytes());
        this.resources = List.of(networkIn, networkOut);

        // today's concurrency, also the budget while adaptive upload concurrency starts
        final int floor = threadPool.info(ThreadPool.Names.SNAPSHOT).getMax();
        this.todaysUploadConcurrency = floor;
        this.uploadBudget = floor;
        this.uploadTaskRunnerCap = floor;
        this.nodeUploadConcurrencyCeiling = Math.max(floor, threadPool.info(ThreadPool.Names.SNAPSHOT_UPLOAD).getMax());
        this.uploadConcurrencyController = new UploadConcurrencyController(floor, nodeUploadConcurrencyCeiling);

        clusterSettings.initializeAndWatch(UPLOAD_CONCURRENCY_MAX_SETTING, max -> this.uploadConcurrencyMax = max);
        uploadConcurrencyController.setCeiling(Math.min(uploadConcurrencyMax, nodeUploadConcurrencyCeiling));
        clusterSettings.initializeAndWatch(BACKGROUND_QOS_ENABLED_SETTING, this::setBackgroundQosEnabled);
        clusterSettings.initializeAndWatch(ADAPTIVE_UPLOAD_CONCURRENCY_ENABLED_SETTING, this::setAdaptiveUploadConcurrencyEnabled);
    }

    private void setBackgroundQosEnabled(boolean enabled) {
        synchronized (lock) {
            this.backgroundQosEnabled = enabled;
            updateTick();
        }
    }

    private void setAdaptiveUploadConcurrencyEnabled(boolean enabled) {
        // serialized with the tick, which must not be deciding a target while this puts the controller back to the floor
        synchronized (lock) {
            this.adaptiveUploadConcurrencyEnabled = enabled;
            // off: do not leave the adaptive target in place until the next interval. On: start from today's concurrency.
            trackingInterval = false;
            uploadConcurrencyController.reset();
            uploadBudget = uploadConcurrencyController.getFloor();
            // the setting may have changed while nothing was looking
            uploadConcurrencyController.setCeiling(Math.min(uploadConcurrencyMax, nodeUploadConcurrencyCeiling));
            // what waited for the budget before is not waiting for this one: applying the caps makes the runners try, and wait again
            synchronized (waitingUploadTaskRunners) {
                waitingUploadTaskRunners.clear();
            }
            // the runners' caps change with the switch, which also lets them start what the new limits allow
            applyUploadTaskRunnerCap();
            updateTick();
        }
    }

    /**
     * Runs the periodic tick only while a switch is on, so that nothing is scheduled, woken up or measured on a node that has both off.
     * Must be called with the lock held whenever a switch or the lifecycle changed.
     */
    private void updateTick() {
        if (started && isActive()) {
            if (scheduledTick == null) {
                scheduledTick = threadPool.scheduleWithFixedDelay(this::tick, TICK_INTERVAL, threadPool.generic());
            }
        } else if (scheduledTick != null) {
            scheduledTick.cancel();
            scheduledTick = null;
            // nothing ticks to put the limiters back to their floor and to forget the measurements any more
            stopTracking();
        }
    }

    /**
     * Whether snapshots should use {@link #getIngressLimiter()} and {@link #getEgressLimiter()}.
     */
    public boolean isBackgroundQosEnabled() {
        return stateless && backgroundQosEnabled && networkIn.capacityBytesPerSec > 0L;
    }

    /**
     * Whether shard snapshot uploads are limited by the node-wide budget, see {@link #registerUploadTaskRunner}, and run on the
     * {@link ThreadPool.Names#SNAPSHOT_UPLOAD} pool.
     */
    public boolean isAdaptiveUploadConcurrencyEnabled() {
        return adaptiveUploadConcurrencyEnabled;
    }

    /**
     * Whether either switch is on. Off, snapshots behave as they always did and this does not measure or count anything.
     */
    public boolean isActive() {
        return adaptiveUploadConcurrencyEnabled || isBackgroundQosEnabled();
    }

    public RateLimiter getIngressLimiter() {
        return networkIn.limiter;
    }

    public RateLimiter getEgressLimiter() {
        return networkOut.limiter;
    }

    /**
     * The executor for the shard snapshot tasks of a repository's task runner: the {@link ThreadPool.Names#SNAPSHOT} pool, as it has
     * always been, or the {@link ThreadPool.Names#SNAPSHOT_UPLOAD} pool while adaptive upload concurrency is on, which has the threads
     * for more tasks than that. Chosen as each task is started.
     */
    public Executor getUploadExecutor() {
        return command -> threadPool.executor(
            adaptiveUploadConcurrencyEnabled ? ThreadPool.Names.SNAPSHOT_UPLOAD : ThreadPool.Names.SNAPSHOT
        ).execute(command);
    }

    /**
     * Asks for the budget to start a shard snapshot task, to be given to the runner of a repository as its
     * {@link AbstractThrottledTaskRunner.StartPermits}. While adaptive upload concurrency is on this is only granted while fewer tasks
     * than the budget are running on the node. While it is off it is always granted, so that the budget has no say in what runs, but
     * still counted.
     *
     * @param retry makes the runner try again, which is called, on another thread, when the budget is not used up any more, in the order
     *              in which runners were refused
     * @return the permit to close when the task finishes, or {@code null} if the budget is used up
     */
    @Nullable
    public Releasable tryAcquireUploadPermit(Runnable retry) {
        if (adaptiveUploadConcurrencyEnabled == false) {
            runningUploadTasks.incrementAndGet();
            return Releasables.releaseOnce(this::releaseUploadPermit);
        }
        // Budget goes to the runner that has waited longest, so a runner that is not that one, and not being offered the budget now, has
        // to wait even if there is some, which it is about to be offered
        final boolean first;
        synchronized (waitingUploadTaskRunners) {
            first = waitingUploadTaskRunners.isEmpty() || waitingUploadTaskRunners.iterator().next() == retry || offeredTo.get() == retry;
        }
        final Releasable permit = first ? tryAcquireBudget() : null;
        if (permit != null) {
            offeredTo.remove();
            synchronized (waitingUploadTaskRunners) {
                waitingUploadTaskRunners.remove(retry);
            }
        } else {
            synchronized (waitingUploadTaskRunners) {
                waitingUploadTaskRunners.add(retry);
            }
            // some may have been given back before the runner was in the queue, which then nobody offers it to
            if (hasBudget()) {
                offerBudgetToWaitingRunners();
            }
        }
        return permit;
    }

    @Nullable
    private Releasable tryAcquireBudget() {
        while (true) {
            final int running = runningUploadTasks.get();
            if (running >= uploadBudget) {
                return null;
            }
            if (runningUploadTasks.compareAndSet(running, running + 1)) {
                return Releasables.releaseOnce(this::releaseUploadPermit);
            }
        }
    }

    private boolean hasBudget() {
        return runningUploadTasks.get() < uploadBudget;
    }

    private void releaseUploadPermit() {
        runningUploadTasks.decrementAndGet();
        if (adaptiveUploadConcurrencyEnabled) {
            offerBudgetToWaitingRunners();
        }
    }

    /**
     * Makes the runners that wait for budget try again, in the order in which they started waiting, for as long as there is budget. It
     * is never done on the call stack of the runner that gives the budget back: when the budget is given back because a task is rejected,
     * as it is when the node shuts down, that would nest one call into another for every task that is queued.
     */
    private void offerBudgetToWaitingRunners() {
        synchronized (waitingUploadTaskRunners) {
            if (waitingUploadTaskRunners.isEmpty()) {
                return;
            }
        }
        if (waitersOfferScheduled.compareAndSet(false, true)) {
            try {
                threadPool.generic().execute(() -> {
                    // what changes from here on is offered again by whoever changes it
                    waitersOfferScheduled.set(false);
                    Runnable waiting;
                    while (hasBudget() && (waiting = pollWaitingUploadTaskRunner()) != null) {
                        // it is its turn, which lasts for the first task it starts
                        offeredTo.set(waiting);
                        try {
                            waiting.run();
                        } finally {
                            offeredTo.remove();
                        }
                    }
                });
            } catch (Exception e) {
                // the node is shutting down
                waitersOfferScheduled.set(false);
                logger.trace("could not offer upload budget to waiting runners", e);
            }
        }
    }

    @Nullable
    private Runnable pollWaitingUploadTaskRunner() {
        synchronized (waitingUploadTaskRunners) {
            final var iterator = waitingUploadTaskRunners.iterator();
            if (iterator.hasNext() == false) {
                return null;
            }
            final Runnable first = iterator.next();
            iterator.remove();
            return first;
        }
    }

    /**
     * Registers the task runner of a repository, which has to use {@link #tryAcquireUploadPermit} and {@link #getUploadExecutor}. From
     * then on this sets how many tasks it runs at once, and asks it to start tasks when the budget allows.
     *
     * @return what to close when the repository is closed
     */
    public Releasable registerUploadTaskRunner(PrioritizedThrottledTaskRunner<?> taskRunner) {
        uploadTaskRunners.add(taskRunner);
        // whatever the state of the switch is now, which a change does not miss as it applies the cap to the runners it finds
        taskRunner.setMaxRunningTasks(uploadTaskRunnerCap);
        return () -> uploadTaskRunners.remove(taskRunner);
    }

    private void applyUploadTaskRunnerCap() {
        uploadTaskRunnerCap = adaptiveUploadConcurrencyEnabled ? uploadConcurrencyController.getCeiling() : todaysUploadConcurrency;
        uploadTaskRunners.forEach(taskRunner -> taskRunner.setMaxRunningTasks(uploadTaskRunnerCap));
    }

    /**
     * Wraps a snapshot upload stream to count the background bytes, an input to the limiters and to the upload concurrency controller.
     * Every byte is counted once each time it is read, so bytes that are read again after a reset of the stream, or by a retried
     * upload, are counted again: they cross the network again, except when the stream replays them from memory, which overstates the
     * background traffic a little. The foreground traffic is what is left of the node's traffic after the background traffic, so it is
     * understated by as much, and the limiters, which give background work what the foreground does not use, become a little less
     * careful, never more.
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
     * Counts a failed upload that failed reading the source, which is shared with foreground work, if adaptive upload concurrency is on,
     * as the errors are only an input to it.
     */
    public void onUploadReadError() {
        if (adaptiveUploadConcurrencyEnabled) {
            uploadReadErrors.increment();
        }
    }

    /**
     * Counts a failed upload that failed writing to the repository, if adaptive upload concurrency is on.
     */
    public void onUploadWriteError() {
        if (adaptiveUploadConcurrencyEnabled) {
            uploadWriteErrors.increment();
        }
    }

    // package-private for tests
    long getCountedUploadBytes() {
        return uploadBytes.sum();
    }

    /**
     * The number of shard snapshot tasks that the repositories' runners have started and not finished on this node.
     */
    public int getRunningUploadTasks() {
        return runningUploadTasks.get();
    }

    // package-private for tests
    int getUploadBudget() {
        return uploadBudget;
    }

    // package-private for tests
    int getUploadConcurrencyCeiling() {
        synchronized (lock) {
            return uploadConcurrencyController.getCeiling();
        }
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
        synchronized (lock) {
            started = true;
            updateTick();
        }
    }

    @Override
    protected void doStop() {
        synchronized (lock) {
            started = false;
            updateTick();
        }
    }

    @Override
    protected void doClose() {}

    private void logProbesOnce() {
        if (loggedProbes == false) {
            loggedProbes = true;
            logger.info(
                "background qos measurements: network [{}], cpu.pressure [{}], cpu.stat throttling [{}]",
                availability(probes.network()),
                availability(probes.cpuPressure()),
                availability(probes.cpuThrottling())
            );
        }
    }

    private static String availability(Supplier<?> probe) {
        return probe.get() == null ? "unavailable" : "available";
    }

    // package-private for tests
    void tick() {
        synchronized (lock) {
            try {
                final long now = nanoTimeSupplier.getAsLong();
                final boolean qosEnabled = isBackgroundQosEnabled();
                final boolean adaptive = adaptiveUploadConcurrencyEnabled;
                if (qosEnabled == false && adaptive == false) {
                    stopTracking();
                    return;
                }
                logProbesOnce();
                tickCount++;

                boolean backgroundActive = false;
                if (qosEnabled) {
                    final NetworkProbe.NetworkStats networkStats = probes.network().get();
                    if (trackingNetwork == false) {
                        trackingNetwork = true;
                        resources.forEach(Resource::startTracking);
                        lastNetworkTickNanos = now;
                    }
                    final long elapsedNanos = now - lastNetworkTickNanos;
                    lastNetworkTickNanos = now;
                    for (Resource resource : resources) {
                        resource.update(networkStats, elapsedNanos, recoverySettings.getExplicitMaxBytesPerSec().getBytes());
                        backgroundActive |= resource.lastBackgroundBytesPerSec > 0;
                    }
                } else if (trackingNetwork) {
                    stopTrackingNetwork();
                }

                final long uploadBytesNow = uploadBytes.sum();
                if (backgroundActive || uploadBytesNow != lastUploadBytes) {
                    lastActiveNanos = now;
                    everActive = true;
                }
                lastUploadBytes = uploadBytesNow;

                if (adaptive && tickCount % UPLOAD_CONCURRENCY_INTERVAL_TICKS == 0) {
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
    }

    private void stopTracking() {
        tickCount = 0;
        trackingInterval = false;
        if (trackingNetwork) {
            stopTrackingNetwork();
        }
    }

    private void stopTrackingNetwork() {
        // back to the floor, so that switching on starts from today's rate
        trackingNetwork = false;
        resources.forEach(Resource::stopTracking);
    }

    private void updateUploadConcurrency(long now, long uploadBytesNow) {
        final long uploadPauseNanosNow = uploadPauseNanos.sum();
        final long readErrorsNow = uploadReadErrors.sum();
        final long writeErrorsNow = uploadWriteErrors.sum();
        final CgroupV2Probe.CpuPressure cpuPressureNow = probes.cpuPressure().get();
        final CgroupV2Probe.CpuThrottling cpuThrottlingNow = probes.cpuThrottling().get();

        final boolean hadStart = trackingInterval;
        final long intervalNanos = now - intervalStartNanos;
        final long bytes = uploadBytesNow - intervalStartUploadBytes;
        final long pauseNanos = uploadPauseNanosNow - intervalStartUploadPauseNanos;
        final UploadConcurrencyController.Signals signals = new UploadConcurrencyController.Signals(
            queuedUploadTasks(),
            runningUploadTasks.get(),
            intervalNanos > 0L ? bytes * (double) TimeUnit.SECONDS.toNanos(1) / intervalNanos : 0.0,
            pauseNanos,
            intervalNanos,
            cpuPressure(intervalStartCpuPressure, cpuPressureNow, intervalNanos),
            throttledMicros(intervalStartCpuThrottling, cpuThrottlingNow),
            readErrorsNow - intervalStartReadErrors,
            writeErrorsNow - intervalStartWriteErrors
        );

        intervalStartNanos = now;
        intervalStartUploadBytes = uploadBytesNow;
        intervalStartUploadPauseNanos = uploadPauseNanosNow;
        intervalStartReadErrors = readErrorsNow;
        intervalStartWriteErrors = writeErrorsNow;
        intervalStartCpuPressure = cpuPressureNow;
        intervalStartCpuThrottling = cpuThrottlingNow;
        trackingInterval = true;

        // the setting may have changed
        final int previousCeiling = uploadConcurrencyController.getCeiling();
        uploadConcurrencyController.setCeiling(Math.min(uploadConcurrencyMax, nodeUploadConcurrencyCeiling));
        if (uploadConcurrencyController.getCeiling() != previousCeiling) {
            applyUploadTaskRunnerCap();
        }
        if (hadStart == false || intervalNanos <= 0L) {
            // the first interval after switching on has nothing to compare with
            return;
        }
        readErrorsSinceLog += signals.readErrors();
        writeErrorsSinceLog += signals.uploadErrors();
        lastSignals = signals;
        setUploadBudget(uploadConcurrencyController.onInterval(signals).target());
    }

    private int queuedUploadTasks() {
        int queued = 0;
        for (PrioritizedThrottledTaskRunner<?> taskRunner : uploadTaskRunners) {
            queued += taskRunner.queueSize();
        }
        return queued;
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

    private void setUploadBudget(int budget) {
        final int current = uploadBudget;
        if (current != budget) {
            logger.debug("changing snapshot upload concurrency from [{}] to [{}]", current, budget);
            uploadBudget = budget;
            if (budget > current) {
                // runners may have tasks waiting for it
                offerBudgetToWaitingRunners();
            }
        }
    }

    private void logStatus(boolean qosEnabled) {
        final UploadConcurrencyController.Decision decision = uploadConcurrencyController.getLastDecision();
        final UploadConcurrencyController.Signals signals = lastSignals;
        logger.info(
            "background network qos [{}]; {}; {}; cpu pressure [{}] throttled [{}us]; "
                + "upload errors since last log read [{}] upload [{}]; uploads adaptive [{}] target [{}] running [{}] queued [{}] "
                + "ceiling [{}] last decision [{}: {}]",
            qosEnabled ? "on" : "off",
            networkIn.describeAndResetPause(),
            networkOut.describeAndResetPause(),
            signals == null || signals.cpuPressure().isEmpty() ? "unknown" : format("%.3f", signals.cpuPressure().getAsDouble()),
            signals == null || signals.throttledMicros().isEmpty() ? "unknown" : signals.throttledMicros().getAsLong(),
            readErrorsSinceLog,
            writeErrorsSinceLog,
            adaptiveUploadConcurrencyEnabled ? "on" : "off",
            uploadBudget,
            runningUploadTasks.get(),
            queuedUploadTasks(),
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

    /**
     * The rate the limiter applies: the computed rate, but never more than an operator has set {@code indices.recovery.max_bytes_per_sec}
     * to.
     *
     * @param operatorMaxBytesPerSec the setting if it was set explicitly, otherwise zero or less, where zero is no limit
     */
    static long applyOperatorMax(long rate, long operatorMaxBytesPerSec) {
        return operatorMaxBytesPerSec > 0L ? Math.min(rate, operatorMaxBytesPerSec) : rate;
    }

    private static long perSecond(long delta, long elapsedNanos) {
        return Math.round(delta * (double) TimeUnit.SECONDS.toNanos(1) / elapsedNanos);
    }

    private static double toMBPerSec(long bytesPerSec) {
        return bytesPerSec / (double) ByteSizeUnit.MB.toBytes(1);
    }

    /**
     * Limiter and measurements for one resource. Only the limiter is accessed outside the lock.
     */
    private static class Resource {
        private final String name;
        private final long capacityBytesPerSec;
        private final LongSupplier backgroundBytes;
        private final ToLongFunction<NetworkProbe.NetworkStats> usageBytes;
        private final CountingRateLimiter limiter;
        private final long floorBytesPerSec;

        // the rate by the computation, and the rate applied to the limiter, which an operator's setting may lower
        private long rateBytesPerSec;
        private long appliedRateBytesPerSec;
        private long lastUsageBytes = -1L;
        private long lastBackgroundBytes;
        private long lastPauseNanos;
        private long pauseNanosSinceLog;
        private long lastUsageBytesPerSec = -1L;
        private long lastBackgroundBytesPerSec;

        /**
         * @param backgroundBytes the cumulative bytes background work used of this resource
         * @param usageBytes      the cumulative bytes the whole pod used of this resource from the stats (which may be null), or negative
         *                        if unknown
         */
        Resource(
            String name,
            long capacityBytesPerSec,
            LongSupplier backgroundBytes,
            ToLongFunction<NetworkProbe.NetworkStats> usageBytes
        ) {
            this.name = name;
            this.capacityBytesPerSec = capacityBytesPerSec;
            this.backgroundBytes = backgroundBytes;
            this.usageBytes = usageBytes;
            this.floorBytesPerSec = computeFloor(capacityBytesPerSec);
            this.limiter = new CountingRateLimiter(toMBPerSec(floorBytesPerSec));
            stopTracking();
        }

        /** Starts from the floor, with nothing to compare the next measurement with. */
        void startTracking() {
            lastUsageBytes = -1L;
            lastUsageBytesPerSec = -1L;
            lastBackgroundBytes = backgroundBytes.getAsLong();
            lastBackgroundBytesPerSec = 0L;
            lastPauseNanos = limiter.getPauseNanos();
            pauseNanosSinceLog = 0L;
            setRate(floorBytesPerSec, floorBytesPerSec);
        }

        void stopTracking() {
            lastUsageBytes = -1L;
            lastUsageBytesPerSec = -1L;
            lastBackgroundBytesPerSec = 0L;
            setRate(floorBytesPerSec, floorBytesPerSec);
        }

        private void setRate(long rate, long applied) {
            rateBytesPerSec = rate;
            if (applied != appliedRateBytesPerSec || limiter.getMBPerSec() != toMBPerSec(applied)) {
                appliedRateBytesPerSec = applied;
                limiter.setMBPerSec(toMBPerSec(applied));
            }
        }

        void update(@Nullable NetworkProbe.NetworkStats stats, long elapsedNanos, long operatorMaxBytesPerSec) {
            final long usage = usageBytes.applyAsLong(stats);
            final long background = backgroundBytes.getAsLong();
            final long pauseNanos = limiter.getPauseNanos();
            if (elapsedNanos > 0L) {
                lastBackgroundBytesPerSec = perSecond(background - lastBackgroundBytes, elapsedNanos);
                // counters may reset, e.g. if an interface goes away; treat that as unknown
                lastUsageBytesPerSec = usage >= 0L && lastUsageBytes >= 0L && usage >= lastUsageBytes
                    ? perSecond(usage - lastUsageBytes, elapsedNanos)
                    : -1L;
            }
            pauseNanosSinceLog += pauseNanos - lastPauseNanos;
            lastUsageBytes = usage;
            lastBackgroundBytes = background;
            lastPauseNanos = pauseNanos;

            final long newRate = computeRate(rateBytesPerSec, capacityBytesPerSec, lastUsageBytesPerSec, lastBackgroundBytesPerSec);
            setRate(newRate, applyOperatorMax(newRate, operatorMaxBytesPerSec));
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
                ByteSizeValue.ofBytes(appliedRateBytesPerSec),
                TimeUnit.NANOSECONDS.toMillis(pauseNanosSinceLog)
            );
            pauseNanosSinceLog = 0L;
            return description;
        }
    }
}
