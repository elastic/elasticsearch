/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.AbstractThrottledTaskRunner;
import org.elasticsearch.common.util.concurrent.PrioritizedThrottledTaskRunner;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.monitor.os.CgroupV2Probe;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.LinkedHashSet;
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

import static org.elasticsearch.core.Strings.format;

/**
 * Node-level control of the number of concurrent shard snapshot uploads, behind {@link #ADAPTIVE_UPLOAD_CONCURRENCY_ENABLED_SETTING}.
 * The uploads run on their own thread pool and have a fixed target per node, which an {@link UploadConcurrencyController} backs off from
 * every few seconds and then recovers to, one upload at a time: 10 below 8GiB of node memory, 20 from 8GiB, 40 from 64GiB, capped by
 * {@link #UPLOAD_CONCURRENCY_MAX_SETTING}. It backs off when CPU contention (cgroup pressure stall information and throttling,
 * read directly because uploads also use CPU outside their threads) delays foreground work, or when uploads fail. Off, uploads run
 * on the snapshot pool at today's concurrency.
 * <p>
 * Every repository always has its own task runner and queue for shard snapshot tasks, as it has always had. Adaptive upload concurrency
 * only adds a limit shared by all of them, a node-wide budget of tasks that may run at once, which a repository's runner asks for before
 * it starts a task and gives back when the task finishes. The runners that find the budget used up wait in a queue, and budget that is
 * given back is offered to them in the order in which they started waiting, so it is first come first served between repositories,
 * each getting what its runner can use; earliest deadline first, which the completion targets of the repositories would allow, is a
 * later design decision. While adaptive upload concurrency is off the budget is not consulted, so what runs is exactly what the
 * repositories' own runners allow; the tasks are still counted, which is all it costs, so that the budget is right when the switch is
 * turned on while snapshots run.
 * While it is off nothing else here does anything: no measurements are read and nothing is scheduled.
 * It only exists on stateless nodes, where snapshots read from the object store: other nodes have no such service and snapshot as
 * they always have, whatever the setting is.
 */
public class UploadConcurrencyService extends AbstractLifecycleComponent {

    private static final Logger logger = LogManager.getLogger(UploadConcurrencyService.class);

    public static final Setting<Boolean> ADAPTIVE_UPLOAD_CONCURRENCY_ENABLED_SETTING = Setting.boolSetting(
        "indices.recovery.adaptive_upload_concurrency.enabled",
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * The most shard snapshot uploads the node runs at once: the ceiling is the lower of this and what the node's size allows (10 below
     * 8GiB of node memory, 20 from 8GiB, 40 from 64GiB, see {@link ThreadPool.Names#SNAPSHOT_UPLOAD}), which is also how far this can be
     * raised at runtime. Never below today's concurrency.
     * <p>
     * The default is 40 so that it does not clip the largest nodes. Smaller nodes stay at the target for their size.
     * <p>
     * This must stay below the connection limit of the object store client, keeping a share of the connections for foreground work:
     * every upload holds a connection while it runs, and uploads waiting for a connection would only look like a slow object store.
     */
    public static final Setting<Integer> UPLOAD_CONCURRENCY_MAX_SETTING = Setting.intSetting(
        "indices.recovery.upload_concurrency.max",
        40,
        1,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private static final String BUDGET_CHANGE_MESSAGE = "changing snapshot upload concurrency from [{}] to [{}] ({}: {})";

    /** How often the upload concurrency controller makes a decision. */
    static final TimeValue TICK_INTERVAL = TimeValue.timeValueSeconds(5);
    /** The status line is only logged while uploads ran or were queued in this time. */
    static final long LOG_ACTIVE_WINDOW_NANOS = TimeUnit.SECONDS.toNanos(30);

    /**
     * Where the node's measurements come from. Each supplier returns {@code null} if the measurement is not available.
     */
    record Probes(Supplier<CgroupV2Probe.CpuPressure> cpuPressure, Supplier<CgroupV2Probe.CpuThrottling> cpuThrottling) {
        static Probes forNode() {
            final CgroupV2Probe cgroupProbe = CgroupV2Probe.getInstance();
            return new Probes(cgroupProbe::getCpuPressure, cgroupProbe::getCpuThrottling);
        }
    }

    private final ThreadPool threadPool;
    private final Probes probes;
    private final LongSupplier nanoTimeSupplier;

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
    private final LongAdder uploadReadErrors = new LongAdder();
    private final LongAdder uploadWriteErrors = new LongAdder();

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
    private long lastActiveNanos;
    private boolean everActive;
    // whether the controller's interval measurements have a start to compare with
    private boolean trackingInterval;
    private long intervalStartNanos;
    private long intervalStartReadErrors;
    private long intervalStartWriteErrors;
    @Nullable
    private CgroupV2Probe.CpuPressure intervalStartCpuPressure;
    @Nullable
    private CgroupV2Probe.CpuThrottling intervalStartCpuThrottling;
    private UploadConcurrencyController.Signals lastSignals;
    private long readErrorsSinceLog;
    private long writeErrorsSinceLog;

    @SuppressWarnings("this-escape")
    public UploadConcurrencyService(ClusterSettings clusterSettings, ThreadPool threadPool) {
        this(clusterSettings, threadPool, Probes.forNode(), System::nanoTime);
    }

    @SuppressWarnings("this-escape")
    UploadConcurrencyService(ClusterSettings clusterSettings, ThreadPool threadPool, Probes probes, LongSupplier nanoTimeSupplier) {
        this.threadPool = threadPool;
        this.probes = probes;
        this.nanoTimeSupplier = nanoTimeSupplier;

        // today's concurrency, also the budget while adaptive upload concurrency starts
        final int floor = threadPool.info(ThreadPool.Names.SNAPSHOT).getMax();
        this.todaysUploadConcurrency = floor;
        this.uploadBudget = floor;
        this.uploadTaskRunnerCap = floor;
        this.nodeUploadConcurrencyCeiling = Math.max(floor, threadPool.info(ThreadPool.Names.SNAPSHOT_UPLOAD).getMax());
        this.uploadConcurrencyController = new UploadConcurrencyController(floor, nodeUploadConcurrencyCeiling);

        clusterSettings.initializeAndWatch(UPLOAD_CONCURRENCY_MAX_SETTING, max -> this.uploadConcurrencyMax = max);
        uploadConcurrencyController.setCeiling(Math.min(uploadConcurrencyMax, nodeUploadConcurrencyCeiling));
        clusterSettings.initializeAndWatch(ADAPTIVE_UPLOAD_CONCURRENCY_ENABLED_SETTING, this::setAdaptiveUploadConcurrencyEnabled);
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
     * Runs the periodic tick only while adaptive upload concurrency is on, so that nothing is scheduled, woken up or measured on a node
     * that has it off. Must be called with the lock held whenever the switch or the lifecycle changed.
     */
    private void updateTick() {
        if (started && adaptiveUploadConcurrencyEnabled) {
            if (scheduledTick == null) {
                scheduledTick = threadPool.scheduleWithFixedDelay(this::tick, TICK_INTERVAL, threadPool.generic());
            }
        } else if (scheduledTick != null) {
            scheduledTick.cancel();
            scheduledTick = null;
            // nothing ticks to forget the measurements any more
            stopTracking();
        }
    }

    /**
     * Whether shard snapshot uploads are limited by the node-wide budget, see {@link #registerUploadTaskRunner}, and run on the
     * {@link ThreadPool.Names#SNAPSHOT_UPLOAD} pool: when the setting is on.
     */
    public boolean isAdaptiveUploadConcurrencyEnabled() {
        return adaptiveUploadConcurrencyEnabled;
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
        // under the lock that applying the cap holds, so that a change of the cap is not lost between reading it and setting it
        synchronized (lock) {
            uploadTaskRunners.add(taskRunner);
            taskRunner.setMaxRunningTasks(uploadTaskRunnerCap);
        }
        return () -> uploadTaskRunners.remove(taskRunner);
    }

    private void applyUploadTaskRunnerCap() {
        uploadTaskRunnerCap = adaptiveUploadConcurrencyEnabled ? uploadConcurrencyController.getCeiling() : todaysUploadConcurrency;
        uploadTaskRunners.forEach(taskRunner -> taskRunner.setMaxRunningTasks(uploadTaskRunnerCap));
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
                "snapshot upload concurrency measurements: cpu.pressure [{}], cpu.stat throttling [{}]",
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
                if (adaptiveUploadConcurrencyEnabled == false) {
                    stopTracking();
                    return;
                }
                logProbesOnce();

                if (runningUploadTasks.get() > 0 || queuedUploadTasks() > 0) {
                    lastActiveNanos = now;
                    everActive = true;
                }

                updateUploadConcurrency(now);
                if (everActive && now - lastActiveNanos <= LOG_ACTIVE_WINDOW_NANOS) {
                    if (logger.isDebugEnabled()) {
                        logStatus();
                    }
                    // counted since the status was last due, whether or not it is logged
                    readErrorsSinceLog = 0L;
                    writeErrorsSinceLog = 0L;
                }
            } catch (Exception e) {
                logger.warn("snapshot upload concurrency update failed", e);
                assert false : e;
            }
        }
    }

    private void stopTracking() {
        trackingInterval = false;
    }

    private void updateUploadConcurrency(long now) {
        final long readErrorsNow = uploadReadErrors.sum();
        final long writeErrorsNow = uploadWriteErrors.sum();
        final CgroupV2Probe.CpuPressure cpuPressureNow = probes.cpuPressure().get();
        final CgroupV2Probe.CpuThrottling cpuThrottlingNow = probes.cpuThrottling().get();

        final boolean hadStart = trackingInterval;
        final long intervalNanos = now - intervalStartNanos;
        final UploadConcurrencyController.Signals signals = new UploadConcurrencyController.Signals(
            intervalNanos,
            cpuPressure(intervalStartCpuPressure, cpuPressureNow, intervalNanos),
            throttledMicros(intervalStartCpuThrottling, cpuThrottlingNow),
            readErrorsNow - intervalStartReadErrors,
            writeErrorsNow - intervalStartWriteErrors
        );

        intervalStartNanos = now;
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
            final UploadConcurrencyController.Decision decision = uploadConcurrencyController.getLastDecision();
            // A raise is one step per interval while ramping up, which is only of interest when looking into it. A cut is not.
            if (budget > current) {
                logger.debug(BUDGET_CHANGE_MESSAGE, current, budget, decision.action(), decision.reason());
            } else {
                logger.info(BUDGET_CHANGE_MESSAGE, current, budget, decision.action(), decision.reason());
            }
            uploadBudget = budget;
            if (budget > current) {
                // runners may have tasks waiting for it
                offerBudgetToWaitingRunners();
            }
        }
    }

    private void logStatus() {
        final UploadConcurrencyController.Decision decision = uploadConcurrencyController.getLastDecision();
        final UploadConcurrencyController.Signals signals = lastSignals;
        logger.debug(
            "snapshot upload concurrency; cpu pressure [{}] throttled [{}us]; "
                + "upload errors since last log read [{}] upload [{}]; uploads adaptive [{}] target [{}] running [{}] queued [{}] "
                + "ceiling [{}] last decision [{}: {}]",
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
    }
}
