/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Abstract thread-safe base class for GeoIP database downloaders.
 *
 * <p>Work is driven from two sources: a periodic schedule ({@link #scheduledPeriodicRun}) that
 * fires {@link #runPeriodic()} at the configured poll interval, and direct calls to
 * {@link #requestRunOnDemand()}. Both funnel into the state machine described below, which
 * serializes them onto a single worker.
 *
 * <h2>Concurrency model</h2>
 * The downloader is a state machine over {@link RunState}, driven exclusively by CAS on
 * {@link #runState}. The state name is the source of truth for what the downloader currently
 * owes:
 *
 * <ul>
 *   <li>{@link RunState#IDLE} — nothing running, nothing pending.</li>
 *   <li>{@link RunState#REQUESTED} — a worker has been submitted to {@code threadPool.generic()}
 *       and will pick up the request.</li>
 *   <li>{@link RunState#RUNNING} — {@link #runDownloader()} is executing on a worker.</li>
 *   <li>{@link RunState#RUNNING_AND_REQUESTED} — {@code runDownloader()} is executing and another
 *       run is owed when it finishes (any number of concurrent {@code requestRunOnDemand} calls
 *       coalesce into this single owed run).</li>
 *   <li>{@link RunState#DRAINING} — cancellation arrived while {@code runDownloader()}
 *       was in progress; the worker will fire {@link #markAsCompleted()} after it returns.</li>
 *   <li>{@link RunState#COMPLETED} — terminal; {@link #markAsCompleted()} has been (or is about
 *       to be) invoked exactly once.</li>
 * </ul>
 *
 * <h3>State diagram</h3>
 * <pre>
 *  Active cycle:
 *
 *                ┌──────┐
 *                │ IDLE │ ◀────────────────────────────┐
 *                └──┬───┘                              │
 *                   │  ▲                               │
 *                   │  │ rollback                      │  done
 *      request      │  │ (submit failed)               │  (no run owed)
 *      (submit)     ▼  │                               │
 *              ┌───────────┐                           │
 *              │ REQUESTED │ ◀──────────────────────┐  │
 *              └─────┬─────┘                        │  │
 *                    │                              │  │
 *                    │  pickup                      │  │
 *                    ▼                              │  │
 *              ┌─────────┐                          │  │
 *              │ RUNNING │ ─────────────────────────┼──┘
 *              └────┬────┘                          │
 *                   │  ▲                            │
 *      request      │  │  loop in place             │  handoff
 *      (coalesce)   │  │  transitionAfterRun(true)  │  transitionAfterRun(false)
 *                   ▼  │                            │
 *              ┌───────────────────────┐            │
 *              │ RUNNING_AND_REQUESTED │ ───────────┘
 *              └───────────────────────┘
 *
 *  Cancellation (onCancelled, from any non-terminal state):
 *
 *      IDLE                  ─────────────────────────▶ COMPLETED  (markAsCompleted immediately)
 *      REQUESTED             ─────────────────────────▶ COMPLETED  (markAsCompleted immediately)
 *      RUNNING               ───────▶ DRAINING ───────▶ COMPLETED  (markAsCompleted when runDownloader returns)
 *      RUNNING_AND_REQUESTED ───────▶ DRAINING ───────▶ COMPLETED  (markAsCompleted when runDownloader returns)
 * </pre>
 *
 * <h2>Drain contract</h2>
 * Two invariants together define the drain contract:
 * <ul>
 *   <li><b>{@link #markAsCompleted()} is fired at most once.</b> Completion is fired only on a
 *       successful CAS <i>into</i> {@code COMPLETED}, and {@code COMPLETED} is terminal, so at
 *       most one such CAS succeeds across the lifetime of the task.</li>
 *   <li><b>{@link #markAsCompleted()} never overlaps {@link #runDownloader()}.</b> While
 *       {@code runDownloader()} is executing the state is one of {@code RUNNING},
 *       {@code RUNNING_AND_REQUESTED}, or {@code DRAINING}; from those states
 *       {@link #onCancelled()} can only transition to {@code DRAINING} (never directly to
 *       {@code COMPLETED}), so the worker is the one that performs the
 *       {@code DRAINING → COMPLETED} CAS, after {@code runDownloader()} returns.</li>
 * </ul>
 *
 * <p>The persistent-tasks framework treats {@code markAsCompleted()} as the signal that no
 * further work will be done on this node, and downstream lifecycle hooks (notably the
 * {@code onRemove} callback that calls
 * {@link GeoIpDownloaderTaskExecutor#deleteGeoIpDatabasesIndex}) rely on it: if it fires while
 * {@code runDownloader()} is still bulk-indexing into {@code .geoip_databases}, the concurrent
 * DELETE races with auto-create on the next bulk and resurrects the index.
 */
public abstract class AbstractGeoIpDownloader extends AllocatedPersistentTask {

    private enum RunState {
        IDLE,
        REQUESTED,
        RUNNING,
        RUNNING_AND_REQUESTED,
        DRAINING,
        COMPLETED
    }

    private final Logger logger;
    private final ThreadPool threadPool;
    private final Supplier<TimeValue> pollIntervalSupplier;

    private final AtomicReference<RunState> runState = new AtomicReference<>(RunState.IDLE);

    /**
     * The currently active periodic schedule, or {@code null} if none. Created by
     * {@link #replaceScheduledPeriodicRun()} via {@code threadPool.scheduleWithFixedDelay}; the
     * scheduler re-invokes {@link #runPeriodic()} after each completion plus the configured
     * delay, until {@link Scheduler.Cancellable#cancel() cancel()} is called on the returned
     * {@link Scheduler.Cancellable}. Only mutated via atomic {@code getAndSet}; never under a
     * monitor.
     */
    private final AtomicReference<Scheduler.Cancellable> scheduledPeriodicRun = new AtomicReference<>();

    public AbstractGeoIpDownloader(
        long id,
        String type,
        String action,
        String description,
        TaskId parentTask,
        Map<String, String> headers,
        ThreadPool threadPool,
        Supplier<TimeValue> pollIntervalSupplier
    ) {
        super(id, type, action, description, parentTask, headers);
        this.logger = LogManager.getLogger(getClass());
        this.threadPool = threadPool;
        this.pollIntervalSupplier = pollIntervalSupplier;
    }

    /**
     * Cancels the currently scheduled periodic run (if any), schedules a fresh one using the
     * current poll interval, and requests an on-demand run now. The on-demand request matters
     * when the persistent task has just been (re)assigned to this node: we want to download
     * immediately rather than wait for the first periodic tick.
     */
    public void restartPeriodicRun() {
        if (isCancelled() || isCompleted() || threadPool.scheduler().isShutdown()) {
            logger.debug("Not restarting periodic run because task is cancelled, completed, or shutting down");
            return;
        }
        logger.debug("Restarting periodic run");
        replaceScheduledPeriodicRun();
        requestRunOnDemand();
    }

    /**
     * Periodic timer body. The enclosing {@link Scheduler.Cancellable} re-arms itself, so this
     * method only needs to request an on-demand run. It is harmless if an on-demand run is already
     * in progress: the request is coalesced by the state machine in {@link #requestRunOnDemand}.
     */
    private void runPeriodic() {
        if (isCancelled() || isCompleted() || threadPool.scheduler().isShutdown()) {
            logger.debug("Not running periodic downloader because task is cancelled, completed, or shutting down");
            return;
        }
        logger.trace("Running periodic downloader");
        requestRunOnDemand();
    }

    /**
     * Starts a new periodic schedule at the current poll interval and cancels whatever schedule
     * was previously active. The post-set cancellation guard handles the race in which a
     * concurrent {@link #onCancelled()} clears the field between our schedule and our
     * {@code getAndSet}: without it, the new schedule would keep firing {@link #runPeriodic()}
     * forever — each tick would observe {@code isCancelled()} and no-op, but the scheduler would
     * keep re-arming because nothing called {@code cancel()} on the new {@code Cancellable}.
     */
    private void replaceScheduledPeriodicRun() {
        Scheduler.Cancellable next = threadPool.scheduleWithFixedDelay(this::runPeriodic, pollIntervalSupplier.get(), threadPool.generic());
        Scheduler.Cancellable previous = scheduledPeriodicRun.getAndSet(next);
        if (previous != null) {
            previous.cancel();
        }
        if (isCancelled() || isCompleted()) {
            // onCancelled raced past us; whichever party owns the live reference at this point
            // (us, via the field; or onCancelled, via its getAndSet) will cancel — but we may be
            // the only one that still has a handle, so cancel here unconditionally. cancel() is
            // idempotent.
            next.cancel();
        }
    }

    /**
     * Requests that the downloader runs against the latest cluster state. Multiple concurrent
     * calls coalesce into at most one owed run in addition to any currently in-flight one.
     */
    public void requestRunOnDemand() {
        if (isCancelled() || isCompleted()) {
            logger.debug("Not requesting downloader to run on demand because task is cancelled or completed");
            return;
        }
        logger.trace("Requesting downloader run on demand");
        while (true) {
            RunState current = runState.get();
            switch (current) {
                case IDLE -> {
                    if (runState.compareAndSet(RunState.IDLE, RunState.REQUESTED)) {
                        try {
                            threadPool.generic().submit(this::runOnDemand);
                        } catch (Throwable t) {
                            // Roll back so a future caller (or the next periodic tick) can
                            // retry. The rollback CAS may fail if cancellation raced ahead
                            // and moved us to COMPLETED — that's fine, the task is
                            // terminating either way.
                            runState.compareAndSet(RunState.REQUESTED, RunState.IDLE);
                            throw t;
                        }
                        return;
                    }
                    // CAS failed; loop and re-read state.
                }
                case RUNNING -> {
                    if (runState.compareAndSet(RunState.RUNNING, RunState.RUNNING_AND_REQUESTED)) {
                        return;
                    }
                    // CAS failed; loop.
                }
                case REQUESTED, RUNNING_AND_REQUESTED -> {
                    return; // a run is already pending; nothing to do
                }
                case DRAINING, COMPLETED -> {
                    return; // task is terminating
                }
            }
        }
    }

    /**
     * Worker entry point. Transitions {@code REQUESTED → RUNNING}, calls {@link #runDownloader()}
     * possibly more than once (to drain coalesced requests on the same worker thread), and
     * transitions out to one of:
     * <ul>
     *   <li>{@code IDLE} — normal exit with no owed run, or rollback after a failed handoff
     *       submit on the exception path;</li>
     *   <li>{@code REQUESTED} — exception path with a successful handoff to a fresh worker;</li>
     *   <li>{@code COMPLETED} — cancellation reached us while running (firing
     *       {@link #markAsCompleted()}).</li>
     * </ul>
     * <p>
     * If {@code runDownloader()} throws (only {@link Error}s and unchecked exceptions are
     * possible — the abstract method has no checked-exception declaration), the state machine
     * is settled via {@link #transitionAfterRun(boolean)} before the throw propagates out, so
     * the drain contract holds. Any owed run is handed off to a fresh worker on a best-effort
     * basis: if the handoff {@code submit} is rejected, the state rolls back to {@code IDLE}
     * and the owed run is recovered by the next periodic tick or external
     * {@link #requestRunOnDemand()} call.
     */
    private void runOnDemand() {
        if (runState.compareAndSet(RunState.REQUESTED, RunState.RUNNING) == false) {
            // Cancellation must have raced REQUESTED → COMPLETED before we picked up the runnable.
            assert runState.get() == RunState.COMPLETED : "unexpected state at worker entry: " + runState.get();
            logger.debug("Worker bailing out: task already completed");
            return;
        }
        while (true) {
            try {
                logger.trace("Running downloader on demand");
                runDownloader();
                logger.trace("Downloader completed successfully");
            } catch (Throwable t) {
                try {
                    transitionAfterRun(false);
                } catch (Throwable settleErr) {
                    // Settle propagated (e.g. an Error from the handoff submit). Preserve the
                    // original runDownloader throwable as the primary cause and attach the
                    // settle throwable as suppressed, mirroring try-with-resources semantics.
                    t.addSuppressed(settleErr);
                }
                throw t;
            }
            if (transitionAfterRun(true) == false) {
                return;
            }
            // Looped: state transitioned RUNNING_AND_REQUESTED → RUNNING; drain the owed run
            // on this worker thread.
        }
    }

    /**
     * Settles the state machine after a {@code runDownloader()} call. When the post-run state is
     * {@link RunState#DRAINING} (cancellation arrived while we were running),
     * {@link #markAsCompleted()} is invoked from here, immediately after the
     * {@code DRAINING → COMPLETED} CAS.
     * <p>
     * Any {@link Exception} from the handoff submit (typically
     * {@link java.util.concurrent.RejectedExecutionException} on shutdown) is treated as
     * recoverable: it is logged and the state is rolled back to {@link RunState#IDLE}.
     * Unrecoverable {@link Error}s are rolled back as well but rethrown so the worker (and the
     * executor) can decide how to react.
     *
     * @param canLoopInPlace {@code true} iff {@code runDownloader()} returned normally and the
     *                       worker is therefore allowed to loop on this thread to drain an owed
     *                       run; {@code false} when the worker is about to propagate an
     *                       exception and the owed run must be handed off to a fresh worker.
     * @return {@code true} to loop and run {@code runDownloader()} again on this worker;
     *         {@code false} to exit the worker.
     */
    private boolean transitionAfterRun(boolean canLoopInPlace) {
        while (true) {
            RunState current = runState.get();
            switch (current) {
                case RUNNING -> {
                    if (runState.compareAndSet(RunState.RUNNING, RunState.IDLE)) {
                        return false;
                    }
                }
                case RUNNING_AND_REQUESTED -> {
                    if (canLoopInPlace) {
                        // Loop in-place on this worker thread.
                        if (runState.compareAndSet(RunState.RUNNING_AND_REQUESTED, RunState.RUNNING)) {
                            return true;
                        }
                    } else {
                        // We're about to throw and won't loop. Hand the owed run off to a
                        // fresh worker via REQUESTED, mirroring the pre-refactor behavior.
                        if (runState.compareAndSet(RunState.RUNNING_AND_REQUESTED, RunState.REQUESTED)) {
                            try {
                                threadPool.generic().submit(this::runOnDemand);
                            } catch (Exception e) {
                                // Recoverable (e.g. RejectedExecutionException on shutdown).
                                // Roll the state machine back to IDLE so periodic ticks (or
                                // the next external requestRunOnDemand) can recover. The
                                // original runDownloader exception is in flight — let it
                                // propagate.
                                logger.warn("Failed to hand off owed downloader run after exception; will recover on next tick", e);
                                runState.compareAndSet(RunState.REQUESTED, RunState.IDLE);
                            } catch (Throwable error) {
                                // Unrecoverable (e.g. OutOfMemoryError). Roll the state
                                // machine back to IDLE and propagate; the worker's catch
                                // attaches it as suppressed on the in-flight runDownloader
                                // throwable so neither is lost.
                                logger.error("Unrecoverable error handing off owed downloader run after exception", error);
                                runState.compareAndSet(RunState.REQUESTED, RunState.IDLE);
                                throw error;
                            }
                            return false;
                        }
                    }
                }
                case DRAINING -> {
                    boolean swapped = runState.compareAndSet(RunState.DRAINING, RunState.COMPLETED);
                    assert swapped : "DRAINING → COMPLETED CAS must succeed: only the in-flight worker can transition out of DRAINING";
                    if (swapped) {
                        logger.debug("In-flight downloader run drained after cancellation, completing task");
                        markAsCompleted();
                        return false;
                    }
                }
                default -> {
                    logger.error("Unexpected state during worker exit: [{}] — exiting worker without further action", current);
                    assert false : "unexpected state during worker exit: " + current;
                    return false;
                }
            }
            // CAS failed; another thread changed state between our get and CAS. Loop.
        }
    }

    /**
     * Returns {@code true} while the downloader is expected to keep working inside
     * {@link #runDownloader()} — i.e., the run-state is {@link RunState#RUNNING} or
     * {@link RunState#RUNNING_AND_REQUESTED}. Returns {@code false} as soon as cancellation has
     * reached the state machine ({@link RunState#DRAINING}) or the task has otherwise stopped.
     * <p>
     * Subclasses should poll this between long-running sub-operations (e.g. between individual
     * database downloads) to bail out promptly on cancellation rather than continuing to do
     * work that will be discarded.
     */
    protected final boolean shouldKeepRunning() {
        RunState current = runState.get();
        return current == RunState.RUNNING || current == RunState.RUNNING_AND_REQUESTED;
    }

    /**
     * Download, update, and clean up GeoIP databases as required by the GeoIP processors in the cluster.
     * <p>
     * Guaranteed to not be called concurrently with itself, and guaranteed to have returned before
     * {@link #markAsCompleted()} is invoked (the "drain contract"). Implementations may therefore
     * assume exclusive access to any state mutated only by {@code runDownloader()} for the
     * duration of the call.
     */
    abstract void runDownloader();

    @Override
    protected void onCancelled() {
        Scheduler.Cancellable scheduled = scheduledPeriodicRun.getAndSet(null);
        if (scheduled != null) {
            scheduled.cancel();
        }
        while (true) {
            RunState current = runState.get();
            switch (current) {
                case IDLE, REQUESTED -> {
                    if (runState.compareAndSet(current, RunState.COMPLETED)) {
                        markAsCompleted();
                        return;
                    }
                }
                case RUNNING, RUNNING_AND_REQUESTED -> {
                    if (runState.compareAndSet(current, RunState.DRAINING)) {
                        // Worker will fire markAsCompleted after runDownloader returns.
                        return;
                    }
                }
                case DRAINING, COMPLETED -> {
                    return; // already cancelled or completed
                }
            }
        }
    }
}
