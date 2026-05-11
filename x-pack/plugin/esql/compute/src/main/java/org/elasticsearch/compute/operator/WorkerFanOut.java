/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Internal helper that fans page-level work across a fixed pool of worker slots,
 * each owning its own per-slot state of type {@code W}. The owning {@link Operator}
 * holds an instance via composition and forwards its lifecycle calls (addInput /
 * finish / getOutput / isBlocked / close) to this helper's same-named methods.
 * Subclasses route incoming pages to worker slots via the {@link Dispatcher} callback.
 * Each slot owns a small inbox queue and an at-most-one-task-running invariant: when
 * the Driver enqueues a page, it CAS-claims the slot's running flag and submits a
 * single drain task; the drain task processes the inbox until empty before exiting.
 * This gives "soft affinity" — the same executor thread handles a burst of pages
 * for the same slot, keeping caches warm — and prevents multiple threads from
 * parking on a per-slot lock the way a one-task-per-page model would.
 *
 * <p>This class tracks an in-flight count for backpressure and cooperatively yields
 * via {@link #isBlocked()}. Once {@link #finish()} has been called and all in-flight
 * work has drained, {@link #mergeAndBuildResult} is invoked once on the Driver thread
 * to produce the output iterator.
 *
 * <p>This class is intentionally <em>not</em> an {@link Operator}: it is only ever
 * used as an internal helper. Its method shapes mirror Operator's lifecycle so the
 * owning operator can forward calls 1:1, but it carries no factory and is never
 * handed to a Driver directly.
 *
 * <p>Modeled on {@link AsyncOperator}; reuses the same blocked-future pattern,
 * {@link FailureCollector} for cross-thread error propagation, and
 * {@link DriverContext#addAsyncAction} bookkeeping. Unlike AsyncOperator, this
 * class assumes mutable per-worker state (not 1-page-in-1-result-out) and defers
 * all output to a single merge phase after {@code finish()}.
 *
 * <p>Threading: {@link #createWorkerState}, {@link #dispatch}, {@link #mergeAndBuildResult},
 * and all forwarded lifecycle methods run on the Driver thread. {@link #processPage}
 * runs on a worker thread. The "one drain task per slot" invariant guarantees the slot
 * state is touched single-threaded — the per-slot lock exists only as a
 * close-vs-active-drain coordination primitive and is uncontended on the hot path.
 */
public abstract class WorkerFanOut<W extends Releasable> implements Releasable {

    /**
     * Callback supplied to {@link #dispatch}. Ownership of {@code page} passes
     * to the dispatcher; the caller must not touch it after the call returns.
     */
    public interface Dispatcher {
        void submitTo(int workerIndex, Page page);
    }

    private final DriverContext driverContext;
    private final Executor executor;
    private final FailureCollector failureCollector = new FailureCollector();
    private final int maxInFlightPages;

    private final WorkerSlot<W>[] workers;
    private final Dispatcher dispatcher = this::submitTo;

    private final AtomicInteger inFlight = new AtomicInteger();
    private volatile boolean finished;
    private volatile boolean closed;

    /** Guarded by {@code this}. Mirrors {@link AsyncOperator}'s pattern. */
    private SubscribableListener<Void> blockedFuture;

    /** Built lazily on the Driver thread once {@code finished && inFlight == 0}. */
    private ReleasableIterator<Page> output;

    @SuppressWarnings({ "unchecked", "this-escape" })
    protected WorkerFanOut(DriverContext driverContext, Executor executor, int workerCount, int maxInFlightPages) {
        if (workerCount < 1) {
            throw new IllegalArgumentException("workerCount must be >= 1, got " + workerCount);
        }
        if (maxInFlightPages < 1) {
            throw new IllegalArgumentException("maxInFlightPages must be >= 1, got " + maxInFlightPages);
        }
        this.driverContext = driverContext;
        this.executor = executor;
        this.maxInFlightPages = maxInFlightPages;
        WorkerSlot<W>[] slots = (WorkerSlot<W>[]) new WorkerSlot<?>[workerCount];
        boolean success = false;
        try {
            for (int i = 0; i < workerCount; i++) {
                slots[i] = new WorkerSlot<>(createWorkerState(i));
            }
            success = true;
        } finally {
            if (success == false) {
                for (WorkerSlot<W> s : slots) {
                    if (s != null) {
                        Releasables.closeExpectNoException(s.state);
                    }
                }
            }
        }
        this.workers = slots;
    }

    /**
     * Replace the state at {@code workerIndex} with {@code state}, releasing the
     * one allocated by {@link #createWorkerState}. Intended for promotion from a
     * sequential operator: the caller hands its already-accumulated state to
     * worker 0 so no rows are lost in transition. Must be called before any
     * {@link #addInput} call.
     */
    public final void seedWorkerState(int workerIndex, W state) {
        WorkerSlot<W> slot = workers[workerIndex];
        slot.lock.lock();
        try {
            Releasables.closeExpectNoException(slot.state);
            slot.state = state;
        } finally {
            slot.lock.unlock();
        }
    }

    /** Allocate the per-slot state. Called once per slot at construction, on the Driver thread. */
    protected abstract W createWorkerState(int workerIndex);

    /**
     * Route an incoming page to one or more workers via {@code dispatcher}. Runs
     * on the Driver thread. Ownership of {@code page} passes to the implementor:
     * it must either submit it (handing ownership to a worker) or release it.
     * For row-level splitting, build sub-pages, submit each, then release the
     * original.
     */
    protected abstract void dispatch(Page page, Dispatcher dispatcher);

    /** Per-page work, executed on a worker thread under the slot's lock. */
    protected abstract void processPage(W state, Page page);

    /**
     * Build the final result iterator from the per-worker states. Called once on
     * the Driver thread after {@code finish()} and all in-flight work drained.
     * Implementations take ownership of every state in the list and are
     * responsible for releasing them (typically by closing each after extracting
     * its contents into the returned iterator's backing storage).
     */
    protected abstract ReleasableIterator<Page> mergeAndBuildResult(List<W> states);

    public final boolean needsInput() {
        return finished == false && output == null && inFlight.get() < maxInFlightPages && failureCollector.hasFailure() == false;
    }

    public final void addInput(Page page) {
        if (closed || failureCollector.hasFailure()) {
            page.releaseBlocks();
            return;
        }
        try {
            dispatch(page, dispatcher);
        } catch (Exception e) {
            failureCollector.unwrapAndCollect(e);
            page.releaseBlocks();
        }
    }

    private void submitTo(int workerIndex, Page page) {
        if (closed || failureCollector.hasFailure()) {
            page.releaseBlocks();
            return;
        }
        WorkerSlot<W> slot = workers[workerIndex];
        inFlight.incrementAndGet();
        driverContext.addAsyncAction();
        page.allowPassingToDifferentDriver();
        slot.inbox.add(page);
        if (slot.running.compareAndSet(false, true)) {
            scheduleDrain(slot);
        }
        // else: an existing drain task will pick this page up.
    }

    /**
     * Submit a drain task for {@code slot} to the executor. The caller is the
     * exclusive owner of {@code slot.running == true} (won the CAS). On rejection,
     * we have to surface the failure and clear the inbox + flag ourselves since no
     * drain task will run.
     */
    private void scheduleDrain(WorkerSlot<W> slot) {
        try {
            executor.execute(new AbstractRunnable() {
                @Override
                protected void doRun() {
                    drainSlot(slot);
                }

                @Override
                public void onFailure(Exception e) {
                    // doRun internally guards processPage with try/catch, so reaching here means
                    // something outside the per-page work failed (lock acquisition, executor
                    // wrapping, etc.). Clear running so a future producer can re-claim.
                    failureCollector.unwrapAndCollect(e);
                    slot.running.set(false);
                    notifyIfBlocked();
                }

                @Override
                public void onRejection(Exception e) {
                    handleRejection(slot, e);
                }
            });
        } catch (RejectedExecutionException e) {
            // Synchronous rejection — onRejection wasn't invoked.
            handleRejection(slot, e);
        }
    }

    /**
     * Drain {@code slot.inbox} on a worker thread. Holds {@code slot.lock} for the
     * duration of the drain so {@link #close()} can wait for active processing to
     * finish before tearing down state. On exit, clears {@code slot.running} and
     * checks for the producer-added-page race; if a page snuck into the inbox
     * between the loop ending and the flag clearing, re-claim and resubmit.
     */
    private void drainSlot(WorkerSlot<W> slot) {
        slot.lock.lock();
        try {
            Page page;
            while ((page = slot.inbox.poll()) != null) {
                try {
                    W state = slot.state;
                    if (closed == false && failureCollector.hasFailure() == false && state != null) {
                        processPage(state, page);
                    }
                } catch (Exception e) {
                    failureCollector.unwrapAndCollect(e);
                } finally {
                    page.releaseBlocks();
                    inFlight.decrementAndGet();
                    driverContext.removeAsyncAction();
                    notifyIfBlocked();
                }
            }
        } finally {
            slot.lock.unlock();
        }
        // Exit protocol: clear running, then re-check for the producer-add race.
        slot.running.set(false);
        if (slot.inbox.peek() != null && slot.running.compareAndSet(false, true)) {
            scheduleDrain(slot);
        }
    }

    /**
     * Synchronously drain {@code slot.inbox} when the executor refused to run our
     * drain task. We're the claim holder ({@code slot.running == true}); release
     * every page we promised to process, surface the failure, and clear the flag.
     */
    private void handleRejection(WorkerSlot<W> slot, Exception cause) {
        failureCollector.unwrapAndCollect(cause);
        try {
            Page page;
            while ((page = slot.inbox.poll()) != null) {
                page.releaseBlocks();
                inFlight.decrementAndGet();
                driverContext.removeAsyncAction();
            }
        } finally {
            slot.running.set(false);
            notifyIfBlocked();
        }
    }

    private void notifyIfBlocked() {
        if (blockedFuture != null) {
            SubscribableListener<Void> f;
            synchronized (this) {
                f = blockedFuture;
                blockedFuture = null;
            }
            if (f != null) {
                f.onResponse(null);
            }
        }
    }

    public final void finish() {
        finished = true;
        notifyIfBlocked();
    }

    public final boolean isFinished() {
        checkFailure();
        return finished && inFlight.get() == 0 && output != null && output.hasNext() == false;
    }

    public final boolean canProduceMoreDataWithoutExtraInput() {
        return (output != null && output.hasNext()) || (finished && inFlight.get() > 0) || (finished && output == null);
    }

    public final Page getOutput() {
        checkFailure();
        if (output == null) {
            if (finished == false || inFlight.get() > 0) {
                return null;
            }
            output = buildResultFromWorkers();
        }
        return output.hasNext() ? output.next() : null;
    }

    private ReleasableIterator<Page> buildResultFromWorkers() {
        List<W> states = new ArrayList<>(workers.length);
        for (WorkerSlot<W> slot : workers) {
            slot.lock.lock();
            try {
                W s = slot.state;
                slot.state = null;   // ownership transferred to mergeAndBuildResult
                states.add(s);
            } finally {
                slot.lock.unlock();
            }
        }
        return mergeAndBuildResult(states);
    }

    public final IsBlockedResult isBlocked() {
        if (notBlocked()) {
            return Operator.NOT_BLOCKED;
        }
        synchronized (this) {
            if (notBlocked()) {
                return Operator.NOT_BLOCKED;
            }
            if (blockedFuture == null) {
                blockedFuture = new SubscribableListener<>();
            }
            return new IsBlockedResult(blockedFuture, "WorkerFanOut");
        }
    }

    private boolean notBlocked() {
        if (output != null && output.hasNext()) {
            return true;
        }
        if (finished == false && inFlight.get() < maxInFlightPages) {
            return true;
        }
        if (finished && inFlight.get() == 0) {
            return true;
        }
        return false;
    }

    private void checkFailure() {
        if (failureCollector.hasFailure()) {
            throw ExceptionsHelper.convertToRuntime(failureCollector.getFailure());
        }
    }

    @Override
    public final void close() {
        closed = true;
        finished = true;
        notifyIfBlocked();
        Releasables.closeExpectNoException(output);
        output = null;
        for (WorkerSlot<W> slot : workers) {
            // Acquiring the lock waits for any active drain task to finish before we
            // release state. Drain tasks already running will observe closed==true on
            // each iteration and short-circuit, so the wait is bounded by at most one
            // in-flight page per slot.
            slot.lock.lock();
            try {
                // Release any inbox stragglers — pages enqueued by the Driver but never
                // picked up by a drain task (e.g. close() racing with submitTo, or a
                // drain task exited before observing the latest enqueue).
                Page page;
                while ((page = slot.inbox.poll()) != null) {
                    page.releaseBlocks();
                    inFlight.decrementAndGet();
                    driverContext.removeAsyncAction();
                }
                Releasables.closeExpectNoException(slot.state);
                slot.state = null;
            } finally {
                slot.lock.unlock();
            }
        }
    }

    private static final class WorkerSlot<W extends Releasable> {
        /** Held by the active drain task and by {@link #close()}; uncontended on the hot path. */
        final ReentrantLock lock = new ReentrantLock();
        /** Pages awaiting this slot. Drain task polls; submitters append. */
        final ConcurrentLinkedDeque<Page> inbox = new ConcurrentLinkedDeque<>();
        /** True iff a drain task is currently scheduled or running for this slot. */
        final AtomicBoolean running = new AtomicBoolean(false);
        W state;

        WorkerSlot(W state) {
            this.state = state;
        }
    }
}
