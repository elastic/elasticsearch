/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.Accountable;
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
 * each owning its own per-slot state of type {@code W}. Used via composition by
 * an owning {@link Operator}, which forwards lifecycle calls (addInput / finish /
 * getOutput / isBlocked / close) to this helper's same-named methods. Not itself
 * an Operator — no factory, never handed to a Driver directly.
 *
 * <p>Per slot: one inbox queue + at-most-one drain task running. Producers enqueue
 * and CAS-claim the slot's running flag; the winner submits a single drain task
 * that processes the inbox to empty before exiting. This gives soft thread affinity
 * (a burst of pages for one slot tends to run on the same executor thread) and
 * avoids the lock-contention of a one-task-per-page model.
 *
 * <p>Threading: {@link #createWorkerState}, {@link #chooseWorker},
 * {@link #mergeAndBuildResult}, and all forwarded lifecycle methods run on the
 * Driver thread. {@link #processPage} runs on a worker thread; the
 * one-drain-task-per-slot invariant keeps that state single-threaded. The per-slot
 * lock is uncontended on the hot path — it exists only to let {@link #close()}
 * wait for an active drain before tearing down state.
 */
public abstract class WorkerFanOut<W extends Releasable & Accountable> implements Releasable, Accountable {

    private final DriverContext driverContext;
    private final Executor executor;
    private final FailureCollector failureCollector = new FailureCollector();
    private final int maxInFlightPages;

    private final WorkerSlot<W>[] workers;

    private final AtomicInteger inFlight = new AtomicInteger();
    private volatile boolean finished;
    private volatile boolean closed;

    /** Guarded by {@code this}. */
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
                slots[i] = new WorkerSlot<>(i, createWorkerState(i));
                if (slots[i].state != null) {
                    slots[i].ramBytesSnapshot = slots[i].state.ramBytesUsed();
                }
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
     * Allocate the per-slot state. Called once per slot at construction. Subclasses
     * promoting from a sequential operator can return their pre-existing state for
     * a chosen index (typically 0) here.
     */
    protected abstract W createWorkerState(int workerIndex);

    /**
     * Choose which worker slot {@code page} should be routed to. The base class
     * owns the page lifecycle; implementors only inspect.
     */
    protected abstract int chooseWorker(Page page);

    /**
     * Per-page work, executed on a worker thread. {@code slotIndex} identifies
     * which worker slot is processing this page.
     */
    protected abstract void processPage(W state, Page page, int slotIndex);

    /**
     * Build the final result iterator from the per-worker states. Called once
     * after {@code finish()} and all in-flight work has drained. Implementations
     * take ownership of every state in the list and are responsible for releasing them.
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
        final int workerIndex;
        try {
            workerIndex = chooseWorker(page);
        } catch (Exception e) {
            failureCollector.unwrapAndCollect(e);
            page.releaseBlocks();
            return;
        }
        assert workerIndex >= 0 && workerIndex < workers.length
            : "chooseWorker returned " + workerIndex + " but worker count is " + workers.length;
        submitTo(workerIndex, page);
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
     * Submit a drain task. Caller owns {@code slot.running == true}; on rejection
     * we must drain the inbox and clear the flag ourselves.
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
                    // doRun guards processPage with try/catch, so reaching here means something
                    // outside the per-page work failed. Clear running so a producer can re-claim.
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
            handleRejection(slot, e);
        }
    }

    /**
     * Drain {@code slot.inbox} on a worker thread. Holds {@code slot.lock} so
     * {@link #close()} can wait for active processing. On exit, clears running
     * and resubmits if a producer added a page during the gap.
     */
    private void drainSlot(WorkerSlot<W> slot) {
        slot.lock.lock();
        try {
            Page page;
            while ((page = slot.inbox.poll()) != null) {
                try {
                    W state = slot.state;
                    if (closed == false && failureCollector.hasFailure() == false && state != null) {
                        processPage(state, page, slot.index);
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
            // Refresh RAM snapshot while we still hold the lock.
            if (slot.state != null) {
                slot.ramBytesSnapshot = slot.state.ramBytesUsed();
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
     * Synchronously drain when the executor refused our drain task. We hold the
     * running claim, so we must release the inbox and clear the flag ourselves.
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

    /**
     * Sum of per-slot state sizes from the most recent drain. Non-blocking:
     * each drain refreshes its slot's snapshot before releasing the lock, so the
     * value is at most one-drain stale and remains live under sustained pressure.
     */
    @Override
    public final long ramBytesUsed() {
        long total = 0;
        for (WorkerSlot<W> slot : workers) {
            total += slot.ramBytesSnapshot;
        }
        return total;
    }

    private ReleasableIterator<Page> buildResultFromWorkers() {
        List<W> states = new ArrayList<>(workers.length);
        for (WorkerSlot<W> slot : workers) {
            slot.lock.lock();
            try {
                W s = slot.state;
                slot.state = null;   // ownership transferred to mergeAndBuildResult
                slot.ramBytesSnapshot = 0;
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
            // Lock acquisition waits for an active drain; drains observe closed and short-circuit.
            slot.lock.lock();
            try {
                // Release inbox stragglers (pages enqueued but not yet picked up by a drainer).
                Page page;
                while ((page = slot.inbox.poll()) != null) {
                    page.releaseBlocks();
                    inFlight.decrementAndGet();
                    driverContext.removeAsyncAction();
                }
                Releasables.closeExpectNoException(slot.state);
                slot.state = null;
                slot.ramBytesSnapshot = 0;
            } finally {
                slot.lock.unlock();
            }
        }
    }

    private static final class WorkerSlot<W extends Releasable & Accountable> {
        /** Coordinates active drain task with {@link #close()}. */
        final ReentrantLock lock = new ReentrantLock();
        final ConcurrentLinkedDeque<Page> inbox = new ConcurrentLinkedDeque<>();
        /** True iff a drain task is scheduled or running. */
        final AtomicBoolean running = new AtomicBoolean(false);
        final int index;
        W state;
        /**
         * Snapshot of {@code state}'s size, refreshed by each drain before releasing
         * {@link #lock}. Read by {@link #ramBytesUsed()} without a lock.
         */
        volatile long ramBytesSnapshot;

        WorkerSlot(int index, W state) {
            this.index = index;
            this.state = state;
        }
    }
}
