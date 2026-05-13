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
import java.util.function.Function;

/**
 * Fans page-level work across a fixed pool of worker slots, each owning per-slot state of type {@code W}.
 * Composed into an owning {@link Operator} which forwards lifecycle calls; not itself an Operator.
 *
 * <p>Per slot: one inbox + at-most-one drain task. Producers CAS-claim the running flag and the winner
 * submits a single drain task that processes the inbox to empty.
 *
 * <p>Threading: {@link #chooseWorker}, {@link #mergeAndBuildResult}, and lifecycle methods run on the
 * Driver thread; {@link #processPage} runs on a worker thread. The per-slot lock exists only so
 * {@link #close()} can wait for an active drain.
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

    // volatile so workers' unsynchronized fast-path read in notifyIfBlocked observes the Driver's write
    // (same pattern as AsyncOperator).
    private volatile SubscribableListener<Void> blockedFuture;

    private ReleasableIterator<Page> output;

    // this-escape: createWorkerState runs before construction completes; overrides must read only
    // fully-initialized fields.
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

    /** Allocate per-slot state; subclasses promoting from a sequential operator may return pre-existing state. */
    protected abstract W createWorkerState(int workerIndex);

    /** Route {@code page} to a worker slot; implementors only inspect, lifecycle stays with the base class. */
    protected abstract int chooseWorker(Page page);

    /** Per-page work, executed on a worker thread. */
    protected abstract void processPage(W state, Page page, int slotIndex);

    /** Build the final result; takes ownership of and must release every state in the list. */
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
            // a thread is not yet running for the worker, so start one
            scheduleDrain(slot);
        }
    }

    // Caller owns slot.running == true; on rejection we must drain and clear it ourselves.
    private void scheduleDrain(WorkerSlot<W> slot) {
        try {
            executor.execute(new AbstractRunnable() {
                @Override
                protected void doRun() {
                    drainSlot(slot);
                }

                @Override
                public void onFailure(Exception e) {
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

    // Holds slot.lock so close() can wait for active processing.
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
                    releasePage(page);
                    notifyIfBlocked();
                }
            }
            if (slot.state != null) {
                slot.ramBytesSnapshot = slot.state.ramBytesUsed();
            }
        } finally {
            slot.lock.unlock();
        }

        // no longer running
        slot.running.set(false);

        // Driver thread may have added a page to inbox before worker thread stopped running.
        // Check if there is more work and start a new thread if needed.
        if (slot.inbox.peek() != null && slot.running.compareAndSet(false, true)) {
            scheduleDrain(slot);
        }
    }

    private void handleRejection(WorkerSlot<W> slot, Exception cause) {
        failureCollector.unwrapAndCollect(cause);
        try {
            Page page;
            while ((page = slot.inbox.poll()) != null) {
                releasePage(page);
            }
        } finally {
            slot.running.set(false);
            notifyIfBlocked();
        }
    }

    private void notifyIfBlocked() {
        if (blockedFuture != null) {
            final SubscribableListener<Void> future;
            synchronized (this) {
                future = blockedFuture;
                this.blockedFuture = null;
            }
            if (future != null) {
                future.onResponse(null);
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

    // Non-blocking; each drain refreshes its slot's snapshot, so the value is at most one drain stale.
    @Override
    public final long ramBytesUsed() {
        long total = 0;
        for (WorkerSlot<W> slot : workers) {
            total += slot.ramBytesSnapshot;
        }
        return total;
    }

    /**
     * Apply {@code mapper} to each non-null worker state in slot order, lock-free. The mapper races
     * with both worker threads and Driver-thread cleanup, so it must read only volatile primitives
     * or final thread-safe references on the state — anything else can NPE or see torn data.
     * Released slots are skipped.
     */
    public final <T> List<T> mapStates(Function<W, T> mapper) {
        List<T> results = new ArrayList<>(workers.length);
        for (WorkerSlot<W> slot : workers) {
            W state = slot.state;
            if (state != null) {
                results.add(mapper.apply(state));
            }
        }
        return results;
    }

    private ReleasableIterator<Page> buildResultFromWorkers() {
        List<W> states = new ArrayList<>(workers.length);
        for (WorkerSlot<W> slot : workers) {
            slot.lock.lock();
            try {
                W s = slot.state;
                slot.state = null;
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
        Exception e = failureCollector.getFailure();
        if (e != null) {
            throw ExceptionsHelper.convertToRuntime(e);
        }
    }

    @Override
    public final void close() {
        finish();
        closed = true;
        Releasables.closeExpectNoException(output);
        output = null;
        for (WorkerSlot<W> slot : workers) {
            // Wait for any active drain; drains observe closed and short-circuit.
            slot.lock.lock();
            try {
                Page page;
                while ((page = slot.inbox.poll()) != null) {
                    releasePage(page);
                }
                Releasables.closeExpectNoException(slot.state);
                slot.state = null;
                slot.ramBytesSnapshot = 0;
            } finally {
                slot.lock.unlock();
            }
        }
    }

    private void releasePage(Page page) {
        page.releaseBlocks();
        inFlight.decrementAndGet();
        driverContext.removeAsyncAction();
    }

    private static final class WorkerSlot<W extends Releasable & Accountable> {
        final ReentrantLock lock = new ReentrantLock();
        final ConcurrentLinkedDeque<Page> inbox = new ConcurrentLinkedDeque<>();
        final AtomicBoolean running = new AtomicBoolean(false);
        final int index;
        // Volatile so mapStates can read without acquiring the slot lock.
        volatile W state;
        // Refreshed by each drain under lock; read lock-free by ramBytesUsed.
        volatile long ramBytesSnapshot;

        WorkerSlot(int index, W state) {
            this.index = index;
            this.state = state;
        }
    }
}
