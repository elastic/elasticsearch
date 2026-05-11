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
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Base class for operators that fan page-level work across a fixed pool of worker
 * slots, each owning its own per-slot state of type {@code W}. The Driver thread
 * delivers incoming pages via {@link #addInput} and the subclass routes each page
 * (or a sub-page) to a worker slot via the {@link Dispatcher} callback. The base
 * class submits each routed page to {@code executor}, holds a per-slot lock so
 * that {@link #processPage} is only ever invoked single-threaded on a given
 * state, tracks in-flight count for backpressure, and cooperatively yields the
 * Driver via {@link #isBlocked()}. Once {@link #finish()} has been called and
 * all in-flight work has drained, {@link #mergeAndBuildResult} is invoked once
 * on the Driver thread to produce the output iterator.
 *
 * <p>Modeled on {@link AsyncOperator}; reuses the same blocked-future pattern,
 * {@link FailureCollector} for cross-thread error propagation, and
 * {@link DriverContext#addAsyncAction} bookkeeping. Unlike AsyncOperator, this
 * class assumes mutable per-worker state (not 1-page-in-1-result-out) and
 * defers all output to a single merge phase after {@code finish()}.
 *
 * <p>Threading: {@link #createWorkerState}, {@link #dispatch},
 * {@link #mergeAndBuildResult}, and all {@link Operator} lifecycle methods run
 * on the Driver thread. {@link #processPage} runs on a worker thread under the
 * slot's lock; the state passed to it is therefore touched single-threaded.
 */
public abstract class ParallelOperator<W extends Releasable> implements Operator {

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
    protected ParallelOperator(DriverContext driverContext, Executor executor, int workerCount, int maxInFlightPages) {
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

    @Override
    public final boolean needsInput() {
        return finished == false && output == null && inFlight.get() < maxInFlightPages && failureCollector.hasFailure() == false;
    }

    @Override
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
        boolean submitted = false;
        try {
            executor.execute(new AbstractRunnable() {
                @Override
                protected void doRun() {
                    if (closed || failureCollector.hasFailure()) {
                        return;
                    }
                    slot.lock.lock();
                    try {
                        W state = slot.state;
                        if (state == null) {
                            return;   // close() raced ahead
                        }
                        processPage(state, page);
                    } finally {
                        slot.lock.unlock();
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    failureCollector.unwrapAndCollect(e);
                }

                @Override
                public void onAfter() {
                    page.releaseBlocks();
                    inFlight.decrementAndGet();
                    driverContext.removeAsyncAction();
                    notifyIfBlocked();
                }

                @Override
                public void onRejection(Exception e) {
                    // onAfter does NOT fire on rejection; unwind manually.
                    failureCollector.unwrapAndCollect(e);
                    page.releaseBlocks();
                    inFlight.decrementAndGet();
                    driverContext.removeAsyncAction();
                    notifyIfBlocked();
                }
            });
            submitted = true;
        } finally {
            if (submitted == false) {
                // executor.execute itself threw rather than invoking onRejection.
                page.releaseBlocks();
                inFlight.decrementAndGet();
                driverContext.removeAsyncAction();
            }
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

    @Override
    public final void finish() {
        finished = true;
        notifyIfBlocked();
    }

    @Override
    public final boolean isFinished() {
        checkFailure();
        return finished && inFlight.get() == 0 && output != null && output.hasNext() == false;
    }

    @Override
    public final boolean canProduceMoreDataWithoutExtraInput() {
        return (output != null && output.hasNext()) || (finished && inFlight.get() > 0) || (finished && output == null);
    }

    @Override
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

    @Override
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
            return new IsBlockedResult(blockedFuture, "ParallelOperator");
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
            slot.lock.lock();
            try {
                Releasables.closeExpectNoException(slot.state);
                slot.state = null;
            } finally {
                slot.lock.unlock();
            }
        }
    }

    private static final class WorkerSlot<W extends Releasable> {
        final ReentrantLock lock = new ReentrantLock();
        W state;

        WorkerSlot(W state) {
            this.state = state;
        }
    }
}
