/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.FailureCollector;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.exchange.ExchangeBuffer;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A parallel variant of {@link TopNOperator} that dispatches incoming pages to multiple
 * worker {@link TopNOperator}s running on {@code esql_worker} threads. The driver thread
 * is never blocked waiting for workers; it uses {@link ExchangeBuffer} as a bounded FIFO
 * between the driver and the workers, relying on {@link #isBlocked()} to back-pressure the
 * upstream operator when the buffer is full.
 *
 * <h2>Lifecycle</h2>
 * <ol>
 *   <li><b>Collecting</b> – pages arrive via {@link #addInput}; they are enqueued in
 *       {@code in} and background workers drain that buffer concurrently.</li>
 *   <li><b>Finishing</b> – {@link #finish()} marks the buffer as complete; workers drain
 *       the remaining pages and build their local top-K queues.</li>
 *   <li><b>Merging</b> – once all workers have signalled completion via
 *       {@link #allWorkersDone}, the driver thread pops each worker's priority-queue
 *       directly into {@code mergeTarget}'s queue (no page encode/decode), calls
 *       {@link TopNOperator#finish()} on it, and delegates subsequent {@link #getOutput()}
 *       calls to it.</li>
 * </ol>
 *
 * <h2>Memory accounting and LC ownership transfer</h2>
 * {@code mergeTarget} charges allocations directly to the driver's circuit breaker. Each
 * background worker has its own {@link org.elasticsearch.compute.data.LocalCircuitBreaker}
 * (LC) via {@link DriverContext#createChildBlockFactory()}.
 * The LC backs every allocation the worker makes: the {@link TopNQueue} struct, the spare
 * {@link TopNRow}, and the bytes inside each row (each {@link TopNRow} holds a direct
 * reference to its creator's breaker).
 *
 * <p><b>Aborting workers</b> clean up on the worker thread: {@link TopNOperator#close()}
 * closes all rows and the queue struct, returning their bytes to the LC;
 * {@link DriverContext#releaseChildBlockFactory} then closes the LC and returns its reserved bytes
 * to the driver's breaker.
 *
 * <p><b>Non-aborting workers</b> transfer ownership of their operator — including their LC —
 * to the driver thread by signalling done without calling either method. The driver then:
 * <ol>
 *   <li>Pops the worker's queue into {@code mergeTarget}'s queue in {@link #getOutput()}.
 *       {@link org.apache.lucene.util.PriorityQueue#pop()} nulls each heap slot, so the
 *       worker queue is empty after the drain; transferred rows still reference their worker
 *       LC. {@code allWorkersDone} guarantees the driver sees each LC's fully-updated state.</li>
 *   <li>Calls {@link TopNOperator#close()} on the worker. The queue is empty so no rows are
 *       closed; only the queue's struct bytes are returned to the LC.</li>
 *   <li>Calls {@link DriverContext#releaseChildBlockFactory} for all workers in {@link #close()},
 *       <em>after</em> {@code mergeTarget.close()} — which closes any surviving rows, each
 *       returning its bytes to its worker LC — so every LC is fully drained before being
 *       released to the driver's breaker.</li>
 * </ol>
 *
 * <p>Input pages have {@link Page#allowPassingToDifferentDriver()} called on the driver
 * thread in {@link #addInput} before being enqueued.
 */
public class ParallelTopNOperator implements Operator, Accountable {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(ParallelTopNOperator.class);

    /** Bounded FIFO queue from driver thread to background workers. */
    private final ExchangeBuffer in;

    /**
     * The initial {@link TopNOperator} that was promoted and already holds rows accumulated
     * before promotion. Never dispatched to a background thread; used as the sole merge
     * target in {@link #getOutput()} and closed on the driver thread in {@link #close()}.
     */
    private final TopNOperator mergeTarget;

    /** Background worker operators, one per {@code esql_worker} thread. */
    private final List<Worker> workers;

    private final DriverContext driverContext;
    private final Executor executor;
    private final FailureCollector failureCollector = new FailureCollector();

    private final PendingTasks pendingTasks;
    /** Completes when all background workers have finished (or there were none). */
    private final SubscribableListener<Void> allWorkersDone = new SubscribableListener<>();

    /** True once {@link #finish()} has been called. */
    private boolean finishCalled = false;

    /** True once {@link #close()} has been called; signals workers to stop. */
    private volatile boolean closed = false;

    /**
     * True once the merge phase (popping worker queues into {@link #mergeTarget}) has
     * completed and {@link TopNOperator#finish()} has been called on {@link #mergeTarget}.
     */
    private boolean mergeDone = false;

    /**
     * @param config          parallel worker configuration (executor, worker count, etc.)
     * @param driverContext   driver context for block-factory and async-action tracking
     * @param initialWorker   the promoted {@link TopNOperator} that holds pre-promotion rows;
     *                        becomes {@link #mergeTarget} and is never sent to a background thread
     */
    public ParallelTopNOperator(TopNOperator.ParallelWorkerConfig config, DriverContext driverContext, TopNOperator initialWorker) {
        this.in = new ExchangeBuffer(config.maxInFlightPages());
        this.driverContext = driverContext;
        this.executor = config.executor();
        this.mergeTarget = initialWorker;

        int backgroundWorkerCount = config.workerCount();
        if (backgroundWorkerCount < 1) {
            throw new IllegalArgumentException(
                "ParallelTopNOperator requires at least one background worker, got " + backgroundWorkerCount
            );
        }
        this.workers = new ArrayList<>(backgroundWorkerCount);
        boolean success = false;
        try {
            for (int i = 0; i < backgroundWorkerCount; i++) {
                BlockFactory childFactory = driverContext.createChildBlockFactory();
                try {
                    TopNOperator workerOp = initialWorker.spawnWorker(childFactory);
                    workers.add(new Worker(childFactory, workerOp));
                    childFactory = null;
                } finally {
                    if (childFactory != null) {
                        driverContext.releaseChildBlockFactory(childFactory);
                    }
                }
            }
            success = true;
        } finally {
            if (success == false) {
                releaseWorkers();
            }
        }
        this.pendingTasks = new PendingTasks(() -> {
            allWorkersDone.onResponse(null);
            if (closed) {
                releaseWorkers();
            }
            driverContext.removeAsyncAction();
        });
        driverContext.addAsyncAction();
        for (var worker : workers) {
            scheduleWorker(worker.topN);
        }
    }

    private void scheduleWorker(TopNOperator worker) {
        executor.execute(new AbstractRunnable() {
            @Override
            protected void doRun() {
                // Incremented here, not before execute(), so queued tasks don't block completion.
                // Safe because `finish` draining the buffer with `processPagesWithCurrentThread` before calling `pendingTasks#finishTask`
                pendingTasks.newTask();
                try {
                    Page page;
                    while ((page = in.pollPage()) != null) {
                        worker.addInput(page);
                    }
                    // Buffer temporarily empty; reschedule when more pages arrive or buffer finishes.
                    if (in.isFinished() == false) {
                        in.waitForReading().listener().addListener(ActionListener.running(() -> scheduleWorker(worker)));
                    }
                } finally {
                    pendingTasks.finishTask();
                }
            }

            @Override
            public void onFailure(Exception e) {
                failureCollector.unwrapAndCollect(e);
                in.finish(true);
            }

            @Override
            public void onRejection(Exception e) {
                // fine, let the main thread merge this queue later
            }
        });
    }

    @Override
    public boolean needsInput() {
        return mergeTarget.needsInput();
    }

    private void processPagesWithCurrentThread() {
        try {
            Page page;
            while ((page = in.pollPage()) != null) {
                mergeTarget.addInput(page);
            }
        } catch (Exception e) {
            in.finish(true);
            throw e;
        }
    }

    private void releaseWorkers() {
        Releasables.close(workers);
    }

    @Override
    public void addInput(Page page) {
        page.allowPassingToDifferentDriver();
        in.addPage(page);
        // if `in` buffer is full, process on Driver thread
        if (in.waitForWriting().listener().isDone() == false) {
            processPagesWithCurrentThread();
        }
    }

    @Override
    public void finish() {
        if (finishCalled == false) {
            finishCalled = true;
            in.finish(false);
            try {
                processPagesWithCurrentThread();
            } finally {
                pendingTasks.finishTask();
            }
        }
    }

    @Override
    public boolean isFinished() {
        return mergeDone && mergeTarget.isFinished();
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        if (finishCalled == false) {
            // Workers consume input but produce nothing until finish() signals end-of-input.
            return false;
        }
        if (mergeDone == false) {
            // Workers are sorting (or done but not yet merged); output will arrive without further external input.
            return true;
        }
        return mergeTarget.canProduceMoreDataWithoutExtraInput();
    }

    @Override
    public IsBlockedResult isBlocked() {
        if (finishCalled) {
            if (allWorkersDone.isDone() == false) {
                return new IsBlockedResult(allWorkersDone, "waiting for parallel topn workers");
            }
        }
        return NOT_BLOCKED;
    }

    @Override
    public Page getOutput() {
        if (failureCollector.hasFailure()) {
            throw ExceptionsHelper.convertToRuntime(failureCollector.getFailure());
        }
        if (allWorkersDone.isDone() == false) {
            return null;
        }
        if (mergeDone == false) {
            // Pop each worker's queue directly into the merge-target, bypassing page encode/decode.
            // Abort-path workers already closed themselves (inputQueue==null); skip them.
            // PriorityQueue.pop() nulls each heap slot, so worker.close() after the drain only
            // releases the empty queue's struct bytes — the transferred rows are not double-closed.
            // Worker LCs are released in close() after mergeTarget.close() drains surviving rows.
            for (var worker : workers) {
                if (worker.topN.inputQueue != null) {
                    worker.topN.inputQueue.popAllInto(mergeTarget.inputQueue);
                }
            }
            mergeTarget.finish();
            mergeDone = true;
        }
        return mergeTarget.getOutput();
    }

    @Override
    public void close() {
        in.finish(true);
        mergeTarget.close();
        closed = true;
        if (finishCalled == false) {
            pendingTasks.finishTask();
        }
        if (allWorkersDone.isDone()) {
            releaseWorkers();
        }
    }

    @Override
    public long ramBytesUsed() {
        long size = SHALLOW_SIZE;
        size += mergeTarget.ramBytesUsed();
        // Worker accounting is unsafe to read concurrently; only include it once all workers have exited.
        if (allWorkersDone.isDone()) {
            for (var worker : workers) {
                size += worker.topN.ramBytesUsed();
            }
        }
        return size;
    }

    @Override
    public String toString() {
        return "ParallelTopNOperator[workers=" + (workers.size() + 1) + "]";
    }

    private static class PendingTasks {
        final AtomicInteger instances = new AtomicInteger(1);
        final AtomicBoolean completed = new AtomicBoolean();
        final Runnable completion;

        PendingTasks(Runnable completion) {
            this.completion = completion;
        }

        void newTask() {
            int refs = instances.incrementAndGet();
            assert refs > 0;
        }

        void finishTask() {
            int refs = instances.decrementAndGet();
            assert refs >= 0;
            if (refs == 0 && completed.compareAndSet(false, true)) {
                completion.run();
            }
        }
    }

    private class Worker implements Releasable {
        final BlockFactory factory;
        final TopNOperator topN;
        private final AtomicBoolean released = new AtomicBoolean();

        Worker(BlockFactory factory, TopNOperator topN) {
            this.factory = factory;
            this.topN = topN;
        }

        @Override
        public void close() {
            if (released.compareAndSet(false, true)) {
                Releasables.close(topN, () -> driverContext.releaseChildBlockFactory(factory));
            }
        }
    }
}
