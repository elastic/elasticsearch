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
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.FailureCollector;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.exchange.ExchangeBuffer;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
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
 *       the remaining pages, build their local top-K results, and transfer the resulting
 *       pages to {@link #mergePages}.</li>
 *   <li><b>Merging</b> – once all workers have signalled completion via
 *       {@link #allWorkersDone}, the driver thread feeds {@code mergePages} into
 *       {@code workers.get(0)} (the initial pre-promotion operator that holds the rows
 *       accumulated before promotion), calls {@link TopNOperator#finish()} on it, and
 *       delegates subsequent {@link #getOutput()} calls to it.</li>
 * </ol>
 *
 * <h2>Memory accounting</h2>
 * Each background worker is created with a separate {@link org.elasticsearch.compute.data.LocalCircuitBreaker}
 * via {@link DriverContext#workerBlockFactory()}. Workers call
 * {@link Page#allowPassingToDifferentDriver()} on their output pages before adding them to
 * {@code mergePages}, which transfers block memory from the worker's local breaker to the
 * global breaker so the driver thread can release it safely.
 */
public class ParallelTopNOperator implements Operator, Accountable {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(ParallelTopNOperator.class);

    /** Bounded FIFO queue from driver thread to background workers. */
    private final ExchangeBuffer in;

    /**
     * Workers list. {@code workers.get(0)} is the initial {@link TopNOperator} that was
     * promoted and already holds rows accumulated before promotion. It is never dispatched
     * to a background thread. {@code workers.get(1..n)} are fresh workers running on
     * {@code esql_worker} threads.
     */
    private final List<TopNOperator> workers;

    private final DriverContext driverContext;
    private final Executor executor;
    private final FailureCollector failureCollector = new FailureCollector();

    /** Decrements as each background worker permanently exits; fires {@link #allWorkersDone}. */
    private final AtomicInteger runningWorkerTasks;
    /** Completes when all background workers have finished (or there were none). */
    private final SubscribableListener<Void> allWorkersDone = new SubscribableListener<>();

    /**
     * Pages produced by background workers after they finish processing the {@link #in}
     * buffer. These pages have had {@link Page#allowPassingToDifferentDriver()} called and
     * are safe to consume on the driver thread during the merge phase.
     */
    private final Queue<Page> mergePages = new ConcurrentLinkedQueue<>();

    /** True once {@link #finish()} has been called. */
    private volatile boolean finishCalled = false;

    /** True once {@link #close()} has been called; signals workers to stop. */
    private volatile boolean closed = false;

    /**
     * True once the merge phase (feeding {@link #mergePages} into {@code workers.get(0)})
     * has completed and {@link TopNOperator#finish()} has been called on {@code workers.get(0)}.
     */
    private boolean mergeDone = false;

    /**
     * @param config          parallel worker configuration (executor, worker count, etc.)
     * @param driverContext   driver context for block-factory and async-action tracking
     * @param factory         factory used to create fresh background worker operators
     * @param initialWorker   the promoted {@link TopNOperator} that holds pre-promotion rows;
     *                        becomes {@code workers.get(0)} and is never sent to a background thread
     */
    public ParallelTopNOperator(
        TopNOperator.ParallelWorkerConfig config,
        DriverContext driverContext,
        TopNOperator.TopNOperatorFactory factory,
        TopNOperator initialWorker
    ) {
        this.in = new ExchangeBuffer(config.maxInFlightPages());
        this.driverContext = driverContext;
        this.executor = config.executor();

        int backgroundWorkerCount = config.workerCount();
        this.workers = new ArrayList<>(backgroundWorkerCount + 1);
        this.workers.add(initialWorker);
        for (int i = 0; i < backgroundWorkerCount; i++) {
            workers.add(factory.getWorkerOperator(driverContext));
        }

        this.runningWorkerTasks = new AtomicInteger(backgroundWorkerCount);
        if (backgroundWorkerCount == 0) {
            allWorkersDone.onResponse(null);
        } else {
            for (int i = 1; i < workers.size(); i++) {
                driverContext.addAsyncAction();
                scheduleWorker(workers.get(i));
            }
        }
    }

    private void scheduleWorker(TopNOperator worker) {
        executor.execute(new AbstractRunnable() {
            @Override
            protected void doRun() {
                runWorker(worker);
            }

            @Override
            public void onFailure(Exception e) {
                failureCollector.unwrapAndCollect(e);
                in.finish(true);
                workerPermanentlyExited(worker, true);
            }

            @Override
            public void onRejection(Exception e) {
                failureCollector.unwrapAndCollect(e);
                in.finish(true);
                workerPermanentlyExited(worker, true);
            }
        });
    }

    private void runWorker(TopNOperator worker) {
        try {
            Page page;
            while (closed == false && (page = in.pollPage()) != null) {
                page.allowPassingToDifferentDriver();
                worker.addInput(page);
            }
        } catch (Exception e) {
            failureCollector.unwrapAndCollect(e);
            in.finish(true);
            workerPermanentlyExited(worker, true);
            return;
        }

        if (in.noMoreInputs() || closed) {
            // Finished draining (or aborting). Produce output pages and transfer them to the driver.
            boolean abort = closed || failureCollector.hasFailure();
            if (abort == false) {
                try {
                    worker.finish();
                    Page outputPage;
                    while ((outputPage = worker.getOutput()) != null) {
                        outputPage.allowPassingToDifferentDriver();
                        if (closed) {
                            outputPage.releaseBlocks();
                        } else {
                            mergePages.add(outputPage);
                        }
                    }
                } catch (Exception e) {
                    failureCollector.unwrapAndCollect(e);
                    abort = true;
                }
            }
            workerPermanentlyExited(worker, abort);
        } else {
            // Buffer temporarily empty; reschedule when more pages arrive or buffer finishes.
            in.waitForReading().listener().addListener(ActionListener.running(() -> scheduleWorker(worker)));
        }
    }

    /**
     * Called when a background worker has permanently stopped (either normally or due to an
     * error). Closes the worker (safe because we are on the worker thread that owns its
     * {@link org.elasticsearch.compute.data.LocalCircuitBreaker}), then decrements the
     * running-worker counter, and notifies the driver thread when all workers are done.
     *
     * @param abort true when called from an error/rejection handler, meaning {@link TopNOperator#finish}
     *              may not have been called yet and we must call it before closing.
     */
    private void workerPermanentlyExited(TopNOperator worker, boolean abort) {
        if (abort) {
            try {
                worker.finish();
            } catch (Exception ignored) {
                // best-effort
            }
        }
        try {
            worker.close();
        } catch (Exception e) {
            failureCollector.unwrapAndCollect(e);
        } finally {
            if (runningWorkerTasks.decrementAndGet() == 0) {
                allWorkersDone.onResponse(null);
            }
            driverContext.removeAsyncAction();
        }
    }

    @Override
    public boolean needsInput() {
        return finishCalled == false && in.waitForWriting() == Operator.NOT_BLOCKED;
    }

    @Override
    public void addInput(Page page) {
        in.addPage(page);
    }

    @Override
    public void finish() {
        if (finishCalled == false) {
            finishCalled = true;
            in.finish(false);
        }
    }

    @Override
    public boolean isFinished() {
        return mergeDone && workers.get(0).isFinished();
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        return mergeDone && workers.get(0).canProduceMoreDataWithoutExtraInput();
    }

    @Override
    public IsBlockedResult isBlocked() {
        if (finishCalled) {
            if (allWorkersDone.isDone() == false) {
                return new IsBlockedResult(allWorkersDone, "waiting for parallel topn workers");
            }
            return NOT_BLOCKED;
        }
        return in.waitForWriting();
    }

    @Override
    public Page getOutput() {
        if (failureCollector.hasFailure()) {
            // Release any pages accumulated for merge before throwing.
            Page p;
            while ((p = mergePages.poll()) != null) {
                p.releaseBlocks();
            }
            throw ExceptionsHelper.convertToRuntime(failureCollector.getFailure());
        }
        if (allWorkersDone.isDone() == false) {
            return null;
        }
        if (mergeDone == false) {
            // Feed all pages produced by background workers into workers[0] for merge.
            Page p;
            while ((p = mergePages.poll()) != null) {
                workers.get(0).addInput(p);
            }
            workers.get(0).finish();
            mergeDone = true;
        }
        return workers.get(0).getOutput();
    }

    @Override
    public void close() {
        closed = true;
        in.finish(true);
        // Release any merge pages already queued; workers still running will release their own.
        Page p;
        while ((p = mergePages.poll()) != null) {
            p.releaseBlocks();
        }
        // Close workers[0] here (driver thread). Background workers close themselves.
        workers.get(0).close();
    }

    @Override
    public long ramBytesUsed() {
        long size = SHALLOW_SIZE;
        for (TopNOperator worker : workers) {
            size += worker.ramBytesUsed();
        }
        return size;
    }

    @Override
    public String toString() {
        return "ParallelTopNOperator[workers=" + workers.size() + "]";
    }
}
