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
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.FailureCollector;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.exchange.ExchangeBuffer;

import java.util.ArrayList;
import java.util.List;
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
 *       pages to their per-worker output list.</li>
 *   <li><b>Merging</b> – once all workers have signalled completion via
 *       {@link #allWorkersDone}, the driver thread feeds all per-worker output pages into
 *       {@code mergeTarget} (the initial pre-promotion operator that holds the rows
 *       accumulated before promotion), calls {@link TopNOperator#finish()} on it, and
 *       delegates subsequent {@link #getOutput()} calls to it.</li>
 * </ol>
 *
 * <h2>Memory accounting</h2>
 * Each background worker is created with a separate {@link org.elasticsearch.compute.data.LocalCircuitBreaker}
 * via {@link DriverContext#workerBlockFactory()}. Workers call
 * {@link Page#allowPassingToDifferentDriver()} on their output pages before adding them to
 * their per-worker output list, which transfers block memory from the worker's local breaker
 * to the global breaker so the driver thread can release it safely. Input pages have
 * {@link Page#allowPassingToDifferentDriver()} called on the driver thread in
 * {@link #addInput} before being enqueued, for the same reason.
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
    private final List<TopNOperator> workers;

    /**
     * Per-worker output page lists. Each background worker writes exclusively to its own
     * list; by the time the driver reads from these lists {@link #allWorkersDone} has fired,
     * so reads and writes never overlap and no concurrent data structure is needed.
     */
    private final List<List<Page>> workerOutputs;

    private final DriverContext driverContext;
    private final Executor executor;
    private final FailureCollector failureCollector = new FailureCollector();

    /** Decrements as each background worker permanently exits; fires {@link #allWorkersDone}. */
    private final AtomicInteger runningWorkerTasks;
    /** Completes when all background workers have finished (or there were none). */
    private final SubscribableListener<Void> allWorkersDone = new SubscribableListener<>();

    /** True once {@link #finish()} has been called. */
    private boolean finishCalled = false;

    /** True once {@link #close()} has been called; signals workers to stop. */
    private volatile boolean closed = false;

    /**
     * True once the merge phase (feeding per-worker output pages into {@link #mergeTarget})
     * has completed and {@link TopNOperator#finish()} has been called on {@link #mergeTarget}.
     */
    private boolean mergeDone = false;

    /**
     * @param config          parallel worker configuration (executor, worker count, etc.)
     * @param driverContext   driver context for block-factory and async-action tracking
     * @param factory         factory used to create fresh background worker operators
     * @param initialWorker   the promoted {@link TopNOperator} that holds pre-promotion rows;
     *                        becomes {@link #mergeTarget} and is never sent to a background thread
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
        this.mergeTarget = initialWorker;

        int backgroundWorkerCount = config.workerCount();
        if (backgroundWorkerCount < 1) {
            throw new IllegalArgumentException(
                "ParallelTopNOperator requires at least one background worker, got " + backgroundWorkerCount
            );
        }
        this.workers = new ArrayList<>(backgroundWorkerCount);
        this.workerOutputs = new ArrayList<>(backgroundWorkerCount);
        for (int i = 0; i < backgroundWorkerCount; i++) {
            workers.add(factory.getWorkerOperator(driverContext));
            workerOutputs.add(new ArrayList<>());
        }

        this.runningWorkerTasks = new AtomicInteger(backgroundWorkerCount);
        for (int i = 0; i < workers.size(); i++) {
            driverContext.addAsyncAction();
            scheduleWorker(workers.get(i), workerOutputs.get(i));
        }
    }

    private void scheduleWorker(TopNOperator worker, List<Page> workerOutput) {
        executor.execute(new AbstractRunnable() {
            @Override
            protected void doRun() throws Exception {
                runWorker(worker, workerOutput);
            }

            @Override
            public void onFailure(Exception e) {
                failureCollector.unwrapAndCollect(e);
                in.finish(true);
                workerPermanentlyExited(worker, true);
            }

            @Override
            public void onRejection(Exception e) {
                if (e instanceof EsRejectedExecutionException) {
                    // The thread pool queue is full, run on the driver thread. While there are pages available
                    // on the input buffer, the driver thread will keep worker. If pages are exhausted,
                    // scheduleWorker will be called again, given the chance to go back to the worker thread.
                    try {
                        runWorker(worker, workerOutput);
                    } catch (Exception ex) {
                        onFailure(ex);
                    }
                } else {
                    onFailure(e);
                }
            }
        });
    }

    private void runWorker(TopNOperator worker, List<Page> workerOutput) {
        Page page;
        while (closed == false && (page = in.pollPage()) != null) {
            worker.addInput(page);
        }

        if (in.noMoreInputs() || closed) {
            // Finished draining (or aborting). Produce output pages and transfer them to the driver.
            boolean abort = closed || failureCollector.hasFailure();
            if (abort == false) {
                worker.finish();
                Page outputPage;
                while ((outputPage = worker.getOutput()) != null) {
                    try {
                        outputPage.allowPassingToDifferentDriver();
                    } catch (Exception e) {
                        outputPage.releaseBlocks();
                        throw e;
                    }
                    if (closed) {
                        outputPage.releaseBlocks();
                    } else {
                        workerOutput.add(outputPage);
                    }
                }
            }
            workerPermanentlyExited(worker, abort);
        } else {
            // Buffer temporarily empty; reschedule when more pages arrive or buffer finishes.
            in.waitForReading().listener().addListener(ActionListener.running(() -> scheduleWorker(worker, workerOutput)));
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
                // The failure is already recorded in failureCollector at the call site; swallow
                // any secondary exception here to avoid masking the root cause.
            }
        }
        try {
            worker.close();
        } catch (Exception e) {
            failureCollector.unwrapAndCollect(e);
        }
        try {
            worker.releaseBreaker();
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
        page.allowPassingToDifferentDriver();
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
            return NOT_BLOCKED;
        }
        return in.waitForWriting();
    }

    @Override
    public Page getOutput() {
        if (failureCollector.hasFailure()) {
            // Release any pages accumulated for merge before throwing.
            for (List<Page> pages : workerOutputs) {
                for (Page p : pages) {
                    p.releaseBlocks();
                }
                pages.clear();
            }
            throw ExceptionsHelper.convertToRuntime(failureCollector.getFailure());
        }
        if (allWorkersDone.isDone() == false) {
            return null;
        }
        if (mergeDone == false) {
            // Feed all pages produced by background workers into mergeTarget. Pages are removed
            // from the list before addInput so that: (a) on success, the list is empty when we
            // finish and close() has nothing to double-release; (b) on exception, the throwing
            // page is already cleaned up by TopNOperator.addInput's finally block and the
            // remaining pages stay in the list for close() to release.
            for (List<Page> pages : workerOutputs) {
                var it = pages.iterator();
                while (it.hasNext()) {
                    Page p = it.next();
                    it.remove();
                    mergeTarget.addInput(p);
                }
            }
            mergeTarget.finish();
            mergeDone = true;
        }
        return mergeTarget.getOutput();
    }

    @Override
    public void close() {
        closed = true;
        in.finish(true);
        // Release any merge pages already queued; workers still running will release their own.
        for (List<Page> pages : workerOutputs) {
            for (Page p : pages) {
                p.releaseBlocks();
            }
            pages.clear();
        }
        // Close mergeTarget here (driver thread). Background workers close themselves.
        mergeTarget.close();
    }

    @Override
    public long ramBytesUsed() {
        long size = SHALLOW_SIZE;
        size += mergeTarget.ramBytesUsed();
        // Workers run on their own threads and own non-thread-safe mutable state; only read
        // their accounting after they have exited and closed themselves.
        // NOTE: during the collecting phase the worker input queues are not reflected here,
        // which is a monitoring gap. The memory is still protected by the circuit breaker
        // (each worker's LocalCircuitBreaker charges allocations to the parent breaker as
        // they happen). Fixing this would require storing the LocalCircuitBreaker references
        // and summing their getUsed() — left as a known limitation for now.
        if (allWorkersDone.isDone()) {
            for (TopNOperator worker : workers) {
                size += worker.ramBytesUsed();
            }
        }
        return size;
    }

    @Override
    public String toString() {
        return "ParallelTopNOperator[workers=" + (workers.size() + 1) + "]";
    }
}
