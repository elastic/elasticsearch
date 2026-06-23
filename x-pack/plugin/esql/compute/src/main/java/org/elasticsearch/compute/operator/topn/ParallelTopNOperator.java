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
import java.util.concurrent.ThreadPoolExecutor;
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
 * (LC) via {@link DriverContext#workerBlockFactory()}.
 * The LC backs every allocation the worker makes: the {@link TopNQueue} struct, the spare
 * {@link TopNRow}, and the bytes inside each row (each {@link TopNRow} holds a direct
 * reference to its creator's breaker).
 *
 * <p><b>Aborting workers</b> clean up on the worker thread: {@link TopNOperator#close()}
 * closes all rows and the queue struct, returning their bytes to the LC;
 * {@link TopNOperator#releaseBreaker()} then closes the LC and returns its reserved bytes
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
 *   <li>Calls {@link TopNOperator#releaseBreaker()} for all workers in {@link #close()},
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
    private final List<TopNOperator> workers;

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
     * True once the merge phase (popping worker queues into {@link #mergeTarget}) has
     * completed and {@link TopNOperator#finish()} has been called on {@link #mergeTarget}.
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
        for (int i = 0; i < backgroundWorkerCount; i++) {
            workers.add(factory.getWorkerOperator(driverContext));
        }

        this.runningWorkerTasks = new AtomicInteger(backgroundWorkerCount);
        for (TopNOperator worker : workers) {
            driverContext.addAsyncAction();
            scheduleWorker(worker);
        }
    }

    private void scheduleWorker(TopNOperator worker) {
        // If the queue depth is at least as large as the thread count, even an immediately-free
        // thread wouldn't reach our task soon. Run inline instead.
        if (executor instanceof ThreadPoolExecutor tpe && tpe.getQueue().size() >= tpe.getMaximumPoolSize()) {
            runWorkerInline(worker);
            return;
        }

        executor.execute(new AbstractRunnable() {
            @Override
            protected void doRun() throws Exception {
                runWorker(worker);
            }

            @Override
            public void onFailure(Exception e) {
                ParallelTopNOperator.this.onFailure(worker, e);
            }

            @Override
            public void onRejection(Exception e) {
                if (e instanceof EsRejectedExecutionException) {
                    // Thread pool full; run inline on the driver thread instead.
                    // If the buffer empties before finishing, the worker is rescheduled.
                    runWorkerInline(worker);
                } else {
                    onFailure(e);
                }
            }
        });
    }

    private void runWorkerInline(TopNOperator worker) {
        try {
            runWorker(worker);
        } catch (Exception e) {
            onFailure(worker, e);
        }
    }

    private void onFailure(TopNOperator worker, Exception e) {
        failureCollector.unwrapAndCollect(e);
        in.finish(true);
        workerPermanentlyExited(worker, true);
    }

    private void runWorker(TopNOperator worker) {
        Page page;
        while (closed == false && (page = in.pollPage()) != null) {
            worker.addInput(page);
        }

        if (in.isFinished()) {
            boolean abort = closed || failureCollector.hasFailure();
            workerPermanentlyExited(worker, abort);
        } else {
            // Buffer temporarily empty; reschedule when more pages arrive or buffer finishes.
            in.waitForReading().listener().addListener(ActionListener.running(() -> scheduleWorker(worker)));
        }
    }

    /**
     * Called when a background worker has permanently stopped.
     *
     * <p>For the <b>abort path</b> ({@code abort == true}): the worker's queue rows are
     * closed and the LC is released here on the worker thread.
     *
     * <p>For the <b>non-abort transfer path</b> ({@code abort == false}): the worker exits
     * without touching its queue or LC, transferring ownership to the driver thread.
     * The driver merges the worker's queue in {@link #getOutput()} and releases its
     * LC in {@link #close()}.
     */
    private void workerPermanentlyExited(TopNOperator worker, boolean abort) {
        if (abort) {
            worker.close();
            try {
                worker.releaseBreaker();
            } catch (Exception e) {
                failureCollector.unwrapAndCollect(e);
            }
        }
        // else: non-abort with closed==false → driver owns this worker's queue and LC.
        if (runningWorkerTasks.decrementAndGet() == 0) {
            allWorkersDone.onResponse(null);
        }
        driverContext.removeAsyncAction();
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
            for (TopNOperator worker : workers) {
                if (worker.inputQueue != null) {
                    worker.inputQueue.popAllInto(mergeTarget.inputQueue);
                    worker.close();
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
        // Close mergeTarget first so surviving rows return their bytes to worker LCs
        // before those LCs are released.
        mergeTarget.close();
        if (allWorkersDone.isDone()) {
            releaseAllWorkers();
        } else {
            // Workers still running will see closed==true and abort; release all workers
            // (including any that already transferred ownership) once they have all exited.
            allWorkersDone.addListener(ActionListener.running(this::releaseAllWorkers));
        }
    }

    private void releaseAllWorkers() {
        for (TopNOperator worker : workers) {
            // Abort-path workers already closed themselves (inputQueue==null); skip them.
            if (worker.inputQueue != null) {
                worker.close();
            }
            try {
                // releaseBreaker() calls LocalCircuitBreaker.close() which is idempotent
                worker.releaseBreaker();
            } catch (Exception e) {
                failureCollector.unwrapAndCollect(e);
            }
        }
    }

    @Override
    public long ramBytesUsed() {
        long size = SHALLOW_SIZE;
        size += mergeTarget.ramBytesUsed();
        // Worker accounting is unsafe to read concurrently; only include it once all workers have exited.
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
