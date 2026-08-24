/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.compute.Describable;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A parallel variant of {@link HashAggregationOperator} that dispatches incoming pages to multiple
 * worker operators, each responsible for a disjoint partition of the group key space.
 *
 * <h2>States</h2>
 * <ul>
 *   <li><b>PROBING</b> — incoming pages are buffered raw; no aggregation yet. Transitions to
 *       FALLBACK if any multivalue key is detected, or when {@code promotionThresholdRows} rows
 *       have been buffered.</li>
 *   <li><b>FALLBACK</b> — the inner {@link HashAggregationOperator} is used directly. The buffer
 *       is replayed into it, then new pages are delegated. Behavior is identical to today.</li>
 *   <li><b>PARALLEL</b> — P background workers each own a disjoint partition of the key space.
 *       Incoming pages are split by bucket and dispatched to the appropriate worker queue.</li>
 * </ul>
 *
 * <h2>Partition correctness</h2>
 * {@link GroupKeyBucketer} ensures every row with the same composite group key always maps to the
 * same bucket, so each group is fully aggregated by exactly one worker. Workers emit final rows
 * and the results are concatenated without any cross-worker merge.
 *
 * <h2>Memory accounting</h2>
 * Each worker has its own {@link org.elasticsearch.compute.data.LocalCircuitBreaker} via
 * {@link DriverContext#createChildBlockFactory()}. {@link Page#allowPassingToDifferentDriver()} is
 * called on each sub-page on the driver thread before it is published to a worker queue, which
 * transfers block ownership to the global factory and makes all refcounts thread-safe.
 */
public final class ParallelHashAggregationOperator implements Operator {

    private enum State {
        PROBING,
        FALLBACK,
        PARALLEL
    }

    private final HashAggregationOperator.Factory inner;
    private final GroupKeyBucketer bucketer;
    private final int numWorkers;
    private final int promotionThresholdRows;
    private final DriverContext driverContext;
    private final Executor executor;

    // --- PROBING state ---
    private final List<Page> buffer = new ArrayList<>();
    private int bufferedRows = 0;
    private State state = State.PROBING;

    // --- FALLBACK state ---
    private HashAggregationOperator fallbackOp;

    // --- PARALLEL state ---
    private List<Worker> workers;
    private FailureCollector failureCollector;
    private AtomicInteger workersDoneCount;
    private SubscribableListener<Void> allWorkersDoneListener;
    private IsBlockedResult blockedResult;
    private List<Page> outputPages;
    private int outputIdx;

    private boolean finishCalled = false;
    private volatile boolean closed = false;
    /** Guards the single call to {@link DriverContext#removeAsyncAction()} across close() and workerFinished(). */
    private final AtomicBoolean asyncActionRemoved = new AtomicBoolean(true);

    public ParallelHashAggregationOperator(
        HashAggregationOperator.Factory inner,
        List<BlockHash.GroupSpec> groupSpecs,
        int numWorkers,
        int promotionThresholdRows,
        DriverContext driverContext,
        Executor executor
    ) {
        this.inner = inner;
        this.bucketer = new GroupKeyBucketer(groupSpecs);
        this.numWorkers = numWorkers;
        this.promotionThresholdRows = promotionThresholdRows;
        this.driverContext = driverContext;
        this.executor = executor;
    }

    // -------------------------------------------------------------------------
    // Operator interface
    // -------------------------------------------------------------------------

    @Override
    public boolean needsInput() {
        return switch (state) {
            case PROBING -> true;
            case FALLBACK -> fallbackOp.needsInput();
            case PARALLEL -> finishCalled == false;
        };
    }

    @Override
    public void addInput(Page page) {
        switch (state) {
            case PROBING -> probingAddInput(page);
            case FALLBACK -> fallbackOp.addInput(page);
            case PARALLEL -> parallelAddInput(page);
        }
    }

    @Override
    public void finish() {
        if (finishCalled) return;
        finishCalled = true;
        switch (state) {
            case PROBING -> {
                promoteToFallback();
                fallbackOp.finish();
            }
            case FALLBACK -> fallbackOp.finish();
            case PARALLEL -> signalWorkersFinished();
        }
    }

    @Override
    public boolean isFinished() {
        return switch (state) {
            case PROBING -> false;
            case FALLBACK -> fallbackOp.isFinished();
            case PARALLEL -> finishCalled && allWorkersDone() && drainedAllOutput();
        };
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        return switch (state) {
            case PROBING -> false;
            case FALLBACK -> fallbackOp.canProduceMoreDataWithoutExtraInput();
            case PARALLEL -> finishCalled && (allWorkersDone() == false || drainedAllOutput() == false);
        };
    }

    @Override
    public IsBlockedResult isBlocked() {
        if (state == State.PARALLEL && finishCalled && allWorkersDone() == false) {
            return blockedResult;
        }
        return NOT_BLOCKED;
    }

    @Override
    public Page getOutput() {
        return switch (state) {
            case PROBING -> null;
            case FALLBACK -> fallbackOp.getOutput();
            case PARALLEL -> parallelGetOutput();
        };
    }

    @Override
    public void close() {
        closed = true;
        switch (state) {
            case PROBING -> {
                for (Page p : buffer) {
                    p.releaseBlocks();
                }
                buffer.clear();
            }
            case FALLBACK -> {
                if (fallbackOp != null) {
                    fallbackOp.close();
                }
                // If fallbackOp construction failed, buffer pages were released in promoteToFallback().
            }
            case PARALLEL -> {
                // Signal all workers to stop waiting; they will see closed==true and call
                // workerFinished() so driverContext.removeAsyncAction() is eventually called.
                if (workers != null) {
                    for (Worker w : workers) {
                        w.markInputDone();
                    }
                    Releasables.close(workers);
                }
                if (outputPages != null) {
                    for (int i = outputIdx; i < outputPages.size(); i++) {
                        outputPages.get(i).releaseBlocks();
                    }
                }
                // If workers never all finished (e.g. addInput threw before finish()), the
                // async action was never removed. Remove it here so the Driver can exit.
                if (asyncActionRemoved.compareAndSet(false, true)) {
                    driverContext.removeAsyncAction();
                }
            }
        }
    }

    @Override
    public String toString() {
        return switch (state) {
            case PROBING -> "ParallelHashAggregationOperator[workers=" + numWorkers + ", state=PROBING]";
            case FALLBACK -> "ParallelHashAggregationOperator[workers=" + numWorkers + ", state=FALLBACK, op=" + fallbackOp + "]";
            case PARALLEL -> "ParallelHashAggregationOperator[workers=" + numWorkers + ", state=PARALLEL]";
        };
    }

    @Override
    public Operator.Status status() {
        return switch (state) {
            case PROBING -> new HashAggregationOperator.Status(0, 0, 0, bufferedRows, 0, 0, 0);
            case FALLBACK -> fallbackOp.status();
            case PARALLEL -> {
                long hash = 0, agg = 0, emit = 0;
                int pages = 0;
                long rowsIn = 0, rowsOut = 0;
                if (workers != null) {
                    for (Worker w : workers) {
                        Operator.Status ws = w.op.status();
                        if (ws instanceof HashAggregationOperator.Status hs) {
                            hash += hs.hashNanos();
                            agg += hs.aggregationNanos();
                            emit += hs.emitNanos();
                            pages += hs.pagesProcessed();
                            rowsIn += hs.rowsReceived();
                            rowsOut += hs.rowsEmitted();
                        }
                    }
                }
                yield new HashAggregationOperator.Status(hash, agg, pages, rowsIn, rowsOut, emit, 0);
            }
        };
    }

    // -------------------------------------------------------------------------
    // PROBING state helpers
    // -------------------------------------------------------------------------

    private void probingAddInput(Page page) {
        for (BlockHash.GroupSpec spec : bucketer.specs()) {
            if (page.getBlock(spec.channel()).mayHaveMultivaluedFields()) {
                promoteToFallback();
                fallbackOp.addInput(page);
                return;
            }
        }

        buffer.add(page);
        bufferedRows += page.getPositionCount();

        if (bufferedRows >= promotionThresholdRows) {
            promoteToParallel();
        }
    }

    private void promoteToFallback() {
        // Set state first so close() reaches the FALLBACK branch even if construction below fails.
        state = State.FALLBACK;
        try {
            fallbackOp = inner.get(driverContext);
        } catch (Exception e) {
            // Construction failed; release buffered pages ourselves then propagate.
            for (Page p : buffer) {
                p.releaseBlocks();
            }
            buffer.clear();
            throw e;
        }
        for (Page p : buffer) {
            fallbackOp.addInput(p);
        }
        buffer.clear();
    }

    private void promoteToParallel() {
        workers = new ArrayList<>(numWorkers);
        boolean success = false;
        try {
            for (int i = 0; i < numWorkers; i++) {
                BlockFactory childFactory = driverContext.createChildBlockFactory();
                try {
                    DriverContext workerCtx = new DriverContext(
                        driverContext.bigArrays(),
                        childFactory,
                        driverContext.localBreakerSettings()
                    );
                    HashAggregationOperator op = inner.get(workerCtx);
                    workers.add(new Worker(i, childFactory, op));
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
                Releasables.close(workers);
                workers = null;
                // Fall back to serial.
                promoteToFallback();
                return;
            }
        }

        state = State.PARALLEL;
        failureCollector = new FailureCollector();
        workersDoneCount = new AtomicInteger(0);
        allWorkersDoneListener = new SubscribableListener<>();
        blockedResult = new IsBlockedResult(allWorkersDoneListener, "waiting for parallel hash agg workers");
        outputPages = null;
        outputIdx = 0;

        asyncActionRemoved.set(false);
        driverContext.addAsyncAction();

        // Replay buffered pages into the parallel pipeline.
        for (Page p : buffer) {
            parallelAddInputInternal(p);
        }
        buffer.clear();

        // Schedule all workers.
        for (Worker w : workers) {
            scheduleWorker(w);
        }
    }

    // -------------------------------------------------------------------------
    // PARALLEL state helpers
    // -------------------------------------------------------------------------

    private void parallelAddInput(Page page) {
        if (closed) {
            page.releaseBlocks();
            return;
        }
        parallelAddInputInternal(page);
    }

    private void parallelAddInputInternal(Page page) {
        int positionCount = page.getPositionCount();
        if (positionCount == 0) {
            page.releaseBlocks();
            return;
        }

        int[] buckets = new int[positionCount];
        int mv = bucketer.computeBuckets(page, numWorkers, buckets, new BytesRef(), new MurmurHash3.Hash128());
        if (mv == GroupKeyBucketer.MULTIVALUE_DETECTED) {
            page.releaseBlocks();
            failureCollector.unwrapAndCollect(new IllegalStateException("multivalue group key detected after promotion to parallel mode"));
            return;
        }

        // Counting sort: determine per-bucket sizes and build permutation.
        int[] counts = new int[numWorkers];
        for (int i = 0; i < positionCount; i++) {
            counts[buckets[i]]++;
        }

        int[] starts = new int[numWorkers];
        for (int b = 1; b < numWorkers; b++) {
            starts[b] = starts[b - 1] + counts[b - 1];
        }

        int[] order = new int[positionCount];
        int[] cursors = starts.clone();
        for (int i = 0; i < positionCount; i++) {
            int b = buckets[i];
            order[cursors[b]++] = i;
        }

        // Split and dispatch. The original page is always released on the driver thread via
        // try-finally; workers receive either a filter copy or a shallow copy (for the
        // whole-page case) so they never share the same Page reference with the driver thread.
        try {
            for (int b = 0; b < numWorkers; b++) {
                if (counts[b] == 0) continue;
                Page subPage;
                if (counts[b] == positionCount) {
                    // All rows go to one bucket: shallow-copy to give the worker its own
                    // Page reference while sharing the block data via reference counting.
                    subPage = page.shallowCopy();
                } else {
                    subPage = page.filter(false, order, starts[b], counts[b]);
                }
                subPage.allowPassingToDifferentDriver();
                workers.get(b).enqueue(subPage);
            }
        } finally {
            page.releaseBlocks();
        }
    }

    private void signalWorkersFinished() {
        for (Worker w : workers) {
            w.markInputDone();
        }
    }

    private boolean allWorkersDone() {
        return workersDoneCount != null && workersDoneCount.get() == numWorkers;
    }

    private boolean drainedAllOutput() {
        return outputPages != null && outputIdx >= outputPages.size();
    }

    private Page parallelGetOutput() {
        if (failureCollector != null && failureCollector.hasFailure()) {
            throw ExceptionsHelper.convertToRuntime(failureCollector.getFailure());
        }
        if (allWorkersDone() == false) {
            return null;
        }
        if (outputPages == null) {
            outputPages = new ArrayList<>();
            for (Worker w : workers) {
                outputPages.addAll(w.outputPages);
                // Transfer ownership: Worker.close() must not release pages we have taken.
                w.outputPages.clear();
            }
        }
        if (outputIdx < outputPages.size()) {
            return outputPages.get(outputIdx++);
        }
        return null;
    }

    private void scheduleWorker(Worker worker) {
        executor.execute(new AbstractRunnable() {
            @Override
            protected void doRun() {
                if (closed) {
                    workerFinished();
                    return;
                }
                try {
                    worker.drainQueue();
                    if (worker.inputDone && worker.queue.isEmpty()) {
                        // All input consumed — produce output.
                        worker.op.finish();
                        Page out;
                        while ((out = worker.op.getOutput()) != null) {
                            out.allowPassingToDifferentDriver();
                            worker.outputPages.add(out);
                        }
                        workerFinished();
                    } else if (worker.inputDone == false) {
                        // Wait for more input.
                        worker.waitForMore(() -> scheduleWorker(worker));
                    } else {
                        // Race: drain exhausted but queue had pages — reschedule.
                        scheduleWorker(worker);
                    }
                } catch (Exception e) {
                    failureCollector.unwrapAndCollect(e);
                    workerFinished();
                }
            }

            @Override
            public void onFailure(Exception e) {
                failureCollector.unwrapAndCollect(e);
                workerFinished();
            }

            @Override
            public void onRejection(Exception e) {
                // Rejected = thread pool full. Fail loudly — never silently drop rows.
                failureCollector.unwrapAndCollect(
                    new IllegalStateException("parallel hash agg worker rejected by executor; data would be lost", e)
                );
                workerFinished();
            }

            private void workerFinished() {
                int done = workersDoneCount.incrementAndGet();
                if (done == numWorkers) {
                    allWorkersDoneListener.onResponse(null);
                    if (asyncActionRemoved.compareAndSet(false, true)) {
                        driverContext.removeAsyncAction();
                    }
                }
            }
        });
    }

    // -------------------------------------------------------------------------
    // Worker inner class
    // -------------------------------------------------------------------------

    /**
     * Factory that wraps a {@link HashAggregationOperator.Factory} and produces
     * {@link ParallelHashAggregationOperator} instances when {@code numWorkers > 1}.
     */
    public static final class Factory implements OperatorFactory, Describable {
        private final HashAggregationOperator.Factory inner;
        private final List<BlockHash.GroupSpec> groupSpecs;
        private final int numWorkers;
        private final int promotionThresholdRows;
        private final Executor executor;

        public Factory(
            HashAggregationOperator.Factory inner,
            List<BlockHash.GroupSpec> groupSpecs,
            int numWorkers,
            int promotionThresholdRows,
            Executor executor
        ) {
            this.inner = inner;
            this.groupSpecs = List.copyOf(groupSpecs);
            this.numWorkers = numWorkers;
            this.promotionThresholdRows = promotionThresholdRows;
            this.executor = executor;
        }

        @Override
        public Operator get(DriverContext driverContext) {
            return new ParallelHashAggregationOperator(inner, groupSpecs, numWorkers, promotionThresholdRows, driverContext, executor);
        }

        @Override
        public String describe() {
            return "ParallelHashAggregationOperator[workers=" + numWorkers + ", inner=" + inner.describe() + "]";
        }
    }

    private final class Worker implements Releasable {
        final int index;
        final BlockFactory factory;
        final HashAggregationOperator op;
        final ConcurrentLinkedQueue<Page> queue = new ConcurrentLinkedQueue<>();
        final List<Page> outputPages = new ArrayList<>();
        volatile boolean inputDone = false;
        private final List<Runnable> waitCallbacks = new ArrayList<>();
        private final AtomicBoolean released = new AtomicBoolean();

        Worker(int index, BlockFactory factory, HashAggregationOperator op) {
            this.index = index;
            this.factory = factory;
            this.op = op;
        }

        void enqueue(Page page) {
            queue.offer(page);
            notifyWaiters();
        }

        void markInputDone() {
            inputDone = true;
            notifyWaiters();
        }

        synchronized void waitForMore(Runnable callback) {
            if (inputDone || queue.isEmpty() == false) {
                callback.run();
            } else {
                waitCallbacks.add(callback);
            }
        }

        synchronized void notifyWaiters() {
            for (Runnable cb : waitCallbacks) {
                cb.run();
            }
            waitCallbacks.clear();
        }

        void drainQueue() {
            Page page;
            while ((page = queue.poll()) != null) {
                op.addInput(page);
                // Drain any partial-emit output produced by this addInput call.
                // HashAggregationOperator emits partial results periodically to avoid OOM;
                // we must consume them so needsInput() returns true for the next page.
                drainOutput();
            }
        }

        void drainOutput() {
            Page out;
            while ((out = op.getOutput()) != null) {
                out.allowPassingToDifferentDriver();
                outputPages.add(out);
            }
        }

        @Override
        public void close() {
            if (released.compareAndSet(false, true)) {
                Page p;
                while ((p = queue.poll()) != null) {
                    p.releaseBlocks();
                }
                for (Page out : outputPages) {
                    out.releaseBlocks();
                }
                outputPages.clear();
                Releasables.close(op, () -> driverContext.releaseChildBlockFactory(factory));
            }
        }
    }
}
