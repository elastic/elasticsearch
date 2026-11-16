/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.blockhash.PartitionedBlockHash;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.PartitionedAggregationBlock;
import org.elasticsearch.compute.operator.exchange.ExchangeBuffer;
import org.elasticsearch.compute.spill.QueryScope;
import org.elasticsearch.compute.spill.SliceRunReader;
import org.elasticsearch.compute.spill.SpillCompletion;
import org.elasticsearch.compute.spill.SpillStatus;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A parallel version of {@link HashAggregationOperator} that partitions the aggregation state so
 * many workers can build and combine it concurrently.
 *
 * <p>Aggregation runs in two phases. In the first phase, incoming pages are queued on an input
 * exchange and workers compete to drain it, each aggregating the pages it processes into its own
 * private {@link HashAggregationOperator}. Whenever a worker's state exceeds its key-count or
 * memory threshold, the worker splits that state into partitions and hands them off to a shared
 * registry. After the last input page has been processed, every worker performs one final split.
 *
 * <p>The second phase begins once all workers have completed their final split. Workers claim
 * partitions from the registry, merge the slices of that partition produced by all the splits,
 * and emit the resulting pages to an output exchange, where the driver picks them up.
 *
 * <p>Known limitations: the exchanges are sized and throttled by page count rather than by bytes,
 * which causes unnecessary yields and reschedules. Increasing {@link #pagesPerWorker} mitigates
 * this at the cost of buffering more memory. The emit phase now can dump the entire partition
 * to the exchange instead checking the capacity for every emit.
 *
 * <p>The same partitioning spills large aggregations to disk without sorting keys, reusing the
 * two-phase flow above, when the driver's query has a {@link QueryScope} that spills and every key
 * and state family of the aggregation has a partition codec. In the first phase, once the registry
 * holds the own-state minimum and the query crosses its budget, a split generation is written to a
 * slice run instead of being kept in memory, received partitioned blocks are admitted by the same
 * rule as they arrive, and the workers' split threshold rises to a share of the budget. In the
 * second phase, a worker claiming a partition restores its spilled slices a bounded number at a
 * time, combines them with the resident ones one generation at a time, and emits as usual; a
 * restore that has not landed parks the worker, which the landing reschedules. Only the slices of
 * the partitions being merged are in memory, so peak memory is bounded by those partitions rather
 * than by the whole aggregation.
 */
public final class ParallelHashAggregationOperator implements Operator {
    public static final int PAGE_PER_WORKER = 10;
    public static final int MAX_WORKERS = 32;

    /** The driver's context, not the workers' child contexts. Worker tasks meter their CPU time into it. */
    private final DriverContext driverContext;
    private final Executor executor;
    private final int partitionKeysThreshold;
    private final int pagesPerWorker;

    private final Worker[] workers;
    private final AtomicInteger scheduledOrRunningWorkers = new AtomicInteger();

    private final ExchangeBuffer in;
    private final int inputCapacity;
    private int lastPendingPages = 0;
    private boolean finishCalled = false;

    private final AtomicInteger pendingSplits;
    /** Set once every worker's last split and every generation's write are done: nothing is admitted after it. */
    private final AtomicBoolean phaseTwo = new AtomicBoolean();
    private final SubscribableListener<Void> splitsDone = new SubscribableListener<>();

    private final ExchangeBuffer out;
    private IsBlockedResult blocked = NOT_BLOCKED;
    /**
     * Completed and replaced whenever a parked worker could move again, a restore landed or a task was rejected, so
     * that the driver, which then helps inline, never sleeps on output a rejected worker will not produce.
     */
    private SubscribableListener<Void> progress = new SubscribableListener<>();

    private final PartitionedHashAggregations partitions;
    private final FailureCollector failureCollector = new FailureCollector();

    private volatile boolean operatorClosed = false;

    private long addInputNanos;
    private long addInputInlineCount;
    private long addInputInlineRows;
    private long addInputInlineNanos;
    private int partitionedBlocksReceived;
    private long finishNanos;
    private long inlineEmitCount;
    private long inlineEmitRows;
    private long inlineEmitNanos;
    private final AtomicReferenceArray<WorkerStatus> workerStatuses;

    public ParallelHashAggregationOperator(HashAggregationOperator.ParallelConfig config, HashAggregationOperator operator) {
        DriverContext mainDriverContext = operator.driverContext;
        final int numWorkers = Math.clamp(config.numWorkers(), 1, MAX_WORKERS);
        this.pagesPerWorker = Math.max(1, config.pagesPerWorker());
        this.driverContext = mainDriverContext;
        this.executor = config.executor();
        this.inputCapacity = numWorkers * pagesPerWorker;
        this.in = new ExchangeBuffer(inputCapacity);
        this.out = new ExchangeBuffer(numWorkers * pagesPerWorker);
        this.partitionKeysThreshold = config.partitionKeysThreshold();
        this.workers = new Worker[numWorkers];
        this.workerStatuses = new AtomicReferenceArray<>(numWorkers + 1);
        this.partitions = newRegistry(mainDriverContext, operator, numWorkers);
        this.pendingSplits = new AtomicInteger(numWorkers);
        boolean success = false;
        try {
            // TODO: this is serial, but can be done in the background thread if the hash operator has its own
            if (operator.blockHash.numKeys() > 0) {
                long startNanos = System.nanoTime();
                partitions.split(mainDriverContext.breaker(), operator);
                long endNanos = System.nanoTime();
                workerStatuses.set(
                    numWorkers,
                    new WorkerStatus((HashAggregationOperator.Status) operator.status(), 1, endNanos - startNanos, 0, 0, 0, 0)
                );
            }
            for (int w = 0; w < numWorkers; w++) {
                workers[w] = new Worker(w, operator.spawnWorker());
            }
            try {
                for (PartitionedAggregationBlock block : operator.partitionedAggregationBlocks) {
                    admitReceived(block);
                }
            } finally {
                Releasables.close(operator.partitionedAggregationBlocks);
                operator.partitionedAggregationBlocks.clear();
            }
            success = true;
        } finally {
            if (success == false) {
                releaseWorkers();
                Releasables.close(partitions);
            }
        }
    }

    /**
     * The registry, spilling when the query's scope does and every key and state family has a partition codec: a
     * time series hash or an aggregator without one keeps the whole aggregation resident.
     */
    private PartitionedHashAggregations newRegistry(DriverContext mainDriverContext, HashAggregationOperator operator, int numWorkers) {
        var globalBreaker = mainDriverContext.blockFactory().parent().breaker();
        QueryScope scope = mainDriverContext.queryScope();
        boolean codecs = operator.blockHash instanceof PartitionedBlockHash hash
            && hash.supportsPartitionCodec()
            && operator.aggregators.stream().map(GroupingAggregator::aggregatorFunction).allMatch(f -> f.supportsPartitionCodec());
        if (scope == null || scope.spillEnabled() == false || codecs == false) {
            return new PartitionedHashAggregations(globalBreaker);
        }
        HashAggregationOperator decoder = operator.spawnWorker();
        try {
            var registry = new PartitionedHashAggregations(globalBreaker, scope, mainDriverContext, decoder, numWorkers, new Events());
            decoder = null;
            return registry;
        } finally {
            if (decoder != null) {
                decoder.close();
            }
        }
    }

    /**
     * What the registry's writes and restores report, from codec and I/O threads.
     */
    private final class Events implements PartitionedHashAggregations.Events {
        @Override
        public void generationPublished() {
            maybeStartPhaseTwo();
            if (phaseTwo.get() == false) {
                // a parked worker resumes with the input the write's slot held back
                for (Worker worker : workers) {
                    scheduleWorker(worker);
                }
            }
        }

        @Override
        public void restoreLanded() {
            signalProgress();
        }

        @Override
        public void failed(Exception e) {
            if (operatorClosed) {
                return;
            }
            failureCollector.unwrapAndCollect(e);
            in.finish(true);
            out.finish(true);
            splitsDone.onResponse(null);
        }
    }

    /**
     * Admits a received partitioned block to the registry as it arrives; while its write is pending the operator takes
     * no input and the driver waits on it.
     */
    private void admitReceived(PartitionedAggregationBlock block) {
        partitionedBlocksReceived++;
        partitions.addPartitionedBlock(block);
        if (partitions.pendingReceivedWrites() > 0) {
            blocked = new IsBlockedResult(partitions.receivedWritesIdle(), "spilling a received partition");
        }
    }

    @Override
    public boolean needsInput() {
        if (partitions.pendingReceivedWrites() > 0) {
            // a received partition is being written out; the driver waits on it through admitReceived's blocked result
            return false;
        }
        if (partitions.writesBacklogged() && in.size() >= inputCapacity) {
            // the workers are parked on a backlogged write and the input is full: wait for either to move
            blocked = anyOf(in.waitForWriting(), partitions.nextWriteDone(), "waiting for a spilled generation's write");
            return false;
        }
        return true;
    }

    @Override
    public void addInput(Page page) {
        long startNanos = System.nanoTime();
        if (failureCollector.hasFailure()) {
            page.close();
            throw ExceptionsHelper.convertToRuntime(failureCollector.getFailure());
        }
        if (page.getBlockCount() == 1 && page.getBlock(0) instanceof PartitionedAggregationBlock pb) {
            // the registry takes the block's keys and states; the page's close lets the emptied block go
            try {
                admitReceived(pb);
            } finally {
                page.close();
            }
            return;
        }
        page.allowPassingToDifferentDriver();
        in.addPage(page);
        final int pendingPages = in.size();
        // Avoid scheduling too many workers in quick succession to process only a few pages:
        // scale up when the backlog grows by half the pages-per-worker target, and reset the
        // baseline when it shrinks by a full target so scaling can resume.
        if (pendingPages - lastPendingPages >= pagesPerWorker / 2) {
            lastPendingPages = pendingPages;
            final int desiredWorkers = Math.min(Math.floorDiv(pendingPages, pagesPerWorker), workers.length);
            int extraWorkers = desiredWorkers - scheduledOrRunningWorkers.get();
            if (extraWorkers > 0) {
                for (int w = 0; extraWorkers > 0 && w < desiredWorkers; w++) {
                    if (scheduleWorker(workers[w])) {
                        --extraWorkers;
                    }
                }
            }
            final var blockedOnWriting = in.waitForWriting();
            if (blockedOnWriting.listener().isDone() == false) {
                blocked = blockedOnWriting;
                processInputPagesWithMainThread();
            }
        } else if (lastPendingPages - pendingPages >= pagesPerWorker) {
            // Reset after the pending-page decreases by pages-per-worker, allowing to scale up again.
            lastPendingPages = pendingPages;
        }
        addInputNanos += (System.nanoTime() - startNanos);
    }

    private void processInputPagesWithMainThread() {
        final long startNanos = System.nanoTime();
        final int startWorker = Randomness.get().nextInt(workers.length);
        final int halfCurrentSize = Math.ceilDiv(in.size(), 2);
        for (int i = 0; i < workers.length; i++) {
            final int w = (i + startWorker) % workers.length;
            final Worker worker = workers[w];
            if (worker.tryLock()) {
                try {
                    Page page;
                    // a backlogged write parks this thread's share too, as it parks the workers
                    while (in.size() >= halfCurrentSize && partitions.writesBacklogged() == false && (page = in.pollPage()) != null) {
                        addInputInlineRows += page.getPositionCount();
                        worker.processOnePage(page);
                    }
                } finally {
                    worker.unlock();
                }
                break;
            }
        }
        addInputInlineCount++;
        addInputInlineNanos += (System.nanoTime() - startNanos);
    }

    @Override
    public void finish() {
        long nanoStart = System.nanoTime();
        if (hasFailure() || finishCalled) {
            return;
        }
        finishCalled = true;
        in.finish(false);
        for (Worker worker : workers) {
            scheduleWorker(worker);
        }
        helpPhaseOne();
        finishNanos = System.nanoTime() - nanoStart;
    }

    /**
     * The driver thread's share of the end of phase one: drains and splits what the pool has not, unless the writes
     * are backlogged, and sets what the driver waits on: the next write when backlogged, the splits otherwise. Called
     * again from {@link #getOutput()} while phase two has not started, so that a rejecting pool does not stall it.
     */
    private void helpPhaseOne() {
        SubscribableListener<Void> progressBefore;
        synchronized (this) {
            progressBefore = progress;
        }
        for (Worker worker : workers) {
            if (worker.tryLock()) {
                try {
                    if (worker.splitLastInput == false && partitions.writesBacklogged() == false) {
                        Page page;
                        while (partitions.writesBacklogged() == false && (page = in.pollPage()) != null) {
                            worker.processOnePage(page);
                        }
                        if (partitions.writesBacklogged() == false) {
                            worker.splitLastInput();
                        }
                    }
                } finally {
                    worker.unlock();
                }
                // unfortunately, we have to reschedule as we might have taken over the background task turn
                scheduleWorker(worker);
            }
        }
        // either way the driver also wakes on progress: a worker's task the pool rejected is for this thread to finish
        blocked = partitions.writesBacklogged()
            ? anyOf(new IsBlockedResult(partitions.nextWriteDone(), "waiting for a spilled generation's write"), progressBefore)
            : anyOf(new IsBlockedResult(splitsDone, "Waiting for splits"), progressBefore);
    }

    @Override
    public boolean isFinished() {
        if (hasFailure()) {
            throw ExceptionsHelper.convertToRuntime(failureCollector.getFailure());
        }
        return out.isFinished();
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        return out.isFinished() == false;
    }

    @Override
    public Page getOutput() {
        if (failureCollector.hasFailure()) {
            throw ExceptionsHelper.convertToRuntime(failureCollector.getFailure());
        }
        if (phaseTwo.get() == false) {
            if (finishCalled) {
                helpPhaseOne();
            }
            return null;
        }
        Page page = out.pollPage();
        if (page != null) {
            return page;
        }
        tryEmitOnePartitionInline();
        return out.pollPage();
    }

    /**
     * The driver's share of phase two: every worker it can lock is moved, one that waits on a slice is left to the
     * landing and the pool, and the first partition emitted ends the pass. With nothing emitted the driver waits for
     * output or for progress signalled since the pass began, so that a slice landing or a task rejected during the
     * pass is not missed; the pool may reject a parked worker's task, and this thread is then the one to move it.
     */
    private void tryEmitOnePartitionInline() {
        final long startNanos = System.nanoTime();
        SubscribableListener<Void> progressBefore;
        synchronized (this) {
            progressBefore = progress;
        }
        boolean emitted = false;
        for (Worker worker : workers) {
            if (worker.tryLock() == false) {
                continue;
            }
            Worker.Step step;
            try {
                step = worker.advance();
            } finally {
                worker.unlock();
            }
            if (step == null) {
                continue;
            }
            if (step.waitFor() != null) {
                scheduleWorker(worker);
                continue;
            }
            inlineEmitRows += step.rows();
            emitted = true;
            if (partitions.hasUncombinedPartitions()) {
                scheduleWorker(worker);
            }
            break;
        }
        if (emitted == false) {
            blocked = waitForOutputOrProgress(progressBefore);
        }
        inlineEmitCount++;
        inlineEmitNanos += (System.nanoTime() - startNanos);
    }

    private void signalProgress() {
        SubscribableListener<Void> signalled;
        synchronized (this) {
            signalled = progress;
            progress = new SubscribableListener<>();
        }
        signalled.onResponse(null);
    }

    /**
     * Output, or {@code progress} as it stood when the caller started looking: done already when something was
     * signalled meanwhile, so that the driver looks again rather than sleeping past it.
     */
    private IsBlockedResult waitForOutputOrProgress(SubscribableListener<Void> progress) {
        return anyOf(out.waitForReading(), progress);
    }

    /**
     * Whichever completes first: the result, or the signal.
     */
    private static IsBlockedResult anyOf(IsBlockedResult result, SubscribableListener<Void> signal) {
        return anyOf(result, signal, result.reason());
    }

    private static IsBlockedResult anyOf(IsBlockedResult result, SubscribableListener<Void> signal, String reason) {
        if (result.listener().isDone() || signal.isDone()) {
            return NOT_BLOCKED;
        }
        SubscribableListener<Void> either = new SubscribableListener<>();
        result.listener().addListener(ActionListener.running(() -> either.onResponse(null)));
        signal.addListener(ActionListener.running(() -> either.onResponse(null)));
        return new IsBlockedResult(either, reason);
    }

    /**
     * Phase two starts once every worker's last split is admitted and every generation's write is published; both
     * counters are checked after either reaches zero, so the last of the two starts it.
     */
    private void maybeStartPhaseTwo() {
        if (pendingSplits.get() == 0 && partitions.pendingWrites() == 0 && phaseTwo.compareAndSet(false, true)) {
            splitsDone.onResponse(null);
            for (Worker worker : workers) {
                scheduleWorker(worker);
            }
        }
    }

    @Override
    public IsBlockedResult isBlocked() {
        return blocked;
    }

    @Override
    public void close() {
        if (operatorClosed == false) {
            operatorClosed = true;
            in.finish(true);
            out.finish(true);
            releaseWorkers();
            partitions.close();
        }
    }

    boolean hasFailure() {
        return failureCollector.hasFailure();
    }

    private void releaseWorkers() {
        for (Worker worker : workers) {
            if (worker != null) {
                worker.close();
            }
        }
    }

    private boolean scheduleWorker(@Nullable Worker worker) {
        // a generation published while the constructor's initial split runs sees no workers yet
        if (worker != null && worker.scheduled.compareAndSet(false, true)) {
            scheduledOrRunningWorkers.incrementAndGet();
            executor.execute(new WorkerTask(worker));
            return true;
        } else {
            return false;
        }
    }

    final class WorkerTask extends AbstractRunnable {
        private final Worker worker;

        WorkerTask(Worker worker) {
            this.worker = worker;
            worker.runsCount.incrementAndGet();
        }

        @Override
        protected void doRun() {
            worker.scheduled.set(false);
            if (phaseTwo.get() == false) {
                processInputPages();
            }
            if (phaseTwo.get()) {
                emitPartitions();
            }
        }

        private void processInputPages() {
            for (;;) {
                if (worker.tryLock() == false) {
                    return;
                }
                try {
                    if (worker.splitLastInput || partitions.writesBacklogged()) {
                        // a backlogged write parks the worker; the write's completion reschedules it
                        return;
                    }
                    driverContext.meteredCpu(() -> {
                        Page p;
                        while (partitions.writesBacklogged() == false && (p = in.pollPage()) != null) {
                            worker.processOnePage(p);
                        }
                        if (in.isFinished() && partitions.writesBacklogged() == false) {
                            worker.splitLastInput();
                        }
                        worker.updateStatus();
                    });
                } finally {
                    // Outside the measurement: once the lock is released the driver thread can close the worker, which
                    // removes its async action and can let the driver publish its final status.
                    worker.unlock();
                }
                if (in.noMoreInputs() == false) {
                    return;
                }
            }
        }

        private void emitPartitions() {
            while (operatorClosed == false && hasFailure() == false) {
                if (worker.tryLock() == false) {
                    return;
                }
                Worker.Step step;
                try {
                    if (worker.emitter.inProgress() == false) {
                        SubscribableListener<Void> outListener = out.waitForWriting().listener();
                        if (outListener.isDone() == false) {
                            outListener.addListener(ActionListener.running(() -> scheduleWorker(worker)));
                            return;
                        }
                    }
                    // TODO: we should check for every page
                    step = driverContext.<Worker.Step, RuntimeException>meteredCpu(worker::advance);
                } finally {
                    // Outside the measurement, as in processInputPages.
                    worker.unlock();
                }
                if (step == null) {
                    return;
                }
                if (step.waitFor() != null) {
                    // the restore in flight reschedules this worker when it lands
                    step.waitFor().listener().addListener(ActionListener.running(() -> scheduleWorker(worker)));
                    return;
                }
            }
        }

        @Override
        public void onAfter() {
            scheduledOrRunningWorkers.decrementAndGet();
            if (operatorClosed) {
                // Closing a worker removes its async action, which counts this measurement first.
                driverContext.meteredCpu(() -> releaseWorkers());
            }
        }

        @Override
        public void onRejection(Exception e) {
            // ignore rejection - handle by the driver thread, but we should stop schedule
            worker.scheduled.set(false);
            signalProgress();
        }

        @Override
        public void onFailure(Exception e) {
            failureCollector.unwrapAndCollect(e);
            in.finish(true);
            out.finish(true);
            splitsDone.onResponse(null);
        }
    }

    class Worker implements Releasable {
        private final int workerIndex;
        private final HashAggregationOperator op;
        private final PartitionedHashAggregations.Combiner emitter;
        private final ReentrantLock lock = new ReentrantLock();
        private boolean initialized = false;
        private boolean splitLastInput = false;
        private boolean closed = false;
        private final AtomicLong runsCount = new AtomicLong();
        final AtomicBoolean scheduled = new AtomicBoolean();

        long splitCount;
        long splitNanos;
        long rowsEmitted;
        long emitNanos;
        long emitCount;

        Worker(int workerIndex, HashAggregationOperator op) {
            this.workerIndex = workerIndex;
            this.op = op;
            partitions.mustIncRef();
            this.emitter = partitions.newCombiner(op);
        }

        void processOnePage(Page page) {
            if (initialized == false) {
                try {
                    if (partitionKeysThreshold < Integer.MAX_VALUE) {
                        op.blockHash.ensureCapacity(partitionKeysThreshold);
                    }
                    initialized = true;
                } finally {
                    if (initialized == false) {
                        Releasables.close(page);
                    }
                }
            }
            if (partitions.spilling()) {
                // generations grow with the budget once they go to disk, which bounds their count and metadata
                op.raisePartitioningMemoryThreshold(partitions.spillSplitThreshold());
            }
            op.addInput(page);
            if (op.partitioningThresholdReached()) {
                split();
            }
        }

        private void split() {
            if (operatorClosed || hasFailure()) {
                return;
            }
            long startNanos = System.nanoTime();
            partitions.split(op.driverContext.breaker(), op);
            splitNanos += (System.nanoTime() - startNanos);
            splitCount++;
            updateStatus();
        }

        void splitLastInput() {
            assert splitLastInput == false;
            splitLastInput = true;
            split();
            if (pendingSplits.decrementAndGet() == 0) {
                maybeStartPhaseTwo();
            }
        }

        /**
         * The outcome of one {@link #advance()}: the rows emitted, or the restore the partition waits on.
         */
        record Step(long rows, @Nullable IsBlockedResult waitFor) {}

        /**
         * One step of phase two, under the worker's lock: claims a partition when none is in progress, combines the
         * generations whose slices are at hand and emits the partition once every generation is combined. Returns null
         * when no partition is left to claim.
         */
        @Nullable
        Step advance() {
            if (emitter.inProgress() == false) {
                int p = partitions.claimPartition();
                if (p == PartitionedHashAggregations.NO_MORE_PARTITION) {
                    return null;
                }
                emitter.begin(p);
            }
            SpillCompletion<SliceRunReader.Slice> pending = emitter.advance();
            if (pending != null) {
                return new Step(0, pending.blocked("restoring a spilled partition"));
            }
            // TODO: we should respect the exchange buffer for every output page not partition level
            long rows = 0;
            long startNanos = System.nanoTime();
            op.emit();
            Page page;
            while ((page = op.getOutput()) != null) {
                rows += page.getPositionCount();
                page.allowPassingToDifferentDriver();
                out.addPage(page);
            }
            rowsEmitted += rows;
            emitNanos += (System.nanoTime() - startNanos);
            emitCount++;
            updateStatus();
            if (partitions.completePartition()) {
                out.finish(false);
            }
            return new Step(rows, null);
        }

        void updateStatus() {
            var status = new WorkerStatus(
                (HashAggregationOperator.Status) op.status(),
                splitCount,
                splitNanos,
                rowsEmitted,
                emitNanos,
                emitCount,
                runsCount.get()
            );
            workerStatuses.set(workerIndex, status);
        }

        @Override
        public void close() {
            // Intentionally keep the lock held permanently to prevent any further operations on this closed worker.
            if (lock.tryLock()) {
                if (closed == false) {
                    closed = true;
                    updateStatus();
                    emitter.close();
                    op.close();
                    partitions.decRef();
                }
            }
        }

        boolean tryLock() {
            if (lock.isHeldByCurrentThread()) {
                // a task run inline by its own thread, which a direct executor does, would nest inside the frame that
                // holds the lock; that frame carries on with the worker's state, so the nested task has nothing to do
                return false;
            }
            if (lock.tryLock()) {
                if (closed) {
                    lock.unlock();
                    return false;
                }
                return true;
            }
            return false;
        }

        void unlock() {
            lock.unlock();
        }
    }

    record WorkerStatus(
        HashAggregationOperator.Status opStatus,
        long splitCount,
        long splitNanos,
        long rowsEmitted,
        long emitNanos,
        long emitCount,
        long runsCount
    ) {}

    @Override
    public Status status() {
        long hashNanos = 0;
        long aggregationNanos = 0;
        int pagesProcessed = 0;
        long rowsReceived = 0;
        long rowsEmitted = 0;
        long emitNanos = 0;
        long emitCount = 0;
        long splitCount = 0;
        long splitNanos = 0;
        long workerTasks = 0;
        List<Status.ExtraStatus> extraFields = new ArrayList<>();
        for (int w = 0; w < workerStatuses.length(); w++) {
            WorkerStatus ws = workerStatuses.get(w);
            if (ws == null) {
                continue;
            }
            hashNanos += ws.opStatus.hashNanos();
            aggregationNanos += ws.opStatus.aggregationNanos();
            pagesProcessed += ws.opStatus.pagesProcessed();
            rowsReceived += ws.opStatus.rowsReceived();
            rowsEmitted += ws.rowsEmitted;
            emitNanos += ws.emitNanos;
            emitCount += ws.emitCount;
            splitCount += ws.splitCount;
            splitNanos += ws.splitNanos;
            workerTasks += ws.runsCount;
            extraFields.addAll(ws.opStatus.extraFields());
        }
        var partitioningStatus = new PartitioningStatus(
            addInputNanos,
            addInputInlineCount,
            addInputInlineRows,
            addInputInlineNanos,
            partitionedBlocksReceived,
            finishNanos,
            splitCount,
            splitNanos,
            inlineEmitCount,
            inlineEmitRows,
            inlineEmitNanos,
            workerTasks,
            partitions.spillStatus()
        );
        return new HashAggregationOperator.Status(
            hashNanos,
            aggregationNanos,
            pagesProcessed,
            rowsReceived,
            rowsEmitted,
            emitNanos,
            emitCount,
            CollectionUtils.appendToCopy(extraFields, partitioningStatus)
        );
    }

    public static class PartitioningStatus extends Status.ExtraStatus {
        private static final TransportVersion PARTITIONS_RECEIVED = TransportVersion.fromName("esql_parallel_aggs_partitions_received");

        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Status.ExtraStatus.class,
            "parallel_hashagg_extra_fields",
            PartitioningStatus::new
        );
        private static final ProfileSchema<PartitioningStatus> PARTITIONING_SCHEMA = SpillStatus.addTo(
            ProfileSchema.of(PartitioningStatus.class, (builder, status, params) -> builder)
        )
            .required("add_input_nanos", ProfileSchema.integer())
            .optional("add_input_time", ProfileSchema.string())
            .required("add_input_inline_count", ProfileSchema.integer())
            .required("add_input_inline_rows", ProfileSchema.integer())
            .required("add_input_inline_nanos", ProfileSchema.integer())
            .optional("add_input_inline_time", ProfileSchema.string())
            .required("partitioned_blocks_received", ProfileSchema.integer())
            .required("finish_nanos", ProfileSchema.integer())
            .optional("finish_time", ProfileSchema.string())
            .required("split_count", ProfileSchema.integer())
            .required("split_nanos", ProfileSchema.integer())
            .optional("split_time", ProfileSchema.string())
            .required("inline_emit_count", ProfileSchema.integer())
            .required("inline_emit_rows", ProfileSchema.integer())
            .required("inline_emit_nanos", ProfileSchema.integer())
            .optional("inline_emit_time", ProfileSchema.string())
            .required("worker_tasks", ProfileSchema.integer())
            .build();
        public static final ProfileSchema<PartitioningStatus> PROFILE_SCHEMA = ProfileSchema.fragment(
            PartitioningStatus.class,
            (builder, status, params) -> {
                status.writeProfile(builder);
                return builder;
            }
        ).required("partitioning", PARTITIONING_SCHEMA.schema()).build();

        static Map<String, Object> partitioningSchema() {
            return PARTITIONING_SCHEMA.schema();
        }

        private final long addInputNanos;
        private final long addInputInlineCount;
        private final long addInputInlineRows;
        private final long addInputInlineNanos;
        private final int partitionedBlocksReceived;
        private final long finishNanos;
        private final long splitCount;
        private final long splitNanos;
        private final long inlineEmitCount;
        private final long inlineEmitRows;
        private final long inlineEmitNanos;
        private final long workerTasks;
        /** The spill counters of the registry: generations written as runs, their bytes, slices read back. */
        private final SpillStatus spill;

        public PartitioningStatus(
            long addInputNanos,
            long addInputInlineCount,
            long addInputInlineRows,
            long addInputInlineNanos,
            int partitionedBlocksReceived,
            long finishNanos,
            long splitCount,
            long splitNanos,
            long inlineEmitCount,
            long inlineEmitRows,
            long inlineEmitNanos,
            long workerTasks,
            SpillStatus spill
        ) {
            this.addInputNanos = addInputNanos;
            this.addInputInlineCount = addInputInlineCount;
            this.addInputInlineRows = addInputInlineRows;
            this.addInputInlineNanos = addInputInlineNanos;
            this.partitionedBlocksReceived = partitionedBlocksReceived;
            this.finishNanos = finishNanos;
            this.splitCount = splitCount;
            this.splitNanos = splitNanos;
            this.inlineEmitCount = inlineEmitCount;
            this.inlineEmitRows = inlineEmitRows;
            this.inlineEmitNanos = inlineEmitNanos;
            this.workerTasks = workerTasks;
            this.spill = spill;
        }

        PartitioningStatus(StreamInput in) throws IOException {
            this(
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.getTransportVersion().supports(PARTITIONS_RECEIVED) ? in.readVInt() : 0,
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                SpillStatus.readFrom(in)
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(addInputNanos);
            out.writeVLong(addInputInlineCount);
            out.writeVLong(addInputInlineRows);
            out.writeVLong(addInputInlineNanos);
            if (out.getTransportVersion().supports(PARTITIONS_RECEIVED)) {
                out.writeVInt(partitionedBlocksReceived);
            }
            out.writeVLong(finishNanos);
            out.writeVLong(splitCount);
            out.writeVLong(splitNanos);
            out.writeVLong(inlineEmitCount);
            out.writeVLong(inlineEmitRows);
            out.writeVLong(inlineEmitNanos);
            out.writeVLong(workerTasks);
            spill.writeTo(out);
        }

        public int partitionedBlocksReceived() {
            return partitionedBlocksReceived;
        }

        public SpillStatus spill() {
            return spill;
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        @Override
        protected void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            PROFILE_SCHEMA.write(builder, this, params);
        }

        private void writeProfile(XContentBuilder builder) throws IOException {
            builder.startObject("partitioning");
            builder.field("add_input_nanos", addInputNanos);
            if (builder.humanReadable()) {
                builder.field("add_input_time", TimeValue.timeValueNanos(addInputNanos));
            }
            builder.field("add_input_inline_count", addInputInlineCount);
            builder.field("add_input_inline_rows", addInputInlineRows);
            builder.field("add_input_inline_nanos", addInputInlineNanos);
            if (builder.humanReadable()) {
                builder.field("add_input_inline_time", TimeValue.timeValueNanos(addInputInlineNanos));
            }
            builder.field("partitioned_blocks_received", partitionedBlocksReceived);
            builder.field("finish_nanos", finishNanos);
            if (builder.humanReadable()) {
                builder.field("finish_time", TimeValue.timeValueNanos(finishNanos));
            }
            builder.field("split_count", splitCount);
            builder.field("split_nanos", splitNanos);
            if (builder.humanReadable()) {
                builder.field("split_time", TimeValue.timeValueNanos(splitNanos));
            }
            builder.field("inline_emit_count", inlineEmitCount);
            builder.field("inline_emit_rows", inlineEmitRows);
            builder.field("inline_emit_nanos", inlineEmitNanos);
            if (builder.humanReadable()) {
                builder.field("inline_emit_time", TimeValue.timeValueNanos(inlineEmitNanos));
            }
            builder.field("worker_tasks", workerTasks);
            spill.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PartitioningStatus other = (PartitioningStatus) o;
            return addInputNanos == other.addInputNanos
                && addInputInlineCount == other.addInputInlineCount
                && addInputInlineRows == other.addInputInlineRows
                && addInputInlineNanos == other.addInputInlineNanos
                && partitionedBlocksReceived == other.partitionedBlocksReceived
                && finishNanos == other.finishNanos
                && splitCount == other.splitCount
                && splitNanos == other.splitNanos
                && inlineEmitCount == other.inlineEmitCount
                && inlineEmitRows == other.inlineEmitRows
                && inlineEmitNanos == other.inlineEmitNanos
                && workerTasks == other.workerTasks
                && spill.equals(other.spill);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                addInputNanos,
                addInputInlineCount,
                addInputInlineRows,
                addInputInlineNanos,
                partitionedBlocksReceived,
                finishNanos,
                splitCount,
                splitNanos,
                inlineEmitCount,
                inlineEmitRows,
                inlineEmitNanos,
                workerTasks,
                spill
            );
        }
    }

    @Override
    public String toString() {
        // TODO: include hash and aggregations
        return getClass().getSimpleName();
    }
}
