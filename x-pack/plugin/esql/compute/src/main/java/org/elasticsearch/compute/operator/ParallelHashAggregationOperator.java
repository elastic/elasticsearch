/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.exchange.ExchangeBuffer;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
 * private {@link HashAggregationOperator}. Whenever a worker's state grows beyond
 * {@link #partitionKeysThreshold} keys, the worker splits that state into partitions and hands them
 * off to a shared registry. After the last input page has been processed, every worker performs
 * one final split.
 *
 * <p>The second phase begins once all workers have completed their final split. Workers claim
 * partitions from the registry, merge the slices of that partition produced by all the splits,
 * and emit the resulting pages to an output exchange, where the driver picks them up.
 *
 * <p>Known limitations: the exchanges are sized and throttled by page count rather than by bytes,
 * which causes unnecessary yields and reschedules. Increasing {@link #pagesPerWorker} mitigates
 * this at the cost of buffering more memory. The emit phase now can dump the entire partition
 * to the exchange instead checking the capacity for every emit.
 */
public final class ParallelHashAggregationOperator implements Operator {
    public static final int PAGE_PER_WORKER = 10;
    public static final int MAX_WORKERS = 32;

    private final Executor executor;
    private final int partitionKeysThreshold;
    private final int pagesPerWorker;

    private final Worker[] workers;
    private final AtomicInteger scheduledOrRunningWorkers = new AtomicInteger();

    private final ExchangeBuffer in;
    private int lastPendingPages = 0;
    private boolean finishCalled = false;

    private final AtomicInteger pendingSplits;
    private final SubscribableListener<Void> splitsDone = new SubscribableListener<>();

    private final ExchangeBuffer out;
    private IsBlockedResult blocked = NOT_BLOCKED;

    private final PartitionedHashAggregations partitions;
    private final FailureCollector failureCollector = new FailureCollector();

    private volatile boolean operatorClosed = false;

    private long addInputNanos;
    private long addInputInlineCount;
    private long addInputInlineRows;
    private long addInputInlineNanos;
    private long finishNanos;
    private long inlineEmitCount;
    private long inlineEmitRows;
    private long inlineEmitNanos;
    private final AtomicReferenceArray<WorkerStatus> workerStatuses;

    public ParallelHashAggregationOperator(HashAggregationOperator.ParallelConfig config, HashAggregationOperator operator) {
        DriverContext mainDriverContext = operator.driverContext;
        final int numWorkers = Math.clamp(config.numWorkers(), 1, MAX_WORKERS);
        this.pagesPerWorker = Math.max(1, config.pagesPerWorker());
        this.executor = config.executor();
        this.in = new ExchangeBuffer(numWorkers * pagesPerWorker);
        this.out = new ExchangeBuffer(numWorkers * pagesPerWorker);
        this.partitionKeysThreshold = config.partitionKeysThreshold();
        this.workers = new Worker[numWorkers];
        this.workerStatuses = new AtomicReferenceArray<>(numWorkers + 1);
        this.partitions = new PartitionedHashAggregations(mainDriverContext.blockFactory().parent().breaker());
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
            success = true;
        } finally {
            if (success == false) {
                releaseWorkers();
                Releasables.close(partitions);
            }
        }
    }

    @Override
    public boolean needsInput() {
        // can accept input anytime
        return true;
    }

    @Override
    public void addInput(Page page) {
        long startNanos = System.nanoTime();
        if (failureCollector.hasFailure()) {
            page.close();
            throw ExceptionsHelper.convertToRuntime(failureCollector.getFailure());
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
                    while (in.size() >= halfCurrentSize && (page = in.pollPage()) != null) {
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
        for (Worker worker : workers) {
            if (worker.tryLock()) {
                try {
                    if (worker.splitLastInput == false) {
                        Page page;
                        while ((page = in.pollPage()) != null) {
                            worker.processOnePage(page);
                        }
                        worker.splitLastInput();
                    }
                } finally {
                    worker.unlock();
                }
                // unfortunately, we have to reschedule as we might have taken over the background task turn
                scheduleWorker(worker);
            }
        }
        blocked = new IsBlockedResult(splitsDone, "Waiting for splits");
        finishNanos = System.nanoTime() - nanoStart;
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
        if (pendingSplits.get() > 0) {
            return null;
        }
        Page page = out.pollPage();
        if (page != null) {
            return page;
        }
        tryEmitOnePartitionInline();
        return out.pollPage();
    }

    private void tryEmitOnePartitionInline() {
        final long startNanos = System.nanoTime();
        boolean emitted = false;
        for (Worker worker : workers) {
            if (worker.tryLock()) {
                try {
                    int p = partitions.claimPartition();
                    if (p != PartitionedHashAggregations.NO_MORE_PARTITION) {
                        inlineEmitRows += worker.emitOnePartition(p);
                        emitted = true;
                    }
                } finally {
                    worker.unlock();
                }
                if (partitions.hasUncombinedPartitions()) {
                    scheduleWorker(worker);
                }
                break;
            }
        }
        if (emitted == false) {
            blocked = out.waitForReading();
        }
        inlineEmitCount++;
        inlineEmitNanos += (System.nanoTime() - startNanos);
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

    private boolean scheduleWorker(Worker worker) {
        if (worker.scheduled.compareAndSet(false, true)) {
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
            final int pending = pendingSplits.get();
            if (pending > 0) {
                processInputPages();
            }
            if (pending == 0 || pendingSplits.get() == 0) {
                emitPartitions();
            }
        }

        private void processInputPages() {
            for (;;) {
                if (worker.tryLock() == false) {
                    return;
                }
                try {
                    if (worker.splitLastInput) {
                        return;
                    }
                    Page p;
                    while ((p = in.pollPage()) != null) {
                        worker.processOnePage(p);
                    }
                    if (in.isFinished()) {
                        worker.splitLastInput();
                    }
                    worker.updateStatus();
                } finally {
                    worker.unlock();
                }
                if (in.noMoreInputs() == false) {
                    return;
                }
            }
        }

        private void emitPartitions() {
            while (operatorClosed == false && hasFailure() == false) {
                SubscribableListener<Void> outListener = out.waitForWriting().listener();
                if (outListener.isDone() == false) {
                    outListener.addListener(ActionListener.running(() -> scheduleWorker(worker)));
                    return;
                }
                if (worker.tryLock() == false) {
                    return;
                }
                try {
                    final int p = partitions.claimPartition();
                    if (p == PartitionedHashAggregations.NO_MORE_PARTITION) {
                        return;
                    }
                    // TODO: we should check for every page
                    worker.emitOnePartition(p);
                } finally {
                    worker.unlock();
                }
            }
        }

        @Override
        public void onAfter() {
            scheduledOrRunningWorkers.decrementAndGet();
            if (operatorClosed) {
                releaseWorkers();
            }
        }

        @Override
        public void onRejection(Exception e) {
            // ignore rejection - handle by the driver thread, but we should stop schedule
            worker.scheduled.set(false);
            onAfter();
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
                initialized = true;
                op.blockHash.ensureCapacity(partitionKeysThreshold);
            }
            op.addInput(page);
            if (op.blockHash.numKeys() >= partitionKeysThreshold) {
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
                splitsDone.onResponse(null);
                for (Worker worker : workers) {
                    scheduleWorker(worker);
                }
            }
        }

        long emitOnePartition(int p) {
            // TODO: we should respect the exchange buffer for every output page not partition level
            long rows = 0;
            if (emitter.combine(p)) {
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
            }
            if (partitions.completePartition()) {
                out.finish(false);
            }
            return rows;
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
            finishNanos,
            splitCount,
            splitNanos,
            inlineEmitCount,
            inlineEmitRows,
            inlineEmitNanos,
            workerTasks
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
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Status.ExtraStatus.class,
            "parallel_hashagg_extra_fields",
            PartitioningStatus::new
        );

        private final long addInputNanos;
        private final long addInputInlineCount;
        private final long addInputInlineRows;
        private final long addInputInlineNanos;
        private final long finishNanos;
        private final long splitCount;
        private final long splitNanos;
        private final long inlineEmitCount;
        private final long inlineEmitRows;
        private final long inlineEmitNanos;
        private final long workerTasks;

        public PartitioningStatus(
            long addInputNanos,
            long addInputInlineCount,
            long addInputInlineRows,
            long addInputInlineNanos,
            long finishNanos,
            long splitCount,
            long splitNanos,
            long inlineEmitCount,
            long inlineEmitRows,
            long inlineEmitNanos,
            long workerTasks
        ) {
            this.addInputNanos = addInputNanos;
            this.addInputInlineCount = addInputInlineCount;
            this.addInputInlineRows = addInputInlineRows;
            this.addInputInlineNanos = addInputInlineNanos;
            this.finishNanos = finishNanos;
            this.splitCount = splitCount;
            this.splitNanos = splitNanos;
            this.inlineEmitCount = inlineEmitCount;
            this.inlineEmitRows = inlineEmitRows;
            this.inlineEmitNanos = inlineEmitNanos;
            this.workerTasks = workerTasks;
        }

        PartitioningStatus(StreamInput in) throws IOException {
            this(
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong()
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(addInputNanos);
            out.writeVLong(addInputInlineCount);
            out.writeVLong(addInputInlineRows);
            out.writeVLong(addInputInlineNanos);
            out.writeVLong(finishNanos);
            out.writeVLong(splitCount);
            out.writeVLong(splitNanos);
            out.writeVLong(inlineEmitCount);
            out.writeVLong(inlineEmitRows);
            out.writeVLong(inlineEmitNanos);
            out.writeVLong(workerTasks);
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        @Override
        protected void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
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
                && finishNanos == other.finishNanos
                && splitCount == other.splitCount
                && splitNanos == other.splitNanos
                && inlineEmitCount == other.inlineEmitCount
                && inlineEmitRows == other.inlineEmitRows
                && inlineEmitNanos == other.inlineEmitNanos
                && workerTasks == other.workerTasks;
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                addInputNanos,
                addInputInlineCount,
                addInputInlineRows,
                addInputInlineNanos,
                finishNanos,
                splitCount,
                splitNanos,
                inlineEmitCount,
                inlineEmitRows,
                inlineEmitNanos,
                workerTasks
            );
        }
    }

    @Override
    public String toString() {
        // TODO: include hash and aggregations
        return getClass().getSimpleName();
    }
}
