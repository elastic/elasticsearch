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
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

/**
 * A parallel version of {@link HashAggregationOperator} that partitions the aggregation state so
 * many workers can build and combine it concurrently.
 *
 * <p>Aggregation runs in two phases. In the first phase, incoming pages are queued on an input
 * exchange and workers compete to drain it, each aggregating the pages it processes into its own
 * private {@link HashAggregationOperator}. Whenever a worker's state grows beyond
 * {@link #PARTITION_THRESHOLD} keys, the worker splits that state into partitions and hands them
 * off to a shared registry. After the last input page has been processed, every worker performs
 * one final split.
 *
 * <p>The second phase begins once all workers have completed their final split. Workers claim
 * partitions from the registry, merge the slices of that partition produced by all the splits,
 * and emit the resulting pages to an output exchange, where the driver picks them up.
 *
 * <p>Known limitation: the exchanges are sized and throttled by page count rather than by bytes,
 * which causes unnecessary yields and reschedules. Increasing {@link #PAGE_PER_WORKER} mitigates
 * this at the cost of buffering more memory.
 */
public final class ParallelHashAggregationOperator implements Operator {
    public static final int PARTITION_THRESHOLD = 256 * 1500;
    public static final int MAX_WORKERS = 32;
    private static final int PAGE_PER_WORKER = 10;

    private final DriverContext mainDriverContext;
    private final Executor executor;
    private final Worker[] workers;
    private final AtomicInteger scheduledWorkers = new AtomicInteger();

    private final ExchangeBuffer in;
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
    private long inlineEmitNanos;
    private final WorkerStatus[] workerStatuses;

    public ParallelHashAggregationOperator(
        DriverContext driverContext,
        HashAggregationOperator operator,
        int maxWorkers,
        Function<DriverContext, HashAggregationOperator> fork
    ) {
        this.mainDriverContext = driverContext;
        this.executor = driverContext.executor;
        this.workers = new Worker[maxWorkers];
        this.workerStatuses = new WorkerStatus[maxWorkers];
        this.partitions = new PartitionedHashAggregations();
        this.pendingSplits = new AtomicInteger(maxWorkers);
        boolean success = false;
        try {
            for (int w = 0; w < maxWorkers; w++) {
                workers[w] = createWorker(w, driverContext, fork);
            }
            // TODO: this is serial, but can be done in the background thread
            if (operator.blockHash.numKeys() > 0) {
                partitions.split(operator);
            }
            success = true;
        } finally {
            if (success == false) {
                releaseWorkers();
                Releasables.close(partitions);
            }
        }
        this.in = new ExchangeBuffer(maxWorkers * PAGE_PER_WORKER);
        this.out = new ExchangeBuffer(maxWorkers * PAGE_PER_WORKER);
    }

    @Override
    public boolean needsInput() {
        // always true, throttle with blocked
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
        final int desiredWorkers = Math.min(Math.ceilDiv(in.size(), PAGE_PER_WORKER), workers.length);
        if (scheduledWorkers.getAndUpdate(n -> n < desiredWorkers ? n + 1 : n) < desiredWorkers) {
            executor.execute(new WorkerTask() {
                @Override
                public void onAfter() {
                    scheduledWorkers.decrementAndGet();
                    super.onAfter();
                }

                @Override
                protected void doRun() {
                    for (Worker worker : workers) {
                        if (worker.tryLock()) {
                            try {
                                runWorker(worker);
                            } finally {
                                worker.unlock();
                            }
                            return;
                        }
                    }
                }
            });
        }
        final var blockedOnWriting = in.waitForWriting();
        if (blockedOnWriting.listener().isDone() == false) {
            blocked = blockedOnWriting;
            processInputPagesWithMainThread();
        }
        addInputNanos += (System.nanoTime() - startNanos);
    }

    private void processInputPagesWithMainThread() {
        // use a random worker to avoid splitting on the driver thread
        final long startNanos = System.nanoTime();
        final int startWorker = Randomness.get().nextInt(workers.length);
        for (int i = 0; i < workers.length; i++) {
            final int w = (i + startWorker) % workers.length;
            final Worker worker = workers[w];
            if (worker.tryLock()) {
                final int halfCurrentSize = Math.ceilDiv(in.size(), 2);
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
            executor.execute(new WorkerTask() {
                @Override
                protected void doRun() {
                    runWorker(worker);
                }
            });
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
                        worker.emitOnePartition(p);
                        emitted = true;
                    }
                } finally {
                    worker.unlock();
                }
                if (partitions.hasUncombinedPartitions()) {
                    executor.execute(new WorkerTask() {
                        @Override
                        protected void doRun() {
                            emitPartitions(worker);
                        }
                    });
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

    private Worker createWorker(int workerIndex, DriverContext initDriverContext, Function<DriverContext, HashAggregationOperator> fork) {
        DriverContext dc = initDriverContext.forkDriverContext();
        HashAggregationOperator op = null;
        boolean success = false;
        try {
            op = fork.apply(dc);
            Worker worker = new Worker(workerIndex, op);
            success = true;
            return worker;
        } finally {
            if (success == false) {
                Releasables.close(op);
                initDriverContext.releaseChildBlockFactory(dc.blockFactory());
            }
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

    abstract class WorkerTask extends AbstractRunnable {
        void runWorker(Worker worker) {
            processInputPages(worker);
            if (pendingSplits.get() == 0) {
                emitPartitions(worker);
            }
        }

        void processInputPages(Worker worker) {
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
                        return;
                    }
                } finally {
                    worker.unlock();
                }
                // need to re-check after releasing the lock
                if (in.waitForReading().listener().isDone() == false) {
                    return;
                }
            }
        }

        void emitPartitions(Worker worker) {
            while (operatorClosed == false && hasFailure() == false) {
                SubscribableListener<Void> outListener = out.waitForWriting().listener();
                if (outListener.isDone() == false) {
                    outListener.addListener(ActionListener.running(() -> executor.execute(new WorkerTask() {
                        @Override
                        protected void doRun() {
                            emitPartitions(worker);
                        }
                    })));
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
                    worker.emitOnePartition(p);
                } finally {
                    worker.unlock();
                }
            }
        }

        @Override
        public void onAfter() {
            if (operatorClosed) {
                releaseWorkers();
            }
        }

        @Override
        public void onRejection(Exception e) {
            // ignore rejection - handle by the driver thread
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

        long splitCount;
        long splitNanos;
        long emitNanos;

        Worker(int workerIndex, HashAggregationOperator op) {
            this.workerIndex = workerIndex;
            this.op = op;
            partitions.mustIncRef();
            mainDriverContext.addAsyncAction();
            this.emitter = partitions.newCombiner(op);
        }

        void processOnePage(Page page) {
            if (initialized == false) {
                initialized = true;
                op.blockHash.ensureCapacity(PARTITION_THRESHOLD);
            }
            op.addInput(page);
            if (op.blockHash.numKeys() >= PARTITION_THRESHOLD) {
                split();
            }
        }

        private void split() {
            long startNanos = System.nanoTime();
            partitions.split(op);
            splitNanos += (System.nanoTime() - startNanos);
            splitCount++;
        }

        void splitLastInput() {
            assert splitLastInput == false;
            splitLastInput = true;
            split();
            if (pendingSplits.decrementAndGet() == 0) {
                splitsDone.onResponse(null);
                for (Worker worker : workers) {
                    executor.execute(new WorkerTask() {
                        @Override
                        protected void doRun() {
                            emitPartitions(worker);
                        }
                    });
                }
            }
        }

        void emitOnePartition(int p) {
            if (emitter.combine(p)) {
                long startNanos = System.nanoTime();
                op.emit();
                Page page;
                while ((page = op.getOutput()) != null) {
                    page.allowPassingToDifferentDriver();
                    out.addPage(page);
                }
                emitNanos += (System.nanoTime() - startNanos);
            }
            if (partitions.completePartition()) {
                out.finish(false);
            }
        }

        WorkerStatus status() {
            return new WorkerStatus((HashAggregationOperator.Status) op.status(), splitCount, splitNanos, emitNanos);
        }

        @Override
        public void close() {
            if (tryLock()) {
                workerStatuses[workerIndex] = status();
                op.close();
                partitions.decRef();
                mainDriverContext.removeAsyncAction();
                // Intentionally keep the lock held permanently to prevent any further operations on this closed worker.
            }
        }

        boolean tryLock() {
            return lock.tryLock();
        }

        void unlock() {
            lock.unlock();
        }
    }

    record WorkerStatus(HashAggregationOperator.Status opStatus, long splitCount, long splitNanos, long emitNanos) {}

    @Override
    public Status status() {
        for (int w = 0; w < workers.length; w++) {
            final Worker worker = workers[w];
            if (worker.tryLock()) {
                final WorkerStatus ws;
                try {
                    ws = worker.status();
                } finally {
                    worker.unlock();
                }
                workerStatuses[w] = ws;
            }
        }
        long hashNanos = 0;
        long aggregationNanos = 0;
        int pagesProcessed = 0;
        long rowsReceived = 0;
        long rowsEmitted = 0;
        long emitNanos = 0;
        long emitCount = 0;
        long splitCount = 0;
        long splitNanos = 0;
        long workerEmitNanos = 0;
        List<Status.ExtraStatus> extraFields = new ArrayList<>();
        for (WorkerStatus ws : workerStatuses) {
            if (ws == null) {
                continue;
            }
            hashNanos += ws.opStatus.hashNanos();
            aggregationNanos += ws.opStatus.aggregationNanos();
            pagesProcessed += ws.opStatus.pagesProcessed();
            rowsReceived += ws.opStatus.rowsReceived();
            rowsEmitted += ws.opStatus.rowsEmitted();
            emitNanos += ws.opStatus.emitNanos();
            emitCount += ws.opStatus.emitCount();
            splitCount += ws.splitCount;
            splitNanos += ws.splitNanos;
            workerEmitNanos += ws.emitNanos;
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
            workerEmitNanos,
            inlineEmitCount,
            inlineEmitNanos
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
        private final long emitNanos;
        private final long inlineEmitCount;
        private final long inlineEmitNanos;

        public PartitioningStatus(
            long addInputNanos,
            long addInputInlineCount,
            long addInputInlineRows,
            long addInputInlineNanos,
            long finishNanos,
            long splitCount,
            long splitNanos,
            long emitNanos,
            long inlineEmitCount,
            long inlineEmitNanos
        ) {
            this.addInputNanos = addInputNanos;
            this.addInputInlineCount = addInputInlineCount;
            this.addInputInlineRows = addInputInlineRows;
            this.addInputInlineNanos = addInputInlineNanos;
            this.finishNanos = finishNanos;
            this.splitCount = splitCount;
            this.splitNanos = splitNanos;
            this.emitNanos = emitNanos;
            this.inlineEmitCount = inlineEmitCount;
            this.inlineEmitNanos = inlineEmitNanos;
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
            out.writeVLong(emitNanos);
            out.writeVLong(inlineEmitCount);
            out.writeVLong(inlineEmitNanos);
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
            builder.field("emit_nanos", emitNanos);
            if (builder.humanReadable()) {
                builder.field("emit_time", TimeValue.timeValueNanos(emitNanos));
            }
            builder.field("inline_emit_count", inlineEmitCount);
            builder.field("inline_emit_nanos", inlineEmitNanos);
            if (builder.humanReadable()) {
                builder.field("inline_emit_time", TimeValue.timeValueNanos(inlineEmitNanos));
            }
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
                && emitNanos == other.emitNanos
                && inlineEmitCount == other.inlineEmitCount
                && inlineEmitNanos == other.inlineEmitNanos;
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
                emitNanos,
                inlineEmitCount,
                inlineEmitNanos
            );
        }
    }

    @Override
    public String toString() {
        // TODO: include hash and aggregations
        return getClass().getSimpleName();
    }
}
