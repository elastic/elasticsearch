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
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.GroupingAggregatorEvaluationContext;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.exchange.ExchangeBuffer;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

/**
 * Coordinator-side merge operator for partitioned hash aggregation.
 *
 * <p>Receives intermediate {@link Page}s from data nodes — some tagged with a
 * {@link Page#partitionId()} by {@link PartitionedHashAggregationOperator}, others untagged
 * (from shards that never converted to partitioned mode). Produces final aggregated output.
 *
 * <h2>Lifecycle</h2>
 * <ol>
 *   <li><b>Accumulation</b> – untagged pages accumulate in the {@code noneOp}
 *       (INTERMEDIATE-mode aggregators) on the driver thread. Tagged pages are routed
 *       directly to the owning partition worker's {@link ExchangeBuffer}.</li>
 *   <li><b>Finish</b> – the {@code noneOp}'s contents are distributed to worker buffers
 *       via the evaluate-intermediate → split-by-partition-hash → buffer-enqueue primitive;
 *       each worker then merges its buffer into its own FINAL-mode operator.</li>
 *   <li><b>Output</b> – once all workers signal completion, the driver evaluates each
 *       worker operator to final output pages (disjoint, no k-way merge required).</li>
 * </ol>
 *
 * <p>Background workers run on the {@code esql_worker} thread pool using the same
 * {@link PendingTasks} / {@link SubscribableListener} / {@link ExchangeBuffer} machinery
 * as {@link org.elasticsearch.compute.operator.topn.ParallelTopNOperator}.
 */
public class PartitionedHashMergeOperator implements Operator {

    /**
     * Aggregator specification: supplier plus the <em>external</em> intermediate-state channels
     * that appear in pages arriving from the exchange (key columns excluded).
     */
    public record AggregatorSpec(AggregatorFunctionSupplier supplier, List<Integer> channels) {}

    /**
     * Number of logical merge workers launched per operator instance when not otherwise configured.
     * Workers are reactive tasks (not persistent threads): each holds one slot in the
     * {@code esql_worker} pool only while it has pages to drain. Keeping this well below
     * {@code partitionCount} (e.g. 8 vs 32) limits queue pressure on the thread pool.
     */
    public static final int DEFAULT_MERGE_WORKER_COUNT = 8;

    // ---- Builder / Factory ----

    public static class Builder {
        private List<BlockHash.GroupSpec> groupSpecs;
        private List<AggregatorSpec> aggregators;
        private int partitionCount = PartitionedHashAggregationOperator.DEFAULT_PARTITION_COUNT;
        private int workerCount = DEFAULT_MERGE_WORKER_COUNT;
        private int maxPageSize = Operator.TARGET_PAGE_SIZE / Long.SIZE;
        private int aggregationBatchSize = Operator.TARGET_PAGE_SIZE / Long.SIZE;
        private Executor executor;

        public Builder groupSpecs(List<BlockHash.GroupSpec> groupSpecs) {
            this.groupSpecs = List.copyOf(groupSpecs);
            return this;
        }

        public Builder aggregators(List<AggregatorSpec> aggregators) {
            this.aggregators = aggregators;
            return this;
        }

        public Builder partitionCount(int partitionCount) {
            this.partitionCount = partitionCount;
            return this;
        }

        public Builder workerCount(int workerCount) {
            this.workerCount = workerCount;
            return this;
        }

        public Builder maxPageSize(int maxPageSize) {
            this.maxPageSize = maxPageSize;
            return this;
        }

        public Builder aggregationBatchSize(int aggregationBatchSize) {
            this.aggregationBatchSize = aggregationBatchSize;
            return this;
        }

        public Builder executor(Executor executor) {
            this.executor = executor;
            return this;
        }

        public Factory build() {
            return new Factory(this);
        }
    }

    public static class Factory implements OperatorFactory {
        private final List<BlockHash.GroupSpec> internalGroupSpecs;
        private final List<AggregatorSpec> aggregatorSpecs;
        private final List<GroupingAggregator.Factory> noneAggFactories;
        private final List<GroupingAggregator.Factory> workerAggFactories;
        private final int partitionCount;
        private final int workerCount;
        private final int maxPageSize;
        private final int aggregationBatchSize;
        private final Executor executor;

        private Factory(Builder builder) {
            this.internalGroupSpecs = buildInternalGroupSpecs(builder.groupSpecs);
            this.aggregatorSpecs = requireNonNull(builder.aggregators, "aggregators");

            List<GroupingAggregator.Factory> noneFactories = new ArrayList<>(aggregatorSpecs.size());
            List<GroupingAggregator.Factory> workerFactories = new ArrayList<>(aggregatorSpecs.size());
            int nextChannel = internalGroupSpecs.size();
            for (int i = 0; i < aggregatorSpecs.size(); i++) {
                AggregatorSpec spec = aggregatorSpecs.get(i);
                int intermediateBlockCount = spec.supplier().groupingIntermediateStateDesc().size();
                List<Integer> internalChannels = new ArrayList<>(intermediateBlockCount);
                for (int c = 0; c < intermediateBlockCount; c++) {
                    internalChannels.add(nextChannel + c);
                }
                List<Integer> frozenChannels = List.copyOf(internalChannels);
                noneFactories.add(new GroupingAggregator.Factory() {
                    @Override
                    public GroupingAggregator apply(DriverContext driverContext) {
                        return new GroupingAggregator(
                            spec.supplier().groupingAggregator(driverContext, frozenChannels),
                            AggregatorMode.INTERMEDIATE
                        );
                    }

                    @Override
                    public String describe() {
                        return spec.supplier().describe();
                    }
                });
                workerFactories.add(new GroupingAggregator.Factory() {
                    @Override
                    public GroupingAggregator apply(DriverContext driverContext) {
                        return new GroupingAggregator(
                            spec.supplier().groupingAggregator(driverContext, frozenChannels),
                            AggregatorMode.FINAL
                        );
                    }

                    @Override
                    public String describe() {
                        return spec.supplier().describe();
                    }
                });
                nextChannel += intermediateBlockCount;
            }
            this.noneAggFactories = List.copyOf(noneFactories);
            this.workerAggFactories = List.copyOf(workerFactories);
            this.partitionCount = builder.partitionCount;
            this.workerCount = Math.min(builder.workerCount, builder.partitionCount);
            this.maxPageSize = builder.maxPageSize;
            this.aggregationBatchSize = builder.aggregationBatchSize;
            this.executor = builder.executor;
        }

        @Override
        public PartitionedHashMergeOperator get(DriverContext driverContext) {
            return new PartitionedHashMergeOperator(
                internalGroupSpecs,
                noneAggFactories,
                workerAggFactories,
                partitionCount,
                workerCount,
                maxPageSize,
                aggregationBatchSize,
                executor,
                driverContext
            );
        }

        @Override
        public String describe() {
            return "PartitionedHashMergeOperator[partitionCount="
                + partitionCount
                + ", aggs="
                + aggregatorSpecs.stream().map(s -> s.supplier().describe()).collect(joining(", "))
                + "]";
        }
    }

    // ---- Instance fields ----

    private final int partitionCount;
    private final int maxPageSize;
    private final DriverContext driverContext;
    /** Iterator of output pages; non-null while the operator has output to drain. */
    private ReleasableIterator<Page> output;
    private boolean finishCalled;

    private final int workerCount;
    private final Executor executor;
    private final ExchangeBuffer[] workerBuffers;
    /**
     * Guards against multiple concurrent tasks for the same logical worker. Each entry is
     * {@code true} while a task is submitted or running; {@code false} while the worker is
     * parked waiting for data. Set to {@code true} before submission, cleared to {@code false}
     * inside doRun after all partition buffers have been drained and listeners registered.
     */
    private final AtomicBoolean[] workerSubmitted;
    private final FailureCollector failureCollector = new FailureCollector();
    private final SubscribableListener<Void> allWorkersDone = new SubscribableListener<>();
    private final PendingTasks pendingTasks;
    private volatile boolean closed = false;
    private final AtomicBoolean workerResourcesClosed = new AtomicBoolean();
    private boolean anyPartitionsSeen = false;
    /** Set to {@code true} the first time {@link #getOutput()} calls {@link #buildOutput()}, so that
     * {@link #isFinished()} does not return {@code true} before the driver has a chance to call
     * {@link #getOutput()} and actually produce the final output pages. */
    private boolean buildOutputCalled = false;

    // ---- Status tracking fields ----
    private long reconcileNanos;
    private int pagesProcessed;
    private long rowsReceived;
    /** Hash + aggregation nanos from ops that have already been closed. */
    private long savedHashNanos;
    private long savedAggNanos;

    /** Accumulates all untagged pages on the driver thread. INTERMEDIATE mode. */
    private HashAggregationOperator noneOp;
    /** One FINAL-mode operator per partition; created eagerly in the constructor. */
    private HashAggregationOperator[] workerOps;
    /**
     * One child block factory per logical worker (worker {@code w} uses {@code workerBlockFactories[w]}).
     * Workers run concurrently on separate threads; each needs its own {@code LocalCircuitBreaker}
     * so that block allocation/release inside {@link BlockHash#add} does not trigger the
     * single-thread assertion that the driver's shared factory carries.
     */
    private final BlockFactory[] workerBlockFactories;

    @SuppressWarnings("this-escape")
    PartitionedHashMergeOperator(
        List<BlockHash.GroupSpec> internalGroupSpecs,
        List<GroupingAggregator.Factory> noneAggFactories,
        List<GroupingAggregator.Factory> workerAggFactories,
        int partitionCount,
        int workerCount,
        int maxPageSize,
        int aggregationBatchSize,
        Executor executor,
        DriverContext driverContext
    ) {
        this.partitionCount = partitionCount;
        this.maxPageSize = maxPageSize;
        this.driverContext = driverContext;
        this.workerCount = workerCount;
        this.executor = executor;

        boolean success = false;
        try {
            this.workerBlockFactories = new BlockFactory[workerCount];
            for (int w = 0; w < workerCount; w++) {
                workerBlockFactories[w] = driverContext.createChildBlockFactory();
            }
            this.noneOp = new HashAggregationOperator(
                AggregatorMode.INTERMEDIATE,
                noneAggFactories,
                () -> BlockHash.build(internalGroupSpecs, driverContext.blockFactory(), aggregationBatchSize, false),
                Integer.MAX_VALUE,
                1.0,
                maxPageSize,
                null,
                driverContext
            );
            this.workerOps = new HashAggregationOperator[partitionCount];
            for (int p = 0; p < partitionCount; p++) {
                BlockFactory wf = workerBlockFactories[p % workerCount];
                workerOps[p] = new HashAggregationOperator(
                    AggregatorMode.FINAL,
                    workerAggFactories,
                    () -> BlockHash.build(internalGroupSpecs, wf, aggregationBatchSize, false),
                    Integer.MAX_VALUE,
                    1.0,
                    maxPageSize,
                    null,
                    driverContext.withBlockFactory(wf)
                );
            }
            this.workerBuffers = new ExchangeBuffer[partitionCount];
            for (int p = 0; p < partitionCount; p++) {
                workerBuffers[p] = new ExchangeBuffer(2 * partitionCount);
            }
            this.workerSubmitted = new AtomicBoolean[workerCount];
            for (int w = 0; w < workerCount; w++) {
                workerSubmitted[w] = new AtomicBoolean(true);
            }
            this.pendingTasks = new PendingTasks(() -> {
                allWorkersDone.onResponse(null);
                if (closed) {
                    closeWorkerResources();
                }
                driverContext.removeAsyncAction();
            });
            driverContext.addAsyncAction();
            for (int w = 0; w < workerCount; w++) {
                pendingTasks.newTask();
                scheduleWorker(w);
            }
            success = true;
        } finally {
            if (success == false) {
                close();
            }
        }
    }

    // ---- Operator interface ----

    @Override
    public boolean needsInput() {
        return output == null && finishCalled == false;
    }

    @Override
    public void addInput(Page page) {
        checkState(needsInput(), "Operator is already finishing");
        requireNonNull(page, "page is null");
        pagesProcessed++;
        rowsReceived += page.getPositionCount();
        Integer partitionId = page.partitionId();
        if (partitionId != null) {
            anyPartitionsSeen = true;
            page.allowPassingToDifferentDriver();
            workerBuffers[partitionId].addPage(page);
            // Ownership transferred to buffer — do NOT call page.releaseBlocks()
        } else {
            try {
                noneOp.processPage(page);
            } finally {
                page.releaseBlocks();
            }
        }
    }

    @Override
    public void finish() {
        if (finishCalled) {
            return;
        }
        finishCalled = true;
        emitFinal();
    }

    @Override
    public IsBlockedResult isBlocked() {
        if (failureCollector.hasFailure()) {
            // Will throw in getOutput(); unblock so the driver can reach getOutput().
            return NOT_BLOCKED;
        }
        if (finishCalled && allWorkersDone.isDone() == false) {
            return new IsBlockedResult(allWorkersDone, "waiting for merge workers");
        }
        return NOT_BLOCKED;
    }

    @Override
    public Page getOutput() {
        if (failureCollector.hasFailure()) {
            throw ExceptionsHelper.convertToRuntime(failureCollector.getFailure());
        }
        if (output != null) {
            if (output.hasNext() == false) {
                output.close();
                output = null;
                return null;
            }
            return output.next();
        }
        if (finishCalled == false || allWorkersDone.isDone() == false) {
            return null;
        }
        if (buildOutputCalled == false) {
            buildOutputCalled = true;
            buildOutput();
        }
        if (output == null) {
            return null;
        }
        return output.hasNext() ? output.next() : null;
    }

    @Override
    public boolean isFinished() {
        // Must not return true before getOutput() has called buildOutput(): the driver checks
        // isFinished() before calling getOutput(), so returning true here too early would prevent
        // buildOutput() from ever running and the operator would produce no results.
        return finishCalled && allWorkersDone.isDone() && buildOutputCalled && output == null;
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        if (finishCalled == false) {
            return false;
        }
        if (allWorkersDone.isDone() == false) {
            // Workers still running — output will arrive without further external input.
            return true;
        }
        return buildOutputCalled == false || output != null;
    }

    @Override
    public void close() {
        if (workerBuffers != null) {
            for (ExchangeBuffer buf : workerBuffers) {
                buf.finish(true);
            }
        }
        // Close output BEFORE releasing worker block factories: operators inside output return
        // bytes to their worker-specific LocalCircuitBreakers, which must still be open.
        Releasables.close(noneOp, output);
        noneOp = null;
        output = null;
        closed = true;
        if (finishCalled == false && pendingTasks != null) {
            pendingTasks.finishTask();
        }
        if (allWorkersDone.isDone()) {
            closeWorkerResources();
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[workers=" + workerCount + ", partitionCount=" + partitionCount + "]";
    }

    // ---- Worker scheduling ----

    /**
     * Submits a task for {@code workerIndex} unconditionally. Callers must have already set
     * {@code workerSubmitted[workerIndex]} to {@code true} and called {@link PendingTasks#newTask()}
     * before invoking this. Worker {@code w} is responsible for partitions
     * {@code w, w+workerCount, w+2*workerCount, ...}.
     *
     * <p>Pre-incrementing {@code pendingTasks} before {@code executor.execute()} ensures that
     * tasks spawned by {@code distributeNoneOpToWorkers()} (which fires {@code waitForReading}
     * listeners and may call {@link #maybeScheduleWorker} synchronously) are counted before
     * {@code emitFinal()} decrements the driver's reference.
     */
    private void scheduleWorker(int workerIndex) {
        executor.execute(new AbstractRunnable() {
            @Override
            protected void doRun() {
                try {
                    boolean anyUnfinished = false;
                    for (int p = workerIndex; p < partitionCount; p += workerCount) {
                        ExchangeBuffer buffer = workerBuffers[p];
                        HashAggregationOperator op = workerOps[p];
                        Page page;
                        while ((page = buffer.pollPage()) != null) {
                            try {
                                op.processPage(page);
                            } finally {
                                page.releaseBlocks();
                            }
                        }
                        if (buffer.isFinished() == false) {
                            anyUnfinished = true;
                        }
                    }
                    if (anyUnfinished) {
                        // Register a wake-up listener on each unfinished partition buffer.
                        // Each listener calls maybeScheduleWorker, which uses a CAS to ensure
                        // at most one task is ever submitted for this worker at a time.
                        for (int p = workerIndex; p < partitionCount; p += workerCount) {
                            if (workerBuffers[p].isFinished() == false) {
                                workerBuffers[p].waitForReading()
                                    .listener()
                                    .addListener(ActionListener.running(() -> maybeScheduleWorker(workerIndex)));
                            }
                        }
                    }
                    // Clear the submitted flag AFTER registering listeners. Any listener that
                    // fired while the flag was set had its maybeScheduleWorker CAS fail; re-check
                    // each buffer so we don't miss data that arrived in that window.
                    workerSubmitted[workerIndex].set(false);
                    if (anyUnfinished) {
                        for (int p = workerIndex; p < partitionCount; p += workerCount) {
                            if (workerBuffers[p].isFinished() == false && workerBuffers[p].waitForReading() == NOT_BLOCKED) {
                                maybeScheduleWorker(workerIndex);
                                break;
                            }
                        }
                    }
                } finally {
                    pendingTasks.finishTask();
                }
            }

            @Override
            public void onFailure(Exception e) {
                failureCollector.unwrapAndCollect(e);
                for (int p = workerIndex; p < partitionCount; p += workerCount) {
                    workerBuffers[p].finish(true);
                }
            }

            @Override
            public void onRejection(Exception e) {
                // Allow re-scheduling via listener; driver thread drains remaining pages in buildOutput().
                workerSubmitted[workerIndex].set(false);
                pendingTasks.finishTask();
            }
        });
    }

    /**
     * Schedules a task for {@code workerIndex} if none is currently submitted or running.
     * Uses a CAS on {@link #workerSubmitted} to prevent concurrent tasks for the same worker,
     * which would race on its partition tables.
     */
    private void maybeScheduleWorker(int workerIndex) {
        if (workerSubmitted[workerIndex].compareAndSet(false, true)) {
            pendingTasks.newTask();
            scheduleWorker(workerIndex);
        }
    }

    // ---- finish() helpers ----

    private void emitFinal() {
        if (anyPartitionsSeen) {
            long start = System.nanoTime();
            distributeNoneOpToWorkers();
            reconcileNanos = System.nanoTime() - start;
        }
        // Signal workers that no more pages are coming.
        for (ExchangeBuffer buf : workerBuffers) {
            buf.finish(false);
        }
        // Release the driver's own task reference; workers hold the rest.
        // Any pages left in buffers (e.g. from rejected worker tasks) are drained in buildOutput()
        // after allWorkersDone fires, when it is safe to access workerOps single-threadedly.
        pendingTasks.finishTask();
    }

    private void drainBufferOnDriverThread(int p) {
        Page page;
        while ((page = workerBuffers[p].pollPage()) != null) {
            try {
                workerOps[p].processPage(page);
            } finally {
                page.releaseBlocks();
            }
        }
    }

    /**
     * Evaluates the none operator (INTERMEDIATE mode → per-partition pages) and enqueues each
     * partition's page into the owning worker's {@link ExchangeBuffer}. Closes and nulls
     * {@link #noneOp} on return.
     */
    private void distributeNoneOpToWorkers() {
        if (noneOp.blockHash.numKeys() > 0) {
            var pageBuilder = new GroupingAggregatorPageBuilder(
                noneOp.blockHash,
                noneOp.aggregators,
                Integer.MAX_VALUE,
                GroupingAggregatorPageBuilder.NO_CUSTOMIZATION
            );
            Page[] perPartition = pageBuilder.buildPartitioned(
                partitionCount,
                noneOp.blockHash.partitioner(partitionCount),
                new GroupingAggregatorEvaluationContext(driverContext)
            );
            for (int p = 0; p < partitionCount; p++) {
                Page page = perPartition[p];
                if (page != null) {
                    page.allowPassingToDifferentDriver();
                    workerBuffers[p].addPage(page);
                }
            }
        }
        saveNoneOpTiming();
        noneOp.close();
        noneOp = null;
    }

    private void saveNoneOpTiming() {
        HashAggregationOperator.Status s = (HashAggregationOperator.Status) noneOp.status();
        savedHashNanos += s.hashNanos();
        savedAggNanos += s.aggregationNanos();
    }

    private void buildOutput() {
        if (anyPartitionsSeen) {
            // Drain any pages left in buffers after allWorkersDone (e.g. from rejected worker tasks).
            // Safe here because all workers have exited — no concurrent operator access.
            for (int p = 0; p < partitionCount; p++) {
                drainBufferOnDriverThread(p);
            }
            List<ReleasableIterator<Page>> parts = new ArrayList<>();
            for (int p = 0; p < partitionCount; p++) {
                HashAggregationOperator worker = workerOps[p];
                workerOps[p] = null;
                if (worker != null) {
                    HashAggregationOperator.Status s = (HashAggregationOperator.Status) worker.status();
                    savedHashNanos += s.hashNanos();
                    savedAggNanos += s.aggregationNanos();
                    if (worker.blockHash.numKeys() > 0) {
                        parts.add(closeOpOnClose(evaluateOp(worker, maxPageSize, driverContext), worker));
                    } else {
                        worker.close();
                    }
                }
            }
            if (parts.isEmpty() == false) {
                output = new ConcatenatingPageIterator(parts);
            }
        } else {
            // Non-promoted path: evaluate noneOp's INTERMEDIATE-mode aggregators as FINAL output.
            // The underlying aggregatorFunction state is identical between INTERMEDIATE and FINAL
            // mode; only what evaluate() emits differs. This skips the evaluate-then-re-ingest
            // round trip that would otherwise be needed to convert to final output format.
            HashAggregationOperator op = noneOp;
            noneOp = null;
            if (op != null) {
                HashAggregationOperator.Status s = (HashAggregationOperator.Status) op.status();
                savedHashNanos += s.hashNanos();
                savedAggNanos += s.aggregationNanos();
                if (op.blockHash.numKeys() > 0) {
                    output = closeOpOnClose(op.evaluateAsFinal(), op);
                } else {
                    op.close();
                }
            }
        }
    }

    private void closeWorkerResources() {
        if (workerResourcesClosed.compareAndSet(false, true) == false) {
            return;
        }
        if (workerOps != null) {
            Releasables.close(workerOps);
        }
        if (workerBlockFactories != null) {
            for (BlockFactory wf : workerBlockFactories) {
                if (wf != null) {
                    driverContext.releaseChildBlockFactory(wf);
                }
            }
        }
    }

    /**
     * Validates {@code externalSpecs} and derives internal group specs with channels remapped
     * to 0..keyCount-1. Used by {@link Factory} to build the
     * internal blockHash configuration for noneOp and workerOps.
     */
    private static List<BlockHash.GroupSpec> buildInternalGroupSpecs(List<BlockHash.GroupSpec> externalSpecs) {
        requireNonNull(externalSpecs, "groupSpecs");
        if (externalSpecs.isEmpty()) {
            throw new IllegalArgumentException("groupSpecs must not be empty");
        }
        List<BlockHash.GroupSpec> internalSpecs = new ArrayList<>(externalSpecs.size());
        for (int k = 0; k < externalSpecs.size(); k++) {
            internalSpecs.add(new BlockHash.GroupSpec(k, externalSpecs.get(k).elementType()));
        }
        return List.copyOf(internalSpecs);
    }

    /**
     * Evaluates {@code op}'s current aggregator state to pages using {@code maxPageSizeOverride}
     * as the page-size cap. Pass {@link Integer#MAX_VALUE} for intermediate-only evaluations with
     * no size limit (used when re-ingesting results internally); pass the operator's
     * {@code maxPageSize} for normal emission.
     */
    private static ReleasableIterator<Page> evaluateOp(HashAggregationOperator op, int maxPageSizeOverride, DriverContext driverContext) {
        var pageBuilder = new GroupingAggregatorPageBuilder(
            op.blockHash,
            op.aggregators,
            maxPageSizeOverride,
            GroupingAggregatorPageBuilder.NO_CUSTOMIZATION
        );
        return pageBuilder.build(new GroupingAggregatorEvaluationContext(driverContext));
    }

    /** Wraps {@code delegate} so that closing the iterator also closes {@code op}. */
    private static ReleasableIterator<Page> closeOpOnClose(ReleasableIterator<Page> delegate, HashAggregationOperator op) {
        return new ReleasableIterator<>() {
            @Override
            public boolean hasNext() {
                return delegate.hasNext();
            }

            @Override
            public Page next() {
                return delegate.next();
            }

            @Override
            public void close() {
                Releasables.close(delegate, op);
            }
        };
    }

    private static void checkState(boolean condition, String msg) {
        if (condition == false) {
            throw new IllegalArgumentException(msg);
        }
    }

    private static final class PendingTasks {
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

    @Override
    public Operator.Status status() {
        long hashNanos = savedHashNanos, aggNanos = savedAggNanos;
        if (noneOp != null) {
            HashAggregationOperator.Status s = (HashAggregationOperator.Status) noneOp.status();
            hashNanos += s.hashNanos();
            aggNanos += s.aggregationNanos();
        }
        return new Status(reconcileNanos, hashNanos, aggNanos, pagesProcessed, rowsReceived);
    }

    public static class Status implements Operator.Status {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "partitioned_hash_merge",
            Status::new
        );

        private final long reconcileNanos;
        private final long hashNanos;
        private final long aggregationNanos;
        private final int pagesProcessed;
        private final long rowsReceived;

        public Status(long reconcileNanos, long hashNanos, long aggregationNanos, int pagesProcessed, long rowsReceived) {
            this.reconcileNanos = reconcileNanos;
            this.hashNanos = hashNanos;
            this.aggregationNanos = aggregationNanos;
            this.pagesProcessed = pagesProcessed;
            this.rowsReceived = rowsReceived;
        }

        public Status(StreamInput in) throws IOException {
            reconcileNanos = in.readVLong();
            hashNanos = in.readVLong();
            aggregationNanos = in.readVLong();
            pagesProcessed = in.readVInt();
            rowsReceived = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(reconcileNanos);
            out.writeVLong(hashNanos);
            out.writeVLong(aggregationNanos);
            out.writeVInt(pagesProcessed);
            out.writeVLong(rowsReceived);
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.minimumCompatible();
        }

        /**
         * Nanoseconds spent in {@code distributeNoneOpToWorkers}: evaluating the untagged accumulator
         * and routing its output to per-partition worker buffers.
         */
        public long reconcileNanos() {
            return reconcileNanos;
        }

        /** Nanoseconds inner operators spent hashing grouping keys, summed across noneOp and all worker ops. */
        public long hashNanos() {
            return hashNanos;
        }

        /** Nanoseconds inner operators spent running aggregations, summed across noneOp and all worker ops. */
        public long aggregationNanos() {
            return aggregationNanos;
        }

        /** Count of pages this operator has processed. */
        public int pagesProcessed() {
            return pagesProcessed;
        }

        /** Count of rows this operator has received. */
        public long rowsReceived() {
            return rowsReceived;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Status status = (Status) o;
            return reconcileNanos == status.reconcileNanos
                && hashNanos == status.hashNanos
                && aggregationNanos == status.aggregationNanos
                && pagesProcessed == status.pagesProcessed
                && rowsReceived == status.rowsReceived;
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(reconcileNanos, hashNanos, aggregationNanos, pagesProcessed, rowsReceived);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("reconcile_nanos", reconcileNanos);
            if (builder.humanReadable()) {
                builder.field("reconcile_time", TimeValue.timeValueNanos(reconcileNanos));
            }
            builder.field("hash_nanos", hashNanos);
            if (builder.humanReadable()) {
                builder.field("hash_time", TimeValue.timeValueNanos(hashNanos));
            }
            builder.field("aggregation_nanos", aggregationNanos);
            if (builder.humanReadable()) {
                builder.field("aggregation_time", TimeValue.timeValueNanos(aggregationNanos));
            }
            builder.field("pages_processed", pagesProcessed);
            builder.field("rows_received", rowsReceived);
            return builder.endObject();
        }
    }

    /**
     * Lazily concatenates multiple {@link ReleasableIterator}s of pages without modification,
     * closing each source when exhausted.
     */
    private static final class ConcatenatingPageIterator implements ReleasableIterator<Page> {
        private final Deque<ReleasableIterator<Page>> sources;

        ConcatenatingPageIterator(List<ReleasableIterator<Page>> sources) {
            this.sources = new ArrayDeque<>(sources);
        }

        @Override
        public boolean hasNext() {
            while (sources.isEmpty() == false && sources.peekFirst().hasNext() == false) {
                sources.pollFirst().close();
            }
            return sources.isEmpty() == false;
        }

        @Override
        public Page next() {
            if (hasNext() == false) {
                throw new NoSuchElementException();
            }
            return sources.peekFirst().next();
        }

        @Override
        public void close() {
            for (ReleasableIterator<Page> source : sources) {
                source.close();
            }
        }
    }
}
