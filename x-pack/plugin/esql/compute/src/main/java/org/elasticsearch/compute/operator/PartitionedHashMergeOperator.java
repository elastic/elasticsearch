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
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.GroupingAggregatorEvaluationContext;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.exchange.ExchangeBuffer;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

/**
 * Coordinator-side merge operator for partitioned hash aggregation (Phase 3).
 *
 * <p>Receives intermediate {@link Page}s from data nodes — some tagged with a
 * {@link Page#partitionId()} by {@link PartitionedHashAggregationOperator}, others untagged
 * (from shards that never converted to partitioned mode). Produces final aggregated output.
 *
 * <h2>Lifecycle</h2>
 * <ol>
 *   <li><b>Pre-tagged</b> – untagged pages accumulate in the {@code noneTable}
 *       (INTERMEDIATE-mode aggregators) on the driver thread. Tagged pages are routed
 *       directly to the owning partition worker's {@link ExchangeBuffer}.</li>
 *   <li><b>Finish</b> – the {@code noneTable}'s contents are distributed to worker buffers
 *       via the evaluate-intermediate → split-by-partition-hash → buffer-enqueue primitive;
 *       each worker then merges its buffer into its own FINAL-mode table.</li>
 *   <li><b>Output</b> – once all workers signal completion, the driver evaluates each
 *       worker table to final output pages (disjoint, no k-way merge required).</li>
 * </ol>
 *
 * <p>Background workers run on the {@code esql_worker} thread pool using the same
 * {@link PendingTasks} / {@link SubscribableListener} / {@link ExchangeBuffer} machinery
 * as {@link org.elasticsearch.compute.operator.topn.ParallelTopNOperator}.
 */
public class PartitionedHashMergeOperator implements Operator {

    /** Partition assigned to rows whose grouping key contains a null. */
    private static final int NULL_PARTITION = 0;

    private static final GroupingAggregatorPageBuilder.CustomizeSelected NO_CUSTOMIZATION = (aggregator, selected) -> {
        selected.incRef();
        return selected;
    };

    /**
     * Aggregator specification: supplier plus the <em>external</em> intermediate-state channels
     * that appear in pages arriving from the exchange (key columns excluded).
     */
    public record AggregatorSpec(AggregatorFunctionSupplier supplier, List<Integer> channels) {}

    // ---- Builder / Factory ----

    public static class Builder {
        private List<BlockHash.GroupSpec> groupSpecs;
        private List<AggregatorSpec> aggregators;
        private int partitionCount = PartitionedHashAggregationOperator.DEFAULT_PARTITION_COUNT;
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
        private final List<Integer> groupChannels;
        private final List<BlockHash.GroupSpec> internalGroupSpecs;
        private final List<AggregatorSpec> aggregatorSpecs;
        private final List<GroupingAggregator.Factory> noneAggFactories;
        private final List<GroupingAggregator.Factory> workerAggFactories;
        private final int[] combinedChannelStart;
        private final int internalPageWidth;
        private final int partitionCount;
        private final int maxPageSize;
        private final int aggregationBatchSize;
        private final Executor executor;

        private Factory(Builder builder) {
            List<BlockHash.GroupSpec> externalSpecs = requireNonNull(builder.groupSpecs, "groupSpecs");
            if (externalSpecs.isEmpty()) {
                throw new IllegalArgumentException("groupSpecs must not be empty");
            }
            List<Integer> channels = new ArrayList<>(externalSpecs.size());
            List<BlockHash.GroupSpec> internalSpecs = new ArrayList<>(externalSpecs.size());
            for (int k = 0; k < externalSpecs.size(); k++) {
                channels.add(externalSpecs.get(k).channel());
                internalSpecs.add(new BlockHash.GroupSpec(k, externalSpecs.get(k).elementType()));
            }
            this.groupChannels = List.copyOf(channels);
            this.internalGroupSpecs = List.copyOf(internalSpecs);
            this.aggregatorSpecs = requireNonNull(builder.aggregators, "aggregators");

            List<GroupingAggregator.Factory> noneFactories = new ArrayList<>(aggregatorSpecs.size());
            List<GroupingAggregator.Factory> workerFactories = new ArrayList<>(aggregatorSpecs.size());
            int[] combinedStart = new int[aggregatorSpecs.size()];
            int nextChannel = groupChannels.size();
            for (int i = 0; i < aggregatorSpecs.size(); i++) {
                AggregatorSpec spec = aggregatorSpecs.get(i);
                int intermediateBlockCount = spec.supplier().groupingIntermediateStateDesc().size();
                List<Integer> internalChannels = new ArrayList<>(intermediateBlockCount);
                for (int c = 0; c < intermediateBlockCount; c++) {
                    internalChannels.add(nextChannel + c);
                }
                combinedStart[i] = nextChannel;
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
            this.combinedChannelStart = combinedStart;
            this.internalPageWidth = nextChannel;
            this.partitionCount = builder.partitionCount;
            this.maxPageSize = builder.maxPageSize;
            this.aggregationBatchSize = builder.aggregationBatchSize;
            this.executor = builder.executor;
        }

        @Override
        public PartitionedHashMergeOperator get(DriverContext driverContext) {
            return new PartitionedHashMergeOperator(
                groupChannels,
                internalGroupSpecs,
                aggregatorSpecs,
                noneAggFactories,
                workerAggFactories,
                combinedChannelStart,
                internalPageWidth,
                partitionCount,
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

    private final List<Integer> groupChannels;
    private final List<BlockHash.GroupSpec> internalGroupSpecs;
    private final List<AggregatorSpec> aggregatorSpecs;
    private final List<GroupingAggregator.Factory> noneAggFactories;
    private final List<GroupingAggregator.Factory> workerAggFactories;
    private final int[] combinedChannelStart;
    private final int internalPageWidth;
    private final int partitionCount;
    private final int maxPageSize;
    private final int aggregationBatchSize;
    private final DriverContext driverContext;

    private final Executor executor;
    private final ExchangeBuffer[] workerBuffers;
    private final FailureCollector failureCollector = new FailureCollector();
    private final SubscribableListener<Void> allWorkersDone = new SubscribableListener<>();
    private final PendingTasks pendingTasks;
    private volatile boolean closed = false;
    private boolean anyTaggedSeen = false;
    private boolean finishCalled = false;

    /** Accumulates all untagged pages on the driver thread. INTERMEDIATE mode. */
    private Table noneTable;
    /** One FINAL-mode table per partition; created eagerly in the constructor. */
    private Table[] workerTables;

    private ReleasableIterator<Page> output;

    @SuppressWarnings("this-escape")
    PartitionedHashMergeOperator(
        List<Integer> groupChannels,
        List<BlockHash.GroupSpec> internalGroupSpecs,
        List<AggregatorSpec> aggregatorSpecs,
        List<GroupingAggregator.Factory> noneAggFactories,
        List<GroupingAggregator.Factory> workerAggFactories,
        int[] combinedChannelStart,
        int internalPageWidth,
        int partitionCount,
        int maxPageSize,
        int aggregationBatchSize,
        Executor executor,
        DriverContext driverContext
    ) {
        this.groupChannels = groupChannels;
        this.internalGroupSpecs = internalGroupSpecs;
        this.aggregatorSpecs = aggregatorSpecs;
        this.noneAggFactories = noneAggFactories;
        this.workerAggFactories = workerAggFactories;
        this.combinedChannelStart = combinedChannelStart;
        this.internalPageWidth = internalPageWidth;
        this.partitionCount = partitionCount;
        this.maxPageSize = maxPageSize;
        this.aggregationBatchSize = aggregationBatchSize;
        this.executor = executor;
        this.driverContext = driverContext;

        boolean success = false;
        try {
            this.noneTable = newTable(noneAggFactories);
            this.workerTables = new Table[partitionCount];
            for (int p = 0; p < partitionCount; p++) {
                workerTables[p] = newTable(workerAggFactories);
            }
            this.workerBuffers = new ExchangeBuffer[partitionCount];
            for (int p = 0; p < partitionCount; p++) {
                workerBuffers[p] = new ExchangeBuffer(2 * partitionCount);
            }
            this.pendingTasks = new PendingTasks(() -> {
                allWorkersDone.onResponse(null);
                if (closed) {
                    closeWorkerResources();
                }
                driverContext.removeAsyncAction();
            });
            driverContext.addAsyncAction();
            for (int p = 0; p < partitionCount; p++) {
                scheduleWorker(p);
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
        Integer partitionId = page.partitionId();
        if (partitionId != null) {
            anyTaggedSeen = true;
            page.allowPassingToDifferentDriver();
            workerBuffers[partitionId].addPage(page);
            // Ownership transferred to buffer — do NOT call page.releaseBlocks()
        } else {
            try {
                mergeIntermediateIntoTable(noneTable, toInternalLayout(page));
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
        buildOutput();
        if (output == null) {
            return null;
        }
        return output.hasNext() ? output.next() : null;
    }

    @Override
    public boolean isFinished() {
        return finishCalled && allWorkersDone.isDone() && output == null;
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
        return output != null;
    }

    @Override
    public Operator tryPromote(DriverContext driverContext) {
        // Worker tables are created eagerly; no lazy promotion needed.
        return this;
    }

    @Override
    public void close() {
        closed = true;
        if (workerBuffers != null) {
            for (ExchangeBuffer buf : workerBuffers) {
                buf.finish(true);
            }
        }
        if (finishCalled == false && pendingTasks != null) {
            pendingTasks.finishTask();
        }
        if (allWorkersDone.isDone()) {
            closeWorkerResources();
        }
        Releasables.close(noneTable, output);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[workers=" + partitionCount + ", partitionCount=" + partitionCount + "]";
    }

    // ---- Worker scheduling ----

    private void scheduleWorker(int partitionIndex) {
        // Pre-increment before execute() so the task is tracked before emitFinal() calls
        // finishTask(). Unlike ParallelTopNOperator (which defers newTask() to doRun() because
        // finish() drains the shared buffer before finishTask()), this operator has N independent
        // per-partition buffers; reconcileNoneToBuffers() fires waitForReading() listeners that
        // submit new tasks, and those tasks may hold pages to merge — they must be visible to
        // pendingTasks before emitFinal() decrements the driver's ref.
        pendingTasks.newTask();
        executor.execute(new AbstractRunnable() {
            @Override
            protected void doRun() {
                try {
                    ExchangeBuffer buffer = workerBuffers[partitionIndex];
                    Table table = workerTables[partitionIndex];
                    Page page;
                    while ((page = buffer.pollPage()) != null) {
                        try {
                            mergeIntermediateIntoTable(table, toInternalLayout(page));
                        } finally {
                            page.releaseBlocks();
                        }
                    }
                    if (buffer.isFinished() == false) {
                        buffer.waitForReading().listener().addListener(ActionListener.running(() -> scheduleWorker(partitionIndex)));
                    }
                } finally {
                    pendingTasks.finishTask();
                }
            }

            @Override
            public void onFailure(Exception e) {
                failureCollector.unwrapAndCollect(e);
                workerBuffers[partitionIndex].finish(true);
            }

            @Override
            public void onRejection(Exception e) {
                // Balance the pre-increment; driver thread drains this buffer in buildOutput().
                pendingTasks.finishTask();
            }
        });
    }

    // ---- finish() helpers ----

    private void emitFinal() {
        if (anyTaggedSeen) {
            reconcileNoneToBuffers();
        }
        // Signal workers that no more pages are coming.
        for (ExchangeBuffer buf : workerBuffers) {
            buf.finish(false);
        }
        // Release the driver's own task reference; workers hold the rest.
        // Any pages left in buffers (e.g. from rejected worker tasks) are drained in buildOutput()
        // after allWorkersDone fires, when it is safe to access workerTables single-threadedly.
        pendingTasks.finishTask();
    }

    private void drainBufferOnDriverThread(int p) {
        Page page;
        while ((page = workerBuffers[p].pollPage()) != null) {
            try {
                mergeIntermediateIntoTable(workerTables[p], toInternalLayout(page));
            } finally {
                page.releaseBlocks();
            }
        }
    }

    /**
     * Evaluates the NONE table (INTERMEDIATE mode → intermediate pages), splits each page by
     * partition hash, and enqueues each slice into the owning worker's {@link ExchangeBuffer}.
     * Closes and nulls {@link #noneTable} on return.
     */
    private void reconcileNoneToBuffers() {
        if (noneTable.blockHash.numKeys() == 0) {
            noneTable.close();
            noneTable = null;
            return;
        }
        try (ReleasableIterator<Page> nonePages = evaluateTable(noneTable)) {
            while (nonePages.hasNext()) {
                try (Page p = nonePages.next()) {
                    distributeIntermediatePageToBuffers(p);
                }
            }
        }
        noneTable.close();
        noneTable = null;
    }

    /**
     * Distributes a single intermediate page across {@link #workerBuffers} by partition hash.
     * Creates filtered sub-pages (owning their own blocks) and enqueues them; the caller is
     * responsible for closing the original {@code intermediatePage}.
     */
    private void distributeIntermediatePageToBuffers(Page intermediatePage) {
        int positions = intermediatePage.getPositionCount();
        int keyCount = internalGroupSpecs.size();
        BlockHash.Router probeRouter = workerTables[0].blockHash.router();
        if (probeRouter == null) {
            // Router unsupported for this grouping shape: route everything to partition 0.
            int[] allPos = new int[positions];
            for (int i = 0; i < positions; i++) {
                allPos[i] = i;
            }
            Page subPage = intermediatePage.filter(false, allPos);
            subPage.allowPassingToDifferentDriver();
            workerBuffers[NULL_PARTITION].addPage(subPage);
            return;
        }
        int[] partitionOf = new int[positions];
        for (int i = 0; i < positions; i++) {
            boolean anyNull = false;
            for (int k = 0; k < keyCount; k++) {
                if (intermediatePage.getBlock(k).isNull(i)) {
                    anyNull = true;
                    break;
                }
            }
            partitionOf[i] = anyNull ? NULL_PARTITION : Math.floorMod(probeRouter.partitionHashOfRow(intermediatePage, i), partitionCount);
        }
        for (int p = 0; p < partitionCount; p++) {
            int count = 0;
            for (int i = 0; i < positions; i++) {
                if (partitionOf[i] == p) {
                    count++;
                }
            }
            if (count == 0) {
                continue;
            }
            int[] filterPositions = new int[count];
            int idx = 0;
            for (int i = 0; i < positions; i++) {
                if (partitionOf[i] == p) {
                    filterPositions[idx++] = i;
                }
            }
            Page subPage = intermediatePage.filter(false, filterPositions);
            subPage.allowPassingToDifferentDriver();
            workerBuffers[p].addPage(subPage);
        }
    }

    private void buildOutput() {
        if (anyTaggedSeen) {
            // Drain any pages left in buffers after allWorkersDone (e.g. from rejected worker tasks).
            // Safe here because all workers have exited — no concurrent table access.
            for (int p = 0; p < partitionCount; p++) {
                drainBufferOnDriverThread(p);
            }
            List<ReleasableIterator<Page>> parts = new ArrayList<>();
            for (int p = 0; p < partitionCount; p++) {
                Table worker = workerTables[p];
                workerTables[p] = null;
                if (worker != null && worker.blockHash.numKeys() > 0) {
                    parts.add(closeTableOnClose(evaluateTable(worker), worker));
                } else if (worker != null) {
                    worker.close();
                }
            }
            if (parts.isEmpty() == false) {
                output = new ConcatenatingPageIterator(parts);
            }
        } else {
            // Non-promoted path: rewrap NONE aggregators with FINAL mode and evaluate directly.
            // The underlying aggregatorFunction state is identical between INTERMEDIATE and FINAL
            // mode; only what evaluate() emits differs. This skips the evaluate-then-re-ingest
            // round trip that would otherwise be needed to convert to final output format.
            Table table = noneTable;
            noneTable = null;
            if (table != null && table.blockHash.numKeys() > 0) {
                List<GroupingAggregator> finalAggregators = new ArrayList<>(table.aggregators.size());
                for (GroupingAggregator a : table.aggregators) {
                    finalAggregators.add(new GroupingAggregator(a.aggregatorFunction(), AggregatorMode.FINAL));
                }
                var pageBuilder = new GroupingAggregatorPageBuilder(table.blockHash, finalAggregators, maxPageSize, NO_CUSTOMIZATION);
                output = closeTableOnClose(pageBuilder.build(new GroupingAggregatorEvaluationContext(driverContext)), table);
            } else if (table != null) {
                table.close();
            }
        }
    }

    // ---- shared table helpers ----

    /**
     * Merges an intermediate-format page into {@code table} by re-hashing the key column(s) to
     * get local group ids, then feeding each aggregator's intermediate state to
     * {@code prepareProcessIntermediateInputPage}. Safe to call from worker threads provided
     * each worker operates on its own table.
     */
    private void mergeIntermediateIntoTable(Table table, Page page) {
        List<GroupingAggregatorFunction.AddInput> prepared = new ArrayList<>(table.aggregators.size());
        try {
            for (GroupingAggregator aggregator : table.aggregators) {
                GroupingAggregatorFunction.AddInput addInput = aggregator.aggregatorFunction()
                    .prepareProcessIntermediateInputPage(table.blockHash, page);
                if (addInput != null) {
                    prepared.add(addInput);
                }
            }
            table.blockHash.add(page, new FanOutAddInput(prepared));
        } finally {
            Releasables.closeExpectNoException(Releasables.wrap(prepared));
        }
    }

    /**
     * Evaluates {@code table} to pages reflecting its current state. For INTERMEDIATE-mode
     * tables this produces intermediate pages suitable for re-merging; for FINAL-mode tables
     * this produces final output pages.
     */
    private ReleasableIterator<Page> evaluateTable(Table table) {
        var pageBuilder = new GroupingAggregatorPageBuilder(table.blockHash, table.aggregators, maxPageSize, NO_CUSTOMIZATION);
        return pageBuilder.build(new GroupingAggregatorEvaluationContext(driverContext));
    }

    /**
     * Rearranges {@code page} from the exchange's external channel layout to this operator's
     * internal layout: grouping keys at channels 0..keyCount-1, each aggregator's intermediate
     * state blocks at {@link #combinedChannelStart}[i]..combinedChannelStart[i]+blockCount-1.
     * Never releases the original blocks; {@code page} remains the sole owner.
     */
    private Page toInternalLayout(Page page) {
        Block[] blocks = new Block[internalPageWidth];
        Arrays.fill(blocks, page.getBlock(groupChannels.get(0)));
        for (int k = 0; k < groupChannels.size(); k++) {
            blocks[k] = page.getBlock(groupChannels.get(k));
        }
        for (int a = 0; a < aggregatorSpecs.size(); a++) {
            List<Integer> externalChannels = aggregatorSpecs.get(a).channels();
            int base = combinedChannelStart[a];
            for (int j = 0; j < externalChannels.size(); j++) {
                blocks[base + j] = page.getBlock(externalChannels.get(j));
            }
        }
        return new Page(blocks);
    }

    private Table newTable(List<GroupingAggregator.Factory> factories) {
        BlockHash blockHash = BlockHash.build(internalGroupSpecs, driverContext.blockFactory(), aggregationBatchSize, false);
        boolean success = false;
        try {
            List<GroupingAggregator> aggregators = new ArrayList<>(factories.size());
            for (GroupingAggregator.Factory f : factories) {
                aggregators.add(f.apply(driverContext));
            }
            Table table = new Table(blockHash, aggregators);
            success = true;
            return table;
        } finally {
            if (success == false) {
                blockHash.close();
            }
        }
    }

    private ReleasableIterator<Page> closeTableOnClose(ReleasableIterator<Page> delegate, Table table) {
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
                Releasables.close(delegate::close, table);
            }
        };
    }

    private void closeWorkerResources() {
        if (workerTables != null) {
            Releasables.close(workerTables);
        }
    }

    private static void checkState(boolean condition, String msg) {
        if (condition == false) {
            throw new IllegalArgumentException(msg);
        }
    }

    // ---- Inner types ----

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

    private static final class Table implements Releasable {
        final BlockHash blockHash;
        final List<GroupingAggregator> aggregators;

        Table(BlockHash blockHash, List<GroupingAggregator> aggregators) {
            this.blockHash = blockHash;
            this.aggregators = aggregators;
        }

        @Override
        public void close() {
            Releasables.close(blockHash, () -> Releasables.close(aggregators));
        }
    }

    private static final class FanOutAddInput implements GroupingAggregatorFunction.AddInput {
        private final List<GroupingAggregatorFunction.AddInput> prepared;

        FanOutAddInput(List<GroupingAggregatorFunction.AddInput> prepared) {
            this.prepared = prepared;
        }

        @Override
        public void add(int positionOffset, IntArrayBlock groupIds) {
            for (GroupingAggregatorFunction.AddInput p : prepared) {
                p.add(positionOffset, groupIds);
            }
        }

        @Override
        public void add(int positionOffset, IntBigArrayBlock groupIds) {
            for (GroupingAggregatorFunction.AddInput p : prepared) {
                p.add(positionOffset, groupIds);
            }
        }

        @Override
        public void add(int positionOffset, IntVector groupIds) {
            for (GroupingAggregatorFunction.AddInput p : prepared) {
                p.add(positionOffset, groupIds);
            }
        }

        @Override
        public void close() {}
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
