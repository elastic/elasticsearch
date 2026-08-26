/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PartitionedHashTable;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.GroupingAggregatorEvaluationContext;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.SeenGroupIds;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.swisshash.LongLongSwissHash;
import org.elasticsearch.swisshash.SwissHashFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Single-operator partitioned aggregation using {@link LongLongSwissHash} for two LONG grouping keys.
 *
 * <p>Workers concurrently pre-aggregate rows from the shared input queue into a per-worker
 * {@link LongLongSwissHash}. When the table approaches {@value SCATTER_THRESHOLD} entries,
 * the worker calls {@link LongLongSwissHash#partition partition()} to scatter the current state
 * into a {@link PartitionedHashTable.PartitionedKeysAndAggs} snapshot, clears the table, and
 * continues. After {@link #finish()} is called and all input is drained, workers synchronize at a
 * {@link CyclicBarrier}, then work-steal partitions in the merge phase: each partition's snapshots
 * are folded into a single merge hash, producing one output {@link Page} per non-empty partition.
 *
 * <p>All aggregators passed to this operator must return {@code true} from
 * {@link GroupingAggregatorFunction#supportsPartitionedSplit()}.
 */
public final class SinglePassPartitionedAggregatorV2 implements Operator {

    /** Scatter the hash table when it reaches this many entries. */
    static final int SCATTER_THRESHOLD = 392_000;

    private static final int CHUNK_SIZE = 1024;
    private static final int NUM_PARTITIONS = PartitionedHashTable.NUM_PARTITIONS;

    // ---- Construction parameters ----
    private final int groupKeyChannel0;
    private final int groupKeyChannel1;
    /** True when the original second key type was INT (widened to LONG for hashing; narrowed on output). */
    private final boolean secondKeyIsInt;
    private final List<? extends AggregatorFunctionSupplier> aggregatorSuppliers;
    private final List<List<Integer>> aggregatorChannels;
    private final int workerCount;
    private final Executor executor;
    private final DriverContext driverContext;
    private final SwissHashFactory swissHashFactory;
    private final BigArrays bigArrays;
    private final CircuitBreaker circuitBreaker;

    // ---- Input queue (driver → workers) ----
    private final LinkedBlockingQueue<Page> inputQueue = new LinkedBlockingQueue<>();
    private volatile boolean inputDone = false;

    // ---- Per-worker state ----
    private final Worker[] workers;

    // ---- Phase coordination ----
    private final CyclicBarrier barrier;
    /**
     * Work-stealing cursors. Worker {@code w} initially owns partition range
     * {@code [w*ppw, (w+1)*ppw)}; it steals from peers once its own range is exhausted.
     */
    private final AtomicInteger[] mergeCursors;
    /** Partitions per worker (NUM_PARTITIONS / workerCount). */
    private final int ppw;
    /**
     * All snapshots from all workers; populated before {@link #barrier} and read-only afterwards.
     * Access guarded by {@code synchronized(allSnapshots)} during the collect phase.
     */
    private final List<PartitionedHashTable.PartitionedKeysAndAggs> allSnapshots = new ArrayList<>();

    // ---- Output ----
    private final ConcurrentLinkedQueue<Page> outputQueue = new ConcurrentLinkedQueue<>();

    // ---- Lifecycle tracking ----
    private final SubscribableListener<Void> allWorkersDone = new SubscribableListener<>();
    private final FailureCollector failureCollector = new FailureCollector();
    private final AtomicInteger workersDone = new AtomicInteger(0);
    private volatile boolean finishCalled = false;
    private volatile boolean closed = false;

    /**
     * Constructs and immediately starts the internal worker tasks.
     *
     * @param groupKeyChannel0    channel index for the first (LONG) grouping key
     * @param groupKeyChannel1    channel index for the second (LONG) grouping key
     * @param aggregatorSuppliers suppliers for grouping aggregator functions; each must return
     *                            {@code true} from
     *                            {@link GroupingAggregatorFunction#supportsPartitionedSplit()}
     * @param aggregatorChannels  per-aggregator input channel lists
     * @param workerCount         number of pre-aggregation and merge worker threads
     * @param executor            executor for worker tasks
     * @param driverContext       driver context for allocations and async-action lifecycle tracking
     */
    public SinglePassPartitionedAggregatorV2(
        int groupKeyChannel0,
        int groupKeyChannel1,
        boolean secondKeyIsInt,
        List<? extends AggregatorFunctionSupplier> aggregatorSuppliers,
        List<List<Integer>> aggregatorChannels,
        int workerCount,
        Executor executor,
        DriverContext driverContext
    ) {
        if (aggregatorSuppliers.isEmpty()) {
            throw new IllegalArgumentException("SinglePassPartitionedAggregatorV2 requires at least one aggregator");
        }
        if (aggregatorSuppliers.size() != aggregatorChannels.size()) {
            throw new IllegalArgumentException(
                "aggregatorSuppliers.size() ("
                    + aggregatorSuppliers.size()
                    + ") != aggregatorChannels.size() ("
                    + aggregatorChannels.size()
                    + ")"
            );
        }
        if (workerCount < 1) {
            throw new IllegalArgumentException("workerCount must be >= 1; got " + workerCount);
        }
        SwissHashFactory factory = SwissHashFactory.getInstance();
        if (factory == null) {
            throw new IllegalStateException(
                "SinglePassPartitionedAggregatorV2 requires the Java vector module; SwissHashFactory is unavailable"
            );
        }
        this.groupKeyChannel0 = groupKeyChannel0;
        this.groupKeyChannel1 = groupKeyChannel1;
        this.secondKeyIsInt = secondKeyIsInt;
        this.aggregatorSuppliers = aggregatorSuppliers;
        this.aggregatorChannels = aggregatorChannels;
        this.workerCount = workerCount;
        this.executor = executor;
        this.driverContext = driverContext;
        this.swissHashFactory = factory;
        this.bigArrays = driverContext.bigArrays();
        // TODO: wire a per-operator CircuitBreaker for tighter CB isolation
        this.circuitBreaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);

        this.ppw = NUM_PARTITIONS / workerCount;
        this.mergeCursors = new AtomicInteger[workerCount];
        for (int w = 0; w < workerCount; w++) {
            mergeCursors[w] = new AtomicInteger(w * ppw);
        }

        this.workers = new Worker[workerCount];
        boolean success = false;
        try {
            for (int w = 0; w < workerCount; w++) {
                workers[w] = new Worker();
            }
            success = true;
        } finally {
            if (success == false) {
                for (Worker worker : workers) {
                    if (worker != null) {
                        worker.close();
                    }
                }
            }
        }

        this.barrier = new CyclicBarrier(workerCount);

        driverContext.addAsyncAction();

        for (int w = 0; w < workerCount; w++) {
            final Worker worker = workers[w];
            final int myW = w;
            executor.execute(new AbstractRunnable() {
                @Override
                protected void doRun() throws Exception {
                    runWorker(worker, myW);
                }

                @Override
                public void onFailure(Exception e) {
                    failureCollector.unwrapAndCollect(e);
                    barrier.reset();
                    onWorkerFinished();
                }
            });
        }
    }

    // -------------------------------------------------------------------------
    // Operator interface
    // -------------------------------------------------------------------------

    @Override
    public boolean needsInput() {
        return finishCalled == false;
    }

    @Override
    public void addInput(Page page) {
        page.allowPassingToDifferentDriver();
        inputQueue.add(page);
    }

    @Override
    public void finish() {
        if (finishCalled) {
            return;
        }
        finishCalled = true;
        inputDone = true;
    }

    @Override
    public Page getOutput() {
        if (failureCollector.hasFailure()) {
            throw ExceptionsHelper.convertToRuntime(failureCollector.getFailure());
        }
        return outputQueue.poll();
    }

    @Override
    public boolean isFinished() {
        return finishCalled && allWorkersDone.isDone() && outputQueue.isEmpty();
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        return finishCalled;
    }

    @Override
    public IsBlockedResult isBlocked() {
        if (finishCalled && allWorkersDone.isDone() == false) {
            return new IsBlockedResult(allWorkersDone, "waiting for partitioned aggregation workers");
        }
        return NOT_BLOCKED;
    }

    @Override
    public void close() {
        closed = true;
        Page page;
        while ((page = inputQueue.poll()) != null) {
            page.releaseBlocks();
        }
        while ((page = outputQueue.poll()) != null) {
            page.releaseBlocks();
        }
    }

    // -------------------------------------------------------------------------
    // Internal: worker execution
    // -------------------------------------------------------------------------

    private void runWorker(Worker worker, int myW) throws Exception {
        try {
            // ---- Pre-aggregation phase ----
            while (closed == false) {
                Page page;
                try {
                    page = inputQueue.poll(10L, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                if (page != null) {
                    worker.processPage(page);
                } else if (inputDone) {
                    break;
                }
            }

            if (closed) {
                barrier.reset();
                return;
            }

            // Final scatter of remaining state before the barrier
            worker.finalScatter();

            // ---- Barrier: wait for all workers to finish pre-agg ----
            try {
                barrier.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (BrokenBarrierException e) {
                return;
            }

            if (closed) {
                return;
            }

            // ---- Merge phase: work-steal partitions ----
            worker.mergePhase(myW);
        } finally {
            onWorkerFinished();
        }
    }

    private void onWorkerFinished() {
        if (workersDone.incrementAndGet() == workerCount) {
            for (Worker w : workers) {
                w.close();
            }
            allWorkersDone.onResponse(null);
            driverContext.removeAsyncAction();
        }
    }

    // -------------------------------------------------------------------------
    // Worker inner class
    // -------------------------------------------------------------------------

    /**
     * Per-worker state: a {@link LongLongSwissHash} for the collect phase plus a set of
     * per-aggregator {@link GroupingAggregatorFunction}s. During the merge phase the same
     * worker instance is reused as the merge hash and accumulator for claimed partitions.
     */
    private class Worker implements Releasable {
        final LongLongSwissHash hash;
        final GroupingAggregatorFunction[] collectAggs;
        // Reusable key and id buffers for bulk operations
        final long[] batchKey1s = new long[CHUNK_SIZE];
        final long[] batchKey2s = new long[CHUNK_SIZE];
        final int[] batchIds = new int[CHUNK_SIZE];

        // Merge-phase state (allocated lazily when this worker enters the merge phase)
        private LongLongSwissHash mergeHash;
        private GroupingAggregatorFunction[] mergeAggs;
        private PartitionedHashTable.MergedKeys[] mergedKeys;

        Worker() {
            int numAggs = aggregatorSuppliers.size();
            this.hash = swissHashFactory.newLongLongSwissHash(bigArrays.recycler(), circuitBreaker);
            this.collectAggs = new GroupingAggregatorFunction[numAggs];
            boolean success = false;
            try {
                for (int a = 0; a < numAggs; a++) {
                    collectAggs[a] = aggregatorSuppliers.get(a).groupingAggregator(driverContext, aggregatorChannels.get(a));
                }
                success = true;
            } finally {
                if (success == false) {
                    close();
                }
            }
        }

        void processPage(Page page) {
            try {
                LongVector k1Vec = page.<LongBlock>getBlock(groupKeyChannel0).asVector();
                if (k1Vec == null) {
                    // Multi-valued grouping keys not supported; skip this page.
                    // TODO: route multi-valued rows to a fallback HashAggregationOperator.
                    return;
                }
                LongVector k2LongVec = null;
                IntVector k2IntVec = null;
                if (secondKeyIsInt) {
                    k2IntVec = page.<IntBlock>getBlock(groupKeyChannel1).asVector();
                    if (k2IntVec == null) {
                        return;
                    }
                } else {
                    k2LongVec = page.<LongBlock>getBlock(groupKeyChannel1).asVector();
                    if (k2LongVec == null) {
                        return;
                    }
                }
                int posCount = page.getPositionCount();
                int offset = 0;
                SeenGroupIds seenGroupIds = new SeenGroupIds.Empty();
                while (offset < posCount) {
                    int len = Math.min(posCount - offset, CHUNK_SIZE);
                    if (hash.size() + len > SCATTER_THRESHOLD) {
                        doScatter();
                    }
                    if (secondKeyIsInt) {
                        for (int i = 0; i < len; i++) {
                            batchKey1s[i] = k1Vec.getLong(offset + i);
                            batchKey2s[i] = k2IntVec.getInt(offset + i);
                        }
                    } else {
                        for (int i = 0; i < len; i++) {
                            batchKey1s[i] = k1Vec.getLong(offset + i);
                            batchKey2s[i] = k2LongVec.getLong(offset + i);
                        }
                    }
                    if (hash.supportBulkAdd()) {
                        hash.bulkAdd(batchKey1s, batchKey2s, batchIds, len);
                    } else {
                        for (int i = 0; i < len; i++) {
                            long encoded = hash.add(batchKey1s[i], batchKey2s[i]);
                            batchIds[i] = encoded >= 0 ? (int) encoded : (int) ~encoded;
                        }
                    }
                    try (IntVector idsVec = buildIdsVector(batchIds, len)) {
                        for (GroupingAggregatorFunction agg : collectAggs) {
                            GroupingAggregatorFunction.AddInput addIn = agg.prepareProcessRawInputPage(seenGroupIds, page);
                            if (addIn != null) {
                                try (addIn) {
                                    addIn.add(offset, idsVec);
                                }
                            }
                        }
                    }
                    offset += len;
                }
            } finally {
                page.releaseBlocks();
            }
        }

        private void doScatter() {
            PartitionedHashTable.AggSplitter[] splitters = new PartitionedHashTable.AggSplitter[collectAggs.length];
            try {
                for (int a = 0; a < collectAggs.length; a++) {
                    splitters[a] = collectAggs[a].newSplitter();
                }
                PartitionedHashTable.AggSplitter combined = combinedSplitter(splitters);
                PartitionedHashTable.PartitionedKeys keys = hash.partition(bigArrays, circuitBreaker, combined);
                PartitionedHashTable.PartitionedAgg[] aggParts = new PartitionedHashTable.PartitionedAgg[collectAggs.length];
                for (int a = 0; a < collectAggs.length; a++) {
                    aggParts[a] = splitters[a].finish();
                }
                PartitionedHashTable.PartitionedAgg combinedAgg = new MultiPartitionedAgg(aggParts);
                hash.clear();
                for (GroupingAggregatorFunction agg : collectAggs) {
                    agg.clear();
                }
                synchronized (allSnapshots) {
                    allSnapshots.add(new PartitionedHashTable.PartitionedKeysAndAggs(keys, combinedAgg));
                }
            } finally {
                for (PartitionedHashTable.AggSplitter s : splitters) {
                    if (s != null) {
                        s.close();
                    }
                }
            }
        }

        void finalScatter() {
            if (hash.size() > 0) {
                doScatter();
            }
        }

        void mergePhase(int myW) {
            int numAggs = aggregatorSuppliers.size();
            // Allocate merge-phase resources (merge hash reuses this worker's hash after it's done collecting)
            mergeHash = swissHashFactory.newLongLongSwissHash(bigArrays.recycler(), circuitBreaker);
            mergeAggs = new GroupingAggregatorFunction[numAggs];
            try {
                for (int a = 0; a < numAggs; a++) {
                    mergeAggs[a] = aggregatorSuppliers.get(a).groupingAggregator(driverContext, List.of());
                }
                List<PartitionedHashTable.PartitionedKeysAndAggs> snapshots;
                synchronized (allSnapshots) {
                    snapshots = List.copyOf(allSnapshots);
                }
                mergedKeys = new PartitionedHashTable.MergedKeys[snapshots.size()];

                int myEnd = (myW + 1) * ppw;
                boolean ownExhausted = false;
                outer: for (;;) {
                    int p = -1;
                    if (ownExhausted == false) {
                        int c = mergeCursors[myW].getAndIncrement();
                        if (c < myEnd) {
                            p = c;
                        } else {
                            ownExhausted = true;
                        }
                    }
                    if (p == -1) {
                        for (int i = 1; i <= workerCount; i++) {
                            int v = (myW + i) % workerCount;
                            int stolen = mergeCursors[v].getAndIncrement();
                            if (stolen < (v + 1) * ppw) {
                                p = stolen;
                                break;
                            }
                        }
                        if (p == -1) break outer;
                    }
                    if (closed) {
                        break;
                    }
                    mergePartition(p, snapshots);
                }
            } finally {
                Releasables.close(mergeHash);
                mergeHash = null;
                if (mergeAggs != null) {
                    Releasables.close(mergeAggs);
                    mergeAggs = null;
                }
                if (mergedKeys != null) {
                    Releasables.close(mergedKeys);
                    mergedKeys = null;
                }
            }
        }

        private void mergePartition(int p, List<PartitionedHashTable.PartitionedKeysAndAggs> snapshots) {
            int totalSize = 0;
            for (PartitionedHashTable.PartitionedKeysAndAggs snap : snapshots) {
                totalSize += snap.keys().partitionSize(p);
            }
            if (totalSize == 0) {
                return;
            }

            mergeHash.clear();
            for (int a = 0; a < mergeAggs.length; a++) {
                mergeAggs[a].clear();
            }

            for (int i = 0; i < snapshots.size(); i++) {
                PartitionedHashTable.PartitionedKeysAndAggs snap = snapshots.get(i);
                mergedKeys[i] = mergeHash.mergeKeys(snap.keys(), p, totalSize, mergedKeys[i]);
                snap.keys().releasePartition(p);
            }
            for (int i = 0; i < snapshots.size(); i++) {
                PartitionedHashTable.PartitionedKeysAndAggs snap = snapshots.get(i);
                MultiPartitionedAgg multi = (MultiPartitionedAgg) snap.aggs();
                for (int a = 0; a < mergeAggs.length; a++) {
                    mergeAggs[a].combinePartition(multi.subs[a], p, mergedKeys[i].ids, 0, mergedKeys[i].length);
                    multi.subs[a].releasePartition(p);
                }
            }

            emitPartitionOutput(p);
        }

        private void emitPartitionOutput(int p) {
            int size = (int) mergeHash.size();
            if (size == 0) {
                return;
            }

            long[] key1s = new long[size];
            long[] key2s = new long[size];
            LongLongSwissHash.Itr itr = mergeHash.iterator();
            while (itr.next()) {
                key1s[itr.id()] = itr.key1();
                key2s[itr.id()] = itr.key2();
            }

            int numAggs = mergeAggs.length;
            Block[] blocks = new Block[2 + numAggs];
            boolean success = false;
            try {
                blocks[0] = driverContext.blockFactory().newLongArrayVector(key1s, size).asBlock();
                if (secondKeyIsInt) {
                    int[] intKey2s = new int[size];
                    for (int i = 0; i < size; i++) {
                        intKey2s[i] = (int) key2s[i];
                    }
                    blocks[1] = driverContext.blockFactory().newIntArrayVector(intKey2s, size).asBlock();
                } else {
                    blocks[1] = driverContext.blockFactory().newLongArrayVector(key2s, size).asBlock();
                }
                try (IntVector selected = buildRangeVector(size)) {
                    try (GroupingAggregatorEvaluationContext evalCtx = new GroupingAggregatorEvaluationContext(driverContext)) {
                        for (int a = 0; a < numAggs; a++) {
                            try (
                                GroupingAggregatorFunction.PreparedForEvaluation prep = mergeAggs[a].prepareEvaluateFinal(selected, evalCtx)
                            ) {
                                prep.evaluate(blocks, 2 + a, selected);
                            }
                        }
                    }
                }
                outputQueue.add(new Page(blocks));
                success = true;
            } finally {
                if (success == false) {
                    Releasables.close(blocks);
                }
            }
        }

        @Override
        public void close() {
            Releasables.close(hash);
            Releasables.close(collectAggs);
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private IntVector buildIdsVector(int[] ids, int len) {
        try (IntVector.FixedBuilder builder = driverContext.blockFactory().newIntVectorFixedBuilder(len)) {
            for (int i = 0; i < len; i++) {
                builder.appendInt(i, ids[i]);
            }
            return builder.build();
        }
    }

    private IntVector buildRangeVector(int size) {
        try (IntVector.FixedBuilder builder = driverContext.blockFactory().newIntVectorFixedBuilder(size)) {
            for (int i = 0; i < size; i++) {
                builder.appendInt(i, i);
            }
            return builder.build();
        }
    }

    /**
     * Combines multiple per-aggregator {@link PartitionedHashTable.AggSplitter}s into a single
     * splitter that delegates to each in order.
     */
    private static PartitionedHashTable.AggSplitter combinedSplitter(PartitionedHashTable.AggSplitter[] splitters) {
        return new PartitionedHashTable.AggSplitter() {
            @Override
            public void preAllocate(int[] partitionCounts) {
                for (PartitionedHashTable.AggSplitter s : splitters) {
                    s.preAllocate(partitionCounts);
                }
            }

            @Override
            public void split(
                PartitionedHashTable.ScratchBuffer scratch,
                int idOffset,
                int totalPositions,
                short[] positions,
                int[] fills
            ) {
                for (PartitionedHashTable.AggSplitter s : splitters) {
                    s.split(scratch, idOffset, totalPositions, positions, fills);
                }
            }

            @Override
            public PartitionedHashTable.PartitionedAgg finish() {
                // Each individual splitter's finish() is called separately; this combined splitter
                // does not produce a PartitionedAgg — callers read from each sub-splitter directly.
                throw new UnsupportedOperationException("call finish() on each sub-splitter separately");
            }

            @Override
            public void close() {}
        };
    }

    /**
     * Wraps the per-aggregator {@link PartitionedHashTable.PartitionedAgg} arrays from a single
     * scatter snapshot. The merge phase unwraps these by aggregator index.
     */
    private static final class MultiPartitionedAgg implements PartitionedHashTable.PartitionedAgg {
        final PartitionedHashTable.PartitionedAgg[] subs;

        MultiPartitionedAgg(PartitionedHashTable.PartitionedAgg[] subs) {
            this.subs = subs;
        }

        @Override
        public void releasePartition(int partition) {
            for (PartitionedHashTable.PartitionedAgg sub : subs) {
                sub.releasePartition(partition);
            }
        }

        @Override
        public void close() {
            Releasables.close(subs);
        }
    }

    // -------------------------------------------------------------------------
    // Factory
    // -------------------------------------------------------------------------

    /**
     * {@link OperatorFactory} for {@link SinglePassPartitionedAggregatorV2}.
     *
     * @param groupKeyChannel0    channel index for the first (LONG) grouping key
     * @param groupKeyChannel1    channel index for the second (LONG) grouping key
     * @param aggregatorSuppliers aggregator function suppliers; each must return {@code true} from
     *                            {@link GroupingAggregatorFunction#supportsPartitionedSplit()}
     * @param aggregatorChannels  per-aggregator input channel lists
     * @param workerCount         number of pre-aggregation and merge worker threads
     * @param executor            executor for worker tasks
     */
    public record Factory(
        int groupKeyChannel0,
        int groupKeyChannel1,
        ElementType keyType0,
        ElementType keyType1,
        List<? extends AggregatorFunctionSupplier> aggregatorSuppliers,
        List<List<Integer>> aggregatorChannels,
        int workerCount,
        Executor executor
    ) implements OperatorFactory {

        @Override
        public Operator get(DriverContext driverContext) {
            return new SinglePassPartitionedAggregatorV2(
                groupKeyChannel0,
                groupKeyChannel1,
                keyType1 == ElementType.INT,
                aggregatorSuppliers,
                aggregatorChannels,
                workerCount,
                executor,
                driverContext
            );
        }

        @Override
        public String describe() {
            return "SinglePassPartitionedAggregatorV2[workerCount=" + workerCount + "]";
        }
    }
}
