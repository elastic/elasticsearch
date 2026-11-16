/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.PartitionedHashTable;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.SeenGroupIds;
import org.elasticsearch.compute.aggregation.blockhash.PartitionedBlockHash;
import org.elasticsearch.compute.data.DelegatingCircuitBreaker;
import org.elasticsearch.compute.data.PartitionedAggregationBlock;
import org.elasticsearch.compute.spill.FramedRun;
import org.elasticsearch.compute.spill.FramedRunLayout;
import org.elasticsearch.compute.spill.FramedRunMaxima;
import org.elasticsearch.compute.spill.FramedRunReader;
import org.elasticsearch.compute.spill.FramedRunWriter;
import org.elasticsearch.compute.spill.QueryScope;
import org.elasticsearch.compute.spill.SliceRunReader;
import org.elasticsearch.compute.spill.SliceRunWriter;
import org.elasticsearch.compute.spill.SpillCompletion;
import org.elasticsearch.compute.spill.SpillStatus;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Helper class for {@link ParallelHashAggregationOperator} to hold partitioned aggregation states: the generations
 * the workers split and the partitioned blocks received from earlier operators.
 * <p>
 * Under a query scope that spills, a generation is admitted by one rule, at the split and on arrival alike: it stays
 * resident until the registry holds the own-state minimum and the query crosses its budget, after which every
 * generation is written to a slice run of its own, one record per partition, and published once the run is finished.
 * Phase two restores a spilled generation's slices partition by partition, through {@link Combiner}, which reads
 * ahead a bounded number of slices and combines one generation at a time. The metadata a spilled generation keeps,
 * its run handle, 256 key counts and record sizes and the all-values flags, is bounded by {@link #MAX_SPILLED_GENERATIONS}.
 */
final class PartitionedHashAggregations extends AbstractRefCounted implements Releasable {

    private static final Logger logger = LogManager.getLogger(PartitionedHashAggregations.class);

    /** Returned by {@link #claimPartition()} when every partition has been claimed. */
    static final int NO_MORE_PARTITION = Integer.MAX_VALUE;
    /** Payload bytes per chunk of a slice run, and the reader's window. */
    static final int CHUNK_BYTES = 64 * 1024;
    /** Slices of one partition a worker keeps in flight ahead of its combine. */
    static final int READ_AHEAD = 2;
    /**
     * Generations written at once; the rest queue, and while any queues the workers take no input, so that what waits
     * for the store stays bounded by the workers' own batches.
     */
    static final int MAX_WRITES_IN_FLIGHT = 4;
    /**
     * The most generations one aggregation may spill. Each keeps a few kilobytes of metadata; the split threshold
     * under spill grows with the budget so that this is reached only by an input many thousand times the budget.
     */
    static final int MAX_SPILLED_GENERATIONS = 4096;
    /** The split threshold under spill never exceeds this, which keeps a partition far from its paged form. */
    static final long MAX_SPILL_SPLIT_THRESHOLD = 256L * 1024 * 1024;
    private static final FramedRunWriter.Options RUN_OPTIONS = new FramedRunWriter.Options(
        FramedRunLayout.KIND_SLICE,
        CHUNK_BYTES,
        2,
        FramedRunMaxima.DEFAULT
    );

    /**
     * What the operator hears from the registry's asynchronous work: a generation published, or a write or a restore
     * that failed. Both may be called from a codec or I/O thread.
     */
    interface Events {
        void generationPublished();

        /**
         * A slice landed, success or failure: a worker parked on it may make progress, and so may the driver.
         */
        void restoreLanded();

        void failed(Exception e);
    }

    private final CircuitBreaker globalBreaker;
    private final List<Generation> generations = new ArrayList<>();
    @Nullable
    private final Spill spill;

    private final AtomicInteger nextPartition = new AtomicInteger();
    private final AtomicInteger completedPartitions = new AtomicInteger();

    /**
     * A registry outside any query scope, or in one that does not spill: every generation stays resident.
     */
    PartitionedHashAggregations(CircuitBreaker globalBreaker) {
        this.globalBreaker = globalBreaker;
        this.spill = null;
    }

    /**
     * A registry that may spill.
     *
     * @param scope the query's scope, which decides when a generation spills and owns the store
     * @param driverContext the operator's driver context, whose asynchronous actions the writes and restores are
     * @param decoder an operator of the same shape that never sees input: its block hash and aggregators decode slices
     * @param workers how many workers split, which sizes the raised split threshold
     */
    PartitionedHashAggregations(
        CircuitBreaker globalBreaker,
        QueryScope scope,
        DriverContext driverContext,
        HashAggregationOperator decoder,
        int workers,
        Events events
    ) {
        this.globalBreaker = globalBreaker;
        this.spill = new Spill(scope, driverContext, decoder, workers, events);
    }

    /**
     * Claims the next partition to emit, or returns {@link #NO_MORE_PARTITION} if every partition has been claimed.
     */
    int claimPartition() {
        int p = nextPartition.getAndIncrement();
        return p < PartitionedHashTable.NUM_PARTITIONS ? p : NO_MORE_PARTITION;
    }

    boolean hasUncombinedPartitions() {
        return nextPartition.get() < PartitionedHashTable.NUM_PARTITIONS;
    }

    /**
     * Records that one partition has been fully emitted; returns true when this was the last one.
     */
    boolean completePartition() {
        return completedPartitions.incrementAndGet() == PartitionedHashTable.NUM_PARTITIONS;
    }

    /**
     * Whether the registry has started spilling: once it has, every later generation spills too.
     */
    boolean spilling() {
        return spill != null && spill.spilling;
    }

    /**
     * The memory threshold a worker splits at once the registry spills: a share of the budget per worker.
     */
    long spillSplitThreshold() {
        return spill == null ? Long.MAX_VALUE : spill.splitThreshold;
    }

    /**
     * Writes of generations not yet published, which phase two waits for.
     */
    int pendingWrites() {
        return spill == null ? 0 : spill.pendingWrites.get();
    }

    /**
     * Whether a generation waits for a write slot: the workers take no input until it has one.
     */
    boolean writesBacklogged() {
        return spill != null && spill.writesBacklogged();
    }

    /**
     * Completes when the next write finishes, success or failure; what a parked driver waits on.
     */
    SubscribableListener<Void> nextWriteDone() {
        return spill == null ? SubscribableListener.nullSuccess() : spill.nextWriteDone();
    }

    /**
     * Writes of received generations not yet published; the operator takes no input while there is one.
     */
    int pendingReceivedWrites() {
        return spill == null ? 0 : spill.pendingReceivedWrites();
    }

    /**
     * Completes when no received generation's write is pending; inline if none is.
     */
    SubscribableListener<Void> receivedWritesIdle() {
        return spill == null ? SubscribableListener.nullSuccess() : spill.receivedWritesIdle();
    }

    SpillStatus spillStatus() {
        return spill == null ? SpillStatus.NONE : spill.status();
    }

    /**
     * Splits the operator's current batch into a generation and admits it: resident, or written to a slice run when
     * the admission rule says so. The batch is cleared either way.
     */
    void split(CircuitBreaker breaker, HashAggregationOperator op) {
        if (op.blockHash.numKeys() == 0) {
            return;
        }
        if (spill != null && spill.shouldSpill(0)) {
            // charged to the root view, so that the codec pool may release the arrays as it serializes them
            PartitionedKeyAndAggs partitioned = splitKeysAndAggs(spill.scope.rootView(), op);
            // handed over before the batch is cleared, as the resident path registers first: the replacement aggregators
            // the clear builds may trip the breaker, and a generation nobody owns by then would keep its charge for good
            spill.write(partitioned, spill.scope.rootView(), false);
            op.clearCurrentBatch();
            return;
        }
        MeteredBreaker metered = new MeteredBreaker(breaker);
        PartitionedKeyAndAggs partitioned = splitKeysAndAggs(metered, op);
        addResident(partitioned, metered.bytes());
        op.clearCurrentBatch();
    }

    private void addResident(PartitionedKeyAndAggs partitioned, long bytes) {
        synchronized (generations) {
            generations.add(partitioned);
        }
        if (spill != null) {
            spill.residentBytes.addAndGet(bytes);
        }
    }

    /**
     * Releases the registry's hold: aborts the writes in flight, which release their generations, and drops the
     * operator's reference. Restores in flight hold their own references; the last one to complete closes it.
     */
    @Override
    public void close() {
        if (spill != null) {
            spill.abortWrites();
        }
        decRef();
    }

    @Override
    protected void closeInternal() {
        List<Generation> all;
        synchronized (generations) {
            all = new ArrayList<>(generations);
            generations.clear();
        }
        for (Generation gen : all) {
            gen.releaseAll(globalBreaker);
        }
        if (spill != null) {
            spill.close();
        }
    }

    Combiner newCombiner(HashAggregationOperator op) {
        return new Combiner(op);
    }

    static PartitionedAggregationBlock splitToPartitionedBlock(CircuitBreaker breaker, HashAggregationOperator op) {
        int numKeys = op.blockHash.numKeys();
        MeteredBreaker metered = new MeteredBreaker(breaker);
        PartitionedKeyAndAggs keysAndAggs = splitKeysAndAggs(metered, op);
        try {
            var block = new PartitionedAggregationBlock(
                op.driverContext.blockFactory(),
                numKeys,
                keysAndAggs.keys,
                keysAndAggs.aggs.states,
                metered.bytes()
            );
            keysAndAggs = null;
            return block;
        } finally {
            if (keysAndAggs != null) {
                keysAndAggs.releaseAll(breaker);
            }
        }
    }

    private static PartitionedKeyAndAggs splitKeysAndAggs(CircuitBreaker breaker, HashAggregationOperator op) {
        PartitionedHashTable.PartitionedHashKeys partitionedKeys = null;
        MultiAggsPartitionSplitter aggSplitter = new MultiAggsPartitionSplitter(breaker, op.aggregators);
        try {
            partitionedKeys = ((PartitionedHashTable) op.blockHash).splitPartition(breaker, aggSplitter);
            PartitionedKeyAndAggs result = new PartitionedKeyAndAggs(partitionedKeys, aggSplitter.finishAll(breaker));
            partitionedKeys = null;
            return result;
        } finally {
            aggSplitter.release(breaker);
            if (partitionedKeys != null) {
                partitionedKeys.releaseAll(breaker);
            }
        }
    }

    /**
     * Admits a partitioned block received from an earlier operator, on arrival: its keys and states are taken from
     * the block and either kept or, under the admission rule, written to a slice run. The caller closes the block.
     */
    void addPartitionedBlock(PartitionedAggregationBlock block) {
        var generation = new PartitionedKeyAndAggs(block.takeKeys(), new MultiAggsPartitionedState(block.takeAggs()));
        if (spill != null && spill.shouldSpill(0)) {
            // released through the global breaker, the thread-safe account the block was charged against
            spill.write(generation, globalBreaker, true);
            return;
        }
        addResident(generation, block.ramBytesUsed());
    }

    /**
     * Combines one partition from every generation, one generation at a time: its keys, then every aggregator's state
     * for them, with one id mapping sized to the largest generation of the partition rather than one per generation.
     * Whether an aggregator must track the groups it has seen is decided before any state is combined, from the
     * all-values flags the generations recorded when they were split.
     * <p>
     * A spilled generation's slice is read ahead, at most {@link #READ_AHEAD} at a time, and decoded on the I/O
     * thread; {@link #advance()} combines what has landed and hands back the next read to wait on.
     */
    final class Combiner implements Releasable {
        private final HashAggregationOperator op;
        private final CircuitBreaker breaker;
        private int[] ids = null;
        /** The partition in progress, or -1. */
        private int partition = -1;
        /** The generations of the partition in progress: frozen once phase two starts. */
        private List<Generation> frozen;
        /** The next generation to combine, and the next to read ahead. */
        private int next;
        private int nextRead;
        private final ArrayDeque<Restore> reads = new ArrayDeque<>(READ_AHEAD);

        Combiner(HashAggregationOperator op) {
            this.op = op;
            this.breaker = op.driverContext.breaker();
        }

        boolean inProgress() {
            return partition != -1;
        }

        /**
         * Starts a partition: clears the worker's batch and decides group tracking.
         */
        void begin(int p) {
            assert partition == -1 : "partition [" + partition + "] still in progress";
            synchronized (generations) {
                frozen = List.copyOf(generations);
            }
            op.clearCurrentBatch();
            List<GroupingAggregator> aggregators = op.aggregators;
            // If some group in any generation is missing a value we need to track groupIds
            for (int i = 0; i < aggregators.size(); i++) {
                for (Generation gen : frozen) {
                    if (gen.hasAllValues(i, p) == false) {
                        aggregators.get(i).aggregatorFunction().selectedMayContainUnseenGroups(new SeenGroupIds.Empty());
                        break;
                    }
                }
            }
            partition = p;
            next = 0;
            nextRead = 0;
        }

        /**
         * Combines the generations whose slices are at hand, resident ones and restored ones in order, reading ahead
         * as it goes. Returns the restore to wait for when the next generation's slice has not landed, or null once
         * every generation of the partition is combined, which ends the partition.
         */
        @Nullable
        SpillCompletion<SliceRunReader.Slice> advance() {
            assert partition != -1 : "no partition in progress";
            PartitionedBlockHash blockHash = (PartitionedBlockHash) op.blockHash;
            while (true) {
                readAhead();
                if (next == frozen.size()) {
                    partition = -1;
                    frozen = null;
                    return null;
                }
                Generation gen = frozen.get(next);
                if (gen instanceof PartitionedKeyAndAggs resident) {
                    combine(blockHash, resident.keys, resident.aggs.states, resident.keys.keysInPartition(partition));
                    next++;
                    continue;
                }
                Spilled spilled = (Spilled) gen;
                if (spilled.run.hasSlice(partition) == false) {
                    next++;
                    continue;
                }
                Restore head = reads.peekFirst();
                assert head != null && head.generation == next : "restore of generation [" + next + "] not issued";
                if (head.completion.isDone() == false) {
                    return head.completion;
                }
                reads.pollFirst();
                Exception failure = head.completion.failure();
                if (failure != null) {
                    throw failure instanceof RuntimeException runtime ? runtime : new RuntimeException(failure);
                }
                SliceRunReader.Slice slice = head.completion.result();
                try {
                    if (slice.keyCount() != spilled.run.keyCount()[partition]) {
                        throw new IllegalStateException(
                            "partition ["
                                + partition
                                + "] restored ["
                                + slice.keyCount()
                                + "] keys, expected ["
                                + spilled.run.keyCount()[partition]
                                + "]"
                        );
                    }
                    combine(blockHash, slice.keys(), slice.states(), slice.keyCount());
                } finally {
                    slice.release(breaker);
                }
                next++;
            }
        }

        private void readAhead() {
            while (reads.size() < READ_AHEAD && nextRead < frozen.size()) {
                int g = nextRead++;
                if (frozen.get(g) instanceof Spilled spilled && spilled.run.hasSlice(partition)) {
                    reads.addLast(spill.restore(spilled, g, partition));
                }
            }
        }

        private void combine(
            PartitionedBlockHash blockHash,
            PartitionedHashTable.PartitionedHashKeys keys,
            GroupingAggregatorFunction.PartitionedState[] states,
            int numKeys
        ) {
            ensureIds(numKeys);
            boolean appendOnly = blockHash.combinePartition(keys, partition, ids);
            op.rowsAddedInCurrentBatch += numKeys;
            keys.releasePartition(breaker, partition);
            List<GroupingAggregator> aggregators = op.aggregators;
            for (int i = 0; i < aggregators.size(); i++) {
                final var aggregator = aggregators.get(i).aggregatorFunction();
                aggregator.maybeEnsureCapacity(blockHash.numKeys() + 1);
                aggregator.combinePartition(states[i], partition, appendOnly, ids, numKeys);
                states[i].releasePartition(breaker, partition);
            }
        }

        private void ensureIds(int numKeys) {
            final int oldLength = ids == null ? 0 : ids.length;
            if (ids == null || oldLength < numKeys) {
                final int newLength = ArrayUtil.oversize(numKeys, Integer.BYTES);
                breaker.addEstimateBytesAndMaybeBreak((long) (newLength - oldLength) * Integer.BYTES, "PartitionedHashAggregations");
                ids = new int[newLength];
            }
        }

        @Override
        public void close() {
            if (ids != null) {
                breaker.addWithoutBreaking(-(long) ids.length * Integer.BYTES, "PartitionedHashAggregations");
                ids = null;
            }
            // a slice that landed is released through the root view, as one landing after this close is
            Restore read;
            while ((read = reads.pollFirst()) != null) {
                read.discard();
            }
            partition = -1;
            frozen = null;
        }
    }

    /**
     * One slice read in flight; a slice landing after the read was discarded goes straight back to the breaker.
     */
    private final class Restore {
        final int generation;
        final SpillCompletion<SliceRunReader.Slice> completion = new SpillCompletion<>();

        Restore(int generation) {
            this.generation = generation;
        }

        void discard() {
            completion.addListener(ActionListener.wrap(slice -> slice.release(spill.scope.rootView()), e -> {}));
        }
    }

    private sealed interface Generation permits PartitionedKeyAndAggs, Spilled {
        boolean hasAllValues(int aggregator, int partition);

        void releaseAll(CircuitBreaker breaker);
    }

    /**
     * One resident generation: its keys and states, with the all-values flag of every state for every partition read
     * once, when the generation is admitted, so that a combine decides on group tracking without touching the states.
     */
    private record PartitionedKeyAndAggs(
        PartitionedHashTable.PartitionedHashKeys keys,
        MultiAggsPartitionedState aggs,
        boolean[][] allValues
    ) implements Generation {
        PartitionedKeyAndAggs(PartitionedHashTable.PartitionedHashKeys keys, MultiAggsPartitionedState aggs) {
            this(keys, aggs, allValuesOf(aggs.states));
        }

        @Override
        public boolean hasAllValues(int aggregator, int partition) {
            return allValues[aggregator][partition];
        }

        static boolean[][] allValuesOf(GroupingAggregatorFunction.PartitionedState[] states) {
            boolean[][] allValues = new boolean[states.length][PartitionedHashTable.NUM_PARTITIONS];
            for (int i = 0; i < states.length; i++) {
                for (int p = 0; p < PartitionedHashTable.NUM_PARTITIONS; p++) {
                    allValues[i][p] = states[i].hasAllValues(p);
                }
            }
            return allValues;
        }

        @Override
        public void releaseAll(CircuitBreaker breaker) {
            keys.releaseAll(breaker);
            aggs.releaseAll(breaker);
        }
    }

    /**
     * A generation being written or waiting for a write slot, with what its arrays are charged to. The arrivals count
     * the write's own completion and every queued record's release; the last to arrive releases what the generation
     * still holds, so that no thread releases a partition another is still serializing.
     */
    private record PendingWrite(
        PartitionedKeyAndAggs generation,
        CircuitBreaker breaker,
        boolean received,
        long startedNanos,
        AtomicInteger outstandingReleases
    ) {
        void arrive() {
            if (outstandingReleases.decrementAndGet() == 0) {
                generation.releaseAll(breaker);
            }
        }
    }

    /**
     * One spilled generation: its published run with the metadata, and the reader over it. A partition without keys
     * has no record and counts as having every value.
     */
    private record Spilled(SliceRunWriter.SliceRun run, SliceRunReader reader) implements Generation {
        @Override
        public boolean hasAllValues(int aggregator, int partition) {
            return run.hasSlice(partition) == false || run.allValues()[aggregator][partition];
        }

        /**
         * What the metadata holds: the key counts, the record sizes and the all-values flags.
         */
        static long metadataBytes(SliceRunWriter.SliceRun run) {
            return (long) PartitionedHashTable.NUM_PARTITIONS * (Integer.BYTES + Long.BYTES + run.allValues().length) + 64;
        }

        @Override
        public void releaseAll(CircuitBreaker breaker) {
            breaker.addWithoutBreaking(-metadataBytes(run));
            Releasables.close(reader);
            deleteQuietly(run.run());
        }
    }

    private static void deleteQuietly(FramedRun run) {
        run.object().delete(ActionListener.wrap(ignored -> {}, e -> {
            logger.debug(() -> "spill run [" + run.name() + "] could not be deleted; the scope's close removes it", e);
        }));
    }

    private record MultiAggsPartitionedState(GroupingAggregatorFunction.PartitionedState[] states) {
        void releaseAll(CircuitBreaker breaker) {
            for (var state : states) {
                state.releaseAll(breaker);
            }
        }
    }

    /**
     * The spill side of the registry: the admission rule, the write of a generation to a slice run and the restore of
     * a slice, with the counters of both. Every write and restore is an asynchronous action of the operator's driver
     * and holds a reference on the registry, so that the decoder outlives whatever decodes on an I/O thread.
     */
    private final class Spill {
        final QueryScope scope;
        final DriverContext driverContext;
        final HashAggregationOperator decoder;
        final PartitionedBlockHash codecHash;
        final List<GroupingAggregatorFunction> decoders;
        final Events events;
        final long splitThreshold;
        final AtomicLong residentBytes = new AtomicLong();
        volatile boolean spilling;
        final AtomicInteger pendingWrites = new AtomicInteger();
        private int pendingReceived;
        private SubscribableListener<Void> receivedIdle = SubscribableListener.nullSuccess();
        private int writesInFlight;
        private final ArrayDeque<PendingWrite> queued = new ArrayDeque<>();
        private SubscribableListener<Void> writeDone = new SubscribableListener<>();
        private final Set<FramedRunWriter> writersInFlight = Collections.synchronizedSet(new HashSet<>());
        private final AtomicInteger runNames = new AtomicInteger();
        private int spilledGenerations;
        private final AtomicLong spilledBytes = new AtomicLong();
        private final AtomicLong readBackBytes = new AtomicLong();
        private final AtomicInteger spillRuns = new AtomicInteger();
        private final AtomicLong spillNanos = new AtomicLong();

        Spill(QueryScope scope, DriverContext driverContext, HashAggregationOperator decoder, int workers, Events events) {
            this.scope = scope;
            this.driverContext = driverContext;
            this.decoder = decoder;
            this.codecHash = (PartitionedBlockHash) decoder.blockHash;
            this.decoders = decoder.aggregators.stream().map(GroupingAggregator::aggregatorFunction).toList();
            this.events = events;
            this.splitThreshold = Math.min(MAX_SPILL_SPLIT_THRESHOLD, Math.max(1, scope.budgetBytes() / (4L * Math.max(1, workers))));
        }

        /**
         * The admission rule: once one generation spilled, all later ones do; before that, the scope's policy over what
         * the registry holds resident.
         */
        boolean shouldSpill(long estimate) {
            if (spilling) {
                return true;
            }
            if (scope.shouldSpill(residentBytes.get(), estimate)) {
                spilling = true;
                return true;
            }
            return false;
        }

        synchronized int pendingReceivedWrites() {
            return pendingReceived;
        }

        synchronized SubscribableListener<Void> receivedWritesIdle() {
            return receivedIdle;
        }

        private synchronized void beginReceived() {
            if (pendingReceived++ == 0) {
                receivedIdle = new SubscribableListener<>();
            }
        }

        private void endReceived() {
            SubscribableListener<Void> idle = null;
            synchronized (this) {
                if (--pendingReceived == 0) {
                    idle = receivedIdle;
                }
            }
            if (idle != null) {
                idle.onResponse(null);
            }
        }

        /**
         * Writes a generation to a slice run of its own and publishes it, or queues it when {@link #MAX_WRITES_IN_FLIGHT}
         * are being written. The generation's arrays are released, through {@code breaker}, as the codec serializes
         * them; {@code breaker} must be safe to use from the codec pool.
         */
        void write(PartitionedKeyAndAggs generation, CircuitBreaker breaker, boolean received) {
            try {
                // a driver that has finished refuses: a worker may still be splitting when the operator closes
                driverContext.addAsyncAction();
            } catch (IllegalStateException e) {
                generation.releaseAll(breaker);
                throw e;
            }
            synchronized (generations) {
                if (spilledGenerations + pendingWrites.get() >= MAX_SPILLED_GENERATIONS) {
                    driverContext.removeAsyncAction();
                    generation.releaseAll(breaker);
                    throw new ElasticsearchStatusException(
                        "aggregation spilled [{}] generations, the most it may; raise esql.spill.query_budget or the"
                            + " partitioning thresholds",
                        RestStatus.TOO_MANY_REQUESTS,
                        MAX_SPILLED_GENERATIONS
                    );
                }
                pendingWrites.incrementAndGet();
            }
            if (received) {
                beginReceived();
            }
            mustIncRef();
            PendingWrite pending = new PendingWrite(generation, breaker, received, System.nanoTime(), new AtomicInteger(1));
            boolean start;
            synchronized (this) {
                start = writesInFlight < MAX_WRITES_IN_FLIGHT;
                if (start) {
                    writesInFlight++;
                } else {
                    queued.addLast(pending);
                }
            }
            if (start) {
                start(pending);
            }
        }

        synchronized boolean writesBacklogged() {
            return queued.isEmpty() == false;
        }

        synchronized SubscribableListener<Void> nextWriteDone() {
            return writeDone;
        }

        private void start(PendingWrite pending) {
            String name = "aggregation-" + runNames.getAndIncrement();
            SubscribableListener.<FramedRunWriter>newForked(
                l -> FramedRunWriter.create(scope.spillScope(), name, scope.rootView(), scope.codecExecutor(), RUN_OPTIONS, l)
            ).<SliceRunWriter.SliceRun>andThen((l, writer) -> appendAll(writer, pending, l))
                .<Spilled>andThen((l, run) -> open(run, l))
                .addListener(ActionListener.runAfter(ActionListener.wrap(spilled -> {
                    spillRuns.incrementAndGet();
                    spilledBytes.addAndGet(spilled.run().run().footer().logicalBytes());
                    spillNanos.addAndGet(System.nanoTime() - pending.startedNanos());
                    synchronized (generations) {
                        generations.add(spilled);
                        spilledGenerations++;
                        pendingWrites.decrementAndGet();
                    }
                    if (pending.received()) {
                        endReceived();
                    }
                    events.generationPublished();
                }, e -> {
                    pendingWrites.decrementAndGet();
                    if (pending.received()) {
                        endReceived();
                    }
                    events.failed(e);
                }), () -> {
                    // the partitions went as they were serialized; the generation's base arrays go once every record's
                    // release has run, which may be after this when the codec thread is still inside the last record
                    pending.arrive();
                    PendingWrite next;
                    SubscribableListener<Void> done;
                    synchronized (this) {
                        next = queued.pollFirst();
                        if (next == null) {
                            writesInFlight--;
                        }
                        done = writeDone;
                        writeDone = new SubscribableListener<>();
                    }
                    done.onResponse(null);
                    if (next != null) {
                        start(next);
                    }
                    decRef();
                    driverContext.removeAsyncAction();
                }));
        }

        /**
         * Lets the queued generations go without writing them: the operator is closing.
         */
        private void dropQueued() {
            List<PendingWrite> dropped;
            SubscribableListener<Void> done;
            synchronized (this) {
                dropped = new ArrayList<>(queued);
                queued.clear();
                done = writeDone;
                writeDone = new SubscribableListener<>();
            }
            for (PendingWrite pending : dropped) {
                pending.generation().releaseAll(pending.breaker());
                pendingWrites.decrementAndGet();
                if (pending.received()) {
                    endReceived();
                }
                decRef();
                driverContext.removeAsyncAction();
            }
            done.onResponse(null);
        }

        /**
         * Queues every non-empty partition. Each record's release runs where the codec finished with it and counts
         * down on the write's arrivals, so that the generation's base arrays go only once no thread touches a partition.
         */
        private void appendAll(FramedRunWriter writer, PendingWrite pending, ActionListener<SliceRunWriter.SliceRun> listener) {
            writersInFlight.add(writer);
            PartitionedKeyAndAggs generation = pending.generation();
            CircuitBreaker breaker = pending.breaker();
            GroupingAggregatorFunction.PartitionedState[] states = generation.aggs.states;
            SliceRunWriter slice = new SliceRunWriter(writer, states.length);
            try {
                for (int p = 0; p < PartitionedHashTable.NUM_PARTITIONS; p++) {
                    final int partition = p;
                    final int count = generation.keys.keysInPartition(partition);
                    if (count == 0) {
                        releasePartition(generation, breaker, partition);
                        continue;
                    }
                    boolean[] allValues = new boolean[states.length];
                    List<CheckedConsumer<StreamOutput, IOException>> stateWriters = new ArrayList<>(states.length);
                    for (int i = 0; i < states.length; i++) {
                        allValues[i] = generation.hasAllValues(i, partition);
                        final GroupingAggregatorFunction.PartitionedState state = states[i];
                        stateWriters.add(out -> state.writePartition(partition, count, out));
                    }
                    pending.outstandingReleases().incrementAndGet();
                    boolean queued = false;
                    try {
                        slice.append(
                            partition,
                            count,
                            allValues,
                            out -> codecHash.writePartitionKeys(generation.keys, partition, out),
                            stateWriters,
                            () -> {
                                releasePartition(generation, breaker, partition);
                                pending.arrive();
                            },
                            ActionListener.noop()
                        );
                        queued = true;
                    } finally {
                        if (queued == false) {
                            // refused before it was queued: nothing releases it but the generation's own release
                            pending.outstandingReleases().decrementAndGet();
                        }
                    }
                }
            } catch (Exception e) {
                // the queued records are released by the abort, the rest by the generation's release once it is done
                writersInFlight.remove(writer);
                writer.abort(ActionListener.running(() -> listener.onFailure(e)));
                return;
            }
            slice.finish(ActionListener.runBefore(listener, () -> writersInFlight.remove(writer)));
        }

        /**
         * Charges the generation's metadata and opens the run for reading; a failure lets the published run go.
         */
        private void open(SliceRunWriter.SliceRun run, ActionListener<Spilled> listener) {
            long metadataBytes = Spilled.metadataBytes(run);
            try {
                scope.rootView().addEstimateBytesAndMaybeBreak(metadataBytes, "spilled generation");
            } catch (CircuitBreakingException e) {
                deleteQuietly(run.run());
                listener.onFailure(e);
                return;
            }
            FramedRunReader.open(
                run.run().name(),
                run.run().object(),
                scope.rootView(),
                FramedRunMaxima.DEFAULT,
                CHUNK_BYTES,
                ActionListener.wrap(
                    reader -> listener.onResponse(new Spilled(run, new SliceRunReader(reader, codecHash, decoders, scope.rootView()))),
                    e -> {
                        scope.rootView().addWithoutBreaking(-metadataBytes);
                        deleteQuietly(run.run());
                        listener.onFailure(e);
                    }
                )
            );
        }

        private static void releasePartition(PartitionedKeyAndAggs generation, CircuitBreaker breaker, int partition) {
            generation.keys.releasePartition(breaker, partition);
            for (GroupingAggregatorFunction.PartitionedState state : generation.aggs.states) {
                state.releasePartition(breaker, partition);
            }
        }

        /**
         * Issues the read of one slice; the slice is decoded on the I/O thread and charged to the root view.
         */
        Restore restore(Spilled spilled, int generation, int partition) {
            Restore restore = new Restore(generation);
            driverContext.addAsyncAction();
            mustIncRef();
            long started = System.nanoTime();
            ActionListener<SliceRunReader.Slice> listener = ActionListener.runAfter(ActionListener.wrap(slice -> {
                readBackBytes.addAndGet(spilled.run.recordBytes()[partition]);
                spillNanos.addAndGet(System.nanoTime() - started);
                restore.completion.listener().onResponse(slice);
                events.restoreLanded();
            }, e -> {
                Exception failure = e instanceof CircuitBreakingException
                    ? e
                    : new IllegalStateException("failed to restore partition [" + partition + "] of a spilled aggregation", e);
                restore.completion.listener().onFailure(failure);
                events.failed(failure);
                events.restoreLanded();
            }), () -> {
                decRef();
                driverContext.removeAsyncAction();
            });
            try {
                spilled.reader.readSlice(partition, listener);
            } catch (Exception e) {
                if (restore.completion.isDone() == false) {
                    listener.onFailure(e);
                } else {
                    throw e;
                }
            }
            return restore;
        }

        void abortWrites() {
            dropQueued();
            List<FramedRunWriter> writers;
            synchronized (writersInFlight) {
                writers = new ArrayList<>(writersInFlight);
            }
            for (FramedRunWriter writer : writers) {
                writer.abort(ActionListener.noop());
            }
        }

        SpillStatus status() {
            return new SpillStatus(spilledBytes.get(), readBackBytes.get(), spillRuns.get(), spillNanos.get());
        }

        void close() {
            decoder.close();
        }
    }

    /**
     * Charges and releases through its parent and counts what passes, so that a generation's size is known from the
     * split that produced it, whatever account the arrays end up released against.
     */
    private static final class MeteredBreaker implements DelegatingCircuitBreaker {
        private final CircuitBreaker parent;
        private long bytes;

        MeteredBreaker(CircuitBreaker parent) {
            this.parent = parent;
        }

        long bytes() {
            return bytes;
        }

        @Override
        public CircuitBreaker parentBreaker() {
            return parent;
        }

        @Override
        public void circuitBreak(String fieldName, long bytesNeeded) {
            parent.circuitBreak(fieldName, bytesNeeded);
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
            parent.addEstimateBytesAndMaybeBreak(bytes, label);
            this.bytes += bytes;
        }

        @Override
        public void addWithoutBreaking(long bytes) {
            parent.addWithoutBreaking(bytes);
            this.bytes += bytes;
        }

        @Override
        public long getUsed() {
            return parent.getUsed();
        }

        @Override
        public long getLimit() {
            return parent.getLimit();
        }

        @Override
        public double getOverhead() {
            return parent.getOverhead();
        }

        @Override
        public long getTrippedCount() {
            return parent.getTrippedCount();
        }

        @Override
        public String getName() {
            return parent.getName();
        }

        @Override
        public Durability getDurability() {
            return parent.getDurability();
        }

        @Override
        public void setLimitAndOverhead(long limit, double overhead) {
            parent.setLimitAndOverhead(limit, overhead);
        }
    }

    private static class MultiAggsPartitionSplitter implements PartitionedHashTable.PartitionSplitter {
        final GroupingAggregatorFunction.PartitionSplitter[] splitters;

        MultiAggsPartitionSplitter(CircuitBreaker breaker, List<GroupingAggregator> aggregators) {
            this.splitters = new GroupingAggregatorFunction.PartitionSplitter[aggregators.size()];
            boolean success = false;
            try {
                for (int i = 0; i < splitters.length; i++) {
                    splitters[i] = aggregators.get(i).aggregatorFunction().createPartitioningSplitter(breaker);
                }
                success = true;
            } finally {
                if (success == false) {
                    release(breaker);
                }
            }
        }

        public MultiAggsPartitionedState finishAll(CircuitBreaker breaker) {
            GroupingAggregatorFunction.PartitionedState[] states = new GroupingAggregatorFunction.PartitionedState[splitters.length];
            boolean success = false;
            try {
                for (int i = 0; i < splitters.length; i++) {
                    states[i] = splitters[i].finish();
                }
                success = true;
            } finally {
                if (success == false) {
                    for (var state : states) {
                        if (state != null) {
                            state.releaseAll(breaker);
                        }
                    }
                }
            }
            return new MultiAggsPartitionedState(states);
        }

        @Override
        public void split(int firstId, short[] shiftedIds, int batchSize, int[] batchPartitionCounts, int[] partitionOffsets) {
            for (var splitter : splitters) {
                splitter.split(firstId, shiftedIds, batchSize, batchPartitionCounts, partitionOffsets);
            }
        }

        @Override
        public void release(CircuitBreaker breaker) {
            for (int i = 0; i < splitters.length; i++) {
                var splitter = splitters[i];
                if (splitter != null) {
                    splitters[i] = null;
                    splitter.release(breaker);
                }
            }
        }
    }
}
