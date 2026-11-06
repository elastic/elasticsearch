/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import com.carrotsearch.hppc.BitMixer;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.PagedBytesBuilder;
import org.elasticsearch.common.bytes.PagedBytesCursor;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefHashTable;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.compute.aggregation.blockhash.HashImplFactory;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.spill.DriverSpillOperation;
import org.elasticsearch.compute.spill.FramedRun;
import org.elasticsearch.compute.spill.FramedRunLayout;
import org.elasticsearch.compute.spill.FramedRunMaxima;
import org.elasticsearch.compute.spill.FramedRunReader;
import org.elasticsearch.compute.spill.FramedRunWriter;
import org.elasticsearch.compute.spill.PageRunReader;
import org.elasticsearch.compute.spill.PageRunWriter;
import org.elasticsearch.compute.spill.QueryBreaker;
import org.elasticsearch.compute.spill.QueryScope;
import org.elasticsearch.compute.spill.SpillStatus;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;

/**
 * An operator that limits the number of rows emitted per group.
 * For {@code LIMIT N BY x}, this accepts up to N rows for each distinct value of x.
 * Group keys use list semantics for multivalues: {@code [1,2]} and {@code [2,1]} are different groups.
 * <p>
 * Spilling, when the driver's query has a scope that spills: the operator acquires its headroom, a run writer with its
 * buffers, once its own state reaches the query's own-state minimum, and it freezes its key set when the query would
 * cross its budget, or when the reservation it takes before a page fails. From the freeze on, a row of a key seen
 * before keeps streaming in arrival order, counted as before, and a row of any other key goes to one of
 * {@link #PARTITIONS} runs chosen by a hash of the key bytes already in hand, so memory stops growing at that moment.
 * After the input ends the frozen set is released and each partition is replayed, in arrival order, through a fresh
 * operator with the whole budget, which may freeze and partition again with another seed, up to {@link #MAX_DEPTH}.
 * Nothing of the key set is ever serialized: the keys in the runs are disjoint from the frozen keys by construction, so
 * no emitted row is emitted twice and no count travels to disk. Output order changes for the keys first seen after
 * the freeze only, which {@code LIMIT BY} and {@code DEDUP} allow without {@code SORT}.
 */
public class GroupedLimitOperator implements Operator, Accountable {

    private static final Logger logger = LogManager.getLogger(GroupedLimitOperator.class);

    public static final class Factory implements Operator.OperatorFactory {
        private final int limitPerGroup;
        private final int[] groupChannels;
        private final List<ElementType> elementTypes;

        public Factory(int limitPerGroup, List<Integer> groupChannels, List<ElementType> elementTypes) {
            this.limitPerGroup = limitPerGroup;
            this.groupChannels = groupChannels.stream().mapToInt(Integer::intValue).toArray();
            this.elementTypes = elementTypes;
        }

        @Override
        public GroupedLimitOperator get(DriverContext driverContext) {
            BlockFactory blockFactory = driverContext.blockFactory();
            PagedBytesBuilder row = new PagedBytesBuilder(
                blockFactory.bigArrays().recycler(),
                blockFactory.breaker(),
                "group-key-encoder",
                64
            );
            return new GroupedLimitOperator(
                limitPerGroup,
                new GroupKeyEncoder(groupChannels, elementTypes, row),
                blockFactory,
                driverContext,
                0,
                0
            );
        }

        @Override
        public String describe() {
            return "GroupedLimitOperator[limitPerGroup = " + limitPerGroup + "]";
        }
    }

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(GroupedLimitOperator.class);

    /** Runs a frozen operator routes the rows of new keys to; a partition's key set is about this fraction of the parent's. */
    static final int PARTITIONS = 64;
    /**
     * How deep the replay may freeze again; past it an operator does not spill and a trip fails the query as today.
     * Three levels of 64 partitions split a key set a quarter of a million ways, and every level costs each surviving
     * row one more trip to the store.
     */
    static final int MAX_DEPTH = 3;
    /**
     * Appends queued at the run writer before input is suspended, checked per input page: one page routes to up to
     * {@link #PARTITIONS} parts, so the bytes in flight are at most that of a page plus the writer's buffers.
     */
    static final int MAX_APPENDS_IN_FLIGHT = 2;
    /** Rounds a replay may spend neither producing, consuming, finishing nor waiting before it is declared stalled. */
    static final int MAX_IDLE_ROUNDS = 100_000;
    /** Payload bytes per chunk of a run, and the reader's window. */
    static final int CHUNK_BYTES = 256 * 1024;
    private static final FramedRunWriter.Options RUN_OPTIONS = new FramedRunWriter.Options(
        FramedRunLayout.KIND_PAGE,
        CHUNK_BYTES,
        2,
        FramedRunMaxima.DEFAULT
    );

    private final int limitPerGroup;
    private final GroupKeyEncoder keyEncoder;
    private final BlockFactory blockFactory;
    /**
     * BlockHash unrolls multivalues (i.e. for each unique element of a multivalue [1,2,2,1] it would create
     * a new key).
     *
     * <p>In contrast, in this operator we consider every multivalue as a key. Two multivalues are equal if they hold
     * the same values in the exact same order. That's why we need to use:
     * <ul>
     *   <li>a ref hash table. Every time we add a new key it gives a monotonically increasing integer that we will
     *       use to index in the counts array, {@code Ordinal(key)}</li>
     *   <li>a counts array to keep track of the number of times we have seen each {@code Ordinal(key)}</li>
     * </ul>
     * Both are released at the end of the input once frozen, since the replay needs neither.
     */
    @Nullable
    private BytesRefHashTable seenKeys;
    private BigArrays bigArrays;
    @Nullable
    private IntArray counts;
    private long frozenGroupCount;

    private int pagesProcessed;
    private long rowsReceived;
    private long rowsEmitted;

    private Page lastOutput;
    private boolean finished;

    // ---- spilling; every field below is touched by the driver thread only, except where noted

    /** The driver's context, for the async actions of the spill operations; null for an operator built without one. */
    @Nullable
    private final DriverContext driverContext;
    /** The query's scope when it spills, null otherwise: the pre-feature paths then run untouched. */
    @Nullable
    private final QueryScope scope;
    private final int depth;
    private final int seed;
    /** Whether this operator may freeze: a scope that spills, depth below the maximum, headroom not refused. */
    private boolean spillCapable;
    /** The headroom: the run writer and its buffers, acquired lazily. */
    @Nullable
    private DriverSpillOperation<FramedRunWriter> headroom;
    @Nullable
    private PageRunWriter overflow;
    private boolean frozen;
    /** A page admitted after the freeze while the writer was still being created; routed when it exists. */
    @Nullable
    private Page pending;
    private final Object appendLock = new Object();
    /** Appends issued and not yet completed; guarded by {@link #appendLock}, decremented from the codec thread. */
    private int appendsInFlight;
    /** Completed when an append completes, for an operator waiting at {@link #MAX_APPENDS_IN_FLIGHT}; guarded by {@link #appendLock}. */
    @Nullable
    private SubscribableListener<Void> appendWait;
    /** The first failure of an append, set from the codec thread, thrown on the driver thread. */
    @Nullable
    private volatile Exception spillFailure;
    @Nullable
    private DriverSpillOperation<FramedRun> finishing;
    @Nullable
    private FramedRun run;
    @Nullable
    private DriverSpillOperation<FramedRunReader> opening;
    @Nullable
    private FramedRunReader runReader;
    private int replayPartition = -1;
    @Nullable
    private PageRunReader partitionReader;
    @Nullable
    private GroupedLimitOperator replay;
    private boolean replayFinished;
    /** Consecutive rounds in which the replay neither produced, consumed, finished nor waited; a stall past the bound fails. */
    private int idleRounds;
    @Nullable
    private DriverSpillOperation<Page> nextPage;
    @Nullable
    private DriverSpillOperation<Void> deleting;
    /**
     * The node's shard references, taken from the first document block spilled, and the shards this operator holds a
     * reference on for the run's life: a spilled page releases its own references, and the rows come back on this node.
     */
    @Nullable
    private IndexedByShardId<? extends RefCounted> docRefs;
    private final BitSet pinnedShards = new BitSet();

    private long rowsSpilled;
    private int partitionsReplayed;
    private long spilledBytes;
    private long readBackBytes;
    private int spillRuns;
    private long spillNanos;
    private long waitStartedNanos;
    /** The totals of the replay operators that are gone. */
    private SpillStatus childSpill = SpillStatus.NONE;

    /**
     * An operator outside any query scope: today's paths, nothing spills.
     */
    public GroupedLimitOperator(int limitPerGroup, GroupKeyEncoder keyEncoder, BlockFactory blockFactory) {
        this(limitPerGroup, keyEncoder, blockFactory, null, 0, 0);
    }

    /**
     * @param driverContext the driver's context, whose query scope decides whether this operator may spill
     * @param depth how many freezes this operator's input went through already; zero for the planned operator
     * @param seed changes the partition routing at each depth, so a partition that freezes again splits differently
     */
    GroupedLimitOperator(
        int limitPerGroup,
        GroupKeyEncoder keyEncoder,
        BlockFactory blockFactory,
        @Nullable DriverContext driverContext,
        int depth,
        int seed
    ) {
        boolean success = false;
        try {
            this.limitPerGroup = limitPerGroup;
            this.keyEncoder = keyEncoder;
            this.blockFactory = blockFactory;
            this.bigArrays = blockFactory.bigArrays();
            this.seenKeys = HashImplFactory.newBytesRefHash(blockFactory);
            this.counts = bigArrays.newIntArray(16, false);
            this.driverContext = driverContext;
            QueryScope queryScope = driverContext == null ? null : driverContext.queryScope();
            this.scope = queryScope != null && queryScope.spillEnabled() ? queryScope : null;
            this.depth = depth;
            this.seed = seed;
            this.spillCapable = this.scope != null && depth < MAX_DEPTH;
            success = true;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(keyEncoder, seenKeys);
            }
        }
    }

    @Override
    public boolean needsInput() {
        return finished == false && lastOutput == null && pending == null && (scope == null || appendsBlocked().listener().isDone());
    }

    @Override
    public void addInput(Page page) {
        if (scope == null) {
            addInputResident(page);
            return;
        }
        // the page is the operator's from here on: a throw before it was admitted, routed or held releases it
        boolean handedOver = false;
        try {
            checkSpillFailure();
            assert lastOutput == null : "has pending output page";
            assert pending == null : "has a page held";
            rowsReceived += page.getPositionCount();
            if (limitPerGroup == 0) {
                handedOver = true;
                page.releaseBlocks();
                return;
            }
            if (frozen) {
                handedOver = true;
                routeOrHold(page);
                return;
            }
            maybeAcquireHeadroom();
            resolveHeadroom();
            long estimate = estimateFor(page);
            if (spillCapable && headroom != null && scope.shouldSpill(ramBytesUsed(), estimate)) {
                freeze("the query would cross its budget");
                handedOver = true;
                routeOrHold(page);
                return;
            }
            // the reservation before the page is the backstop at a safe point: it fails here, with the table and the
            // counts untouched, rather than in the middle of the page; with headroom in hand that failure is the
            // freeze, without it the query fails as it does today
            QueryBreaker.View view = driverContext.queryView();
            boolean reserved = false;
            if (view != null) {
                try {
                    view.reserve(estimate, "grouped limit page");
                    reserved = true;
                } catch (CircuitBreakingException e) {
                    if (spillCapable && headroom != null) {
                        freeze("the reservation before the page failed");
                        handedOver = true;
                        routeOrHold(page);
                        return;
                    }
                    throw e;
                }
            }
            try {
                handedOver = true;
                admit(page);
            } finally {
                if (reserved) {
                    view.releaseReserve();
                }
            }
        } catch (RuntimeException e) {
            if (handedOver == false) {
                page.releaseBlocks();
            }
            throw e;
        }
    }

    /**
     * The pre-feature path, untouched: for an operator outside any query scope.
     */
    private void addInputResident(Page page) {
        assert lastOutput == null : "has pending output page";
        rowsReceived += page.getPositionCount();
        if (limitPerGroup == 0) {
            page.releaseBlocks();
            return;
        }
        admit(page);
    }

    /**
     * Today's loop: every key is added to the table and counted, and the accepted positions become the output.
     */
    private void admit(Page page) {
        try {
            int positionCount = page.getPositionCount();
            int acceptedCount = 0;
            int[] accepted = new int[positionCount];

            for (int pos = 0; pos < positionCount; pos++) {
                PagedBytesCursor key = keyEncoder.encode(page, pos);
                long hashOrd = seenKeys.add(key);
                int count;
                long ord;
                if (hashOrd >= 0) {
                    ord = hashOrd;
                    counts = bigArrays.grow(counts, ord + 1);
                    count = 0;
                    counts.set(ord, 0);
                } else {
                    ord = -(hashOrd + 1);
                    count = counts.get(ord);
                }
                if (count < limitPerGroup) {
                    counts.set(ord, count + 1);
                    accepted[acceptedCount++] = pos;
                }
            }

            if (acceptedCount == 0) {
                return;
            }

            /*
             * When all rows in a page are accepted the operator returns the
             * original page instance rather than a filtered copy.
             */
            if (acceptedCount == positionCount) {
                lastOutput = page.shallowCopy();
            } else {
                lastOutput = page.filter(false, accepted, 0, acceptedCount);
            }
        } finally {
            page.releaseBlocks();
        }
    }

    // ---- the freeze

    /**
     * An upper bound of what the page may add to the operator's state: a doubling of the table and the counts, the key
     * bytes of the page, and the filtered output page, which is at most a copy of the input.
     */
    private long estimateFor(Page page) {
        long keyBytes = 0;
        for (int channel : keyEncoder.groupChannels()) {
            keyBytes += page.getBlock(channel).ramBytesUsed();
        }
        return seenKeys.ramBytesUsed() + counts.ramBytesUsed() + keyBytes + page.ramBytesUsedByBlocks();
    }

    /**
     * Acquires the run writer, the operator's headroom, once its own state reaches the query's own-state minimum or the
     * query is half way to its budget: before any spill-dependent state, so that a writer that exists never allocates
     * under pressure. A refused acquisition leaves the operator without headroom for good; a later trip then fails the
     * query, which is the documented residual.
     */
    private void maybeAcquireHeadroom() {
        if (headroom != null || spillCapable == false) {
            return;
        }
        if (ramBytesUsed() < scope.minSpillBytes() && scope.breaker().used() * 2 < scope.budgetBytes()) {
            return;
        }
        String name = runName();
        headroom = DriverSpillOperation.issue(
            driverContext,
            "spill run writer",
            l -> FramedRunWriter.create(scope.spillScope(), name, scope.rootView(), scope.codecExecutor(), RUN_OPTIONS, l)
        );
    }

    private String runName() {
        return "grouped-limit-" + depth + "-" + seed;
    }

    /**
     * Takes the headroom's outcome once it is in: the writer, or, when the acquisition was refused, no spilling for
     * this operator from now on; a frozen operator then fails, since its held page has nowhere to go. Called before
     * every freeze decision, so a freeze never starts on a headroom that has already failed.
     */
    private void resolveHeadroom() {
        if (overflow != null || headroom == null || headroom.isDone() == false) {
            return;
        }
        Exception failure = headroom.failure();
        if (failure == null) {
            overflow = new PageRunWriter(headroom.result());
            return;
        }
        if (frozen) {
            throw rethrow(failure);
        }
        logger.debug(() -> "operator [" + runName() + "] acquired no spill headroom; a later trip fails the query", failure);
        spillCapable = false;
    }

    private boolean writerReady() {
        resolveHeadroom();
        return overflow != null;
    }

    private void freeze(String reason) {
        frozen = true;
        frozenGroupCount = seenKeys.size();
        logger.debug("operator [{}] froze its [{}] keys: {}", runName(), frozenGroupCount, reason);
    }

    private void routeOrHold(Page page) {
        boolean ready;
        try {
            ready = writerReady();
        } catch (RuntimeException e) {
            page.releaseBlocks();
            throw e;
        }
        if (ready) {
            route(page);
        } else {
            pending = page;
        }
    }

    /**
     * The frozen loop: a present key keeps its count and streams, an absent key's row goes to its partition's run.
     */
    private void route(Page page) {
        try {
            int positionCount = page.getPositionCount();
            int acceptedCount = 0;
            int[] accepted = new int[positionCount];
            int[][] byPartition = new int[PARTITIONS][];
            int[] partitionCounts = new int[PARTITIONS];
            for (int pos = 0; pos < positionCount; pos++) {
                PagedBytesCursor key = keyEncoder.encode(page, pos);
                long ord = seenKeys.find(key);
                if (ord >= 0) {
                    int count = counts.get(ord);
                    if (count < limitPerGroup) {
                        counts.set(ord, count + 1);
                        accepted[acceptedCount++] = pos;
                    }
                } else {
                    int partition = partitionOf(key);
                    int[] positions = byPartition[partition];
                    if (positions == null) {
                        positions = new int[8];
                        byPartition[partition] = positions;
                    } else if (partitionCounts[partition] == positions.length) {
                        positions = Arrays.copyOf(positions, positions.length * 2);
                        byPartition[partition] = positions;
                    }
                    positions[partitionCounts[partition]++] = pos;
                }
            }
            if (acceptedCount == positionCount) {
                lastOutput = page.shallowCopy();
                return;
            }
            if (acceptedCount > 0) {
                lastOutput = page.filter(false, accepted, 0, acceptedCount);
            }
            for (int partition = 0; partition < PARTITIONS; partition++) {
                if (partitionCounts[partition] == 0) {
                    continue;
                }
                // a filtered page is one the operator produced, so its memory is reclaimed in full once serialized
                Page part = page.filter(false, byPartition[partition], 0, partitionCounts[partition]);
                try {
                    pinDocuments(part);
                    rowsSpilled += partitionCounts[partition];
                    appendStarted();
                    overflow.append(partition, part, appendListener());
                } finally {
                    part.releaseBlocks();
                }
            }
        } finally {
            page.releaseBlocks();
        }
    }

    /**
     * Holds a reference on every shard the page's document references name, so that the shards outlive the page's
     * own references and the rows read back find them.
     */
    private void pinDocuments(Page page) {
        for (int b = 0; b < page.getBlockCount(); b++) {
            if (page.getBlock(b) instanceof DocBlock documents) {
                DocVector vector = documents.asVector();
                if (docRefs == null) {
                    docRefs = vector.refCounteds();
                } else if (docRefs != vector.refCounteds()) {
                    throw new IllegalStateException("documents from more than one source in one operator");
                }
                IntVector shards = vector.shards();
                for (int p = 0; p < shards.getPositionCount(); p++) {
                    int shard = shards.getInt(p);
                    if (pinnedShards.get(shard) == false) {
                        docRefs.get(shard).mustIncRef();
                        pinnedShards.set(shard);
                    }
                }
            }
        }
    }

    private void unpinDocuments() {
        if (docRefs != null) {
            for (int shard = pinnedShards.nextSetBit(0); shard >= 0; shard = pinnedShards.nextSetBit(shard + 1)) {
                docRefs.get(shard).decRef();
            }
            pinnedShards.clear();
        }
    }

    private int partitionOf(PagedBytesCursor key) {
        int mixed = BitMixer.mix32(key.hashCode() + seed * 0x9E3779B9);
        return mixed & (PARTITIONS - 1);
    }

    private void appendStarted() {
        synchronized (appendLock) {
            appendsInFlight++;
        }
    }

    private void appendDone() {
        SubscribableListener<Void> wake;
        synchronized (appendLock) {
            appendsInFlight--;
            wake = appendWait;
            appendWait = null;
        }
        if (wake != null) {
            wake.onResponse(null);
        }
    }

    private IsBlockedResult appendsBlocked() {
        synchronized (appendLock) {
            if (appendsInFlight < MAX_APPENDS_IN_FLIGHT) {
                return NOT_BLOCKED;
            }
            if (appendWait == null) {
                appendWait = new SubscribableListener<>();
            }
            return new IsBlockedResult(appendWait, "spill appends in flight");
        }
    }

    private ActionListener<Long> appendListener() {
        return new ActionListener<>() {
            @Override
            public void onResponse(Long offset) {
                appendDone();
            }

            @Override
            public void onFailure(Exception e) {
                if (spillFailure == null) {
                    spillFailure = e;
                }
                appendDone();
            }
        };
    }

    private void checkSpillFailure() {
        Exception failure = spillFailure;
        if (failure != null) {
            throw rethrow(failure);
        }
    }

    private static RuntimeException rethrow(Exception e) {
        if (e instanceof RuntimeException runtime) {
            return runtime;
        }
        return new ElasticsearchException("spilling failed", e);
    }

    // ---- the end of the input and the replay

    @Override
    public void finish() {
        finished = true;
        maybeStartFinishing();
    }

    private void maybeStartFinishing() {
        if (finished && frozen && pending == null && overflow != null && finishing == null) {
            PageRunWriter writer = overflow;
            waitStartedNanos = System.nanoTime();
            finishing = DriverSpillOperation.issue(driverContext, "spill run finish", writer::finish);
        }
    }

    @Override
    public boolean isFinished() {
        if (scope == null || frozen == false) {
            return lastOutput == null && finished;
        }
        return finished && lastOutput == null && pending == null && deleting != null && deleting.isDone();
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        return lastOutput != null || pending != null || (frozen && finished);
    }

    @Override
    public IsBlockedResult isBlocked() {
        if (scope == null || spillFailure != null || lastOutput != null) {
            return NOT_BLOCKED;
        }
        if (pending != null && overflow == null && headroom != null) {
            return headroom.blocked();
        }
        IsBlockedResult appends = appendsBlocked();
        if (appends.listener().isDone() == false) {
            return appends;
        }
        if (finishing != null && finishing.isDone() == false) {
            return finishing.blocked();
        }
        if (opening != null && opening.isDone() == false) {
            return opening.blocked();
        }
        if (nextPage != null && nextPage.isDone() == false) {
            return nextPage.blocked();
        }
        if (replay != null) {
            IsBlockedResult child = replay.isBlocked();
            if (child.listener().isDone() == false) {
                return child;
            }
        }
        if (deleting != null && deleting.isDone() == false) {
            return deleting.blocked();
        }
        return NOT_BLOCKED;
    }

    @Override
    public Page getOutput() {
        if (scope == null) {
            return takeLastOutput();
        }
        checkSpillFailure();
        if (lastOutput != null) {
            return takeLastOutput();
        }
        if (pending != null) {
            if (writerReady() == false) {
                return null;
            }
            Page page = pending;
            pending = null;
            route(page);
            maybeStartFinishing();
            if (lastOutput != null) {
                return takeLastOutput();
            }
        }
        if (frozen == false || finished == false) {
            return null;
        }
        Page out = advanceReplay();
        if (out != null) {
            pagesProcessed++;
            rowsEmitted += out.getPositionCount();
        }
        return out;
    }

    private Page takeLastOutput() {
        if (lastOutput == null) {
            return null;
        }
        Page result = lastOutput;
        lastOutput = null;
        pagesProcessed++;
        rowsEmitted += result.getPositionCount();
        return result;
    }

    /**
     * The finish phase: the run is published, the frozen state released, and each non-empty partition replayed through
     * a child operator until it is drained; the run is deleted at the end. Returns the next page of output, or null
     * when there is none yet, in which case {@link #isBlocked} says what is awaited.
     */
    @Nullable
    private Page advanceReplay() {
        if (finishing == null) {
            maybeStartFinishing();
            if (finishing == null) {
                return null;
            }
        }
        if (run == null) {
            if (finishing.isDone() == false) {
                return null;
            }
            spillNanos += System.nanoTime() - waitStartedNanos;
            if (finishing.failure() != null) {
                throw rethrow(finishing.failure());
            }
            run = finishing.result();
            spillRuns++;
            spilledBytes = run.footer().logicalBytes();
            releaseFrozenState();
            FramedRun published = run;
            String name = runName();
            waitStartedNanos = System.nanoTime();
            opening = DriverSpillOperation.issue(
                driverContext,
                "spill run open",
                l -> FramedRunReader.open(name, published.object(), scope.rootView(), FramedRunMaxima.DEFAULT, CHUNK_BYTES, l)
            );
        }
        if (runReader == null) {
            if (opening.isDone() == false) {
                return null;
            }
            spillNanos += System.nanoTime() - waitStartedNanos;
            if (opening.failure() != null) {
                throw rethrow(opening.failure());
            }
            runReader = opening.result();
        }
        while (true) {
            if (replay == null && nextPartition() == false) {
                return null;
            }
            if (nextPage != null) {
                if (nextPage.isDone() == false) {
                    return null;
                }
                spillNanos += System.nanoTime() - waitStartedNanos;
                if (nextPage.failure() != null) {
                    throw rethrow(nextPage.failure());
                }
                Page in = nextPage.result();
                nextPage = null;
                if (in == null) {
                    replay.finish();
                    replayFinished = true;
                } else {
                    readBackBytes += in.ramBytesUsedByBlocks();
                    replay.addInput(in);
                }
            }
            Page out = replay.getOutput();
            if (out != null) {
                idleRounds = 0;
                return out;
            }
            if (replayFinished) {
                if (replay.isFinished()) {
                    idleRounds = 0;
                    closeReplay();
                    continue;
                }
            } else if (replay.needsInput()) {
                idleRounds = 0;
                PageRunReader reader = partitionReader;
                waitStartedNanos = System.nanoTime();
                nextPage = DriverSpillOperation.issue(driverContext, "spill run read", reader::next);
                if (nextPage.isDone() == false) {
                    return null;
                }
                continue;
            }
            if (replay.isBlocked().listener().isDone() == false) {
                idleRounds = 0;
                return null;
            }
            // not blocked and nothing to do: the child's state moved between two looks, which a completion from the
            // codec thread does; the driver calls again. A stall that lasts is a bug, and fails rather than spins
            if (++idleRounds > MAX_IDLE_ROUNDS) {
                throw new IllegalStateException("replay of partition [" + replayPartition + "] made no progress");
            }
            return null;
        }
    }

    /**
     * Moves to the next partition with records; false when none is left, in which case the run is closed and deleted.
     */
    private boolean nextPartition() {
        while (++replayPartition < runReader.partitions()) {
            if (runReader.records(replayPartition) > 0) {
                partitionReader = new PageRunReader(runReader, replayPartition, blockFactory, scope.rootView(), docRefs);
                replay = new GroupedLimitOperator(
                    limitPerGroup,
                    keyEncoder.copy(blockFactory),
                    blockFactory,
                    driverContext,
                    depth + 1,
                    seed + 1
                );
                replayFinished = false;
                partitionsReplayed++;
                return true;
            }
        }
        Releasables.close(runReader);
        runReader = null;
        deleteRun(run);
        return false;
    }

    private void closeReplay() {
        childSpill = childSpill.plus(replay.spillStatus());
        Releasables.close(replay, partitionReader);
        replay = null;
        partitionReader = null;
    }

    private void deleteRun(@Nullable FramedRun toDelete) {
        if (toDelete != null && deleting == null) {
            deleting = DriverSpillOperation.issue(driverContext, "spill run delete", toDelete::delete);
            String name = toDelete.name();
            deleting.completion()
                .addListener(
                    ActionListener.wrap(
                        ignored -> {},
                        e -> logger.debug(() -> "spill run [" + name + "] could not be deleted; the scope's close removes it", e)
                    )
                );
        }
    }

    private void releaseFrozenState() {
        Releasables.close(seenKeys, counts);
        seenKeys = null;
        counts = null;
    }

    // ---- accounting and status

    @Override
    public long ramBytesUsed() {
        long size = SHALLOW_SIZE;
        if (seenKeys != null) {
            size += seenKeys.ramBytesUsed();
        }
        if (counts != null) {
            size += counts.ramBytesUsed();
        }
        size += keyEncoder.ramBytesUsed();
        if (replay != null) {
            size += replay.ramBytesUsed();
        }
        return size;
    }

    private long groupCount() {
        return seenKeys != null ? seenKeys.size() : frozenGroupCount;
    }

    /**
     * This operator's spill counters with those of its replay operators, gone and current.
     */
    SpillStatus spillStatus() {
        SpillStatus own = new SpillStatus(spilledBytes, readBackBytes, spillRuns, spillNanos).plus(childSpill);
        return replay == null ? own : own.plus(replay.spillStatus());
    }

    @Override
    public Status status() {
        return new Status(
            limitPerGroup,
            (int) groupCount(),
            pagesProcessed,
            rowsReceived,
            rowsEmitted,
            ramBytesUsed(),
            rowsSpilled,
            partitionsReplayed,
            spillStatus()
        );
    }

    @Override
    public void close() {
        Releasable lastOutputRelease = lastOutput == null ? () -> {} : lastOutput::releaseBlocks;
        Releasable pendingRelease = pending == null ? () -> {} : pending::releaseBlocks;
        FramedRunReader openedReader = runReader;
        Releasables.closeExpectNoException(
            lastOutputRelease,
            pendingRelease,
            replay,
            partitionReader,
            runReader,
            seenKeys,
            counts,
            keyEncoder
        );
        lastOutput = null;
        pending = null;
        replay = null;
        partitionReader = null;
        runReader = null;
        seenKeys = null;
        counts = null;
        if (scope == null) {
            return;
        }
        // an operation that completed since the driver's last look is released here; one still in flight completes
        // on its own and releases what it borrowed, and the driver waits for each
        if (headroom != null && headroom.isDone() && headroom.failure() == null && overflow == null) {
            overflow = new PageRunWriter(headroom.result());
        }
        if (overflow != null && finishing == null) {
            PageRunWriter writer = overflow;
            driverContext.addAsyncAction();
            writer.abort(ActionListener.running(driverContext::removeAsyncAction));
        } else if (headroom != null && headroom.isDone() == false) {
            driverContext.addAsyncAction();
            headroom.completion()
                .addListener(
                    ActionListener.wrap(
                        writer -> writer.abort(ActionListener.running(driverContext::removeAsyncAction)),
                        e -> driverContext.removeAsyncAction()
                    )
                );
        }
        FramedRun toDelete = run;
        if (finishing != null) {
            if (finishing.isDone() == false) {
                driverContext.addAsyncAction();
                finishing.completion()
                    .addListener(
                        ActionListener.wrap(
                            published -> published.delete(ActionListener.running(driverContext::removeAsyncAction)),
                            e -> driverContext.removeAsyncAction()
                        )
                    );
            } else if (toDelete == null && finishing.failure() == null) {
                toDelete = finishing.result();
            }
        }
        deleteRun(toDelete);
        if (opening != null) {
            if (opening.isDone() == false) {
                driverContext.addAsyncAction();
                opening.completion().addListener(ActionListener.wrap(reader -> {
                    reader.close();
                    driverContext.removeAsyncAction();
                }, e -> driverContext.removeAsyncAction()));
            } else if (opening.failure() == null && opening.result() != openedReader) {
                Releasables.closeExpectNoException(opening.result());
            }
        }
        if (nextPage != null) {
            if (nextPage.isDone() == false) {
                // the page lands after this close and takes its shard references through the pins, which go after it
                driverContext.addAsyncAction();
                nextPage.completion().addListener(ActionListener.wrap(page -> {
                    if (page != null) {
                        page.releaseBlocks();
                    }
                    unpinDocuments();
                    driverContext.removeAsyncAction();
                }, e -> {
                    unpinDocuments();
                    driverContext.removeAsyncAction();
                }));
                return;
            }
            if (nextPage.result() != null) {
                nextPage.result().releaseBlocks();
            }
        }
        unpinDocuments();
    }

    @Override
    public String toString() {
        return "GroupedLimitOperator[limitPerGroup = "
            + limitPerGroup
            + ", groupKeys = "
            + Arrays.toString(keyEncoder.groupChannels())
            + ", groups = "
            + groupCount()
            + "]";
    }

    public static class Status implements Operator.Status {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "grouped_limit",
            Status::new
        );
        public static final ProfileSchema<Status> PROFILE_SCHEMA = SpillStatus.addTo(
            ProfileSchema.of(Status.class, (builder, status, params) -> status.writeProfile(builder))
                .required("limit_per_group", ProfileSchema.integer())
                .required("group_count", ProfileSchema.integer())
                .required("pages_processed", ProfileSchema.integer())
                .required("rows_received", ProfileSchema.integer())
                .required("rows_emitted", ProfileSchema.integer())
                .required("ram_bytes_used", ProfileSchema.integer())
                .required("ram_used", ProfileSchema.string())
                .optional("rows_spilled", ProfileSchema.integer())
                .optional("partitions_replayed", ProfileSchema.integer())
        ).build();

        private final int limitPerGroup;
        private final int groupCount;
        private final int pagesProcessed;
        private final long rowsReceived;
        private final long rowsEmitted;
        private final long ramBytesUsed;
        private final long rowsSpilled;
        private final int partitionsReplayed;
        private final SpillStatus spill;

        protected Status(int limitPerGroup, int groupCount, int pagesProcessed, long rowsReceived, long rowsEmitted, long ramBytesUsed) {
            this(limitPerGroup, groupCount, pagesProcessed, rowsReceived, rowsEmitted, ramBytesUsed, 0, 0, SpillStatus.NONE);
        }

        /**
         * @param rowsSpilled rows routed to a partition run after the freeze, at every depth
         * @param partitionsReplayed partitions replayed through a child operator, at every depth
         * @param spill the shared spill counters, this operator's and its replay operators' together
         */
        protected Status(
            int limitPerGroup,
            int groupCount,
            int pagesProcessed,
            long rowsReceived,
            long rowsEmitted,
            long ramBytesUsed,
            long rowsSpilled,
            int partitionsReplayed,
            SpillStatus spill
        ) {
            this.limitPerGroup = limitPerGroup;
            this.groupCount = groupCount;
            this.pagesProcessed = pagesProcessed;
            this.rowsReceived = rowsReceived;
            this.rowsEmitted = rowsEmitted;
            this.ramBytesUsed = ramBytesUsed;
            this.rowsSpilled = rowsSpilled;
            this.partitionsReplayed = partitionsReplayed;
            this.spill = spill;
        }

        protected Status(StreamInput in) throws IOException {
            limitPerGroup = in.readVInt();
            groupCount = in.readVInt();
            pagesProcessed = in.readVInt();
            rowsReceived = in.readVLong();
            rowsEmitted = in.readVLong();
            ramBytesUsed = in.readVLong();
            if (in.getTransportVersion().supports(SpillStatus.ESQL_SPILL_STATUS)) {
                rowsSpilled = in.readVLong();
                partitionsReplayed = in.readVInt();
            } else {
                rowsSpilled = 0;
                partitionsReplayed = 0;
            }
            spill = SpillStatus.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(limitPerGroup);
            out.writeVInt(groupCount);
            out.writeVInt(pagesProcessed);
            out.writeVLong(rowsReceived);
            out.writeVLong(rowsEmitted);
            out.writeVLong(ramBytesUsed);
            if (out.getTransportVersion().supports(SpillStatus.ESQL_SPILL_STATUS)) {
                out.writeVLong(rowsSpilled);
                out.writeVInt(partitionsReplayed);
            }
            spill.writeTo(out);
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        public int limitPerGroup() {
            return limitPerGroup;
        }

        public int groupCount() {
            return groupCount;
        }

        public int pagesProcessed() {
            return pagesProcessed;
        }

        public long rowsReceived() {
            return rowsReceived;
        }

        public long rowsEmitted() {
            return rowsEmitted;
        }

        public long ramBytesUsed() {
            return ramBytesUsed;
        }

        public long rowsSpilled() {
            return rowsSpilled;
        }

        public int partitionsReplayed() {
            return partitionsReplayed;
        }

        public SpillStatus spill() {
            return spill;
        }

        private XContentBuilder writeProfile(XContentBuilder builder) throws IOException {
            builder.startObject();
            builder.field("limit_per_group", limitPerGroup);
            builder.field("group_count", groupCount);
            builder.field("pages_processed", pagesProcessed);
            builder.field("rows_received", rowsReceived);
            builder.field("rows_emitted", rowsEmitted);
            builder.field("ram_bytes_used", ramBytesUsed);
            builder.field("ram_used", ByteSizeValue.ofBytes(ramBytesUsed));
            // present by value: an operator that never froze writes none of these
            if (rowsSpilled > 0 || partitionsReplayed > 0 || spill.isNone() == false) {
                builder.field("rows_spilled", rowsSpilled);
                builder.field("partitions_replayed", partitionsReplayed);
                spill.toXContent(builder, ToXContent.EMPTY_PARAMS);
            }
            return builder.endObject();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return PROFILE_SCHEMA.write(builder, this, params);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Status status = (Status) o;
            return limitPerGroup == status.limitPerGroup
                && groupCount == status.groupCount
                && pagesProcessed == status.pagesProcessed
                && rowsReceived == status.rowsReceived
                && rowsEmitted == status.rowsEmitted
                && ramBytesUsed == status.ramBytesUsed
                && rowsSpilled == status.rowsSpilled
                && partitionsReplayed == status.partitionsReplayed
                && spill.equals(status.spill);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                limitPerGroup,
                groupCount,
                pagesProcessed,
                rowsReceived,
                rowsEmitted,
                ramBytesUsed,
                rowsSpilled,
                partitionsReplayed,
                spill
            );
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.minimumCompatible();
        }
    }
}
