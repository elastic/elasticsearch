/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import com.carrotsearch.hppc.BitMixer;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.PagedBytesCursor;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
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
import org.elasticsearch.compute.spill.QueryScope;
import org.elasticsearch.compute.spill.SpillStatus;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.BitSet;

import static org.elasticsearch.compute.operator.Operator.NOT_BLOCKED;

/**
 * The spill side of an operator that freezes its key set: the headroom, a run writer with its buffers acquired once
 * the operator's state reaches the query's own-state minimum; the overflow, one run partition per hash of the key for
 * the rows of keys first seen after the freeze; and, once the input ends and the frozen state is released, the replay
 * of every partition through a fresh operator of the host's kind, which may freeze again one level deeper, up to
 * {@link #MAX_DEPTH}. The host keeps its frozen state and decides, row by row, which rows it keeps and which it
 * routes; this class keeps every asynchronous operation as a {@link DriverSpillOperation} the driver waits for, the
 * document pins of the spilled rows, the counters, and the close that aborts, closes or deletes what is in flight.
 * Every method is the driver thread's, except where noted.
 */
final class SpillReplay implements Releasable {

    private static final Logger logger = LogManager.getLogger(SpillReplay.class);

    /**
     * What the host provides: its size, a replay operator of its kind, the release of its frozen state and the spill
     * counters of a replay it made.
     */
    interface Host {
        /** The host's own state size: what decides when headroom is acquired and whether a crossing is this host's to spill. */
        long ownBytes();

        /** A fresh operator of the host's kind for one partition's rows, one level deeper, routing with another seed. */
        Operator newReplay(int depth, int seed);

        /** Releases the frozen state once the run is published, so that the replay runs with the whole budget. */
        void releaseFrozenState();

        /** The spill counters of a replay operator this host created. */
        SpillStatus spillStatusOf(Operator replay);
    }

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

    private final DriverContext driverContext;
    private final QueryScope scope;
    private final Host host;
    private final String namePrefix;
    private final int depth;
    private final int seed;
    /** Whether the host may freeze: depth below the maximum and headroom not refused. */
    private boolean spillCapable;
    @Nullable
    private DriverSpillOperation<FramedRunWriter> headroom;
    @Nullable
    private PageRunWriter overflow;
    private boolean frozen;
    private final Object appendLock = new Object();
    /** Guarded by {@link #appendLock}: the codec thread completes appends. */
    private int appendsInFlight;
    @Nullable
    private SubscribableListener<Void> appendWait;
    /** Set by the codec thread on a failed append, read by the driver. */
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
    private Operator replay;
    private boolean replayFinished;
    /** Consecutive rounds in which the replay neither produced, consumed, finished nor waited; a stall past the bound fails. */
    private int idleRounds;
    @Nullable
    private DriverSpillOperation<Page> nextPage;
    @Nullable
    private DriverSpillOperation<Void> deleting;
    /** The shard references of the documents in the spilled rows, pinned until this closes. */
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
     * Whether a host at {@code depth} in a query scoped by {@code scope} may spill at all; a host outside a scope that
     * spills runs today's path and creates no replay.
     */
    static boolean applies(@Nullable QueryScope scope) {
        return scope != null && scope.spillEnabled();
    }

    /**
     * @param namePrefix the run name's prefix, which the depth and the seed follow
     * @param depth how many freezes the host's input went through already; zero for the planned operator
     * @param seed changes the partition routing at each depth, so a partition that freezes again splits differently
     */
    SpillReplay(DriverContext driverContext, QueryScope scope, Host host, String namePrefix, int depth, int seed) {
        this.driverContext = driverContext;
        this.scope = scope;
        this.host = host;
        this.namePrefix = namePrefix;
        this.depth = depth;
        this.seed = seed;
        this.spillCapable = depth < MAX_DEPTH;
    }

    boolean frozen() {
        return frozen;
    }

    String runName() {
        return namePrefix + "-" + depth + "-" + seed;
    }

    // ---- the headroom and the freeze

    /**
     * Issues the run writer's creation once the host holds the own-state minimum or the query is half way to its budget.
     */
    void maybeAcquireHeadroom() {
        if (headroom != null || spillCapable == false) {
            return;
        }
        if (host.ownBytes() < scope.minSpillBytes() && scope.breaker().used() * 2 < scope.budgetBytes()) {
            return;
        }
        String name = runName();
        headroom = DriverSpillOperation.issue(
            driverContext,
            "spill run writer",
            l -> FramedRunWriter.create(scope.spillScope(), name, scope.rootView(), scope.codecExecutor(), RUN_OPTIONS, l)
        );
    }

    /**
     * Takes the writer when its creation has landed. A failed creation leaves the host resident, so that a later trip
     * fails the query as today, unless the host has frozen on it already, in which case the failure propagates since
     * the held page has nowhere to go. Called before every freeze decision, so a freeze never starts on a headroom that
     * has already failed.
     */
    void resolveHeadroom() {
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
        logger.debug(() -> Strings.format("operator [%s] acquired no spill headroom; a later trip fails the query", runName()), failure);
        spillCapable = false;
    }

    boolean writerReady() {
        resolveHeadroom();
        return overflow != null;
    }

    /**
     * Whether the host may freeze now: headroom was asked for and not refused.
     */
    boolean mayFreeze() {
        return spillCapable && headroom != null;
    }

    /**
     * The policy at the host's safe point: the query would cross its budget with {@code estimate} more and the host
     * holds enough to be the one to spill, with the headroom to do so.
     */
    boolean shouldFreeze(long estimate) {
        return mayFreeze() && scope.shouldSpill(host.ownBytes(), estimate);
    }

    void freeze(String reason, long frozenGroups) {
        frozen = true;
        logger.debug("operator [{}] froze its [{}] keys: {}", runName(), frozenGroups, reason);
    }

    // ---- the overflow

    int partitionOf(PagedBytesCursor key) {
        int mixed = BitMixer.mix32(key.hashCode() + seed * 0x9E3779B9);
        return mixed & (PARTITIONS - 1);
    }

    /**
     * Queues one partition's part of a page; the caller releases {@code part} afterwards, since the writer serializes
     * from its own copy. A filtered page is one the host produced, so its memory is reclaimed in full once serialized.
     */
    void append(int partition, Page part) {
        pinDocuments(part);
        rowsSpilled += part.getPositionCount();
        appendStarted();
        overflow.append(partition, part, appendListener());
    }

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

    /**
     * Blocked while {@link #MAX_APPENDS_IN_FLIGHT} appends are queued; what the host's {@code needsInput} consults.
     */
    IsBlockedResult appendsBlocked() {
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

    void checkSpillFailure() {
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

    /**
     * Starts finishing the run once the input has ended, the host holds no page and the writer exists; called whenever
     * one of those changes.
     */
    void maybeStartFinishing(boolean inputFinished, boolean pageHeld) {
        if (inputFinished && frozen && pageHeld == false && overflow != null && finishing == null) {
            PageRunWriter writer = overflow;
            waitStartedNanos = System.nanoTime();
            finishing = DriverSpillOperation.issue(driverContext, "spill run finish", writer::finish);
        }
    }

    /**
     * Whether the replay has run to its end and the run is gone.
     */
    boolean replayDone() {
        return deleting != null && deleting.isDone();
    }

    /**
     * What the host is blocked on: the headroom while it holds a page for it, then the appends, then each operation of
     * the finish and the replay in turn. Not blocked once spilling failed, so that the failure surfaces.
     */
    IsBlockedResult blocked(boolean pageHeld) {
        if (spillFailure != null) {
            return NOT_BLOCKED;
        }
        if (pageHeld && overflow == null && headroom != null) {
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

    /**
     * The finish phase: the run is published, the frozen state released, and each non-empty partition replayed through
     * a fresh operator whose output this returns page by page. Null when waiting on an operation, or when every partition
     * has been replayed and the run deleted. Called once the host's input ended and it holds no page.
     */
    @Nullable
    Page advanceReplay() {
        if (finishing == null) {
            maybeStartFinishing(true, false);
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
            host.releaseFrozenState();
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
                // decoded on the I/O thread: the query's factory charges the root view, which any thread may use,
                // where the driver's factory sits on a local breaker that only the driver thread may touch
                partitionReader = new PageRunReader(runReader, replayPartition, scope.blockFactory(), scope.rootView(), docRefs);
                replay = host.newReplay(depth + 1, seed + 1);
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
        childSpill = childSpill.plus(host.spillStatusOf(replay));
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
                        e -> logger.debug(
                            () -> Strings.format("spill run [%s] could not be deleted; the scope's close removes it", name),
                            e
                        )
                    )
                );
        }
    }

    // ---- accounting and status

    long rowsSpilled() {
        return rowsSpilled;
    }

    int partitionsReplayed() {
        return partitionsReplayed;
    }

    /**
     * The replay operator's size, when it reports one; the host adds it to its own.
     */
    long replayBytes() {
        return replay instanceof Accountable accountable ? accountable.ramBytesUsed() : 0;
    }

    SpillStatus spillStatus() {
        SpillStatus own = new SpillStatus(spilledBytes, readBackBytes, spillRuns, spillNanos).plus(childSpill);
        return replay == null ? own : own.plus(host.spillStatusOf(replay));
    }

    /**
     * Closes the replay and what the host handed over, then settles every operation: one that completed since the
     * driver's last look is released here, one still in flight completes on its own and releases what it borrowed, and
     * the driver waits for each. The run, when published, is deleted.
     */
    @Override
    public void close() {
        FramedRunReader openedReader = runReader;
        Releasables.closeExpectNoException(replay, partitionReader, runReader);
        replay = null;
        partitionReader = null;
        runReader = null;
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
}
