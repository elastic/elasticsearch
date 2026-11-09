/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.spill.DriverSpillOperation;
import org.elasticsearch.compute.spill.FramedRun;
import org.elasticsearch.compute.spill.FramedRunLayout;
import org.elasticsearch.compute.spill.FramedRunMaxima;
import org.elasticsearch.compute.spill.FramedRunReader;
import org.elasticsearch.compute.spill.FramedRunWriter;
import org.elasticsearch.compute.spill.QueryScope;
import org.elasticsearch.compute.spill.SpillStatus;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.TaskCancelledException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.compute.operator.Operator.NOT_BLOCKED;

/**
 * The values of a top-N's rows once they no longer fit: immutable sealed segments, each one framed run of
 * {@link FramedRunLayout#KIND_VALUE}, plus one active writer per {@link Lane}. A row holds a pointer, segment, offset
 * and length, into the segment its values went to; the segment and the length are set as the append is queued, the
 * offset once the codec reports where the value landed, which every reader of pointers waits for by taking the lane's
 * appends to be quiescent first. The segment a lane writes rolls at {@link #SEGMENT_BYTES}; at the operator's finish
 * every lane is sealed, and the gather of an output page reads its rows' values in physical order, segment by segment
 * and offset by offset, and hands them back in the page's order.
 * <p>
 * A lane is single-threaded, its appender's: the operator's own, or one worker's of the parallel operator, which all
 * share one store so that a row keeps its pointer through the worker merge. A value handed to the codec moves its
 * charge from the appender's breaker to the query's root view as it is queued, so that the codec may let go of it
 * from its own thread, after the appender's turn or after its operator closed. Compaction rewrites a lane's live
 * values into a fresh segment when the dead bytes of evicted rows outgrow them, as a transaction: the live values are
 * copied in physical order with a bounded number of append bytes in flight, the new pointers are published only once
 * the new segment is sealed, and the old segments go afterwards; a failure aborts the new segment and leaves every
 * pointer as it was.
 */
final class TopNValueStore implements Releasable {

    private static final Logger logger = LogManager.getLogger(TopNValueStore.class);

    /** Payload bytes per chunk of a segment, and half a reader's window. */
    static final int CHUNK_BYTES = 256 * 1024;
    /** A lane's active segment is sealed and a new one started past this many value bytes. */
    static final long SEGMENT_BYTES = 64L * 1024 * 1024;
    /** Records a lane's active segment takes before it rolls: each costs an index entry charged while the segment is open. */
    static final long SEGMENT_RECORDS = 1L << 20;
    /**
     * Value bytes the store's lanes together may have queued for the codec before an appender is told to wait. Shared
     * by the lanes rather than given to each: a node runs a Top-N per driver and the parallel operator a lane per
     * worker, and the queued bytes of every lane sit on the root view until the codec takes them.
     */
    static final long MAX_APPEND_BYTES_IN_FLIGHT = 4L * 1024 * 1024;
    private static final FramedRunWriter.Options RUN_OPTIONS = new FramedRunWriter.Options(
        FramedRunLayout.KIND_VALUE,
        CHUNK_BYTES,
        2,
        FramedRunMaxima.DEFAULT
    );

    private final DriverContext driverContext;
    private final QueryScope scope;
    private final String namePrefix;
    private final long segmentBytes;
    /** Guards the store's queued bytes and the wait on them, which the codec's completions update. */
    private final Object queueLock = new Object();
    private long queuedBytes;
    @Nullable
    private SubscribableListener<Void> appendWait;
    /** Every segment ever started, by id; guarded by {@code this}, since lanes on worker threads add to it. */
    private final List<Segment> segments = new ArrayList<>();
    private int lanes;
    private final AtomicLong bytesWritten = new AtomicLong();
    private final AtomicLong bytesRead = new AtomicLong();
    private final AtomicLong spillNanos = new AtomicLong();
    private final AtomicInteger runs = new AtomicInteger();
    private final AtomicInteger compactions = new AtomicInteger();
    private final AtomicInteger compactionsFailed = new AtomicInteger();
    private final AtomicBoolean closed = new AtomicBoolean();

    TopNValueStore(DriverContext driverContext, QueryScope scope, String namePrefix) {
        this(driverContext, scope, namePrefix, SEGMENT_BYTES);
    }

    /**
     * @param segmentBytes where a lane's active segment rolls; tests roll small
     */
    TopNValueStore(DriverContext driverContext, QueryScope scope, String namePrefix, long segmentBytes) {
        this.driverContext = driverContext;
        this.scope = scope;
        this.namePrefix = namePrefix;
        this.segmentBytes = segmentBytes;
    }

    /**
     * One sealed or active segment. The reader is opened by the first gather or compaction that needs it and closed when
     * the segment is retired; its cursor keeps the window of the last read, which ascending offsets share.
     */
    final class Segment {
        final int id;
        final String name;
        @Nullable
        private volatile FramedRun run;
        @Nullable
        private FramedRunReader reader;
        @Nullable
        private FramedRunReader.RecordCursor cursor;
        private volatile boolean retired;

        private Segment(int id) {
            this.id = id;
            this.name = namePrefix + "-" + id;
        }

        /**
         * Publishes the sealed run, or deletes it right away when the segment was retired or the store closed meanwhile.
         */
        private void sealed(FramedRun sealed) {
            boolean gone;
            synchronized (this) {
                run = sealed;
                gone = retired;
            }
            runs.incrementAndGet();
            if (gone) {
                deleteRun(this, sealed);
            } else if (closed.get()) {
                retire(this);
            }
        }

        /**
         * The cursor over the sealed segment, opening the reader first; the listener may complete inline. A retired
         * segment, one no row references, answers with a failure.
         */
        private void cursor(ActionListener<FramedRunReader.RecordCursor> listener) {
            FramedRun sealed = run;
            if (sealed == null) {
                listener.onFailure(new IllegalStateException("value segment [" + name + "] is not sealed"));
                return;
            }
            synchronized (this) {
                if (retired) {
                    listener.onFailure(new IllegalStateException("value segment [" + name + "] was retired"));
                    return;
                }
                if (cursor != null) {
                    listener.onResponse(cursor);
                    return;
                }
            }
            FramedRunReader.open(
                name,
                sealed.object(),
                scope.rootView(),
                FramedRunMaxima.DEFAULT,
                2 * CHUNK_BYTES,
                listener.delegateFailureAndWrap((l, opened) -> {
                    FramedRunReader.RecordCursor result;
                    boolean gone;
                    boolean duplicate;
                    synchronized (this) {
                        gone = retired;
                        duplicate = gone == false && cursor != null;
                        if (gone == false && duplicate == false) {
                            reader = opened;
                            cursor = opened.cursor(0);
                        }
                        result = cursor;
                    }
                    if (gone) {
                        opened.close();
                        l.onFailure(new IllegalStateException("value segment [" + name + "] was retired"));
                        return;
                    }
                    if (duplicate) {
                        opened.close();
                    }
                    l.onResponse(result);
                })
            );
        }

        private synchronized void closeReader() {
            Releasables.close(cursor, reader);
            cursor = null;
            reader = null;
        }
    }

    /**
     * One appender's active writer and its bookkeeping. Everything but what the codec's completions update is the
     * appender thread's.
     */
    final class Lane implements Releasable {
        final int id;
        @Nullable
        private DriverSpillOperation<FramedRunWriter> creating;
        @Nullable
        private FramedRunWriter writer;
        @Nullable
        private Segment segment;
        /** Value bytes queued into the active segment, what decides when it rolls. */
        private long segmentBytes;
        @Nullable
        private DriverSpillOperation<FramedRun> sealing;
        /** The writers whose finish is in flight, so that a close may abort them; the codec's completions remove them. */
        private final Set<FramedRunWriter> sealingWriters = Collections.synchronizedSet(new HashSet<>());
        @Nullable
        private DriverSpillOperation<long[]> compacting;
        /** The compaction in flight, so that a close may cancel it. */
        @Nullable
        private Compaction compaction;
        /** What a compaction in flight rewrites, in the order its new offsets come back. */
        @Nullable
        private List<TopNRow> compactingRows;
        @Nullable
        private Segment compactingInto;
        /** Guards the lane's in-flight count and the rows' append generations, which the codec's completions read. */
        private final Object lock = new Object();
        private long bytesInFlight;
        @Nullable
        private volatile Exception failure;
        private boolean sealedForGood;
        /** Records queued into the active segment; a segment rolls on that as well, since each costs an index entry. */
        private long segmentRecords;
        /** The segments this lane wrote, sealed or active, which a compaction retires but for its target. */
        private final List<Segment> own = new ArrayList<>();
        /** The pointers of the rows a compaction in flight rewrites, as they were when it started. */
        @Nullable
        private long[] compactingPointers;

        private Lane(int id) {
            this.id = id;
        }

        /**
         * Issues the writer's creation, the headroom: the chunk buffers are charged to the query's root view now, so that
         * a lane that exists never allocates under pressure. A no-op once issued or once sealed for good.
         */
        void acquire() {
            if (writer != null || creating != null || sealedForGood) {
                return;
            }
            Segment next = newSegment();
            segment = next;
            own.add(next);
            long started = System.nanoTime();
            creating = DriverSpillOperation.issue(
                driverContext,
                "top-n value segment",
                l -> FramedRunWriter.create(
                    scope.spillScope(),
                    next.name,
                    scope.rootView(),
                    scope.codecExecutor(),
                    RUN_OPTIONS,
                    ActionListener.runBefore(l, () -> spillNanos.addAndGet(System.nanoTime() - started))
                )
            );
        }

        /**
         * Whether the lane may take values: the writer landed. A failed creation poisons the lane.
         */
        boolean ready() {
            if (writer != null) {
                return true;
            }
            if (creating == null || creating.isDone() == false) {
                return false;
            }
            Exception e = creating.failure();
            FramedRunWriter landed = creating.result();
            creating = null;
            if (e != null) {
                fail(e);
                return false;
            }
            writer = landed;
            segmentBytes = 0;
            segmentRecords = 0;
            if (sealedForGood) {
                seal();
                return false;
            }
            return true;
        }

        /**
         * Whether the headroom was asked for and not refused.
         */
        boolean capable() {
            return failure == null && (writer != null || creating != null);
        }

        /**
         * Whether the writer's creation is in flight, with no side effect.
         */
        boolean pending() {
            return creating != null && creating.isDone() == false;
        }

        /**
         * Queues the row's values, held in {@code source}, whose charge moves to the query's root view here and goes
         * back from the codec's thread once the bytes are in the stream; the builder's accounting ends now, its array
         * living on until then. The row's segment and length are set now, its offset once the codec reports it, before
         * the append counts as done, and the row holds no builder from here on; a row evicted and appended again
         * meanwhile keeps the later append's pointer, the earlier completion landing on nothing. The driver holds an
         * action while appends are in flight, so that it never completes with a record still on the codec's thread.
         * Fails, with the row untouched, when the root view refuses the charge or the driver has finished.
         */
        void append(TopNRow row, BreakingBytesRefBuilder source) {
            assert writer != null : "lane is not ready";
            assert row.values == source : "the source is the row's builder";
            BytesRef bytes = source.bytesRefView();
            int length = bytes.length;
            long charge = source.ramBytesUsed();
            int generation;
            synchronized (lock) {
                if (bytesInFlight == 0) {
                    // refused once the driver finished: nothing below has happened yet
                    driverContext.addAsyncAction();
                }
                try {
                    scope.rootView().addEstimateBytesAndMaybeBreak(charge, "top-n value in flight");
                } catch (CircuitBreakingException e) {
                    if (bytesInFlight == 0) {
                        driverContext.removeAsyncAction();
                    }
                    throw e;
                }
                bytesInFlight += length;
                generation = ++row.appendGeneration;
            }
            synchronized (queueLock) {
                queuedBytes += length;
            }
            source.close();
            row.values = null;
            row.valueSegment = segment.id;
            row.valueOffset = -1;
            row.valueLength = length;
            segmentBytes += length;
            segmentRecords++;
            writer.append(
                0,
                out -> out.writeBytes(bytes.bytes, bytes.offset, bytes.length),
                () -> scope.rootView().addWithoutBreaking(-charge),
                new ActionListener<>() {
                    @Override
                    public void onResponse(Long written) {
                        synchronized (lock) {
                            // the row was evicted and appended again, to the next segment, while this append was on the
                            // codec's thread: its pointer is the later append's, and the bytes written here go unreferenced
                            if (row.appendGeneration == generation) {
                                row.valueOffset = written;
                            }
                        }
                        bytesWritten.addAndGet(length);
                        appendDone(length);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        fail(e);
                        appendDone(length);
                    }
                }
            );
        }

        /**
         * Whether no append is in flight, so that every row's pointer is complete; a compaction or a gather needs that.
         */
        boolean quiescent() {
            synchronized (lock) {
                return bytesInFlight == 0;
            }
        }

        private void fail(Exception e) {
            if (failure == null) {
                failure = e;
            }
        }

        private void appendDone(long length) {
            SubscribableListener<Void> wake;
            boolean last;
            synchronized (lock) {
                bytesInFlight -= length;
                last = bytesInFlight == 0;
            }
            synchronized (queueLock) {
                queuedBytes -= length;
                wake = appendWait;
                appendWait = null;
            }
            if (wake != null) {
                wake.onResponse(null);
            }
            if (last) {
                driverContext.removeAsyncAction();
            }
        }

        /**
         * Blocked while {@link #MAX_APPEND_BYTES_IN_FLIGHT} value bytes are queued by the store's lanes together; what
         * the appender's {@code needsInput} consults.
         */
        IsBlockedResult appendsBlocked() {
            synchronized (queueLock) {
                if (queuedBytes < MAX_APPEND_BYTES_IN_FLIGHT) {
                    return NOT_BLOCKED;
                }
                if (appendWait == null) {
                    appendWait = new SubscribableListener<>();
                }
                return new IsBlockedResult(appendWait, "top-n value appends in flight");
            }
        }

        /**
         * Whether the active segment is full, in bytes or in records, each of which costs an index entry while the
         * segment is open; the appender then {@link #roll()}s between pages.
         */
        boolean needsRoll() {
            return writer != null && (this.segmentBytes >= TopNValueStore.this.segmentBytes || segmentRecords >= SEGMENT_RECORDS);
        }

        /**
         * Seals the active segment and starts the next: the lane is not ready until the new writer lands.
         */
        void roll() {
            seal();
            acquire();
        }

        /**
         * Finishes the active writer, which publishes the segment; a no-op without one. A seal still in flight is waited
         * for first, since the operations of one lane land in order; its failure aborts this writer.
         */
        void seal() {
            if (writer == null) {
                return;
            }
            FramedRunWriter finishing = writer;
            Segment finished = segment;
            DriverSpillOperation<FramedRun> previous = sealing;
            writer = null;
            segment = null;
            segmentBytes = 0;
            segmentRecords = 0;
            long sealStarted = System.nanoTime();
            sealingWriters.add(finishing);
            sealing = DriverSpillOperation.issue(driverContext, "top-n value segment seal", l -> {
                ActionListener<FramedRun> publish = ActionListener.runBefore(l, () -> {
                    sealingWriters.remove(finishing);
                    spillNanos.addAndGet(System.nanoTime() - sealStarted);
                }).map(run -> {
                    finished.sealed(run);
                    return run;
                });
                if (previous == null || previous.isDone()) {
                    finishing.finish(publish);
                } else {
                    previous.completion().addListener(new ActionListener<>() {
                        @Override
                        public void onResponse(FramedRun ignored) {
                            finishing.finish(publish);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            finishing.abort(ActionListener.running(() -> publish.onFailure(e)));
                        }
                    });
                }
            });
        }

        /**
         * Seals for good: the lane takes no more values, as at the operator's finish. A writer still being created is
         * sealed as soon as it lands.
         */
        void sealForGood() {
            sealedForGood = true;
            seal();
        }

        /**
         * Whether every segment the lane wrote is published and nothing of its is in flight. Lands what has completed.
         */
        boolean settled() {
            if (creating != null) {
                if (creating.isDone() == false) {
                    return false;
                }
                ready();
            }
            if (sealing != null) {
                if (sealing.isDone() == false) {
                    return false;
                }
                Exception e = sealing.failure();
                sealing = null;
                if (e != null) {
                    fail(e);
                }
            }
            if (compacting != null) {
                return false;
            }
            synchronized (lock) {
                return bytesInFlight == 0;
            }
        }

        /**
         * What the lane waits for: the writer's creation, a seal, a compaction, then the appends in flight.
         */
        IsBlockedResult blocked() {
            if (creating != null && creating.isDone() == false) {
                return creating.blocked();
            }
            if (sealing != null && sealing.isDone() == false) {
                return sealing.blocked();
            }
            if (compacting != null && compacting.isDone() == false) {
                return compacting.blocked();
            }
            return appendsBlocked();
        }

        @Nullable
        Exception failure() {
            return failure;
        }

        /**
         * Whether a compaction is in flight; the appender takes no input meanwhile.
         */
        boolean compacting() {
            return compacting != null;
        }

        /**
         * Starts the rewrite of {@code liveRows}' values into a fresh segment: the active segment is sealed first, so that
         * every value is readable, then the values are copied in physical order into a new writer, at most
         * {@link #MAX_APPEND_BYTES_IN_FLIGHT} queued at a time. The rows keep their pointers until
         * {@link #finishCompaction()} publishes the new ones on this thread, to the rows that still hold the pointers
         * the compaction read, since a row evicted meanwhile may have been reused.
         */
        void compact(List<TopNRow> liveRows) {
            assert compacting == null : "compaction in flight";
            assert creating == null : "writer being created";
            seal();
            Segment target = newSegment();
            own.add(target);
            List<TopNRow> rows = new ArrayList<>(liveRows);
            rows.sort(Comparator.comparingInt((TopNRow r) -> r.valueSegment).thenComparingLong(r -> r.valueOffset));
            long[] pointers = new long[2 * rows.size()];
            for (int i = 0; i < rows.size(); i++) {
                pointers[2 * i] = rows.get(i).valueSegment;
                pointers[2 * i + 1] = rows.get(i).valueOffset;
            }
            DriverSpillOperation<FramedRun> awaited = sealing;
            compactingRows = rows;
            compactingPointers = pointers;
            compactingInto = target;
            long start = System.nanoTime();
            compacting = DriverSpillOperation.issue(driverContext, "top-n value compaction", l -> {
                ActionListener<long[]> timed = ActionListener.runBefore(l, () -> spillNanos.addAndGet(System.nanoTime() - start));
                // created now, started once the seal has landed, so that a close meanwhile has something to cancel
                Compaction task = new Compaction(rows, target, timed);
                compaction = task;
                if (awaited == null || awaited.isDone()) {
                    task.start();
                } else {
                    awaited.completion().addListener(timed.delegateFailureAndWrap((ignored1, ignored2) -> task.start()));
                }
            });
        }

        /**
         * Lands a finished compaction on this thread: the rows that still hold the pointers it read take their new ones
         * and every segment of the lane but the new one goes, or on a failure the new segment goes and the rows keep
         * theirs. Null when none landed, else whether it succeeded.
         */
        @Nullable
        Boolean finishCompaction() {
            if (compacting == null || compacting.isDone() == false) {
                return null;
            }
            DriverSpillOperation<long[]> landed = compacting;
            List<TopNRow> rows = compactingRows;
            long[] pointers = compactingPointers;
            Segment target = compactingInto;
            compacting = null;
            compaction = null;
            compactingRows = null;
            compactingPointers = null;
            compactingInto = null;
            Exception e = landed.failure();
            if (e != null) {
                compactionsFailed.incrementAndGet();
                logger.debug(() -> Strings.format("top-n value compaction into [%s] failed; the rows keep their values", target.name), e);
                own.remove(target);
                retire(target);
                return false;
            }
            long[] offsets = landed.result();
            for (int i = 0; i < rows.size(); i++) {
                TopNRow row = rows.get(i);
                if (row.valueSegment == pointers[2 * i] && row.valueOffset == pointers[2 * i + 1]) {
                    row.valueSegment = target.id;
                    row.valueOffset = offsets[i];
                }
            }
            for (Segment segment : own) {
                if (segment != target) {
                    retire(segment);
                }
            }
            own.clear();
            own.add(target);
            compactions.incrementAndGet();
            return true;
        }

        /**
         * Aborts what is active, the writer and those whose seal is in flight alike, so that the codec drops their queued
         * values rather than writing them out; the driver waits for the appends in flight before it completes, and the
         * codec lets go of their values. A compaction in flight is cancelled: its target goes, and the rows keep their
         * pointers, as after any failed compaction.
         */
        @Override
        public void close() {
            sealedForGood = true;
            if (creating != null) {
                creating.completion().addListener(ActionListener.wrap(w -> w.abort(ActionListener.noop()), e -> {}));
                creating = null;
            }
            if (writer != null) {
                writer.abort(ActionListener.noop());
                writer = null;
            }
            List<FramedRunWriter> finishing;
            synchronized (sealingWriters) {
                finishing = new ArrayList<>(sealingWriters);
            }
            for (FramedRunWriter sealingWriter : finishing) {
                // idempotent, and a no-op on the backend once the finish has published: a seal that lands anyway is retired
                sealingWriter.abort(ActionListener.noop());
            }
            if (compaction != null) {
                compaction.cancel();
            }
        }
    }

    /**
     * Reads each live value in physical order and appends it to the new segment, a bounded number of append bytes queued
     * at a time; publishes the new segment at the end. The loop runs in place while completions arrive inline and
     * resumes on the completing thread otherwise, one read in flight at a time.
     */
    private final class Compaction {
        private final List<TopNRow> rows;
        private final Segment target;
        private final ActionListener<long[]> listener;
        private final long[] offsets;
        private final AtomicInteger turns = new AtomicInteger();
        /** Set by whichever outcome comes first, the publish or a failure, so that the listener hears exactly once. */
        private final AtomicBoolean done = new AtomicBoolean();
        private final Object lock = new Object();
        private volatile FramedRunWriter into;
        private volatile boolean cancelled;
        private int next;
        private long queued;
        private boolean parked;
        private boolean reading;
        private boolean finishing;

        Compaction(List<TopNRow> rows, Segment target, ActionListener<long[]> listener) {
            this.rows = rows;
            this.target = target;
            this.listener = listener;
            this.offsets = new long[rows.size()];
        }

        void start() {
            FramedRunWriter.create(
                scope.spillScope(),
                target.name,
                scope.rootView(),
                scope.codecExecutor(),
                RUN_OPTIONS,
                ActionListener.wrap(writer -> {
                    into = writer;
                    if (cancelled) {
                        // cancelled while the writer was being created: it goes before it takes a value
                        fail(new TaskCancelledException("top-n value compaction into [" + target.name + "] cancelled"));
                        return;
                    }
                    advance();
                }, this::fail)
            );
        }

        /**
         * Fails the compaction and aborts its writer, from the lane's close; a writer still being created is aborted as
         * soon as it lands. Safe to call once the compaction has finished, when it does nothing.
         */
        void cancel() {
            cancelled = true;
            if (into != null) {
                fail(new TaskCancelledException("top-n value compaction into [" + target.name + "] cancelled"));
            }
        }

        private void advance() {
            if (turns.getAndIncrement() != 0) {
                return;
            }
            do {
                step();
            } while (turns.decrementAndGet() != 0);
        }

        private void step() {
            if (done.get()) {
                return;
            }
            synchronized (lock) {
                if (reading || finishing) {
                    return;
                }
                if (next == rows.size()) {
                    if (queued != 0) {
                        return;
                    }
                    finishing = true;
                } else if (queued >= MAX_APPEND_BYTES_IN_FLIGHT) {
                    parked = true;
                    return;
                } else {
                    reading = true;
                }
            }
            if (finishing) {
                into.finish(ActionListener.wrap(run -> {
                    // the run is recorded either way, so that the segment's retirement deletes it; the listener hears
                    // once, which a cancellation while the finish was in flight has already seen to
                    target.sealed(run);
                    if (done.compareAndSet(false, true)) {
                        listener.onResponse(offsets);
                    }
                }, this::fail));
                return;
            }
            TopNRow row = rows.get(next);
            Segment from = segmentOf(row.valueSegment);
            if (from == null) {
                fail(new IllegalStateException("row points at an unknown value segment [" + row.valueSegment + "]"));
                return;
            }
            int i = next++;
            long offset = row.valueOffset;
            int length = row.valueLength;
            from.cursor(ActionListener.wrap(cursor -> cursor.readAt(offset, length, ActionListener.wrap(bytes -> {
                bytesRead.addAndGet(length);
                synchronized (lock) {
                    queued += length;
                    reading = false;
                }
                BytesRef ref = bytes.toBytesRef();
                into.append(0, out -> out.writeBytes(ref.bytes, ref.offset, ref.length), bytes::close, ActionListener.wrap(written -> {
                    offsets[i] = written;
                    bytesWritten.addAndGet(length);
                    appended(length);
                }, e -> {
                    fail(e);
                    appended(length);
                }));
                advance();
            }, this::fail)), this::fail));
        }

        private void appended(long length) {
            boolean resume;
            synchronized (lock) {
                queued -= length;
                resume = parked || (next == rows.size() && queued == 0);
                parked = false;
            }
            if (resume) {
                advance();
            }
        }

        private void fail(Exception e) {
            if (done.compareAndSet(false, true) == false) {
                return;
            }
            if (into != null) {
                into.abort(ActionListener.running(() -> listener.onFailure(e)));
            } else {
                listener.onFailure(e);
            }
        }
    }

    Lane newLane() {
        synchronized (this) {
            return new Lane(lanes++);
        }
    }

    private synchronized Segment newSegment() {
        Segment segment = new Segment(segments.size());
        segments.add(segment);
        return segment;
    }

    @Nullable
    private synchronized Segment segmentOf(int id) {
        return id >= 0 && id < segments.size() ? segments.get(id) : null;
    }

    /**
     * Deletes a segment that no row references any more; its reader goes first. A segment whose seal is still in flight
     * goes when the seal lands; the scope's close removes what a failed deletion leaves behind.
     */
    private void retire(Segment segment) {
        FramedRun run;
        synchronized (segment) {
            if (segment.retired) {
                return;
            }
            segment.retired = true;
            run = segment.run;
        }
        segment.closeReader();
        if (run != null) {
            deleteRun(segment, run);
        }
    }

    private static void deleteRun(Segment segment, FramedRun run) {
        run.delete(
            ActionListener.wrap(
                ignored -> {},
                e -> logger.debug(
                    () -> Strings.format("value segment [%s] could not be deleted; the scope's close removes it", segment.name),
                    e
                )
            )
        );
    }

    /**
     * Reads the values of {@code rows[from, to)} in physical order and hands them back in the rows' order, one read in
     * flight at a time; every value read is held until the caller closes it, so the caller sizes the range by what it may
     * hold at once. Each buffer is the caller's to close; a row whose values never went to a segment gets null.
     */
    DriverSpillOperation<ReleasableBytesReference[]> gather(List<TopNRow> rows, int from, int to) {
        int count = to - from;
        Integer[] order = new Integer[count];
        for (int i = 0; i < count; i++) {
            order[i] = from + i;
        }
        Arrays.sort(
            order,
            Comparator.comparingInt((Integer i) -> rows.get(i).valueSegment).thenComparingLong(i -> rows.get(i).valueOffset)
        );
        long start = System.nanoTime();
        return DriverSpillOperation.issue(
            driverContext,
            "top-n value gather",
            l -> new Gather(rows, from, order, ActionListener.runBefore(l, () -> spillNanos.addAndGet(System.nanoTime() - start))).advance()
        );
    }

    /**
     * The reads of one page's values, in place while they complete inline and on the completing thread otherwise.
     */
    private final class Gather {
        private final List<TopNRow> rows;
        private final int from;
        private final Integer[] order;
        private final ReleasableBytesReference[] values;
        private final ActionListener<ReleasableBytesReference[]> listener;
        private final AtomicInteger turns = new AtomicInteger();
        private final AtomicBoolean done = new AtomicBoolean();
        private int at;
        private boolean reading;

        Gather(List<TopNRow> rows, int from, Integer[] order, ActionListener<ReleasableBytesReference[]> listener) {
            this.rows = rows;
            this.from = from;
            this.order = order;
            this.values = new ReleasableBytesReference[order.length];
            this.listener = listener;
        }

        void advance() {
            if (turns.getAndIncrement() != 0) {
                return;
            }
            do {
                step();
            } while (turns.decrementAndGet() != 0);
        }

        private void step() {
            if (done.get() || reading) {
                return;
            }
            while (at < order.length && rows.get(order[at]).spilled() == false) {
                values[order[at++] - from] = null;
            }
            if (at == order.length) {
                if (done.compareAndSet(false, true)) {
                    listener.onResponse(values);
                }
                return;
            }
            int index = order[at++];
            TopNRow row = rows.get(index);
            Segment segment = segmentOf(row.valueSegment);
            if (segment == null) {
                fail(new IllegalStateException("row points at an unknown value segment [" + row.valueSegment + "]"));
                return;
            }
            reading = true;
            segment.cursor(ActionListener.wrap(cursor -> cursor.readAt(row.valueOffset, row.valueLength, ActionListener.wrap(bytes -> {
                bytesRead.addAndGet(row.valueLength);
                values[index - from] = bytes;
                reading = false;
                advance();
            }, this::fail)), this::fail));
        }

        private void fail(Exception e) {
            if (done.compareAndSet(false, true)) {
                Releasables.closeExpectNoException(values);
                listener.onFailure(e);
            }
        }
    }

    SpillStatus status() {
        return new SpillStatus(bytesWritten.get(), bytesRead.get(), runs.get(), spillNanos.get());
    }

    int compactions() {
        return compactions.get();
    }

    int compactionsFailed() {
        return compactionsFailed.get();
    }

    synchronized int segments() {
        return segments.size();
    }

    /**
     * Retires every segment; the lanes are closed by their owners first. A segment whose seal is in flight goes when the
     * seal lands; the scope's close is the backstop for the rest.
     */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true) == false) {
            return;
        }
        List<Segment> all;
        synchronized (this) {
            all = new ArrayList<>(segments);
        }
        for (Segment segment : all) {
            retire(segment);
        }
    }
}
