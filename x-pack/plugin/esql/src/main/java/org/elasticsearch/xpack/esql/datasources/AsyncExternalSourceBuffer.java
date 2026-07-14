/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReaderStatus;
import org.elasticsearch.xpack.esql.datasources.spi.SkipWarnings;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Thread-safe buffer for async external source data.
 * Modeled after {@link org.elasticsearch.compute.operator.exchange.ExchangeBuffer}.
 *
 * This buffer provides:
 * - Thread-safe page queue for cross-thread communication
 * - Byte-based backpressure control proportional to actual memory usage
 * - Notification via {@link SubscribableListener} when data becomes available
 * - Lifecycle management (finished state tracking)
 */
public final class AsyncExternalSourceBuffer {

    /**
     * Default byte limit for the buffer, preserving the original "10 normal-sized pages" intent.
     */
    public static final long DEFAULT_MAX_BUFFER_BYTES = 10L * Operator.TARGET_PAGE_SIZE;

    private final Queue<Page> queue = new ConcurrentLinkedQueue<>();
    // uses a separate counter for size for CAS; and ConcurrentLinkedQueue#size is not a constant time operation.
    private final AtomicInteger queueSize = new AtomicInteger();
    private final AtomicLong bytesInBuffer = new AtomicLong();
    private final long maxBufferBytes;

    private final Object notEmptyLock = new Object();
    private SubscribableListener<Void> notEmptyFuture = null;

    private final Object notFullLock = new Object();
    private SubscribableListener<Void> notFullFuture = null;

    private final SubscribableListener<Void> completionFuture = new SubscribableListener<>();

    private final AtomicBoolean noMoreInputs = new AtomicBoolean(false);
    private volatile Throwable failure = null;

    /**
     * Set when a live producer is cut by a hard stop — i.e. {@link #finish(boolean) finish(true)} performs the
     * running→finishing transition (task cancel / async DELETE tearing the operator down, or a LIMIT teardown
     * closing the source while the producer is still reading). Unlike {@link #noMoreInputs}, this is <em>not</em>
     * set by async STOP ({@code finish(false)}, which keeps buffered pages for a partial response) nor by natural
     * EOF (where the producer's own {@code finish(false)} wins the transition, so the driver's later
     * {@code finish(true)} on close no longer transitions). It is consulted as the ambient
     * {@link StorageRetryCancellation} signal installed around the runtime producer read so an in-flight storage
     * retry/throttle backoff aborts promptly instead of sleeping through its budget while the query is already
     * cancelled. See {@link StorageRetryCancellation} for why STOP must not trip this, and for the
     * degenerate-query case this does <em>not</em> fix: a read wedged in a genuinely uncancellable operation off
     * the scoped thread (a parallel-parse worker, a native reader) still unwinds only on its own timeout, so the
     * driver's completion and final resource release wait for it even though the task is already marked cancelled.
     */
    private volatile boolean readCancelled = false;

    /**
     * Per-file captured source metadata contributions, populated by the background reader thread as
     * iterators close. Each path's value is a list of flat {@code _stats.*} maps — one per chunk for
     * parallel parsing, one per split for macro-splits, one for whole-file reads. The coordinator
     * merges them via {@code SourceStatisticsSerializer.mergeStatistics} before enriching the
     * {@code SchemaCacheEntry}.
     */
    private final ConcurrentMap<String, List<Map<String, Object>>> capturedSourceMetadata = new ConcurrentHashMap<>();
    private volatile Map<String, List<Map<String, Object>>> cachedMetadataSnapshot = Map.of();
    private volatile int cachedMetadataPathCount = 0;

    /**
     * Client-visible warnings recorded by the background reader path — both genuine partial-results
     * signals (currently a streaming {@code max_record_size} truncation under a non-strict
     * {@code error_mode}, see {@code StreamingParallelParsingCoordinator}) and per-record
     * skip/null-fill warnings relayed from format-reader {@code SkipWarnings} sinks (see
     * {@code FormatReadContext#informationalWarningSink()} / {@code RangeReadContext#informationalWarningSink()}),
     * which do not necessarily imply a dropped record. See {@link #recordWarning} vs {@link
     * #recordInformationalWarning}. Producer / parse-worker threads append here off the driver thread;
     * {@link AsyncExternalSourceOperator#close()} drains and re-emits them via {@link
     * org.elasticsearch.common.logging.HeaderWarning} on the driver thread, whose response headers
     * {@code DriverRunner} collects into the client response. Emitting from the forked worker thread
     * directly would land the header on that worker's {@code ThreadContext}, which is never merged
     * back into the response — so the warning would be invisible to the client.
     */
    private final Queue<String> pendingWarnings = new ConcurrentLinkedQueue<>();

    /**
     * Cap on informational warning lines a single query may emit via {@link #recordInformationalWarning}
     * across every concurrently-parsed segment/chunk. Each {@code SkipWarnings} instance already caps its
     * own detail count at {@link SkipWarnings#MAX_ADDED_WARNINGS}, but that cap is per reader instance, not
     * per query — a parallel or macro-split read constructs one instance per chunk/segment, so without a
     * cap here a single read could add far more than that to {@link #pendingWarnings}, multiplying response
     * header count by chunk/segment count. The {@code +2} mirrors the 1 summary + 1 overflow line a single
     * {@code SkipWarnings} instance adds around its own cap.
     */
    private static final int MAX_INFORMATIONAL_WARNINGS = SkipWarnings.MAX_ADDED_WARNINGS + 2;

    // Each caller gets a unique count, so exactly one caller ever sees count == MAX_INFORMATIONAL_WARNINGS
    // and adds the overflow line — no separate overflow flag needed.
    private final AtomicInteger informationalWarningsAdded = new AtomicInteger();

    /**
     * Set when the background reader path drops data under a lenient policy — currently a streaming
     * {@code max_record_size} truncation under a non-strict {@code error_mode}. Surfaced through the
     * operator's {@code Status} into {@link org.elasticsearch.compute.operator.DriverCompletionInfo} so the
     * coordinator can flip the response's {@code is_partial} flag (the structured counterpart of the
     * client-visible {@link #pendingWarnings} message). {@code volatile}: written on the parse-worker thread,
     * read on the driver thread when building status.
     */
    private volatile boolean partial = false;

    private volatile FormatReaderStatus formatReaderStatus = null;
    // LongAdder (rather than the AtomicLong used for {@link #bytesInBuffer}) because every read
    // iteration adds a delta to bytesRead, so contention between concurrent producer threads on
    // multi-file paths would dominate AtomicLong's CAS cost. bytesInBuffer is a single producer /
    // single consumer counter and stays AtomicLong.
    private final LongAdder bytesRead = new LongAdder();
    private volatile int splitsTotal = 0;
    private final AtomicInteger splitsProcessed = new AtomicInteger();
    private volatile int currentSplit = 0;

    public AsyncExternalSourceBuffer(long maxBufferBytes) {
        if (maxBufferBytes < 1) {
            throw new IllegalArgumentException("max_buffer_bytes must be at least one; got=" + maxBufferBytes);
        }
        this.maxBufferBytes = maxBufferBytes;
    }

    /** The mutable per-file capture sink shared with the iterator wrapping. */
    public ConcurrentMap<String, List<Map<String, Object>>> capturedSourceMetadataSink() {
        return capturedSourceMetadata;
    }

    /**
     * Records a client-visible partial-results warning to be re-emitted on the driver thread when the
     * operator closes, and flips {@link #partial}. Thread-safe: called from the background reader /
     * parse-worker thread.
     * <p>
     * This sink is wired exclusively to the lenient {@code max_record_size} truncation path (see
     * {@code StreamingParallelParsingCoordinator#emitTruncationWarning}): a recorded warning here
     * always means the read returned fewer records than the source held. Per-record {@code SkipWarnings}
     * warnings (row skipped or field null-filled under a lenient {@code ErrorPolicy}) must use
     * {@link #recordInformationalWarning} instead — not because a skipped row is never a "real" partial
     * result, but because {@link #partial} has never tracked that case (this predates warning-sink
     * relaying entirely: on the driver thread such warnings always emitted straight to
     * {@link org.elasticsearch.common.logging.HeaderWarning} without touching this flag). Overloading
     * {@link #partial}'s meaning to also cover {@code SKIP_ROW} drops is a separate, pre-existing
     * question and out of scope here.
     */
    public void recordWarning(String warning) {
        pendingWarnings.add(warning);
        partial = true;
    }

    /**
     * Records a client-visible warning to be re-emitted on the driver thread when the operator closes,
     * without affecting {@link #partial}. Thread-safe: called from the background reader / parse-worker
     * thread.
     * <p>
     * Use this for warnings relayed from format-reader {@code SkipWarnings} sinks (see {@code
     * FormatReadContext#informationalWarningSink()} / {@code RangeReadContext#informationalWarningSink()})
     * — e.g. CSV/NDJSON per-record skip/null-fill handling or Parquet on-disk/planner type mismatches.
     * This preserves these warnings' pre-existing behavior of never flipping {@link #partial} (previously
     * they only ever reached {@link org.elasticsearch.common.logging.HeaderWarning} directly, which has
     * no notion of {@link #partial} either); this method only fixes their delivery when the read runs
     * off the driver thread, without changing what they signal. See {@link #recordWarning} for the one
     * warning that has always mapped to {@link #partial}.
     * <p>
     * Each {@code SkipWarnings} instance caps its own per-event details at
     * {@code SkipWarnings.MAX_ADDED_WARNINGS} (20), but that cap is per reader instance, not per query:
     * a parallel or macro-split read constructs one {@code SkipWarnings} per chunk/segment. This method
     * applies {@link #MAX_INFORMATIONAL_WARNINGS} as a single cap across every caller so that a read
     * split into many chunks/segments cannot multiply {@link #pendingWarnings}'s size by chunk/segment
     * count — otherwise a large enough split count can grow response headers past what the client (or
     * an intermediate proxy) is willing to accept.
     */
    public void recordInformationalWarning(String warning) {
        int count = informationalWarningsAdded.incrementAndGet();
        if (count < MAX_INFORMATIONAL_WARNINGS) {
            pendingWarnings.add(warning);
        } else if (count == MAX_INFORMATIONAL_WARNINGS) {
            pendingWarnings.add("... further reader warnings suppressed (more than " + (MAX_INFORMATIONAL_WARNINGS - 1) + " recorded)");
        }
    }

    /** Removes and returns the next recorded warning, or {@code null} if none remain. */
    public String pollWarning() {
        return pendingWarnings.poll();
    }

    /**
     * Whether the background read dropped data under a lenient policy (see {@link #partial}). Read on the
     * driver thread when assembling the operator {@code Status}.
     */
    public boolean isPartial() {
        return partial;
    }

    /**
     * Returns an immutable point-in-time snapshot of the capture sink. Read by the driver thread
     * via {@link AsyncExternalSourceOperator#status()}. Returning an unmodifiable view defends
     * against downstream callers mutating the snapshot in place, which would silently lose stats
     * before they reach the coordinator's reconciler.
     * <p>
     * The snapshot is cached and rebuilt only when the number of tracked file paths grows or when
     * the buffer reaches completion. In-flight status calls during execution may therefore see a
     * slightly stale view of the per-file contribution lists (e.g. missing a later parallel-parsing
     * chunk for an already-tracked path). The final snapshot taken after {@link #finish} is always
     * rebuilt in full so {@code DriverCompletionInfo} captures all contributions.
     */
    Map<String, List<Map<String, Object>>> capturedSourceMetadataSnapshot() {
        int currentSize = capturedSourceMetadata.size();
        if (currentSize == 0) {
            return Map.of();
        }
        if (currentSize == cachedMetadataPathCount && isFinished() == false) {
            return cachedMetadataSnapshot;
        }
        HashMap<String, List<Map<String, Object>>> snapshot = new HashMap<>(currentSize);
        for (var entry : capturedSourceMetadata.entrySet()) {
            List<Map<String, Object>> list = entry.getValue();
            List<Map<String, Object>> copy;
            synchronized (list) {
                copy = List.copyOf(list);
            }
            snapshot.put(entry.getKey(), copy);
        }
        Map<String, List<Map<String, Object>>> result = Collections.unmodifiableMap(snapshot);
        // Write snapshot before count so that a reader observing the new count via the volatile
        // read is guaranteed (by JMM happens-before) to also see the new snapshot.
        cachedMetadataSnapshot = result;
        cachedMetadataPathCount = currentSize;
        return result;
    }

    /**
     * Add a page to the buffer. Called by the background reader thread.
     */
    public void addPage(Page page) {
        if (failure != null) {
            // Reject the page without touching buffer state, so the trailing invariantsHold()
            // call is intentionally bypassed: nothing was mutated for it to check.
            page.releaseBlocks();
            return;
        }
        long pageBytes = page.ramBytesUsedByBlocks();
        bytesInBuffer.addAndGet(pageBytes);
        queue.add(page);
        queueSize.incrementAndGet();
        // Always notify: the conditional guard on prevBytes==0 previously caused a lost-wakeup race
        // when a consumer drained and blocked on notEmptyFuture between our getAndAdd and queue.add.
        // notifyNotEmpty() is a no-op when no listener is registered, so unconditional fire is cheap.
        notifyNotEmpty();
        if (noMoreInputs.get()) {
            // O(N) but acceptable because it only occurs with finish(), and the queue size should be very small.
            if (queue.removeIf(p -> p == page)) {
                page.releaseBlocks();
                queueSize.decrementAndGet();
                long afterRemove = bytesInBuffer.addAndGet(-pageBytes);
                if (afterRemove < maxBufferBytes) {
                    notifyNotFull();
                }
                if (queueSize.get() == 0) {
                    completionFuture.onResponse(null);
                }
            }
        }
        assert invariantsHold() : "buffer invariants violated after addPage";
    }

    /**
     * Poll a page from the buffer. Called by the operator (driver thread).
     *
     * @return the next page, or {@code null} if no pages available
     */
    public Page pollPage() {
        Page page = queue.poll();
        if (page == null) {
            signalCompletionIfDrained();
            assert invariantsHold() : "buffer invariants violated after pollPage (empty)";
            return null;
        }
        queueSize.decrementAndGet();
        long pageBytes = page.ramBytesUsedByBlocks();
        bytesInBuffer.addAndGet(-pageBytes);
        // Always notify: the previous threshold-crossing guard could miss a crossing because the
        // producer's waitForSpace snapshot of bytesInBuffer can race with concurrent addPage calls,
        // orphaning notFullFuture. notifyNotFull() is a no-op when no listener is registered.
        notifyNotFull();
        signalCompletionIfDrained();
        assert invariantsHold() : "buffer invariants violated after pollPage";
        return page;
    }

    /**
     * Completes {@link #completionFuture} once the queue is drained and no more input is expected.
     * Safe to call repeatedly; no-ops if completion was already signaled.
     */
    private void signalCompletionIfDrained() {
        if (noMoreInputs.get() == false || queueSize.get() != 0 || completionFuture.isDone()) {
            return;
        }
        if (failure != null) {
            completionFuture.onFailure(new Exception(failure));
        } else {
            completionFuture.onResponse(null);
        }
    }

    private void notifyNotEmpty() {
        final SubscribableListener<Void> toNotify;
        synchronized (notEmptyLock) {
            toNotify = notEmptyFuture;
            notEmptyFuture = null;
        }
        if (toNotify != null) {
            toNotify.onResponse(null);
        }
    }

    private void notifyNotFull() {
        final SubscribableListener<Void> toNotify;
        synchronized (notFullLock) {
            toNotify = notFullFuture;
            notFullFuture = null;
        }
        if (toNotify != null) {
            toNotify.onResponse(null);
        }
    }

    /**
     * Returns a {@link SubscribableListener} that completes when the buffer has space for writing.
     * This is the method producers use for backpressure coordination: it integrates directly with
     * ES async patterns and the producer drain loops.
     *
     * @return a listener that completes when space is available, or an already-completed listener if space exists
     */
    public SubscribableListener<Void> waitForSpace() {
        if (bytesInBuffer.get() < maxBufferBytes || noMoreInputs.get()) {
            return SubscribableListener.newSucceeded(null);
        }
        synchronized (notFullLock) {
            if (bytesInBuffer.get() < maxBufferBytes || noMoreInputs.get()) {
                return SubscribableListener.newSucceeded(null);
            }
            if (notFullFuture == null) {
                notFullFuture = new SubscribableListener<>();
            }
            return notFullFuture;
        }
    }

    /**
     * Returns an {@link IsBlockedResult} that completes when the buffer has data for reading.
     * Used by operator to signal driver when waiting for data.
     */
    public IsBlockedResult waitForReading() {
        if (size() > 0 || noMoreInputs.get()) {
            return Operator.NOT_BLOCKED;
        }
        synchronized (notEmptyLock) {
            if (size() > 0 || noMoreInputs.get()) {
                return Operator.NOT_BLOCKED;
            }
            if (notEmptyFuture == null) {
                notEmptyFuture = new SubscribableListener<>();
            }
            return new IsBlockedResult(notEmptyFuture, "async external source buffer empty");
        }
    }

    // Drains and releases every queued page on teardown. Only call from finish/onFailure;
    // bytesInBuffer is reset wholesale, which is only safe when no further pollPage() is expected
    // to subtract from it.
    private void discardPages() {
        Page p;
        while ((p = queue.poll()) != null) {
            queueSize.decrementAndGet();
            p.releaseBlocks();
        }
        bytesInBuffer.set(0);
        assert invariantsHold() : "buffer invariants violated after discardPages";
    }

    /**
     * Mark the buffer as finished. Called when reading is done or an error occurs.
     * <p>
     * {@code drainingPages} is honored regardless of whether this call wins the {@code noMoreInputs}
     * transition: {@link AsyncExternalSourceOperator#close()} always calls {@code finish(true)}, and
     * by the time a driver closes its operator {@code noMoreInputs} has very often already been set
     * by the producer's own {@link #onFailure} or an earlier {@code finish(false)} — e.g. the producer
     * reached natural EOF, or the read failed, before the driver got a chance to drain every page via
     * {@code getOutput()}/{@link #pollPage()}. Gating {@link #discardPages()} behind the transition
     * used to skip it entirely in that (common) case, leaking whatever the producer had already
     * buffered when the driver's close is not preceded by a full drain (e.g. cross-driver task
     * cancellation cutting this operator before its own poll loop ever ran).
     *
     * @return {@code true} if this call performed the running→finishing transition; {@code false} if the buffer had
     *         already been finished (e.g. producer reached natural EOF, or a concurrent {@code finish}/{@code onFailure}
     *         beat us to it). The stop-hook path in {@code AsyncExternalSourceOperatorFactory} uses this to distinguish
     *         "STOP genuinely cut a running producer" (partial result) from "STOP raced with natural completion"
     *         (honestly complete result).
     */
    public boolean finish(boolean drainingPages) {
        boolean transitioned = noMoreInputs.compareAndSet(false, true);
        // A draining finish that actually made the transition is a hard cut of a still-running producer
        // (cancel / DELETE / LIMIT teardown), never natural EOF (producer's own finish(false) wins first) nor
        // STOP (drainingPages == false). Only then arm the read-cancellation signal so an in-flight storage
        // backoff aborts; see the readCancelled javadoc.
        if (drainingPages && transitioned) {
            readCancelled = true;
        }
        // See the javadoc above for why this must not be gated on `transitioned`.
        if (drainingPages) {
            discardPages();
        }
        notifyNotEmpty();
        notifyNotFull(); // wake producers so they observe noMoreInputs and exit
        signalCompletionIfDrained();
        assert invariantsHold() : "buffer invariants violated after finish";
        return transitioned;
    }

    /**
     * Mark the buffer as failed. Called when the background reader encounters an error.
     * <p>
     * Queued pages are retained so the driver can drain them before {@link AsyncExternalSourceOperator}
     * surfaces the failure via {@link org.elasticsearch.compute.operator.SourceOperator#getOutput()}.
     */
    public void onFailure(Throwable t) {
        this.failure = t;
        noMoreInputs.set(true);
        notifyNotEmpty();
        notifyNotFull();
        signalCompletionIfDrained();
        assert invariantsHold() : "buffer invariants violated after onFailure";
    }

    public boolean isFinished() {
        return completionFuture.isDone();
    }

    public boolean noMoreInputs() {
        return noMoreInputs.get();
    }

    /**
     * Whether a live producer was hard-cut (see {@link #readCancelled}). Used as the ambient
     * {@link StorageRetryCancellation} signal around the runtime producer read so a parked storage
     * retry/throttle backoff aborts on cancel rather than sleeping out its budget.
     */
    public boolean readCancelled() {
        return readCancelled;
    }

    public int size() {
        return queueSize.get();
    }

    /**
     * Adds a listener that will be notified when this buffer is finished.
     */
    public void addCompletionListener(ActionListener<Void> listener) {
        completionFuture.addListener(listener);
    }

    public Throwable failure() {
        return failure;
    }

    /**
     * Returns the current number of bytes buffered, as measured by {@link Page#ramBytesUsedByBlocks()}.
     */
    public long bytesInBuffer() {
        return bytesInBuffer.get();
    }

    /** Records the latest format-reader counter snapshot for the operator's status view. */
    public void recordFormatReaderStatus(FormatReaderStatus snapshot) {
        this.formatReaderStatus = snapshot;
    }

    /** Adds {@code delta} cumulative pre-decompression bytes read from the storage layer. */
    public void addBytesRead(long delta) {
        if (delta > 0) {
            bytesRead.add(delta);
        }
    }

    /** Sets the total number of splits the producer expects to process; callable once when known. */
    public void setSplitsTotal(int total) {
        this.splitsTotal = total;
    }

    /** Increments the count of splits the producer has finished processing. */
    public void incSplitsProcessed() {
        splitsProcessed.incrementAndGet();
    }

    /** Records the 1-based index of the split currently being processed by the producer. */
    public void setCurrentSplit(int idx) {
        this.currentSplit = idx;
    }

    /** Returns the latest format-reader counter snapshot, or {@code null} if none recorded yet. */
    public FormatReaderStatus formatReaderStatus() {
        return formatReaderStatus;
    }

    /** Returns cumulative pre-decompression bytes read from the storage layer. */
    public long bytesRead() {
        return bytesRead.sum();
    }

    /** Returns the total number of splits the producer expects to process. */
    public int splitsTotal() {
        return splitsTotal;
    }

    /** Returns the number of splits the producer has finished processing. */
    public int splitsProcessed() {
        return splitsProcessed.get();
    }

    /**
     * Returns the 1-based index of the split currently being processed by the producer.
     * <p>
     * Semantics differ slightly between producer paths and the value should not be compared
     * across them: the slice-queue path counts top-level splits pulled from the queue (a
     * coalesced split with N leaves still increments the index by 1, not N), while the
     * file-list / multi-file path uses the absolute file index. Use this for "where am I in
     * the work" UX in a single-query profile, not for cross-query comparison or rate math.
     */
    public int currentSplit() {
        return currentSplit;
    }

    /**
     * Verifies internal invariants under {@code -ea}. Called from each buffer mutator so that
     * every existing test exercises the checks automatically without dedicated assertions.
     * <p>
     * Scope is intentionally narrow. The buffer is lock-free and counter updates are not atomic
     * across fields, so several legitimate transient states cannot be asserted on without
     * introducing flakiness:
     * <ul>
     * <li>{@link #addPage} updates {@code bytesInBuffer} before {@code queueSize}, so a
     *     concurrent reader may briefly observe {@code bytes > 0 && size == 0}.</li>
     * <li>{@link #pollPage} updates {@code queueSize} before {@code bytesInBuffer}, so a
     *     concurrent reader may briefly observe {@code size < N && bytes} still reflecting
     *     {@code N}.</li>
     * <li>A race between {@link #discardPages()} (which sets {@code bytesInBuffer} to {@code 0}
     *     wholesale) and the {@code noMoreInputs} cleanup branch of {@link #addPage} (which
     *     subtracts its own page bytes) can transiently push counters below zero by an
     *     unpredictable amount.</li>
     * </ul>
     * Hence the only invariant asserted here is the forward direction of completion
     * consistency: if {@code completionFuture} has signalled success, then the buffer must
     * already have observed {@code noMoreInputs}. This catches premature completion (signalling
     * done before {@code finish} / {@code onFailure} was called). Lost-wakeup regressions — the
     * bug class fixed in the unconditional {@code notifyNotEmpty}/{@code notifyNotFull} changes
     * — leave counters and the completion future internally consistent and are not detected
     * here; see {@code AsyncExternalSourceBufferTests#testNoLostWakeupUnderConcurrentAddAndPoll}
     * for that coverage.
     */
    private boolean invariantsHold() {
        if (completionFuture.isDone() && failure == null) {
            assert noMoreInputs.get() : "completionFuture done with no failure but noMoreInputs is false";
        }
        return true;
    }
}
