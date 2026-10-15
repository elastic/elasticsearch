/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.TaskCancelledException;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

/**
 * EQL's positive-stage selection over a globally ordered stream of stage candidates.
 * <p>
 * The latest strict predecessor consumes all earlier predecessors. Because input is merged before
 * matching, no future candidate can need an overwritten prefix: one prefix per key and incomplete
 * stage suffices. An expiry index removes prefixes outside the total maxspan without scanning the
 * key space or accumulating stale queue entries. Only copied keys and source-event handles are
 * retained; event payloads belong to the subsequent fetch phase.
 * <p>
 * Input must be ordered by source ordinal, then descending stage, then the canonical key. Descending
 * stages let an event satisfying multiple stages advance an earlier event before becoming a new
 * anchor itself. Canonical keys encode typed scalar values; null/multivalue handling belongs to the
 * stage reader. Page boundaries have no significance to this matcher.
 * <p>
 * The owning execution serializes calls, including close. Completed matches transfer their own
 * request-breaker reservation to the caller and can be closed independently on any thread.
 */
public final class SequenceMatcher implements Releasable {
    private static final String BREAKER_LABEL = "esql_sequence";
    private static final long ORDINAL_BYTES = RamUsageEstimator.shallowSizeOfInstance(SequenceOrdinal.class);
    private static final long BASE_BYTES = RamUsageEstimator.shallowSizeOfInstance(SequenceMatcher.class) + RamUsageEstimator
        .shallowSizeOfInstance(HashMap.class) + RamUsageEstimator.shallowSizeOfInstance(TreeSet.class) + RamUsageEstimator
            .shallowSizeOfInstance(TreeMap.class) + ORDINAL_BYTES;
    // Cover treeified hash entries and the temporary old/new nodes during a tree split or conversion.
    private static final long HASH_ENTRY_BYTES = 2 * RamUsageEstimator.alignObjectSize(
        RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + 9L * RamUsageEstimator.NUM_BYTES_OBJECT_REF + 12
    );
    private static final long EXPIRY_ENTRY_BYTES = RamUsageEstimator.alignObjectSize(
        RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + 6L * RamUsageEstimator.NUM_BYTES_OBJECT_REF + 1
    );
    private static final Comparator<Candidate> EXPIRY_ORDER = Comparator.comparing((Candidate c) -> c.start).thenComparingLong(c -> c.id);

    private final int stages;
    private final long maxSpanNanos;
    private final CircuitBreaker breaker;
    private final BooleanSupplier cancelled;
    private final Comparator<BytesRef> keyOrder;
    // Initial capacity >=64 prevents collision-driven early resizes below HashMap's treeification threshold.
    private Map<BytesRef, KeyState> keys = new HashMap<>(64);
    private final TreeSet<Candidate> expiry = new TreeSet<>(EXPIRY_ORDER);
    private SequenceOrdinal previousOrdinal;
    private int previousStage;
    private BytesRef previousKey;
    private long nextId;
    private long heldBytes;
    private int tableCapacity;
    private long tableBytes;
    private boolean closed;

    /** Creates an empty matcher using the query's request breaker and cancellation signal. */
    public SequenceMatcher(int stages, TimeValue maxSpan, CircuitBreaker breaker, BooleanSupplier cancelled) {
        this(stages, maxSpan, breaker, cancelled, BytesRef::compareTo);
    }

    /** Accepts the event reader's key ordering without coupling matching or equality to its value encoding. */
    public SequenceMatcher(
        int stages,
        TimeValue maxSpan,
        CircuitBreaker breaker,
        BooleanSupplier cancelled,
        Comparator<BytesRef> keyOrder
    ) {
        if (stages < 2) {
            throw new IllegalArgumentException("a sequence requires at least two stages");
        }
        if (maxSpan.nanos() <= 0) {
            throw new IllegalArgumentException("sequence maxspan must be positive");
        }
        this.stages = stages;
        this.maxSpanNanos = maxSpan.nanos();
        this.breaker = Objects.requireNonNull(breaker);
        this.cancelled = Objects.requireNonNull(cancelled);
        this.keyOrder = Objects.requireNonNull(keyOrder);
        reserve(BASE_BYTES);
    }

    /**
     * Consumes a stage row. A returned match is owned by the caller and must be closed, even if
     * downstream filtering discards it. Keys and handles are copied before retention, allowing
     * the reader to release its page immediately. Any failure closes all pending state.
     */
    @Nullable
    public Match match(int stage, BytesRef key, SequenceOrdinal ordinal, BytesRef handle) {
        ensureOpen();
        try {
            checkCancelled();
            if (stage < 0 || stage >= stages) {
                throw new IllegalArgumentException("invalid sequence stage [" + stage + "]");
            }
            Objects.requireNonNull(key);
            Objects.requireNonNull(ordinal);
            Objects.requireNonNull(handle);
            checkOrder(stage, key, ordinal);
            prune(ordinal);
            KeyState state = keys.get(key);
            if (stage == 0) {
                if (state != null && state.slots[0] != null) {
                    if (state.slots[0].last.equals(ordinal)) {
                        return null; // Duplicate expanded key from the same source event.
                    }
                    discard(state.slots[0]);
                    state = keys.get(key);
                }
                if (state == null) {
                    state = addKey(key);
                }
                long bytes = candidateBytes(stages) + bytesRefBytes(handle);
                reserve(bytes);
                Candidate candidate = new Candidate(state, ordinal, BytesRef.deepCopyOf(handle), stages, nextId++, bytes);
                state.slots[0] = candidate;
                state.active++;
                expiry.add(candidate);
                return null;
            }
            if (state == null) {
                return null;
            }
            Candidate prefix = state.slots[stage - 1];
            if (prefix == null || prefix.last.compareTo(ordinal) >= 0) {
                return null;
            }
            if (stage == stages - 1) {
                Match match = complete(prefix, ordinal, handle);
                discard(prefix);
                heldBytes -= match.bytes; // Transfer the admitted bytes without refunding the caller's reservation.
                return match;
            }
            Candidate displaced = state.slots[stage];
            if (displaced != null) {
                discard(displaced);
            }
            long handleBytes = bytesRefBytes(handle);
            reserve(handleBytes);
            prefix.handles[stage] = BytesRef.deepCopyOf(handle);
            prefix.bytes += handleBytes;
            state.slots[stage - 1] = null;
            state.slots[stage] = prefix;
            prefix.stage = stage;
            prefix.last = ordinal;
            return null;
        } catch (RuntimeException e) {
            close();
            throw e;
        }
    }

    private void checkOrder(int stage, BytesRef key, SequenceOrdinal ordinal) {
        if (previousOrdinal != null) {
            int comparison = previousOrdinal.compareTo(ordinal);
            if (comparison > 0 || (comparison == 0 && stage > previousStage)) {
                throw new IllegalArgumentException("sequence candidates must be ordered by ordinal then descending stage");
            }
            if (comparison == 0 && stage == previousStage && keyOrder.compare(previousKey, key) > 0) {
                throw new IllegalArgumentException("equal-ordinal stage candidates must be ordered by key");
            }
        }
        // The previous key can outlive its prefix and the reader's page. Charge its copy independently.
        long bytes = bytesRefBytes(key);
        reserve(bytes);
        BytesRef copy = BytesRef.deepCopyOf(key);
        if (previousKey != null) {
            release(bytesRefBytes(previousKey));
        }
        previousOrdinal = ordinal;
        previousStage = stage;
        previousKey = copy;
    }

    private KeyState addKey(BytesRef key) {
        long bytes = RamUsageEstimator.shallowSizeOfInstance(KeyState.class) + HASH_ENTRY_BYTES + referenceArrayBytes(stages - 1)
            + bytesRefBytes(key);
        reserve(bytes);
        KeyState state = new KeyState(BytesRef.deepCopyOf(key), stages - 1, bytes);
        long oldTableBytes = tableBytes;
        boolean grow = tableCapacity == 0 || keys.size() + 1 > tableCapacity * 3L / 4;
        if (grow) {
            int newCapacity = tableCapacity == 0 ? 64 : Math.multiplyExact(tableCapacity, 2);
            long newTableBytes = referenceArrayBytes(newCapacity);
            reserve(newTableBytes); // Both arrays are live during HashMap.resize.
            tableCapacity = newCapacity;
            tableBytes = newTableBytes;
        }
        keys.put(state.key, state);
        if (grow) {
            release(oldTableBytes);
        }
        return state;
    }

    private void prune(SequenceOrdinal watermark) {
        while (expiry.isEmpty() == false && watermark.exceedsSpan(expiry.first().start, maxSpanNanos)) {
            checkCancelled();
            discard(expiry.first());
        }
    }

    private void discard(Candidate candidate) {
        expiry.remove(candidate);
        KeyState state = candidate.state;
        state.slots[candidate.stage] = null;
        release(candidate.bytes);
        if (--state.active == 0) {
            keys.remove(state.key);
            release(state.bytes);
        }
    }

    private Match complete(Candidate prefix, SequenceOrdinal ordinal, BytesRef finalHandle) {
        long bytes = RamUsageEstimator.shallowSizeOfInstance(Match.class) + RamUsageEstimator.shallowSizeOfInstance(AtomicBoolean.class)
            + referenceArrayBytes(stages) + 64 // Arrays.asList and its unmodifiable wrapper.
            + bytesRefBytes(prefix.state.key) + 2 * ORDINAL_BYTES + bytesRefBytes(finalHandle);
        for (int stage = 0; stage < stages - 1; stage++) {
            bytes += bytesRefBytes(prefix.handles[stage]);
        }
        reserve(bytes);
        BytesRef[] handles = new BytesRef[stages];
        for (int stage = 0; stage < stages - 1; stage++) {
            handles[stage] = BytesRef.deepCopyOf(prefix.handles[stage]);
        }
        handles[stages - 1] = BytesRef.deepCopyOf(finalHandle);
        return new Match(breaker, bytes, BytesRef.deepCopyOf(prefix.state.key), prefix.start, ordinal, handles);
    }

    private static long candidateBytes(int stages) {
        return RamUsageEstimator.shallowSizeOfInstance(Candidate.class) + EXPIRY_ENTRY_BYTES + referenceArrayBytes(stages) + 2
            * ORDINAL_BYTES;
    }

    private static long bytesRefBytes(BytesRef ref) {
        return RamUsageEstimator.shallowSizeOfInstance(BytesRef.class) + RamUsageEstimator.alignObjectSize(
            RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) ref.length
        );
    }

    private static long referenceArrayBytes(int length) {
        return RamUsageEstimator.alignObjectSize(
            RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) length * RamUsageEstimator.NUM_BYTES_OBJECT_REF
        );
    }

    private void reserve(long bytes) {
        breaker.addEstimateBytesAndMaybeBreak(bytes, BREAKER_LABEL);
        heldBytes += bytes;
    }

    private void release(long bytes) {
        breaker.addWithoutBreaking(-bytes, BREAKER_LABEL);
        heldBytes -= bytes;
    }

    private void checkCancelled() {
        if (cancelled.getAsBoolean()) {
            throw new TaskCancelledException("ES|QL sequence execution cancelled");
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("sequence matcher is closed");
        }
    }

    int pendingCandidates() {
        return expiry.size();
    }

    long ramBytesUsed() {
        return heldBytes;
    }

    @Override
    public void close() {
        if (closed == false) {
            closed = true;
            expiry.clear();
            keys = null;
            previousKey = null;
            previousOrdinal = null;
            release(heldBytes);
        }
    }

    private static final class KeyState {
        private final BytesRef key;
        private final Candidate[] slots;
        private final long bytes;
        private int active;

        private KeyState(BytesRef key, int stages, long bytes) {
            this.key = key;
            this.slots = new Candidate[stages];
            this.bytes = bytes;
        }
    }

    private static final class Candidate {
        private final KeyState state;
        private final SequenceOrdinal start;
        private SequenceOrdinal last;
        private final BytesRef[] handles;
        private final long id;
        private int stage;
        private long bytes;

        private Candidate(KeyState state, SequenceOrdinal ordinal, BytesRef handle, int stages, long id, long bytes) {
            this.state = state;
            this.start = ordinal;
            this.last = ordinal;
            this.handles = new BytesRef[stages];
            this.handles[0] = handle;
            this.id = id;
            this.bytes = bytes;
        }
    }

    /**
     * A completed sequence whose copied payload handles remain valid until close, independently of the matcher.
     * Getters and closure are synchronized, so a concurrent close cannot return a null view. Borrowed views must
     * still be consumed before closing their owner; callers must coordinate the lifetime of in-flight payload reads.
     */
    public static final class Match implements Releasable {
        private final CircuitBreaker breaker;
        private final long bytes;
        private BytesRef key;
        private final SequenceOrdinal start;
        private final SequenceOrdinal end;
        private List<BytesRef> handles;
        private final AtomicBoolean closed = new AtomicBoolean();

        private Match(CircuitBreaker breaker, long bytes, BytesRef key, SequenceOrdinal start, SequenceOrdinal end, BytesRef[] handles) {
            this.breaker = breaker;
            this.bytes = bytes;
            this.key = key;
            this.start = start;
            this.end = end;
            this.handles = Collections.unmodifiableList(Arrays.asList(handles));
        }

        /** Returns a borrowed, read-only view of the canonical key. */
        public synchronized BytesRef key() {
            ensureOpen();
            return key;
        }

        public SequenceOrdinal start() {
            return start;
        }

        public SequenceOrdinal end() {
            return end;
        }

        /** Returns borrowed, read-only source handles in stage order, for deferred payload fetching. */
        public synchronized List<BytesRef> handles() {
            ensureOpen();
            return handles;
        }

        private void ensureOpen() {
            if (closed.get()) {
                throw new IllegalStateException("sequence match is closed");
            }
        }

        @Override
        public synchronized void close() {
            if (closed.compareAndSet(false, true)) {
                handles = null;
                key = null;
                breaker.addWithoutBreaking(-bytes, BREAKER_LABEL);
            }
        }
    }
}
