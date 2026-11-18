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
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;

/**
 * Row-native latest-valid pairing. Recency lists and their remembered chains retain candidates across eviction. Backward search borrows one pointer per earlier stage and binds
 * the arriving row as its terminal. Used markers release their values and output ownership but keep their list
 * slots and remembered references. Calls, including closure, are serialized by the owning source.
 * Returned matches own independent references.
 */
final class SequenceLatestValidMatcher implements SequencePairing {
    private static final String LABEL = "esql_sequence_matcher";
    private static final long ORDINAL_BYTES = RamUsageEstimator.shallowSizeOfInstance(SequenceOrdinal.class);
    private static final long HASH_ENTRY_BYTES = 2 * RamUsageEstimator.alignObjectSize(
        RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + 9L * RamUsageEstimator.NUM_BYTES_OBJECT_REF + 12
    );
    private static final long TREE_ENTRY_BYTES = RamUsageEstimator.alignObjectSize(
        RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + 6L * RamUsageEstimator.NUM_BYTES_OBJECT_REF + 1
    );
    private static final long ROW_BYTES = RamUsageEstimator.shallowSizeOfInstance(Row.class) + ORDINAL_BYTES + TREE_ENTRY_BYTES;
    private static final Row[] EMPTY_ROWS = new Row[0];
    private static final Comparator<Row> RECENCY = Comparator.comparing((Row row) -> row.ordinal)
        .thenComparing((left, right) -> Long.compare(right.rank, left.rank));

    private final int stages;
    private final long maxSpanNanos;
    private final int bound;
    private final long searchStepLimit;
    private final CircuitBreaker breaker;
    private final BooleanSupplier cancelled;
    private final Comparator<BytesRef> keyOrder;
    private SequenceLatestValidConfig config;
    private long[] maxGapNanos;
    private Map<BytesRef, KeyState> keys;
    private TreeSet<Row> expiry;
    private TreeSet<Row> unusedStarts;
    private Row[] selected;
    private int[] positions;
    private ValuesView view;
    private int terminalStage;
    private boolean arrivalSearch;
    private boolean searchAbandoned;
    private SequenceOrdinal finalOrdinal;
    private SequenceCandidateValues finalValues;
    private SequenceLatestValidConfig.PredicateGroup currentGroup;
    private SequenceOrdinal previousOrdinal;
    private int previousStage;
    private BytesRef previousKey;
    private long nextRank;
    private long generation;
    private long heldBytes;
    private int keyCapacity;
    private long keyTableBytes;
    private long boundDrops;
    private long abandonedSearches;
    private long keysThatHitBound;
    private long searchSteps;
    private long maxSearchStepsPerFinalRow;
    private long conditionEvaluations;
    private long expiredCandidates;
    private long candidatesNotKept;
    private long arrivalSearchesAbandoned;
    private long arrivalSearchSteps;
    private long conditionEvaluationsOnArrival;
    private long peakCandidatesKeptPerKey;
    private int largestRingFill;
    private boolean closed;

    /** Copies temporal configuration and admits metadata before allocating searchable state. */
    SequenceLatestValidMatcher(
        int stages,
        TimeValue maxSpan,
        CircuitBreaker breaker,
        BooleanSupplier cancelled,
        Comparator<BytesRef> keyOrder,
        @Nullable long[] maxGapNanos,
        SequenceLatestValidConfig config
    ) {
        if (stages < 2 || maxSpan.nanos() <= 0) {
            throw new IllegalArgumentException("sequence requires at least two stages and a positive maxspan");
        }
        if (config.mandatoryDemands().size() != stages) {
            throw new IllegalArgumentException("sequence mandatory demand layouts must align with stages");
        }
        if (maxGapNanos != null) {
            if (maxGapNanos.length != stages || maxGapNanos[0] != 0) {
                throw new IllegalArgumentException("sequence incoming limits must align with stages and leave stage 1 unconstrained");
            }
            for (long gap : maxGapNanos) {
                if (gap < 0) {
                    throw new IllegalArgumentException("sequence incoming limits must be positive or absent");
                }
            }
        }
        this.stages = stages;
        maxSpanNanos = maxSpan.nanos();
        bound = config.maxCandidates().orElse(Integer.MAX_VALUE);
        searchStepLimit = config.searchStepLimit();
        this.breaker = Objects.requireNonNull(breaker);
        this.cancelled = Objects.requireNonNull(cancelled);
        this.keyOrder = Objects.requireNonNull(keyOrder);
        long bytes = RamUsageEstimator.shallowSizeOfInstance(SequenceLatestValidMatcher.class) + RamUsageEstimator.shallowSizeOfInstance(
            HashMap.class
        ) + 2 * RamUsageEstimator.shallowSizeOfInstance(TreeSet.class) + 2 * RamUsageEstimator.shallowSizeOfInstance(
            java.util.TreeMap.class
        ) + RamUsageEstimator.shallowSizeOfInstance(ValuesView.class) + referenceArrayBytes(stages - 1) + intArrayBytes(stages - 1)
            + ORDINAL_BYTES + 256;
        for (var demands : config.mandatoryDemands()) {
            checkCancelled();
            bytes = Math.addExact(bytes, 64 + referenceArrayBytes(demands.size()) + 32L * demands.size());
        }
        for (var group : config.groups()) {
            checkCancelled();
            bytes = Math.addExact(bytes, 128 + referenceArrayBytes(group.referencedStages().size()));
        }
        if (maxGapNanos != null) {
            bytes += longArrayBytes(stages);
        }
        reserve(bytes);
        try {
            this.config = config;
            this.maxGapNanos = maxGapNanos == null ? null : maxGapNanos.clone();
            keys = new HashMap<>(64);
            expiry = new TreeSet<>(RECENCY);
            unusedStarts = new TreeSet<>(RECENCY);
            selected = new Row[stages - 1];
            positions = new int[stages - 1];
            view = new ValuesView();
        } catch (RuntimeException | Error e) {
            try {
                close();
            } catch (RuntimeException | Error cleanup) {
                if (cleanup != e) {
                    e.addSuppressed(cleanup);
                }
            }
            throw e;
        }
    }

    @Override
    public SequenceMatch match(int stage, BytesRef key, SequenceOrdinal ordinal, BytesRef handle, SequenceCandidateValues values) {
        boolean retainedValues = false;
        try {
            ensureOpen();
            checkCancelled();
            if (stage < 0 || stage >= stages) {
                throw new IllegalArgumentException("invalid sequence stage [" + stage + "]");
            }
            Objects.requireNonNull(key);
            Objects.requireNonNull(ordinal);
            Objects.requireNonNull(handle);
            validateValues(stage, values);
            checkOrder(stage, key, ordinal);
            long rank = nextRank;
            nextRank = Math.incrementExact(nextRank);
            prune(ordinal);
            KeyState state = keys.get(key);
            if (stage < stages - 1) {
                boolean found = false;
                if (stage > 0) {
                    terminalStage = stage;
                    finalOrdinal = ordinal;
                    finalValues = values;
                    arrivalSearch = true;
                    found = state != null && search(state, stage);
                    if (found == false && searchAbandoned == false) {
                        candidatesNotKept = Math.incrementExact(candidatesNotKept);
                        return null;
                    }
                }
                insert(state, stage, key, ordinal, rank, handle, values, found);
                retainedValues = true;
                return null;
            }
            // Even an empty start ring requires the prescribed later-stage visits when this search is invoked.
            if (state == null) {
                return null;
            }
            terminalStage = stage;
            arrivalSearch = false;
            finalOrdinal = ordinal;
            finalValues = values;
            if (search(state, stage) == false) {
                return null;
            }
            findDirectTargets(state);
            SequenceMatch output = complete(state.key, ordinal, handle);
            try {
                commitUsed(state);
                return output;
            } catch (RuntimeException | Error e) {
                try {
                    output.close();
                } catch (RuntimeException | Error cleanup) {
                    if (cleanup != e) {
                        e.addSuppressed(cleanup);
                    }
                }
                throw e;
            }
        } catch (RuntimeException | Error e) {
            try {
                close();
            } catch (RuntimeException | Error cleanup) {
                if (cleanup != e) {
                    e.addSuppressed(cleanup);
                }
            }
            throw e;
        } finally {
            finalOrdinal = null;
            finalValues = null;
            currentGroup = null;
            arrivalSearch = false;
            searchAbandoned = false;
            if (selected != null) {
                Arrays.fill(selected, null);
            }
            if (retainedValues == false && values != null) {
                values.close();
            }
        }
    }

    private void validateValues(int stage, SequenceCandidateValues values) {
        var demands = config.mandatoryDemands().get(stage);
        if (demands.isEmpty() && values == null) {
            return;
        }
        if (values == null || values.size() != demands.size()) {
            throw new IllegalArgumentException("missing mandatory sequence values for stage [" + (stage + 1) + "]");
        }
        for (int slot = 0; slot < demands.size(); slot++) {
            checkCancelled();
            Block block = values.block(slot);
            ElementType expected = PlannerUtils.toElementType(demands.get(slot).attribute().dataType());
            if (block.getPositionCount() != 1 || (block.elementType() != expected && block.elementType() != ElementType.NULL)) {
                throw new IllegalArgumentException("invalid mandatory sequence value type for stage [" + (stage + 1) + "]");
            }
        }
    }

    private void checkOrder(int stage, BytesRef key, SequenceOrdinal ordinal) {
        if (previousOrdinal != null) {
            int order = previousOrdinal.compareTo(ordinal);
            if (order > 0 || (order == 0 && stage > previousStage)) {
                throw new IllegalArgumentException("sequence candidates must be ordered by ordinal then descending stage");
            }
            if (order == 0 && stage == previousStage && keyOrder.compare(previousKey, key) > 0) {
                throw new IllegalArgumentException("equal-ordinal stage candidates must be ordered by key");
            }
        }
        reserve(bytesRefBytes(key));
        BytesRef copy = BytesRef.deepCopyOf(key);
        if (previousKey != null) {
            release(bytesRefBytes(previousKey));
        }
        previousOrdinal = ordinal;
        previousStage = stage;
        previousKey = copy;
    }

    /** Admission owns a flat earlier chain; only listed rows hold those references. */
    private void insert(
        KeyState state,
        int stage,
        BytesRef key,
        SequenceOrdinal ordinal,
        long rank,
        BytesRef handle,
        SequenceCandidateValues values,
        boolean found
    ) {
        if (state == null) {
            state = addKey(key);
        }
        Ring ring = state.rings[stage];
        if (ring == null) {
            long bytes = RamUsageEstimator.shallowSizeOfInstance(Ring.class);
            reserve(bytes);
            ring = new Ring();
            state.rings[stage] = ring;
            state.bytes += bytes;
        }
        // The extra temporary list cell is charged and trimmed before sampling the kept peak.
        ensureRememberedCapacity(state, ring, true, ring.size + 1);
        ensureRememberedCapacity(state, ring, false, ring.keptSize + 1);
        SequencePayloadHandle payload = SequencePayloadHandle.copyOf(handle, breaker);
        boolean published = false;
        try {
            long bytes = ROW_BYTES + (stage == 0 ? TREE_ENTRY_BYTES : 0);
            reserve(bytes);
            DocState document = addDocument(state, ordinal);
            Row row = new Row(state, document, stage, ordinal, rank, payload, values, bytes);
            state.records++;
            if (document != null) {
                document.records++;
            }
            int position = ring.size;
            while (position > 0 && RECENCY.compare(ring.rows[position - 1], row) > 0) {
                position--;
            }
            System.arraycopy(ring.rows, position, ring.rows, position + 1, ring.size - position);
            ring.rows[position] = row;
            ring.size++;
            row.listed = true;
            position = ring.keptSize;
            while (position > 0 && RECENCY.compare(ring.kept[position - 1], row) > 0) {
                position--;
            }
            System.arraycopy(ring.kept, position, ring.kept, position + 1, ring.keptSize - position);
            ring.kept[position] = row;
            ring.keptSize++;
            published = true;
            expiry.add(row);
            if (stage == 0) {
                unusedStarts.add(row);
            }
            // Publish the new holders before releasing the evicted row's chain.
            if (found) {
                long chainBytes = referenceArrayBytes(stage);
                reserve(chainBytes);
                row.remembered = Arrays.copyOf(selected, stage);
                row.bytes += chainBytes;
                for (Row ancestor : row.remembered) {
                    ancestor.holders++;
                }
            }
            if (ring.size > bound) {
                unlist(ring.rows[0], true);
            }
            largestRingFill = Math.max(largestRingFill, ring.size);
            peakCandidatesKeptPerKey = Math.max(peakCandidatesKeptPerKey, state.records);
        } finally {
            if (published == false) {
                payload.decRef();
            }
        }
    }

    private void ensureRememberedCapacity(KeyState state, Ring ring, boolean listed, int needed) {
        Row[] current = listed ? ring.rows : ring.kept;
        if (needed > current.length) {
            int capacity = Math.max(needed, Math.max(4, Math.multiplyExact(current.length, 2)));
            long bytes = referenceArrayBytes(capacity);
            long previous = current.length == 0 ? 0 : referenceArrayBytes(current.length);
            reserve(bytes);
            Row[] next = Arrays.copyOf(current, capacity);
            if (listed) {
                ring.rows = next;
            } else {
                ring.kept = next;
            }
            state.bytes += bytes - previous;
            release(previous);
        }
    }

    private void unlist(Row row, boolean atBound) {
        Ring ring = row.state.rings[row.stage];
        int position = 0;
        while (ring.rows[position] != row) {
            position++;
        }
        System.arraycopy(ring.rows, position + 1, ring.rows, position, ring.size - position - 1);
        ring.rows[--ring.size] = null;
        row.listed = false;
        releaseRemembered(row, atBound);
        if (row.holders == 0 && row.destroyed == false) {
            destroyRemembered(row, atBound);
        }
    }

    private void releaseRemembered(Row row, boolean atBound) {
        Row[] chain = row.remembered;
        row.remembered = null;
        if (chain != null) {
            long bytes = referenceArrayBytes(chain.length);
            row.bytes -= bytes;
            release(bytes);
            for (Row ancestor : chain) {
                if (ancestor != null && --ancestor.holders == 0 && ancestor.listed == false && ancestor.destroyed == false) {
                    destroyRemembered(ancestor, atBound);
                }
            }
        }
    }

    private void expireRemembered(Row row) {
        // Expired references cannot keep row metadata alive after its values leave the window.
        KeyState state = row.state;
        for (Ring ring : state.rings) {
            for (int position = 0; position < size(ring); position++) {
                Row holder = ring.rows[position];
                if (holder.remembered != null && holder.stage > row.stage && holder.remembered[row.stage] == row) {
                    holder.remembered[row.stage] = null;
                    row.holders--;
                }
            }
        }
        if (row.listed) {
            unlist(row, false);
        } else {
            destroyRemembered(row, false);
        }
    }

    private void destroyRemembered(Row row, boolean atBound) {
        assert row.listed == false && row.holders == 0 && row.remembered == null && row.destroyed == false;
        row.destroyed = true;
        expiry.remove(row);
        if (row.stage == 0 && row.used == false) {
            unusedStarts.remove(row);
        }
        KeyState state = row.state;
        Ring ring = state.rings[row.stage];
        int position = 0;
        while (ring.kept[position] != row) {
            position++;
        }
        System.arraycopy(ring.kept, position + 1, ring.kept, position, ring.keptSize - position - 1);
        ring.kept[--ring.keptSize] = null;
        if (atBound) {
            recordBoundDrop(state);
        }
        try {
            row.releaseValues();
        } finally {
            release(row.bytes);
            if (row.document != null && --row.document.records == 0) {
                state.documents.remove(row.document.ordinal);
                release(row.document.bytes);
            }
            if (--state.records == 0) {
                keys.remove(state.key);
                release(state.bytes);
            }
        }
    }

    private void recordBoundDrop(KeyState state) {
        boundDrops = Math.incrementExact(boundDrops);
        if (state.hitBound == false) {
            state.hitBound = true;
            keysThatHitBound = Math.incrementExact(keysThatHitBound);
        }
    }

    private KeyState addKey(BytesRef key) {
        long bytes = RamUsageEstimator.shallowSizeOfInstance(KeyState.class) + HASH_ENTRY_BYTES + bytesRefBytes(key) + referenceArrayBytes(
            stages - 1
        ) + (stages == 2 ? 0 : RamUsageEstimator.shallowSizeOfInstance(HashMap.class));
        reserve(bytes);
        KeyState state = new KeyState(BytesRef.deepCopyOf(key), new Ring[stages - 1], bytes, stages > 2);
        if (keyCapacity == 0 || keys.size() + 1 > keyCapacity * 3L / 4) {
            int next = keyCapacity == 0 ? 64 : Math.multiplyExact(keyCapacity, 2);
            long nextBytes = referenceArrayBytes(next);
            reserve(nextBytes);
            keys.put(state.key, state);
            release(keyTableBytes);
            keyTableBytes = nextBytes;
            keyCapacity = next;
        } else {
            keys.put(state.key, state);
        }
        return state;
    }

    private DocState addDocument(KeyState state, SequenceOrdinal ordinal) {
        if (state.documents == null) {
            // Two stages retain only stage zero, so document cross-marks can never affect another retained stage.
            return null;
        }
        DocState document = state.documents.get(ordinal);
        if (document != null) {
            return document;
        }
        long bytes = RamUsageEstimator.shallowSizeOfInstance(DocState.class) + HASH_ENTRY_BYTES + ORDINAL_BYTES;
        reserve(bytes);
        document = new DocState(ordinal, bytes);
        if (state.documentCapacity == 0 || state.documents.size() + 1 > state.documentCapacity * 3L / 4) {
            int next = state.documentCapacity == 0 ? 64 : Math.multiplyExact(state.documentCapacity, 2);
            long nextBytes = referenceArrayBytes(next);
            reserve(nextBytes);
            state.documents.put(ordinal, document);
            release(state.documentTableBytes);
            state.bytes += nextBytes - state.documentTableBytes;
            state.documentTableBytes = nextBytes;
            state.documentCapacity = next;
        } else {
            state.documents.put(ordinal, document);
        }
        return document;
    }

    private void prune(SequenceOrdinal watermark) {
        while (expiry.isEmpty() == false && watermark.exceedsSpan(expiry.first().ordinal, maxSpanNanos)) {
            checkCancelled();
            expireRemembered(expiry.first());
            expiredCandidates = Math.incrementExact(expiredCandidates);
        }
    }

    /**
     * Search exactly the earlier stages of this terminal row, preserving traversal and charged-visit order.
     * Arrival abandonment is distinct from a miss because it must admit the row without a remembered chain.
     */
    private boolean search(KeyState state, int terminal) {
        long steps = 0;
        searchAbandoned = false;
        long limit = arrivalSearch ? config.arrivalStepLimit() : searchStepLimit;
        try {
            int stage = terminal - 1;
            positions[stage] = poolSize(state.rings[stage]) - 1;
            while (stage < terminal) {
                // Checking every loop also bounds cancellation during scans consisting only of used markers.
                checkCancelled();
                if (positions[stage] < 0) {
                    selected[stage] = null;
                    stage++;
                    continue;
                }
                Row row = poolRow(state.rings[stage], positions[stage]--);
                if (row.used) {
                    continue;
                }
                if (steps == limit) {
                    // Exhaustion at the budget is a miss. Only another unused candidate requires abandonment.
                    abandonedSearches = Math.incrementExact(abandonedSearches);
                    searchAbandoned = true;
                    if (arrivalSearch) {
                        arrivalSearchesAbandoned = Math.incrementExact(arrivalSearchesAbandoned);
                    }
                    return false;
                }
                steps++;
                if (arrivalSearch) {
                    arrivalSearchSteps = Math.incrementExact(arrivalSearchSteps);
                } else {
                    searchSteps = Math.incrementExact(searchSteps);
                }
                selected[stage] = row;
                if (validAt(stage) == false) {
                    selected[stage] = null;
                    continue;
                }
                if (stage == 0) {
                    return true;
                }
                stage--;
                positions[stage] = poolSize(state.rings[stage]) - 1;
            }
            return false;
        } finally {
            if (arrivalSearch == false) {
                maxSearchStepsPerFinalRow = Math.max(maxSearchStepsPerFinalRow, steps);
            }
        }
    }

    private boolean validAt(int stage) {
        SequenceOrdinal ordinal = selected[stage].ordinal;
        SequenceOrdinal next = ordinalAt(stage + 1);
        if (ordinal.compareTo(next) >= 0 || exceedsGap(next, ordinal, stage + 1)) {
            return false;
        }
        if (stage == 0 && finalOrdinal.exceedsSpan(ordinal, maxSpanNanos)) {
            return false;
        }
        for (var group : config.groups()) {
            checkCancelled();
            if (group.ownStage() <= terminalStage && group.minReferencedStage() == stage && evaluate(group) == false) {
                return false;
            }
        }
        return true;
    }

    private boolean exceedsGap(SequenceOrdinal later, SequenceOrdinal earlier, int laterStage) {
        return maxGapNanos != null && maxGapNanos[laterStage] > 0 && later.exceedsSpan(earlier, maxGapNanos[laterStage]);
    }

    private SequenceOrdinal ordinalAt(int stage) {
        return stage == terminalStage ? finalOrdinal : selected[stage].ordinal;
    }

    private boolean evaluate(SequenceLatestValidConfig.PredicateGroup group) {
        currentGroup = group;
        try {
            conditionEvaluations = Math.incrementExact(conditionEvaluations);
            if (arrivalSearch) {
                conditionEvaluationsOnArrival = Math.incrementExact(conditionEvaluationsOnArrival);
            }
            return group.predicate().test(view);
        } finally {
            currentGroup = null;
        }
    }

    private void findDirectTargets(KeyState state) {
        generation = Math.incrementExact(generation);
        for (int stage = 0; stage < stages - 1; stage++) {
            checkCancelled();
            Row emitted = selected[stage];
            directTarget(emitted);
            Ring ring = state.rings[stage];
            for (int position = 0; position < poolSize(ring); position++) {
                checkCancelled();
                Row row = poolRow(ring, position);
                if (row.used || row == emitted) {
                    continue;
                }
                selected[stage] = row;
                try {
                    if (substitutionValid(stage)) {
                        directTarget(row);
                    }
                } finally {
                    selected[stage] = emitted;
                }
            }
        }
    }

    private boolean substitutionValid(int stage) {
        SequenceOrdinal ordinal = selected[stage].ordinal;
        SequenceOrdinal next = ordinalAt(stage + 1);
        if (ordinal.compareTo(next) >= 0 || (stage > 0 && ordinal.compareTo(ordinalAt(stage - 1)) <= 0)) {
            return false;
        }
        if (exceedsGap(next, ordinal, stage + 1)
            || (stage > 0 && exceedsGap(ordinal, ordinalAt(stage - 1), stage))
            || (stage == 0 && finalOrdinal.exceedsSpan(ordinal, maxSpanNanos))) {
            return false;
        }
        for (var group : config.groups()) {
            checkCancelled();
            if ((group.ownStage() == stage || group.referencedStages().contains(stage)) && evaluate(group) == false) {
                return false;
            }
        }
        return true;
    }

    private void directTarget(Row row) {
        row.directGeneration = generation;
        DocState document = row.document;
        if (document == null) {
            return;
        }
        if (document.targetGeneration != generation) {
            document.targetGeneration = generation;
            document.originStage = row.stage;
        } else if (document.originStage != row.stage) {
            document.originStage = -1;
        }
    }

    /** Cross-marks are consequences of direct targets only; propagating them again would consume sibling rows. */
    private void commitUsed(KeyState state) {
        for (Ring ring : state.rings) {
            checkCancelled();
            for (int position = 0; position < poolSize(ring); position++) {
                checkCancelled();
                Row row = poolRow(ring, position);
                if (row.used == false
                    && (row.directGeneration == generation
                        || (row.document != null
                            && row.document.targetGeneration == generation
                            && row.document.originStage != row.stage))) {
                    row.used = true;
                    if (row.stage == 0) {
                        unusedStarts.remove(row);
                        row.bytes -= TREE_ENTRY_BYTES;
                        release(TREE_ENTRY_BYTES);
                    }
                    row.releaseValues();
                }
            }
        }
    }

    private SequenceMatch complete(BytesRef key, SequenceOrdinal end, BytesRef finalHandle) {
        long bytes = RamUsageEstimator.shallowSizeOfInstance(CompletedMatch.class) + 2 * referenceArrayBytes(stages) + bytesRefBytes(key)
            + 2 * ORDINAL_BYTES + 64;
        breaker.addEstimateBytesAndMaybeBreak(bytes, LABEL);
        SequencePayloadHandle[] handles = null;
        try {
            handles = new SequencePayloadHandle[stages];
            BytesRef[] borrowed = new BytesRef[stages];
            for (int stage = 0; stage < stages - 1; stage++) {
                checkCancelled();
                SequencePayloadHandle payload = selected[stage].payload;
                payload.incRef();
                handles[stage] = payload;
                borrowed[stage] = payload.bytes();
            }
            handles[stages - 1] = SequencePayloadHandle.copyOf(finalHandle, breaker);
            borrowed[stages - 1] = handles[stages - 1].bytes();
            CompletedMatch output = new CompletedMatch(
                breaker,
                bytes,
                BytesRef.deepCopyOf(key),
                selected[0].ordinal,
                end,
                handles,
                borrowed
            );
            return output;
        } catch (RuntimeException | Error e) {
            try {
                releaseHandles(handles);
            } catch (RuntimeException | Error cleanup) {
                if (cleanup != e) {
                    e.addSuppressed(cleanup);
                }
            }
            try {
                breaker.addWithoutBreaking(-bytes, LABEL);
            } catch (RuntimeException | Error cleanup) {
                if (cleanup != e) {
                    e.addSuppressed(cleanup);
                }
            }
            throw e;
        }
    }

    @Override
    public int retainedCandidates() {
        return expiry == null ? 0 : expiry.size();
    }

    @Override
    public int retainedKeys() {
        return keys == null ? 0 : keys.size();
    }

    @Override
    public boolean possibleMatch() {
        return unusedStarts != null && unusedStarts.isEmpty() == false;
    }

    @Override
    public SequenceOrdinal latestUnusedStart() {
        return possibleMatch() ? unusedStarts.last().ordinal : null;
    }

    /** Used markers cannot contribute keys; the source monitor keeps these borrowed identities alive during the visit. */
    @Override
    public boolean visitUnusedStartKeys(Predicate<BytesRef> visitor) {
        if (unusedStarts != null) {
            for (Row row : unusedStarts) {
                checkCancelled();
                if (visitor.test(row.state.key) == false) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public SequenceSourceStatus.LatestValidCounters latestValidCounters() {
        return new SequenceSourceStatus.LatestValidCounters(
            boundDrops,
            abandonedSearches,
            keysThatHitBound,
            searchSteps,
            maxSearchStepsPerFinalRow,
            conditionEvaluations,
            largestRingFill
        );
    }

    long boundDrops() {
        return boundDrops;
    }

    long expiredCandidates() {
        return expiredCandidates;
    }

    int largestRingFill() {
        return largestRingFill;
    }

    /** Detached, caller-owned heap observations for tests only; never called on a query execution path. */
    List<RowState> snapshotForTests() {
        ensureOpen();
        List<RowState> rows = new ArrayList<>(expiry.size());
        for (Row row : expiry) {
            rows.add(new RowState(BytesRef.deepCopyOf(row.state.key), row.stage, row.ordinal, row.rank, row.used));
        }
        rows.sort(Comparator.comparingLong(RowState::arrivalRank));
        return List.copyOf(rows);
    }

    /** Identity/marker metadata only: observations never keep payload bytes or predicate blocks alive. */
    record RowState(BytesRef key, int stage, SequenceOrdinal ordinal, long arrivalRank, boolean used) {}

    private void reserve(long bytes) {
        breaker.addEstimateBytesAndMaybeBreak(bytes, LABEL);
        heldBytes += bytes;
    }

    private void release(long bytes) {
        heldBytes -= bytes;
        breaker.addWithoutBreaking(-bytes, LABEL);
    }

    private static int poolSize(Ring ring) {
        return ring == null ? 0 : ring.keptSize;
    }

    private static Row poolRow(Ring ring, int position) {
        return ring.kept[position];
    }

    /** Matcher-local measurements. The kept peak is sampled after list trimming. */
    record AdmissionCounters(
        long candidatesNotKept,
        long arrivalSearchesAbandoned,
        long arrivalSearchSteps,
        long conditionEvaluationsOnArrival,
        long peakCandidatesKeptPerKey
    ) {}

    AdmissionCounters admissionCounters() {
        return new AdmissionCounters(
            candidatesNotKept,
            arrivalSearchesAbandoned,
            arrivalSearchSteps,
            conditionEvaluationsOnArrival,
            peakCandidatesKeptPerKey
        );
    }

    private static int size(Ring ring) {
        return ring == null ? 0 : ring.size;
    }

    private static long bytesRefBytes(BytesRef value) {
        return RamUsageEstimator.shallowSizeOfInstance(BytesRef.class) + RamUsageEstimator.alignObjectSize(
            RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) value.length
        );
    }

    private static long referenceArrayBytes(int size) {
        return RamUsageEstimator.alignObjectSize(
            RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) size * RamUsageEstimator.NUM_BYTES_OBJECT_REF
        );
    }

    private static long intArrayBytes(int size) {
        return RamUsageEstimator.alignObjectSize(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) size * Integer.BYTES);
    }

    private static long longArrayBytes(int size) {
        return RamUsageEstimator.alignObjectSize(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) size * Long.BYTES);
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

    @Override
    public void close() {
        if (closed == false) {
            closed = true;
            Throwable failure = null;
            try {
                if (keys != null) {
                    for (KeyState state : keys.values()) {
                        for (Ring ring : state.rings) {
                            for (int position = 0; position < poolSize(ring); position++) {
                                try {
                                    poolRow(ring, position).releaseValues();
                                } catch (RuntimeException | Error e) {
                                    if (failure == null) {
                                        failure = e;
                                    } else {
                                        failure.addSuppressed(e);
                                    }
                                }
                            }
                        }
                    }
                }
            } finally {
                keys = null;
                expiry = null;
                unusedStarts = null;
                selected = null;
                positions = null;
                view = null;
                config = null;
                maxGapNanos = null;
                previousOrdinal = null;
                previousKey = null;
                release(heldBytes);
            }
            if (failure instanceof RuntimeException runtime) {
                throw runtime;
            }
            if (failure instanceof Error error) {
                throw error;
            }
        }
    }

    private final class ValuesView implements SequenceLatestValidConfig.SelectedValues {
        @Override
        public Block block(int stage, int demandSlot) {
            if (currentGroup == null || (stage != currentGroup.ownStage() && currentGroup.referencedStages().contains(stage) == false)) {
                throw new IllegalArgumentException("sequence predicate accessed an undeclared stage");
            }
            SequenceCandidateValues values = stage == terminalStage ? finalValues : selected[stage].values;
            if (values == null) {
                throw new IllegalArgumentException("sequence predicate accessed a missing mandatory value");
            }
            return values.block(demandSlot);
        }
    }

    private static final class KeyState {
        private final BytesRef key;
        private final Ring[] rings;
        private final Map<SequenceOrdinal, DocState> documents;
        private long bytes;
        private long documentTableBytes;
        private int documentCapacity;
        private int records;
        private boolean hitBound;

        private KeyState(BytesRef key, Ring[] rings, long bytes, boolean sharedDocuments) {
            this.key = key;
            this.rings = rings;
            this.bytes = bytes;
            documents = sharedDocuments ? new HashMap<>(64) : null;
        }
    }

    private static final class Ring {
        private Row[] rows = EMPTY_ROWS;
        private int size;
        private Row[] kept = EMPTY_ROWS;
        private int keptSize;
    }

    private static final class DocState {
        private final SequenceOrdinal ordinal;
        private final long bytes;
        private int records;
        private long targetGeneration;
        private int originStage;

        private DocState(SequenceOrdinal ordinal, long bytes) {
            this.ordinal = ordinal;
            this.bytes = bytes;
        }
    }

    private static final class Row {
        private final KeyState state;
        private final DocState document;
        private final int stage;
        private final SequenceOrdinal ordinal;
        private final long rank;
        private SequencePayloadHandle payload;
        private SequenceCandidateValues values;
        private long bytes;
        private long directGeneration;
        private boolean used;
        private boolean listed;
        private boolean destroyed;
        private int holders;
        private Row[] remembered;

        private Row(
            KeyState state,
            DocState document,
            int stage,
            SequenceOrdinal ordinal,
            long rank,
            SequencePayloadHandle payload,
            SequenceCandidateValues values,
            long bytes
        ) {
            this.state = state;
            this.document = document;
            this.stage = stage;
            this.ordinal = ordinal;
            this.rank = rank;
            this.payload = payload;
            this.values = values;
            this.bytes = bytes;
        }

        private void releaseValues() {
            SequenceCandidateValues ownedValues = values;
            SequencePayloadHandle ownedPayload = payload;
            values = null;
            payload = null;
            try {
                if (ownedValues != null) {
                    ownedValues.close();
                }
            } finally {
                if (ownedPayload != null) {
                    ownedPayload.decRef();
                }
            }
        }
    }

    private static void releaseHandles(SequencePayloadHandle[] handles) {
        Throwable failure = null;
        if (handles != null) {
            for (SequencePayloadHandle handle : handles) {
                if (handle != null) {
                    try {
                        handle.decRef();
                    } catch (RuntimeException | Error e) {
                        if (failure == null) {
                            failure = e;
                        } else {
                            failure.addSuppressed(e);
                        }
                    }
                }
            }
        }
        if (failure instanceof RuntimeException runtime) {
            throw runtime;
        }
        if (failure instanceof Error error) {
            throw error;
        }
    }

    /** Reference ownership, rather than ring lifetime, protects deferred payload reads. */
    private static final class CompletedMatch implements SequenceMatch {
        private final CircuitBreaker breaker;
        private final long bytes;
        private final SequenceOrdinal start;
        private final SequenceOrdinal end;
        private BytesRef key;
        private SequencePayloadHandle[] owners;
        private List<BytesRef> handles;

        private CompletedMatch(
            CircuitBreaker breaker,
            long bytes,
            BytesRef key,
            SequenceOrdinal start,
            SequenceOrdinal end,
            SequencePayloadHandle[] owners,
            BytesRef[] handles
        ) {
            this.breaker = breaker;
            this.bytes = bytes;
            this.key = key;
            this.start = start;
            this.end = end;
            this.owners = owners;
            this.handles = Collections.unmodifiableList(Arrays.asList(handles));
        }

        @Override
        public synchronized BytesRef key() {
            ensureOpen();
            return key;
        }

        @Override
        public SequenceOrdinal start() {
            return start;
        }

        @Override
        public SequenceOrdinal end() {
            return end;
        }

        @Override
        public synchronized List<BytesRef> handles() {
            ensureOpen();
            return handles;
        }

        private void ensureOpen() {
            if (owners == null) {
                throw new IllegalStateException("sequence match is closed");
            }
        }

        @Override
        public synchronized void close() {
            if (owners != null) {
                SequencePayloadHandle[] owned = owners;
                owners = null;
                key = null;
                handles = null;
                try {
                    releaseHandles(owned);
                } finally {
                    breaker.addWithoutBreaking(-bytes, LABEL);
                }
            }
        }
    }
}
