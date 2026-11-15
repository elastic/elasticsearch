/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A test-only port of the pinned row procedure in {@code rows_S.py}, with the unused-visit boundary of
 * {@code worklimit_S.py}. It has its own input types and state and uses no production or declarative matching helpers.
 * Qualification against the pinned external models must precede its use as a bounded engine oracle.
 */
final class SequenceRowProceduralOracle {
    /** Documents share both ordinal components; timestamps alone do not identify a document. */
    record Ordinal(long timestampNanos, long shardDoc) implements Comparable<Ordinal> {
        /** Preserve the PIT physical-position tie break when timestamps are equal. */
        @Override
        public int compareTo(Ordinal other) {
            int time = Long.compare(timestampNanos, other.timestampNanos);
            return time == 0 ? Long.compare(shardDoc, other.shardDoc) : time;
        }
    }

    /** Hexadecimal bytes provide the input's unsigned key order without a production key codec. */
    record Key(String hex) implements Comparable<Key> {
        Key {
            hex = HexFormat.of().formatHex(HexFormat.of().parseHex(Objects.requireNonNull(hex)));
        }

        static Key utf8(String value) {
            return new Key(HexFormat.of().formatHex(value.getBytes(StandardCharsets.UTF_8)));
        }

        /** Equal-width hexadecimal digits retain unsigned byte ordering independently of the production codec. */
        @Override
        public int compareTo(Key other) {
            return hex.compareTo(other.hex);
        }
    }

    /** An immutable candidate row keeps its identity separate from its scalar test value and display label. */
    record Row(String id, Ordinal ordinal, int stage, Key key, long arrivalRank, Object value, Object label) {
        Row {
            Objects.requireNonNull(id);
            Objects.requireNonNull(ordinal);
            Objects.requireNonNull(key);
            if (stage < 1 || arrivalRank < 0) {
                throw new IllegalArgumentException("stage must be positive and arrival rank must be nonnegative");
            }
            value = immutableLiteral(value);
            label = immutableLiteral(label);
        }

        Row(String id, Ordinal ordinal, int stage, Key key, long arrivalRank, Object value) {
            this(id, ordinal, stage, key, arrivalRank, value, id);
        }
    }

    /** Only the declared earlier references are exposed to a conjunct, including during substitution. */
    @FunctionalInterface
    interface Predicate {
        /** A condition passes only when the returned value is a single non-null Boolean true. */
        Object evaluate(Row own, Map<Integer, Row> earlier);
    }

    /** The original reference set determines when this single conjunct becomes fully bound. */
    record Condition(int stage, Set<Integer> referencedStages, Predicate predicate) {
        Condition {
            referencedStages = Collections.unmodifiableSet(new LinkedHashSet<>(referencedStages));
            Objects.requireNonNull(predicate);
            if (referencedStages.isEmpty() || referencedStages.stream().anyMatch(reference -> reference < 1 || reference >= stage)) {
                throw new IllegalArgumentException("a conjunct must reference earlier stages");
            }
        }
    }

    /** Null windows and missing link limits are unrestricted only in this plain-data model. */
    record Query(int stages, Long windowNanos, Map<Integer, Long> linkLimitsNanos, List<Condition> conditions) {
        Query {
            if (stages < 2 || windowNanos != null && windowNanos < 0) {
                throw new IllegalArgumentException("invalid stage count or window");
            }
            linkLimitsNanos = Map.copyOf(linkLimitsNanos);
            conditions = List.copyOf(conditions);
            for (var limit : linkLimitsNanos.entrySet()) {
                if (limit.getKey() < 2 || limit.getKey() > stages || limit.getValue() < 0) {
                    throw new IllegalArgumentException("invalid link limit");
                }
            }
            if (conditions.stream().anyMatch(condition -> condition.stage() > stages)) {
                throw new IllegalArgumentException("condition stage exceeds the query's stages");
            }
        }
    }

    /** Retained used markers and unused candidates are reported independently of historical marks. */
    record Snapshot(Row after, Set<String> retainedIds, Set<String> usedIds, Set<String> unusedIds, Row latestUnusedStart) {}

    /** Before/after snapshots make the absence of consumption on abandonment directly observable. */
    record FinalSearch(
        Row finalRow,
        long steps,
        boolean abandoned,
        List<String> match,
        Snapshot before,
        Snapshot after,
        Set<String> emittedIds,
        Set<String> substituteIds,
        Set<String> crossMarkedIds
    ) {}

    /** Ordered matches and procedural counters can be compared using input identities alone. */
    record Result(
        List<List<String>> matches,
        List<Row> processingOrder,
        List<FinalSearch> finalSearches,
        long abandonedSearches,
        long totalSteps,
        long maxSteps,
        Map<String, Snapshot> snapshots,
        List<String> droppedIds,
        List<String> expiredIds,
        int largestRingFill,
        long keysThatHitBound
    ) {}

    private record Domain(Ordinal ordinal, int stage, Key key, long arrivalRank) {}

    private record GroupKey(int ownStage, int earliestReference) {}

    private record Group(GroupKey key, List<Condition> conjuncts) {}

    private enum Outcome {
        FOUND,
        EXHAUSTED,
        ABANDONED
    }

    private static final class KeyState {
        private final Map<Integer, List<Row>> rings = new LinkedHashMap<>();
        private final Set<String> used = new LinkedHashSet<>();
        private boolean hitBound;

        private KeyState(int stages) {
            for (int stage = 1; stage < stages; stage++) {
                rings.put(stage, new ArrayList<>());
            }
        }
    }

    private static final Comparator<Row> PROCESSING_ORDER = Comparator.comparing(Row::ordinal)
        .thenComparing(Comparator.comparingInt(Row::stage).reversed())
        .thenComparing(Row::key)
        .thenComparingLong(Row::arrivalRank);
    private static final Comparator<Row> RECENCY_ORDER = Comparator.comparing(Row::ordinal)
        .thenComparing(Comparator.comparingLong(Row::arrivalRank).reversed());

    private SequenceRowProceduralOracle() {}

    /** Null bounds and budgets are available for external qualification of the same procedure without those limits. */
    static Result evaluate(Query query, List<Row> history, Integer bound, Long stepLimit) {
        return evaluate(query, history, bound, stepLimit, Set.of());
    }

    /**
     * Process every candidate, even when there is no unused start. A final's later-stage visits still count in that case.
     * Snapshots are taken after eager expiry and before any later incoming row.
     */
    static Result evaluate(Query query, List<Row> history, Integer bound, Long stepLimit, Set<String> snapshotAfterIds) {
        if (bound != null && bound < 1 || stepLimit != null && stepLimit < 0) {
            throw new IllegalArgumentException("bound must be positive and budget nonnegative, or null");
        }
        List<Row> ordered = history.stream().sorted(PROCESSING_ORDER).toList();
        validateRows(query, ordered);
        Map<Integer, List<Group>> groups = groups(query);
        Map<Key, KeyState> states = new LinkedHashMap<>();
        List<List<String>> matches = new ArrayList<>();
        List<FinalSearch> finals = new ArrayList<>();
        Map<String, Snapshot> snapshots = new LinkedHashMap<>();
        List<String> dropped = new ArrayList<>();
        List<String> expired = new ArrayList<>();
        int largestRingFill = 0;
        long keysThatHitBound = 0;
        long abandoned = 0;
        long totalSteps = 0;
        long maxSteps = 0;
        for (Row incoming : ordered) {
            expire(query, states, incoming.ordinal().timestampNanos(), expired);
            KeyState state = states.computeIfAbsent(incoming.key(), ignored -> new KeyState(query.stages()));
            if (incoming.stage() < query.stages()) {
                List<Row> ring = state.rings.get(incoming.stage());
                // A same-ordinal later arrival is older; linear insertion is intentionally independent of production storage.
                int position = 0;
                while (position < ring.size() && RECENCY_ORDER.compare(ring.get(position), incoming) <= 0) {
                    position++;
                }
                ring.add(position, incoming);
                if (bound != null && ring.size() > bound) {
                    dropped.add(ring.removeFirst().id());
                    if (state.hitBound == false) {
                        state.hitBound = true;
                        keysThatHitBound++;
                    }
                }
                largestRingFill = Math.max(largestRingFill, ring.size());
            } else {
                Snapshot before = snapshot(incoming, states);
                Search search = new Search(query, groups, state, incoming, stepLimit);
                Outcome outcome = search.visit(query.stages() - 1);
                totalSteps += search.steps;
                maxSteps = Math.max(maxSteps, search.steps);
                Set<String> emitted = new LinkedHashSet<>();
                Set<String> substitutes = new LinkedHashSet<>();
                Set<String> crossMarked = new LinkedHashSet<>();
                List<String> match = List.of();
                if (outcome == Outcome.ABANDONED) {
                    abandoned++;
                } else if (outcome == Outcome.FOUND) {
                    List<Row> full = new ArrayList<>();
                    for (int stage = 1; stage <= query.stages(); stage++) {
                        full.add(search.bound[stage]);
                    }
                    match = full.stream().map(Row::id).toList();
                    matches.add(match);
                    consume(query, state, full, emitted, substitutes, crossMarked);
                }
                finals.add(
                    new FinalSearch(
                        incoming,
                        search.steps,
                        outcome == Outcome.ABANDONED,
                        match,
                        before,
                        snapshot(incoming, states),
                        immutableSet(emitted),
                        immutableSet(substitutes),
                        immutableSet(crossMarked)
                    )
                );
            }
            if (snapshotAfterIds.contains(incoming.id())) {
                snapshots.put(incoming.id(), snapshot(incoming, states));
            }
        }
        return new Result(
            List.copyOf(matches),
            ordered,
            List.copyOf(finals),
            abandoned,
            totalSteps,
            maxSteps,
            Collections.unmodifiableMap(snapshots),
            List.copyOf(dropped),
            List.copyOf(expired),
            largestRingFill,
            keysThatHitBound
        );
    }

    private static final class Search {
        private final Query query;
        private final Map<Integer, List<Group>> groups;
        private final KeyState state;
        private final Row finalRow;
        private final Long limit;
        private final Row[] bound;
        private long steps;

        private Search(Query query, Map<Integer, List<Group>> groups, KeyState state, Row finalRow, Long limit) {
            this.query = query;
            this.groups = groups;
            this.state = state;
            this.finalRow = finalRow;
            this.limit = limit;
            bound = new Row[query.stages() + 1];
            bound[query.stages()] = finalRow;
        }

        private Outcome visit(int stage) {
            List<Row> ring = state.rings.get(stage);
            for (int position = ring.size() - 1; position >= 0; position--) {
                Row candidate = ring.get(position);
                if (state.used.contains(candidate.id())) {
                    continue;
                }
                // Exactly the pinned script's boundary: exhaustion alone at the budget is not abandonment.
                if (limit != null && steps >= limit) {
                    return Outcome.ABANDONED;
                }
                steps++;
                bound[stage] = candidate;
                if (passesAt(stage)) {
                    Outcome lower = stage == 1 ? Outcome.FOUND : visit(stage - 1);
                    if (lower != Outcome.EXHAUSTED) {
                        return lower;
                    }
                }
                bound[stage] = null;
            }
            return Outcome.EXHAUSTED;
        }

        private boolean passesAt(int stage) {
            Row candidate = bound[stage];
            Row next = bound[stage + 1];
            if (candidate.ordinal().compareTo(next.ordinal()) >= 0) {
                return false;
            }
            if (exceeds(next.ordinal().timestampNanos(), candidate.ordinal().timestampNanos(), query.linkLimitsNanos().get(stage + 1))) {
                return false;
            }
            if (stage == 1 && exceeds(finalRow.ordinal().timestampNanos(), candidate.ordinal().timestampNanos(), query.windowNanos())) {
                return false;
            }
            for (Group group : groups.getOrDefault(stage, List.of())) {
                for (Condition condition : group.conjuncts()) {
                    if (passes(condition, bound) == false) {
                        return false;
                    }
                }
            }
            return true;
        }
    }

    private static Map<Integer, List<Group>> groups(Query query) {
        Map<GroupKey, List<Condition>> grouped = new LinkedHashMap<>();
        for (Condition condition : query.conditions()) {
            int earliest = condition.referencedStages().stream().mapToInt(Integer::intValue).min().orElseThrow();
            grouped.computeIfAbsent(new GroupKey(condition.stage(), earliest), ignored -> new ArrayList<>()).add(condition);
        }
        Map<Integer, List<Group>> result = new LinkedHashMap<>();
        grouped.forEach(
            (key, conjuncts) -> result.computeIfAbsent(key.earliestReference(), ignored -> new ArrayList<>())
                .add(new Group(key, List.copyOf(conjuncts)))
        );
        return result;
    }

    private static boolean passes(Condition condition, Row[] bound) {
        Map<Integer, Row> references = new LinkedHashMap<>();
        for (int reference : condition.referencedStages()) {
            references.put(reference, Objects.requireNonNull(bound[reference], "a declared reference must already be bound"));
        }
        return Boolean.TRUE.equals(
            condition.predicate()
                .evaluate(
                    Objects.requireNonNull(bound[condition.stage()], "the own stage must already be bound"),
                    Collections.unmodifiableMap(references)
                )
        );
    }

    private static void consume(
        Query query,
        KeyState state,
        List<Row> full,
        Set<String> emitted,
        Set<String> substitutes,
        Set<String> crossMarked
    ) {
        Set<String> before = Set.copyOf(state.used);
        Map<String, Row> seeds = new LinkedHashMap<>();
        for (int stage = 1; stage < query.stages(); stage++) {
            Row member = full.get(stage - 1);
            emitted.add(member.id());
            seeds.put(member.id(), member);
            for (Row replacement : state.rings.get(stage)) {
                if (before.contains(replacement.id()) || full.contains(replacement)) {
                    continue;
                }
                List<Row> changed = new ArrayList<>(full);
                changed.set(stage - 1, replacement);
                if (completeChainPasses(query, changed, before)) {
                    substitutes.add(replacement.id());
                    seeds.put(replacement.id(), replacement);
                }
            }
        }
        // Frozen direct seeds propagate once, inside the same key and full document ordinal domain.
        // A newly cross-marked row never becomes another seed, so same-stage siblings remain independent.
        for (List<Row> ring : state.rings.values()) {
            for (Row candidate : ring) {
                for (Row seed : seeds.values()) {
                    if (candidate.ordinal().equals(seed.ordinal()) && candidate.stage() != seed.stage()) {
                        if (before.contains(candidate.id()) == false && seeds.containsKey(candidate.id()) == false) {
                            crossMarked.add(candidate.id());
                        }
                        break;
                    }
                }
            }
        }
        state.used.addAll(seeds.keySet());
        state.used.addAll(crossMarked);
    }

    private static boolean completeChainPasses(Query query, List<Row> full, Set<String> before) {
        for (int position = 0; position < full.size() - 1; position++) {
            Row earlier = full.get(position);
            Row later = full.get(position + 1);
            if (before.contains(earlier.id()) || earlier.ordinal().compareTo(later.ordinal()) >= 0) {
                return false;
            }
            if (exceeds(later.ordinal().timestampNanos(), earlier.ordinal().timestampNanos(), query.linkLimitsNanos().get(position + 2))) {
                return false;
            }
        }
        if (exceeds(full.getLast().ordinal().timestampNanos(), full.getFirst().ordinal().timestampNanos(), query.windowNanos())) {
            return false;
        }
        Row[] bound = new Row[query.stages() + 1];
        for (int stage = 1; stage <= query.stages(); stage++) {
            bound[stage] = full.get(stage - 1);
        }
        for (Condition condition : query.conditions()) {
            if (passes(condition, bound) == false) {
                return false;
            }
        }
        return true;
    }

    private static void expire(Query query, Map<Key, KeyState> states, long incomingTime, List<String> expired) {
        if (query.windowNanos() == null) {
            return;
        }
        for (Iterator<KeyState> iterator = states.values().iterator(); iterator.hasNext();) {
            KeyState state = iterator.next();
            for (List<Row> ring : state.rings.values()) {
                ring.removeIf(row -> {
                    if (exceeds(incomingTime, row.ordinal().timestampNanos(), query.windowNanos())) {
                        expired.add(row.id());
                        return true;
                    }
                    return false;
                });
            }
            if (state.rings.values().stream().allMatch(List::isEmpty)) {
                iterator.remove();
            }
        }
    }

    private static Snapshot snapshot(Row after, Map<Key, KeyState> states) {
        List<Row> retained = new ArrayList<>();
        Set<String> allUsed = new LinkedHashSet<>();
        for (KeyState state : states.values()) {
            state.rings.values().forEach(retained::addAll);
            allUsed.addAll(state.used);
        }
        retained.sort(PROCESSING_ORDER);
        Set<String> retainedIds = new LinkedHashSet<>();
        Set<String> used = new LinkedHashSet<>();
        Set<String> unused = new LinkedHashSet<>();
        Row start = null;
        for (Row row : retained) {
            retainedIds.add(row.id());
            if (allUsed.contains(row.id())) {
                used.add(row.id());
            } else {
                unused.add(row.id());
                if (row.stage() == 1 && (start == null || RECENCY_ORDER.compare(row, start) > 0)) {
                    start = row;
                }
            }
        }
        return new Snapshot(after, immutableSet(retainedIds), immutableSet(used), immutableSet(unused), start);
    }

    private static boolean exceeds(long later, long earlier, Long limit) {
        return limit != null && BigInteger.valueOf(later).subtract(BigInteger.valueOf(earlier)).compareTo(BigInteger.valueOf(limit)) > 0;
    }

    private static void validateRows(Query query, List<Row> ordered) {
        Set<String> ids = new LinkedHashSet<>();
        Set<Domain> identities = new LinkedHashSet<>();
        for (Row row : ordered) {
            if (row.stage() > query.stages() || ids.add(row.id()) == false) {
                throw new IllegalArgumentException("row stage is out of range or row id is duplicated");
            }
            if (identities.add(new Domain(row.ordinal(), row.stage(), row.key(), row.arrivalRank())) == false) {
                throw new IllegalArgumentException("duplicate ordinal/stage/key/arrival-rank row identity");
            }
        }
    }

    private static Object immutableLiteral(Object value) {
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> copy = new LinkedHashMap<>();
            for (var entry : map.entrySet()) {
                if (entry.getKey() instanceof String key) {
                    copy.put(key, immutableLiteral(entry.getValue()));
                } else {
                    throw new IllegalArgumentException("literal object keys must be strings");
                }
            }
            return Collections.unmodifiableMap(copy);
        }
        if (value instanceof List<?> list) {
            List<Object> copy = new ArrayList<>();
            list.forEach(item -> copy.add(immutableLiteral(item)));
            return Collections.unmodifiableList(copy);
        }
        if (value == null || value instanceof String || value instanceof Number || value instanceof Boolean) {
            return value;
        }
        throw new IllegalArgumentException("unsupported test literal value");
    }

    private static Set<String> immutableSet(Set<String> values) {
        return Collections.unmodifiableSet(new LinkedHashSet<>(values));
    }
}
