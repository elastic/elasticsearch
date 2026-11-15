/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * An independent row-history definition of latest-valid pairing, with no production matching helpers.
 * Complete forward Cartesian products are evaluated before an explicit reverse-stage lexicographic choice.
 */
final class SequenceRowDeclarativeOracle {
    /** Full source-document identity; a timestamp alone must never identify a document. */
    record Ordinal(long timestampNanos, long shardDoc) implements Comparable<Ordinal> {
        @Override
        public int compareTo(Ordinal other) {
            int time = Long.compare(timestampNanos, other.timestampNanos);
            return time == 0 ? Long.compare(shardDoc, other.shardDoc) : time;
        }
    }

    /** Canonical hexadecimal bytes provide unsigned key ordering without a production key codec. */
    record Key(String hex) implements Comparable<Key> {
        Key {
            hex = HexFormat.of().formatHex(HexFormat.of().parseHex(Objects.requireNonNull(hex)));
        }

        static Key utf8(String value) {
            return new Key(HexFormat.of().formatHex(value.getBytes(StandardCharsets.UTF_8)));
        }

        @Override
        public int compareTo(Key other) {
            return hex.compareTo(other.hex);
        }
    }

    /** A row owns its stage/key/rank identity while retaining the shared document ordinal. */
    record Row(String id, Ordinal ordinal, int stage, Key key, long arrivalRank, Object value, Object label, Object sourceTime) {
        Row {
            Objects.requireNonNull(id);
            Objects.requireNonNull(ordinal);
            Objects.requireNonNull(key);
            if (stage < 1 || arrivalRank < 0) {
                throw new IllegalArgumentException("stage must be positive and arrival rank must be nonnegative");
            }
            value = immutableLiteral(value);
        }

        Row(String id, Ordinal ordinal, int stage, Key key, long arrivalRank, Object value) {
            this(id, ordinal, stage, key, arrivalRank, value, id, null);
        }
    }

    /** Plain complete-chain predicates keep this model independent of ES|QL compilation. */
    @FunctionalInterface
    interface Predicate {
        Object evaluate(List<Row> completeChain);
    }

    /** Referenced stages document the condition contract; this declarative evaluator does no early pruning. */
    record Condition(int stage, Set<Integer> referencedStages, Predicate predicate) {
        Condition {
            referencedStages = Set.copyOf(referencedStages);
            Objects.requireNonNull(predicate);
            if (referencedStages.isEmpty() || referencedStages.stream().anyMatch(s -> s < 1 || s >= stage)) {
                throw new IllegalArgumentException("conditions must reference earlier stages");
            }
        }
    }

    /** Null windows and absent link limits mean no corresponding restriction in this test model. */
    record Query(int stages, Long windowNanos, Map<Integer, Long> linkLimitsNanos, List<Condition> conditions) {
        Query {
            if (stages < 2 || windowNanos != null && windowNanos < 0) {
                throw new IllegalArgumentException("invalid stages or window");
            }
            linkLimitsNanos = Map.copyOf(linkLimitsNanos);
            conditions = List.copyOf(conditions);
            for (var limit : linkLimitsNanos.entrySet()) {
                if (limit.getKey() < 2 || limit.getKey() > stages || limit.getValue() < 0) {
                    throw new IllegalArgumentException("invalid link limit");
                }
            }
            if (conditions.stream().anyMatch(condition -> condition.stage() > stages)) {
                throw new IllegalArgumentException("condition stage exceeds query stage count");
            }
        }
    }

    /** Snapshots distinguish retained candidates from historical consumption and the current match's direct seeds. */
    record Snapshot(
        Row after,
        Set<String> retainedIds,
        Set<String> usedIds,
        Set<String> unusedIds,
        Row latestUnusedStart,
        Set<String> emittedIds,
        Set<String> substituteIds,
        Set<String> crossMarkedIds,
        List<String> droppedIds,
        List<String> expiredIds
    ) {}

    /** Ordered match identities and requested observations can be compared without exposing matching internals. */
    record Result(
        List<List<String>> matches,
        List<Row> processingOrder,
        Map<String, Snapshot> snapshots,
        Snapshot finalSnapshot,
        List<String> droppedIds,
        List<String> expiredIds
    ) {}

    private static final Comparator<Row> PROCESSING_ORDER = Comparator.comparing(Row::ordinal)
        .thenComparing(Comparator.comparingInt(Row::stage).reversed())
        .thenComparing(Row::key)
        .thenComparingLong(Row::arrivalRank);
    private static final Comparator<Row> RECENCY_ORDER = Comparator.comparing(Row::ordinal)
        .thenComparing(Comparator.comparingLong(Row::arrivalRank).reversed());

    private SequenceRowDeclarativeOracle() {}

    /** Evaluate matching only; qualification requests any prefix observations separately. */
    static Result evaluate(Query query, List<Row> history, Integer bound) {
        return evaluate(query, history, bound, Set.of());
    }

    /**
     * Reconstruct bounded eligibility from the immutable prefix on every input, including eager expiry.
     * Used rows participate in this reconstruction and retain their bounded slots.
     */
    static Result evaluate(Query query, List<Row> history, Integer bound, Set<String> snapshotAfterIds) {
        if (bound != null && bound < 1) {
            throw new IllegalArgumentException("candidate bound must be positive or null");
        }
        List<Row> ordered = history.stream().sorted(PROCESSING_ORDER).toList();
        validateRows(query, ordered);
        Set<String> used = new LinkedHashSet<>();
        List<List<String>> matches = new ArrayList<>();
        Map<String, Snapshot> snapshots = new LinkedHashMap<>();
        List<String> dropped = new ArrayList<>();
        List<String> expired = new ArrayList<>();
        Set<String> previousRetained = Set.of();
        Snapshot finalSnapshot = null;
        for (int position = 0; position < ordered.size(); position++) {
            Row incoming = ordered.get(position);
            List<Row> beforePrefix = ordered.subList(0, position);
            List<Row> prefix = ordered.subList(0, position + 1);
            List<Row> before = retained(query, beforePrefix, incoming.ordinal().timestampNanos(), bound);
            Set<String> beforeIds = ids(before);
            for (String previous : previousRetained) {
                if (beforeIds.contains(previous) == false) {
                    expired.add(previous);
                }
            }
            List<Row> current = retained(query, prefix, incoming.ordinal().timestampNanos(), bound);
            Set<String> currentIds = ids(current);
            if (incoming.stage() < query.stages()) {
                Set<String> pool = new LinkedHashSet<>(beforeIds);
                pool.add(incoming.id());
                pool.removeAll(currentIds);
                dropped.addAll(pool);
            }
            Set<String> emitted = new LinkedHashSet<>();
            Set<String> substitutes = new LinkedHashSet<>();
            Set<String> crossMarked = new LinkedHashSet<>();
            if (incoming.stage() == query.stages()) {
                List<List<Row>> allowed = new ArrayList<>();
                for (int stage = 1; stage < query.stages(); stage++) {
                    int candidateStage = stage;
                    allowed.add(before.stream().filter(row -> row.stage() == candidateStage && row.key().equals(incoming.key())).toList());
                }
                List<Row> best = latestCompleteChain(query, allowed, incoming, used);
                if (best != null) {
                    matches.add(best.stream().map(Row::id).toList());
                    Set<String> beforeMatch = immutableSet(used);
                    Map<String, Row> direct = new LinkedHashMap<>();
                    for (int stage = 0; stage < query.stages() - 1; stage++) {
                        Row member = best.get(stage);
                        emitted.add(member.id());
                        direct.put(member.id(), member);
                        for (Row replacement : allowed.get(stage)) {
                            List<Row> changed = new ArrayList<>(best);
                            changed.set(stage, replacement);
                            if (completeChainValid(query, changed, beforeMatch)) {
                                direct.put(replacement.id(), replacement);
                                if (replacement.id().equals(member.id()) == false) {
                                    substitutes.add(replacement.id());
                                }
                            }
                        }
                    }
                    used.addAll(direct.keySet());
                    // Only original emitted/substitute seeds propagate. Newly cross-marked rows never become seeds.
                    for (Row seed : direct.values()) {
                        for (Row other : prefix) {
                            if (other.stage() < query.stages()
                                && other.stage() != seed.stage()
                                && other.key().equals(seed.key())
                                && other.ordinal().equals(seed.ordinal())) {
                                if (used.add(other.id())) {
                                    crossMarked.add(other.id());
                                }
                            }
                        }
                    }
                }
            }
            if (snapshotAfterIds.contains(incoming.id()) || position == ordered.size() - 1) {
                Set<String> unused = new LinkedHashSet<>(currentIds);
                unused.removeAll(used);
                Row latestStart = current.stream()
                    .filter(row -> row.stage() == 1 && used.contains(row.id()) == false)
                    .max(RECENCY_ORDER)
                    .orElse(null);
                Snapshot snapshot = new Snapshot(
                    incoming,
                    immutableSet(currentIds),
                    immutableSet(used),
                    immutableSet(unused),
                    latestStart,
                    immutableSet(emitted),
                    immutableSet(substitutes),
                    immutableSet(crossMarked),
                    List.copyOf(dropped),
                    List.copyOf(expired)
                );
                if (snapshotAfterIds.contains(incoming.id())) {
                    snapshots.put(incoming.id(), snapshot);
                }
                finalSnapshot = snapshot;
            }
            previousRetained = currentIds;
        }
        return new Result(
            List.copyOf(matches),
            ordered,
            Collections.unmodifiableMap(snapshots),
            finalSnapshot,
            List.copyOf(dropped),
            List.copyOf(expired)
        );
    }

    private static void validateRows(Query query, List<Row> rows) {
        Set<String> rowIds = new LinkedHashSet<>();
        Set<List<Object>> identities = new LinkedHashSet<>();
        for (Row row : rows) {
            if (row.stage() > query.stages() || rowIds.add(row.id()) == false) {
                throw new IllegalArgumentException("invalid or duplicate row id [" + row.id() + "]");
            }
            if (identities.add(List.of(row.ordinal(), row.stage(), row.key(), row.arrivalRank())) == false) {
                throw new IllegalArgumentException("duplicate candidate identity [" + row.id() + "]");
            }
        }
    }

    private static List<Row> retained(Query query, List<Row> prefix, long now, Integer bound) {
        Map<List<Object>, List<Row>> groups = new LinkedHashMap<>();
        for (Row row : prefix) {
            if (row.stage() < query.stages() && exceeds(now, row.ordinal().timestampNanos(), query.windowNanos()) == false) {
                groups.computeIfAbsent(List.of(row.stage(), row.key()), ignored -> new ArrayList<>()).add(row);
            }
        }
        List<Row> result = new ArrayList<>();
        for (List<Row> group : groups.values()) {
            group.sort(RECENCY_ORDER);
            result.addAll(group.subList(bound == null ? 0 : Math.max(0, group.size() - bound), group.size()));
        }
        return result;
    }

    private static List<Row> latestCompleteChain(Query query, List<List<Row>> allowed, Row last, Set<String> used) {
        if (allowed.stream().anyMatch(List::isEmpty)) {
            return null;
        }
        int[] positions = new int[allowed.size()];
        List<Row> best = null;
        boolean more = true;
        while (more) {
            List<Row> full = new ArrayList<>(query.stages());
            for (int stage = 0; stage < allowed.size(); stage++) {
                full.add(allowed.get(stage).get(positions[stage]));
            }
            full.add(last);
            if (completeChainValid(query, full, used) && (best == null || compareFromBack(full, best) > 0)) {
                best = List.copyOf(full);
            }
            // Forward-stage Cartesian product: every complete tuple is tested, without partial-chain pruning.
            int stage = positions.length - 1;
            while (stage >= 0) {
                positions[stage]++;
                if (positions[stage] < allowed.get(stage).size()) {
                    break;
                }
                positions[stage--] = 0;
            }
            more = stage >= 0;
        }
        return best;
    }

    private static int compareFromBack(List<Row> left, List<Row> right) {
        for (int stage = left.size() - 2; stage >= 0; stage--) {
            int recency = RECENCY_ORDER.compare(left.get(stage), right.get(stage));
            if (recency != 0) {
                return recency;
            }
        }
        return 0;
    }

    private static boolean completeChainValid(Query query, List<Row> full, Set<String> used) {
        for (int stage = 0; stage < full.size() - 1; stage++) {
            Row earlier = full.get(stage);
            Row later = full.get(stage + 1);
            if (used.contains(earlier.id()) || earlier.ordinal().compareTo(later.ordinal()) >= 0) {
                return false;
            }
            if (exceeds(later.ordinal().timestampNanos(), earlier.ordinal().timestampNanos(), query.linkLimitsNanos().get(stage + 2))) {
                return false;
            }
        }
        if (exceeds(full.getLast().ordinal().timestampNanos(), full.getFirst().ordinal().timestampNanos(), query.windowNanos())) {
            return false;
        }
        for (Condition condition : query.conditions()) {
            if (Boolean.TRUE.equals(condition.predicate().evaluate(full)) == false) {
                return false;
            }
        }
        return true;
    }

    private static boolean exceeds(long later, long earlier, Long limit) {
        if (limit == null) {
            return false;
        }
        try {
            return Math.subtractExact(later, earlier) > limit;
        } catch (ArithmeticException overflow) {
            return later >= earlier;
        }
    }

    /** Interpret only the frozen literal descriptor grammar, with no ES|QL expression machinery. */
    static Object expression(Map<String, Object> descriptor, int ownStage, List<Row> full) {
        if (descriptor.containsKey("op")) {
            List<?> args = list(descriptor.get("args"));
            if (args.size() != 2 || descriptor.size() != 2) {
                throw new IllegalArgumentException("a literal operation requires exactly two arguments");
            }
            Object left = expression(object(args.get(0)), ownStage, full);
            Object right = expression(object(args.get(1)), ownStage, full);
            if (left == null || right == null || left instanceof List<?> || right instanceof List<?>) {
                return null;
            }
            return switch (Objects.toString(descriptor.get("op"))) {
                case "eq" -> literalEquals(left, right);
                case "ne" -> literalEquals(left, right) == false;
                case "lt" -> compareLiterals(left, right) < 0;
                case "add" -> decimal(left).add(decimal(right));
                default -> throw new IllegalArgumentException("unsupported literal operation [" + descriptor.get("op") + "]");
            };
        }
        if (descriptor.containsKey("own")) {
            if (descriptor.size() != 1 || Boolean.TRUE.equals(descriptor.get("own")) == false) {
                throw new IllegalArgumentException("invalid own-value descriptor");
            }
            return full.get(ownStage - 1).value();
        }
        if (descriptor.containsKey("own_field")) {
            if (descriptor.size() != 1) {
                throw new IllegalArgumentException("invalid own-field descriptor");
            }
            return object(full.get(ownStage - 1).value()).get(descriptor.get("own_field"));
        }
        if (descriptor.containsKey("member")) {
            int stage = Math.toIntExact(integer(descriptor.get("member")));
            if (stage < 1 || stage >= ownStage || descriptor.size() != (descriptor.containsKey("field") ? 2 : 1)) {
                throw new IllegalArgumentException("invalid member descriptor");
            }
            Object value = full.get(stage - 1).value();
            return descriptor.containsKey("field") ? object(value).get(descriptor.get("field")) : value;
        }
        throw new IllegalArgumentException("unsupported literal descriptor " + descriptor.keySet());
    }

    private static boolean literalEquals(Object left, Object right) {
        return left instanceof Number && right instanceof Number ? decimal(left).compareTo(decimal(right)) == 0 : left.equals(right);
    }

    private static int compareLiterals(Object left, Object right) {
        if (left instanceof Number && right instanceof Number) {
            return decimal(left).compareTo(decimal(right));
        }
        if (left instanceof String a && right instanceof String b) {
            return a.compareTo(b);
        }
        throw new IllegalArgumentException("incomparable literal values");
    }

    private static BigDecimal decimal(Object value) {
        if (value instanceof Number number) {
            return new BigDecimal(number.toString());
        }
        throw new IllegalArgumentException("literal arithmetic requires numbers");
    }

    static long integer(Object value) {
        return decimal(value).longValueExact();
    }

    static Map<String, Object> object(Object value) {
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> result = new LinkedHashMap<>();
            for (var entry : map.entrySet()) {
                if (entry.getKey() instanceof String key) {
                    result.put(key, entry.getValue());
                } else {
                    throw new IllegalArgumentException("literal object keys must be strings");
                }
            }
            return result;
        }
        throw new IllegalArgumentException("expected a literal object");
    }

    static List<?> list(Object value) {
        if (value instanceof List<?> list) {
            return list;
        }
        throw new IllegalArgumentException("expected a literal list");
    }

    private static Object immutableLiteral(Object value) {
        if (value instanceof Map<?, ?>) {
            Map<String, Object> result = new LinkedHashMap<>();
            object(value).forEach((key, item) -> result.put(key, immutableLiteral(item)));
            return Collections.unmodifiableMap(result);
        }
        if (value instanceof List<?> list) {
            List<Object> result = new ArrayList<>();
            list.forEach(item -> result.add(immutableLiteral(item)));
            return Collections.unmodifiableList(result);
        }
        if (value == null || value instanceof String || value instanceof Number || value instanceof Boolean) {
            return value;
        }
        throw new IllegalArgumentException("unsupported literal value");
    }

    private static Set<String> ids(List<Row> rows) {
        Set<String> result = new LinkedHashSet<>();
        rows.forEach(row -> result.add(row.id()));
        return result;
    }

    private static Set<String> immutableSet(Set<String> values) {
        return Collections.unmodifiableSet(new LinkedHashSet<>(values));
    }
}
