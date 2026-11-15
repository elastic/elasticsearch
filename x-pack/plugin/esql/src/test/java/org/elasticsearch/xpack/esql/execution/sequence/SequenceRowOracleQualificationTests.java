/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Qualifies the independent declarative model against frozen portable expectations before differential use.
 * The four procedural requirements remain explicit obligations of separately qualified supplied models.
 */
public class SequenceRowOracleQualificationTests extends ESTestCase {
    private static final String REGISTRY_HASH = "ac1800f14690e2385158ade03f0af23c5c9b5da3e5ebc512f8da32dde61a9133";
    private static final String GOLDEN_HASH = "1d3e43c1f2475f9c5d31f65aa8d4678793ca3e2dbd20280a3bbe1ffb6b598769";
    private static final Set<String> PROCEDURAL_IDS = Set.of("comparison-069", "comparison-079", "comparison-080", "comparison-082");

    /** Every applicable source comparison, including duplicate expectations and empty matches, gets an actual comparison. */
    public void testFrozenQualification() throws Exception {
        Map<String, Object> registry = resource("frozen-registry.json", REGISTRY_HASH);
        Map<String, Object> golden = resource("nonrecursive-crossmark-golden.json", GOLDEN_HASH);
        List<?> fixtureData = list(registry.get("fixtures"));
        List<?> comparisonData = list(registry.get("comparisons"));
        assertEquals(80, fixtureData.size());
        assertEquals(109, comparisonData.size());
        assertEquals(3, list(registry.get("aliases")).size());
        Map<String, Map<String, Object>> fixtures = new LinkedHashMap<>();
        for (Object item : fixtureData) {
            Map<String, Object> fixture = object(item);
            assertNull(fixtures.put(text(fixture.get("id")), fixture));
        }
        Map<String, SequenceRowDeclarativeOracle.Result> evaluated = new HashMap<>();
        List<Map<String, Object>> comparisons = new ArrayList<>();
        List<Map<String, Object>> proceduralRequirements = new ArrayList<>();
        List<String> failures = new ArrayList<>();
        Map<String, Integer> comparedContracts = new LinkedHashMap<>();
        Set<String> representedProceduralIds = new LinkedHashSet<>();
        for (int index = 0; index < comparisonData.size(); index++) {
            Map<String, Object> comparison = object(comparisonData.get(index));
            String comparisonId = text(comparison.get("id"));
            assertEquals("comparison-" + String.format(java.util.Locale.ROOT, "%03d", index + 1), comparisonId);
            String fixtureId = text(comparison.get("fixture"));
            Map<String, Object> fixture = Objects.requireNonNull(fixtures.get(fixtureId), "missing fixture " + fixtureId);
            String contract = text(comparison.get("contract"));
            if (contract.equals("procedural")) {
                assertTrue(PROCEDURAL_IDS.contains(comparisonId));
                assertTrue(representedProceduralIds.add(comparisonId));
                Map<String, Object> procedure = object(fixture.get("procedure"));
                assertTrue(Set.of("grouped-row-search", "single-value-search").contains(procedure.get("kind")));
                proceduralRequirements.add(
                    fields(
                        "id",
                        comparisonId,
                        "fixture",
                        fixtureId,
                        "contract",
                        contract,
                        "procedure",
                        procedure,
                        "raw_expected_source_labels",
                        comparison.get("expected_source_labels"),
                        "canonical_expected_labels",
                        comparison.containsKey("expected_ordered_matches")
                            ? canonicalLabels(comparison.get("expected_ordered_matches"), inputLabels(fixture))
                            : null,
                        "status",
                        "requires_separate_supplied_model_qualification"
                    )
                );
                continue;
            }
            assertTrue(Set.of("declarative", "operational").contains(contract));
            comparedContracts.merge(contract, 1, Integer::sum);
            Map<String, Object> record = fields(
                "id",
                comparisonId,
                "fixture",
                fixtureId,
                "contract",
                contract,
                "raw_expected_source_labels",
                comparison.get("expected_source_labels")
            );
            try {
                if (fixture.get("procedure") != null) {
                    throw new IllegalArgumentException("a procedural fixture cannot be evaluated declaratively");
                }
                SequenceRowDeclarativeOracle.Result actual = evaluated.computeIfAbsent(fixtureId, ignored -> {
                    List<SequenceRowDeclarativeOracle.Row> rows = rows(fixture);
                    Set<String> requested = new LinkedHashSet<>();
                    for (Object candidate : comparisonData) {
                        Map<String, Object> request = object(candidate);
                        if (fixtureId.equals(request.get("fixture")) && request.containsKey("expected_observation")) {
                            rows.forEach(row -> requested.add(row.id()));
                        }
                    }
                    SequenceRowDeclarativeOracle.Result result = SequenceRowDeclarativeOracle.evaluate(
                        query(fixture),
                        rows,
                        bound(fixture),
                        requested
                    );
                    requireEqual(
                        fixture.get("processing_order"),
                        result.processingOrder().stream().map(SequenceRowDeclarativeOracle.Row::id).toList(),
                        "processing_order"
                    );
                    return result;
                });
                Map<String, Object> result = fields(
                    "ordered_matches",
                    actual.matches(),
                    "source_labels",
                    labels(actual),
                    "canonical_labels",
                    labels(actual)
                );
                Map<String, Object> inputLabels = inputLabels(fixture);
                if (comparison.containsKey("expected_ordered_matches")) {
                    record.put("canonical_expected_labels", canonicalLabels(comparison.get("expected_ordered_matches"), inputLabels));
                }
                if (comparison.containsKey("expected_observation")) {
                    result.put(
                        "observation",
                        observation(object(comparison.get("expected_observation")), actual, query(fixture), nanosPerTick(fixture))
                    );
                }
                if (comparison.containsKey("expected_counters")) {
                    throw new IllegalArgumentException("unexpected declarative counter contract");
                }
                record.put("actual", result);
                record.put("time_normalization", fixture.get("time_normalization"));
                compareRecord(comparison, result, inputLabels);
                record.put("status", "pass");
            } catch (RuntimeException failure) {
                record.put("status", "fail");
                record.put("error", failure.toString());
                failures.add(comparisonId + ": " + failure);
            }
            comparisons.add(record);
        }
        assertEquals(PROCEDURAL_IDS, representedProceduralIds);
        assertEquals(Map.of("declarative", 90, "operational", 15), comparedContracts);
        assertEquals(105, comparisons.size());
        List<Map<String, Object>> supplemental = supplemental(golden, failures);
        Map<String, Object> receipt = fields(
            "schema",
            "esql.sequence.row-oracle.java-actuals.v1",
            "portable_registry_sha256",
            REGISTRY_HASH,
            "portable_golden_sha256",
            GOLDEN_HASH,
            "registry_parent_sha256",
            registry.get("parent_sha256"),
            "golden_parent_sha256",
            golden.get("parent_sha256"),
            "source_label_protocol",
            "frozen-row-identity-to-input-label.v1",
            "compared_contracts",
            comparedContracts,
            "java_compared_records",
            comparisons.size(),
            "comparisons",
            comparisons,
            "procedural_requirements",
            proceduralRequirements,
            "supplemental",
            supplemental,
            "failures",
            failures,
            "oracle_use_authorized",
            false
        );
        writeRequestedReceipt(receipt);
        assertTrue(String.join("\n", failures), failures.isEmpty());
    }

    /** Literal arithmetic must not accidentally inherit JSON parser Integer/Long wrapper distinctions. */
    public void testNumericLiteralOperationsAreValueBased() {
        var key = SequenceRowDeclarativeOracle.Key.utf8("k");
        List<SequenceRowDeclarativeOracle.Row> full = List.of(
            new SequenceRowDeclarativeOracle.Row("a", new SequenceRowDeclarativeOracle.Ordinal(0, 0), 1, key, 0, 1),
            new SequenceRowDeclarativeOracle.Row("b", new SequenceRowDeclarativeOracle.Ordinal(1, 1), 2, key, 0, 2L),
            new SequenceRowDeclarativeOracle.Row("c", new SequenceRowDeclarativeOracle.Ordinal(2, 2), 3, key, 0, 3L)
        );
        assertEquals(
            Boolean.TRUE,
            SequenceRowDeclarativeOracle.expression(
                fields(
                    "op",
                    "eq",
                    "args",
                    List.of(fields("own", true), fields("op", "add", "args", List.of(fields("member", 1), fields("member", 2))))
                ),
                3,
                full
            )
        );
        List<SequenceRowDeclarativeOracle.Row> same = List.of(
            full.getFirst(),
            new SequenceRowDeclarativeOracle.Row("d", new SequenceRowDeclarativeOracle.Ordinal(1, 1), 2, key, 0, 1L)
        );
        assertEquals(
            Boolean.TRUE,
            SequenceRowDeclarativeOracle.expression(fields("op", "eq", "args", List.of(fields("own", true), fields("member", 1))), 2, same)
        );
    }

    /** Source aliases and integral display tokens map by frozen identity; incorrect member identity/order must still fail. */
    public void testSourceDisplayLabelsUseFrozenIdentity() {
        Map<String, Object> labels = Map.of("a", "A#0", "f", "F#0", "zero", "0", "one", "1");
        List<List<String>> ordered = List.of(List.of("a", "f"), List.of("zero", "one"));
        Map<String, Object> expected = fields(
            "expected_ordered_matches",
            ordered,
            "expected_source_labels",
            List.of(List.of("A#0", "F"), List.of(0, 1))
        );
        List<List<Object>> canonical = canonicalLabels(ordered, labels);
        assertEquals(List.of(List.of("A#0", "F#0"), List.of("0", "1")), canonical);
        Map<String, Object> actual = fields("ordered_matches", ordered, "canonical_labels", canonical);
        compareRecord(expected, actual, labels);
        Map<String, Object> wrongIdentity = fields(
            "ordered_matches",
            List.of(List.of("other", "f"), List.of("zero", "one")),
            "canonical_labels",
            canonical
        );
        expectThrows(IllegalArgumentException.class, () -> compareRecord(expected, wrongIdentity, labels));
        Map<String, Object> wrongOrder = fields(
            "ordered_matches",
            List.of(List.of("f", "a"), List.of("zero", "one")),
            "canonical_labels",
            canonical
        );
        expectThrows(IllegalArgumentException.class, () -> compareRecord(expected, wrongOrder, labels));
        Map<String, Object> wrongEmissionOrder = fields(
            "ordered_matches",
            List.of(ordered.get(1), ordered.get(0)),
            "canonical_labels",
            canonical
        );
        expectThrows(IllegalArgumentException.class, () -> compareRecord(expected, wrongEmissionOrder, labels));
        assertEquals(List.of(List.of("A#0", "F"), List.of(0, 1)), expected.get("expected_source_labels"));
    }

    private static List<Map<String, Object>> supplemental(Map<String, Object> golden, List<String> failures) {
        List<Map<String, Object>> records = new ArrayList<>();
        List<SequenceRowDeclarativeOracle.Row> rows = rows(golden);
        Set<String> requested = new LinkedHashSet<>();
        rows.forEach(row -> requested.add(row.id()));
        for (Object configuration : list(golden.get("configurations"))) {
            Map<String, Object> config = object(configuration);
            String id = text(config.get("id"));
            Map<String, Object> record = fields(
                "id",
                id,
                "candidate_bound",
                config.get("candidate_bound"),
                "raw_expected_source_labels",
                golden.get("expected_source_labels")
            );
            try {
                var result = SequenceRowDeclarativeOracle.evaluate(query(golden), rows, bound(config), requested);
                Map<String, Object> actual = fields(
                    "ordered_matches",
                    result.matches(),
                    "source_labels",
                    labels(result),
                    "canonical_labels",
                    labels(result),
                    "processing_order",
                    result.processingOrder().stream().map(SequenceRowDeclarativeOracle.Row::id).toList(),
                    "after_first_match",
                    observation(object(golden.get("after_first_match")), result, query(golden), 1_000_000L),
                    "after_second_match",
                    observation(object(golden.get("after_second_match")), result, query(golden), 1_000_000L)
                );
                record.put("actual", actual);
                List<List<Object>> canonicalExpected = canonicalLabels(golden.get("expected_ordered_matches"), inputLabels(golden));
                record.put("canonical_expected_labels", canonicalExpected);
                requireEqual(golden.get("expected_ordered_matches"), actual.get("ordered_matches"), "ordered_matches");
                requireEqual(canonicalExpected, actual.get("canonical_labels"), "canonical_labels");
                requireEqual(golden.get("processing_order"), actual.get("processing_order"), "processing_order");
                requireEqual(golden.get("after_first_match"), actual.get("after_first_match"), "after_first_match");
                requireEqual(golden.get("after_second_match"), actual.get("after_second_match"), "after_second_match");
                record.put("status", "pass");
            } catch (RuntimeException failure) {
                record.put("status", "fail");
                record.put("error", failure.toString());
                failures.add(id + ": " + failure);
            }
            records.add(record);
        }
        if (records.size() != 2) {
            throw new IllegalArgumentException("exactly two supplemental configurations are required");
        }
        return records;
    }

    private static Map<String, Object> observation(
        Map<String, Object> request,
        SequenceRowDeclarativeOracle.Result result,
        SequenceRowDeclarativeOracle.Query query,
        long nanosPerTick
    ) {
        var snapshot = result.finalSnapshot();
        if (request.containsKey("after_final_id")) {
            snapshot = Objects.requireNonNull(
                result.snapshots().get(text(request.get("after_final_id"))),
                "missing requested final snapshot"
            );
        } else if (request.containsKey("after")) {
            Object label = request.get("after");
            var row = result.processingOrder()
                .stream()
                .filter(candidate -> Objects.equals(candidate.label(), label))
                .findFirst()
                .orElseThrow();
            snapshot = Objects.requireNonNull(result.snapshots().get(row.id()), "missing requested row snapshot");
        }
        Objects.requireNonNull(snapshot, "cannot observe an empty input");
        Map<String, Object> actual = new LinkedHashMap<>();
        for (String field : request.keySet()) {
            Object value = switch (field) {
                case "after_final_id" -> snapshot.after().id();
                case "after" -> snapshot.after().label();
                case "used_earlier_ids" -> new ArrayList<>(snapshot.usedIds());
                case "unused_earlier_ids" -> new ArrayList<>(snapshot.unusedIds());
                case "direct_used_earlier_ids" -> new ArrayList<>(snapshot.emittedIds());
                case "substitute_used_earlier_ids" -> new ArrayList<>(snapshot.substituteIds());
                case "additional_cross_marked_earlier_ids" -> new ArrayList<>(snapshot.crossMarkedIds());
                case "final_not_used_up" -> snapshot.usedIds().contains(snapshot.after().id()) == false;
                case "latest_unused_start" -> sourceTime(snapshot.latestUnusedStart(), nanosPerTick);
                case "latest_unused_start_trace" -> latestStartTrace(result, query, nanosPerTick);
                case "dropped" -> snapshot.droppedIds().size();
                case "expired" -> snapshot.expiredIds().size();
                case "dropped_row_ids" -> snapshot.droppedIds();
                case "dropped_row_id" -> {
                    if (snapshot.droppedIds().size() != 1) {
                        throw new IllegalArgumentException("single dropped-row observation requires exactly one drop");
                    }
                    yield snapshot.droppedIds().getFirst();
                }
                case "flags" -> flags(result, snapshot);
                case "not_arrived_row_ids" -> notArrived(result, snapshot);
                case "final_rows_used" -> {
                    Set<String> used = snapshot.usedIds();
                    yield result.processingOrder().stream().anyMatch(row -> row.stage() == query.stages() && used.contains(row.id()));
                }
                default -> throw new IllegalArgumentException("unrepresented observation [" + field + "]");
            };
            actual.put(field, value);
        }
        return actual;
    }

    private static List<Map<String, Object>> latestStartTrace(
        SequenceRowDeclarativeOracle.Result result,
        SequenceRowDeclarativeOracle.Query query,
        long nanosPerTick
    ) {
        List<Map<String, Object>> trace = new ArrayList<>();
        for (var row : result.processingOrder()) {
            if (row.stage() < query.stages()) {
                var snapshot = Objects.requireNonNull(result.snapshots().get(row.id()), "missing trace snapshot");
                Long latest = sourceTime(snapshot.latestUnusedStart(), nanosPerTick);
                if (query.windowNanos() == null || query.windowNanos() % nanosPerTick != 0) {
                    throw new IllegalArgumentException("trace window has no exact source-tick representation");
                }
                trace.add(
                    fields(
                        "after_label",
                        row.label(),
                        "latest_unused_start",
                        latest,
                        "upper_bound",
                        latest == null ? null : Math.addExact(latest, query.windowNanos() / nanosPerTick),
                        "latest_unused_start_nanos",
                        snapshot.latestUnusedStart() == null ? null : snapshot.latestUnusedStart().ordinal().timestampNanos(),
                        "upper_bound_nanos",
                        latest == null ? null : Math.addExact(snapshot.latestUnusedStart().ordinal().timestampNanos(), query.windowNanos())
                    )
                );
            }
        }
        // Keep both units in the actual receipt; comparison projects the frozen source-tick fields explicitly.
        return trace;
    }

    private static Long sourceTime(SequenceRowDeclarativeOracle.Row row, long nanosPerTick) {
        if (row == null) {
            return null;
        }
        long source = row.sourceTime() instanceof String string ? Long.parseLong(string) : integer(row.sourceTime());
        if (Math.multiplyExact(source, nanosPerTick) != row.ordinal().timestampNanos()) {
            throw new IllegalArgumentException("source-time annotation disagrees with normalized timestamp");
        }
        return source;
    }

    private static List<Map<String, Object>> flags(
        SequenceRowDeclarativeOracle.Result result,
        SequenceRowDeclarativeOracle.Snapshot snapshot
    ) {
        List<Map<String, Object>> flags = new ArrayList<>();
        for (var row : result.processingOrder()) {
            flags.add(
                fields(
                    "row_id",
                    row.id(),
                    "used",
                    snapshot.usedIds().contains(row.id()),
                    "in_earlier_ring",
                    snapshot.retainedIds().contains(row.id())
                )
            );
            if (row.id().equals(snapshot.after().id())) {
                break;
            }
        }
        return flags;
    }

    private static List<String> notArrived(SequenceRowDeclarativeOracle.Result result, SequenceRowDeclarativeOracle.Snapshot snapshot) {
        int position = result.processingOrder().indexOf(snapshot.after());
        return result.processingOrder()
            .subList(position + 1, result.processingOrder().size())
            .stream()
            .map(SequenceRowDeclarativeOracle.Row::id)
            .toList();
    }

    private static void compareRecord(Map<String, Object> expected, Map<String, Object> actual, Map<String, Object> inputLabels) {
        if (expected.containsKey("expected_ordered_matches")) {
            requireEqual(expected.get("expected_ordered_matches"), actual.get("ordered_matches"), "ordered_matches");
            requireEqual(
                canonicalLabels(expected.get("expected_ordered_matches"), inputLabels),
                actual.get("canonical_labels"),
                "canonical_labels"
            );
        }
        if (expected.containsKey("expected_observation")) {
            Map<String, Object> observation = object(actual.get("observation"));
            if (observation.containsKey("latest_unused_start_trace")) {
                List<Map<String, Object>> sourceTicks = new ArrayList<>();
                for (Object item : list(observation.get("latest_unused_start_trace"))) {
                    Map<String, Object> entry = object(item);
                    sourceTicks.add(
                        fields(
                            "after_label",
                            entry.get("after_label"),
                            "latest_unused_start",
                            entry.get("latest_unused_start"),
                            "upper_bound",
                            entry.get("upper_bound")
                        )
                    );
                }
                observation.put("latest_unused_start_trace", sourceTicks);
            }
            requireEqual(expected.get("expected_observation"), observation, "observation");
        }
    }

    private static void requireEqual(Object expected, Object actual, String field) {
        if (equal(expected, actual, field) == false) {
            throw new IllegalArgumentException(field + " expected " + expected + " but actual " + actual);
        }
    }

    private static boolean equal(Object expected, Object actual, String field) {
        if (expected instanceof Number a && actual instanceof Number b) {
            return new BigDecimal(a.toString()).compareTo(new BigDecimal(b.toString())) == 0;
        }
        if (expected instanceof Map<?, ?> && actual instanceof Map<?, ?>) {
            Map<String, Object> left = object(expected);
            Map<String, Object> right = object(actual);
            return left.keySet().equals(right.keySet())
                && left.keySet().stream().allMatch(key -> equal(left.get(key), right.get(key), key));
        }
        if (expected instanceof List<?> left && actual instanceof List<?> right) {
            if (field.endsWith("_ids")) {
                return new LinkedHashSet<>(left).size() == left.size()
                    && new LinkedHashSet<>(right).size() == right.size()
                    && new LinkedHashSet<>(left).equals(new LinkedHashSet<>(right));
            }
            if (left.size() != right.size()) {
                return false;
            }
            for (int index = 0; index < left.size(); index++) {
                if (equal(left.get(index), right.get(index), field) == false) {
                    return false;
                }
            }
            return true;
        }
        return Objects.equals(expected, actual);
    }

    private static List<List<Object>> labels(SequenceRowDeclarativeOracle.Result result) {
        Map<String, Object> labels = new HashMap<>();
        result.processingOrder().forEach(row -> labels.put(row.id(), row.label()));
        return result.matches().stream().map(match -> match.stream().map(labels::get).toList()).toList();
    }

    private static Map<String, Object> inputLabels(Map<String, Object> fixture) {
        Map<String, Object> labels = new LinkedHashMap<>();
        for (Object item : list(fixture.get("rows"))) {
            Map<String, Object> row = object(item);
            String id = text(row.get("id"));
            if (labels.containsKey(id)) {
                throw new IllegalArgumentException("duplicate frozen label identity [" + id + "]");
            }
            labels.put(id, row.get("label"));
        }
        return labels;
    }

    /** Source spellings are evidence; canonical expected labels come only from frozen expected IDs and immutable input rows. */
    private static List<List<Object>> canonicalLabels(Object frozenOrderedIds, Map<String, Object> inputLabels) {
        List<List<Object>> labels = new ArrayList<>();
        for (Object item : list(frozenOrderedIds)) {
            List<Object> tuple = new ArrayList<>();
            for (Object identity : list(item)) {
                String id = text(identity);
                if (inputLabels.containsKey(id) == false) {
                    throw new IllegalArgumentException("unknown frozen label identity [" + id + "]");
                }
                tuple.add(inputLabels.get(id));
            }
            labels.add(tuple);
        }
        return labels;
    }

    private static List<SequenceRowDeclarativeOracle.Row> rows(Map<String, Object> fixture) {
        List<SequenceRowDeclarativeOracle.Row> rows = new ArrayList<>();
        for (Object item : list(fixture.get("rows"))) {
            Map<String, Object> row = object(item);
            Map<String, Object> values = object(row.get("values"));
            if (values.size() != 1 || values.containsKey("v") == false) {
                throw new IllegalArgumentException("unsupported frozen value columns");
            }
            rows.add(
                new SequenceRowDeclarativeOracle.Row(
                    text(row.get("id")),
                    new SequenceRowDeclarativeOracle.Ordinal(
                        Long.parseLong(text(row.get("timestamp_nanos"))),
                        integer(row.get("shard_doc"))
                    ),
                    Math.toIntExact(integer(row.get("stage"))),
                    new SequenceRowDeclarativeOracle.Key(text(row.get("key_utf8_hex"))),
                    integer(row.get("arrival_rank")),
                    values.get("v"),
                    row.get("label"),
                    row.containsKey("source_time") ? row.get("source_time") : row.get("source_time_ms")
                )
            );
        }
        return rows;
    }

    private static SequenceRowDeclarativeOracle.Query query(Map<String, Object> fixture) {
        Map<String, Object> input = object(fixture.get("query"));
        if (input.get("search_step_limit") != null) {
            throw new IllegalArgumentException("declarative evaluation cannot enforce a procedural step limit");
        }
        Map<Integer, Long> limits = new LinkedHashMap<>();
        object(input.get("link_limits_nanos")).forEach((stage, limit) -> limits.put(Integer.valueOf(stage), Long.parseLong(text(limit))));
        List<SequenceRowDeclarativeOracle.Condition> conditions = new ArrayList<>();
        for (Object item : list(input.get("conditions"))) {
            Map<String, Object> condition = object(item);
            int stage = Math.toIntExact(integer(condition.get("stage")));
            Set<Integer> references = new LinkedHashSet<>();
            list(condition.get("referenced_stages")).forEach(value -> references.add(Math.toIntExact(integer(value))));
            Map<String, Object> expression = object(condition.get("expression"));
            conditions.add(
                new SequenceRowDeclarativeOracle.Condition(
                    stage,
                    references,
                    full -> SequenceRowDeclarativeOracle.expression(expression, stage, full)
                )
            );
        }
        Object window = input.get("window_nanos");
        return new SequenceRowDeclarativeOracle.Query(
            Math.toIntExact(integer(input.get("stages"))),
            window == null ? null : Long.parseLong(text(window)),
            limits,
            conditions
        );
    }

    private static Integer bound(Map<String, Object> fixture) {
        Object bound = fixture.get("candidate_bound");
        return bound == null ? null : Math.toIntExact(integer(bound));
    }

    private static long nanosPerTick(Map<String, Object> fixture) {
        return switch (text(fixture.get("time_normalization"))) {
            case "source integer tick * 60000000000; original source_time preserved" -> 60_000_000_000L;
            case "already nanoseconds; copied unchanged" -> 1L;
            default -> throw new IllegalArgumentException("unrepresented time normalization");
        };
    }

    private static Map<String, Object> resource(String name, String expectedHash) throws Exception {
        try (
            InputStream input = Objects.requireNonNull(
                SequenceRowOracleQualificationTests.class.getResourceAsStream("/sequence/latest-valid/" + name)
            )
        ) {
            byte[] bytes = input.readAllBytes();
            String actualHash = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
            if (actualHash.equals(expectedHash) == false) {
                throw new IllegalArgumentException("portable resource pin mismatch [" + name + "]");
            }
            try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, bytes)) {
                return parser.mapOrdered();
            }
        }
    }

    private static void writeRequestedReceipt(Map<String, Object> receipt) throws IOException {
        String output = System.getProperty("tests.sequence.oracle.receipt");
        if (output == null) {
            return;
        }
        String ownedRoot = Objects.requireNonNull(
            System.getProperty("tests.sequence.oracle.receipt_root"),
            "an explicit owned receipt root is required"
        );
        Path root = Path.of(ownedRoot).toAbsolutePath().normalize();
        Path path = Path.of(output).toAbsolutePath().normalize();
        if (path.startsWith(root) == false || path.equals(root) || root.getParent() == null) {
            throw new IllegalArgumentException("receipt must be below its explicit owned root");
        }
        Files.createDirectories(path.getParent());
        if (path.getParent().toRealPath().startsWith(root.toRealPath()) == false) {
            throw new IllegalArgumentException("receipt parent escapes its explicit owned root");
        }
        try (
            XContentBuilder builder = new XContentBuilder(
                XContentType.JSON.xContent(),
                Files.newOutputStream(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)
            )
        ) {
            builder.map(receipt);
        }
    }

    private static Map<String, Object> fields(Object... pairs) {
        if (pairs.length % 2 != 0) {
            throw new IllegalArgumentException("fields require key/value pairs");
        }
        Map<String, Object> result = new LinkedHashMap<>();
        for (int index = 0; index < pairs.length; index += 2) {
            result.put(text(pairs[index]), pairs[index + 1]);
        }
        return result;
    }

    private static Map<String, Object> object(Object value) {
        return SequenceRowDeclarativeOracle.object(value);
    }

    private static List<?> list(Object value) {
        return SequenceRowDeclarativeOracle.list(value);
    }

    private static long integer(Object value) {
        return SequenceRowDeclarativeOracle.integer(value);
    }

    private static String text(Object value) {
        return Objects.requireNonNull(value).toString();
    }
}
