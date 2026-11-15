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

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Qualifies the procedural helper using frozen external F4/F8 inputs and literal visit-boundary controls.
 * No production matcher is instantiated or consulted. A successful run still requires independent receipt review
 * before the helper is admitted for engine comparisons.
 */
public class SequenceRowProceduralOracleQualificationTests extends ESTestCase {
    private static final String REGISTRY_HASH = "ac1800f14690e2385158ade03f0af23c5c9b5da3e5ebc512f8da32dde61a9133";
    private static final String CONTRACT_HASH = "a9db66e0dcc5597e8e655aeb8ee6fca649ec14716e2c72cfabd8c6f8998a8aa1";
    private static final String SPEC_HASH = "8013089be14ec3660cf9f21a0febbc821e1cd9c6766ab4abd1d0f30d79ef8d3b";
    private static final String ROWS_SCRIPT_HASH = "19427ba40595b83b4de4424052ebaba280e61351bcbf177e36cd9f0e840a5446";
    private static final String WORKLIMIT_SCRIPT_HASH = "d89e9c99b9fcd143ecf1c5b705491e7ec22f008362eb4be9218e363bebda0d43";
    private static final String SOURCE_PATH = "x-pack/plugin/esql/src/test/java/org/elasticsearch/xpack/esql/execution/sequence/";
    private static final List<String> COMPARISON_IDS = List.of(
        "comparison-067",
        "comparison-068",
        "comparison-069",
        "comparison-079",
        "comparison-080",
        "comparison-081",
        "comparison-082"
    );
    private static final Set<String> FIXTURE_IDS = Set.of(
        "r2.F4A",
        "r2.F4B.procedural",
        "r2.F8.limited",
        "r2.F8.unlimited",
        "r2.F8.unlimited-f1-prefix"
    );
    private static final SequenceRowProceduralOracle.Key KEY = SequenceRowProceduralOracle.Key.utf8("k");

    private record Control(
        String id,
        SequenceRowProceduralOracle.Query query,
        List<SequenceRowProceduralOracle.Row> rows,
        int bound,
        long limit,
        List<List<String>> expectedMatches,
        List<Long> expectedSteps,
        long expectedAbandoned,
        Set<String> expectedUsedAfter
    ) {}

    /** Seven unchanged external records and the separately frozen controls qualify behavior without engine agreement. */
    public void testFrozenQualification() throws Exception {
        Map<String, Object> registry = resource("frozen-registry.json", REGISTRY_HASH);
        Map<String, Map<String, Object>> fixtures = new LinkedHashMap<>();
        for (Object item : list(registry.get("fixtures"))) {
            Map<String, Object> fixture = object(item);
            if (FIXTURE_IDS.contains(text(fixture.get("id")))) {
                assertNull(fixtures.put(text(fixture.get("id")), fixture));
            }
        }
        assertEquals(FIXTURE_IDS, fixtures.keySet());
        Map<String, SequenceRowProceduralOracle.Result> results = new LinkedHashMap<>();
        for (var entry : fixtures.entrySet()) {
            Map<String, Object> fixture = entry.getValue();
            var result = SequenceRowProceduralOracle.evaluate(query(fixture), rows(fixture), bound(fixture), stepLimit(fixture));
            requireEqual(
                fixture.get("processing_order"),
                result.processingOrder().stream().map(SequenceRowProceduralOracle.Row::id).toList(),
                entry.getKey() + ".processing_order"
            );
            results.put(entry.getKey(), result);
        }
        List<Map<String, Object>> comparisons = new ArrayList<>();
        for (Object item : list(registry.get("comparisons"))) {
            Map<String, Object> expected = object(item);
            String id = text(expected.get("id"));
            if (COMPARISON_IDS.contains(id) == false) {
                continue;
            }
            String fixtureId = text(expected.get("fixture"));
            var actual = Objects.requireNonNull(results.get(fixtureId));
            Map<String, Object> fixture = fixtures.get(fixtureId);
            Map<String, Object> record = fields(
                "id",
                id,
                "fixture",
                fixtureId,
                "contract",
                expected.get("contract"),
                "fixture_input_sha256",
                fixture.get("input_sha256"),
                "raw_expected_source_labels",
                expected.get("expected_source_labels"),
                "actual",
                actual(actual)
            );
            if (expected.containsKey("expected_ordered_matches")) {
                requireEqual(expected.get("expected_ordered_matches"), actual.matches(), id + ".ordered_matches");
                List<List<Object>> canonical = canonicalLabels(expected.get("expected_ordered_matches"), actual.processingOrder());
                requireEqual(canonical, labels(actual), id + ".canonical_source_labels");
                record.put("canonical_expected_labels", canonical);
            }
            if (expected.containsKey("expected_counters")) {
                Map<String, Object> counters = counters(actual);
                object(expected.get("expected_counters")).forEach(
                    (name, value) -> requireEqual(value, counters.get(name), id + "." + name)
                );
            }
            if (expected.containsKey("expected_observation")) {
                requireEqual(expected.get("expected_observation"), abandonmentObservation(actual), id + ".observation");
                Map<String, Object> actualRecord = object(record.get("actual"));
                actualRecord.put("observation", abandonmentObservation(actual));
                record.put("actual", actualRecord);
            }
            record.put("status", "pass");
            comparisons.add(record);
        }
        assertEquals(COMPARISON_IDS, comparisons.stream().map(record -> text(record.get("id"))).toList());
        var f4a = results.get("r2.F4A");
        assertEquals(2L, f4a.finalSearches().getFirst().steps());
        var limited = results.get("r2.F8.limited");
        assertEquals(1L, limited.abandonedSearches());
        assertEquals(131_072L, limited.finalSearches().getFirst().steps());
        assertTrue(limited.finalSearches().getFirst().abandoned());
        assertEquals(0, limited.finalSearches().getFirst().before().usedIds().size());
        assertEquals(731, limited.finalSearches().getFirst().before().unusedIds().size());
        requireNoMarks(limited.finalSearches().getFirst());
        var unlimited = results.get("r2.F8.unlimited");
        assertEquals(133_731L, unlimited.finalSearches().getFirst().steps());
        assertEquals(0L, unlimited.abandonedSearches());
        assertEquals(2, unlimited.finalSearches().size());
        assertEquals(List.of(), unlimited.finalSearches().get(1).match());
        List<Map<String, Object>> controls = controls();
        List<Map<String, Object>> hardNegatives = hardNegativeFamily();
        Map<String, Object> receipt = fields(
            "schema",
            "esql.sequence.row-procedural-oracle.java-actuals.v1",
            "actual_source",
            "independent Java port of pinned Python row procedure",
            "source_label_protocol",
            "frozen-row-identity-to-input-label.v1",
            "source_pins",
            fields(
                "portable_registry_sha256",
                REGISTRY_HASH,
                "external_contract_sha256",
                CONTRACT_HASH,
                "semantics_spec_sha256",
                SPEC_HASH,
                "rows_S_sha256",
                ROWS_SCRIPT_HASH,
                "worklimit_S_sha256",
                WORKLIMIT_SCRIPT_HASH
            ),
            "comparison_count",
            comparisons.size(),
            "fixture_count",
            results.size(),
            "comparisons",
            comparisons,
            "literal_controls",
            controls,
            "hard_negative_family",
            hardNegatives,
            "oracle_use_authorized",
            false,
            "limits",
            "Covered frozen inputs only; independent external-model receipt review and admission remain required."
        );
        emitRequestedReceipt(receipt, fixtures, comparisons, controls, hardNegatives);
    }

    /** Numeric wrapper types and multivalued/null results must not change the pinned scalar descriptor meaning. */
    public void testLiteralPredicateDescriptors() {
        var own = row("own", 2, 3, 3L);
        var left = row("left", 0, 1, 1);
        var right = row("right", 1, 2, 2L);
        Map<String, Object> sum = fields(
            "op",
            "eq",
            "args",
            List.of(fields("own", true), fields("op", "add", "args", List.of(fields("member", 1), fields("member", 2))))
        );
        assertEquals(Boolean.TRUE, expression(sum, own, Map.of(1, left, 2, right)));
        assertNull(expression(sum, own, Map.of(1, row("null", 0, 1, null), 2, right)));
        assertNull(expression(sum, own, Map.of(1, row("multi", 0, 1, List.of(1, 1)), 2, right)));
        assertEquals(
            Boolean.TRUE,
            expression(fields("op", "lt", "args", List.of(fields("member", 1), fields("own", true))), own, Map.of(1, left))
        );
        assertEquals(
            Boolean.TRUE,
            expression(fields("op", "ne", "args", List.of(fields("member", 1), fields("own", true))), own, Map.of(1, left))
        );
        for (Object value : List.of(false, List.of(true, false), List.of(true, true))) {
            var query = new SequenceRowProceduralOracle.Query(
                2,
                10L,
                Map.of(),
                List.of(new SequenceRowProceduralOracle.Condition(2, Set.of(1), (current, earlier) -> earlier.get(1).value()))
            );
            assertEquals(
                List.of(),
                SequenceRowProceduralOracle.evaluate(query, List.of(row("a", 0, 1, value), row("f", 1, 2, 0)), 1, 1L).matches()
            );
        }
        assertEquals(Set.of(1, 2), descriptorReferences(sum, 3));
    }

    private static List<Map<String, Object>> controls() {
        List<Map<String, Object>> records = new ArrayList<>();
        for (Control control : literalControls()) {
            var result = SequenceRowProceduralOracle.evaluate(control.query(), control.rows(), control.bound(), control.limit());
            requireEqual(control.expectedMatches(), result.matches(), control.id() + ".matches");
            requireEqual(
                control.expectedSteps(),
                result.finalSearches().stream().map(SequenceRowProceduralOracle.FinalSearch::steps).toList(),
                control.id() + ".per_final_steps"
            );
            requireEqual(control.expectedAbandoned(), result.abandonedSearches(), control.id() + ".abandoned");
            if (control.expectedUsedAfter() != null) {
                requireEqual(control.expectedUsedAfter(), result.finalSearches().getLast().after().usedIds(), control.id() + ".used_after");
            }
            result.finalSearches()
                .stream()
                .filter(SequenceRowProceduralOracle.FinalSearch::abandoned)
                .forEach(SequenceRowProceduralOracleQualificationTests::requireNoMarks);
            records.add(
                fields(
                    "id",
                    control.id(),
                    "candidate_bound",
                    control.bound(),
                    "step_limit",
                    control.limit(),
                    "input_rows",
                    inputRows(control.rows()),
                    "actual",
                    actual(result),
                    "status",
                    "pass"
                )
            );
        }
        assertEquals(7, records.size());
        return records;
    }

    private static List<Control> literalControls() {
        // These histories and expectations were frozen outside this helper in FROZEN-CONTRACT.r2.json.
        var equality = equalityQuery(2, 10L);
        var noConditions = new SequenceRowProceduralOracle.Query(3, 10L, Map.of(), List.of());
        List<SequenceRowProceduralOracle.Row> boundary = List.of(row("a", 0, 1, 1), row("b", 1, 1, 0), row("f", 2, 2, 1));
        List<SequenceRowProceduralOracle.Row> emptyStart = List.of(row("b0", 0, 2, 0), row("b1", 1, 2, 0), row("f", 2, 3, 0));
        return List.of(
            new Control("last_allowed_success", equality, boundary, 2, 2L, List.of(List.of("a", "f")), List.of(2L), 0L, null),
            new Control("next_unused_abandons", equality, boundary, 2, 1L, List.of(), List.of(1L), 1L, Set.of()),
            new Control(
                "exhausted_at_budget_is_not_abandonment",
                equality,
                List.of(row("a", 0, 1, 0), row("f", 1, 2, 1)),
                1,
                1L,
                List.of(),
                List.of(1L),
                0L,
                Set.of()
            ),
            new Control(
                "used_marker_costs_no_step",
                equality,
                List.of(row("a", 0, 1, 1), row("f1", 1, 2, 1), row("b", 2, 1, 2), row("f2", 3, 2, 3)),
                2,
                1L,
                List.of(List.of("a", "f1")),
                List.of(1L, 1L),
                0L,
                null
            ),
            new Control("empty_start_still_counts_later_visits", noConditions, emptyStart, 16, 1L, List.of(), List.of(1L), 1L, Set.of()),
            new Control(
                "used_newest_marker_before_success",
                equality,
                List.of(row("a", 0, 1, 1), row("b", 1, 1, 2), row("f1", 2, 2, 2), row("f2", 3, 2, 1)),
                2,
                1L,
                List.of(List.of("b", "f1"), List.of("a", "f2")),
                List.of(1L, 1L),
                0L,
                null
            ),
            new Control("empty_start_exhausts_at_budget", noConditions, emptyStart, 16, 2L, List.of(), List.of(2L), 0L, Set.of())
        );
    }

    private static List<Map<String, Object>> hardNegativeFamily() {
        List<Map<String, Object>> records = new ArrayList<>();
        Map<Integer, Long> expectedSteps = Map.of(3, 272L, 4, 4368L, 6, 131_072L);
        for (int stages : List.of(3, 4, 6)) {
            for (boolean allValid : List.of(false, true)) {
                List<SequenceRowProceduralOracle.Row> history = new ArrayList<>();
                List<String> latest = new ArrayList<>();
                for (int stage = 1; stage < stages; stage++) {
                    for (int index = 0; index < 16; index++) {
                        history.add(row("s" + stage + "_" + index, (stage - 1L) * 16 + index, stage, 0));
                    }
                    latest.add("s" + stage + "_15");
                }
                history.add(row("f", (stages - 1L) * 16, stages, allValid ? 0 : 1));
                latest.add("f");
                var result = SequenceRowProceduralOracle.evaluate(equalityQuery(stages, 1_000_000L), history, 16, 131_072L);
                long expected = allValid ? stages - 1L : expectedSteps.get(stages);
                long abandoned = allValid == false && stages == 6 ? 1L : 0L;
                requireEqual(allValid ? List.of(latest) : List.of(), result.matches(), "hard-negative matches");
                requireEqual(expected, result.finalSearches().getFirst().steps(), "hard-negative steps");
                requireEqual(abandoned, result.abandonedSearches(), "hard-negative abandoned");
                requireEqual(0, result.droppedIds().size(), "hard-negative drops");
                if (abandoned != 0) {
                    requireNoMarks(result.finalSearches().getFirst());
                }
                records.add(
                    fields(
                        "id",
                        "hard-negative.k" + stages + (allValid ? ".all-valid" : ".no-match"),
                        "stages",
                        stages,
                        "candidate_bound",
                        16,
                        "window_nanos",
                        "1000000",
                        "step_limit",
                        131072,
                        "all_valid",
                        allValid,
                        "input_rows",
                        inputRows(history),
                        "actual",
                        actual(result),
                        "status",
                        "pass"
                    )
                );
            }
        }
        assertEquals(6, records.size());
        return records;
    }

    private static SequenceRowProceduralOracle.Query equalityQuery(int stages, Long window) {
        return new SequenceRowProceduralOracle.Query(
            stages,
            window,
            Map.of(),
            List.of(
                new SequenceRowProceduralOracle.Condition(
                    stages,
                    Set.of(1),
                    (own, earlier) -> equalScalar(own.value(), earlier.get(1).value())
                )
            )
        );
    }

    private static SequenceRowProceduralOracle.Row row(String id, long ordinal, int stage, Object value) {
        return new SequenceRowProceduralOracle.Row(id, new SequenceRowProceduralOracle.Ordinal(ordinal, ordinal), stage, KEY, 0, value);
    }

    private static void requireNoMarks(SequenceRowProceduralOracle.FinalSearch search) {
        assertTrue(search.abandoned());
        assertEquals(List.of(), search.match());
        assertEquals(search.before().retainedIds(), search.after().retainedIds());
        assertEquals(search.before().usedIds(), search.after().usedIds());
        assertEquals(search.before().unusedIds(), search.after().unusedIds());
        assertEquals(Set.of(), search.emittedIds());
        assertEquals(Set.of(), search.substituteIds());
        assertEquals(Set.of(), search.crossMarkedIds());
    }

    private static Map<String, Object> counters(SequenceRowProceduralOracle.Result result) {
        return fields(
            "steps",
            result.totalSteps(),
            "steps_max",
            result.maxSteps(),
            "abandoned",
            result.abandonedSearches(),
            "dropped",
            result.droppedIds().size(),
            "bound_keys",
            result.keysThatHitBound(),
            "max_fill",
            result.largestRingFill()
        );
    }

    private static Map<String, Object> actual(SequenceRowProceduralOracle.Result result) {
        List<Map<String, Object>> finals = new ArrayList<>();
        List<List<Object>> steps = new ArrayList<>();
        for (var search : result.finalSearches()) {
            steps.add(List.of(search.finalRow().label(), search.steps()));
            finals.add(
                fields(
                    "final_id",
                    search.finalRow().id(),
                    "final_label",
                    search.finalRow().label(),
                    "steps",
                    search.steps(),
                    "abandoned",
                    search.abandoned(),
                    "match",
                    search.match(),
                    "before",
                    snapshot(search.before()),
                    "after",
                    snapshot(search.after()),
                    "emitted_ids",
                    new ArrayList<>(search.emittedIds()),
                    "substitute_ids",
                    new ArrayList<>(search.substituteIds()),
                    "cross_marked_ids",
                    new ArrayList<>(search.crossMarkedIds())
                )
            );
        }
        return fields(
            "ordered_matches",
            result.matches(),
            "source_labels",
            labels(result),
            "canonical_labels",
            labels(result),
            "counters",
            counters(result),
            "per_final_steps",
            steps,
            "final_searches",
            finals,
            "dropped_ids",
            result.droppedIds(),
            "expired_ids",
            result.expiredIds()
        );
    }

    private static Map<String, Object> snapshot(SequenceRowProceduralOracle.Snapshot snapshot) {
        return fields(
            "retained_ids",
            new ArrayList<>(snapshot.retainedIds()),
            "used_ids",
            new ArrayList<>(snapshot.usedIds()),
            "unused_ids",
            new ArrayList<>(snapshot.unusedIds()),
            "latest_unused_start_id",
            snapshot.latestUnusedStart() == null ? null : snapshot.latestUnusedStart().id()
        );
    }

    private static Map<String, Object> abandonmentObservation(SequenceRowProceduralOracle.Result result) {
        List<SequenceRowProceduralOracle.FinalSearch> abandoned = result.finalSearches()
            .stream()
            .filter(SequenceRowProceduralOracle.FinalSearch::abandoned)
            .toList();
        assertEquals(1, abandoned.size());
        var search = abandoned.getFirst();
        requireNoMarks(search);
        return fields(
            "abandoned",
            result.abandonedSearches(),
            "abandoned_final_label",
            search.finalRow().label(),
            "abandoned_final_marks_nothing",
            true
        );
    }

    private static List<Map<String, Object>> inputRows(List<SequenceRowProceduralOracle.Row> rows) {
        return rows.stream()
            .map(
                row -> fields(
                    "id",
                    row.id(),
                    "timestamp_nanos",
                    Long.toString(row.ordinal().timestampNanos()),
                    "shard_doc",
                    row.ordinal().shardDoc(),
                    "stage",
                    row.stage(),
                    "key_utf8_hex",
                    row.key().hex(),
                    "arrival_rank",
                    row.arrivalRank(),
                    "value",
                    row.value()
                )
            )
            .toList();
    }

    private static List<List<Object>> labels(SequenceRowProceduralOracle.Result result) {
        return canonicalLabels(result.matches(), result.processingOrder());
    }

    private static List<List<Object>> canonicalLabels(Object orderedIds, List<SequenceRowProceduralOracle.Row> rows) {
        Map<String, Object> byId = new LinkedHashMap<>();
        rows.forEach(row -> byId.put(row.id(), row.label()));
        List<List<Object>> result = new ArrayList<>();
        for (Object item : list(orderedIds)) {
            List<Object> match = new ArrayList<>();
            for (Object id : list(item)) {
                if (byId.containsKey(text(id)) == false) {
                    throw new IllegalArgumentException("expected match names a row outside its input");
                }
                match.add(byId.get(text(id)));
            }
            result.add(match);
        }
        return result;
    }

    private static List<SequenceRowProceduralOracle.Row> rows(Map<String, Object> fixture) {
        List<SequenceRowProceduralOracle.Row> rows = new ArrayList<>();
        for (Object item : list(fixture.get("rows"))) {
            Map<String, Object> row = object(item);
            Map<String, Object> values = object(row.get("values"));
            if (values.size() != 1 || values.containsKey("v") == false) {
                throw new IllegalArgumentException("unsupported frozen value columns");
            }
            rows.add(
                new SequenceRowProceduralOracle.Row(
                    text(row.get("id")),
                    new SequenceRowProceduralOracle.Ordinal(
                        Long.parseLong(text(row.get("timestamp_nanos"))),
                        integer(row.get("shard_doc"))
                    ),
                    Math.toIntExact(integer(row.get("stage"))),
                    new SequenceRowProceduralOracle.Key(text(row.get("key_utf8_hex"))),
                    integer(row.get("arrival_rank")),
                    values.get("v"),
                    row.get("label")
                )
            );
        }
        return rows;
    }

    private static SequenceRowProceduralOracle.Query query(Map<String, Object> fixture) {
        Map<String, Object> input = object(fixture.get("query"));
        Map<Integer, Long> limits = new LinkedHashMap<>();
        object(input.get("link_limits_nanos")).forEach((stage, limit) -> limits.put(Integer.valueOf(stage), Long.parseLong(text(limit))));
        List<SequenceRowProceduralOracle.Condition> conditions = new ArrayList<>();
        for (Object item : list(input.get("conditions"))) {
            Map<String, Object> condition = object(item);
            int stage = Math.toIntExact(integer(condition.get("stage")));
            Set<Integer> references = new LinkedHashSet<>();
            list(condition.get("referenced_stages")).forEach(reference -> references.add(Math.toIntExact(integer(reference))));
            Map<String, Object> descriptor = object(condition.get("expression"));
            requireEqual(references, descriptorReferences(descriptor, stage), "original conjunct references");
            conditions.add(
                new SequenceRowProceduralOracle.Condition(stage, references, (own, earlier) -> expression(descriptor, own, earlier))
            );
        }
        Object window = input.get("window_nanos");
        return new SequenceRowProceduralOracle.Query(
            Math.toIntExact(integer(input.get("stages"))),
            window == null ? null : Long.parseLong(text(window)),
            limits,
            conditions
        );
    }

    private static Integer bound(Map<String, Object> fixture) {
        return fixture.get("candidate_bound") == null ? null : Math.toIntExact(integer(fixture.get("candidate_bound")));
    }

    private static Long stepLimit(Map<String, Object> fixture) {
        if (fixture.get("procedure") == null) {
            return null;
        }
        Map<String, Object> procedure = object(fixture.get("procedure"));
        if (Set.of("grouped-row-search", "single-value-search").contains(procedure.get("kind")) == false
            || procedure.containsKey("group") && "min".equals(procedure.get("group")) == false) {
            throw new IllegalArgumentException("unsupported frozen search procedure");
        }
        return procedure.get("step_limit") == null ? null : integer(procedure.get("step_limit"));
    }

    // This scalar descriptor interpreter is local test input plumbing. It shares no production expression/compiler code,
    // declarative helper code, candidate storage, search, or substitution code. F4A/F4B qualify its actual descriptor path.
    private static Object expression(
        Map<String, Object> descriptor,
        SequenceRowProceduralOracle.Row own,
        Map<Integer, SequenceRowProceduralOracle.Row> earlier
    ) {
        if (descriptor.containsKey("op")) {
            List<?> args = list(descriptor.get("args"));
            if (args.size() != 2 || descriptor.size() != 2) {
                throw new IllegalArgumentException("a scalar operation requires two arguments");
            }
            Object left = expression(object(args.get(0)), own, earlier);
            Object right = expression(object(args.get(1)), own, earlier);
            if (left == null || right == null || left instanceof List<?> || right instanceof List<?>) {
                return null;
            }
            return switch (text(descriptor.get("op"))) {
                case "eq" -> equalScalar(left, right);
                case "ne" -> equalScalar(left, right) == false;
                case "lt" -> compareScalar(left, right) < 0;
                case "add" -> decimal(left).add(decimal(right));
                default -> throw new IllegalArgumentException("unsupported frozen scalar operation");
            };
        }
        if (descriptor.containsKey("own")) {
            if (descriptor.size() != 1 || Boolean.TRUE.equals(descriptor.get("own")) == false) {
                throw new IllegalArgumentException("invalid own value descriptor");
            }
            return own.value();
        }
        if (descriptor.containsKey("own_field")) {
            if (descriptor.size() != 1) {
                throw new IllegalArgumentException("invalid own field descriptor");
            }
            return object(own.value()).get(descriptor.get("own_field"));
        }
        if (descriptor.containsKey("member")) {
            int reference = Math.toIntExact(integer(descriptor.get("member")));
            if (descriptor.size() != (descriptor.containsKey("field") ? 2 : 1)) {
                throw new IllegalArgumentException("invalid member descriptor");
            }
            Object value = Objects.requireNonNull(earlier.get(reference), "undeclared or unbound earlier reference").value();
            return descriptor.containsKey("field") ? object(value).get(descriptor.get("field")) : value;
        }
        throw new IllegalArgumentException("unsupported frozen scalar descriptor");
    }

    private static Set<Integer> descriptorReferences(Map<String, Object> descriptor, int ownStage) {
        Set<Integer> references = new LinkedHashSet<>();
        if (descriptor.containsKey("op")) {
            list(descriptor.get("args")).forEach(argument -> references.addAll(descriptorReferences(object(argument), ownStage)));
        } else if (descriptor.containsKey("member")) {
            int reference = Math.toIntExact(integer(descriptor.get("member")));
            if (reference < 1 || reference >= ownStage) {
                throw new IllegalArgumentException("a member descriptor must name an earlier stage");
            }
            references.add(reference);
        } else if (descriptor.containsKey("own") == false && descriptor.containsKey("own_field") == false) {
            throw new IllegalArgumentException("unsupported frozen descriptor references");
        }
        return references;
    }

    private static Boolean equalScalar(Object left, Object right) {
        if (left == null || right == null || left instanceof List<?> || right instanceof List<?>) {
            return null;
        }
        return left instanceof Number && right instanceof Number ? decimal(left).compareTo(decimal(right)) == 0 : left.equals(right);
    }

    private static int compareScalar(Object left, Object right) {
        if (left instanceof Number && right instanceof Number) {
            return decimal(left).compareTo(decimal(right));
        }
        if (left instanceof String a && right instanceof String b) {
            return a.compareTo(b);
        }
        throw new IllegalArgumentException("incomparable scalar literals");
    }

    private static Map<String, Object> resource(String name, String hash) throws Exception {
        try (
            InputStream input = Objects.requireNonNull(
                SequenceRowProceduralOracleQualificationTests.class.getResourceAsStream("/sequence/latest-valid/" + name)
            )
        ) {
            return pinnedJson(input.readAllBytes(), hash);
        }
    }

    private static Map<String, Object> pinnedJson(byte[] bytes, String hash) throws Exception {
        requireEqual(hash, sha256(bytes), "pinned JSON sha256");
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, bytes)) {
            return parser.mapOrdered();
        }
    }

    private static void emitRequestedReceipt(
        Map<String, Object> receipt,
        Map<String, Map<String, Object>> fixtures,
        List<Map<String, Object>> comparisons,
        List<Map<String, Object>> controls,
        List<Map<String, Object>> hardNegatives
    ) throws Exception {
        String output = System.getProperty("tests.sequence.procedural.receipt");
        boolean print = Boolean.parseBoolean(System.getProperty("tests.sequence.procedural.actuals", "false"));
        if (output == null && print == false) {
            return;
        }
        Path contractPath = Path.of(
            Objects.requireNonNull(
                System.getProperty("tests.sequence.procedural.contract"),
                "the frozen external contract path is required for a receipt"
            )
        );
        Map<String, Object> external = pinnedJson(Files.readAllBytes(contractPath), CONTRACT_HASH);
        checkExternalContract(external, fixtures, comparisons, controls, hardNegatives);
        Path sourceRoot = Path.of(
            Objects.requireNonNull(
                System.getProperty("tests.sequence.procedural.source_root"),
                "the exact qualified source root is required for a receipt"
            )
        ).toAbsolutePath().normalize();
        Map<String, Object> sourcePins = object(receipt.get("source_pins"));
        Map<String, String> javaSources = new LinkedHashMap<>();
        for (String name : List.of("SequenceRowProceduralOracle.java", "SequenceRowProceduralOracleQualificationTests.java")) {
            javaSources.put(SOURCE_PATH + name, sha256(Files.readAllBytes(sourceRoot.resolve(SOURCE_PATH + name))));
        }
        sourcePins.put("java_sources", javaSources);
        receipt.put("source_pins", sourcePins);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (XContentBuilder builder = new XContentBuilder(XContentType.JSON.xContent(), bytes)) {
            builder.map(receipt);
        }
        if (output != null) {
            Path ownedRoot = Path.of(
                Objects.requireNonNull(
                    System.getProperty("tests.sequence.procedural.receipt_root"),
                    "an explicit owned receipt root is required"
                )
            ).toAbsolutePath().normalize();
            Path path = Path.of(output).toAbsolutePath().normalize();
            if (path.startsWith(ownedRoot) == false || path.equals(ownedRoot) || ownedRoot.getParent() == null) {
                throw new IllegalArgumentException("receipt path must be below its explicit owned root");
            }
            Files.createDirectories(path.getParent());
            if (path.getParent().toRealPath().startsWith(ownedRoot.toRealPath()) == false) {
                throw new IllegalArgumentException("receipt parent escapes the explicit owned root");
            }
            Files.write(path, bytes.toByteArray(), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        }
        if (print) {
            System.out.println("SEQUENCE_PROCEDURAL_ACTUALS_JSON=" + bytes.toString(StandardCharsets.UTF_8));
        }
    }

    private static void checkExternalContract(
        Map<String, Object> external,
        Map<String, Map<String, Object>> fixtures,
        List<Map<String, Object>> comparisons,
        List<Map<String, Object>> controls,
        List<Map<String, Object>> hardNegatives
    ) {
        assertEquals(Boolean.TRUE, external.get("expected_not_from_engine"));
        requireEqual(
            COMPARISON_IDS,
            list(external.get("comparisons")).stream().map(item -> text(object(item).get("id"))).toList(),
            "external records"
        );
        for (Object item : list(external.get("fixtures"))) {
            Map<String, Object> fixture = object(item);
            requireEqual(fixture, fixtures.get(text(fixture.get("id"))), "unchanged external input fixture");
        }
        for (int index = 0; index < comparisons.size(); index++) {
            Map<String, Object> expected = object(list(external.get("comparisons")).get(index));
            Map<String, Object> actual = object(comparisons.get(index).get("actual"));
            if (expected.containsKey("expected_ordered_matches")) {
                requireEqual(expected.get("expected_ordered_matches"), actual.get("ordered_matches"), "external ordered matches");
            }
        }
        Map<String, Map<String, Object>> controlsById = new LinkedHashMap<>();
        controls.forEach(control -> controlsById.put(text(control.get("id")), control));
        assertEquals(list(external.get("literal_controls")).size(), controlsById.size());
        for (Object item : list(external.get("literal_controls"))) {
            Map<String, Object> expected = object(item);
            Map<String, Object> control = controlsById.get(text(expected.get("id")));
            Map<String, Object> actual = object(control.get("actual"));
            requireEqual(expected.get("step_limit"), control.get("step_limit"), "external control budget");
            requireEqual(expected.get("expected_matches"), actual.get("ordered_matches"), "external control matches");
            requireEqual(expected.get("expected_abandoned"), object(actual.get("counters")).get("abandoned"), "external control abandoned");
            requireEqual(
                expected.get("expected_per_final_steps"),
                list(actual.get("final_searches")).stream().map(search -> object(search).get("steps")).toList(),
                "external control steps"
            );
            if (expected.containsKey("expected_used_after")) {
                Map<String, Object> last = object(list(actual.get("final_searches")).getLast());
                requireEqual(expected.get("expected_used_after"), object(last.get("after")).get("used_ids"), "external control used after");
            }
        }
        Map<String, Object> family = object(external.get("hard_negative_family"));
        for (Object item : list(family.get("expected"))) {
            Map<String, Object> expected = object(item);
            for (Map<String, Object> record : hardNegatives) {
                if (Boolean.FALSE.equals(record.get("all_valid")) && integer(record.get("stages")) == integer(expected.get("stages"))) {
                    Map<String, Object> counters = object(object(record.get("actual")).get("counters"));
                    requireEqual(expected.get("steps"), counters.get("steps"), "external hard-negative steps");
                    requireEqual(expected.get("abandoned"), counters.get("abandoned"), "external hard-negative abandoned");
                }
            }
        }
    }

    private static void requireEqual(Object expected, Object actual, String field) {
        assertEquals(field, comparable(expected), comparable(actual));
    }

    private static Object comparable(Object value) {
        if (value instanceof Number) {
            return decimal(value).stripTrailingZeros();
        }
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> result = new LinkedHashMap<>();
            map.forEach((key, item) -> result.put(text(key), comparable(item)));
            return result;
        }
        if (value instanceof Set<?> set) {
            return set.stream().map(SequenceRowProceduralOracleQualificationTests::comparable).collect(java.util.stream.Collectors.toSet());
        }
        if (value instanceof List<?> list) {
            return list.stream().map(SequenceRowProceduralOracleQualificationTests::comparable).toList();
        }
        return value;
    }

    private static String sha256(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }

    private static Map<String, Object> fields(Object... pairs) {
        if (pairs.length % 2 != 0) {
            throw new IllegalArgumentException("key/value pairs required");
        }
        Map<String, Object> result = new LinkedHashMap<>();
        for (int index = 0; index < pairs.length; index += 2) {
            result.put(text(pairs[index]), pairs[index + 1]);
        }
        return result;
    }

    private static Map<String, Object> object(Object value) {
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> result = new LinkedHashMap<>();
            for (var entry : map.entrySet()) {
                if (entry.getKey() instanceof String key) {
                    result.put(key, entry.getValue());
                } else {
                    throw new IllegalArgumentException("test JSON object keys must be strings");
                }
            }
            return result;
        }
        throw new IllegalArgumentException("a test JSON object is required");
    }

    private static List<?> list(Object value) {
        if (value instanceof List<?> list) {
            return list;
        }
        throw new IllegalArgumentException("a test JSON list is required");
    }

    private static BigDecimal decimal(Object value) {
        if (value instanceof Number number) {
            return new BigDecimal(number.toString());
        }
        throw new IllegalArgumentException("a numeric test literal is required");
    }

    private static long integer(Object value) {
        return decimal(value).longValueExact();
    }

    private static String text(Object value) {
        return Objects.requireNonNull(value).toString();
    }
}
