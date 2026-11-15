/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

/**
 * Compares constructor-selected matching with the separately qualified row-declarative reference.
 * Values deliberately travel as canonical JSON in an owned KEYWORD cell. This exercises opaque matching values,
 * including nulls, lists and objects, rather than ES|QL expression compilation or native expression type semantics.
 * Reference admission is an external prerequisite for selecting this test, never inferred from engine agreement.
 */
public class SequenceLatestValidDifferentialTests extends ESTestCase {
    private static final String REGISTRY_HASH = "ac1800f14690e2385158ade03f0af23c5c9b5da3e5ebc512f8da32dde61a9133";
    private static final String GOLDEN_HASH = "1d3e43c1f2475f9c5d31f65aa8d4678793ca3e2dbd20280a3bbe1ffb6b598769";
    private static final String SPEC_HASH = "8013089be14ec3660cf9f21a0febbc821e1cd9c6766ab4abd1d0f30d79ef8d3b";
    private static final String PARENT_HASH = "63c1cc582e452f5a0e0421b9eec9bd7cf030ece4f242650d8aca52e2dbb18c11";
    private static final long GENERATOR_SEED = 0x53E0_2026_1114_0096L;
    private static final int GENERATED_HISTORIES = 96;
    private static final String ROW_GENERATOR_HASH = "19427ba40595b83b4de4424052ebaba280e61351bcbf177e36cd9f0e840a5446";
    private static final long RANDOM_SHAPE_SEED = 0x53E0_2026_1114_0300L;
    private static final int RANDOM_SHAPE_HISTORIES = 96;
    private static final List<Integer> CANDIDATE_BOUNDS = Arrays.asList(1, 2, 3, 16, null);
    private static final Comparator<InputRow> FEED_ORDER = Comparator.comparingLong(InputRow::nanos)
        .thenComparingLong(InputRow::document)
        .thenComparing(Comparator.comparingInt(InputRow::stage).reversed())
        .thenComparing(InputRow::keyHex)
        .thenComparingLong(InputRow::localRank);

    public void testRowKeyEncodingUsesOnlyBytesRefSlice() {
        String prefix = row("prefix", 0, 0, 1, "a", 0, null).keyHex();
        String withNull = row("with-null", 0, 0, 1, "a" + (char) 0, 0, null).keyHex();
        assertEquals("61", prefix);
        assertEquals("6100", withNull);
        assertNotEquals(prefix, withNull);
        assertEquals("6b31", row("key", 0, 0, 1, "k1", 0, null).keyHex());
    }

    /** This local literal can be selected independently without changing the externally frozen fixture registry. */
    public void testIncomingLinkSubstitutionDifferential() {
        History history = incomingLinkSubstitutionHistory();
        List<String> failures = new ArrayList<>();
        for (Integer candidateBound : CANDIDATE_BOUNDS) {
            List<List<String>> expected = candidateBound != null && candidateBound == 1
                ? List.of(List.of("a2", "b2", "c2"))
                : List.of(List.of("a", "b1", "c"), List.of("a2", "b2", "c2"));
            compare(
                "review-incoming-link.N" + candidateBound,
                "review-regression",
                history,
                candidateBound,
                List.of(fields("id", "literal-incoming-link", "expected_ordered_matches", expected)),
                null,
                failures
            );
        }
        assertTrue(String.join("\n", failures), failures.isEmpty());
    }

    /** Fixed expectations and generated inputs are read or constructed before their corresponding evaluations. */
    public void testAdmittedReferenceDifferential() throws Exception {
        Map<String, Object> registry = resource("frozen-registry.json", REGISTRY_HASH);
        Map<String, Object> golden = resource("nonrecursive-crossmark-golden.json", GOLDEN_HASH);
        // Freeze all campaign inputs and input-only coverage before either implementation is evaluated.
        List<History> planted = plantedHistories();
        List<History> randomized = randomizedHistories();
        History incomingLink = incomingLinkSubstitutionHistory();
        Map<String, Object> plantedCoverage = inputCoverage(planted);
        Map<String, Object> randomizedCoverage = inputCoverage(randomized);
        for (String feature : List.of(
            "histories_with_middle_stage_conditions",
            "histories_with_final_stage_conditions",
            "two_member_conjuncts",
            "member_member_ne_conjuncts",
            "member_add_conjuncts",
            "skip_predecessor_conjuncts",
            "histories_with_same_stage_key_expansion",
            "histories_with_multistage_documents",
            "histories_with_multiple_keys",
            "histories_with_multikey_documents",
            "histories_with_equal_timestamp_distinct_documents",
            "null_valued_rows",
            "list_valued_rows"
        )) {
            assertTrue("fixed-seed random input lacks " + feature, number(randomizedCoverage.get(feature)) > 0);
        }
        Map<String, Object> randomOperations = object(randomizedCoverage.get("expression_operation_occurrences"));
        for (String operation : List.of("eq", "ne", "lt", "add")) {
            assertTrue("fixed-seed random input lacks " + operation, number(randomOperations.get(operation)) > 0);
        }
        Map<String, Object> randomLinks = object(randomizedCoverage.get("link_limit_occurrences"));
        for (String link : List.of("1", "2", "4")) {
            assertTrue("fixed-seed random input lacks link " + link, number(randomLinks.get(link)) > 0);
        }
        List<Map<String, Object>> actuals = new ArrayList<>();
        List<String> failures = new ArrayList<>();
        Set<String> fixedIds = new LinkedHashSet<>();
        Set<String> proceduralIds = new LinkedHashSet<>();
        int fixedComparisons = 0;
        int frozenMatchExpectations = 0;
        for (Object item : array(registry.get("fixtures"))) {
            Map<String, Object> fixture = object(item);
            String id = string(fixture.get("id"));
            if (fixture.get("procedure") != null) {
                proceduralIds.add(id);
                continue;
            }
            assertTrue("duplicate fixed fixture " + id, fixedIds.add(id));
            History history = history(fixture);
            List<Map<String, Object>> declarations = new ArrayList<>();
            for (Object candidate : array(registry.get("comparisons"))) {
                Map<String, Object> comparison = object(candidate);
                if (id.equals(comparison.get("fixture"))) {
                    declarations.add(comparison);
                    if (comparison.containsKey("expected_ordered_matches")) {
                        frozenMatchExpectations++;
                    }
                }
            }
            actuals.add(compare(id, "fixed", history, bound(fixture), declarations, null, failures));
            fixedComparisons++;
        }
        // These are configuration counts, derived by the loop from the hash-pinned registry, not printed-block counts.
        assertEquals(80, array(registry.get("fixtures")).size());
        assertEquals(3, proceduralIds.size());
        assertEquals(77, fixedComparisons);
        assertEquals(90, frozenMatchExpectations);

        int supplementalComparisons = 0;
        History supplemental = history(golden);
        for (Object item : array(golden.get("configurations"))) {
            Map<String, Object> configuration = object(item);
            Map<String, Object> declaration = fields(
                "id",
                "supplemental-ordered-matches",
                "expected_ordered_matches",
                golden.get("expected_ordered_matches")
            );
            actuals.add(
                compare(
                    string(configuration.get("id")),
                    "supplemental",
                    supplemental,
                    bound(configuration),
                    List.of(declaration),
                    golden,
                    failures
                )
            );
            supplementalComparisons++;
        }
        assertEquals(2, supplementalComparisons);

        int reviewComparisons = 0;
        for (Integer candidateBound : CANDIDATE_BOUNDS) {
            List<List<String>> expected = candidateBound != null && candidateBound == 1
                ? List.of(List.of("a2", "b2", "c2"))
                : List.of(List.of("a", "b1", "c"), List.of("a2", "b2", "c2"));
            actuals.add(
                compare(
                    "review-incoming-link.N" + candidateBound,
                    "review-regression",
                    incomingLink,
                    candidateBound,
                    List.of(fields("id", "literal-incoming-link", "expected_ordered_matches", expected)),
                    null,
                    failures
                )
            );
            reviewComparisons++;
        }
        assertEquals(5, reviewComparisons);

        List<Map<String, Object>> generatedInputs = new ArrayList<>();
        int generatedComparisons = 0;
        int positiveComparisons = 0;
        int negativeComparisons = 0;
        for (int index = 0; index < planted.size(); index++) {
            History history = planted.get(index);
            generatedInputs.add(describe(history));
            for (Integer candidateBound : CANDIDATE_BOUNDS) {
                Map<String, Object> actual = compare(
                    "generated-" + index + ".N" + candidateBound,
                    "planted",
                    history,
                    candidateBound,
                    List.of(),
                    null,
                    failures
                );
                actuals.add(actual);
                if (Boolean.TRUE.equals(actual.get("positive_control_matched"))) {
                    positiveComparisons++;
                }
                if (Boolean.TRUE.equals(actual.get("negative_control_unmatched"))) {
                    negativeComparisons++;
                }
                generatedComparisons++;
            }
        }
        assertEquals(GENERATED_HISTORIES * 5, generatedComparisons);
        check(failures, "campaign", "positive controls", generatedComparisons, positiveComparisons);
        check(failures, "campaign", "negative controls", generatedComparisons, negativeComparisons);
        List<Map<String, Object>> randomizedInputs = new ArrayList<>();
        int randomizedComparisons = 0;
        for (int index = 0; index < randomized.size(); index++) {
            History history = randomized.get(index);
            randomizedInputs.add(describe(history));
            for (Integer candidateBound : CANDIDATE_BOUNDS) {
                actuals.add(
                    compare(
                        "random-shape-" + index + ".N" + candidateBound,
                        "random-shape",
                        history,
                        candidateBound,
                        List.of(),
                        null,
                        failures
                    )
                );
                randomizedComparisons++;
            }
        }
        assertEquals(RANDOM_SHAPE_HISTORIES * 5, randomizedComparisons);
        check(failures, "campaign", "total comparisons", 1044, actuals.size());
        Map<String, Object> receipt = fields(
            "schema",
            "esql.sequence.latest-valid.differential.v2",
            "spec_revision",
            3,
            "spec_sha256",
            SPEC_HASH,
            "qualified_parent_archive_sha256",
            PARENT_HASH,
            "portable_registry_sha256",
            REGISTRY_HASH,
            "portable_golden_sha256",
            GOLDEN_HASH,
            "generator_seed_hex",
            Long.toHexString(GENERATOR_SEED),
            "generator_seed_decimal",
            Long.toString(GENERATOR_SEED),
            "fixed_fixture_configurations",
            fixedComparisons,
            "fixed_fixture_ids",
            new ArrayList<>(fixedIds),
            "frozen_ordered_match_expectations",
            frozenMatchExpectations,
            "wave3_procedural_fixture_ids",
            new ArrayList<>(proceduralIds),
            "procedural_exclusion_reason",
            "Only procedure=null enters this declarative bridge; procedural contracts remain Wave 3",
            "supplemental_configurations",
            supplementalComparisons,
            "review_regression_configurations",
            reviewComparisons,
            "review_regression_input",
            describe(incomingLink),
            "generated_compatibility_scope",
            "generated_* keys retain the original planted campaign only; total_comparisons includes every category",
            "generated_histories",
            planted.size(),
            "generated_comparisons",
            generatedComparisons,
            "generated_positive_control_comparisons",
            positiveComparisons,
            "generated_negative_control_comparisons",
            negativeComparisons,
            "planted_histories",
            planted.size(),
            "planted_stage_variant_slots",
            24,
            "planted_slot_description",
            "96 instantiations of 24 stage-count/variant slots; structurally coincident slots are not distinct shapes",
            "planted_comparisons",
            generatedComparisons,
            "planted_input_coverage",
            plantedCoverage,
            "random_shape_histories",
            randomized.size(),
            "random_shape_comparisons",
            randomizedComparisons,
            "random_shape_seed_hex",
            Long.toHexString(RANDOM_SHAPE_SEED),
            "random_shape_seed_decimal",
            Long.toString(RANDOM_SHAPE_SEED),
            "random_shape_rng",
            "java.util.Random; distribution/shape port, not Python random.Random bit-identical output",
            "random_shape_source_sha256",
            ROW_GENERATOR_HASH,
            "random_shape_source",
            "rows_S.py:400-435 rand_rows_query/rand_rows; F4 caller stage choice [3,3,4]",
            "random_shape_stop_rule",
            "exactly 96 histories constructed before evaluation; no retries, rejection, or result feedback",
            "random_shape_input_coverage",
            randomizedCoverage,
            "legacy_total_comparisons",
            fixedComparisons + supplementalComparisons + generatedComparisons,
            "total_comparisons",
            actuals.size(),
            "unsupported_or_failed_cases",
            actuals.stream()
                .filter(item -> item.containsKey("error"))
                .map(item -> fields("id", item.get("id"), "error", item.get("error")))
                .toList(),
            "opaque_value_adapter",
            "one canonical JSON envelope per demanded KEYWORD cell; predicates decode actual owned blocks only",
            "oracle_admission",
            "external prerequisite; engine agreement does not qualify the reference",
            "generated_inputs",
            generatedInputs,
            "random_shape_inputs",
            randomizedInputs,
            "comparisons",
            actuals,
            "mismatches",
            failures
        );
        writeRequestedReceipt(receipt);
        assertTrue(String.join("\n", failures), failures.isEmpty());
    }

    private static Map<String, Object> compare(
        String id,
        String category,
        History history,
        Integer bound,
        List<Map<String, Object>> declarations,
        Map<String, Object> golden,
        List<String> failures
    ) {
        int before = failures.size();
        Map<String, Object> actual = fields(
            "id",
            id,
            "category",
            category,
            "candidate_bound",
            bound,
            "original_window_nanos",
            history.window(),
            "row_count",
            history.rows().size()
        );
        List<String> declarationIds = declarations.stream().map(item -> string(item.get("id"))).toList();
        actual.put("source_comparison_ids", declarationIds);
        try {
            List<InputRow> ordered = history.rows().stream().sorted(FEED_ORDER).toList();
            Set<String> requested = new LinkedHashSet<>();
            ordered.forEach(row -> requested.add(row.id()));
            var reference = SequenceRowDeclarativeOracle.evaluate(referenceQuery(history), referenceRows(history), bound, requested);
            List<String> feedIds = ordered.stream().map(InputRow::id).toList();
            check(
                failures,
                id,
                "reference processing order",
                feedIds,
                reference.processingOrder().stream().map(SequenceRowDeclarativeOracle.Row::id).toList()
            );
            if (history.declaredOrder() != null) {
                check(failures, id, "frozen processing order", history.declaredOrder(), feedIds);
            }
            long nativeWindow = nativeWindow(history);
            actual.put("native_window_nanos", nativeWindow);
            actual.put(
                "absent_window_equivalence",
                history.window() == null
                    ? "Long.MAX_VALUE nanos covers the complete finite history; full span checked with BigInteger"
                    : "not required"
            );
            CircuitBreaker breaker = breaker();
            BlockFactory blocks = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker).build();
            List<List<String>> nativeMatches = new ArrayList<>();
            Map<String, Object> requestedStates = new LinkedHashMap<>();
            int stateComparisons = 0;
            try {
                SequenceLatestValidConfig config = nativeConfig(history, bound);
                try (
                    SequenceLatestValidMatcher matcher = new SequenceLatestValidMatcher(
                        history.stages(),
                        TimeValue.timeValueNanos(nativeWindow),
                        breaker,
                        () -> false,
                        BytesRef::compareTo,
                        nativeGaps(history),
                        config
                    )
                ) {
                    for (InputRow row : ordered) {
                        try (
                            SequenceMatch match = matcher.match(
                                row.stage() - 1,
                                key(row),
                                ordinal(row),
                                new BytesRef(row.id()),
                                values(row, config, blocks, breaker)
                            )
                        ) {
                            if (match != null) {
                                // Copy all borrowed bytes into immutable IDs before releasing the match owner.
                                nativeMatches.add(match.handles().stream().map(BytesRef::utf8ToString).toList());
                            }
                        }
                        var expected = Objects.requireNonNull(reference.snapshots().get(row.id()), "unrepresented reference snapshot");
                        Map<String, Boolean> retained = nativeState(matcher, ordered, row.id(), id, failures);
                        check(failures, id, "retained after " + row.id(), expected.retainedIds(), retained.keySet());
                        Set<String> used = new LinkedHashSet<>();
                        Set<String> unused = new LinkedHashSet<>();
                        retained.forEach((identity, consumed) -> (consumed ? used : unused).add(identity));
                        Set<String> retainedUsed = new LinkedHashSet<>(expected.usedIds());
                        retainedUsed.retainAll(expected.retainedIds());
                        check(failures, id, "retained used after " + row.id(), retainedUsed, used);
                        check(failures, id, "retained unused after " + row.id(), expected.unusedIds(), unused);
                        check(failures, id, "bound drops after " + row.id(), (long) expected.droppedIds().size(), matcher.boundDrops());
                        check(failures, id, "expiry after " + row.id(), (long) expected.expiredIds().size(), matcher.expiredCandidates());
                        check(
                            failures,
                            id,
                            "retained count after " + row.id(),
                            expected.retainedIds().size(),
                            matcher.retainedCandidates()
                        );
                        check(
                            failures,
                            id,
                            "possible match after " + row.id(),
                            expected.latestUnusedStart() != null,
                            matcher.possibleMatch()
                        );
                        SequenceOrdinal latest = expected.latestUnusedStart() == null
                            ? null
                            : ordinal(expected.latestUnusedStart().ordinal());
                        check(failures, id, "latest unused start after " + row.id(), latest, matcher.latestUnusedStart());
                        if (golden != null) {
                            compareGoldenState(golden, row.id(), retained, matcher, id, failures, requestedStates);
                        }
                        stateComparisons++;
                    }
                    actual.put("final_bound_drops", matcher.boundDrops());
                    actual.put("final_expired_candidates", matcher.expiredCandidates());
                }
            } finally {
                check(failures, id, "released breaker bytes", 0L, breaker.getUsed());
            }
            check(failures, id, "ordered row IDs", reference.matches(), nativeMatches);
            for (Map<String, Object> declaration : declarations) {
                if (declaration.containsKey("expected_ordered_matches")) {
                    Object expected = declaration.get("expected_ordered_matches");
                    check(failures, id, "frozen reference IDs " + declaration.get("id"), expected, reference.matches());
                    check(failures, id, "frozen engine IDs " + declaration.get("id"), expected, nativeMatches);
                }
            }
            actual.put("expected_ordered_matches", reference.matches());
            actual.put("actual_ordered_matches", nativeMatches);
            actual.put("prefix_state_comparisons", stateComparisons);
            if (golden != null) {
                actual.put("requested_states", requestedStates);
                check(failures, id, "requested supplemental state count", 2, requestedStates.size());
            }
            if (category.equals("planted")) {
                actual.put("positive_control_matched", nativeMatches.stream().anyMatch(tuple -> tuple.getLast().equals("positive-final")));
                actual.put(
                    "negative_control_unmatched",
                    nativeMatches.stream().noneMatch(tuple -> tuple.getLast().equals("negative-final"))
                );
            }
        } catch (Exception | AssertionError failure) {
            failures.add(id + ": unrepresented or failed case: " + failure);
            actual.put("error", failure.toString());
        }
        actual.put("mismatch_count", failures.size() - before);
        actual.put("status", failures.size() == before ? "pass" : "fail");
        return actual;
    }

    private static Map<String, Boolean> nativeState(
        SequenceLatestValidMatcher matcher,
        List<InputRow> feed,
        String after,
        String id,
        List<String> failures
    ) {
        Map<String, Boolean> state = new LinkedHashMap<>();
        for (var row : matcher.snapshotForTests()) {
            InputRow source = feed.get(Math.toIntExact(row.arrivalRank()));
            check(failures, id, "rank/stage after " + after, source.stage() - 1, row.stage());
            check(failures, id, "rank/ordinal after " + after, ordinal(source), row.ordinal());
            check(failures, id, "rank/key after " + after, key(source), row.key());
            if (state.put(source.id(), row.used()) != null) {
                throw new IllegalArgumentException("duplicate retained row identity");
            }
        }
        return state;
    }

    private static void compareGoldenState(
        Map<String, Object> golden,
        String after,
        Map<String, Boolean> retained,
        SequenceLatestValidMatcher matcher,
        String id,
        List<String> failures,
        Map<String, Object> requestedStates
    ) {
        for (String point : List.of("after_first_match", "after_second_match")) {
            Map<String, Object> expected = object(golden.get(point));
            if (after.equals(expected.get("after_final_id")) == false) {
                continue;
            }
            Set<String> used = new LinkedHashSet<>();
            Set<String> unused = new LinkedHashSet<>();
            retained.forEach((identity, consumed) -> (consumed ? used : unused).add(identity));
            requestedStates.put(
                point,
                fields(
                    "after_final_id",
                    after,
                    "retained_flags",
                    retained,
                    "retained_used_ids",
                    new ArrayList<>(used),
                    "retained_unused_ids",
                    new ArrayList<>(unused),
                    "bound_drops",
                    matcher.boundDrops(),
                    "expired_candidates",
                    matcher.expiredCandidates(),
                    "possible_match",
                    matcher.possibleMatch()
                )
            );
            check(failures, id, point + " frozen used", new LinkedHashSet<>(array(expected.get("used_earlier_ids"))), used);
            check(failures, id, point + " frozen unused", new LinkedHashSet<>(array(expected.get("unused_earlier_ids"))), unused);
            check(failures, id, point + " frozen drops", number(expected.get("dropped")), matcher.boundDrops());
            check(failures, id, point + " frozen expiry", number(expected.get("expired")), matcher.expiredCandidates());
            if (expected.containsKey("flags")) {
                for (Object item : array(expected.get("flags"))) {
                    Map<String, Object> flag = object(item);
                    String row = string(flag.get("row_id"));
                    check(failures, id, point + " frozen ring " + row, flag.get("in_earlier_ring"), retained.containsKey(row));
                    check(failures, id, point + " frozen used " + row, flag.get("used"), Boolean.TRUE.equals(retained.get(row)));
                }
            }
        }
    }

    private static SequenceLatestValidConfig nativeConfig(History history, Integer bound) {
        Set<Integer> demanded = new LinkedHashSet<>();
        List<SequenceLatestValidConfig.PredicateGroup> groups = new ArrayList<>();
        for (Condition condition : history.conditions()) {
            Set<Integer> readMembers = new LinkedHashSet<>();
            ValueExpression expression = compile(condition.expression(), condition.stage(), demanded, readMembers);
            if (readMembers.equals(new LinkedHashSet<>(condition.references())) == false) {
                throw new IllegalArgumentException("condition references do not describe its actual member reads");
            }
            groups.add(
                new SequenceLatestValidConfig.PredicateGroup(
                    condition.stage() - 1,
                    condition.references().stream().sorted().map(stage -> stage - 1).toList(),
                    values -> Boolean.TRUE.equals(expression.evaluate(values))
                )
            );
        }
        List<List<SequenceLatestValidConfig.ValueDemand>> demands = new ArrayList<>();
        for (int stage = 1; stage <= history.stages(); stage++) {
            demands.add(
                demanded.contains(stage)
                    ? List.of(
                        new SequenceLatestValidConfig.ValueDemand(
                            new ReferenceAttribute(Source.EMPTY, null, "opaque_stage_" + stage, DataType.KEYWORD)
                        )
                    )
                    : List.of()
            );
        }
        // Declarative comparisons disable the work limit; production capped behavior is tested separately.
        return bound == null
            ? SequenceLatestValidConfig.unboundedForTests(demands, groups)
            : SequenceLatestValidConfig.boundedForTests(bound, Long.MAX_VALUE, demands, groups);
    }

    /** Compile the descriptor independently; the resulting function reads exclusively from SelectedValues blocks. */
    private static ValueExpression compile(Map<String, Object> descriptor, int own, Set<Integer> demanded, Set<Integer> readMembers) {
        if (descriptor.containsKey("op")) {
            List<?> arguments = array(descriptor.get("args"));
            if (descriptor.size() != 2 || arguments.size() != 2) {
                throw new IllegalArgumentException("binary expression required");
            }
            ValueExpression a = compile(object(arguments.get(0)), own, demanded, readMembers);
            ValueExpression b = compile(object(arguments.get(1)), own, demanded, readMembers);
            String operation = string(descriptor.get("op"));
            if (Set.of("eq", "ne", "lt", "add").contains(operation) == false) {
                throw new IllegalArgumentException("unrepresented operation " + operation);
            }
            return selected -> {
                Object left = a.evaluate(selected);
                Object right = b.evaluate(selected);
                if (left == null || right == null || left instanceof List<?> || right instanceof List<?>) {
                    return null;
                }
                if (operation.equals("add")) {
                    return decimal(left).add(decimal(right));
                }
                if (operation.equals("lt")) {
                    return left instanceof Number && right instanceof Number
                        ? decimal(left).compareTo(decimal(right)) < 0
                        : stringLiteral(left).compareTo(stringLiteral(right)) < 0;
                }
                boolean equal = left instanceof Number && right instanceof Number
                    ? decimal(left).compareTo(decimal(right)) == 0
                    : Objects.equals(left, right);
                return operation.equals("eq") == equal;
            };
        }
        int stage;
        String field;
        if (descriptor.containsKey("member")) {
            stage = Math.toIntExact(number(descriptor.get("member")));
            if (stage < 1 || stage >= own || descriptor.size() != (descriptor.containsKey("field") ? 2 : 1)) {
                throw new IllegalArgumentException("invalid member value read");
            }
            readMembers.add(stage);
            field = descriptor.containsKey("field") ? string(descriptor.get("field")) : null;
        } else if (descriptor.size() == 1 && (Boolean.TRUE.equals(descriptor.get("own")) || descriptor.containsKey("own_field"))) {
            stage = own;
            field = descriptor.containsKey("own_field") ? string(descriptor.get("own_field")) : null;
        } else {
            throw new IllegalArgumentException("unrepresented value expression " + descriptor);
        }
        demanded.add(stage);
        int selectedStage = stage - 1;
        return selected -> {
            Object value = decode(selected, selectedStage);
            return field == null ? value : object(value).get(field);
        };
    }

    private static Object decode(SequenceLatestValidConfig.SelectedValues selected, int stage) {
        BytesRefBlock block = (BytesRefBlock) selected.block(stage, 0);
        if (block.getPositionCount() != 1 || block.isNull(0) || block.getValueCount(0) != 1) {
            throw new IllegalArgumentException("opaque value must be one JSON KEYWORD cell");
        }
        BytesRef cell = block.getBytesRef(block.getFirstValueIndex(0), new BytesRef());
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(XContentParserConfiguration.EMPTY, Arrays.copyOfRange(cell.bytes, cell.offset, cell.offset + cell.length))
        ) {
            Map<String, Object> envelope = parser.mapOrdered();
            if (envelope.size() != 1 || envelope.containsKey("v") == false) {
                throw new IllegalArgumentException("invalid opaque value envelope");
            }
            return envelope.get("v");
        } catch (IOException failure) {
            throw new IllegalArgumentException("invalid carried JSON value", failure);
        }
    }

    private static SequenceCandidateValues values(
        InputRow row,
        SequenceLatestValidConfig config,
        BlockFactory blocks,
        CircuitBreaker breaker
    ) throws IOException {
        var demands = config.mandatoryDemands().get(row.stage() - 1);
        if (demands.isEmpty()) {
            return null;
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (XContentBuilder builder = new XContentBuilder(XContentType.JSON.xContent(), bytes)) {
            builder.startObject().field("v", canonical(row.value())).endObject();
        }
        // The caller owns this input page; copyRow supplies the independent owner transferred by match().
        try (Page page = new Page(blocks.newConstantBytesRefBlockWith(new BytesRef(bytes.toByteArray()), 1))) {
            return SequenceCandidateValues.copyRow(page, 0, demands, new int[] { 0 }, blocks, breaker);
        }
    }

    private static Object canonical(Object value) {
        if (value instanceof Map<?, ?>) {
            Map<String, Object> sorted = new TreeMap<>();
            object(value).forEach((key, item) -> sorted.put(key, canonical(item)));
            return sorted;
        }
        if (value instanceof List<?> items) {
            return items.stream().map(SequenceLatestValidDifferentialTests::canonical).toList();
        }
        return value;
    }

    private static SequenceRowDeclarativeOracle.Query referenceQuery(History history) {
        List<SequenceRowDeclarativeOracle.Condition> conditions = new ArrayList<>();
        for (Condition condition : history.conditions()) {
            conditions.add(
                new SequenceRowDeclarativeOracle.Condition(
                    condition.stage(),
                    new LinkedHashSet<>(condition.references()),
                    full -> SequenceRowDeclarativeOracle.expression(condition.expression(), condition.stage(), full)
                )
            );
        }
        return new SequenceRowDeclarativeOracle.Query(history.stages(), history.window(), history.links(), conditions);
    }

    private static List<SequenceRowDeclarativeOracle.Row> referenceRows(History history) {
        return history.rows()
            .stream()
            .map(
                row -> new SequenceRowDeclarativeOracle.Row(
                    row.id(),
                    new SequenceRowDeclarativeOracle.Ordinal(row.nanos(), row.document()),
                    row.stage(),
                    new SequenceRowDeclarativeOracle.Key(row.keyHex()),
                    row.localRank(),
                    row.value()
                )
            )
            .toList();
    }

    private static long nativeWindow(History history) {
        if (history.window() != null) {
            if (history.window() <= 0) {
                throw new IllegalArgumentException("zero/negative reference-only maxspan cannot be represented natively");
            }
            return history.window();
        }
        long minimum = history.rows().stream().mapToLong(InputRow::nanos).min().orElse(0);
        long maximum = history.rows().stream().mapToLong(InputRow::nanos).max().orElse(0);
        BigInteger span = BigInteger.valueOf(maximum).subtract(BigInteger.valueOf(minimum));
        if (span.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0) {
            throw new IllegalArgumentException("absent window lacks a faithful native finite-history representation");
        }
        return Long.MAX_VALUE;
    }

    private static long[] nativeGaps(History history) {
        if (history.links().isEmpty()) {
            return null;
        }
        long[] limits = new long[history.stages()];
        history.links().forEach((stage, limit) -> {
            if (stage < 2 || stage > history.stages() || limit <= 0) {
                throw new IllegalArgumentException("zero/negative reference-only link limit cannot be represented natively");
            }
            limits[stage - 1] = limit;
        });
        return limits;
    }

    private static SequenceOrdinal ordinal(InputRow row) {
        return new SequenceOrdinal(Math.floorDiv(row.nanos(), 1_000_000), Math.floorMod(row.nanos(), 1_000_000), row.document());
    }

    private static SequenceOrdinal ordinal(SequenceRowDeclarativeOracle.Ordinal ordinal) {
        return new SequenceOrdinal(
            Math.floorDiv(ordinal.timestampNanos(), 1_000_000),
            Math.floorMod(ordinal.timestampNanos(), 1_000_000),
            ordinal.shardDoc()
        );
    }

    private static BytesRef key(InputRow row) {
        return new BytesRef(HexFormat.of().parseHex(row.keyHex()));
    }

    private static History history(Map<String, Object> fixture) {
        Map<String, Object> query = object(fixture.get("query"));
        if (query.get("search_step_limit") != null) {
            throw new IllegalArgumentException("procedural limit passed to unlimited differential");
        }
        List<InputRow> rows = new ArrayList<>();
        for (Object item : array(fixture.get("rows"))) {
            Map<String, Object> row = object(item);
            Map<String, Object> values = object(row.get("values"));
            if (values.size() != 1 || values.containsKey("v") == false) {
                throw new IllegalArgumentException("unrepresented fixture value layout");
            }
            rows.add(
                new InputRow(
                    string(row.get("id")),
                    number(row.get("timestamp_nanos")),
                    number(row.get("shard_doc")),
                    Math.toIntExact(number(row.get("stage"))),
                    string(row.get("key_utf8_hex")),
                    number(row.get("arrival_rank")),
                    values.get("v")
                )
            );
        }
        List<Condition> conditions = new ArrayList<>();
        for (Object item : array(query.get("conditions"))) {
            Map<String, Object> condition = object(item);
            conditions.add(
                new Condition(
                    Math.toIntExact(number(condition.get("stage"))),
                    array(condition.get("referenced_stages")).stream().map(value -> Math.toIntExact(number(value))).toList(),
                    object(condition.get("expression"))
                )
            );
        }
        Map<Integer, Long> links = new LinkedHashMap<>();
        object(query.get("link_limits_nanos")).forEach((stage, limit) -> links.put(Integer.valueOf(stage), number(limit)));
        List<String> declared = array(fixture.get("processing_order")).stream().map(SequenceLatestValidDifferentialTests::string).toList();
        return new History(
            Math.toIntExact(number(query.get("stages"))),
            nullableNumber(query.get("window_nanos")),
            links,
            conditions,
            rows,
            declared
        );
    }

    /** Frozen review history is local evidence and does not modify either admitted external fixture resource. */
    private static History incomingLinkSubstitutionHistory() {
        return new History(
            3,
            10L,
            Map.of(2, 2L),
            List.of(new Condition(3, List.of(1), operation("eq", fields("own", true), fields("member", 1)))),
            List.of(
                row("a", 0, 10, 1, "k", 0, 1),
                row("b1", 1, 20, 2, "k", 0, 0),
                row("a2", 4, 30, 1, "k", 0, 2),
                row("b2", 5, 40, 2, "k", 0, 0),
                row("c", 6, 50, 3, "k", 0, 1),
                row("c2", 7, 60, 3, "k", 0, 2)
            ),
            null
        );
    }

    /**
     * Port the input distributions of pinned rows_S.py:400-435, using its F4 caller's [3,3,4] stage choice.
     * The Java RNG has its own fixed seed; it does not reproduce Python's RNG bit stream. In particular, Python's
     * lt predicate reverses its operands: {@code MEMBER(ref) < own}. Times are translated by a constant into submillisecond
     * nanoseconds; nondecreasing timestamps and the document tie breaker preserve the script's ordinal order.
     */
    private static List<History> randomizedHistories() {
        Random random = new Random(RANDOM_SHAPE_SEED);
        int[] stageChoices = { 3, 3, 4 };
        Long[] windowChoices = { null, 3L, 6L };
        long[] linkChoices = { 1L, 2L, 4L };
        int[] conjunctChoices = { 0, 1, 1, 2 };
        int[] timeChoices = { 0, 0, 1, 1, 2 };
        int[] expansionChoices = { 1, 1, 1, 2, 3 };
        List<History> histories = new ArrayList<>();
        for (int index = 0; index < RANDOM_SHAPE_HISTORIES; index++) {
            int stages = stageChoices[random.nextInt(stageChoices.length)];
            Long window = windowChoices[random.nextInt(windowChoices.length)];
            Map<Integer, Long> links = new LinkedHashMap<>();
            List<Condition> conditions = new ArrayList<>();
            for (int stage = 2; stage <= stages; stage++) {
                if (random.nextDouble() < 0.3) {
                    links.put(stage, linkChoices[random.nextInt(linkChoices.length)]);
                }
                int conjuncts = conjunctChoices[random.nextInt(conjunctChoices.length)];
                for (int conjunct = 0; conjunct < conjuncts; conjunct++) {
                    int kind = random.nextInt(stage >= 3 ? 5 : 3);
                    if (kind >= 3) {
                        int first = 1 + random.nextInt(stage - 1);
                        int second = 1 + random.nextInt(stage - 2);
                        if (second >= first) {
                            second++;
                        }
                        int a = Math.min(first, second);
                        int b = Math.max(first, second);
                        Map<String, Object> left = fields("member", a);
                        Map<String, Object> right = fields("member", b);
                        Map<String, Object> expression = kind == 3
                            ? operation("eq", fields("own", true), operation("add", left, right))
                            : operation("ne", left, right);
                        conditions.add(new Condition(stage, List.of(a, b), expression));
                    } else {
                        int reference = 1 + random.nextInt(stage - 1);
                        Map<String, Object> own = fields("own", true);
                        Map<String, Object> member = fields("member", reference);
                        Map<String, Object> expression = switch (kind) {
                            case 0 -> operation("eq", own, member);
                            case 1 -> operation("ne", own, member);
                            case 2 -> operation("lt", member, own);
                            default -> throw new AssertionError("unknown random predicate");
                        };
                        conditions.add(new Condition(stage, List.of(reference), expression));
                    }
                }
            }
            List<InputRow> rows = new ArrayList<>();
            int documents = 2 + random.nextInt(6);
            long nanos = -2_000_005L;
            for (int document = 0; document < documents; document++) {
                nanos += timeChoices[random.nextInt(timeChoices.length)];
                List<Integer> selectedStages = new ArrayList<>();
                if (random.nextDouble() < 0.3) {
                    for (int stage = 1; stage <= stages; stage++) {
                        if (random.nextDouble() < 0.5) {
                            selectedStages.add(stage);
                        }
                    }
                } else {
                    selectedStages.add(1 + random.nextInt(stages));
                }
                if (selectedStages.isEmpty()) {
                    selectedStages.add(1 + random.nextInt(stages));
                }
                for (int stage : selectedStages) {
                    int expansions = expansionChoices[random.nextInt(expansionChoices.length)];
                    for (int rank = 0; rank < expansions; rank++) {
                        Object value = switch (random.nextInt(6)) {
                            case 0 -> 0;
                            case 1 -> 1;
                            case 2 -> 2;
                            case 3 -> 3;
                            case 4 -> null;
                            case 5 -> List.of(0, 1);
                            default -> throw new AssertionError("unknown random literal");
                        };
                        String key = random.nextInt(4) == 3 ? "k2" : "k1";
                        rows.add(row("d" + document + ".s" + stage + "#" + rank, nanos, document, stage, key, rank, value));
                    }
                }
            }
            histories.add(new History(stages, window, links, conditions, rows, null));
        }
        return List.copyOf(histories);
    }

    /** Report observed input structure without consulting either implementation's matches or candidate state. */
    private static Map<String, Object> inputCoverage(List<History> histories) {
        Map<String, Long> stages = new TreeMap<>();
        Map<String, Long> windows = new TreeMap<>();
        Map<String, Long> links = new TreeMap<>();
        Map<String, Long> operations = new TreeMap<>();
        Set<Map<String, Object>> queryShapes = new LinkedHashSet<>();
        int middle = 0;
        int finals = 0;
        int twoMember = 0;
        int memberNe = 0;
        int memberAdd = 0;
        int skipPredecessor = 0;
        int expanded = 0;
        int multistage = 0;
        int multikey = 0;
        int sameDocumentMultikey = 0;
        int tiedDocuments = 0;
        int nulls = 0;
        int lists = 0;
        int negativeTimestamps = 0;
        int submillisecondTimestamps = 0;
        int rowCount = 0;
        int largestHistory = 0;
        for (History history : histories) {
            stages.merge(Integer.toString(history.stages()), 1L, Long::sum);
            windows.merge(String.valueOf(history.window()), 1L, Long::sum);
            history.links().values().forEach(limit -> links.merge(limit.toString(), 1L, Long::sum));
            middle += history.conditions().stream().anyMatch(condition -> condition.stage() < history.stages()) ? 1 : 0;
            finals += history.conditions().stream().anyMatch(condition -> condition.stage() == history.stages()) ? 1 : 0;
            for (Condition condition : history.conditions()) {
                twoMember += condition.references().size() > 1 ? 1 : 0;
                if (condition.references().size() > 1) {
                    memberNe += "ne".equals(condition.expression().get("op")) ? 1 : 0;
                    memberAdd += "eq".equals(condition.expression().get("op")) ? 1 : 0;
                }
                skipPredecessor += condition.references().stream().anyMatch(stage -> stage < condition.stage() - 1) ? 1 : 0;
                expressionCoverage(condition.expression(), operations);
            }
            Map<String, Object> queryShape = describe(history);
            queryShape.remove("rows");
            queryShapes.add(queryShape);
            Map<List<Object>, Set<Long>> expansionRanks = new LinkedHashMap<>();
            Map<List<Long>, Set<Integer>> documentStages = new LinkedHashMap<>();
            Map<List<Long>, Set<String>> documentKeys = new LinkedHashMap<>();
            Map<Long, Set<Long>> timestampDocuments = new LinkedHashMap<>();
            Set<String> keys = new LinkedHashSet<>();
            for (InputRow row : history.rows()) {
                List<Long> document = List.of(row.nanos(), row.document());
                expansionRanks.computeIfAbsent(
                    List.of(row.nanos(), row.document(), row.stage(), row.keyHex()),
                    ignored -> new LinkedHashSet<>()
                ).add(row.localRank());
                documentStages.computeIfAbsent(document, ignored -> new LinkedHashSet<>()).add(row.stage());
                documentKeys.computeIfAbsent(document, ignored -> new LinkedHashSet<>()).add(row.keyHex());
                timestampDocuments.computeIfAbsent(row.nanos(), ignored -> new LinkedHashSet<>()).add(row.document());
                keys.add(row.keyHex());
                nulls += row.value() == null ? 1 : 0;
                lists += row.value() instanceof List<?> ? 1 : 0;
                negativeTimestamps += row.nanos() < 0 ? 1 : 0;
                submillisecondTimestamps += Math.floorMod(row.nanos(), 1_000_000) != 0 ? 1 : 0;
            }
            expanded += expansionRanks.values().stream().anyMatch(ranks -> ranks.size() > 1) ? 1 : 0;
            multistage += documentStages.values().stream().anyMatch(values -> values.size() > 1) ? 1 : 0;
            multikey += keys.size() > 1 ? 1 : 0;
            sameDocumentMultikey += documentKeys.values().stream().anyMatch(values -> values.size() > 1) ? 1 : 0;
            tiedDocuments += timestampDocuments.values().stream().anyMatch(values -> values.size() > 1) ? 1 : 0;
            rowCount += history.rows().size();
            largestHistory = Math.max(largestHistory, history.rows().size());
        }
        return fields(
            "histories",
            histories.size(),
            "stage_count_histories",
            stages,
            "window_histories",
            windows,
            "link_limit_occurrences",
            links,
            "expression_operation_occurrences",
            operations,
            "distinct_query_descriptors",
            queryShapes.size(),
            "histories_with_middle_stage_conditions",
            middle,
            "histories_with_final_stage_conditions",
            finals,
            "two_member_conjuncts",
            twoMember,
            "member_member_ne_conjuncts",
            memberNe,
            "member_add_conjuncts",
            memberAdd,
            "skip_predecessor_conjuncts",
            skipPredecessor,
            "histories_with_same_stage_key_expansion",
            expanded,
            "histories_with_multistage_documents",
            multistage,
            "histories_with_multiple_keys",
            multikey,
            "histories_with_multikey_documents",
            sameDocumentMultikey,
            "histories_with_equal_timestamp_distinct_documents",
            tiedDocuments,
            "null_valued_rows",
            nulls,
            "list_valued_rows",
            lists,
            "negative_timestamp_rows",
            negativeTimestamps,
            "submillisecond_timestamp_rows",
            submillisecondTimestamps,
            "rows",
            rowCount,
            "largest_history_rows",
            largestHistory
        );
    }

    private static void expressionCoverage(Map<String, Object> expression, Map<String, Long> operations) {
        if (expression.containsKey("op")) {
            operations.merge(string(expression.get("op")), 1L, Long::sum);
            for (Object argument : array(expression.get("args"))) {
                expressionCoverage(object(argument), operations);
            }
        }
    }

    /** Planted stage/variant slots preserve mechanism coverage separately from the random-shape campaign. */
    private static List<History> plantedHistories() {
        Random random = new Random(GENERATOR_SEED);
        List<History> histories = new ArrayList<>();
        for (int index = 0; index < GENERATED_HISTORIES; index++) {
            int stages = 2 + index % 3;
            int variant = index % 8;
            List<InputRow> rows = new ArrayList<>();
            List<Condition> conditions = new ArrayList<>();
            boolean fields = variant == 7;
            Map<String, Object> own = fields ? fields("own_field", "a") : fields("own", true);
            Map<String, Object> member = fields ? fields("member", 1, "field", "a") : fields("member", 1);
            if (variant != 2 && variant != 5) {
                conditions.add(new Condition(stages, List.of(1), operation("eq", own, member)));
            }
            if (variant == 7 && stages >= 3) {
                conditions.add(
                    new Condition(
                        stages,
                        List.of(1, 2),
                        operation(
                            "eq",
                            fields("own_field", "b"),
                            operation("add", fields("member", 1, "field", "a"), fields("member", 2, "field", "a"))
                        )
                    )
                );
            }
            Long window = variant == 1 || variant == 7 ? null : 10L;
            Map<Integer, Long> links = new LinkedHashMap<>();
            if (variant == 4) {
                for (int stage = 2; stage <= stages; stage++) {
                    links.put(stage, 3L);
                }
            }
            long offset = -2_000_005L + random.nextInt(11);
            // Expanded siblings have distinct ranks across two conceptual branches at every earlier stage and final stage.
            for (int stage = 1; stage <= stages; stage++) {
                long timestamp = offset + (variant == 1 ? 0 : (stage - 1) * 2L);
                long document = 10 + stage;
                for (int rank = 0; rank < 3; rank++) {
                    rows.add(
                        row(
                            "expanded-s" + stage + "-branch" + (rank / 2) + "-r" + rank,
                            timestamp,
                            document,
                            stage,
                            "expanded",
                            rank,
                            literal(fields, rank == 0 ? 1 : 2)
                        )
                    );
                }
                if (stage > 1 && stage < stages) {
                    rows.add(row("cross-s" + stage + "-start-r0", timestamp, document, 1, "expanded", 0, literal(fields, 1)));
                    rows.add(row("cross-s" + stage + "-start-r1", timestamp, document, 1, "expanded", 1, literal(fields, 2)));
                }
            }
            // Null and multivalue literals are genuine carried inputs, not missing owners or expected-value lookups.
            rows.add(row("null-start", offset + 1, 80, 1, "nullable", 0, fields ? fields("a", null, "b", 2) : null));
            rows.add(row("multi-start", offset + 2, 81, 1, "nullable", 0, fields ? fields("a", List.of(1, 2), "b", 2) : List.of(1, 2)));
            for (int stage = 2; stage <= stages; stage++) {
                rows.add(row("nullable-s" + stage, offset + 2 + stage, 81 + stage, stage, "nullable", 0, literal(fields, 1)));
            }
            // Strict substitution: the second start lies after the first middle row and must survive its match.
            for (int stage = 1; stage <= stages; stage++) {
                rows.add(row("strict-first-s" + stage, offset + stage * 2L, 100 + stage, stage, "strict", 0, literal(fields, 1)));
            }
            rows.add(row("strict-later-start", offset + 5, 150, 1, "strict", 0, literal(fields, 1)));
            for (int stage = 2; stage <= stages; stage++) {
                rows.add(row("strict-second-s" + stage, offset + 7 + stage, 150 + stage, stage, "strict", 0, literal(fields, 1)));
            }
            // Inclusive link boundary followed by one nanosecond outside it; equal-time docs retain full shardDoc order.
            for (int stage = 1; stage <= stages; stage++) {
                rows.add(row("boundary-s" + stage, offset + (stage - 1) * 3L, 200 + stage, stage, "boundary", 0, literal(fields, 1)));
            }
            rows.add(row("outside-final", offset + (stages - 1) * 3L + 1, 220, stages, "boundary", 0, literal(fields, 1)));
            // Used markers, same-document self-drop at N=1, and eager expiry before a later insertion.
            rows.add(row("expiry-old", offset, 250, 1, "expiry", 0, literal(fields, 1)));
            rows.add(row("expiry-sibling", offset, 250, 1, "expiry", 1, literal(fields, 2)));
            rows.add(row("expiry-middle", offset + 5, 251, 1, "expiry", 0, literal(fields, 1)));
            rows.add(row("expiry-new", offset + 12, 252, 1, "expiry", 0, literal(fields, 1)));
            for (int stage = 2; stage <= stages; stage++) {
                rows.add(row("expiry-s" + stage, offset + 12 + stage, 252 + stage, stage, "expiry", 0, literal(fields, 1)));
            }
            for (int noise = 0; noise < 8; noise++) {
                int stage = 1 + random.nextInt(stages);
                Object value = switch (random.nextInt(4)) {
                    case 0 -> null;
                    case 1 -> List.of(0, 1);
                    case 2 -> 0;
                    case 3 -> 1;
                    default -> throw new AssertionError("unknown literal");
                };
                if (fields) {
                    value = fields("a", value, "b", 2);
                }
                rows.add(row("noise-" + noise, offset + random.nextInt(24), 300 + noise, stage, noise % 2 == 0 ? "\u00ff" : "z", 0, value));
            }
            // Two keys on one document also force unsigned byte key ordering within an otherwise identical feed position.
            for (String encodedKey : List.of("z", "\u00ff")) {
                for (int stage = 1; stage <= stages; stage++) {
                    rows.add(
                        row(
                            "shared-document-key-" + (encodedKey.equals("z") ? "ascii" : "high") + "-s" + stage,
                            offset + 30 + stage,
                            350 + stage,
                            stage,
                            encodedKey,
                            0,
                            literal(fields, 1)
                        )
                    );
                }
            }
            // Isolated controls guarantee both a positive final and a negative final for every N, independent of noise.
            for (int stage = 1; stage <= stages; stage++) {
                rows.add(
                    row(
                        stage == stages ? "positive-final" : "positive-s" + stage,
                        offset + 100 + stage,
                        400 + stage,
                        stage,
                        "positive",
                        0,
                        literal(fields, 1)
                    )
                );
                if (stage > 1) {
                    rows.add(
                        row(
                            stage == stages ? "negative-final" : "negative-s" + stage,
                            offset + 100 + stage,
                            500 + stage,
                            stage,
                            "negative",
                            0,
                            literal(fields, 1)
                        )
                    );
                }
            }
            histories.add(new History(stages, window, links, conditions, rows, null));
        }
        return List.copyOf(histories);
    }

    private static Object literal(boolean fields, int value) {
        return fields ? fields("a", value, "b", value * 2) : value;
    }

    private static Map<String, Object> operation(String name, Map<String, Object> a, Map<String, Object> b) {
        return fields("op", name, "args", List.of(a, b));
    }

    private static InputRow row(String id, long nanos, long document, int stage, String key, long rank, Object value) {
        BytesRef encoded = new BytesRef(key);
        return new InputRow(
            id,
            nanos,
            document,
            stage,
            HexFormat.of().formatHex(encoded.bytes, encoded.offset, encoded.offset + encoded.length),
            rank,
            value
        );
    }

    private static Map<String, Object> describe(History history) {
        List<Map<String, Object>> rows = history.rows()
            .stream()
            .map(
                row -> fields(
                    "id",
                    row.id(),
                    "timestamp_nanos",
                    row.nanos(),
                    "shard_doc",
                    row.document(),
                    "stage",
                    row.stage(),
                    "key_hex",
                    row.keyHex(),
                    "local_arrival_rank",
                    row.localRank(),
                    "value",
                    row.value()
                )
            )
            .toList();
        List<Map<String, Object>> conditions = history.conditions()
            .stream()
            .map(
                condition -> fields(
                    "stage",
                    condition.stage(),
                    "referenced_stages",
                    condition.references(),
                    "expression",
                    condition.expression()
                )
            )
            .toList();
        Map<String, Object> links = new LinkedHashMap<>();
        history.links().forEach((stage, limit) -> links.put(stage.toString(), limit));
        return fields(
            "stages",
            history.stages(),
            "window_nanos",
            history.window(),
            "link_limits_nanos",
            links,
            "conditions",
            conditions,
            "rows",
            rows
        );
    }

    private static CircuitBreaker breaker() {
        Settings settings = Settings.builder()
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "32mb")
            .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
            .build();
        return new HierarchyCircuitBreakerService(
            CircuitBreakerMetrics.NOOP,
            settings,
            List.of(),
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        ).getBreaker(CircuitBreaker.REQUEST);
    }

    private static Map<String, Object> resource(String name, String expectedHash) throws Exception {
        try (
            InputStream stream = Objects.requireNonNull(
                SequenceLatestValidDifferentialTests.class.getResourceAsStream("/sequence/latest-valid/" + name)
            )
        ) {
            byte[] bytes = stream.readAllBytes();
            String hash = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
            if (hash.equals(expectedHash) == false) {
                throw new IllegalArgumentException("fixture hash mismatch: " + name);
            }
            try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, bytes)) {
                return parser.mapOrdered();
            }
        }
    }

    private static void writeRequestedReceipt(Map<String, Object> receipt) throws IOException {
        String output = System.getProperty("tests.sequence.differential.receipt");
        if (output == null) {
            return;
        }
        if (Boolean.parseBoolean(System.getProperty("tests.sequence.differential.owned_vm")) == false) {
            throw new IllegalArgumentException("a requested receipt requires an explicit owned_vm property");
        }
        String sourceHash = Objects.requireNonNull(
            System.getProperty("tests.sequence.differential.source_manifest_sha256"),
            "a requested receipt requires its immutable source manifest hash"
        );
        if (sourceHash.matches("[0-9a-f]{64}") == false) {
            throw new IllegalArgumentException("invalid source manifest hash");
        }
        receipt.put("source_manifest_sha256", sourceHash);
        Path root = Path.of(
            Objects.requireNonNull(
                System.getProperty("tests.sequence.differential.receipt_root"),
                "an explicit owned receipt root is required"
            )
        ).toAbsolutePath().normalize();
        Path target = Path.of(output).toAbsolutePath().normalize();
        if (root.getParent() == null || target.equals(root) || target.startsWith(root) == false) {
            throw new IllegalArgumentException("receipt must be below its explicit owned root");
        }
        Files.createDirectories(target.getParent());
        if (target.getParent().toRealPath().startsWith(root.toRealPath()) == false) {
            throw new IllegalArgumentException("receipt parent escapes its explicit owned root");
        }
        try (
            XContentBuilder builder = new XContentBuilder(
                XContentType.JSON.xContent(),
                Files.newOutputStream(target, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)
            )
        ) {
            builder.map(receipt);
        }
    }

    private static void check(List<String> failures, String id, String part, Object expected, Object actual) {
        if (Objects.equals(expected, actual) == false) {
            failures.add(id + ": " + part + ": expected=" + expected + ", actual=" + actual);
        }
    }

    private static Map<String, Object> fields(Object... pairs) {
        if (pairs.length % 2 != 0) {
            throw new IllegalArgumentException("key/value pairs required");
        }
        Map<String, Object> result = new LinkedHashMap<>();
        for (int index = 0; index < pairs.length; index += 2) {
            result.put(string(pairs[index]), pairs[index + 1]);
        }
        return result;
    }

    private static Map<String, Object> object(Object value) {
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> result = new LinkedHashMap<>();
            map.forEach((key, item) -> {
                if (key instanceof String text) {
                    result.put(text, item);
                } else {
                    throw new IllegalArgumentException("object key must be a string");
                }
            });
            return result;
        }
        throw new IllegalArgumentException("expected object");
    }

    private static List<?> array(Object value) {
        if (value instanceof List<?> list) {
            return list;
        }
        throw new IllegalArgumentException("expected array");
    }

    private static long number(Object value) {
        return new BigDecimal(string(value)).longValueExact();
    }

    private static Long nullableNumber(Object value) {
        return value == null ? null : number(value);
    }

    private static Integer bound(Map<String, Object> fixture) {
        Object value = fixture.get("candidate_bound");
        return value == null ? null : Math.toIntExact(number(value));
    }

    private static BigDecimal decimal(Object value) {
        if (value instanceof Number number) {
            return new BigDecimal(number.toString());
        }
        throw new IllegalArgumentException("arithmetic requires a numeric value");
    }

    private static String stringLiteral(Object value) {
        if (value instanceof String text) {
            return text;
        }
        throw new IllegalArgumentException("ordering requires two numbers or two strings");
    }

    private static String string(Object value) {
        return Objects.requireNonNull(value).toString();
    }

    /** Input identity deliberately stays independent of production SequenceOrdinal and row state. */
    private record InputRow(String id, long nanos, long document, int stage, String keyHex, long localRank, Object value) {
        private InputRow {
            // Canonical lowercase hexadecimal string comparison is unsigned byte lexicographic comparison.
            keyHex = HexFormat.of().formatHex(HexFormat.of().parseHex(keyHex));
        }
    }

    /** Descriptor data, rather than decisions made by either matcher, defines both predicate implementations. */
    private record Condition(int stage, List<Integer> references, Map<String, Object> expression) {}

    /** Null windows and candidate bounds remain explicit in the reference-side input. */
    private record History(
        int stages,
        Long window,
        Map<Integer, Long> links,
        List<Condition> conditions,
        List<InputRow> rows,
        List<String> declaredOrder
    ) {}

    /** Compiled opaque-value test expressions borrow only the engine's current selected values. */
    @FunctionalInterface
    private interface ValueExpression {
        Object evaluate(SequenceLatestValidConfig.SelectedValues selected);
    }
}
