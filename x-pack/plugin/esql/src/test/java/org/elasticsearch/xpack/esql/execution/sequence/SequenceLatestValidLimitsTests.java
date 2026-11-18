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
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.Sequence;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.hamcrest.Matchers.containsString;

/** Fixed limits regressions whose matches, visits and counter vectors were frozen before implementation. */
public class SequenceLatestValidLimitsTests extends ESTestCase {
    private static final SequenceLatestValidConfig.ValueDemand VALUE = demand("v");
    private static final SequenceLatestValidConfig.ValueDemand OTHER_VALUE = demand("other");
    private static final long CONTRACT_TICK_MILLIS = 60_000;
    private static final long CONTRACT_WINDOW_MILLIS = 60_000_000;

    /** Stage ceilings constrain production configuration independently of assertions and release flags. */
    public void testProductionStageCeilingsAndNonPowerOfTwoBounds() {
        int[][] table = { { 2, 1024 }, { 3, 1024 }, { 4, 64 } };
        for (int[] row : table) {
            int stages = row[0];
            int ceiling = row[1];
            assertEquals(ceiling, SequenceLatestValidConfig.maximumCandidates(stages));
            SequenceLatestValidConfig defaults = SequenceLatestValidConfig.defaults(demands(stages, 0), List.of());
            assertEquals(8, defaults.maxCandidates().orElseThrow());
            assertEquals(131072L, defaults.searchStepLimit());
            defaults.validateForProduction(stages);
            SequenceLatestValidConfig.validateCandidateBound(ceiling, stages);
            SequenceLatestValidConfig.bounded(ceiling, demands(stages, 0), List.of()).validateForProduction(stages);
            expectThrows(IllegalArgumentException.class, () -> SequenceLatestValidConfig.validateCandidateBound(ceiling + 1, stages));
            expectThrows(
                IllegalArgumentException.class,
                () -> SequenceLatestValidConfig.bounded(ceiling + 1, demands(stages, 0), List.of())
            );
            for (int nonPowerOfTwo : new int[] { 3, 63, 380 }) {
                if (nonPowerOfTwo <= ceiling) {
                    SequenceLatestValidConfig.validateCandidateBound(nonPowerOfTwo, stages);
                    SequenceLatestValidConfig.bounded(nonPowerOfTwo, demands(stages, 0), List.of()).validateForProduction(stages);
                }
            }
        }
        for (int stages : new int[] { -1, 0, 1 }) {
            expectThrows(IllegalArgumentException.class, () -> SequenceLatestValidConfig.maximumCandidates(stages));
            expectThrows(IllegalArgumentException.class, () -> SequenceLatestValidConfig.validateCandidateBound(1, stages));
        }
        for (int stages : new int[] { 5, 6, 7, Sequence.MAX_STAGES }) {
            expectThrows(IllegalArgumentException.class, () -> SequenceLatestValidConfig.defaults(demands(stages, 0), List.of()));
            var overlong = SequenceLatestValidConfig.bounded(1, demands(stages, 0), List.of());
            assertThat(
                expectThrows(IllegalArgumentException.class, () -> overlong.validateForProduction(stages)).getMessage(),
                containsString("SEQUENCE supports at most [4] stages")
            );
        }
        for (int bound : new int[] { -1, 0 }) {
            expectThrows(IllegalArgumentException.class, () -> SequenceLatestValidConfig.validateCandidateBound(bound, 2));
            expectThrows(IllegalArgumentException.class, () -> SequenceLatestValidConfig.bounded(bound, demands(2, 0), List.of()));
        }
        assertEquals(8, SequenceLatestValidConfig.DEFAULT_MAX_CANDIDATES);
        assertEquals(1024, SequenceLatestValidConfig.MAX_CANDIDATES);
        assertEquals(20, SequenceLatestValidConfig.MAX_CANDIDATE_EXPONENT);
        assertEquals(131072L, SequenceLatestValidConfig.SEARCH_STEP_LIMIT);
    }

    /** The earlier-stage kept pools grow by one list of eight at each depth. */
    public void testEverySupportedDefaultFitsSearchStepLimit() {
        List<Long> visits = new ArrayList<>();
        for (int stages = 2; stages <= Sequence.MAX_EXECUTION_STAGES; stages++) {
            int candidates = SequenceLatestValidConfig.defaults(demands(stages, 0), List.of()).maxCandidates().orElseThrow();
            long steps = 0;
            long level = 1;
            for (int distance = 1; distance < stages; distance++) {
                level = Math.multiplyExact(level, (long) candidates * distance);
                steps = Math.addExact(steps, level);
            }
            visits.add(steps);
            assertTrue("default search for [" + stages + "] stages takes at most [" + steps + "] visits", steps < 131072);
        }
        assertEquals(List.of(8L, 136L, 3208L), visits);
    }

    /** Exhaust a default four-stage history with all three candidate lists full. */
    public void testLargestDefaultSearchDoesNotAbandon() {
        int stages = 4;
        var config = SequenceLatestValidConfig.defaults(demands(stages, 1), List.of(equal(stages - 1, 0))).withRememberedChains(true);
        try (Harness h = new Harness(config, CONTRACT_WINDOW_MILLIS)) {
            int candidates = config.maxCandidates().orElseThrow();
            for (int stage = 0; stage < stages - 1; stage++) {
                for (int row = 0; row < candidates; row++) {
                    long tick = stage * candidates + row;
                    h.add(stage, tick * CONTRACT_TICK_MILLIS, tick, "s" + stage + "_" + row, 0);
                }
            }
            long finalTick = (stages - 1L) * candidates;
            h.add(stages - 1, finalTick * CONTRACT_TICK_MILLIS, finalTick, "f", 1);
            assertEquals(List.of(), h.matches);
            assertEquals(List.of(584L), h.perFinalSteps);
            assertSearch(h, 584, 584, 0);
            assertEquals(0, h.matcher.boundDrops());
        }
    }

    /** Numerically identical test configuration must retain its explicit nonproduction provenance. */
    public void testTestProvenanceCannotPassOrdinaryProductionValidation() {
        List<List<SequenceLatestValidConfig.ValueDemand>> demands = demands(2, 1);
        var groups = List.of(equal(1, 0));
        SequenceLatestValidConfig unbounded = SequenceLatestValidConfig.unboundedForTests(demands, groups);
        assertTrue(unbounded.maxCandidates().isEmpty());
        expectThrows(IllegalArgumentException.class, () -> unbounded.validateForProduction(2));
        for (long budget : new long[] { 1, 131072, 131073, Long.MAX_VALUE }) {
            SequenceLatestValidConfig explicitTest = SequenceLatestValidConfig.boundedForTests(16, budget, demands, groups);
            assertEquals(16, explicitTest.maxCandidates().orElseThrow());
            assertEquals(budget, explicitTest.searchStepLimit());
            expectThrows(IllegalArgumentException.class, () -> explicitTest.validateForProduction(2));
        }
        for (long budget : new long[] { -1, 0 }) {
            expectThrows(IllegalArgumentException.class, () -> SequenceLatestValidConfig.boundedForTests(16, budget, demands, groups));
        }
        expectThrows(IllegalArgumentException.class, () -> SequenceLatestValidConfig.boundedForTests(0, 1, demands, groups));
        SequenceLatestValidConfig production = SequenceLatestValidConfig.bounded(16, demands, groups);
        production.validateForProduction(2);
        expectThrows(IllegalArgumentException.class, () -> production.validateForProduction(3));
    }

    /** Success on the last permitted unused visit is allowed; the next unused visit abandons without marks. */
    public void testLastAllowedSuccessAndAbandonmentPreserveOwners() {
        try (Harness h = new Harness(testConfig(2, 2, 2, 1, List.of(equal(1, 0))), 10)) {
            h.add(0, 0, 0, "a", 1);
            h.add(0, 1, 1, "b", 0);
            h.add(1, 2, 2, "f", 1);
            assertEquals(List.of("a f"), h.matches);
            assertEquals(List.of(2L), h.perFinalSteps);
            assertSearch(h, 2, 2, 0);
            assertEquals(List.of(true, false), h.used());

        }
        try (Harness h = new Harness(testConfig(2, 1, 2, 1, List.of(equal(1, 0))), 10)) {
            h.add(0, 0, 0, "a", 1);
            h.add(0, 1, 1, "b", 0);
            List<SequenceLatestValidMatcher.RowState> before = h.matcher.snapshotForTests();
            h.add(1, 2, 2, "f1", 1);
            assertEquals(List.of(), h.matches);
            assertEquals(List.of(1L), h.perFinalSteps);
            assertSearch(h, 1, 1, 1);
            assertEquals(before, h.matcher.snapshotForTests());
            assertOpen(h.owners.get("a"));
            assertOpen(h.owners.get("b"));
            assertClosed(h.owners.get("f1"));

            h.add(1, 3, 3, "f2", 0);
            assertEquals(List.of("b f2"), h.matches);
            assertEquals(List.of(false, true), h.used());
            assertOpen(h.owners.get("a"));
            assertClosed(h.owners.get("b"));
            assertClosed(h.owners.get("f2"));
            assertSearch(h, 2, 1, 1);

        }
    }

    /** Exhaustion at the budget is distinct from finding another unused candidate beyond that budget. */
    public void testExactExhaustionAndUsedMarkersDoNotAbandon() {
        try (Harness h = new Harness(testConfig(1, 1, 2, 1, List.of(equal(1, 0))), 10)) {
            h.add(0, 0, 0, "a", 0);
            h.add(1, 1, 1, "f", 1);
            assertEquals(List.of(), h.matches);
            assertEquals(List.of(false), h.used());
            assertEquals(List.of(1L), h.perFinalSteps);
            assertSearch(h, 1, 1, 0);

        }
        try (Harness h = new Harness(testConfig(2, 1, 2, 1, List.of(equal(1, 0))), 10)) {
            h.add(0, 0, 0, "a", 1);
            h.add(1, 1, 1, "f1", 1);
            h.add(0, 2, 2, "b", 2);
            h.add(1, 3, 3, "f2", 3);
            assertEquals(List.of("a f1"), h.matches);
            assertEquals(List.of(1L, 1L), h.perFinalSteps);
            assertSearch(h, 2, 1, 0);

        }
        try (Harness h = new Harness(testConfig(2, 1, 2, 1, List.of(equal(1, 0))), 10)) {
            h.add(0, 0, 0, "a", 1);
            h.add(0, 1, 1, "b", 2);
            h.add(1, 2, 2, "f1", 2);
            h.add(1, 3, 3, "f2", 1);
            assertEquals(List.of("b f1", "a f2"), h.matches);
            assertEquals(List.of(1L, 1L), h.perFinalSteps);
            assertSearch(h, 2, 1, 0);

        }
    }

    /** A middle row with no possible predecessor is discarded before the final search. */
    public void testEmptyStartStillChargesLaterStageVisits() {
        for (long budget : new long[] { 1, 2 }) {
            try (Harness h = new Harness(testConfig(16, budget, 3, 0, List.of()).withRememberedChains(true), 10)) {
                h.add(1, 0, 0, "b0");
                h.add(1, 1, 1, "b1");
                assertFalse(h.matcher.possibleMatch());
                assertNull(h.matcher.latestUnusedStart());
                h.add(2, 2, 2, "f");
                assertEquals(List.of(), h.matches);
                assertEquals(List.of(), h.used());
                assertEquals(List.of(0L), h.perFinalSteps);
                assertSearch(h, 0, 0, 0);
                assertAdmission(h, 2, 0, 0);

            }
        }
    }

    /** Frozen r2.F4B uses separate (own3,min2) and (own3,min0) predicate groups. */
    public void testF4BGroupedPredicatesUseSixtySixVisits() {
        var groups = List.of(
            new SequenceLatestValidConfig.PredicateGroup(3, List.of(2), rows -> integer(rows, 3, 0) == integer(rows, 2, 0)),
            new SequenceLatestValidConfig.PredicateGroup(3, List.of(0), rows -> integer(rows, 3, 1) == integer(rows, 0, 1))
        );
        try (Harness h = new Harness(SequenceLatestValidConfig.bounded(64, demands(4, 2), groups), CONTRACT_WINDOW_MILLIS)) {
            for (int stage = 0; stage < 3; stage++) {
                for (int row = 0; row < 64; row++) {
                    long tick = stage * 64L + row;
                    h.add(
                        stage,
                        tick * CONTRACT_TICK_MILLIS,
                        tick,
                        "s" + (stage + 1) + "_" + row,
                        stage == 2 && row == 0 ? 1 : 0,
                        stage < 2 ? 1 : 0
                    );
                }
            }
            h.add(3, 192 * CONTRACT_TICK_MILLIS, 192, "f", 1, 1);
            assertEquals(List.of("s1_63 s2_63 s3_0 f"), h.matches);
            assertEquals(List.of(66L), h.perFinalSteps);
            assertSearch(h, 66, 66, 0);
            assertEquals(0, h.matcher.expiredCandidates());
            assertEquals(0, h.matcher.boundDrops());
        }
    }

    /** Frozen r2.F8 keeps the minute-to-nanosecond normalization and a positive nonbinding runtime window. */
    public void testF8UnlimitedPrefixAndLimitedCascade() {
        var groups = List.of(equal(1, 0), equal(2, 0));
        try (Harness h = new Harness(testConfig(380, Long.MAX_VALUE, 3, 1, groups).withRememberedChains(true), CONTRACT_WINDOW_MILLIS)) {
            f8Prefix(h);
            h.add(2, 731 * CONTRACT_TICK_MILLIS, 731, "731", 1);
            assertEquals(List.of("0 380 731"), h.matches);
            assertEquals(List.of(381L), h.perFinalSteps);
            assertSearch(h, 381, 381, 0);
            assertEquals(0, h.matcher.expiredCandidates());
        }
        try (
            Harness h = new Harness(
                SequenceLatestValidConfig.bounded(380, demands(3, 1), groups).withRememberedChains(true),
                CONTRACT_WINDOW_MILLIS
            )
        ) {
            f8Prefix(h);
            var before = h.matcher.snapshotForTests();
            assertEquals(381, before.size());
            assertEquals(350L, h.matcher.admissionCounters().candidatesNotKept());
            h.add(2, 731 * CONTRACT_TICK_MILLIS, 731, "731", 1);
            assertEquals(List.of("0 380 731"), h.matches);
            assertEquals(List.of(381L), h.perFinalSteps);
            assertSearch(h, 381, 381, 0);

            var after = h.matcher.snapshotForTests();
            assertEquals(
                before.stream().map(SequenceLatestValidMatcher.RowState::arrivalRank).toList(),
                after.stream().map(SequenceLatestValidMatcher.RowState::arrivalRank).toList()
            );
            assertEquals(
                List.of(0L, 380L),
                after.stream()
                    .filter(SequenceLatestValidMatcher.RowState::used)
                    .map(SequenceLatestValidMatcher.RowState::arrivalRank)
                    .toList()
            );
            h.owners.forEach((label, owner) -> {
                int row = Integer.parseInt(label);
                if (row == 0 || row >= 380) {
                    assertClosed(owner);
                } else {
                    assertOpen(owner);
                }
            });

            h.add(1, 732 * CONTRACT_TICK_MILLIS, 732, "732", 1);
            h.add(2, 733 * CONTRACT_TICK_MILLIS, 733, "733", 1);
            assertEquals(List.of("0 380 731"), h.matches);
            assertEquals(List.of(381L, 0L), h.perFinalSteps);
            assertSearch(h, 381, 381, 0);
            assertEquals(351L, h.matcher.admissionCounters().candidatesNotKept());
            assertClosed(h.owners.get("732"));
            assertClosed(h.owners.get("733"));
            assertEquals(0, h.matcher.expiredCandidates());
            assertEquals(0, h.matcher.boundDrops());
        }
    }

    /** The old rule's exhaustion and replacement are the comparison control while both rules are available. */
    public void testLegacyF8UnlimitedPrefixAndLimitedCascade() {
        var groups = List.of(equal(1, 0), equal(2, 0));
        try (Harness h = new Harness(testConfig(380, Long.MAX_VALUE, 3, 1, groups).withRememberedChains(false), CONTRACT_WINDOW_MILLIS)) {
            f8Prefix(h);
            h.add(2, 731 * CONTRACT_TICK_MILLIS, 731, "731", 1);
            assertEquals(List.of("0 380 731"), h.matches);
            assertEquals(List.of(133731L), h.perFinalSteps);
            assertSearch(h, 133731, 133731, 0);
            assertEquals(0, h.matcher.expiredCandidates());

        }
        var config = SequenceLatestValidConfig.bounded(380, demands(3, 1), groups).withRememberedChains(false);
        try (Harness h = new Harness(config, CONTRACT_WINDOW_MILLIS)) {
            f8Prefix(h);
            List<SequenceLatestValidMatcher.RowState> before = h.matcher.snapshotForTests();
            h.add(2, 731 * CONTRACT_TICK_MILLIS, 731, "731", 1);
            assertEquals(List.of(), h.matches);
            assertEquals(List.of(131072L), h.perFinalSteps);
            assertSearch(h, 131072, 131072, 1);
            assertEquals(before, h.matcher.snapshotForTests());
            h.owners.forEach((label, owner) -> {
                if (label.equals("731")) {
                    assertClosed(owner);
                } else {
                    assertOpen(owner);
                }
            });

            h.add(1, 732 * CONTRACT_TICK_MILLIS, 732, "732", 1);
            h.add(2, 733 * CONTRACT_TICK_MILLIS, 733, "733", 1);
            assertEquals(List.of("0 732 733"), h.matches);
            assertEquals(1L, h.matcher.latestValidCounters().searchesAbandonedAtWorkLimit());
            assertClosed(h.owners.get("0"));
            assertClosed(h.owners.get("380"));
            assertClosed(h.owners.get("732"));
            assertClosed(h.owners.get("733"));
            assertEquals(0, h.matcher.expiredCandidates());
            assertEquals(0, h.matcher.boundDrops());

        }
    }

    /** Frozen hard negatives distinguish complete enumeration from the k6 work-limit cut-off. */
    public void testHardNegativesAndAllValidSiblings() {
        int[][] expected = { { 3, 272, 0 }, { 4, 4368, 0 }, { 6, 131072, 1 } };
        for (int[] row : expected) {
            int stages = row[0];
            for (boolean valid : List.of(false, true)) {
                try (
                    Harness h = new Harness(
                        SequenceLatestValidConfig.boundedForTests(
                            16,
                            SequenceLatestValidConfig.SEARCH_STEP_LIMIT,
                            demands(stages, 1),
                            List.of(equal(stages - 1, 0))
                        ),
                        1_000_000
                    )
                ) {
                    hardNegativePrefix(h, stages);
                    long tick = (stages - 1) * 16L;
                    h.add(stages - 1, tick, tick, "f", valid ? 0 : 1);
                    if (valid) {
                        List<String> chain = new ArrayList<>();
                        for (int stage = 1; stage < stages; stage++) {
                            chain.add("s" + stage + "_15");
                        }
                        chain.add("f");
                        assertEquals(List.of(String.join(" ", chain)), h.matches);
                        assertSearch(h, stages - 1, stages - 1, 0);
                    } else {
                        assertEquals(List.of(), h.matches);
                        assertSearch(h, row[1], row[1], row[2]);
                        assertTrue(h.used().stream().noneMatch(Boolean::booleanValue));
                    }
                    assertEquals(0, h.matcher.boundDrops());
                    assertEquals(0, h.matcher.expiredCandidates());

                }
            }
        }
    }

    /** Temporal rejection is charged before predicates; global eager expiry itself is not a search visit. */
    public void testIncomingGapRejectionIsChargedAndExpiryIsNot() {
        try (
            Harness h = new Harness(
                SequenceLatestValidConfig.bounded(3, demands(3, 1), List.of(equal(2, 0))).withRememberedChains(true),
                10,
                new long[] { 0, 2_000_000, 0 },
                () -> false
            )
        ) {
            h.add(0, 0, 0, "a", 1);
            h.add(1, 1, 1, "b1", 0);
            h.add(1, 4, 4, "b2", 0);
            h.add(2, 5, 5, "f", 1);
            assertEquals(List.of("a b1 f"), h.matches);
            assertSearch(h, 2, 2, 0);
            assertAdmission(h, 1, 2, 2);
            assertEquals(1L, h.matcher.latestValidCounters().conditionEvaluations());

        }
        // Monotone input and eager expiry remove a window-rejected row before search can visit it.
        var config = SequenceLatestValidConfig.bounded(2, demands(2, 1), List.of(equal(1, 0))).withRememberedChains(true);
        try (Harness h = new Harness(config, 10)) {
            h.add(0, 0, 0, "expired", 1);
            h.add(0, 10, 10, "a", 1);
            h.add(1, 11, 11, "f", 1);
            assertEquals(List.of("a f"), h.matches);
            assertSearch(h, 1, 1, 0);
            assertEquals(1L, h.matcher.latestValidCounters().conditionEvaluations());
            assertEquals(1, h.matcher.expiredCandidates());
            assertEquals(0, h.matcher.boundDrops());
            assertClosed(h.owners.get("expired"));

        }
    }

    /** Equal-ordinal sibling rows are marked after a match. */
    public void testCase6PrefixDirectSubstituteDiscriminator() {
        var config = SequenceLatestValidConfig.bounded(3, demands(3, 1), List.of(equal(2, 0))).withRememberedChains(true);
        try (Harness h = new Harness(config, 10)) {
            h.add(0, 0, 0, "a", 1);
            h.add(1, 1, 1, "x_s2", 1);
            h.add(0, 1, 1, "x_s1#0", 1);
            h.add(0, 1, 1, "x_s1#1", 2);
            h.add(2, 2, 2, "c1", 1);
            assertEquals(List.of("a x_s2 c1"), h.matches);
            assertEquals(List.of(true, true, true, true), h.used());
            assertEquals(List.of(4L), h.perFinalSteps);
            assertCounters(h, 0, 0, 0, 4, 4, 1, 3);
            assertAdmission(h, 0, 1, 4);
            assertClosed(h.owners.get("x_s1#0"));
            assertClosed(h.owners.get("x_s1#1"));

        }
    }

    /** Direct substitutes are marked and cannot be selected by a later final row. */
    public void testDirectSubstituteAndAlreadyUsedExclusion() {
        var config = SequenceLatestValidConfig.bounded(3, demands(2, 1), List.of(equal(1, 0))).withRememberedChains(true);
        try (Harness h = new Harness(config, 10)) {
            h.add(0, 0, 0, "a", 1);
            h.add(0, 1, 1, "b", 1);
            h.add(0, 2, 2, "c", 2);
            h.add(1, 3, 3, "f1", 1);
            assertEquals(List.of("b f1"), h.matches);
            assertCounters(h, 0, 0, 0, 2, 2, 4, 3);
            assertAdmission(h, 0, 0, 3);
            assertClosed(h.owners.get("a"));
            assertClosed(h.owners.get("b"));
            assertOpen(h.owners.get("c"));

            h.add(1, 4, 4, "f2", 2);
            assertEquals(List.of("b f1", "c f2"), h.matches);
            assertEquals(List.of(true, true, true), h.used());
            assertEquals(List.of(2L, 1L), h.perFinalSteps);
            assertCounters(h, 0, 0, 0, 3, 2, 5, 3);
            assertAdmission(h, 0, 0, 3);

            var snapshot = h.matcher.latestValidCounters();
            h.matcher.close();
            assertEquals(snapshot, h.matcher.latestValidCounters());
            assertAdmission(h, 0, 0, 3);
        }
    }

    /** Largest fill describes one ring, while substitutes may come from several rings. */
    public void testMaximumIsOneRingNotKeyTotal() {
        try (Harness h = new Harness(SequenceLatestValidConfig.bounded(2, demands(3, 0), List.of()).withRememberedChains(true), 10)) {
            h.add(0, 0, 0, "a0");
            h.add(0, 1, 1, "a1");
            h.add(1, 2, 2, "b0");
            h.add(1, 3, 3, "b1");
            h.add(2, 4, 4, "f");
            assertEquals(List.of("a1 b1 f"), h.matches);
            assertEquals(List.of(true, true, true, true), h.used());
            assertCounters(h, 0, 0, 0, 2, 2, 0, 2);
            assertAdmission(h, 0, 2, 4);

        }
    }

    /** Recreating a fully expired key starts a new bound-hit lifetime; merely filling a ring is not a hit. */
    public void testKeyLifetimeExpiresAndRecreates() {
        try (Harness h = new Harness(SequenceLatestValidConfig.bounded(1, demands(2, 0), List.of()).withRememberedChains(true), 10)) {
            h.add(0, 0, 0, "a");
            assertCounters(h, 0, 0, 0, 0, 0, 0, 1);
            assertAdmission(h, 0, 0, 1);
            h.add(0, 1, 1, "b");
            assertCounters(h, 1, 0, 1, 0, 0, 0, 1);
            assertAdmission(h, 0, 0, 1);
            h.addKey(0, 20, 20, "other", "other");
            h.add(0, 21, 21, "c");
            assertCounters(h, 1, 0, 1, 0, 0, 0, 1);
            assertAdmission(h, 0, 0, 1);
            h.add(0, 22, 22, "d");
            assertEquals(List.of(), h.matches);
            assertEquals(List.of(false, false), h.used());
            assertEquals(1, h.matcher.expiredCandidates());
            assertCounters(h, 2, 0, 2, 0, 0, 0, 1);
            assertAdmission(h, 0, 0, 1);

        }
    }

    /** Cancellation is measured from an actual predicate callback midway through the k6 hard negative. */
    public void testCancellationWithinOneThousandTwentyFourAdditionalChargedVisits() {
        AtomicBoolean cancelled = new AtomicBoolean();
        AtomicInteger calls = new AtomicInteger();
        AtomicLong armedAt = new AtomicLong(-1);
        AtomicReference<SequenceLatestValidMatcher> matcher = new AtomicReference<>();
        var predicate = new SequenceLatestValidConfig.PredicateGroup(5, List.of(0), rows -> {
            if (calls.incrementAndGet() == 64) {
                armedAt.set(matcher.get().latestValidCounters().searchSteps());
                cancelled.set(true);
            }
            return false;
        });
        try (
            Harness h = new Harness(
                SequenceLatestValidConfig.boundedForTests(
                    16,
                    SequenceLatestValidConfig.SEARCH_STEP_LIMIT,
                    demands(6, 1),
                    List.of(predicate)
                ),
                1_000_000,
                null,
                cancelled::get
            )
        ) {
            matcher.set(h.matcher);
            hardNegativePrefix(h, 6);
            expectThrows(TaskCancelledException.class, () -> h.add(5, 80, 80, "f", 1));
            assertTrue(armedAt.get() > 0);
            long additional = h.matcher.latestValidCounters().searchSteps() - armedAt.get();
            assertTrue("additional charged visits: " + additional, additional >= 0 && additional <= 1024);
            assertEquals(0L, h.matcher.latestValidCounters().searchesAbandonedAtWorkLimit());
            h.assertAllOwnersClosed();
            assertEquals(0, h.breaker.getUsed());
        }
    }

    /** Zero-cost markers still provide cancellation opportunities during a long scan. */
    public void testCancellationDuringUsedMarkerScan() {
        AtomicBoolean armed = new AtomicBoolean();
        AtomicInteger checks = new AtomicInteger();
        var group = new SequenceLatestValidConfig.PredicateGroup(1, List.of(0), rows -> integer(rows, 0, 0) == 1);
        var config = SequenceLatestValidConfig.boundedForTests(8192, 131072, List.of(List.of(VALUE), List.of()), List.of(group));
        try (Harness h = new Harness(config, 10, null, () -> armed.get() && checks.incrementAndGet() >= 3)) {
            h.add(0, 0, 0, "unused", 0);
            for (int row = 1; row <= 4096; row++) {
                h.add(0, 0, row, "marker" + row, 1);
            }
            h.add(1, 1, 4097, "f1");
            assertEquals(4096, h.used().stream().filter(Boolean::booleanValue).count());
            assertOpen(h.owners.get("unused"));
            armed.set(true);
            expectThrows(TaskCancelledException.class, () -> h.add(1, 2, 4098, "f2"));
            assertEquals(1L, h.matcher.latestValidCounters().searchSteps());
            assertEquals(0L, h.matcher.latestValidCounters().searchesAbandonedAtWorkLimit());
            h.assertAllOwnersClosed();
            assertEquals(0, h.breaker.getUsed());
        }
    }

    /** Predicate callbacks arm cancellation after search succeeds and substitution has started. */
    public void testCancellationDuringSubstitution() {
        AtomicBoolean cancelled = new AtomicBoolean();
        AtomicInteger evaluations = new AtomicInteger();
        var predicate = new SequenceLatestValidConfig.PredicateGroup(1, List.of(0), rows -> {
            if (evaluations.incrementAndGet() == 2) {
                cancelled.set(true);
            }
            return true;
        });
        try (Harness h = new Harness(testConfig(4096, 131072, 2, 1, List.of(predicate)), 10, null, cancelled::get)) {
            for (int row = 0; row < 2048; row++) {
                h.add(0, 0, row, "a" + row, 1);
            }
            expectThrows(TaskCancelledException.class, () -> h.add(1, 1, 2048, "f", 1));
            assertTrue(evaluations.get() >= 2);
            assertEquals(1L, h.matcher.latestValidCounters().searchSteps());
            assertEquals(0L, h.matcher.latestValidCounters().searchesAbandonedAtWorkLimit());
            h.assertAllOwnersClosed();
            assertEquals(0, h.breaker.getUsed());
        }
    }

    /** Closing the first expired real values owner arms cancellation while further expired rows remain. */
    public void testCancellationDuringExpiry() {
        AtomicBoolean armed = new AtomicBoolean();
        AtomicReference<SequenceCandidateValues> first = new AtomicReference<>();
        try (Harness h = new Harness(testConfig(4096, 131072, 2, 1, List.of()), 5, null, () -> armed.get() && isClosed(first.get()))) {
            for (int row = 0; row < 2048; row++) {
                h.add(0, 0, row, "a" + row, 1);
            }
            first.set(h.owners.get("a0"));
            armed.set(true);
            expectThrows(TaskCancelledException.class, () -> h.add(1, 10, 2048, "f", 1));
            assertTrue(h.matcher.expiredCandidates() >= 1);
            assertTrue(h.matcher.expiredCandidates() < 2048);
            assertSearch(h, 0, 0, 0);
            h.assertAllOwnersClosed();
            assertEquals(0, h.breaker.getUsed());
        }
    }

    private static void f8Prefix(Harness h) {
        for (int tick = 0; tick < 380; tick++) {
            h.add(0, tick * CONTRACT_TICK_MILLIS, tick, Integer.toString(tick), tick == 0 ? 1 : 0);
        }
        for (int tick = 380; tick < 731; tick++) {
            h.add(1, tick * CONTRACT_TICK_MILLIS, tick, Integer.toString(tick), tick == 380 ? 1 : 2);
        }
    }

    private static void hardNegativePrefix(Harness h, int stages) {
        for (int stage = 0; stage < stages - 1; stage++) {
            for (int row = 0; row < 16; row++) {
                long tick = stage * 16L + row;
                h.add(stage, tick, tick, "s" + (stage + 1) + "_" + row, 0);
            }
        }
    }

    private static SequenceLatestValidConfig.ValueDemand demand(String name) {
        return new SequenceLatestValidConfig.ValueDemand(new ReferenceAttribute(Source.EMPTY, null, name, DataType.INTEGER));
    }

    private static List<List<SequenceLatestValidConfig.ValueDemand>> demands(int stages, int slots) {
        List<List<SequenceLatestValidConfig.ValueDemand>> result = new ArrayList<>();
        for (int stage = 0; stage < stages; stage++) {
            result.add(switch (slots) {
                case 0 -> List.of();
                case 1 -> List.of(VALUE);
                case 2 -> List.of(VALUE, OTHER_VALUE);
                default -> throw new IllegalArgumentException("unsupported fixture demand count");
            });
        }
        return List.copyOf(result);
    }

    private static SequenceLatestValidConfig testConfig(
        int bound,
        long budget,
        int stages,
        int slots,
        List<SequenceLatestValidConfig.PredicateGroup> groups
    ) {
        return SequenceLatestValidConfig.boundedForTests(bound, budget, demands(stages, slots), groups);
    }

    private static SequenceLatestValidConfig.PredicateGroup equal(int own, int earlier) {
        return new SequenceLatestValidConfig.PredicateGroup(
            own,
            List.of(earlier),
            rows -> integer(rows, own, 0) == integer(rows, earlier, 0)
        );
    }

    private static int integer(SequenceLatestValidConfig.SelectedValues rows, int stage, int slot) {
        IntBlock block = (IntBlock) rows.block(stage, slot);
        return block.getInt(block.getFirstValueIndex(0));
    }

    private static void assertSearch(Harness h, long steps, long maximum, long abandoned) {
        var counters = h.matcher.latestValidCounters();
        assertNotNull(counters);
        assertEquals(steps, counters.searchSteps());
        assertEquals(maximum, counters.maxSearchStepsPerFinalRow());
        assertEquals(abandoned, counters.searchesAbandonedAtWorkLimit());
    }

    private static void assertCounters(
        Harness h,
        long dropped,
        long abandoned,
        long keys,
        long steps,
        long maximum,
        long evaluations,
        long largestRing
    ) {
        var counters = h.matcher.latestValidCounters();
        assertEquals(dropped, counters.candidatesDroppedAtBound());
        assertEquals(abandoned, counters.searchesAbandonedAtWorkLimit());
        assertEquals(keys, counters.keysThatHitBound());
        assertEquals(steps, counters.searchSteps());
        assertEquals(maximum, counters.maxSearchStepsPerFinalRow());
        assertEquals(evaluations, counters.conditionEvaluations());
        assertEquals(largestRing, counters.largestRingFill());
    }

    private static void assertAdmission(Harness h, long rejected, long steps, long peak) {
        assertEquals(new SequenceLatestValidMatcher.AdmissionCounters(rejected, 0, steps, 0, peak), h.matcher.admissionCounters());
    }

    private static void assertOpen(SequenceCandidateValues values) {
        assertNotNull(values);
        assertTrue(values.size() > 0);
    }

    private static void assertClosed(SequenceCandidateValues values) {
        assertNotNull(values);
        expectThrows(IllegalStateException.class, values::size);
    }

    private static boolean isClosed(SequenceCandidateValues values) {
        try {
            values.size();
            return false;
        } catch (IllegalStateException e) {
            return true;
        }
    }

    private static CircuitBreaker breaker() {
        Settings settings = Settings.builder()
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "64mb")
            .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
            .build();
        return new HierarchyCircuitBreakerService(
            CircuitBreakerMetrics.NOOP,
            settings,
            List.of(),
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        ).getBreaker(CircuitBreaker.REQUEST);
    }

    /** Real mandatory blocks and payloads let every scenario verify retained and transferred ownership. */
    private static final class Harness implements Releasable {
        private final CircuitBreaker breaker = breaker();
        private final BlockFactory blocks = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker).build();
        private final SequenceLatestValidConfig config;
        private final SequenceLatestValidMatcher matcher;
        private final Map<String, SequenceCandidateValues> owners = new LinkedHashMap<>();
        private final List<String> matches = new ArrayList<>();
        private final List<Long> perFinalSteps = new ArrayList<>();

        private Harness(SequenceLatestValidConfig config, long windowMillis) {
            this(config, windowMillis, null, () -> false);
        }

        private Harness(SequenceLatestValidConfig config, long windowMillis, long[] gaps, BooleanSupplier cancelled) {
            this.config = config;
            matcher = new SequenceLatestValidMatcher(
                config.mandatoryDemands().size(),
                TimeValue.timeValueMillis(windowMillis),
                breaker,
                cancelled,
                BytesRef::compareTo,
                gaps,
                config
            );
        }

        private SequenceCandidateValues values(int stage, Integer[] cells) {
            var demands = config.mandatoryDemands().get(stage);
            assertEquals(demands.size(), cells.length);
            if (cells.length == 0) {
                return null;
            }
            Block[] row = new Block[cells.length];
            boolean transferred = false;
            try {
                int[] channels = new int[cells.length];
                for (int slot = 0; slot < cells.length; slot++) {
                    row[slot] = cells[slot] == null ? blocks.newConstantNullBlock(1) : blocks.newConstantIntBlockWith(cells[slot], 1);
                    channels[slot] = slot;
                }
                try (Page page = new Page(row)) {
                    transferred = true;
                    return SequenceCandidateValues.copyRow(page, 0, demands, channels, blocks, breaker);
                }
            } finally {
                if (transferred == false) {
                    Releasables.close(row);
                }
            }
        }

        private void add(int stage, long millis, long doc, String label, Integer... cells) {
            addKey(stage, millis, doc, "k", label, cells);
        }

        private void addKey(int stage, long millis, long doc, String key, String label, Integer... cells) {
            SequenceCandidateValues values = values(stage, cells);
            if (values != null) {
                assertNull(owners.put(label, values));
            }
            long before = matcher.latestValidCounters().searchSteps();
            try (
                SequenceMatch match = matcher.match(
                    stage,
                    new BytesRef(key),
                    new SequenceOrdinal(millis, 0, doc),
                    new BytesRef(label),
                    values
                )
            ) {
                if (match != null) {
                    matches.add(String.join(" ", match.handles().stream().map(BytesRef::utf8ToString).toList()));
                }
                if (stage == config.mandatoryDemands().size() - 1) {
                    perFinalSteps.add(matcher.latestValidCounters().searchSteps() - before);
                }
            }
        }

        private List<Boolean> used() {
            return matcher.snapshotForTests().stream().map(SequenceLatestValidMatcher.RowState::used).toList();
        }

        private void assertAllOwnersClosed() {
            owners.values().forEach(SequenceLatestValidLimitsTests::assertClosed);
        }

        @Override
        public void close() {
            matcher.close();
            matcher.close();
            assertAllOwnersClosed();
            assertEquals(0, breaker.getUsed());
        }
    }
}
