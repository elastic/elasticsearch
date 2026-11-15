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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

/** Fixed limits regressions whose matches, visits and counter vectors were frozen before implementation. */
public class SequenceLatestValidLimitsTests extends ESTestCase {
    private static final SequenceLatestValidConfig.ValueDemand VALUE = demand("v");
    private static final SequenceLatestValidConfig.ValueDemand OTHER_VALUE = demand("other");
    private static final long CONTRACT_TICK_MILLIS = 60_000;
    private static final long CONTRACT_WINDOW_MILLIS = 60_000_000;

    /** Stage ceilings constrain production configuration independently of assertions and release flags. */
    public void testProductionStageCeilingsAndNonPowerOfTwoBounds() {
        int[][] table = {
            { 2, 1024, 16 },
            { 3, 1024, 16 },
            { 4, 64, 16 },
            { 5, 32, 16 },
            { 6, 16, 16 },
            { 7, 8, 8 },
            { 8, 4, 4 },
            { 11, 4, 4 },
            { 12, 2, 2 },
            { 21, 2, 2 },
            { 22, 1, 1 },
            { 32, 1, 1 } };
        for (int[] row : table) {
            int stages = row[0];
            int ceiling = row[1];
            assertEquals(ceiling, SequenceLatestValidConfig.maximumCandidates(stages));
            SequenceLatestValidConfig defaults = SequenceLatestValidConfig.defaults(demands(stages, 0), List.of());
            assertEquals(row[2], defaults.maxCandidates().orElseThrow());
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
        for (int bound : new int[] { -1, 0 }) {
            expectThrows(IllegalArgumentException.class, () -> SequenceLatestValidConfig.validateCandidateBound(bound, 2));
            expectThrows(IllegalArgumentException.class, () -> SequenceLatestValidConfig.bounded(bound, demands(2, 0), List.of()));
        }
        assertEquals(16, SequenceLatestValidConfig.DEFAULT_MAX_CANDIDATES);
        assertEquals(1024, SequenceLatestValidConfig.MAX_CANDIDATES);
        assertEquals(20, SequenceLatestValidConfig.MAX_CANDIDATE_EXPONENT);
        assertEquals(131072L, SequenceLatestValidConfig.SEARCH_STEP_LIMIT);
        assertEquals(1024, SequenceLatestValidConfig.CANCELLATION_CHECK_INTERVAL);
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
            h.assertProcedure(List.of(oracleEqual(2, 1)));
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
            h.assertProcedure(List.of(oracleEqual(2, 1)));
            h.add(1, 3, 3, "f2", 0);
            assertEquals(List.of("b f2"), h.matches);
            assertEquals(List.of(false, true), h.used());
            assertOpen(h.owners.get("a"));
            assertClosed(h.owners.get("b"));
            assertClosed(h.owners.get("f2"));
            assertSearch(h, 2, 1, 1);
            h.assertProcedure(List.of(oracleEqual(2, 1)));
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
            h.assertProcedure(List.of(oracleEqual(2, 1)));
        }
        try (Harness h = new Harness(testConfig(2, 1, 2, 1, List.of(equal(1, 0))), 10)) {
            h.add(0, 0, 0, "a", 1);
            h.add(1, 1, 1, "f1", 1);
            h.add(0, 2, 2, "b", 2);
            h.add(1, 3, 3, "f2", 3);
            assertEquals(List.of("a f1"), h.matches);
            assertEquals(List.of(1L, 1L), h.perFinalSteps);
            assertSearch(h, 2, 1, 0);
            h.assertProcedure(List.of(oracleEqual(2, 1)));
        }
        try (Harness h = new Harness(testConfig(2, 1, 2, 1, List.of(equal(1, 0))), 10)) {
            h.add(0, 0, 0, "a", 1);
            h.add(0, 1, 1, "b", 2);
            h.add(1, 2, 2, "f1", 2);
            h.add(1, 3, 3, "f2", 1);
            assertEquals(List.of("b f1", "a f2"), h.matches);
            assertEquals(List.of(1L, 1L), h.perFinalSteps);
            assertSearch(h, 2, 1, 0);
            h.assertProcedure(List.of(oracleEqual(2, 1)));
        }
    }

    /** Missing starts cannot suppress visits to retained unused later-stage candidates. */
    public void testEmptyStartStillChargesLaterStageVisits() {
        for (long budget : new long[] { 1, 2 }) {
            try (Harness h = new Harness(testConfig(16, budget, 3, 0, List.of()), 10)) {
                h.add(1, 0, 0, "b0");
                h.add(1, 1, 1, "b1");
                assertFalse(h.matcher.possibleMatch());
                assertNull(h.matcher.latestUnusedStart());
                h.add(2, 2, 2, "f");
                assertEquals(List.of(), h.matches);
                assertEquals(List.of(false, false), h.used());
                assertEquals(List.of(budget), h.perFinalSteps);
                assertSearch(h, budget, budget, budget == 1 ? 1 : 0);
                h.assertProcedure(List.of());
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
            h.assertProcedure(
                List.of(
                    new SequenceRowProceduralOracle.Condition(
                        4,
                        Set.of(3),
                        (own, earlier) -> Objects.equals(((Map<?, ?>) own.value()).get("a"), ((Map<?, ?>) earlier.get(3).value()).get("a"))
                    ),
                    new SequenceRowProceduralOracle.Condition(
                        4,
                        Set.of(1),
                        (own, earlier) -> Objects.equals(((Map<?, ?>) own.value()).get("b"), ((Map<?, ?>) earlier.get(1).value()).get("b"))
                    )
                )
            );
        }
    }

    /** Frozen r2.F8 keeps the minute-to-nanosecond normalization and a positive nonbinding runtime window. */
    public void testF8UnlimitedPrefixAndLimitedCascade() {
        var groups = List.of(equal(1, 0), equal(2, 0));
        try (Harness h = new Harness(testConfig(380, Long.MAX_VALUE, 3, 1, groups), CONTRACT_WINDOW_MILLIS)) {
            f8Prefix(h);
            h.add(2, 731 * CONTRACT_TICK_MILLIS, 731, "731", 1);
            assertEquals(List.of("0 380 731"), h.matches);
            assertEquals(List.of(133731L), h.perFinalSteps);
            assertSearch(h, 133731, 133731, 0);
            assertEquals(0, h.matcher.expiredCandidates());
            h.assertProcedure(List.of(oracleEqual(2, 1), oracleEqual(3, 1)));
        }
        try (Harness h = new Harness(SequenceLatestValidConfig.bounded(380, demands(3, 1), groups), CONTRACT_WINDOW_MILLIS)) {
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
            h.assertProcedure(List.of(oracleEqual(2, 1), oracleEqual(3, 1)));
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
            h.assertProcedure(List.of(oracleEqual(2, 1), oracleEqual(3, 1)));
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
                        SequenceLatestValidConfig.bounded(16, demands(stages, 1), List.of(equal(stages - 1, 0))),
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
                    h.assertProcedure(List.of(oracleEqual(stages, 1)));
                }
            }
        }
    }

    /** Temporal rejection is charged before predicates; global eager expiry itself is not a search visit. */
    public void testIncomingGapRejectionIsChargedAndExpiryIsNot() {
        try (
            Harness h = new Harness(
                SequenceLatestValidConfig.bounded(3, demands(3, 1), List.of(equal(2, 0))),
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
            assertSearch(h, 4, 4, 0);
            assertEquals(1L, h.matcher.latestValidCounters().conditionEvaluations());
            h.assertProcedure(List.of(oracleEqual(3, 1)));
        }
        // Monotone input and eager expiry remove a window-rejected row before search can visit it.
        try (Harness h = new Harness(SequenceLatestValidConfig.bounded(2, demands(2, 1), List.of(equal(1, 0))), 10)) {
            h.add(0, 0, 0, "expired", 1);
            h.add(0, 10, 10, "a", 1);
            h.add(1, 11, 11, "f", 1);
            assertEquals(List.of("a f"), h.matches);
            assertSearch(h, 1, 1, 0);
            assertEquals(1L, h.matcher.latestValidCounters().conditionEvaluations());
            assertEquals(1, h.matcher.expiredCandidates());
            assertEquals(0, h.matcher.boundDrops());
            assertClosed(h.owners.get("expired"));
            h.assertProcedure(List.of(oracleEqual(2, 1)));
        }
    }

    /** The equal-ordinal siblings are consequence-only marks, not direct substitutes. */
    public void testCase6PrefixDirectSubstituteDiscriminator() {
        try (Harness h = new Harness(SequenceLatestValidConfig.bounded(3, demands(3, 1), List.of(equal(2, 0))), 10)) {
            h.add(0, 0, 0, "a", 1);
            h.add(1, 1, 1, "x_s2", 1);
            h.add(0, 1, 1, "x_s1#0", 1);
            h.add(0, 1, 1, "x_s1#1", 2);
            h.add(2, 2, 2, "c1", 1);
            assertEquals(List.of("a x_s2 c1"), h.matches);
            assertEquals(List.of(true, true, true, true), h.used());
            assertEquals(List.of(4L), h.perFinalSteps);
            assertCounters(h, 0, 0, 0, 4, 4, 1, 0, 3, 0);
            assertClosed(h.owners.get("x_s1#0"));
            assertClosed(h.owners.get("x_s1#1"));
            h.assertProcedure(List.of(oracleEqual(3, 1)));
        }
    }

    /** Direct substitutes count once, and already-used markers cannot increment the counter again. */
    public void testDirectSubstituteAndAlreadyUsedExclusion() {
        try (Harness h = new Harness(SequenceLatestValidConfig.bounded(3, demands(2, 1), List.of(equal(1, 0))), 10)) {
            h.add(0, 0, 0, "a", 1);
            h.add(0, 1, 1, "b", 1);
            h.add(0, 2, 2, "c", 2);
            h.add(1, 3, 3, "f1", 1);
            assertEquals(List.of("b f1"), h.matches);
            assertCounters(h, 0, 0, 0, 2, 2, 4, 2, 3, 1);
            assertClosed(h.owners.get("a"));
            assertClosed(h.owners.get("b"));
            assertOpen(h.owners.get("c"));
            h.assertProcedure(List.of(oracleEqual(2, 1)));
            h.add(1, 4, 4, "f2", 2);
            assertEquals(List.of("b f1", "c f2"), h.matches);
            assertEquals(List.of(true, true, true), h.used());
            assertEquals(List.of(2L, 1L), h.perFinalSteps);
            assertCounters(h, 0, 0, 0, 3, 2, 5, 2, 3, 1);
            h.assertProcedure(List.of(oracleEqual(2, 1)));
            var snapshot = h.matcher.latestValidCounters();
            h.matcher.close();
            assertEquals(snapshot, h.matcher.latestValidCounters());
        }
    }

    /** Largest fill describes one ring, while direct substitutes may come from several rings. */
    public void testMaximumIsOneRingNotKeyTotal() {
        try (Harness h = new Harness(SequenceLatestValidConfig.bounded(2, demands(3, 0), List.of()), 10)) {
            h.add(0, 0, 0, "a0");
            h.add(0, 1, 1, "a1");
            h.add(1, 2, 2, "b0");
            h.add(1, 3, 3, "b1");
            h.add(2, 4, 4, "f");
            assertEquals(List.of("a1 b1 f"), h.matches);
            assertEquals(List.of(true, true, true, true), h.used());
            assertCounters(h, 0, 0, 0, 2, 2, 0, 0, 2, 2);
            h.assertProcedure(List.of());
        }
    }

    /** Recreating a fully expired key starts a new bound-hit lifetime; merely filling a ring is not a hit. */
    public void testKeyLifetimeExpiresAndRecreates() {
        try (Harness h = new Harness(SequenceLatestValidConfig.bounded(1, demands(2, 0), List.of()), 10)) {
            h.add(0, 0, 0, "a");
            assertCounters(h, 0, 0, 0, 0, 0, 0, 0, 1, 0);
            h.add(0, 1, 1, "b");
            assertCounters(h, 1, 0, 1, 0, 0, 0, 0, 1, 0);
            h.addKey(0, 20, 20, "other", "other");
            h.add(0, 21, 21, "c");
            assertCounters(h, 1, 0, 1, 0, 0, 0, 0, 1, 0);
            h.add(0, 22, 22, "d");
            assertEquals(List.of(), h.matches);
            assertEquals(List.of(false, false), h.used());
            assertEquals(1, h.matcher.expiredCandidates());
            assertCounters(h, 2, 0, 2, 0, 0, 0, 0, 1, 0);
            h.assertProcedure(List.of());
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
                SequenceLatestValidConfig.bounded(16, demands(6, 1), List.of(predicate)),
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

    /** Legacy pairing has no optional latest-valid snapshot, including after closure. */
    public void testLegacyCounterSnapshotRemainsAbsent() {
        CircuitBreaker breaker = breaker();
        SequencePairing legacy = SequencePairing.legacy(2, TimeValue.timeValueMillis(10), breaker, () -> false, BytesRef::compareTo, null);
        try (legacy) {
            assertNull(legacy.latestValidCounters());
        }
        assertNull(legacy.latestValidCounters());
        assertEquals(0, breaker.getUsed());
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

    private static SequenceRowProceduralOracle.Condition oracleEqual(int own, int earlier) {
        return new SequenceRowProceduralOracle.Condition(
            own,
            Set.of(earlier),
            (row, previous) -> Objects.equals(row.value(), previous.get(earlier).value())
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
        long falseOrNull,
        long largestRing,
        long directSubstitutes
    ) {
        assertEquals(
            new SequenceSourceStatus.LatestValidCounters(
                dropped,
                abandoned,
                keys,
                steps,
                maximum,
                evaluations,
                falseOrNull,
                largestRing,
                directSubstitutes
            ),
            h.matcher.latestValidCounters()
        );
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
        private final long windowMillis;
        private final long[] gaps;
        private final Map<String, SequenceCandidateValues> owners = new LinkedHashMap<>();
        private final List<String> matches = new ArrayList<>();
        private final List<Long> perFinalSteps = new ArrayList<>();
        private final List<SequenceRowProceduralOracle.Row> history = new ArrayList<>();

        private Harness(SequenceLatestValidConfig config, long windowMillis) {
            this(config, windowMillis, null, () -> false);
        }

        private Harness(SequenceLatestValidConfig config, long windowMillis, long[] gaps, BooleanSupplier cancelled) {
            this.config = config;
            this.windowMillis = windowMillis;
            this.gaps = gaps == null ? null : gaps.clone();
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
            Object literal = switch (cells.length) {
                case 0 -> null;
                case 1 -> cells[0];
                case 2 -> Map.of("a", cells[0], "b", cells[1]);
                default -> throw new IllegalArgumentException("unsupported fixture demand count");
            };
            history.add(
                new SequenceRowProceduralOracle.Row(
                    label,
                    new SequenceRowProceduralOracle.Ordinal(Math.multiplyExact(millis, 1_000_000), doc),
                    stage + 1,
                    SequenceRowProceduralOracle.Key.utf8(key),
                    history.size(),
                    literal,
                    label
                )
            );
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

        /** Adapt only plain fixture inputs; the admitted oracle supplies its own matching and search procedure. */
        private void assertProcedure(List<SequenceRowProceduralOracle.Condition> conditions) {
            Map<Integer, Long> links = new LinkedHashMap<>();
            if (gaps != null) {
                for (int stage = 1; stage < gaps.length; stage++) {
                    if (gaps[stage] > 0) {
                        links.put(stage + 1, gaps[stage]);
                    }
                }
            }
            var query = new SequenceRowProceduralOracle.Query(
                config.mandatoryDemands().size(),
                Math.multiplyExact(windowMillis, 1_000_000),
                links,
                conditions
            );
            Integer bound = config.maxCandidates().isPresent() ? config.maxCandidates().getAsInt() : null;
            Long budget = config.searchStepLimit() == Long.MAX_VALUE ? null : config.searchStepLimit();
            String lastId = history.getLast().id();
            var expected = SequenceRowProceduralOracle.evaluate(query, List.copyOf(history), bound, budget, Set.of(lastId));
            assertEquals(expected.matches().stream().map(chain -> String.join(" ", chain)).toList(), matches);
            assertEquals(expected.finalSearches().stream().map(SequenceRowProceduralOracle.FinalSearch::steps).toList(), perFinalSteps);
            assertSearch(this, expected.totalSteps(), expected.maxSteps(), expected.abandonedSearches());
            assertEquals(expected.droppedIds().size(), matcher.latestValidCounters().candidatesDroppedAtBound());
            assertEquals(expected.expiredIds().size(), matcher.expiredCandidates());
            assertEquals(expected.largestRingFill(), matcher.latestValidCounters().largestRingFill());
            assertEquals(expected.keysThatHitBound(), matcher.latestValidCounters().keysThatHitBound());
            Set<String> retained = new HashSet<>();
            Set<String> used = new HashSet<>();
            for (var row : matcher.snapshotForTests()) {
                String id = history.get(Math.toIntExact(row.arrivalRank())).id();
                retained.add(id);
                if (row.used()) {
                    used.add(id);
                }
            }
            var snapshot = expected.snapshots().get(lastId);
            assertNotNull(snapshot);
            assertEquals(snapshot.retainedIds(), retained);
            assertEquals(snapshot.usedIds(), used);
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
