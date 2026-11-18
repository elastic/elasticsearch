/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

/** Literal semantic regressions and resource checks; expected matches do not come from another implementation. */
public class SequenceLatestValidMatcherTests extends ESTestCase {
    private static final SequenceLatestValidConfig.ValueDemand VALUE = new SequenceLatestValidConfig.ValueDemand(
        new ReferenceAttribute(Source.EMPTY, null, "value", DataType.INTEGER)
    );
    private static final List<Integer> BOUNDS = List.of(1, 2, 3, 8, 16, Integer.MAX_VALUE);

    /** Frozen retired two-stage history: no condition, both ordinary and per-link time windows. */
    public void testTwoStageMatchesKeepFrozenRetiredMembersAndOrderAtEveryBound() {
        for (int bound : BOUNDS) {
            for (long[] gaps : new long[][] { null, { 0, 4_000_000 } }) {
                try (Harness h = new Harness(config(bound, 2, false, List.of()), 30, gaps, () -> false)) {
                    h.add(0, 0, 0, "k", "superseded", null);
                    h.add(0, 1, 1, "k", "a", null);
                    h.add(1, 2, 2, "k", "b", null);
                    h.add(1, 3, 3, "k", "used-start", null);
                    h.add(0, 4, 4, "k", "c", null);
                    h.add(0, 5, 5, "other", "foreign", null);
                    h.add(1, 6, 6, "k", "d", null);
                    assertEquals(List.of("a b", "c d"), h.matches);
                }
            }
        }
    }

    /** Specification P7: different installers remain available for their separate final events. */
    public void testP7DifferentInstallersProduceTwoMatches() {
        try (Harness h = new Harness(config(8, 3, true, List.of(equal(1, 0))), 30, null, () -> false)) {
            h.add(0, 0, 0, "k", "dl_A", 1);
            h.add(0, 1, 1, "k", "dl_B", 2);
            h.add(1, 2, 2, "k", "msi_A", 1);
            h.add(1, 3, 3, "k", "msi_B", 2);
            h.add(2, 4, 4, "k", "file_1", 0);
            h.add(2, 5, 5, "k", "file_2", 0);
            assertEquals(List.of("dl_B msi_B file_1", "dl_A msi_A file_2"), h.matches);
        }
    }

    /** Specification P6: substitutable downloads and executions produce one match even with two finals. */
    public void testP6SameInstallerWithBothFinalsProducesOneMatch() {
        try (Harness h = new Harness(config(8, 3, true, List.of(equal(1, 0))), 30, null, () -> false)) {
            h.add(0, 0, 0, "k", "dl_1", 1);
            h.add(0, 1, 1, "k", "dl_2", 1);
            h.add(1, 2, 2, "k", "msi_1", 1);
            h.add(1, 3, 3, "k", "msi_2", 1);
            h.add(2, 4, 4, "k", "file_1", 0);
            h.add(2, 5, 5, "k", "file_2", 0);
            assertEquals(List.of("dl_2 msi_2 file_1"), h.matches);
        }
    }

    /** A listed middle row preserves the only earlier start when the start leaves its list. */
    public void testEightLaterStartsEvictTheOnlyCompleteChainAtDefaultBound() {
        for (int bound : List.of(8, 9)) {
            try (Harness h = new Harness(config(bound, 3, false, List.of()), 30, null, () -> false)) {
                h.add(0, 0, 0, "k", "a", null);
                h.add(1, 1, 1, "k", "b", null);
                for (int later = 2; later <= 9; later++) {
                    h.add(0, later, later, "k", "later-" + later, null);
                }
                h.add(2, 10, 10, "k", "c", null);
                assertEquals(List.of("a b c"), h.matches);
                assertEquals(0, h.matcher.boundDrops());
            }
        }
    }

    public void testSameOrdinalFirstArrivalIsNewestAndCanSelfDrop() {
        for (int bound : BOUNDS) {
            try (Harness h = new Harness(config(bound, 2, false, List.of()), 10, null, () -> false)) {
                h.add(0, 0, 10, "k", "a#0", null);
                h.add(0, 0, 10, "k", "branch-sibling#0", null);
                h.add(1, 1, 20, "k", "f", null);
                assertEquals(List.of("a#0 f"), h.matches);
                assertEquals(bound == 1 ? 1 : 0, h.matcher.boundDrops());
                assertEquals(Math.min(bound, 2), h.matcher.retainedCandidates());
                assertFalse(h.matcher.possibleMatch());
            }
            try (Harness h = new Harness(config(bound, 2, true, List.of(equal(1, 0))), 10, null, () -> false)) {
                h.add(0, 0, 10, "k", "a#0", 1);
                h.add(0, 0, 10, "k", "a#1", 2);
                h.add(1, 1, 20, "k", "f", 2);
                assertEquals(bound == 1 ? List.of() : List.of("a#1 f"), h.matches);
            }
        }
    }

    public void testExpandedFinalRowsAreIndependentAndSameStageSiblingsNeedSubstitution() {
        try (Harness h = new Harness(config(16, 2, true, List.of(equal(1, 0))), 10, null, () -> false)) {
            h.add(0, 0, 10, "k", "a#0", 1);
            h.add(0, 0, 10, "k", "a#1", 2);
            h.add(1, 1, 20, "k", "f#0", 2);
            h.add(1, 1, 20, "k", "f#1", 1);
            assertEquals(List.of("a#1 f#0", "a#0 f#1"), h.matches);
        }
        try (Harness h = new Harness(config(16, 2, true, List.of(equal(1, 0))), 10, null, () -> false)) {
            SequenceCandidateValues first = h.values(0, 1);
            SequenceCandidateValues second = h.values(0, 1);
            assertNull(h.matcher.match(0, new BytesRef("k"), ordinal(0, 10), new BytesRef("a#0"), first));
            assertNull(h.matcher.match(0, new BytesRef("k"), ordinal(0, 10), new BytesRef("a#1"), second));
            h.add(1, 1, 20, "k", "f1", 1);
            h.add(1, 2, 30, "k", "f2", 1);
            assertEquals(List.of("a#0 f1"), h.matches);
            assertClosed(first);
            assertClosed(second);
            assertTrue(h.matcher.snapshotForTests().stream().allMatch(SequenceLatestValidMatcher.RowState::used));
        }
    }

    public void testSubstitutionRequiresStrictOrderBetweenEmittedNeighbours() {
        for (int bound : BOUNDS) {
            try (Harness h = new Harness(config(bound, 3, false, List.of()), 10, null, () -> false)) {
                h.add(0, 0, 10, "k", "a1", null);
                h.add(1, 1, 20, "k", "b1", null);
                h.add(0, 2, 30, "k", "a2", null);
                h.add(2, 3, 40, "k", "c1", null);
                h.add(1, 4, 50, "k", "b2", null);
                h.add(2, 5, 60, "k", "c2", null);
                assertEquals(List.of("a1 b1 c1", "a2 b2 c2"), h.matches);
            }
        }
    }

    public void testBackwardSearchFindsLostChainWithIncomingLimit() {
        for (int bound : BOUNDS) {
            try (Harness h = new Harness(config(bound, 3, false, List.of()), 20, new long[] { 0, 10_000_000, 2_000_000 }, () -> false)) {
                h.add(0, 4, 10, "k", "a", null);
                h.add(1, 5, 20, "k", "b1", null);
                h.add(1, 7, 30, "k", "b2", null);
                h.add(2, 8, 40, "k", "c", null);
                assertEquals(List.of("a b2 c"), h.matches);
            }
        }
    }

    /** A middle-stage substitute must satisfy its incoming link against the emitted previous member. */
    public void testSubstitutionChecksIncomingLinkAgainstEmittedPreviousMember() {
        for (int bound : BOUNDS) {
            try (Harness h = new Harness(config(bound, 3, true, List.of(equal(2, 0))), 10, new long[] { 0, 2_000_000, 0 }, () -> false)) {
                h.add(0, 0, 10, "k", "a", 1);
                h.add(1, 1, 20, "k", "b1", 0);
                h.add(0, 4, 30, "k", "a2", 2);
                h.add(1, 5, 40, "k", "b2", 0);
                h.add(2, 6, 50, "k", "c", 1);
                h.add(2, 7, 60, "k", "c2", 2);
                assertEquals(bound == 1 ? List.of("a2 b2 c2") : List.of("a b1 c", "a2 b2 c2"), h.matches);
            }
        }
    }

    public void testCrossMarkingIsNotRecursive() {
        for (int bound : List.of(16, Integer.MAX_VALUE)) {
            var config = config(bound, 3, true, List.of(equal(2, 0)));
            try (Harness h = new Harness(config, 10, null, () -> false)) {
                h.add(1, 1, 101, "k", "x_s2", 0);
                h.add(0, 1, 101, "k", "x#0", 1);
                h.add(0, 1, 101, "k", "x#1", 2);
                h.add(1, 2, 201, "k", "y1", 0);
                h.add(2, 3, 301, "k", "f1", 1);
                var state = h.matcher.snapshotForTests();
                assertEquals(List.of(true, false, true), state.stream().map(SequenceLatestValidMatcher.RowState::used).toList());
                assertEquals(List.of(1L, 2L, 3L), state.stream().map(SequenceLatestValidMatcher.RowState::arrivalRank).toList());
                h.add(1, 4, 401, "k", "y2", 0);
                h.add(2, 5, 501, "k", "f2", 2);
                assertEquals(List.of("x#0 y1 f1", "x#1 y2 f2"), h.matches);
            }
        }
    }

    public void testFullOrdinalIdentityDoesNotConsumeDistinctEqualTimestampDocuments() {
        try (Harness h = new Harness(config(16, 3, true, List.of(equal(2, 0))), 10, null, () -> false)) {
            h.add(0, 1, 10, "k", "a", 1);
            h.add(1, 1, 20, "k", "x_s2", 0);
            h.add(0, 1, 20, "k", "x#0", 1);
            h.add(0, 1, 20, "k", "x#1", 2);
            h.add(0, 1, 30, "k", "b", 2);
            h.add(2, 2, 40, "k", "f1", 1);
            assertTrue(
                h.matcher.snapshotForTests()
                    .stream()
                    .filter(row -> row.ordinal().shardDoc() == 20)
                    .allMatch(SequenceLatestValidMatcher.RowState::used)
            );
            assertFalse(
                h.matcher.snapshotForTests().stream().filter(row -> row.ordinal().shardDoc() == 30).findFirst().orElseThrow().used()
            );
            h.add(1, 3, 50, "k", "y", 0);
            h.add(2, 4, 60, "k", "f2", 2);
            assertEquals(List.of("a x_s2 f1", "b y f2"), h.matches);
        }
    }

    public void testKeysAreIndependentAndFinalDocumentCanBecomeAnEarlierCandidate() {
        try (Harness h = new Harness(config(16, 2, false, List.of()), 10, null, () -> false)) {
            h.add(0, 0, 10, "a", "a-key", null);
            h.add(0, 0, 10, "b", "b-key", null);
            h.add(1, 1, 20, "a", "fa", null);
            h.add(1, 1, 20, "b", "fb", null);
            h.add(0, 1, 20, "a", "fa-start", null);
            h.add(1, 2, 30, "a", "ga", null);
            assertEquals(List.of("a-key fa", "b-key fb", "fa-start ga"), h.matches);
        }
    }

    public void testEagerExpiryPrecedesBoundInsertionAndReleasesValues() {
        try (Harness h = new Harness(config(2, 2, true, List.of()), 10, null, () -> false)) {
            SequenceCandidateValues a1 = h.values(0, 1);
            assertNull(h.matcher.match(0, new BytesRef("k"), ordinal(0, 10), new BytesRef("a1"), a1));
            h.add(0, 5, 20, "k", "a2", 2);
            h.add(0, 12, 30, "k", "a3", 3);
            assertClosed(a1);
            h.add(1, 15, 40, "k", "f", 0);
            assertEquals(List.of("a3 f"), h.matches);
            assertEquals(0, h.matcher.boundDrops());
            assertEquals(1, h.matcher.expiredCandidates());
            assertEquals(2, h.matcher.largestRingFill());
        }
    }

    public void testUsedMarkerStillOccupiesItsBoundedSlot() {
        try (Harness h = new Harness(config(2, 2, true, List.of(equal(1, 0))), 10, null, () -> false)) {
            h.add(0, 0, 10, "k", "a1", 1);
            h.add(0, 1, 20, "k", "a2", 2);
            h.add(1, 2, 30, "k", "f1", 1);
            assertEquals(2, h.matcher.retainedCandidates());
            assertEquals(
                List.of(true, false),
                h.matcher.snapshotForTests().stream().map(SequenceLatestValidMatcher.RowState::used).toList()
            );
            h.add(0, 3, 40, "k", "a3", 3);
            assertEquals(0, h.matcher.boundDrops());
            assertEquals(2, h.matcher.retainedCandidates());
            h.add(1, 4, 50, "k", "f2", 2);
            assertEquals(List.of("a1 f1", "a2 f2"), h.matches);
        }
    }

    public void testUsedMarkersAndMiddleStageTailsAreNotViableStarts() {
        try (Harness h = new Harness(config(16, 3, false, List.of()), 10, null, () -> false)) {
            h.add(0, 0, 10, "k", "a", null);
            assertEquals(ordinal(0, 10), h.matcher.latestUnusedStart());
            h.add(1, 1, 20, "k", "b", null);
            h.add(2, 2, 30, "k", "c", null);
            assertFalse(h.matcher.possibleMatch());
            assertNull(h.matcher.latestUnusedStart());
            h.add(1, 3, 40, "k", "tail", null);
            assertEquals(2, h.matcher.retainedCandidates());
            assertFalse(h.matcher.possibleMatch());
            h.add(1, 20, 50, "other", "foreign-tail", null);
            assertEquals(2, h.matcher.expiredCandidates());
            assertEquals(0, h.matcher.retainedCandidates());
            assertEquals(2L, h.matcher.admissionCounters().candidatesNotKept());
            assertEquals(List.of("a b c"), h.matches);
            assertNull(h.matcher.latestUnusedStart());
        }
    }

    public void testGroupedPredicatesRunWhenTheirLowestReferenceIsBound() {
        AtomicInteger earlyCalls = new AtomicInteger();
        AtomicInteger lateCalls = new AtomicInteger();
        var early = new SequenceLatestValidConfig.PredicateGroup(3, List.of(2), rows -> {
            earlyCalls.incrementAndGet();
            return integer(rows, 3) == integer(rows, 2);
        });
        var late = new SequenceLatestValidConfig.PredicateGroup(3, List.of(0, 1), rows -> {
            lateCalls.incrementAndGet();
            return integer(rows, 0) == integer(rows, 1);
        });
        try (Harness h = new Harness(config(16, 4, true, List.of(early, late)), 10, null, () -> false)) {
            h.add(0, 0, 10, "k", "a", 1);
            h.add(1, 1, 20, "k", "b", 1);
            h.add(2, 2, 30, "k", "c-good", 1);
            h.add(2, 3, 40, "k", "c-bad", 0);
            h.add(3, 4, 50, "k", "f", 1);
            assertEquals(List.of("a b c-good f"), h.matches);
            assertTrue(earlyCalls.get() >= 2);
            assertTrue(lateCalls.get() >= 1);
        }
    }

    public void testTransferredValuesCloseOnEveryEntryFailure() {
        for (int scenario = 0; scenario < 5; scenario++) {
            try (Harness h = new Harness(config(16, 2, true, List.of()), 10, null, () -> false)) {
                h.add(0, 1, 10, "b", "stored", 1);
                SequenceCandidateValues incoming = h.values(0, 2);
                int selectedScenario = scenario;
                if (scenario == 0) {
                    h.matcher.close();
                }
                expectThrows(RuntimeException.class, () -> {
                    switch (selectedScenario) {
                        case 0 -> h.matcher.match(0, new BytesRef("b"), ordinal(2, 20), new BytesRef("closed"), incoming);
                        case 1 -> h.matcher.match(-1, new BytesRef("b"), ordinal(2, 20), new BytesRef("stage"), incoming);
                        case 2 -> h.matcher.match(0, new BytesRef("b"), ordinal(0, 20), new BytesRef("order"), incoming);
                        case 3 -> h.matcher.match(0, new BytesRef("a"), ordinal(1, 10), new BytesRef("key-order"), incoming);
                        case 4 -> h.matcher.match(0, null, ordinal(2, 20), new BytesRef("null-key"), incoming);
                        default -> throw new AssertionError();
                    }
                });
                assertClosed(incoming);
                assertEquals(0, h.breaker.getUsed());
            }
        }
        try (Harness h = new Harness(config(16, 2, true, List.of()), 10, null, () -> false); Page empty = new Page(1)) {
            SequenceCandidateValues wrong = SequenceCandidateValues.copyRow(empty, 0, List.of(), new int[0], h.blocks, h.breaker);
            expectThrows(
                IllegalArgumentException.class,
                () -> h.matcher.match(0, new BytesRef("k"), ordinal(0, 10), new BytesRef("wrong-demand"), wrong)
            );
            assertClosed(wrong);
            assertEquals(0, h.breaker.getUsed());
        }
    }

    public void testCancellationInsideSearchAndPredicateFailureReleaseAllOwners() {
        AtomicBoolean cancelled = new AtomicBoolean();
        var predicate = new SequenceLatestValidConfig.PredicateGroup(2, List.of(0), rows -> {
            cancelled.set(true);
            return false;
        });
        try (Harness h = new Harness(config(16, 3, true, List.of(predicate)), 10, null, cancelled::get)) {
            h.add(0, 0, 10, "k", "a", 1);
            h.add(1, 1, 20, "k", "b", 1);
            expectThrows(TaskCancelledException.class, () -> h.add(2, 2, 30, "k", "f", 1));
            assertEquals(0, h.breaker.getUsed());
        }
        var throwing = new SequenceLatestValidConfig.PredicateGroup(
            1,
            List.of(0),
            rows -> { throw new IllegalStateException("predicate failure"); }
        );
        try (Harness h = new Harness(config(16, 2, true, List.of(throwing)), 10, null, () -> false)) {
            h.add(0, 0, 10, "k", "a", 1);
            expectThrows(IllegalStateException.class, () -> h.add(1, 1, 20, "k", "f", 1));
            assertEquals(0, h.breaker.getUsed());
        }
    }

    public void testOutputSurvivesUseExpiryAndMatcherCloseWhileValuesAreReleased() {
        try (Harness h = new Harness(config(16, 2, true, List.of(equal(1, 0))), 10, null, () -> false)) {
            SequenceCandidateValues earlier = h.values(0, 1);
            h.matcher.match(0, new BytesRef("k"), ordinal(0, 10), new BytesRef("a"), earlier);
            try (SequenceMatch match = h.matcher.match(1, new BytesRef("k"), ordinal(1, 20), new BytesRef("f"), h.values(1, 1))) {
                assertNotNull(match);
                assertClosed(earlier);
                h.add(0, 20, 30, "other", "later", 2);
                assertEquals(1, h.matcher.expiredCandidates());
                h.matcher.close();
                assertEquals(List.of("a", "f"), match.handles().stream().map(BytesRef::utf8ToString).toList());
                assertEquals(new BytesRef("k"), match.key());
                assertTrue(h.breaker.getUsed() > 0);
            }
            assertEquals(0, h.breaker.getUsed());
        }
    }

    public void testPayloadAdmissionFailureRollsBackPendingAndCompletedOwnership() {
        for (boolean finalFailure : List.of(false, true)) {
            try (Harness h = new Harness(config(16, 2, true, List.of()), 10, null, () -> false)) {
                if (finalFailure) {
                    h.add(0, 0, 10, "k", "a", 1);
                }
                SequenceCandidateValues values = h.values(finalFailure ? 1 : 0, 1);
                expectThrows(
                    CircuitBreakingException.class,
                    () -> h.matcher.match(
                        finalFailure ? 1 : 0,
                        new BytesRef("k"),
                        ordinal(1, 20),
                        new BytesRef(new byte[20 * 1024 * 1024]),
                        values
                    )
                );
                assertClosed(values);
                assertEquals(0, h.breaker.getUsed());
            }
        }
    }

    public void testPartialMetadataAdmissionAndConstructorFailuresRefundReservations() {
        int failures = 0;
        int successes = 0;
        for (int allowance = 0; allowance <= 8192; allowance += 128) {
            try (Harness h = new Harness(config(16, 2, false, List.of()), 10, null, () -> false)) {
                h.breaker.setLimitAndOverhead(h.breaker.getUsed() + allowance, 1.0);
                try {
                    h.add(0, 0, 10, "k", "payload", null);
                    successes++;
                } catch (CircuitBreakingException e) {
                    failures++;
                    assertEquals(0, h.breaker.getUsed());
                }
            }
        }
        assertTrue(failures > 0);
        assertTrue(successes > 0);
        CircuitBreaker breaker = breaker();
        breaker.setLimitAndOverhead(1, 1.0);
        expectThrows(
            CircuitBreakingException.class,
            () -> new SequenceLatestValidMatcher(
                2,
                TimeValue.timeValueMillis(10),
                breaker,
                () -> false,
                BytesRef::compareTo,
                null,
                config(16, 2, false, List.of())
            )
        );
        assertEquals(0, breaker.getUsed());
        expectThrows(
            IllegalArgumentException.class,
            () -> new SequenceLatestValidMatcher(
                2,
                TimeValue.timeValueMillis(10),
                breaker,
                () -> false,
                BytesRef::compareTo,
                new long[] { 1, 0 },
                config(16, 2, false, List.of())
            )
        );
        assertEquals(0, breaker.getUsed());
    }

    public void testBoundedHotKeyKeepsOnlyRingState() {
        try (Harness h = new Harness(config(2, 2, false, List.of()), 10_000, null, () -> false)) {
            long steady = 0;
            for (int row = 0; row < 1000; row++) {
                h.add(0, row, row, "k", "payload", null);
                if (row == 100) {
                    steady = h.breaker.getUsed();
                }
            }
            assertEquals(2, h.matcher.retainedCandidates());
            assertEquals(2, h.matcher.largestRingFill());
            assertEquals(998, h.matcher.boundDrops());
            assertEquals(steady, h.breaker.getUsed());
        }
    }

    public void testCancellationDuringExpiryClosesTheRemainingRecords() {
        AtomicBoolean armed = new AtomicBoolean();
        AtomicInteger checks = new AtomicInteger();
        try (Harness h = new Harness(config(16, 2, false, List.of()), 5, null, () -> armed.get() && checks.incrementAndGet() >= 3)) {
            h.add(0, 0, 10, "k", "a", null);
            h.add(0, 1, 20, "k", "b", null);
            h.add(0, 2, 30, "k", "c", null);
            armed.set(true);
            expectThrows(TaskCancelledException.class, () -> h.add(1, 10, 40, "k", "f", null));
            assertEquals(1, h.matcher.expiredCandidates());
            assertEquals(0, h.breaker.getUsed());
        }
    }

    private static SequenceLatestValidConfig.PredicateGroup equal(int own, int earlier) {
        return new SequenceLatestValidConfig.PredicateGroup(own, List.of(earlier), rows -> integer(rows, own) == integer(rows, earlier));
    }

    private static int integer(SequenceLatestValidConfig.SelectedValues rows, int stage) {
        IntBlock block = (IntBlock) rows.block(stage, 0);
        return block.getInt(block.getFirstValueIndex(0));
    }

    private static SequenceLatestValidConfig config(
        int bound,
        int stages,
        boolean values,
        List<SequenceLatestValidConfig.PredicateGroup> groups
    ) {
        List<List<SequenceLatestValidConfig.ValueDemand>> demands = new ArrayList<>();
        for (int stage = 0; stage < stages; stage++) {
            demands.add(values ? List.of(VALUE) : List.of());
        }
        return bound == Integer.MAX_VALUE
            ? SequenceLatestValidConfig.unboundedForTests(demands, groups)
            : SequenceLatestValidConfig.bounded(bound, demands, groups);
    }

    private static SequenceOrdinal ordinal(long millis, long document) {
        return new SequenceOrdinal(millis, 0, document);
    }

    private static void assertClosed(SequenceCandidateValues values) {
        expectThrows(IllegalStateException.class, values::size);
    }

    private static CircuitBreaker breaker() {
        Settings settings = Settings.builder()
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "10mb")
            .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
            .build();
        return new HierarchyCircuitBreakerService(
            CircuitBreakerMetrics.NOOP,
            settings,
            List.of(),
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        ).getBreaker(CircuitBreaker.REQUEST);
    }

    private static final class Harness implements Releasable {
        private final CircuitBreaker breaker = breaker();
        private final BlockFactory blocks = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker).build();
        private final SequenceLatestValidConfig config;
        private final SequenceLatestValidMatcher matcher;
        private final List<String> matches = new ArrayList<>();

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

        private SequenceCandidateValues values(int stage, Integer value) {
            if (config.mandatoryDemands().get(stage).isEmpty()) {
                return null;
            }
            try (Page page = new Page(value == null ? blocks.newConstantNullBlock(1) : blocks.newConstantIntBlockWith(value, 1))) {
                return SequenceCandidateValues.copyRow(page, 0, config.mandatoryDemands().get(stage), new int[] { 0 }, blocks, breaker);
            }
        }

        private void add(int stage, long millis, long doc, String key, String handle, Integer value) {
            try (
                SequenceMatch match = matcher.match(
                    stage,
                    new BytesRef(key),
                    ordinal(millis, doc),
                    new BytesRef(handle),
                    values(stage, value)
                )
            ) {
                if (match != null) {
                    matches.add(String.join(" ", match.handles().stream().map(BytesRef::utf8ToString).toList()));
                }
            }
        }

        @Override
        public void close() {
            matcher.close();
            assertEquals(0, breaker.getUsed());
        }
    }
}
