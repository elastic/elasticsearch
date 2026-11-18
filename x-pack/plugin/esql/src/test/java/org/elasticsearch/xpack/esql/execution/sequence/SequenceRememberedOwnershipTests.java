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
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
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

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongPredicate;

import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/** Failure and expiry must reach values whose rows are retained only by a listed holder. */
public class SequenceRememberedOwnershipTests extends ESTestCase {
    private static final SequenceLatestValidConfig.ValueDemand VALUE = new SequenceLatestValidConfig.ValueDemand(
        new ReferenceAttribute(Source.EMPTY, null, "value", DataType.INTEGER)
    );

    /** A forced admission error refunds source ownership while preserving a separate reservation. */
    public void testRememberedOnlyValuesCloseAfterBudgetFailure() {
        try (Harness h = new Harness(List.of())) {
            h.rememberStart();
            SequenceCandidateValues arriving = h.values(1);
            // This separate reservation leaves the source one byte, forcing its next metadata admission to fail.
            long filler = h.limit - h.budget.currentBytes() - 1;
            h.scoped.addEstimateBytesAndMaybeBreak(filler, "other-owner-before-failure");
            try {
                expectThrows(CircuitBreakingException.class, () -> h.add(1, 4, "B1", arriving));
                assertEquals(filler, h.budget.currentBytes());
                assertEquals(filler, h.breaker.getUsed());
                assertClosed(arriving);
                assertClosed(h.remembered);
                h.matcher.close();
                assertEquals(filler, h.breaker.getUsed());
            } finally {
                h.scoped.addWithoutBreaking(-filler);
            }
            assertEquals(0L, h.budget.currentBytes());
        }
    }

    public void testKeptArrayGrowthFailureClosesRememberedAndArrivingValues() {
        try (Harness h = new Harness(List.of(), true)) {
            h.rememberStart();
            var secondHolder = h.values(1);
            h.add(1, 4, "B1", secondHolder);
            h.add(0, 5, "A3", h.values(1));
            h.add(0, 6, "A4", h.values(1));
            assertEquals(List.of(0L, 3L, 5L, 6L), startTimes(h));
            assertEquals(1, secondHolder.size());

            AtomicInteger reservations = new AtomicInteger();
            h.assertAdmissionFailure(0, 7, "A5", bytes -> {
                if (reservations.incrementAndGet() != 2) {
                    return false;
                }
                // Key-order admission runs first. The list has room for a third row, while kept grows from four to eight.
                assertEquals(referenceArrayBytes(8), bytes);
                assertEquals(List.of(0L, 3L, 5L, 6L), startTimes(h));
                return true;
            });
            assertEquals(2, reservations.get());
            assertClosed(secondHolder);
        }
    }

    public void testRememberedChainReservationFailureClosesPublishedHolder() {
        try (Harness h = new Harness(List.of(), true)) {
            h.rememberStart();
            h.assertAdmissionFailure(1, 4, "B1", bytes -> {
                boolean published = h.matcher.snapshotForTests()
                    .stream()
                    .anyMatch(row -> row.stage() == 1 && row.ordinal().epochMillis() == 4);
                if (published) {
                    // The holder is already in expiry and the kept pool. Its flat chain needs one earlier-stage cell.
                    assertEquals(referenceArrayBytes(1), bytes);
                    assertEquals(5, h.matcher.retainedCandidates());
                }
                return published;
            });
        }
    }

    private static List<Long> startTimes(Harness h) {
        return h.matcher.snapshotForTests().stream().filter(row -> row.stage() == 0).map(row -> row.ordinal().epochMillis()).toList();
    }

    private static long referenceArrayBytes(int length) {
        return RamUsageEstimator.alignObjectSize(
            RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) RamUsageEstimator.NUM_BYTES_OBJECT_REF * length
        );
    }

    /** Predicate failure must close rows outside the lists as well as the arriving cells. */
    public void testRememberedOnlyValuesCloseAfterConditionThrows() {
        AtomicBoolean fail = new AtomicBoolean();
        var group = new SequenceLatestValidConfig.PredicateGroup(1, List.of(0), values -> {
            if (fail.get()) {
                throw new IllegalStateException("controlled arrival condition failure");
            }
            return true;
        });
        try (Harness h = new Harness(List.of(group))) {
            h.rememberStart();
            fail.set(true);
            var arriving = h.values(1);
            var failure = expectThrows(IllegalStateException.class, () -> h.add(1, 4, "B1", arriving));
            assertEquals("controlled arrival condition failure", failure.getMessage());
            assertClosed(h.remembered);
            assertClosed(arriving);
            assertEquals(0L, h.budget.currentBytes());
            assertEquals(0L, h.breaker.getUsed());
        }
    }

    /** Cancellation must not leave an off-list start alive through its former holder. */
    public void testRememberedOnlyValuesCloseAfterCancellation() {
        try (Harness h = new Harness(List.of())) {
            h.rememberStart();
            var arriving = h.values(1);
            h.cancelled.set(true);
            expectThrows(TaskCancelledException.class, () -> h.add(1, 4, "B1", arriving));
            assertClosed(h.remembered);
            assertClosed(arriving);
            assertEquals(0L, h.budget.currentBytes());
            assertEquals(0L, h.breaker.getUsed());
        }
    }

    /** Window expiry removes the remembered start without expiring its younger listed holder. */
    public void testRememberedOnlyExpiryLeavesItsLiveHolderOwned() {
        try (Harness h = new Harness(List.of())) {
            h.rememberStart();
            try (
                var match = h.matcher.match(
                    2,
                    new BytesRef("other"),
                    new SequenceOrdinal(11, 0, 11),
                    new BytesRef("unmatched-final"),
                    h.values(1)
                )
            ) {
                assertNull(match);
            }
            assertEquals(1L, h.matcher.expiredCandidates());
            assertClosed(h.remembered);
            assertEquals(3, h.matcher.retainedCandidates());
            assertFalse(h.matcher.snapshotForTests().stream().anyMatch(row -> row.ordinal().epochMillis() == 0));
            assertTrue(h.matcher.snapshotForTests().stream().anyMatch(row -> row.ordinal().epochMillis() == 1));
            assertTrue(h.budget.currentBytes() > 0);
            assertEquals(0L, h.matcher.boundDrops());
        }
    }

    private static void assertClosed(SequenceCandidateValues values) {
        expectThrows(IllegalStateException.class, values::size);
    }

    private static final class Harness implements Releasable {
        private final long limit = 1_000_000;
        private final SequenceMemoryBudget budget = new SequenceMemoryBudget(limit);
        private final CircuitBreaker breaker;
        private final CircuitBreaker scoped;
        private final CircuitBreaker matcherBreaker;
        private final BlockFactory blocks;
        private final AtomicBoolean cancelled = new AtomicBoolean();
        private final SequenceLatestValidConfig config;
        private final SequenceLatestValidMatcher matcher;
        private SequenceCandidateValues remembered;

        private Harness(List<SequenceLatestValidConfig.PredicateGroup> groups) {
            this(groups, false);
        }

        private Harness(List<SequenceLatestValidConfig.PredicateGroup> groups, boolean interceptReservations) {
            Settings settings = Settings.builder()
                .put("indices.breaker.request.limit", "10mb")
                .put("indices.breaker.total.use_real_memory", false)
                .build();
            breaker = new HierarchyCircuitBreakerService(
                CircuitBreakerMetrics.NOOP,
                settings,
                List.of(),
                new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
            ).getBreaker(CircuitBreaker.REQUEST);
            scoped = budget.newScope().breaker(breaker);
            matcherBreaker = interceptReservations ? mock(CircuitBreaker.class, delegatesTo(scoped)) : scoped;
            blocks = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(scoped).build();
            config = SequenceLatestValidConfig.boundedForTests(2, 131072, java.util.Collections.nCopies(3, List.of(VALUE)), groups);
            matcher = new SequenceLatestValidMatcher(
                3,
                TimeValue.timeValueMillis(10),
                matcherBreaker,
                cancelled::get,
                BytesRef::compareTo,
                null,
                config
            );
        }

        private void rememberStart() {
            remembered = values(1);
            add(0, 0, "A0", remembered);
            add(1, 1, "B0", values(1));
            add(0, 2, "A1", values(1));
            add(0, 3, "A2", values(1));
            assertEquals(4, matcher.retainedCandidates());
            assertEquals(0L, matcher.boundDrops());
            assertEquals(1, remembered.size());
        }

        private SequenceCandidateValues values(int value) {
            try (Page page = new Page(blocks.newConstantIntBlockWith(value, 1))) {
                return SequenceCandidateValues.copyRow(page, 0, List.of(VALUE), new int[] { 0 }, blocks, scoped);
            }
        }

        private void add(int stage, int tick, String handle, SequenceCandidateValues values) {
            try (var match = matcher.match(stage, new BytesRef("k"), new SequenceOrdinal(tick, 0, tick), new BytesRef(handle), values)) {
                assertNull(match);
            }
        }

        private void assertAdmissionFailure(int stage, int tick, String handle, LongPredicate failAt) {
            var arriving = values(1);
            AtomicLong filler = new AtomicLong();
            doAnswer(invocation -> {
                long bytes = invocation.getArgument(0);
                if (failAt.test(bytes)) {
                    assertEquals(0L, filler.get());
                    long reservation = limit - budget.currentBytes() - bytes + 1;
                    assertTrue(reservation > 0);
                    scoped.addEstimateBytesAndMaybeBreak(reservation, "other-owner-at-target-reservation");
                    filler.set(reservation);
                }
                scoped.addEstimateBytesAndMaybeBreak(bytes, invocation.getArgument(1));
                return null;
            }).when(matcherBreaker).addEstimateBytesAndMaybeBreak(anyLong(), eq("esql_sequence_matcher"));
            try {
                var failure = expectThrows(CircuitBreakingException.class, () -> add(stage, tick, handle, arriving));
                assertEquals(limit + 1, failure.getBytesWanted());
                assertEquals(limit, failure.getByteLimit());
                assertTrue(filler.get() > 0);
                assertClosed(arriving);
                assertClosed(remembered);
                assertEquals(filler.get(), budget.currentBytes());
                assertEquals(filler.get(), breaker.getUsed());
                matcher.close();
                assertEquals(filler.get(), breaker.getUsed());
            } finally {
                scoped.addWithoutBreaking(-filler.get());
            }
            assertEquals(0L, budget.currentBytes());
            assertEquals(0L, breaker.getUsed());
        }

        @Override
        public void close() {
            matcher.close();
            assertEquals(0L, budget.currentBytes());
            assertEquals(0L, breaker.getUsed());
        }
    }
}
