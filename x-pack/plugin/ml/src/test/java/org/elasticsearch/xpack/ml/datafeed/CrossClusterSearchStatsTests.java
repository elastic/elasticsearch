/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.datafeed.CrossClusterSearchStats.ScopeChangeResult;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class CrossClusterSearchStatsTests extends ESTestCase {

    private static final Duration ONE_MINUTE = Duration.ofMinutes(1);

    private static LinkedClusterState available(String alias) {
        return new LinkedClusterState(alias, LinkedClusterState.Status.AVAILABLE, null, 10L);
    }

    private static LinkedClusterState skipped(String alias) {
        return new LinkedClusterState(alias, LinkedClusterState.Status.SKIPPED, "unavailable", 0L);
    }

    private static LinkedClusterState failed(String alias) {
        return new LinkedClusterState(alias, LinkedClusterState.Status.FAILED, "connection error", 0L);
    }

    public void testEmptyCycleIsNoOp() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.now());
        CrossClusterSearchStats stats = new CrossClusterSearchStats(clock::get);

        ScopeChangeResult result = stats.update(List.of());

        assertFalse(result.scopeChanged());
        assertThat(result.confirmedLinks(), equalTo(Set.of()));
        assertThat(result.confirmedUnlinks(), equalTo(Set.of()));
        assertThat(result.changeTimestamp(), nullValue());
        assertThat(stats.getTotalClusters(), equalTo(0));
    }

    public void testFirstCycleEstablishesBaseline() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.now());
        CrossClusterSearchStats stats = new CrossClusterSearchStats(clock::get);

        ScopeChangeResult result = stats.update(List.of(available("origin"), available("P1"), available("P2")));

        assertFalse(result.scopeChanged());
        assertThat(stats.getConfirmedAliases(), containsInAnyOrder("origin", "P1", "P2"));
        assertThat(stats.getTotalClusters(), equalTo(3));
        assertThat(stats.getAvailableClusters(), equalTo(3));
        assertThat(stats.getSkippedClusters(), equalTo(0));
        assertThat(stats.getAvailabilityRatio(), closeTo(1.0, 0.001));
    }

    public void testLinkingRequires12ConsecutiveCycles() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.EPOCH);
        CrossClusterSearchStats stats = new CrossClusterSearchStats(clock::get);

        stats.update(List.of(available("origin"), available("P1")));

        for (int i = 1; i <= 11; i++) {
            clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(i)));
            ScopeChangeResult result = stats.update(List.of(available("origin"), available("P1"), available("P3")));
            assertFalse("Cycle " + i + " should not trigger stabilization", result.scopeChanged());
        }

        clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(12)));
        ScopeChangeResult result = stats.update(List.of(available("origin"), available("P1"), available("P3")));

        assertTrue(result.scopeChanged());
        assertThat(result.confirmedLinks(), equalTo(Set.of("P3")));
        assertThat(result.confirmedUnlinks(), equalTo(Set.of()));
        assertThat(stats.getConfirmedAliases(), containsInAnyOrder("origin", "P1", "P3"));
    }

    public void testUnlinkingRequires12ConsecutiveCycles() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.EPOCH);
        CrossClusterSearchStats stats = new CrossClusterSearchStats(clock::get);

        stats.update(List.of(available("origin"), available("P1"), available("P2")));

        for (int i = 1; i <= 11; i++) {
            clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(i)));
            ScopeChangeResult result = stats.update(List.of(available("origin"), available("P1")));
            assertFalse("Cycle " + i + " should not trigger removal", result.scopeChanged());
        }

        clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(12)));
        ScopeChangeResult result = stats.update(List.of(available("origin"), available("P1")));

        assertTrue(result.scopeChanged());
        assertThat(result.confirmedUnlinks(), equalTo(Set.of("P2")));
        assertThat(result.confirmedLinks(), equalTo(Set.of()));
        assertThat(stats.getConfirmedAliases(), containsInAnyOrder("origin", "P1"));
    }

    public void testLinkingTimeFloorPreventsEarlyStabilization() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.EPOCH);
        CrossClusterSearchStats stats = new CrossClusterSearchStats(clock::get);

        stats.update(List.of(available("origin")));

        Instant firstSeen = Instant.EPOCH.plusSeconds(10);
        clock.set(firstSeen);
        stats.update(List.of(available("origin"), available("P1")));

        for (int i = 2; i <= 12; i++) {
            clock.set(Instant.EPOCH.plusSeconds(i * 10));
            ScopeChangeResult result = stats.update(List.of(available("origin"), available("P1")));
            assertFalse("Should not stabilize before 5 minutes even with 12+ cycles", result.scopeChanged());
        }

        clock.set(firstSeen.plus(Duration.ofMinutes(5)));
        ScopeChangeResult result = stats.update(List.of(available("origin"), available("P1")));

        assertTrue(result.scopeChanged());
        assertThat(result.confirmedLinks(), equalTo(Set.of("P1")));
    }

    public void testUnlinkingTimeFloorPreventsEarlyStabilization() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.EPOCH);
        CrossClusterSearchStats stats = new CrossClusterSearchStats(clock::get);

        stats.update(List.of(available("origin"), available("P1")));

        Instant firstAbsent = Instant.EPOCH.plusSeconds(10);
        clock.set(firstAbsent);
        stats.update(List.of(available("origin")));

        for (int i = 2; i <= 12; i++) {
            clock.set(Instant.EPOCH.plusSeconds(i * 10));
            ScopeChangeResult result = stats.update(List.of(available("origin")));
            assertFalse("Should not confirm removal before 5 minutes even with 12+ cycles", result.scopeChanged());
        }

        clock.set(firstAbsent.plus(Duration.ofMinutes(5)));
        ScopeChangeResult result = stats.update(List.of(available("origin")));

        assertTrue(result.scopeChanged());
        assertThat(result.confirmedUnlinks(), equalTo(Set.of("P1")));
    }

    public void testLinkingPresenceGapResetsCounter() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.EPOCH);
        CrossClusterSearchStats stats = new CrossClusterSearchStats(clock::get);

        stats.update(List.of(available("origin")));

        for (int i = 1; i <= 5; i++) {
            clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(i)));
            stats.update(List.of(available("origin"), available("P3")));
        }

        clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(6)));
        stats.update(List.of(available("origin")));

        for (int i = 7; i <= 17; i++) {
            clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(i)));
            ScopeChangeResult result = stats.update(List.of(available("origin"), available("P3")));
            assertFalse("Cycle " + i + " after reset should not yet stabilize", result.scopeChanged());
        }

        clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(18)));
        ScopeChangeResult result = stats.update(List.of(available("origin"), available("P3")));

        assertTrue(result.scopeChanged());
        assertThat(result.confirmedLinks(), equalTo(Set.of("P3")));
        assertThat(result.changeTimestamp(), equalTo(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(7))));
    }

    public void testUnlinkingAbsenceGapResetsCounter() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.EPOCH);
        CrossClusterSearchStats stats = new CrossClusterSearchStats(clock::get);

        stats.update(List.of(available("origin"), available("P2")));

        for (int i = 1; i <= 5; i++) {
            clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(i)));
            stats.update(List.of(available("origin")));
        }

        clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(6)));
        stats.update(List.of(available("origin"), available("P2")));

        for (int i = 7; i <= 17; i++) {
            clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(i)));
            ScopeChangeResult result = stats.update(List.of(available("origin")));
            assertFalse("Cycle " + i + " after reset should not yet confirm removal", result.scopeChanged());
        }

        clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(18)));
        ScopeChangeResult result = stats.update(List.of(available("origin")));

        assertTrue(result.scopeChanged());
        assertThat(result.confirmedUnlinks(), equalTo(Set.of("P2")));
        assertThat(result.changeTimestamp(), equalTo(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(7))));
    }

    public void testLinkingFlipFlapProducesNoConfirmation() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.EPOCH);
        CrossClusterSearchStats stats = new CrossClusterSearchStats(clock::get);

        stats.update(List.of(available("origin")));

        for (int i = 1; i <= 24; i++) {
            clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(i)));
            List<LinkedClusterState> cycle = (i % 2 == 1) ? List.of(available("origin"), available("P3")) : List.of(available("origin"));
            ScopeChangeResult result = stats.update(cycle);
            assertFalse("Flip-flap cycle " + i + " should never confirm", result.scopeChanged());
        }

        assertThat(stats.getConfirmedAliases(), equalTo(Set.of("origin")));
    }

    public void testUnlinkingFlipFlapProducesNoConfirmation() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.EPOCH);
        CrossClusterSearchStats stats = new CrossClusterSearchStats(clock::get);

        stats.update(List.of(available("origin"), available("P2")));

        for (int i = 1; i <= 24; i++) {
            clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(i)));
            List<LinkedClusterState> cycle = (i % 2 == 1) ? List.of(available("origin")) : List.of(available("origin"), available("P2"));
            ScopeChangeResult result = stats.update(cycle);
            assertFalse("Flip-flap cycle " + i + " should never confirm", result.scopeChanged());
        }

        assertThat(stats.getConfirmedAliases(), containsInAnyOrder("origin", "P2"));
    }

    public void testSimultaneousLinkingAndUnlinking() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.EPOCH);
        CrossClusterSearchStats stats = new CrossClusterSearchStats(clock::get);

        stats.update(List.of(available("origin"), available("P1"), available("P2")));

        Instant firstChangeTime = null;
        for (int i = 1; i <= 11; i++) {
            clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(i)));
            if (firstChangeTime == null) {
                firstChangeTime = clock.get();
            }
            stats.update(List.of(available("origin"), available("P1"), available("P3")));
        }

        clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(12)));
        ScopeChangeResult result = stats.update(List.of(available("origin"), available("P1"), available("P3")));

        assertTrue(result.scopeChanged());
        assertThat(result.confirmedLinks(), equalTo(Set.of("P3")));
        assertThat(result.confirmedUnlinks(), equalTo(Set.of("P2")));
        assertThat(stats.getConfirmedAliases(), containsInAnyOrder("origin", "P1", "P3"));
        assertThat(result.changeTimestamp(), equalTo(firstChangeTime));
    }

    public void testMultipleProjectsStabilizingSimultaneously() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.EPOCH);
        CrossClusterSearchStats stats = new CrossClusterSearchStats(clock::get);

        stats.update(List.of(available("origin")));

        for (int i = 1; i <= 11; i++) {
            clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(i)));
            stats.update(List.of(available("origin"), available("P1"), available("P2"), available("P3")));
        }

        clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(12)));
        ScopeChangeResult result = stats.update(List.of(available("origin"), available("P1"), available("P2"), available("P3")));

        assertTrue(result.scopeChanged());
        assertThat(result.confirmedLinks(), containsInAnyOrder("P1", "P2", "P3"));
        assertThat(stats.getConfirmedAliases(), containsInAnyOrder("origin", "P1", "P2", "P3"));
    }

    public void testSingleProjectDatafeed() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.EPOCH);
        CrossClusterSearchStats stats = new CrossClusterSearchStats(clock::get);

        stats.update(List.of(available("origin")));
        assertThat(stats.getConfirmedAliases(), equalTo(Set.of("origin")));
        assertThat(stats.getAvailabilityRatio(), closeTo(1.0, 0.001));

        for (int i = 1; i <= 12; i++) {
            clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(i)));
            ScopeChangeResult result = stats.update(List.of(available("origin")));
            assertFalse(result.scopeChanged());
        }
    }

    public void testSkippedProjectCountsAsPresent() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.EPOCH);
        CrossClusterSearchStats stats = new CrossClusterSearchStats(clock::get);

        stats.update(List.of(available("origin"), available("P1")));

        for (int i = 1; i <= 11; i++) {
            clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(i)));
            stats.update(List.of(available("origin"), skipped("P1"), available("P2")));
        }

        clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(12)));
        ScopeChangeResult result = stats.update(List.of(available("origin"), skipped("P1"), available("P2")));

        assertTrue(result.scopeChanged());
        assertThat(result.confirmedLinks(), equalTo(Set.of("P2")));
        assertThat(result.confirmedUnlinks(), equalTo(Set.of()));
        assertThat(stats.getConfirmedAliases(), containsInAnyOrder("origin", "P1", "P2"));
    }

    public void testFailedProjectCountsAsPresent() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.EPOCH);
        CrossClusterSearchStats stats = new CrossClusterSearchStats(clock::get);

        stats.update(List.of(available("origin"), available("P1")));

        for (int i = 1; i <= 12; i++) {
            clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(i)));
            ScopeChangeResult result = stats.update(List.of(available("origin"), failed("P1")));
            assertFalse(result.scopeChanged());
        }

        assertThat(stats.getConfirmedAliases(), containsInAnyOrder("origin", "P1"));
    }

    public void testConsecutiveSkipsTracking() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.EPOCH);
        CrossClusterSearchStats stats = new CrossClusterSearchStats(clock::get);

        stats.update(List.of(available("origin"), available("P1")));

        clock.set(Instant.EPOCH.plus(ONE_MINUTE));
        stats.update(List.of(available("origin"), skipped("P1")));
        assertThat(stats.getConsecutiveUnavailable(), hasEntry("P1", 1));

        clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(2)));
        stats.update(List.of(available("origin"), skipped("P1")));
        assertThat(stats.getConsecutiveUnavailable(), hasEntry("P1", 2));

        clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(3)));
        stats.update(List.of(available("origin"), available("P1")));
        assertThat(stats.getConsecutiveUnavailable(), hasEntry("P1", 0));
    }

    public void testAvailabilityRatioUpdatesPerCycle() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.EPOCH);
        CrossClusterSearchStats stats = new CrossClusterSearchStats(clock::get);

        stats.update(List.of(available("origin"), available("P1"), available("P2"), available("P3"), available("P4")));
        assertThat(stats.getAvailabilityRatio(), closeTo(1.0, 0.001));

        clock.set(Instant.EPOCH.plus(ONE_MINUTE));
        stats.update(List.of(available("origin"), skipped("P1"), available("P2"), available("P3"), available("P4")));
        assertThat(stats.getTotalClusters(), equalTo(5));
        assertThat(stats.getAvailableClusters(), equalTo(4));
        assertThat(stats.getSkippedClusters(), equalTo(1));
        assertThat(stats.getAvailabilityRatio(), closeTo(0.8, 0.001));
    }

    public void testChangeTimestampReflectsFirstObservation() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.EPOCH);
        CrossClusterSearchStats stats = new CrossClusterSearchStats(clock::get);

        stats.update(List.of(available("origin")));

        Instant firstSeen = Instant.EPOCH.plus(ONE_MINUTE);
        ScopeChangeResult confirmedResult = null;

        for (int i = 1; i <= 12; i++) {
            clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(i)));
            ScopeChangeResult result = stats.update(List.of(available("origin"), available("P1")));
            if (result.scopeChanged()) {
                confirmedResult = result;
                break;
            }
        }

        assertThat(confirmedResult, notNullValue());
        assertTrue(confirmedResult.scopeChanged());
        assertThat(confirmedResult.changeTimestamp(), equalTo(firstSeen));
    }

    public void testChangeTimestampUsesEarliestAmongSimultaneousChanges() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.EPOCH);
        CrossClusterSearchStats stats = new CrossClusterSearchStats(clock::get);

        stats.update(List.of(available("origin"), available("P1")));

        Instant p1AbsentStart = Instant.EPOCH.plus(ONE_MINUTE);

        ScopeChangeResult confirmedResult = null;
        for (int i = 1; i <= 13; i++) {
            clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(i)));
            ScopeChangeResult result = stats.update(List.of(available("origin"), available("P2")));
            if (result.scopeChanged()) {
                confirmedResult = result;
                break;
            }
        }

        assertThat(confirmedResult, notNullValue());
        assertTrue(confirmedResult.scopeChanged());
        assertThat(confirmedResult.confirmedLinks(), equalTo(Set.of("P2")));
        assertThat(confirmedResult.confirmedUnlinks(), equalTo(Set.of("P1")));
        assertThat(confirmedResult.changeTimestamp(), equalTo(p1AbsentStart));
    }

    public void testNoChangeAfterBaselineWhenProjectSetUnchanged() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.EPOCH);
        CrossClusterSearchStats stats = new CrossClusterSearchStats(clock::get);

        stats.update(List.of(available("origin"), available("P1"), available("P2")));

        for (int i = 1; i <= 20; i++) {
            clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(i)));
            ScopeChangeResult result = stats.update(List.of(available("origin"), available("P1"), available("P2")));
            assertFalse(result.scopeChanged());
        }
    }

    public void testLinkingConfirmationDoesNotRepeat() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.EPOCH);
        CrossClusterSearchStats stats = new CrossClusterSearchStats(clock::get);

        stats.update(List.of(available("origin")));

        int confirmedAtCycle = -1;
        for (int i = 1; i <= 13; i++) {
            clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(i)));
            ScopeChangeResult result = stats.update(List.of(available("origin"), available("P1")));
            if (result.scopeChanged()) {
                confirmedAtCycle = i;
                break;
            }
        }
        assertTrue("Should have confirmed linking", confirmedAtCycle > 0);

        for (int i = confirmedAtCycle + 1; i <= confirmedAtCycle + 10; i++) {
            clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(i)));
            ScopeChangeResult result = stats.update(List.of(available("origin"), available("P1")));
            assertFalse("Post-confirmation cycle " + i + " should not re-trigger", result.scopeChanged());
        }
    }

    public void testStabilizedProjectAliasesReturnsDefensiveCopy() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.EPOCH);
        CrossClusterSearchStats stats = new CrossClusterSearchStats(clock::get);

        stats.update(List.of(available("origin"), available("P1")));

        Set<String> aliases = stats.getConfirmedAliases();
        expectThrows(UnsupportedOperationException.class, () -> aliases.add("hacked"));
    }

    public void testConsecutiveSkipsReturnsDefensiveCopy() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.EPOCH);
        CrossClusterSearchStats stats = new CrossClusterSearchStats(clock::get);

        stats.update(List.of(available("origin"), skipped("P1")));

        var skips = stats.getConsecutiveUnavailable();
        expectThrows(UnsupportedOperationException.class, () -> skips.put("hacked", 99));
    }

    public void testFluctuatingProjectPresence() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.EPOCH);
        CrossClusterSearchStats stats = new CrossClusterSearchStats(clock::get);

        stats.update(List.of(available("origin")));

        for (int i = 1; i <= 30; i++) {
            clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(i)));
            List<LinkedClusterState> cycle = (i % 3 == 0) ? List.of(available("origin")) : List.of(available("origin"), available("P1"));
            ScopeChangeResult result = stats.update(cycle);
            assertFalse("Fluctuating cycle " + i + " should not confirm", result.scopeChanged());
        }
        assertThat(stats.getConfirmedAliases(), equalTo(Set.of("origin")));
    }

    public void testSimultaneousLinkingAndUnlinkingWithDifferentStartTimes() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.EPOCH);
        CrossClusterSearchStats stats = new CrossClusterSearchStats(clock::get);

        stats.update(List.of(available("origin"), available("P1")));

        // P2 appears for the first time at t=1
        clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(1)));
        stats.update(List.of(available("origin"), available("P1"), available("P2")));

        // P1 disappears for the first time at t=2
        clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(2)));
        stats.update(List.of(available("origin"), available("P2")));

        // Run cycles until P2 is nearly stabilized
        for (int i = 3; i < 12; i++) { // loop until t=11
            clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(i)));
            stats.update(List.of(available("origin"), available("P2")));
        }

        // At t=12, P2 stabilizes
        clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(12)));
        ScopeChangeResult result1 = stats.update(List.of(available("origin"), available("P2")));
        assertTrue(result1.scopeChanged());
        assertThat(result1.confirmedLinks(), equalTo(Set.of("P2")));
        assertThat(result1.confirmedUnlinks(), equalTo(Set.of()));

        // At t=13, P1 is confirmed as unlinked
        clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(13)));
        ScopeChangeResult result2 = stats.update(List.of(available("origin"), available("P2")));
        assertTrue(result2.scopeChanged());
        assertThat(result2.confirmedLinks(), equalTo(Set.of()));
        assertThat(result2.confirmedUnlinks(), equalTo(Set.of("P1")));

        assertThat(stats.getConfirmedAliases(), containsInAnyOrder("origin", "P2"));
    }

    public void testStaleEntryCleanup() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.EPOCH);
        CrossClusterSearchStats stats = new CrossClusterSearchStats(clock::get);

        // Baseline with origin and P1, where P1 is skipped
        stats.update(List.of(available("origin"), skipped("P1")));
        assertThat(stats.getConsecutiveUnavailable(), hasEntry("P1", 1));
        assertThat(stats.getConsecutiveUnavailable(), hasEntry("origin", 0));

        // In the next cycle, P1 is absent. Its skip entry should be removed immediately.
        clock.set(Instant.EPOCH.plus(ONE_MINUTE));
        stats.update(List.of(available("origin")));

        // The entry for P1 should be gone, but origin's should remain.
        assertThat(stats.getConsecutiveUnavailable(), not(hasEntry("P1", 1)));
        assertThat(stats.getConsecutiveUnavailable(), hasEntry("origin", 0));
    }

    public void testDelayedTimeBasedStabilization() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.EPOCH);
        CrossClusterSearchStats stats = new CrossClusterSearchStats(clock::get);

        stats.update(List.of(available("origin")));

        Instant firstSeen = Instant.EPOCH.plus(ONE_MINUTE);
        clock.set(firstSeen);
        stats.update(List.of(available("origin"), available("P1")));

        // P1 is present for 11 more cycles, but the time floor is not met
        for (int i = 2; i <= 12; i++) {
            clock.set(firstSeen.plus(Duration.ofSeconds(i * 10)));
            ScopeChangeResult result = stats.update(List.of(available("origin"), available("P1")));
            assertFalse("Should not stabilize before 5 minutes", result.scopeChanged());
        }

        // Advance the clock to meet the time floor
        clock.set(firstSeen.plus(Duration.ofMinutes(5).plusSeconds(1)));
        ScopeChangeResult result = stats.update(List.of(available("origin"), available("P1")));

        assertTrue(result.scopeChanged());
        assertThat(result.confirmedLinks(), equalTo(Set.of("P1")));
    }

    public void testBuildScopeChangeMessageLinkingOnly() {
        ScopeChangeResult result = new ScopeChangeResult(Set.of("P3", "P4"), Set.of(), true, Instant.EPOCH);
        String msg = CrossClusterSearchStats.buildScopeChangeMessage(result);
        assertThat(msg, containsString("[P3, P4] linked"));
        assertThat(msg, containsString("new data sources"));
        assertThat(msg, not(containsString("unlinked")));
    }

    public void testBuildScopeChangeMessageUnlinkingOnly() {
        ScopeChangeResult result = new ScopeChangeResult(Set.of(), Set.of("P2"), true, Instant.EPOCH);
        String msg = CrossClusterSearchStats.buildScopeChangeMessage(result);
        assertThat(msg, containsString("[P2] unlinked"));
        assertThat(msg, containsString("removed data sources"));
        assertThat(msg, not(containsString("linked,")));
    }

    public void testBuildScopeChangeMessageBothLinkingAndUnlinking() {
        ScopeChangeResult result = new ScopeChangeResult(Set.of("P3"), Set.of("P2"), true, Instant.EPOCH);
        String msg = CrossClusterSearchStats.buildScopeChangeMessage(result);
        assertThat(msg, containsString("[P3] linked"));
        assertThat(msg, containsString("[P2] unlinked"));
        assertThat(msg, containsString("Data distribution may have changed"));
    }

    public void testBuildScopeChangeMessageSortsAliases() {
        ScopeChangeResult result = new ScopeChangeResult(Set.of("Z1", "A1", "M1"), Set.of(), true, Instant.EPOCH);
        String msg = CrossClusterSearchStats.buildScopeChangeMessage(result);
        assertThat(msg, containsString("[A1, M1, Z1]"));
    }

    public void testUnlinkingConfirmationDoesNotRepeat() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.EPOCH);
        CrossClusterSearchStats stats = new CrossClusterSearchStats(clock::get);

        stats.update(List.of(available("origin"), available("P1")));

        int confirmedAtCycle = -1;
        for (int i = 1; i <= 13; i++) {
            clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(i)));
            ScopeChangeResult result = stats.update(List.of(available("origin")));
            if (result.scopeChanged()) {
                confirmedAtCycle = i;
                break;
            }
        }
        assertTrue("Should have confirmed unlinking", confirmedAtCycle > 0);

        for (int i = confirmedAtCycle + 1; i <= confirmedAtCycle + 10; i++) {
            clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(i)));
            ScopeChangeResult result = stats.update(List.of(available("origin")));
            assertFalse("Post-confirmation cycle " + i + " should not re-trigger", result.scopeChanged());
        }
    }

    public void testMultipleProjectsUnlinkingSimultaneously() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.EPOCH);
        CrossClusterSearchStats stats = new CrossClusterSearchStats(clock::get);

        stats.update(List.of(available("origin"), available("P1"), available("P2"), available("P3")));

        for (int i = 1; i <= 11; i++) {
            clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(i)));
            stats.update(List.of(available("origin")));
        }

        clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(12)));
        ScopeChangeResult result = stats.update(List.of(available("origin")));

        assertTrue(result.scopeChanged());
        assertThat(result.confirmedUnlinks(), containsInAnyOrder("P1", "P2", "P3"));
        assertThat(stats.getConfirmedAliases(), equalTo(Set.of("origin")));
    }

    public void testMixedSkippedAndFailedStates() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.EPOCH);
        CrossClusterSearchStats stats = new CrossClusterSearchStats(clock::get);

        stats.update(List.of(available("origin"), available("P1")));

        for (int i = 1; i <= 6; i++) {
            clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(i)));
            stats.update(List.of(available("origin"), skipped("P1")));
        }

        for (int i = 7; i <= 12; i++) {
            clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(i)));
            stats.update(List.of(available("origin"), failed("P1")));
        }

        assertThat(stats.getConsecutiveUnavailable(), hasEntry("P1", 12));
    }

    public void testCustomStabilizationCycles() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.EPOCH);
        CrossClusterSearchStats stats = new CrossClusterSearchStats(clock::get, 3, Duration.ZERO);

        stats.update(List.of(available("origin")));

        for (int i = 1; i <= 2; i++) {
            clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(i)));
            ScopeChangeResult result = stats.update(List.of(available("origin"), available("P1")));
            assertFalse("Cycle " + i + " should not yet stabilize with 3-cycle threshold", result.scopeChanged());
        }

        clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(3)));
        ScopeChangeResult result = stats.update(List.of(available("origin"), available("P1")));
        assertTrue(result.scopeChanged());
        assertThat(result.confirmedLinks(), equalTo(Set.of("P1")));
    }

    public void testCustomStabilizationFloor() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.EPOCH);
        CrossClusterSearchStats stats = new CrossClusterSearchStats(clock::get, 2, Duration.ofSeconds(30));

        stats.update(List.of(available("origin")));

        clock.set(Instant.EPOCH.plusSeconds(5));
        ScopeChangeResult r1 = stats.update(List.of(available("origin"), available("P1")));
        assertFalse(r1.scopeChanged());

        clock.set(Instant.EPOCH.plusSeconds(10));
        ScopeChangeResult r2 = stats.update(List.of(available("origin"), available("P1")));
        assertFalse("2 cycles met but floor of 30s not met", r2.scopeChanged());

        clock.set(Instant.EPOCH.plusSeconds(36));
        ScopeChangeResult r3 = stats.update(List.of(available("origin"), available("P1")));
        assertTrue(r3.scopeChanged());
        assertThat(r3.confirmedLinks(), equalTo(Set.of("P1")));
    }

    public void testInvalidStabilizationCyclesThrows() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.EPOCH);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new CrossClusterSearchStats(clock::get, 0, Duration.ZERO)
        );
        assertThat(e.getMessage(), containsString("stabilizationCycles must be >= 1"));
    }
}
