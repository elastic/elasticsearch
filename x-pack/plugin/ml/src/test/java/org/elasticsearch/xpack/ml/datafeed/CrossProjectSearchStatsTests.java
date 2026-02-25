/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.datafeed.CrossProjectSearchStats.CycleResult;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class CrossProjectSearchStatsTests extends ESTestCase {

    private static final Duration ONE_MINUTE = Duration.ofMinutes(1);

    private static LinkedProjectState available(String alias) {
        return new LinkedProjectState(alias, LinkedProjectState.Status.AVAILABLE, null, 10L);
    }

    private static LinkedProjectState skipped(String alias) {
        return new LinkedProjectState(alias, LinkedProjectState.Status.SKIPPED, "unavailable", 0L);
    }

    private static LinkedProjectState failed(String alias) {
        return new LinkedProjectState(alias, LinkedProjectState.Status.FAILED, "connection error", 0L);
    }

    public void testEmptyCycleIsNoOp() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.now());
        CrossProjectSearchStats stats = new CrossProjectSearchStats(clock::get);

        CycleResult result = stats.update(List.of());

        assertFalse(result.scopeChanged());
        assertThat(result.newlyStabilizedProjects(), equalTo(Set.of()));
        assertThat(result.confirmedRemovals(), equalTo(Set.of()));
        assertThat(result.changeTimestamp(), nullValue());
        assertThat(stats.getTotalProjects(), equalTo(0));
    }

    public void testFirstCycleEstablishesBaseline() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.now());
        CrossProjectSearchStats stats = new CrossProjectSearchStats(clock::get);

        CycleResult result = stats.update(List.of(available("origin"), available("P1"), available("P2")));

        assertFalse(result.scopeChanged());
        assertThat(stats.getStabilizedProjectAliases(), containsInAnyOrder("origin", "P1", "P2"));
        assertThat(stats.getTotalProjects(), equalTo(3));
        assertThat(stats.getAvailableProjects(), equalTo(3));
        assertThat(stats.getSkippedProjects(), equalTo(0));
        assertThat(stats.getAvailabilityRatio(), closeTo(1.0, 0.001));
    }

    public void testLinkingRequires12ConsecutiveCycles() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.EPOCH);
        CrossProjectSearchStats stats = new CrossProjectSearchStats(clock::get);

        stats.update(List.of(available("origin"), available("P1")));

        for (int i = 1; i <= 11; i++) {
            clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(i)));
            CycleResult result = stats.update(List.of(available("origin"), available("P1"), available("P3")));
            assertFalse("Cycle " + i + " should not trigger stabilization", result.scopeChanged());
        }

        clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(12)));
        CycleResult result = stats.update(List.of(available("origin"), available("P1"), available("P3")));

        assertTrue(result.scopeChanged());
        assertThat(result.newlyStabilizedProjects(), equalTo(Set.of("P3")));
        assertThat(result.confirmedRemovals(), equalTo(Set.of()));
        assertThat(stats.getStabilizedProjectAliases(), containsInAnyOrder("origin", "P1", "P3"));
    }

    public void testUnlinkingRequires12ConsecutiveCycles() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.EPOCH);
        CrossProjectSearchStats stats = new CrossProjectSearchStats(clock::get);

        stats.update(List.of(available("origin"), available("P1"), available("P2")));

        for (int i = 1; i <= 11; i++) {
            clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(i)));
            CycleResult result = stats.update(List.of(available("origin"), available("P1")));
            assertFalse("Cycle " + i + " should not trigger removal", result.scopeChanged());
        }

        clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(12)));
        CycleResult result = stats.update(List.of(available("origin"), available("P1")));

        assertTrue(result.scopeChanged());
        assertThat(result.confirmedRemovals(), equalTo(Set.of("P2")));
        assertThat(result.newlyStabilizedProjects(), equalTo(Set.of()));
        assertThat(stats.getStabilizedProjectAliases(), containsInAnyOrder("origin", "P1"));
    }

    public void testLinkingTimeFloorPreventsEarlyStabilization() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.EPOCH);
        CrossProjectSearchStats stats = new CrossProjectSearchStats(clock::get);

        stats.update(List.of(available("origin")));

        Instant firstSeen = Instant.EPOCH.plusSeconds(10);
        clock.set(firstSeen);
        stats.update(List.of(available("origin"), available("P1")));

        for (int i = 2; i <= 12; i++) {
            clock.set(Instant.EPOCH.plusSeconds(i * 10));
            CycleResult result = stats.update(List.of(available("origin"), available("P1")));
            assertFalse("Should not stabilize before 5 minutes even with 12+ cycles", result.scopeChanged());
        }

        clock.set(firstSeen.plus(Duration.ofMinutes(5)));
        CycleResult result = stats.update(List.of(available("origin"), available("P1")));

        assertTrue(result.scopeChanged());
        assertThat(result.newlyStabilizedProjects(), equalTo(Set.of("P1")));
    }

    public void testUnlinkingTimeFloorPreventsEarlyStabilization() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.EPOCH);
        CrossProjectSearchStats stats = new CrossProjectSearchStats(clock::get);

        stats.update(List.of(available("origin"), available("P1")));

        Instant firstAbsent = Instant.EPOCH.plusSeconds(10);
        clock.set(firstAbsent);
        stats.update(List.of(available("origin")));

        for (int i = 2; i <= 12; i++) {
            clock.set(Instant.EPOCH.plusSeconds(i * 10));
            CycleResult result = stats.update(List.of(available("origin")));
            assertFalse("Should not confirm removal before 5 minutes even with 12+ cycles", result.scopeChanged());
        }

        clock.set(firstAbsent.plus(Duration.ofMinutes(5)));
        CycleResult result = stats.update(List.of(available("origin")));

        assertTrue(result.scopeChanged());
        assertThat(result.confirmedRemovals(), equalTo(Set.of("P1")));
    }

    public void testLinkingPresenceGapResetsCounter() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.EPOCH);
        CrossProjectSearchStats stats = new CrossProjectSearchStats(clock::get);

        stats.update(List.of(available("origin")));

        for (int i = 1; i <= 5; i++) {
            clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(i)));
            stats.update(List.of(available("origin"), available("P3")));
        }

        clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(6)));
        stats.update(List.of(available("origin")));

        for (int i = 7; i <= 17; i++) {
            clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(i)));
            CycleResult result = stats.update(List.of(available("origin"), available("P3")));
            assertFalse("Cycle " + i + " after reset should not yet stabilize", result.scopeChanged());
        }

        clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(18)));
        CycleResult result = stats.update(List.of(available("origin"), available("P3")));

        assertTrue(result.scopeChanged());
        assertThat(result.newlyStabilizedProjects(), equalTo(Set.of("P3")));
        assertThat(result.changeTimestamp(), equalTo(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(7))));
    }

    public void testUnlinkingAbsenceGapResetsCounter() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.EPOCH);
        CrossProjectSearchStats stats = new CrossProjectSearchStats(clock::get);

        stats.update(List.of(available("origin"), available("P2")));

        for (int i = 1; i <= 5; i++) {
            clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(i)));
            stats.update(List.of(available("origin")));
        }

        clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(6)));
        stats.update(List.of(available("origin"), available("P2")));

        for (int i = 7; i <= 17; i++) {
            clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(i)));
            CycleResult result = stats.update(List.of(available("origin")));
            assertFalse("Cycle " + i + " after reset should not yet confirm removal", result.scopeChanged());
        }

        clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(18)));
        CycleResult result = stats.update(List.of(available("origin")));

        assertTrue(result.scopeChanged());
        assertThat(result.confirmedRemovals(), equalTo(Set.of("P2")));
        assertThat(result.changeTimestamp(), equalTo(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(7))));
    }

    public void testLinkingFlipFlapProducesNoConfirmation() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.EPOCH);
        CrossProjectSearchStats stats = new CrossProjectSearchStats(clock::get);

        stats.update(List.of(available("origin")));

        for (int i = 1; i <= 24; i++) {
            clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(i)));
            List<LinkedProjectState> cycle = (i % 2 == 1) ? List.of(available("origin"), available("P3")) : List.of(available("origin"));
            CycleResult result = stats.update(cycle);
            assertFalse("Flip-flap cycle " + i + " should never confirm", result.scopeChanged());
        }

        assertThat(stats.getStabilizedProjectAliases(), equalTo(Set.of("origin")));
    }

    public void testUnlinkingFlipFlapProducesNoConfirmation() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.EPOCH);
        CrossProjectSearchStats stats = new CrossProjectSearchStats(clock::get);

        stats.update(List.of(available("origin"), available("P2")));

        for (int i = 1; i <= 24; i++) {
            clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(i)));
            List<LinkedProjectState> cycle = (i % 2 == 1) ? List.of(available("origin")) : List.of(available("origin"), available("P2"));
            CycleResult result = stats.update(cycle);
            assertFalse("Flip-flap cycle " + i + " should never confirm", result.scopeChanged());
        }

        assertThat(stats.getStabilizedProjectAliases(), containsInAnyOrder("origin", "P2"));
    }

    public void testSimultaneousLinkingAndUnlinking() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.EPOCH);
        CrossProjectSearchStats stats = new CrossProjectSearchStats(clock::get);

        stats.update(List.of(available("origin"), available("P1"), available("P2")));

        for (int i = 1; i <= 11; i++) {
            clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(i)));
            stats.update(List.of(available("origin"), available("P1"), available("P3")));
        }

        clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(12)));
        CycleResult result = stats.update(List.of(available("origin"), available("P1"), available("P3")));

        assertTrue(result.scopeChanged());
        assertThat(result.newlyStabilizedProjects(), equalTo(Set.of("P3")));
        assertThat(result.confirmedRemovals(), equalTo(Set.of("P2")));
        assertThat(stats.getStabilizedProjectAliases(), containsInAnyOrder("origin", "P1", "P3"));
    }

    public void testMultipleProjectsStabilizingSimultaneously() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.EPOCH);
        CrossProjectSearchStats stats = new CrossProjectSearchStats(clock::get);

        stats.update(List.of(available("origin")));

        for (int i = 1; i <= 11; i++) {
            clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(i)));
            stats.update(List.of(available("origin"), available("P1"), available("P2"), available("P3")));
        }

        clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(12)));
        CycleResult result = stats.update(List.of(available("origin"), available("P1"), available("P2"), available("P3")));

        assertTrue(result.scopeChanged());
        assertThat(result.newlyStabilizedProjects(), containsInAnyOrder("P1", "P2", "P3"));
        assertThat(stats.getStabilizedProjectAliases(), containsInAnyOrder("origin", "P1", "P2", "P3"));
    }

    public void testSingleProjectDatafeed() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.EPOCH);
        CrossProjectSearchStats stats = new CrossProjectSearchStats(clock::get);

        stats.update(List.of(available("origin")));
        assertThat(stats.getStabilizedProjectAliases(), equalTo(Set.of("origin")));
        assertThat(stats.getAvailabilityRatio(), closeTo(1.0, 0.001));

        for (int i = 1; i <= 12; i++) {
            clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(i)));
            CycleResult result = stats.update(List.of(available("origin")));
            assertFalse(result.scopeChanged());
        }
    }

    public void testSkippedProjectCountsAsPresent() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.EPOCH);
        CrossProjectSearchStats stats = new CrossProjectSearchStats(clock::get);

        stats.update(List.of(available("origin"), available("P1")));

        for (int i = 1; i <= 11; i++) {
            clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(i)));
            stats.update(List.of(available("origin"), skipped("P1"), available("P2")));
        }

        clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(12)));
        CycleResult result = stats.update(List.of(available("origin"), skipped("P1"), available("P2")));

        assertTrue(result.scopeChanged());
        assertThat(result.newlyStabilizedProjects(), equalTo(Set.of("P2")));
        assertThat(result.confirmedRemovals(), equalTo(Set.of()));
        assertThat(stats.getStabilizedProjectAliases(), containsInAnyOrder("origin", "P1", "P2"));
    }

    public void testFailedProjectCountsAsPresent() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.EPOCH);
        CrossProjectSearchStats stats = new CrossProjectSearchStats(clock::get);

        stats.update(List.of(available("origin"), available("P1")));

        for (int i = 1; i <= 12; i++) {
            clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(i)));
            CycleResult result = stats.update(List.of(available("origin"), failed("P1")));
            assertFalse(result.scopeChanged());
        }

        assertThat(stats.getStabilizedProjectAliases(), containsInAnyOrder("origin", "P1"));
    }

    public void testConsecutiveSkipsTracking() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.EPOCH);
        CrossProjectSearchStats stats = new CrossProjectSearchStats(clock::get);

        stats.update(List.of(available("origin"), available("P1")));

        clock.set(Instant.EPOCH.plus(ONE_MINUTE));
        stats.update(List.of(available("origin"), skipped("P1")));
        assertThat(stats.getConsecutiveSkips(), hasEntry("P1", 1));

        clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(2)));
        stats.update(List.of(available("origin"), skipped("P1")));
        assertThat(stats.getConsecutiveSkips(), hasEntry("P1", 2));

        clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(3)));
        stats.update(List.of(available("origin"), available("P1")));
        assertThat(stats.getConsecutiveSkips(), hasEntry("P1", 0));
    }

    public void testAvailabilityRatioUpdatesPerCycle() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.EPOCH);
        CrossProjectSearchStats stats = new CrossProjectSearchStats(clock::get);

        stats.update(List.of(available("origin"), available("P1"), available("P2"), available("P3"), available("P4")));
        assertThat(stats.getAvailabilityRatio(), closeTo(1.0, 0.001));

        clock.set(Instant.EPOCH.plus(ONE_MINUTE));
        stats.update(List.of(available("origin"), skipped("P1"), available("P2"), available("P3"), available("P4")));
        assertThat(stats.getTotalProjects(), equalTo(5));
        assertThat(stats.getAvailableProjects(), equalTo(4));
        assertThat(stats.getSkippedProjects(), equalTo(1));
        assertThat(stats.getAvailabilityRatio(), closeTo(0.8, 0.001));
    }

    public void testChangeTimestampReflectsFirstObservation() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.EPOCH);
        CrossProjectSearchStats stats = new CrossProjectSearchStats(clock::get);

        stats.update(List.of(available("origin")));

        Instant firstSeen = Instant.EPOCH.plus(ONE_MINUTE);
        CycleResult confirmedResult = null;

        for (int i = 1; i <= 12; i++) {
            clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(i)));
            CycleResult result = stats.update(List.of(available("origin"), available("P1")));
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
        CrossProjectSearchStats stats = new CrossProjectSearchStats(clock::get);

        stats.update(List.of(available("origin"), available("P1")));

        Instant p1AbsentStart = Instant.EPOCH.plus(ONE_MINUTE);

        CycleResult confirmedResult = null;
        for (int i = 1; i <= 13; i++) {
            clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(i)));
            CycleResult result = stats.update(List.of(available("origin"), available("P2")));
            if (result.scopeChanged()) {
                confirmedResult = result;
                break;
            }
        }

        assertThat(confirmedResult, notNullValue());
        assertTrue(confirmedResult.scopeChanged());
        assertThat(confirmedResult.newlyStabilizedProjects(), equalTo(Set.of("P2")));
        assertThat(confirmedResult.confirmedRemovals(), equalTo(Set.of("P1")));
        assertThat(confirmedResult.changeTimestamp(), equalTo(p1AbsentStart));
    }

    public void testNoChangeAfterBaselineWhenProjectSetUnchanged() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.EPOCH);
        CrossProjectSearchStats stats = new CrossProjectSearchStats(clock::get);

        stats.update(List.of(available("origin"), available("P1"), available("P2")));

        for (int i = 1; i <= 20; i++) {
            clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(i)));
            CycleResult result = stats.update(List.of(available("origin"), available("P1"), available("P2")));
            assertFalse(result.scopeChanged());
        }
    }

    public void testLinkingConfirmationDoesNotRepeat() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.EPOCH);
        CrossProjectSearchStats stats = new CrossProjectSearchStats(clock::get);

        stats.update(List.of(available("origin")));

        int confirmedAtCycle = -1;
        for (int i = 1; i <= 13; i++) {
            clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(i)));
            CycleResult result = stats.update(List.of(available("origin"), available("P1")));
            if (result.scopeChanged()) {
                confirmedAtCycle = i;
                break;
            }
        }
        assertTrue("Should have confirmed linking", confirmedAtCycle > 0);

        for (int i = confirmedAtCycle + 1; i <= confirmedAtCycle + 10; i++) {
            clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(i)));
            CycleResult result = stats.update(List.of(available("origin"), available("P1")));
            assertFalse("Post-confirmation cycle " + i + " should not re-trigger", result.scopeChanged());
        }
    }

    public void testStabilizedProjectAliasesReturnsDefensiveCopy() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.EPOCH);
        CrossProjectSearchStats stats = new CrossProjectSearchStats(clock::get);

        stats.update(List.of(available("origin"), available("P1")));

        Set<String> aliases = stats.getStabilizedProjectAliases();
        expectThrows(UnsupportedOperationException.class, () -> aliases.add("hacked"));
    }

    public void testConsecutiveSkipsReturnsDefensiveCopy() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.EPOCH);
        CrossProjectSearchStats stats = new CrossProjectSearchStats(clock::get);

        stats.update(List.of(available("origin"), skipped("P1")));

        var skips = stats.getConsecutiveSkips();
        expectThrows(UnsupportedOperationException.class, () -> skips.put("hacked", 99));
    }

    public void testFluctuatingProjectPresence() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.EPOCH);
        CrossProjectSearchStats stats = new CrossProjectSearchStats(clock::get);

        stats.update(List.of(available("origin")));

        for (int i = 1; i <= 30; i++) {
            clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(i)));
            List<LinkedProjectState> cycle = (i % 3 == 0) ? List.of(available("origin")) : List.of(available("origin"), available("P1"));
            CycleResult result = stats.update(cycle);
            assertFalse("Fluctuating cycle " + i + " should not confirm", result.scopeChanged());
        }
        assertThat(stats.getStabilizedProjectAliases(), equalTo(Set.of("origin")));
    }

    public void testSimultaneousLinkingAndUnlinkingWithDifferentStartTimes() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.EPOCH);
        CrossProjectSearchStats stats = new CrossProjectSearchStats(clock::get);

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
        CycleResult result1 = stats.update(List.of(available("origin"), available("P2")));
        assertTrue(result1.scopeChanged());
        assertThat(result1.newlyStabilizedProjects(), equalTo(Set.of("P2")));
        assertThat(result1.confirmedRemovals(), equalTo(Set.of()));

        // At t=13, P1 is confirmed as unlinked
        clock.set(Instant.EPOCH.plus(ONE_MINUTE.multipliedBy(13)));
        CycleResult result2 = stats.update(List.of(available("origin"), available("P2")));
        assertTrue(result2.scopeChanged());
        assertThat(result2.newlyStabilizedProjects(), equalTo(Set.of()));
        assertThat(result2.confirmedRemovals(), equalTo(Set.of("P1")));

        assertThat(stats.getStabilizedProjectAliases(), containsInAnyOrder("origin", "P2"));
    }

    public void testStaleEntryCleanup() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.EPOCH);
        CrossProjectSearchStats stats = new CrossProjectSearchStats(clock::get);

        // Baseline with origin and P1, where P1 is skipped
        stats.update(List.of(available("origin"), skipped("P1")));
        assertThat(stats.getConsecutiveSkips(), hasEntry("P1", 1));
        assertThat(stats.getConsecutiveSkips(), hasEntry("origin", 0));

        // In the next cycle, P1 is absent. Its skip entry should be removed immediately.
        clock.set(Instant.EPOCH.plus(ONE_MINUTE));
        stats.update(List.of(available("origin")));

        // The entry for P1 should be gone, but origin's should remain.
        assertThat(stats.getConsecutiveSkips(), not(hasEntry("P1", 1)));
        assertThat(stats.getConsecutiveSkips(), hasEntry("origin", 0));
    }

    public void testDelayedTimeBasedStabilization() {
        AtomicReference<Instant> clock = new AtomicReference<>(Instant.EPOCH);
        CrossProjectSearchStats stats = new CrossProjectSearchStats(clock::get);

        stats.update(List.of(available("origin")));

        Instant firstSeen = Instant.EPOCH.plus(ONE_MINUTE);
        clock.set(firstSeen);
        stats.update(List.of(available("origin"), available("P1")));

        // P1 is present for 11 more cycles, but the time floor is not met
        for (int i = 2; i <= 12; i++) {
            clock.set(firstSeen.plus(Duration.ofSeconds(i * 10)));
            CycleResult result = stats.update(List.of(available("origin"), available("P1")));
            assertFalse("Should not stabilize before 5 minutes", result.scopeChanged());
        }

        // Advance the clock to meet the time floor
        clock.set(firstSeen.plus(Duration.ofMinutes(5).plusSeconds(1)));
        CycleResult result = stats.update(List.of(available("origin"), available("P1")));

        assertTrue(result.scopeChanged());
        assertThat(result.newlyStabilizedProjects(), equalTo(Set.of("P1")));
    }
}
