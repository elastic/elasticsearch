/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.indices.recovery.UploadConcurrencyController.Decision;
import org.elasticsearch.indices.recovery.UploadConcurrencyController.Signals;
import org.elasticsearch.test.ESTestCase;

import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.indices.recovery.UploadConcurrencyController.CONTENDED_CPU_PRESSURE;
import static org.elasticsearch.indices.recovery.UploadConcurrencyController.ERROR_COOLDOWN_INTERVALS;
import static org.elasticsearch.indices.recovery.UploadConcurrencyController.QUIET_CPU_PRESSURE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class UploadConcurrencyControllerTests extends ESTestCase {

    private static final long INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(5);
    private static final int FLOOR = 10;
    private static final int CEILING = 20;

    private final UploadConcurrencyController controller = new UploadConcurrencyController(FLOOR, CEILING);

    /** every signal known and quiet, no errors */
    private static Signals quiet() {
        return new Signals(
            INTERVAL_NANOS,
            OptionalDouble.of(randomDoubleBetween(0.0, QUIET_CPU_PRESSURE, false)),
            OptionalLong.of(0L),
            0L,
            0L
        );
    }

    private static Signals withCpu(Signals s, OptionalDouble cpuPressure, OptionalLong throttledMicros) {
        return new Signals(s.intervalNanos(), cpuPressure, throttledMicros, s.readErrors(), s.uploadErrors());
    }

    private static Signals withErrors(Signals s, long readErrors, long uploadErrors) {
        return new Signals(s.intervalNanos(), s.cpuPressure(), s.throttledMicros(), readErrors, uploadErrors);
    }

    private static Signals contended() {
        return withCpu(quiet(), OptionalDouble.of(Math.nextUp(CONTENDED_CPU_PRESSURE)), OptionalLong.of(0L));
    }

    private static Signals erroring() {
        return withErrors(quiet(), 0L, randomLongBetween(1, 3));
    }

    private void climbTo(int target) {
        while (controller.getTarget() < target) {
            controller.onInterval(quiet());
        }
        assertThat(controller.getTarget(), equalTo(target));
    }

    public void testStartsAtFloor() {
        assertThat(controller.getTarget(), equalTo(FLOOR));
        expectThrows(IllegalArgumentException.class, () -> new UploadConcurrencyController(0, 10));
        expectThrows(IllegalArgumentException.class, () -> new UploadConcurrencyController(10, 9));
    }

    public void testClimbsToTheCeilingOneUploadPerInterval() {
        for (int expected = FLOOR + 1; expected <= CEILING; expected++) {
            final Decision decision = controller.onInterval(quiet());
            assertThat(decision.action(), equalTo("raise"));
            assertThat(decision.target(), equalTo(expected));
            assertThat(controller.getTarget(), equalTo(expected));
        }
        final Decision atCeiling = controller.onInterval(quiet());
        assertThat(atCeiling.action(), equalTo("hold"));
        assertThat(atCeiling.reason(), equalTo("at ceiling"));
        assertThat(controller.getTarget(), equalTo(CEILING));
    }

    public void testTargetIsTheCeilingOnLargeAndSmallNodes() {
        // 10 on a node below 8GiB: the floor is the ceiling, nothing to climb to
        final var small = new UploadConcurrencyController(10, 10);
        assertThat(small.onInterval(quiet()).target(), equalTo(10));
        assertThat(small.onInterval(quiet()).reason(), equalTo("at ceiling"));
        // 20 from 8GiB
        final var large = new UploadConcurrencyController(10, 20);
        for (int i = 0; i < 30; i++) {
            large.onInterval(quiet());
        }
        assertThat(large.getTarget(), equalTo(20));
    }

    public void testCeilingCappedBySetting() {
        // a setting below the node's target caps it, and a setting above it does not raise it
        controller.setCeiling(15);
        assertThat(controller.getCeiling(), equalTo(15));
        climbTo(15);
        assertThat(controller.onInterval(quiet()).reason(), equalTo("at ceiling"));
        assertThat(controller.getTarget(), equalTo(15));
        // raised again, it climbs by one per interval and does not jump
        controller.setCeiling(20);
        assertThat(controller.onInterval(quiet()).target(), equalTo(16));
        assertThat(controller.onInterval(quiet()).target(), equalTo(17));
    }

    public void testLoweredCeilingCutsTheTarget() {
        climbTo(CEILING);
        controller.setCeiling(13);
        final Decision decision = controller.onInterval(quiet());
        assertThat(decision.action(), equalTo("cut"));
        assertThat(decision.reason(), containsString("ceiling lowered"));
        assertThat(decision.target(), equalTo(13));
        assertThat(controller.getCeiling(), equalTo(13));
    }

    public void testCeilingNeverBelowFloor() {
        controller.setCeiling(1);
        assertThat(controller.getCeiling(), equalTo(FLOOR));
        assertThat(controller.onInterval(quiet()).reason(), equalTo("at ceiling"));
        assertThat(controller.getTarget(), equalTo(FLOOR));
    }

    public void testHeapGuardedNodeStaysAtTodaysValue() {
        // today's value is below 10 on a node with little heap, and so is the ceiling: there is nothing above it
        final int todays = randomIntBetween(1, 5);
        final var guarded = new UploadConcurrencyController(todays, todays);
        for (int i = 0; i < 20; i++) {
            guarded.onInterval(quiet());
            assertThat(guarded.getTarget(), equalTo(todays));
        }
        // it backs off no lower than today's value either
        assertThat(guarded.onInterval(contended()).target(), equalTo(todays));
        assertThat(guarded.onInterval(erroring()).target(), equalTo(todays));
    }

    public void testCutsToThreeQuartersOnCpuPressure() {
        climbTo(CEILING);
        final Decision decision = controller.onInterval(contended());
        assertThat(decision.action(), equalTo("cut"));
        assertThat(decision.reason(), containsString("cpu pressure"));
        assertThat(decision.target(), equalTo(15));
        assertThat(controller.getTarget(), equalTo(15));
    }

    public void testCutsToThreeQuartersOnThrottling() {
        climbTo(CEILING);
        // more than 1% of the interval
        final long throttledMicros = randomLongBetween(50_001L, 5_000_000L);
        final Decision decision = controller.onInterval(withCpu(quiet(), quiet().cpuPressure(), OptionalLong.of(throttledMicros)));
        assertThat(decision.action(), equalTo("cut"));
        assertThat(decision.reason(), containsString("cpu throttled"));
        assertThat(decision.target(), equalTo(15));
    }

    public void testContentionThresholds() {
        climbTo(CEILING);
        // the thresholds themselves are not contention: 1% of the interval throttled, and the pressure threshold
        final long onePercentMicros = TimeUnit.NANOSECONDS.toMicros(INTERVAL_NANOS) / 100;
        final Decision atThreshold = controller.onInterval(
            withCpu(quiet(), OptionalDouble.of(CONTENDED_CPU_PRESSURE), OptionalLong.of(onePercentMicros))
        );
        assertThat(atThreshold.action(), equalTo("hold"));
        assertThat(controller.getTarget(), equalTo(CEILING));
        // just above either cuts
        assertThat(controller.onInterval(contended()).action(), equalTo("cut"));
        controller.reset();
        climbTo(CEILING);
        final Decision throttled = controller.onInterval(withCpu(quiet(), quiet().cpuPressure(), OptionalLong.of(onePercentMicros + 1)));
        assertThat(throttled.action(), equalTo("cut"));
    }

    public void testContentionReasonNamesEverySignal() {
        climbTo(CEILING);
        final Decision decision = controller.onInterval(withCpu(quiet(), OptionalDouble.of(0.5), OptionalLong.of(1_000_000L)));
        assertThat(decision.reason(), containsString("cpu pressure"));
        assertThat(decision.reason(), containsString("cpu throttled"));
    }

    public void testContentionCutStopsAtFloor() {
        climbTo(13);
        assertThat(controller.onInterval(contended()).target(), equalTo(FLOOR));
        for (int i = 0; i < 3; i++) {
            final Decision decision = controller.onInterval(contended());
            assertThat(decision.action(), equalTo("cut"));
            assertThat(decision.target(), equalTo(FLOOR));
        }
    }

    public void testHalvesOnUploadErrors() {
        climbTo(CEILING);
        final long readErrors = randomLongBetween(0, 3);
        final long uploadErrors = readErrors == 0 ? randomLongBetween(1, 3) : randomLongBetween(0, 3);
        final Decision decision = controller.onInterval(withErrors(quiet(), readErrors, uploadErrors));
        assertThat(decision.action(), equalTo("cut"));
        assertThat(decision.target(), equalTo(CEILING / 2));
        assertThat(decision.reason(), containsString(readErrors + " reading the source"));
        assertThat(decision.reason(), containsString(uploadErrors + " writing to the repository"));
    }

    public void testErrorCutStopsAtFloor() {
        climbTo(FLOOR + 3);
        // half of 13 is below the floor
        assertThat(controller.onInterval(erroring()).target(), equalTo(FLOOR));
        assertThat(controller.onInterval(erroring()).target(), equalTo(FLOOR));
    }

    public void testErrorCooldownThenGradualRecovery() {
        climbTo(CEILING);
        controller.onInterval(erroring());
        assertThat(controller.getTarget(), equalTo(FLOOR));
        // no recovery during the cooldown, however quiet the node is
        for (int i = 0; i < ERROR_COOLDOWN_INTERVALS; i++) {
            final Decision hold = controller.onInterval(quiet());
            assertThat(hold.action(), equalTo("hold"));
            assertThat(hold.reason(), equalTo("error cooldown"));
            assertThat(hold.target(), equalTo(FLOOR));
        }
        // then one more per interval
        assertThat(controller.onInterval(quiet()).target(), equalTo(FLOOR + 1));
        assertThat(controller.onInterval(quiet()).target(), equalTo(FLOOR + 2));
    }

    public void testErrorsInTheCooldownRestartIt() {
        climbTo(CEILING);
        controller.onInterval(erroring());
        for (int i = 0; i < ERROR_COOLDOWN_INTERVALS - 1; i++) {
            assertThat(controller.onInterval(quiet()).action(), equalTo("hold"));
        }
        controller.onInterval(erroring());
        for (int i = 0; i < ERROR_COOLDOWN_INTERVALS; i++) {
            assertThat(controller.onInterval(quiet()).reason(), equalTo("error cooldown"));
        }
        assertThat(controller.onInterval(quiet()).action(), equalTo("raise"));
    }

    public void testErrorsBeatContention() {
        climbTo(CEILING);
        final Decision decision = controller.onInterval(withErrors(contended(), 0L, 1L));
        assertThat(decision.target(), equalTo(CEILING / 2));
        assertThat(decision.reason(), containsString("upload errors"));
    }

    public void testGradualRecoveryAfterContention() {
        climbTo(CEILING);
        assertThat(controller.onInterval(contended()).target(), equalTo(15));
        // there is no cooldown after contention, but the target never jumps straight back
        for (int expected = 16; expected <= CEILING; expected++) {
            final Decision decision = controller.onInterval(quiet());
            assertThat(decision.action(), equalTo("raise"));
            assertThat(decision.target(), equalTo(expected));
        }
        assertThat(controller.onInterval(quiet()).reason(), equalTo("at ceiling"));
    }

    public void testNoRecoveryWhileCpuIsNotQuiet() {
        climbTo(CEILING);
        controller.onInterval(contended());
        final int target = controller.getTarget();
        // between quiet and contended, the target holds
        final Decision pressure = controller.onInterval(withCpu(quiet(), OptionalDouble.of(QUIET_CPU_PRESSURE), OptionalLong.of(0L)));
        assertThat(pressure.action(), equalTo("hold"));
        assertThat(pressure.reason(), containsString("cpu pressure"));
        // brief throttling, 1% of the interval or less, stops recovery but does not cut
        final long brief = TimeUnit.NANOSECONDS.toMicros(INTERVAL_NANOS) / 100;
        final Decision throttled = controller.onInterval(
            withCpu(quiet(), quiet().cpuPressure(), OptionalLong.of(randomLongBetween(1L, brief)))
        );
        assertThat(throttled.action(), equalTo("hold"));
        assertThat(throttled.reason(), containsString("cpu throttled"));
        assertThat(controller.getTarget(), equalTo(target));
        // just below the quiet threshold recovers
        final Signals justQuiet = withCpu(quiet(), OptionalDouble.of(Math.nextDown(QUIET_CPU_PRESSURE)), OptionalLong.of(0L));
        assertThat(controller.onInterval(justQuiet).target(), equalTo(target + 1));
    }

    public void testProbeUnavailableNeverRaisesAboveTodaysValue() {
        final var noPressure = withCpu(quiet(), OptionalDouble.empty(), OptionalLong.of(0L));
        final var noThrottling = withCpu(quiet(), OptionalDouble.of(0.0), OptionalLong.empty());
        final var neither = withCpu(quiet(), OptionalDouble.empty(), OptionalLong.empty());
        for (int i = 0; i < 20; i++) {
            final Signals signals = randomFrom(noPressure, noThrottling, neither);
            final Decision decision = controller.onInterval(signals);
            assertThat(decision.action(), equalTo("hold"));
            assertThat(decision.reason(), containsString("unavailable"));
            assertThat(controller.getTarget(), equalTo(FLOOR));
        }
        assertThat(controller.onInterval(noPressure).reason(), equalTo("cpu pressure unavailable"));
        assertThat(controller.onInterval(noThrottling).reason(), equalTo("cpu throttling unavailable"));
    }

    public void testUnavailableSignalsStillBackOffOnErrorsAndOnTheOtherSignal() {
        climbTo(CEILING);
        // a signal that is not available does not count as contention, and does not move the target
        assertThat(controller.onInterval(withCpu(quiet(), OptionalDouble.empty(), OptionalLong.empty())).target(), equalTo(CEILING));
        // the one that is available still does
        final Signals onlyPressure = withCpu(quiet(), OptionalDouble.of(1.0), OptionalLong.empty());
        assertThat(controller.onInterval(onlyPressure).target(), equalTo(15));
        final Signals onlyThrottling = withCpu(quiet(), OptionalDouble.empty(), OptionalLong.of(1_000_000L));
        assertThat(controller.onInterval(onlyThrottling).target(), equalTo(15 * 3 / 4));
        // and errors need no probe at all
        climbTo(CEILING);
        assertThat(
            controller.onInterval(withErrors(withCpu(quiet(), OptionalDouble.empty(), OptionalLong.empty()), 1L, 0L)).target(),
            equalTo(CEILING / 2)
        );
    }

    public void testReset() {
        controller.onInterval(quiet());
        assertThat(controller.getTarget(), equalTo(FLOOR + 1));
        controller.onInterval(erroring());
        controller.reset();
        assertThat(controller.getTarget(), equalTo(FLOOR));
        // no cooldown after a reset
        assertThat(controller.onInterval(quiet()).action(), equalTo("raise"));
    }
}
