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
import static org.elasticsearch.indices.recovery.UploadConcurrencyController.CONTENDED_WRITE_QUEUE_WAIT_MILLIS;
import static org.elasticsearch.indices.recovery.UploadConcurrencyController.CONTENTION_COOLDOWN_INTERVALS;
import static org.elasticsearch.indices.recovery.UploadConcurrencyController.ERROR_COOLDOWN_INTERVALS;
import static org.elasticsearch.indices.recovery.UploadConcurrencyController.QUIET_CPU_PRESSURE;
import static org.elasticsearch.indices.recovery.UploadConcurrencyController.QUIET_WRITE_QUEUE_WAIT_MILLIS;
import static org.elasticsearch.indices.recovery.UploadConcurrencyController.REVERT_COOLDOWN_INTERVALS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class UploadConcurrencyControllerTests extends ESTestCase {

    private static final long INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(5);
    private static final int FLOOR = 10;
    private static final int CEILING = 140;

    private final UploadConcurrencyController controller = new UploadConcurrencyController(FLOOR, CEILING);

    /** queued work, no limiter waits, every other signal known and quiet */
    private Signals quiet(double throughput) {
        return quiet(randomIntBetween(1, 100), controller.getTarget(), throughput);
    }

    private static Signals quiet(int queued, int running, double throughput) {
        return new Signals(
            queued,
            running,
            throughput,
            0L,
            INTERVAL_NANOS,
            OptionalDouble.of(randomDoubleBetween(0.0, QUIET_CPU_PRESSURE, false)),
            OptionalLong.of(0L),
            OptionalDouble.of(randomDoubleBetween(0.0, QUIET_WRITE_QUEUE_WAIT_MILLIS, false)),
            false,
            0L,
            0L
        );
    }

    private static Signals with(Signals s, OptionalDouble cpuPressure, OptionalLong throttledMicros, OptionalDouble writeQueueWaitMillis) {
        return new Signals(
            s.queued(),
            s.running(),
            s.throughputBytesPerSec(),
            s.limiterPauseNanos(),
            s.intervalNanos(),
            cpuPressure,
            throttledMicros,
            writeQueueWaitMillis,
            s.writeStalled(),
            s.readErrors(),
            s.uploadErrors()
        );
    }

    private static Signals withErrors(Signals s, long readErrors, long uploadErrors) {
        return new Signals(
            s.queued(),
            s.running(),
            s.throughputBytesPerSec(),
            s.limiterPauseNanos(),
            s.intervalNanos(),
            s.cpuPressure(),
            s.throttledMicros(),
            s.writeQueueWaitMillis(),
            s.writeStalled(),
            readErrors,
            uploadErrors
        );
    }

    private static Signals withPause(Signals s, long limiterPauseNanos) {
        return new Signals(
            s.queued(),
            s.running(),
            s.throughputBytesPerSec(),
            limiterPauseNanos,
            s.intervalNanos(),
            s.cpuPressure(),
            s.throttledMicros(),
            s.writeQueueWaitMillis(),
            s.writeStalled(),
            s.readErrors(),
            s.uploadErrors()
        );
    }

    public void testStartsAtFloor() {
        assertThat(controller.getTarget(), equalTo(FLOOR));
        expectThrows(IllegalArgumentException.class, () -> new UploadConcurrencyController(0, 10));
        expectThrows(IllegalArgumentException.class, () -> new UploadConcurrencyController(10, 9));
    }

    public void testRaisesWhenQueuedAndQuiet() {
        final Decision decision = controller.onInterval(quiet(100.0));
        assertThat(decision.action(), equalTo("raise"));
        assertThat(decision.target(), equalTo(FLOOR + 1));
        assertThat(controller.getTarget(), equalTo(FLOOR + 1));
    }

    public void testRaisesByOneWhateverTheTarget() {
        // additive increase: one more per interval, however large the target is
        final var big = new UploadConcurrencyController(100, 140);
        assertThat(big.onInterval(quiet(1, 100, 1000.0)).target(), equalTo(101));
        assertThat(big.onInterval(quiet(1, 101, 1100.0)).target(), equalTo(102));
        assertThat(big.onInterval(quiet(1, 102, 1200.0)).target(), equalTo(103));
    }

    public void testQuietThresholds() {
        final Signals base = quiet(100.0);
        // just below the quiet thresholds raises
        assertThat(
            controller.onInterval(
                with(
                    base,
                    OptionalDouble.of(Math.nextDown(QUIET_CPU_PRESSURE)),
                    OptionalLong.of(0L),
                    OptionalDouble.of(Math.nextDown(QUIET_WRITE_QUEUE_WAIT_MILLIS))
                )
            ).action(),
            equalTo("raise")
        );
        controller.reset();

        // at the thresholds it holds, with the signal as the reason
        final Decision pressure = controller.onInterval(
            with(base, OptionalDouble.of(QUIET_CPU_PRESSURE), OptionalLong.of(0L), base.writeQueueWaitMillis())
        );
        assertThat(pressure.action(), equalTo("hold"));
        assertThat(pressure.reason(), containsString("cpu pressure"));
        final Decision wait = controller.onInterval(
            with(base, base.cpuPressure(), OptionalLong.of(0L), OptionalDouble.of(QUIET_WRITE_QUEUE_WAIT_MILLIS))
        );
        assertThat(wait.action(), equalTo("hold"));
        assertThat(wait.reason(), containsString("write queue wait"));
        assertThat(controller.getTarget(), equalTo(FLOOR));
    }

    public void testNeverRaisesBlind() {
        final Signals base = quiet(100.0);
        final var empty = OptionalDouble.empty();
        final Decision noPressure = controller.onInterval(with(base, empty, base.throttledMicros(), base.writeQueueWaitMillis()));
        assertThat(noPressure.action(), equalTo("hold"));
        assertThat(noPressure.reason(), equalTo("cpu pressure unavailable"));
        final Decision noThrottling = controller.onInterval(
            with(base, base.cpuPressure(), OptionalLong.empty(), base.writeQueueWaitMillis())
        );
        assertThat(noThrottling.action(), equalTo("hold"));
        assertThat(noThrottling.reason(), equalTo("cpu throttling unavailable"));
        final Decision noWait = controller.onInterval(with(base, base.cpuPressure(), base.throttledMicros(), empty));
        assertThat(noWait.action(), equalTo("hold"));
        assertThat(noWait.reason(), equalTo("write queue wait unavailable"));
        assertThat(controller.getTarget(), equalTo(FLOOR));
    }

    public void testUnavailableSignalsDoNotCut() {
        final Decision decision = controller.onInterval(
            with(quiet(100.0), OptionalDouble.empty(), OptionalLong.empty(), OptionalDouble.empty())
        );
        assertThat(decision.action(), equalTo("hold"));
    }

    public void testKeepsRaiseAndRaisesAgainInTheSameInterval() {
        controller.onInterval(quiet(100.0));
        // each of the 10 uploads brought 10, the 11th brings 6, more than half of that
        final Decision decision = controller.onInterval(quiet(106.0));
        assertThat(decision.action(), equalTo("raise"));
        assertThat(decision.reason(), containsString("kept"));
        assertThat(decision.target(), equalTo(FLOOR + 2));
        assertThat(controller.getTarget(), equalTo(FLOOR + 2));
    }

    public void testKeepRuleIsHalfTheIdealGain() {
        // at 100 uploads each brings 10 on average, so a raise must bring at least 5
        final var big = new UploadConcurrencyController(100, 140);
        big.onInterval(quiet(1, 100, 1000.0));
        assertThat(big.onInterval(quiet(1, 101, 1004.9)).action(), equalTo("revert"));
        assertThat(big.getTarget(), equalTo(100));

        final var other = new UploadConcurrencyController(100, 140);
        other.onInterval(quiet(1, 100, 1000.0));
        assertThat(other.onInterval(quiet(1, 101, 1005.0)).action(), equalTo("raise"));
        assertThat(other.getTarget(), equalTo(102));
    }

    public void testClimbsPastTwentyWhenEachUploadAddsMostOfItsShare() {
        final int cap = 40;
        final var climbing = new UploadConcurrencyController(FLOOR, cap);
        int previous = FLOOR;
        boolean passedTwenty = false;
        for (int interval = 0; interval < 60; interval++) {
            final int target = climbing.getTarget();
            // 100 per upload at the floor, and every further upload adds 80% of that
            final double throughput = 100.0 * FLOOR + 80.0 * (target - FLOOR);
            climbing.onInterval(quiet(10, target, throughput));
            assertThat(climbing.getTarget(), org.hamcrest.Matchers.greaterThanOrEqualTo(previous));
            previous = climbing.getTarget();
            passedTwenty |= previous > 20;
        }
        assertTrue(passedTwenty);
        assertThat(climbing.getTarget(), equalTo(cap));
    }

    public void testFlatThroughputIsNoise() {
        controller.onInterval(quiet(100.0));
        final Decision flat = controller.onInterval(quiet(100.0));
        assertThat(flat.action(), equalTo("revert"));
        assertThat(controller.getTarget(), equalTo(FLOOR));
        // a little more than flat is still not half the ideal gain
        final var other = new UploadConcurrencyController(FLOOR, CEILING);
        other.onInterval(quiet(1, FLOOR, 100.0));
        assertThat(other.onInterval(quiet(1, FLOOR + 1, 102.0)).action(), equalTo("revert"));
        // and so is less
        final var down = new UploadConcurrencyController(FLOOR, CEILING);
        down.onInterval(quiet(1, FLOOR, 100.0));
        assertThat(down.onInterval(quiet(1, FLOOR + 1, 90.0)).action(), equalTo("revert"));
    }

    public void testNoGainFromZeroThroughput() {
        controller.onInterval(quiet(0.0));
        assertThat(controller.onInterval(quiet(0.0)).action(), equalTo("revert"));
    }

    public void testRevertsRaiseWithoutGainAndCoolsDown() {
        controller.onInterval(quiet(100.0));
        final Decision decision = controller.onInterval(quiet(104.0));
        assertThat(decision.action(), equalTo("revert"));
        assertThat(decision.target(), equalTo(FLOOR));
        for (int i = 0; i < REVERT_COOLDOWN_INTERVALS; i++) {
            final Decision hold = controller.onInterval(quiet(100.0));
            assertThat(hold.action(), equalTo("hold"));
            assertThat(hold.reason(), equalTo("cooldown"));
            assertThat(hold.target(), equalTo(FLOOR));
        }
        assertThat(controller.onInterval(quiet(100.0)).action(), equalTo("raise"));
    }

    public void testHoldsWhenNothingQueued() {
        final Decision decision = controller.onInterval(quiet(0, randomIntBetween(0, FLOOR), 100.0));
        assertThat(decision.action(), equalTo("hold"));
        assertThat(decision.reason(), equalTo("nothing queued"));
        assertThat(decision.target(), equalTo(FLOOR));
    }

    public void testHoldsWhenLimiterIsWaiting() {
        final int running = FLOOR;
        // each running upload paused for 10% of the interval or more
        final long pauseNanos = INTERVAL_NANOS * running / 10 + randomLongBetween(0L, INTERVAL_NANOS * running);
        final Decision decision = controller.onInterval(withPause(quiet(randomIntBetween(1, 100), running, 100.0), pauseNanos));
        assertThat(decision.action(), equalTo("hold"));
        assertThat(decision.reason(), containsString("limiter wait"));
        assertThat(decision.target(), equalTo(FLOOR));
        // just below 10% raises
        final long lowPauseNanos = INTERVAL_NANOS * running / 10 - 1;
        assertThat(controller.onInterval(withPause(quiet(1, running, 100.0), lowPauseNanos)).action(), equalTo("raise"));
    }

    public void testStopsAtCeiling() {
        final var small = new UploadConcurrencyController(10, 12);
        assertThat(small.onInterval(quiet(1, 10, 100.0)).target(), equalTo(11));
        assertThat(small.onInterval(quiet(1, 11, 200.0)).target(), equalTo(12));
        // the last raise is kept, and there is nothing to raise to
        final Decision kept = small.onInterval(quiet(1, 12, 300.0));
        assertThat(kept.action(), equalTo("keep"));
        assertThat(kept.reason(), containsString("at ceiling"));
        assertThat(kept.target(), equalTo(12));
        final Decision decision = small.onInterval(quiet(1, 12, 300.0));
        assertThat(decision.action(), equalTo("hold"));
        assertThat(decision.reason(), equalTo("at ceiling"));
        assertThat(decision.target(), equalTo(12));
    }

    public void testLoweredCeilingCutsTheTargetAndCancelsProbe() {
        final double throughput = climbToPendingProbe();
        final int target = controller.getTarget();
        controller.setCeiling(target - 5);
        final Decision decision = controller.onInterval(quiet(throughput));
        assertThat(decision.action(), equalTo("cut"));
        assertThat(decision.reason(), containsString("ceiling lowered"));
        assertThat(decision.target(), equalTo(target - 5));
        assertThat(controller.getCeiling(), equalTo(target - 5));
        // the probe was cancelled and the target stays at the ceiling
        final Decision next = controller.onInterval(quiet(0.0));
        assertThat(next.action(), equalTo("hold"));
        assertThat(next.reason(), equalTo("at ceiling"));
        // raised again, it can grow again
        controller.setCeiling(target + 5);
        assertThat(controller.onInterval(quiet(0.0)).action(), equalTo("raise"));
    }

    public void testCeilingNeverBelowFloor() {
        controller.setCeiling(1);
        assertThat(controller.getCeiling(), equalTo(FLOOR));
        assertThat(controller.onInterval(quiet(100.0)).reason(), equalTo("at ceiling"));
        assertThat(controller.getTarget(), equalTo(FLOOR));
    }

    private static final int CLIMB_STEPS = 20;

    /** Climbs a few steps, each raise kept by doubled throughput, and so one raise per interval, the last one still pending. */
    private double climbToPendingProbe() {
        double throughput = 100.0;
        for (int i = 0; i < CLIMB_STEPS; i++) {
            controller.onInterval(quiet(throughput));
            throughput *= 2;
        }
        assertThat(controller.getTarget(), equalTo(FLOOR + CLIMB_STEPS));
        return throughput;
    }

    public void testCutsOnCpuPressureAndCancelsProbe() {
        final double throughput = climbToPendingProbe();
        final int target = controller.getTarget();
        final Decision decision = controller.onInterval(
            with(quiet(1, target, 0.0), OptionalDouble.of(Math.nextUp(CONTENDED_CPU_PRESSURE)), OptionalLong.of(0L), OptionalDouble.of(0.0))
        );
        assertThat(decision.action(), equalTo("cut"));
        assertThat(decision.reason(), containsString("cpu pressure"));
        assertThat(decision.target(), equalTo(target * 3 / 4));
        // the pending probe was cancelled, so low throughput next time does not revert, but the cooldown holds it
        final Decision next = controller.onInterval(quiet(throughput));
        assertThat(next.action(), equalTo("hold"));
        assertThat(next.reason(), equalTo("cooldown"));
    }

    public void testCutsOnThrottlingAndOnWriteQueueWait() {
        final Signals base = quiet(100.0);
        // more than 1% of the interval
        final long throttledMicros = randomLongBetween(50_001L, 5_000_000L);
        final Decision throttled = controller.onInterval(
            with(base, base.cpuPressure(), OptionalLong.of(throttledMicros), base.writeQueueWaitMillis())
        );
        assertThat(throttled.action(), equalTo("cut"));
        assertThat(throttled.reason(), containsString("cpu throttled"));

        controller.reset();
        final Decision waiting = controller.onInterval(
            with(base, base.cpuPressure(), base.throttledMicros(), OptionalDouble.of(Math.nextUp(CONTENDED_WRITE_QUEUE_WAIT_MILLIS)))
        );
        assertThat(waiting.action(), equalTo("cut"));
        assertThat(waiting.reason(), containsString("write queue wait"));

        // the thresholds themselves are not contention
        controller.reset();
        final Decision atThreshold = controller.onInterval(
            with(base, OptionalDouble.of(CONTENDED_CPU_PRESSURE), OptionalLong.of(0L), OptionalDouble.of(CONTENDED_WRITE_QUEUE_WAIT_MILLIS))
        );
        assertThat(atThreshold.action(), equalTo("hold"));
    }

    public void testBriefThrottlingStopsRaisesButDoesNotCut() {
        final Signals base = quiet(100.0);
        // 1% of the interval exactly is not more than 1%
        final long brief = TimeUnit.NANOSECONDS.toMicros(INTERVAL_NANOS) / 100;
        final Decision decision = controller.onInterval(
            with(base, base.cpuPressure(), OptionalLong.of(randomLongBetween(1L, brief)), base.writeQueueWaitMillis())
        );
        assertThat(decision.action(), equalTo("hold"));
        assertThat(decision.reason(), containsString("cpu throttled"));
        assertThat(controller.getTarget(), equalTo(FLOOR));
    }

    public void testStalledWritePoolIsContention() {
        final Signals base = quiet(100.0);
        final Signals stalled = new Signals(
            base.queued(),
            base.running(),
            base.throughputBytesPerSec(),
            base.limiterPauseNanos(),
            base.intervalNanos(),
            base.cpuPressure(),
            base.throttledMicros(),
            OptionalDouble.of(0.0),
            true,
            0L,
            0L
        );
        final Decision decision = controller.onInterval(stalled);
        assertThat(decision.action(), equalTo("cut"));
        assertThat(decision.reason(), containsString("write pool stalled"));
    }

    public void testContentionReasonNamesEverySignal() {
        final Decision decision = controller.onInterval(
            with(quiet(100.0), OptionalDouble.of(0.5), OptionalLong.of(1_000_000L), OptionalDouble.of(50.0))
        );
        assertThat(decision.reason(), containsString("cpu pressure"));
        assertThat(decision.reason(), containsString("cpu throttled"));
        assertThat(decision.reason(), containsString("write queue wait"));
    }

    public void testContentionCutStopsAtFloorAndCoolsDown() {
        final Signals contended = with(quiet(1, FLOOR, 0.0), OptionalDouble.of(1.0), OptionalLong.of(0L), OptionalDouble.of(0.0));
        for (int i = 0; i < 3; i++) {
            final Decision decision = controller.onInterval(contended);
            assertThat(decision.action(), equalTo("cut"));
            assertThat(decision.target(), equalTo(FLOOR));
        }
        for (int i = 0; i < CONTENTION_COOLDOWN_INTERVALS; i++) {
            assertThat(controller.onInterval(quiet(100.0)).action(), equalTo("hold"));
        }
        assertThat(controller.onInterval(quiet(100.0)).action(), equalTo("raise"));
    }

    public void testCutsToHalfOnUploadErrors() {
        final double throughput = climbToPendingProbe();
        final long readErrors = randomLongBetween(0, 3);
        final long uploadErrors = readErrors == 0 ? randomLongBetween(1, 3) : randomLongBetween(0, 3);
        final int target = controller.getTarget();
        final Decision decision = controller.onInterval(withErrors(quiet(1, target, 0.0), readErrors, uploadErrors));
        assertThat(decision.action(), equalTo("cut"));
        assertThat(decision.target(), equalTo(target / 2));
        assertThat(decision.reason(), containsString(readErrors + " reading the source"));
        assertThat(decision.reason(), containsString(uploadErrors + " writing to the repository"));

        // the pending probe was cancelled and raising waits for the cooldown
        for (int i = 0; i < ERROR_COOLDOWN_INTERVALS; i++) {
            final Decision hold = controller.onInterval(quiet(throughput));
            assertThat(hold.action(), equalTo("hold"));
            assertThat(hold.reason(), equalTo("cooldown"));
        }
        assertThat(controller.onInterval(quiet(throughput)).action(), equalTo("raise"));
    }

    public void testErrorsBeatContentionAndLongCooldownIsKept() {
        // errors and contention in the same interval: the errors rule applies
        climbToPendingProbe();
        final int target = controller.getTarget();
        final Signals both = withErrors(
            with(quiet(1, target, 0.0), OptionalDouble.of(1.0), OptionalLong.of(0L), OptionalDouble.of(0.0)),
            0L,
            1L
        );
        final Decision decision = controller.onInterval(both);
        assertThat(decision.target(), equalTo(target / 2));
        assertThat(decision.reason(), containsString("upload errors"));

        // a contention cut right after must not shorten the cooldown the errors started
        controller.onInterval(with(quiet(1, target / 2, 0.0), OptionalDouble.of(1.0), OptionalLong.of(0L), OptionalDouble.of(0.0)));
        for (int i = 0; i < ERROR_COOLDOWN_INTERVALS - 1; i++) {
            assertThat(controller.onInterval(quiet(100.0)).action(), equalTo("hold"));
        }
        assertThat(controller.onInterval(quiet(100.0)).action(), equalTo("raise"));
    }

    public void testErrorCutStopsAtFloor() {
        final Decision decision = controller.onInterval(withErrors(quiet(1, FLOOR, 0.0), 1L, 0L));
        assertThat(decision.action(), equalTo("cut"));
        assertThat(decision.target(), equalTo(FLOOR));
    }

    public void testReset() {
        controller.onInterval(quiet(100.0));
        assertThat(controller.getTarget(), equalTo(FLOOR + 1));
        controller.reset();
        assertThat(controller.getTarget(), equalTo(FLOOR));
        // no pending probe after a reset
        assertThat(controller.onInterval(quiet(0.0)).action(), equalTo("raise"));
    }
}
