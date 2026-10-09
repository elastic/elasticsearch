/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.TimeUnit;

import static org.elasticsearch.indices.recovery.UploadConcurrencyController.COOLDOWN_INTERVALS;
import static org.elasticsearch.indices.recovery.UploadConcurrencyController.CPU_LIMIT_PERCENT;
import static org.hamcrest.Matchers.equalTo;

public class UploadConcurrencyControllerTests extends ESTestCase {

    private static final long INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(5);
    private static final int FLOOR = 10;
    private static final int CEILING = 140;

    private final UploadConcurrencyController controller = new UploadConcurrencyController(FLOOR, CEILING);

    private int cpu() {
        return randomIntBetween(-1, CPU_LIMIT_PERCENT - 1);
    }

    /** queued work, no limiter waits */
    private UploadConcurrencyController.Decision unconstrained(double throughput) {
        return controller.onInterval(randomIntBetween(1, 100), controller.getTarget(), throughput, 0L, INTERVAL_NANOS, cpu());
    }

    public void testStartsAtFloor() {
        assertThat(controller.getTarget(), equalTo(FLOOR));
        expectThrows(IllegalArgumentException.class, () -> new UploadConcurrencyController(0, 10));
        expectThrows(IllegalArgumentException.class, () -> new UploadConcurrencyController(10, 9));
    }

    public void testRaisesWhenQueuedAndLimiterNotWaiting() {
        final var decision = unconstrained(100.0);
        assertThat(decision.action(), equalTo("raise"));
        assertThat(decision.target(), equalTo(FLOOR + FLOOR / 4));
        assertThat(controller.getTarget(), equalTo(12));
    }

    public void testRaisesByAtLeastOne() {
        final var small = new UploadConcurrencyController(1, 5);
        assertThat(small.onInterval(1, 1, 100.0, 0L, INTERVAL_NANOS, cpu()).target(), equalTo(2));
    }

    public void testKeepsRaiseWhenThroughputGrows() {
        unconstrained(100.0);
        final var decision = unconstrained(106.0);
        assertThat(decision.action(), equalTo("keep"));
        assertThat(decision.target(), equalTo(12));
        // and probes again on the next interval
        assertThat(unconstrained(106.0).action(), equalTo("raise"));
        assertThat(controller.getTarget(), equalTo(15));
    }

    public void testRevertsRaiseWithoutGainAndCoolsDown() {
        unconstrained(100.0);
        final var decision = unconstrained(104.0);
        assertThat(decision.action(), equalTo("revert"));
        assertThat(decision.target(), equalTo(FLOOR));
        for (int i = 0; i < COOLDOWN_INTERVALS; i++) {
            final var hold = unconstrained(100.0);
            assertThat(hold.action(), equalTo("hold"));
            assertThat(hold.reason(), equalTo("cooldown"));
            assertThat(hold.target(), equalTo(FLOOR));
        }
        assertThat(unconstrained(100.0).action(), equalTo("raise"));
    }

    public void testHoldsWhenNothingQueued() {
        final var decision = controller.onInterval(0, randomIntBetween(0, FLOOR), 100.0, 0L, INTERVAL_NANOS, cpu());
        assertThat(decision.action(), equalTo("hold"));
        assertThat(decision.reason(), equalTo("nothing queued"));
        assertThat(decision.target(), equalTo(FLOOR));
    }

    public void testHoldsWhenLimiterIsWaiting() {
        final int running = FLOOR;
        // each running upload paused for 10% of the interval or more
        final long pauseNanos = INTERVAL_NANOS * running / 10 + randomLongBetween(0L, INTERVAL_NANOS * running);
        final var decision = controller.onInterval(randomIntBetween(1, 100), running, 100.0, pauseNanos, INTERVAL_NANOS, cpu());
        assertThat(decision.action(), equalTo("hold"));
        assertThat(decision.target(), equalTo(FLOOR));
        // just below 10% raises
        final long lowPauseNanos = INTERVAL_NANOS * running / 10 - 1;
        assertThat(controller.onInterval(1, running, 100.0, lowPauseNanos, INTERVAL_NANOS, cpu()).action(), equalTo("raise"));
    }

    public void testStopsAtCeiling() {
        final var small = new UploadConcurrencyController(10, 13);
        assertThat(small.onInterval(1, 10, 100.0, 0L, INTERVAL_NANOS, cpu()).target(), equalTo(12));
        assertThat(small.onInterval(1, 12, 200.0, 0L, INTERVAL_NANOS, cpu()).action(), equalTo("keep"));
        assertThat(small.onInterval(1, 12, 200.0, 0L, INTERVAL_NANOS, cpu()).target(), equalTo(13));
        assertThat(small.onInterval(1, 13, 300.0, 0L, INTERVAL_NANOS, cpu()).action(), equalTo("keep"));
        final var decision = small.onInterval(1, 13, 300.0, 0L, INTERVAL_NANOS, cpu());
        assertThat(decision.action(), equalTo("hold"));
        assertThat(decision.reason(), equalTo("at ceiling"));
        assertThat(decision.target(), equalTo(13));
    }

    public void testCutsOnHighCpuAndCancelsProbe() {
        // climb a few steps
        double throughput = 100.0;
        for (int i = 0; i < 4; i++) {
            unconstrained(throughput);
            throughput *= 2;
            unconstrained(throughput);
        }
        // 10 -> 12 -> 15 -> 18 -> 22
        assertThat(controller.getTarget(), equalTo(22));
        unconstrained(throughput); // raise to 27, probe pending
        assertThat(controller.getTarget(), equalTo(27));

        final var decision = controller.onInterval(1, 27, 0.0, 0L, INTERVAL_NANOS, randomIntBetween(CPU_LIMIT_PERCENT, 100));
        assertThat(decision.action(), equalTo("cut"));
        assertThat(decision.target(), equalTo(27 * 3 / 4));
        // the pending probe was cancelled, so low throughput next time does not revert, it raises again
        assertThat(unconstrained(0.0).action(), equalTo("raise"));
    }

    public void testCpuCutStopsAtFloor() {
        for (int i = 0; i < 3; i++) {
            final var decision = controller.onInterval(1, FLOOR, 0.0, 0L, INTERVAL_NANOS, randomIntBetween(CPU_LIMIT_PERCENT, 100));
            assertThat(decision.action(), equalTo("cut"));
            assertThat(decision.target(), equalTo(FLOOR));
        }
    }

    public void testReset() {
        unconstrained(100.0);
        assertThat(controller.getTarget(), equalTo(12));
        controller.reset();
        assertThat(controller.getTarget(), equalTo(FLOOR));
        // no pending probe after a reset
        assertThat(unconstrained(0.0).action(), equalTo("raise"));
    }
}
