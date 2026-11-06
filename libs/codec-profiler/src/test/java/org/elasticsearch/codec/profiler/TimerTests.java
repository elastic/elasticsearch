/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.codec.profiler;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;

import java.lang.management.ManagementFactory;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongSupplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class TimerTests extends ESTestCase {

    private static final TimerMetric BLOCK_TIME = TimerMetric.of(
        "columnar",
        "blocks",
        "read_time_nanos",
        TimingPolicy.ADAPTIVE,
        "time spent reading blocks"
    );
    private static final TimerMetric CHUNK_TIME = TimerMetric.of(
        "columnar",
        "chunks",
        "decompress_time_nanos",
        TimingPolicy.EXACT,
        "time spent inflating chunks"
    );
    private static final CounterMetric EVENTS = CounterMetric.of("columnar", "test", "events", Metric.Unit.COUNT, "events that happened");
    private static final TimerMetric EXACT_BLOCK_TIME = TimerMetric.of(BLOCK_TIME.id(), TimingPolicy.EXACT, BLOCK_TIME.description());
    private static final String BLOCK_CPU_NAME = "read_cpu_time_nanos";
    private static final String BLOCK_CPU_DESCRIPTION = "thread CPU time spent reading blocks";
    private static final Metric.Id BLOCK_CPU_TIME = new Metric.Id("columnar", "blocks", BLOCK_CPU_NAME);
    private static final TimerMetric CHUNK_TIME_WITH_CPU = CHUNK_TIME.withCpuTime(
        "decompress_cpu_time_nanos",
        "thread CPU time spent inflating chunks"
    );

    private static final class ScriptedClock implements LongSupplier {
        long now;
        long reads;

        ScriptedClock(long origin) {
            this.now = origin;
        }

        @Override
        public long getAsLong() {
            reads++;
            return now;
        }

        void advance(long nanos) {
            now += nanos;
        }
    }

    // NOTE: A poisoned clock returns -1, as the platform CPU clock does when thread CPU time is disabled.
    private static final class PoisonableClock implements LongSupplier {
        long now;
        long reads;
        boolean poisoned;

        @Override
        public long getAsLong() {
            reads++;
            return poisoned ? -1 : now;
        }

        void advance(long nanos) {
            now += nanos;
        }
    }

    private static void call(Timer timer, ScriptedClock clock, long duration) {
        timer.start();
        clock.advance(duration);
        timer.stop();
    }

    private static void call(Timer timer, ScriptedClock elapsed, ScriptedClock cpu, long duration, long cpuDuration) {
        timer.start();
        elapsed.advance(duration);
        cpu.advance(cpuDuration);
        timer.stop();
    }

    private static long time(ProfileCollector stats, Metric metric) {
        return time(stats, metric.id());
    }

    private static long time(ProfileCollector stats, Metric.Id id) {
        return TestCollectors.toKeyedValues(stats.snapshot()).get(id.key());
    }

    private static TimerMetric withCpu(TimerMetric timer) {
        return timer.withCpuTime(BLOCK_CPU_NAME, BLOCK_CPU_DESCRIPTION);
    }

    public void testTheAdaptiveScheduleIsPinned() {
        // NOTE: The figures were taken from the timer before it moved out of the collector; they pin the schedule the
        // TimingPolicy Javadoc describes, including the cap: 1024 calls skipped, then one measured.
        final long[] now = { 0 };
        final ProfileCollector stats = TestCollectors.withClock(() -> now[0]);
        final Timer timer = stats.timer(BLOCK_TIME);
        final List<Integer> measured = new ArrayList<>();
        long sampled = 0;
        for (int call = 0; call < 400_000; call++) {
            timer.start();
            now[0] += 3;
            timer.stop();
            final long after = stats.snapshot().get(BLOCK_TIME).sampledCalls();
            if (after > sampled) {
                measured.add(call);
                sampled = after;
            }
        }
        assertThat(measured.size(), equalTo(2_059));
        for (int call = 0; call < 256; call++) {
            assertThat("every one of the first 256 calls is measured", measured.get(call), equalTo(call));
        }
        assertTrue(measured.contains(383_991));
        assertThat(measured.get(measured.indexOf(383_991) + 1), equalTo(385_016));
        assertThat(measured.get(measured.size() - 1) - measured.get(measured.size() - 2), equalTo(1_025));
        assertThat("a constant duration is estimated exactly", stats.snapshot().get(BLOCK_TIME).elapsedNanos(), equalTo(1_200_000L));
    }

    public void testAConstantDurationIsEstimatedExactlyAtEveryCallCountWhateverTheSamplingRate() {
        final ScriptedClock clock = new ScriptedClock(randomBoolean() ? 0 : randomNonNegativeLong() >>> 2);
        final ProfileCollector stats = TestCollectors.withClock(clock);
        final Timer timer = stats.timer(BLOCK_TIME);
        final long duration = between(1, 50);
        final long calls = 300_000;
        for (long n = 1; n <= calls; n++) {
            call(timer, clock, duration);
            assertThat("after " + n + " calls", time(stats, BLOCK_TIME), equalTo(n * duration));
        }
        assertThat("the schedule did skip most clock reads", clock.reads * 2, lessThan(calls / 10));
    }

    public void testAVaryingDurationIsEstimatedFromTheWeightedMeasurementsOfAnIndependentReplay() {
        final ScriptedClock clock = new ScriptedClock(0);
        final ProfileCollector stats = TestCollectors.withClock(clock);
        final Timer timer = stats.timer(BLOCK_TIME);
        final long[] cycle = new long[between(2, 7)];
        for (int i = 0; i < cycle.length; i++) {
            cycle[i] = between(1, 500);
        }
        long count = 0;
        long last = 0;
        long weighted = 0;
        long published = 0;
        for (int n = 0; n < 20_000; n++) {
            final long duration = cycle[n % cycle.length];
            final boolean measured = (count - last) >= Math.min(last >>> 8, 1024);
            count++;
            if (measured) {
                weighted += (count - last) * duration;
                last = count;
            }
            final long estimate = BigInteger.valueOf(weighted)
                .add(BigInteger.valueOf(count - last).multiply(BigInteger.valueOf(weighted)).divide(BigInteger.valueOf(last)))
                .longValueExact();
            published = Math.max(published, estimate);
            call(timer, clock, duration);
            assertThat("after " + count + " calls", time(stats, BLOCK_TIME), equalTo(published));
        }
    }

    public void testAPairedTimerEstimatesBothSidesExactlyForConstantDurations() {
        final ScriptedClock elapsed = new ScriptedClock(0);
        final ScriptedClock cpu = new ScriptedClock(randomBoolean() ? 0 : randomNonNegativeLong() >>> 2);
        final ProfileCollector stats = TestCollectors.withClocks(elapsed, cpu);
        final TimerMetric elapsedMetric = TimerMetric.of(BLOCK_TIME.id(), randomFrom(TimingPolicy.values()), BLOCK_TIME.description());
        final Timer timer = stats.timer(withCpu(elapsedMetric));
        final int duration = between(2, 50);
        final long cpuDuration = between(0, duration);
        final long calls = 50_000;
        for (long n = 1; n <= calls; n++) {
            call(timer, elapsed, cpu, duration, cpuDuration);
            assertThat("after " + n + " calls", time(stats, BLOCK_TIME), equalTo(n * duration));
            assertThat("after " + n + " calls", time(stats, BLOCK_CPU_TIME), equalTo(n * cpuDuration));
        }
        assertThat("both clocks follow one sampling decision", cpu.reads, equalTo(elapsed.reads));
    }

    public void testAPairedVaryingWorkloadMatchesAnIndependentReplayOnBothSides() {
        final ScriptedClock elapsed = new ScriptedClock(0);
        final ScriptedClock cpu = new ScriptedClock(0);
        final ProfileCollector stats = TestCollectors.withClocks(elapsed, cpu);
        final Timer timer = stats.timer(withCpu(BLOCK_TIME));
        final long[] cycle = new long[between(2, 7)];
        final long[] cpuCycle = new long[cycle.length];
        for (int i = 0; i < cycle.length; i++) {
            cycle[i] = between(1, 500);
            cpuCycle[i] = between(0, (int) cycle[i]);
        }
        long count = 0;
        long last = 0;
        long weighted = 0;
        long cpuWeighted = 0;
        long published = 0;
        long cpuPublished = 0;
        for (int n = 0; n < 20_000; n++) {
            final long duration = cycle[n % cycle.length];
            final long cpuDuration = cpuCycle[n % cycle.length];
            final boolean measured = (count - last) >= Math.min(last >>> 8, 1024);
            count++;
            if (measured) {
                weighted += (count - last) * duration;
                cpuWeighted += (count - last) * cpuDuration;
                last = count;
            }
            published = Math.max(published, Timer.estimate(count, last, weighted));
            cpuPublished = Math.max(cpuPublished, Timer.estimate(count, last, cpuWeighted));
            call(timer, elapsed, cpu, duration, cpuDuration);
            assertThat("after " + count + " calls", time(stats, BLOCK_TIME), equalTo(published));
            assertThat("after " + count + " calls", time(stats, BLOCK_CPU_TIME), equalTo(cpuPublished));
        }
    }

    public void testAnInvalidCpuReadingFailsClosedForTheCollectorsLife() {
        final ScriptedClock elapsed = new ScriptedClock(0);
        final PoisonableClock cpu = new PoisonableClock();
        final ProfileCollector stats = TestCollectors.withClocks(elapsed, cpu);
        final Timer timer = stats.timer(withCpu(EXACT_BLOCK_TIME));
        for (int n = 0; n < 10; n++) {
            timer.start();
            elapsed.advance(5);
            cpu.advance(3);
            timer.stop();
        }
        assertThat(time(stats, BLOCK_CPU_TIME), equalTo(30L));
        assertTrue(stats.measuresThreadCpuTime());

        cpu.poisoned = true;
        timer.start();
        elapsed.advance(5);
        timer.stop();
        assertFalse("an invalid reading trips the collector", stats.measuresThreadCpuTime());
        assertThat(
            "the CPU metric leaves the snapshots",
            TestCollectors.toKeyedValues(stats.snapshot()),
            not(hasKey(BLOCK_CPU_TIME.key()))
        );
        assertThat("elapsed time is unaffected", time(stats, BLOCK_TIME), equalTo(55L));

        cpu.poisoned = false;
        final long readsWhenTripped = cpu.reads;
        for (int n = 0; n < 10; n++) {
            timer.start();
            elapsed.advance(5);
            cpu.advance(3);
            timer.stop();
        }
        assertThat(
            "a recovered clock does not resurrect the metric",
            TestCollectors.toKeyedValues(stats.snapshot()),
            not(hasKey(BLOCK_CPU_TIME.key()))
        );
        assertThat("no CPU clock read is attempted after the trip", cpu.reads, equalTo(readsWhenTripped));
        assertThat("elapsed time keeps growing", time(stats, BLOCK_TIME), equalTo(105L));
    }

    public void testAnInvalidStartAgainstAValidStopTripsWithoutAHugeValue() {
        final ScriptedClock elapsed = new ScriptedClock(0);
        final PoisonableClock cpu = new PoisonableClock();
        cpu.now = 1_000_000;
        final ProfileCollector stats = TestCollectors.withClocks(elapsed, cpu);
        final Timer timer = stats.timer(withCpu(EXACT_BLOCK_TIME));
        cpu.poisoned = true;
        timer.start();
        cpu.poisoned = false;
        elapsed.advance(5);
        timer.stop();
        assertFalse(stats.measuresThreadCpuTime());
        assertThat(TestCollectors.toKeyedValues(stats.snapshot()), not(hasKey(BLOCK_CPU_TIME.key())));
    }

    public void testAnInvalidStopReadingAloneTripsTheLatch() {
        final ScriptedClock elapsed = new ScriptedClock(0);
        final PoisonableClock cpu = new PoisonableClock();
        cpu.now = 500;
        final ProfileCollector stats = TestCollectors.withClocks(elapsed, cpu);
        final Timer timer = stats.timer(withCpu(EXACT_BLOCK_TIME));
        timer.start();
        elapsed.advance(5);
        cpu.poisoned = true;
        timer.stop();
        assertFalse("a valid start against an invalid stop trips the collector", stats.measuresThreadCpuTime());
        assertThat(TestCollectors.toKeyedValues(stats.snapshot()), not(hasKey(BLOCK_CPU_TIME.key())));
        assertThat("the elapsed side of the call is kept", time(stats, BLOCK_TIME), equalTo(5L));
    }

    public void testACpuCompanionRegisteredAfterTheTripStaysAbsent() {
        final ScriptedClock elapsed = new ScriptedClock(0);
        final PoisonableClock cpu = new PoisonableClock();
        final ProfileCollector stats = TestCollectors.withClocks(elapsed, cpu);
        final Timer first = stats.timer(withCpu(EXACT_BLOCK_TIME));
        cpu.poisoned = true;
        first.start();
        first.stop();
        assertFalse(stats.measuresThreadCpuTime());

        // A reader opened later on the same collector registers another pair; the latch is collector-wide.
        final Timer later = stats.timer(CHUNK_TIME_WITH_CPU);
        cpu.poisoned = false;
        call(later, elapsed, 7);
        assertThat(
            "a pair registered after the trip never surfaces its CPU key",
            TestCollectors.toKeyedValues(stats.snapshot()),
            not(hasKey(CHUNK_TIME_WITH_CPU.cpuTime().id().key()))
        );
        assertThat(TestCollectors.toKeyedValues(stats.snapshot()), not(hasKey(BLOCK_CPU_TIME.key())));
        assertThat("its elapsed side works normally", time(stats, CHUNK_TIME), equalTo(7L));
    }

    public void testNestedPairedTimersAccumulateIndependently() {
        final ScriptedClock elapsed = new ScriptedClock(0);
        final ScriptedClock cpu = new ScriptedClock(0);
        final ProfileCollector stats = TestCollectors.withClocks(elapsed, cpu);
        final Timer outer = stats.timer(withCpu(EXACT_BLOCK_TIME));
        final Timer inner = stats.timer(CHUNK_TIME_WITH_CPU);
        outer.start();
        call(inner, elapsed, cpu, 10, 4);
        elapsed.advance(7);
        cpu.advance(2);
        outer.stop();
        assertThat("the outer window contains the inner one", time(stats, BLOCK_TIME), equalTo(17L));
        assertThat(time(stats, BLOCK_CPU_TIME), equalTo(6L));
        assertThat(time(stats, CHUNK_TIME), equalTo(10L));
        assertThat(time(stats, CHUNK_TIME_WITH_CPU.cpuTime().id()), equalTo(4L));
    }

    public void testAZeroCpuSampleIsValidAndDistinguishableFromAnInvalidOne() {
        final ScriptedClock elapsed = new ScriptedClock(0);
        final ScriptedClock cpu = new ScriptedClock(50);
        final ProfileCollector stats = TestCollectors.withClocks(elapsed, cpu);
        final Timer timer = stats.timer(withCpu(EXACT_BLOCK_TIME));
        for (int n = 0; n < 5; n++) {
            call(timer, elapsed, cpu, 7, 0);
        }
        assertTrue("a zero sample is a valid sample", stats.measuresThreadCpuTime());
        assertThat("a short call can accumulate no CPU time", time(stats, BLOCK_CPU_TIME), equalTo(0L));
        assertThat(time(stats, BLOCK_TIME), equalTo(35L));
    }

    public void testTheEstimatorKeepsTheQuotientWhenOnlyTheProductOverflows() {
        assertThat(Timer.estimate(10_000, 1_000, 10_000_000_000_000_000L), equalTo(100_000_000_000_000_000L));
    }

    public void testTheEstimatorSaturatesOnlyWhenTheResultDoesNotFit() {
        assertThat("the tail alone does not fit", Timer.estimate(10_000_000, 1_000, 100_000_000_000_000_000L), equalTo(Long.MAX_VALUE));
        assertThat("the sum does not fit though the tail does", Timer.estimate(2, 1, 5_000_000_000_000_000_000L), equalTo(Long.MAX_VALUE));
        assertThat("a result just below the limit is kept", Timer.estimate(2, 1, Long.MAX_VALUE / 2), equalTo(Long.MAX_VALUE / 2 * 2));
        assertThat(Timer.estimate(5, 5, 123), equalTo(123L));
        assertThat("nothing measured yet", Timer.estimate(3, 0, 0), equalTo(0L));
    }

    public void testAnEstimateNeverDecreasesBetweenSnapshots() {
        final ScriptedClock clock = new ScriptedClock(0);
        final ProfileCollector stats = TestCollectors.withClock(clock);
        final Timer timer = stats.timer(BLOCK_TIME);
        for (int n = 0; n < 5_000; n++) {
            call(timer, clock, n < 2_000 ? between(1, 5) : between(1, 5_000));
        }
        long previous = 0;
        for (int n = 0; n < 5_000; n++) {
            call(timer, clock, rarely() ? 1_000_000 : 1);
            final long now = time(stats, BLOCK_TIME);
            assertThat(now, greaterThanOrEqualTo(previous));
            previous = now;
        }
    }

    public void testAWorkloadThatTurnsExpensiveIsFollowedWithinAStatedBound() {
        final ScriptedClock clock = new ScriptedClock(0);
        final ProfileCollector stats = TestCollectors.withClock(clock);
        final Timer timer = stats.timer(BLOCK_TIME);
        for (int n = 0; n < 1_000; n++) {
            call(timer, clock, 10);
        }
        final long afterCheapPhase = time(stats, BLOCK_TIME);
        assertThat(afterCheapPhase, equalTo(10_000L));
        for (int n = 0; n < 100; n++) {
            call(timer, clock, 1_000);
        }
        final long trueTotal = 10_000 + 100 * 1_000;
        final long estimate = time(stats, BLOCK_TIME);
        assertThat("the expensive phase raised the estimate", estimate, greaterThan(afterCheapPhase));
        assertThat("within 10 percent of the true total", estimate, greaterThanOrEqualTo(trueTotal - trueTotal / 10));
        assertThat(estimate, lessThanOrEqualTo(trueTotal + trueTotal / 10));
    }

    public void testAnExactTimerSumsEveryCallWithAClockThatStartsAtZero() {
        final ScriptedClock clock = new ScriptedClock(0);
        final ProfileCollector stats = TestCollectors.withClock(clock);
        final Timer timer = stats.timer(CHUNK_TIME);
        call(timer, clock, 30);
        call(timer, clock, 12);
        assertThat(time(stats, CHUNK_TIME), equalTo(42L));
        assertThat("two reads for two calls", clock.reads, equalTo(4L));
    }

    public void testACallThatTakesNoTimeStillCountsAsOneNanosecond() {
        final ScriptedClock clock = new ScriptedClock(7);
        final ProfileCollector stats = TestCollectors.withClock(clock);
        final Timer timer = stats.timer(CHUNK_TIME);
        call(timer, clock, 0);
        assertThat(time(stats, CHUNK_TIME), equalTo(1L));
    }

    public void testASecondStartWithoutAStopIsRejectedWhetherOrNotTheCallIsMeasured() {
        final ScriptedClock clock = new ScriptedClock(0);
        final Timer exact = TestCollectors.withClock(clock).timer(CHUNK_TIME);
        exact.start();
        expectThrows(AssertionError.class, exact::start);

        final Timer sampled = TestCollectors.withClock(clock).timer(BLOCK_TIME);
        for (int n = 0; n < 256; n++) {
            call(sampled, clock, 1);
        }
        final long before = clock.reads;
        sampled.start();
        assertThat("the 257th call is not measured", clock.reads, equalTo(before));
        expectThrows(AssertionError.class, sampled::start);
    }

    public void testAStopWithoutAStartIsRejected() {
        final Timer timer = TestCollectors.withClock(new ScriptedClock(0)).timer(CHUNK_TIME);
        expectThrows(AssertionError.class, timer::stop);
    }

    public void testASkippedCallReadsNoClock() {
        final ScriptedClock clock = new ScriptedClock(0);
        final Timer timer = TestCollectors.withClock(clock).timer(BLOCK_TIME);
        for (int n = 0; n < 256; n++) {
            call(timer, clock, 1);
        }
        final long before = clock.reads;
        call(timer, clock, 1);
        assertThat(clock.reads, equalTo(before));
    }

    public void testRegisteringOrReadingATimerReadsNoClock() {
        final ScriptedClock clock = new ScriptedClock(0);
        final ProfileCollector stats = TestCollectors.withClock(clock);
        stats.timer(CHUNK_TIME);
        stats.timer(BLOCK_TIME);
        assertThat(
            TestCollectors.toKeyedValues(stats.snapshot()),
            equalTo(java.util.Map.of("columnar.blocks.read_time_nanos", 0L, "columnar.chunks.decompress_time_nanos", 0L))
        );
        assertThat(clock.reads, equalTo(0L));
    }

    public void testACollectorWithoutTimingHandsOutTheInertTimerAndRegistersNothing() {
        final ProfileCollector stats = TestCollectors.countersOnly();
        final Timer chunk = stats.timer(CHUNK_TIME);
        assertThat(chunk, sameInstance(Timer.INERT));
        assertThat(stats.timer(BLOCK_TIME), sameInstance(Timer.INERT));
        for (int call = 0; call < between(1, 1000); call++) {
            chunk.start();
            chunk.stop();
        }
        assertThat("the inert timer counts nothing", chunk.calls(), equalTo(0L));
        assertTrue("the timer metrics are absent, not zero", stats.isEmpty());
        stats.counter(EVENTS).add(1);
        assertThat(TestCollectors.toKeyedValues(stats.snapshot()), equalTo(java.util.Map.of("columnar.test.events", 1L)));
    }

    public void testTheInertTimerToleratesAnyCallSequenceOnAnyThread() throws Exception {
        final Thread other = new Thread(() -> {
            Timer.INERT.start();
            Timer.INERT.start();
            Timer.INERT.stop();
        });
        other.start();
        other.join();
        Timer.INERT.stop();
        assertThat(Timer.INERT.calls(), equalTo(0L));
        assertThat(Timer.INERT.measuredCalls(), equalTo(0L));
    }

    public void testTheSameTimerMetricAndPolicyGivesTheSameTimer() {
        final ProfileCollector stats = TestCollectors.withClock(new ScriptedClock(0));
        final Timer first = stats.timer(CHUNK_TIME);
        assertThat(stats.timer(CHUNK_TIME), sameInstance(first));
    }

    public void testTwoReadersOfOneCollectorContributeToOneAccumulator() {
        final ScriptedClock clock = new ScriptedClock(0);
        final ProfileCollector stats = TestCollectors.withClock(clock);
        final Timer readerA = stats.timer(CHUNK_TIME);
        final Timer readerB = stats.timer(CHUNK_TIME);
        call(readerA, clock, 10);
        call(readerB, clock, 20);
        call(readerA, clock, 5);
        assertThat(TestCollectors.toKeyedValues(stats.snapshot()).size(), equalTo(1));
        assertThat("the aggregate, not whichever timer registered last", time(stats, CHUNK_TIME), equalTo(35L));
    }

    public void testTwoReadersSharingASampledTimerShareOneSchedule() {
        final ScriptedClock clock = new ScriptedClock(0);
        final ProfileCollector stats = TestCollectors.withClock(clock);
        final Timer readerA = stats.timer(BLOCK_TIME);
        final Timer readerB = stats.timer(BLOCK_TIME);
        for (int n = 1; n <= 5_000; n++) {
            call(n % 2 == 0 ? readerA : readerB, clock, 10);
        }
        assertThat(time(stats, BLOCK_TIME), equalTo(50_000L));
    }

    public void testAnIdentityCannotBeBothATimerAndACounter() {
        final ProfileCollector stats = TestCollectors.withClock(new ScriptedClock(0));
        stats.timer(CHUNK_TIME);
        final CounterMetric sameIdAsACounter = CounterMetric.of(CHUNK_TIME.id(), Metric.Unit.COUNT, "a count");
        expectThrows(IllegalStateException.class, () -> stats.counter(sameIdAsACounter));
    }

    public void testATimerMetricCannotChangePolicy() {
        final ProfileCollector stats = TestCollectors.withClock(new ScriptedClock(0));
        stats.timer(BLOCK_TIME);
        expectThrows(IllegalStateException.class, () -> stats.timer(EXACT_BLOCK_TIME));
    }

    public void testATimerCountsTowardTheMetricLimit() {
        final ProfileCollector stats = TestCollectors.withClock(new ScriptedClock(0));
        for (int i = 0; i < ProfileCollector.MAX_METRICS - 1; i++) {
            stats.counter(CounterMetric.of("columnar", "test", "metric_" + i, Metric.Unit.COUNT, "metric " + i));
        }
        stats.timer(CHUNK_TIME);
        expectThrows(IllegalStateException.class, () -> stats.timer(BLOCK_TIME));
        stats.timer(CHUNK_TIME);
    }

    public void testATimeMetricNameHasToShowItsUnit() {
        TimerMetric.of("columnar", "test", "anything_time_nanos", TimingPolicy.ADAPTIVE, "d");
        expectThrows(
            IllegalArgumentException.class,
            () -> TimerMetric.of("columnar", "test", "anything_nanos", TimingPolicy.ADAPTIVE, "d")
        );
        expectThrows(IllegalArgumentException.class, () -> TimerMetric.of("columnar", "test", "anything_time", TimingPolicy.ADAPTIVE, "d"));
    }

    public void testATimerNeverReportsANegativeValueEvenWithAHugeClockJump() {
        final ScriptedClock clock = new ScriptedClock(0);
        final ProfileCollector stats = TestCollectors.withClock(clock);
        final Timer timer = stats.timer(CHUNK_TIME);
        call(timer, clock, Long.MAX_VALUE - 5);
        call(timer, clock, 1_000);
        assertThat(time(stats, CHUNK_TIME), equalTo(Long.MAX_VALUE));
    }

    @SuppressForbidden(reason = "measures the allocation of the calling thread, which only the com.sun.management bean reports")
    public void testTimingAllocatesNothingPerCall() {
        final java.lang.management.ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        assumeTrue(
            "thread allocation is not measurable on this JVM",
            bean instanceof com.sun.management.ThreadMXBean sun
                && sun.isThreadAllocatedMemorySupported()
                && sun.isThreadAllocatedMemoryEnabled()
        );
        final com.sun.management.ThreadMXBean sun = (com.sun.management.ThreadMXBean) bean;
        final long[] now = { 0 };
        final ProfileCollector stats = TestCollectors.withClock(() -> now[0] += 3);
        final Timer exact = stats.timer(CHUNK_TIME);
        final Timer sampled = stats.timer(BLOCK_TIME);
        for (int warm = 0; warm < 300_000; warm++) {
            exact.start();
            exact.stop();
            sampled.start();
            sampled.stop();
        }
        final long thread = Thread.currentThread().threadId();
        final long before = sun.getThreadAllocatedBytes(thread);
        final int calls = 1_000_000;
        for (int i = 0; i < calls; i++) {
            exact.start();
            exact.stop();
            sampled.start();
            sampled.stop();
        }
        final long allocated = sun.getThreadAllocatedBytes(thread) - before;
        assertThat("less than one byte per timed call, so nothing is allocated per call", allocated, lessThan((long) calls));
    }
}
