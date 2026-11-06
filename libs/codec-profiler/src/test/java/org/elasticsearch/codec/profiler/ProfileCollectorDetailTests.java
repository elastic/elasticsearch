/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.codec.profiler;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class ProfileCollectorDetailTests extends ESTestCase {

    private static final ProfileCollector.Reservations NO_ACCOUNTING = bytes -> {};

    private static final CounterMetric EVENTS = CounterMetric.of("test_codec", "blocks", "read", Metric.Unit.COUNT, "events");
    private static final TimerMetric TIME = TimerMetric.of("test_codec", "blocks", "read_time_nanos", TimingPolicy.EXACT, "time");
    private static final TimerMetric ADAPTIVE_TIME = TimerMetric.of(TIME.id(), TimingPolicy.ADAPTIVE, TIME.description());
    private static final TimerMetric TIME_WITH_CPU = TIME.withCpuTime("read_cpu_time_nanos", "cpu");
    private static final TimerMetric OTHER_TIME = TimerMetric.of(
        "test_codec",
        "pages",
        "read_time_nanos",
        TimingPolicy.EXACT,
        "other time"
    );

    // NOTE: The clock moves only when the test does work, never when it is read, so clock reads do not change the times.
    private static final class ManualClock implements LongSupplier {
        long now;
        long reads;

        @Override
        public long getAsLong() {
            reads++;
            return now;
        }
    }

    private record Call(int pair, long cost) {}

    public void testADetailCounterCountsItsEventsAndAddsThemToItsParent() {
        final ProfileCollector parent = TestCollectors.countersOnly();
        final ProfileCollector detail = parent.detail(NO_ACCOUNTING);
        final Counter direct = parent.counter(EVENTS);
        final Counter linked = detail.counter(EVENTS);
        direct.add(3);
        linked.add(5);
        assertThat(TestCollectors.toKeyedValues(detail.snapshot()), equalTo(Map.of("test_codec.blocks.read", 5L)));
        assertThat(
            "the parent counts every event once",
            TestCollectors.toKeyedValues(parent.snapshot()),
            equalTo(Map.of("test_codec.blocks.read", 8L))
        );
    }

    public void testRegisteringOnADetailRegistersOnItsParent() {
        final ProfileCollector parent = TestCollectors.withClock(new ManualClock());
        final ProfileCollector detail = parent.detail(NO_ACCOUNTING);
        detail.counter(EVENTS);
        detail.timer(ADAPTIVE_TIME);
        assertThat(
            TestCollectors.toKeyedValues(parent.snapshot()).keySet(),
            equalTo(TestCollectors.toKeyedValues(detail.snapshot()).keySet())
        );
    }

    public void testADetailReportsEachAllocationBeforeItHappens() {
        final ProfileCollector parent = TestCollectors.withClocks(new ManualClock(), new ManualClock());
        final List<Long> seen = new ArrayList<>();
        final ProfileCollector detail = parent.detail(seen::add);
        detail.counter(EVENTS);
        detail.counter(EVENTS);
        detail.timer(OTHER_TIME);
        detail.timer(TIME_WITH_CPU);
        assertThat(
            seen,
            equalTo(
                List.of(
                    ProfileCollector.DETAIL_BYTES,
                    ProfileCollector.COUNTER_BYTES,
                    ProfileCollector.TIMER_BYTES,
                    ProfileCollector.TIMER_BYTES + ProfileCollector.CPU_TIME_BYTES
                )
            )
        );
    }

    public void testARefusedAllocationLeavesTheMetricUnregisteredOnTheDetail() {
        final ProfileCollector parent = TestCollectors.countersOnly();
        final int[] reservations = { 0 };
        final ProfileCollector detail = parent.detail(bytes -> {
            if (reservations[0]++ > 0) {
                throw new IllegalStateException("no memory");
            }
        });
        expectThrows(IllegalStateException.class, () -> detail.counter(EVENTS));
        assertTrue(detail.isEmpty());
    }

    public void testADetailCannotHaveDetailsOfItsOwn() {
        expectThrows(IllegalStateException.class, () -> TestCollectors.elapsed().detail(NO_ACCOUNTING).detail(NO_ACCOUNTING));
    }

    public void testADetailOfACollectorWithoutAClockHasOnlyTheInertTimer() {
        final ProfileCollector parent = TestCollectors.countersOnly();
        final ProfileCollector detail = parent.detail(NO_ACCOUNTING);
        assertThat(detail.timer(TIME), sameInstance(Timer.INERT));
        assertTrue("nothing is registered on the detail or its parent", detail.isEmpty() && parent.isEmpty());
    }

    public void testAPolicyThatDisagreesWithTheParentsIsRefused() {
        final ProfileCollector parent = TestCollectors.withClock(new ManualClock());
        parent.timer(TIME);
        expectThrows(IllegalStateException.class, () -> parent.detail(NO_ACCOUNTING).timer(ADAPTIVE_TIME));
    }

    public void testTheParentEndsInTheSameStateWhateverIsADetail() {
        for (TimingPolicy policy : TimingPolicy.values()) {
            final List<Call> stream = randomStream(between(2, 6), between(1, 5_000));
            final int pairs = stream.stream().mapToInt(Call::pair).max().orElse(0) + 1;
            final boolean[] linked = new boolean[pairs];
            for (int i = 0; i < pairs; i++) {
                linked[i] = randomBoolean();
            }
            final Run allDirect = run(stream, policy, new boolean[pairs]);
            final Run mixed = run(stream, policy, linked);
            assertThat("decisions and accounting of the parent", mixed.parent.snapshot(), equalTo(allDirect.parent.snapshot()));
        }
    }

    public void testEachDetailEqualsACollectorThatSawOnlyItsCalls() {
        for (TimingPolicy policy : TimingPolicy.values()) {
            final List<Call> stream = randomStream(between(2, 6), between(1, 5_000));
            final int pairs = stream.stream().mapToInt(Call::pair).max().orElse(0) + 1;
            final boolean[] allLinked = new boolean[pairs];
            java.util.Arrays.fill(allLinked, true);
            final Run run = run(stream, policy, allLinked);
            for (int pair = 0; pair < pairs; pair++) {
                final int selected = pair;
                final Run oracle = run(stream.stream().filter(call -> call.pair == selected).toList(), policy, new boolean[pairs]);
                assertThat("pair " + pair, run.details[pair].snapshot(), equalTo(oracle.parent.snapshot()));
            }
        }
    }

    public void testTwoFieldsInTurnAreEachEstimatedFromTheirOwnCalls() {
        final List<Call> stream = new ArrayList<>();
        for (int i = 0; i < 20_000; i++) {
            stream.add(new Call(i % 2, i % 2 == 0 ? 1 : 100));
        }
        final Run run = run(stream, TimingPolicy.ADAPTIVE, new boolean[] { true, true });
        assertThat(TestCollectors.toKeyedValues(run.details[0].snapshot()).get("test_codec.blocks.read_time_nanos"), equalTo(10_000L));
        assertThat(TestCollectors.toKeyedValues(run.details[1].snapshot()).get("test_codec.blocks.read_time_nanos"), equalTo(1_000_000L));
        assertThat(TestCollectors.toKeyedValues(run.parent.snapshot()).get("test_codec.blocks.read_time_nanos"), greaterThan(0L));
    }

    public void testFieldsInRoundRobinWithTheSamplingGapAreEachEstimated() {
        for (int fields = 2; fields <= 8; fields++) {
            final List<Call> stream = new ArrayList<>();
            for (int i = 0; i < 4_000 * fields; i++) {
                stream.add(new Call(i % fields, (i % fields) + 1));
            }
            final boolean[] linked = new boolean[fields];
            java.util.Arrays.fill(linked, true);
            final Run run = run(stream, TimingPolicy.ADAPTIVE, linked);
            for (int field = 0; field < fields; field++) {
                assertThat(
                    fields + " fields, field " + field,
                    TestCollectors.toKeyedValues(run.details[field].snapshot()).get("test_codec.blocks.read_time_nanos"),
                    equalTo(4_000L * (field + 1))
                );
            }
        }
    }

    public void testARareFieldIsMeasuredEvenWhenTheParentSamplesSparsely() {
        final List<Call> stream = new ArrayList<>();
        for (int i = 0; i < 300_000; i++) {
            stream.add(new Call(i >= 262_144 && i % 1_000 == 0 ? 1 : 0, i % 1_000 == 0 ? 50 : 1));
        }
        final Run run = run(stream, TimingPolicy.ADAPTIVE, new boolean[] { false, true });
        final long rareCalls = stream.stream().filter(call -> call.pair == 1).count();
        assertThat(
            TestCollectors.toKeyedValues(run.details[1].snapshot()).get("test_codec.blocks.read_time_nanos"),
            equalTo(rareCalls * 50)
        );
        assertThat(run.details[1].snapshot().get(ADAPTIVE_TIME).sampledCalls(), equalTo(rareCalls));
    }

    public void testACostThatDriftsIsEstimatedLikeAStandaloneCollectorWould() {
        final List<Call> stream = new ArrayList<>();
        for (int i = 0; i < 10_000; i++) {
            stream.add(new Call(randomIntBetween(0, 2), i < 1_000 ? 500 : 5));
        }
        final Run run = run(stream, TimingPolicy.ADAPTIVE, new boolean[] { true, true, true });
        for (int pair = 0; pair < 3; pair++) {
            final int selected = pair;
            final Run oracle = run(stream.stream().filter(call -> call.pair == selected).toList(), TimingPolicy.ADAPTIVE, new boolean[3]);
            assertThat(run.details[pair].snapshot(), equalTo(oracle.parent.snapshot()));
        }
    }

    public void testTheClockIsReadOnlyForCallsThatTheParentOrTheDetailMeasures() {
        final ManualClock clock = new ManualClock();
        final ProfileCollector parent = TestCollectors.withClock(clock);
        final Timer direct = parent.timer(ADAPTIVE_TIME);
        final Timer linked = parent.detail(NO_ACCOUNTING).timer(ADAPTIVE_TIME);
        final Cursor parentCursor = new Cursor();
        final Cursor detailCursor = new Cursor();
        long expectedReads = 0;
        for (int i = 0; i < 20_000; i++) {
            final boolean throughDetail = randomBoolean();
            final boolean parentMeasures = parentCursor.next();
            final boolean detailMeasures = throughDetail && detailCursor.next();
            if (parentMeasures || detailMeasures) {
                expectedReads += 2;
            }
            final Timer timer = throughDetail ? linked : direct;
            timer.start();
            clock.now++;
            timer.stop();
        }
        assertThat(clock.reads, equalTo(expectedReads));
    }

    public void testAThrowingCallIsCountedAndTimedByBoth() {
        final ManualClock clock = new ManualClock();
        final ProfileCollector parent = TestCollectors.withClock(clock);
        final ProfileCollector detail = parent.detail(NO_ACCOUNTING);
        final Timer timer = detail.timer(TIME);
        expectThrows(IllegalStateException.class, () -> {
            timer.start();
            try {
                clock.now += 7;
                throw new IllegalStateException("boom");
            } finally {
                timer.stop();
            }
        });
        assertThat(TestCollectors.toKeyedValues(parent.snapshot()).get("test_codec.blocks.read_time_nanos"), equalTo(7L));
        assertThat(TestCollectors.toKeyedValues(detail.snapshot()).get("test_codec.blocks.read_time_nanos"), equalTo(7L));
        assertThat(detail.snapshot().get(TIME).totalCalls(), equalTo(1L));
    }

    public void testOverlappingCallsOnTheParentAndADetailAreAContractViolation() {
        final ProfileCollector parent = TestCollectors.withClock(new ManualClock());
        final Timer direct = parent.timer(TIME);
        final Timer linked = parent.detail(NO_ACCOUNTING).timer(TIME);
        linked.start();
        expectThrows(AssertionError.class, direct::start);
    }

    public void testAMissingStopFailsTheNextStart() {
        final Timer linked = TestCollectors.withClock(new ManualClock()).detail(NO_ACCOUNTING).timer(TIME);
        linked.start();
        expectThrows(AssertionError.class, linked::start);
    }

    public void testCallsOfDifferentMetricsNestFreely() {
        final ManualClock clock = new ManualClock();
        final ProfileCollector parent = TestCollectors.withClock(clock);
        final ProfileCollector detail = parent.detail(NO_ACCOUNTING);
        final Timer outer = detail.timer(OTHER_TIME);
        final Timer inner = detail.timer(TIME);
        outer.start();
        clock.now += 2;
        inner.start();
        clock.now += 3;
        inner.stop();
        outer.stop();
        assertThat(TestCollectors.toKeyedValues(detail.snapshot()).get("test_codec.pages.read_time_nanos"), equalTo(5L));
        assertThat(TestCollectors.toKeyedValues(detail.snapshot()).get("test_codec.blocks.read_time_nanos"), equalTo(3L));
        assertThat(parent.snapshot(), equalTo(detail.snapshot()));
    }

    public void testTheParentsInFlightStateIsNotTouchedByALinkedCall() {
        final ManualClock clock = new ManualClock();
        final ProfileCollector parent = TestCollectors.withClock(clock);
        final Timer direct = parent.timer(TIME);
        final Timer linked = parent.detail(NO_ACCOUNTING).timer(TIME);
        clock.now = 100;
        linked.start();
        clock.now = 103;
        linked.stop();
        clock.now = 200;
        direct.start();
        clock.now = 210;
        direct.stop();
        assertThat(
            "each call is timed from its own start",
            TestCollectors.toKeyedValues(parent.snapshot()).get("test_codec.blocks.read_time_nanos"),
            equalTo(13L)
        );
    }

    public void testADetailMeasuresCpuTimeWithItsParentAndFailsClosedWithIt() {
        final ManualClock clock = new ManualClock();
        final ManualClock cpu = new ManualClock();
        final ProfileCollector parent = TestCollectors.withClocks(clock, cpu);
        final ProfileCollector detail = parent.detail(NO_ACCOUNTING);
        final Timer timer = detail.timer(TIME_WITH_CPU);
        timer.start();
        clock.now += 10;
        cpu.now += 4;
        timer.stop();
        assertThat(TestCollectors.toKeyedValues(detail.snapshot()).get("test_codec.blocks.read_cpu_time_nanos"), equalTo(4L));
        assertThat(TestCollectors.toKeyedValues(parent.snapshot()).get("test_codec.blocks.read_cpu_time_nanos"), equalTo(4L));
        assertThat(
            "the CPU side of a call is measured with its elapsed side, under one coverage",
            detail.snapshot().get(TIME_WITH_CPU),
            equalTo(new TimerMeasurement(TIME_WITH_CPU, 10, java.util.OptionalLong.of(4), 1, 1, false))
        );

        timer.start();
        cpu.now -= 100;
        timer.stop();
        for (ProfileCollector stats : List.of(parent, detail)) {
            assertThat(TestCollectors.toKeyedValues(stats.snapshot()), not(hasKey("test_codec.blocks.read_cpu_time_nanos")));
            assertThat(stats.snapshot().get(TIME_WITH_CPU).cpuNanos(), equalTo(java.util.OptionalLong.empty()));
            assertThat(TestCollectors.toKeyedValues(stats.snapshot()), hasKey("test_codec.blocks.read_time_nanos"));
        }
    }

    public void testADetailOfATimerThatMeasuresCpuTimeMeasuresItToo() {
        final ManualClock clock = new ManualClock();
        final ManualClock cpu = new ManualClock();
        final ProfileCollector parent = TestCollectors.withClocks(clock, cpu);
        parent.timer(TIME_WITH_CPU);
        final ProfileCollector detail = parent.detail(NO_ACCOUNTING);
        final Timer timer = detail.timer(TIME_WITH_CPU);
        timer.start();
        clock.now += 10;
        cpu.now += 6;
        timer.stop();
        assertThat(TestCollectors.toKeyedValues(detail.snapshot()).get("test_codec.blocks.read_cpu_time_nanos"), equalTo(6L));
        assertThat(TestCollectors.toKeyedValues(parent.snapshot()).get("test_codec.blocks.read_cpu_time_nanos"), equalTo(6L));
    }

    public void testCountsAndExactTimesHoldAtTheLargestValueOnBothSides() {
        final ManualClock clock = new ManualClock();
        final ProfileCollector parent = TestCollectors.withClock(clock);
        final ProfileCollector first = parent.detail(NO_ACCOUNTING);
        final ProfileCollector second = parent.detail(NO_ACCOUNTING);
        first.counter(EVENTS).add(Long.MAX_VALUE - 1);
        second.counter(EVENTS).add(5);
        assertThat(TestCollectors.toKeyedValues(parent.snapshot()).get("test_codec.blocks.read"), equalTo(Long.MAX_VALUE));
        assertThat(
            MetricRules.add(
                TestCollectors.toKeyedValues(first.snapshot()).get("test_codec.blocks.read"),
                TestCollectors.toKeyedValues(second.snapshot()).get("test_codec.blocks.read")
            ),
            equalTo(Long.MAX_VALUE)
        );

        final Timer timer = first.timer(TIME);
        timer.start();
        clock.now = Long.MAX_VALUE / 2 + 1;
        timer.stop();
        timer.start();
        clock.now = Long.MAX_VALUE;
        timer.stop();
        timer.start();
        timer.stop();
        assertThat(TestCollectors.toKeyedValues(first.snapshot()).get("test_codec.blocks.read_time_nanos"), equalTo(Long.MAX_VALUE));
        assertThat(TestCollectors.toKeyedValues(parent.snapshot()).get("test_codec.blocks.read_time_nanos"), equalTo(Long.MAX_VALUE));
    }

    public void testASnapshotReportsThePolicyAndTheCoverageOfEachTimer() {
        final ManualClock clock = new ManualClock();
        final ProfileCollector stats = TestCollectors.withClock(clock);
        final Timer exact = stats.timer(OTHER_TIME);
        final Timer adaptive = stats.timer(ADAPTIVE_TIME);
        stats.counter(EVENTS);
        final int calls = between(1, 3_000);
        for (int i = 0; i < calls; i++) {
            for (Timer timer : List.of(exact, adaptive)) {
                timer.start();
                clock.now++;
                timer.stop();
            }
        }
        final Snapshot snapshot = stats.snapshot();
        assertThat(snapshot.get(EVENTS), equalTo(new CounterMeasurement(EVENTS, 0)));
        assertThat(
            snapshot.get(OTHER_TIME),
            equalTo(new TimerMeasurement(OTHER_TIME, calls, java.util.OptionalLong.empty(), calls, calls, false))
        );
        final TimerMeasurement sampled = snapshot.get(ADAPTIVE_TIME);
        assertThat(sampled.definition().policy(), equalTo(TimingPolicy.ADAPTIVE));
        assertThat(sampled.totalCalls(), equalTo((long) calls));
        assertThat(sampled.sampledCalls(), equalTo(new Cursor().countMeasured(calls)));
        assertThat("an estimate exactly when calls went unmeasured", sampled.estimated(), equalTo(sampled.sampledCalls() < calls));
    }

    public void testTheSaturatingAddHoldsAtTheLargestValue() {
        assertThat(MetricRules.add(3, 4), equalTo(7L));
        assertThat(MetricRules.add(Long.MAX_VALUE, 1), equalTo(Long.MAX_VALUE));
        assertThat(MetricRules.add(Long.MAX_VALUE - 2, Long.MAX_VALUE - 2), equalTo(Long.MAX_VALUE));
    }

    public void testADetailCounterIsHandedOutOncePerMetric() {
        final ProfileCollector detail = TestCollectors.countersOnly().detail(NO_ACCOUNTING);
        assertThat(detail.counter(EVENTS), sameInstance(detail.counter(EVENTS)));
    }

    // NOTE: An independent copy of the ADAPTIVE rule, used as the oracle of which calls a timer measures.
    private static final class Cursor {
        long calls;
        long measuredUpTo;

        boolean next() {
            final boolean measures = (calls - measuredUpTo) >= Math.min(measuredUpTo >>> 8, 1024);
            calls++;
            if (measures) {
                measuredUpTo = calls;
            }
            return measures;
        }

        long countMeasured(int calls) {
            long measured = 0;
            for (int i = 0; i < calls; i++) {
                if (next()) {
                    measured++;
                }
            }
            return measured;
        }
    }

    private record Run(ProfileCollector parent, ProfileCollector[] details) {}

    private static Run run(List<Call> stream, TimingPolicy policy, boolean[] linked) {
        final ManualClock clock = new ManualClock();
        final ProfileCollector parent = TestCollectors.withClock(clock);
        final ProfileCollector[] details = new ProfileCollector[linked.length];
        final Timer[] timers = new Timer[linked.length];
        final Counter[] counters = new Counter[linked.length];
        for (int pair = 0; pair < linked.length; pair++) {
            final ProfileCollector target = linked[pair] ? (details[pair] = parent.detail(NO_ACCOUNTING)) : parent;
            timers[pair] = target.timer(TimerMetric.of(TIME.id(), policy, TIME.description()));
            counters[pair] = target.counter(EVENTS);
        }
        for (Call call : stream) {
            timers[call.pair].start();
            clock.now += call.cost;
            timers[call.pair].stop();
            counters[call.pair].add(1);
        }
        return new Run(parent, details);
    }

    private List<Call> randomStream(int pairs, int length) {
        final List<Call> stream = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            stream.add(new Call(randomIntBetween(0, pairs - 1), randomLongBetween(0, 1_000)));
        }
        return stream;
    }
}
