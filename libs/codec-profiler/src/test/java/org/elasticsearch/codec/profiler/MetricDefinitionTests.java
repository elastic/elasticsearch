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
import java.util.OptionalLong;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class MetricDefinitionTests extends ESTestCase {

    private static final CounterMetric BYTES = CounterMetric.of("ColumNAR", "blocks", "read_bytes", Metric.Unit.BYTES, "bytes");
    private static final TimerMetric DECODE = TimerMetric.of("ColumNAR", "blocks", "decode_time_nanos", TimingPolicy.ADAPTIVE, "time");
    private static final TimerMetric DECODE_WITH_CPU = DECODE.withCpuTime("decode_cpu_time_nanos", "cpu");

    public void testADefinitionIsValidatedWhenItIsBuilt() {
        expectThrows(IllegalArgumentException.class, () -> CounterMetric.of("C", "a", "b_time_nanos", Metric.Unit.TIME_NANOS, "d"));
        expectThrows(IllegalArgumentException.class, () -> CounterMetric.of("C", "a", "b", Metric.Unit.BYTES, "the unit must show"));
        expectThrows(IllegalArgumentException.class, () -> CounterMetric.of("C", "a", "docs", Metric.Unit.DOC_IDS, "the unit must show"));
        expectThrows(IllegalArgumentException.class, () -> CounterMetric.of("C", "a", "b", Metric.Unit.COUNT, " "));
        expectThrows(IllegalArgumentException.class, () -> TimerMetric.of("C", "a", "b_time", TimingPolicy.EXACT, "the unit must show"));
        expectThrows(NullPointerException.class, () -> TimerMetric.of("C", "a", "b_time_nanos", null, "d"));
        expectThrows(IllegalArgumentException.class, () -> DECODE.withCpuTime("decode_cpu", "the unit must show"));
        expectThrows(IllegalArgumentException.class, () -> DECODE.withCpuTime("decode_time_nanos", "its own CPU time"));
        expectThrows(IllegalArgumentException.class, () -> DECODE.withCpuTime("decode_cpu_time_nanos", ""));
    }

    public void testACpuTimeCompanionIsANewDefinitionThatNeverChangesTheTimer() {
        assertThat(DECODE.cpuTime(), nullValue());
        assertThat(DECODE_WITH_CPU.cpuTime().id(), equalTo(new Metric.Id("ColumNAR", "blocks", "decode_cpu_time_nanos")));
        assertThat("a companion keeps the timer's identity", DECODE_WITH_CPU.id(), equalTo(DECODE.id()));
        assertThat(DECODE_WITH_CPU.policy(), equalTo(DECODE.policy()));
        assertThat("a timer with a companion is another definition", DECODE_WITH_CPU, not(equalTo(DECODE)));
        final IllegalStateException e = expectThrows(IllegalStateException.class, () -> DECODE_WITH_CPU.withCpuTime("x_time_nanos", "d"));
        assertThat(e.getMessage(), containsString("already has the CPU time companion"));
    }

    public void testEveryPartOfADefinitionTellsTwoDefinitionsApart() {
        final List<Metric> variants = new ArrayList<>();
        variants.add(CounterMetric.of(BYTES.id(), Metric.Unit.COUNT, BYTES.description()));
        variants.add(CounterMetric.of(BYTES.id(), BYTES.unit(), "another description"));
        variants.add(TimerMetric.of(DECODE.id(), TimingPolicy.EXACT, DECODE.description()));
        variants.add(TimerMetric.of(DECODE.id(), DECODE.policy(), "another description"));
        variants.add(DECODE.withCpuTime("other_cpu_time_nanos", "cpu"));
        variants.add(DECODE.withCpuTime("decode_cpu_time_nanos", "another cpu description"));
        for (Metric variant : variants) {
            assertThat(variant, not(equalTo(variant instanceof CounterMetric ? BYTES : DECODE_WITH_CPU)));
        }
        assertThat(CounterMetric.of("ColumNAR", "blocks", "read_bytes", Metric.Unit.BYTES, "bytes"), equalTo(BYTES));
        assertThat(DECODE.withCpuTime("decode_cpu_time_nanos", "cpu"), equalTo(DECODE_WITH_CPU));
        assertThat(DECODE.withCpuTime("decode_cpu_time_nanos", "cpu").hashCode(), equalTo(DECODE_WITH_CPU.hashCode()));
    }

    public void testACollectorRefusesAnotherDefinitionOfARegisteredIdentity() {
        final ProfileCollector collector = TestCollectors.withClocks(() -> 0L, () -> 0L);
        collector.counter(BYTES);
        collector.counter(CounterMetric.of("ColumNAR", "blocks", "counted_cpu_time_nanos", Metric.Unit.COUNT, "a counter"));
        collector.timer(DECODE_WITH_CPU);
        final List<Runnable> conflicts = List.of(
            () -> collector.counter(CounterMetric.of(BYTES.id(), Metric.Unit.COUNT, BYTES.description())),
            () -> collector.counter(CounterMetric.of(BYTES.id(), BYTES.unit(), "another description")),
            () -> collector.timer(
                TimerMetric.of("ColumNAR", "blocks", "fetch_time_nanos", TimingPolicy.EXACT, "d")
                    .withCpuTime("counted_cpu_time_nanos", "a companion named like a registered counter")
            ),
            () -> collector.timer(
                TimerMetric.of(DECODE.id(), TimingPolicy.EXACT, DECODE.description()).withCpuTime("decode_cpu_time_nanos", "cpu")
            ),
            () -> collector.timer(DECODE),
            () -> collector.timer(DECODE.withCpuTime("other_cpu_time_nanos", "a second companion")),
            () -> collector.counter(
                CounterMetric.of("ColumNAR", "blocks", "decode_cpu_time_nanos", Metric.Unit.COUNT, "a counter under the companion's name")
            )
        );
        for (Runnable conflict : conflicts) {
            final IllegalStateException e = expectThrows(IllegalStateException.class, conflict::run);
            assertThat(e.getMessage(), containsString("already registered"));
        }
        assertThat(
            "an equal definition gets the same timer",
            collector.timer(DECODE.withCpuTime("decode_cpu_time_nanos", "cpu")),
            equalTo(collector.timer(DECODE_WITH_CPU))
        );
    }

    public void testATimerUsedWithoutACompanionCannotGainOneLater() {
        final long[] now = { 0 };
        final ProfileCollector collector = TestCollectors.withClocks(() -> now[0], () -> now[0]);
        final Timer timer = collector.timer(DECODE);
        timer.start();
        now[0] += 5;
        timer.stop();
        expectThrows(IllegalStateException.class, () -> collector.timer(DECODE_WITH_CPU));
        final TimerMeasurement measured = collector.snapshot().get(DECODE);
        assertThat("no CPU time was ever reported for calls that were not CPU-timed", measured.cpuNanos(), equalTo(OptionalLong.empty()));
    }

    public void testADetailMeasuresCpuTimeLikeItsParentInEitherOrder() {
        for (boolean parentFirst : List.of(true, false)) {
            final long[] elapsedNow = { 0 };
            final long[] cpuNow = { 0 };
            final ProfileCollector parent = TestCollectors.withClocks(() -> elapsedNow[0], () -> cpuNow[0]);
            if (parentFirst) {
                parent.timer(DECODE_WITH_CPU);
            }
            final ProfileCollector detail = parent.detail(bytes -> {});
            final Timer timer = detail.timer(DECODE_WITH_CPU);
            timer.start();
            elapsedNow[0] += 10;
            cpuNow[0] += 3;
            timer.stop();
            final TimerMeasurement expected = new TimerMeasurement(DECODE_WITH_CPU, 10, OptionalLong.of(3), 1, 1, false);
            assertThat(detail.snapshot().get(DECODE_WITH_CPU), equalTo(expected));
            assertThat(parent.snapshot().get(DECODE_WITH_CPU), equalTo(expected));
        }
    }

    public void testACollectorWithoutCpuTimeRegistersOnlyTheElapsedIdentity() {
        final ProfileCollector collector = TestCollectors.withClock(() -> 0L);
        collector.timer(DECODE_WITH_CPU);
        assertThat(collector.snapshot().get(DECODE_WITH_CPU).cpuNanos(), equalTo(OptionalLong.empty()));
        final CounterMetric companionIdentity = CounterMetric.of(
            "ColumNAR",
            "blocks",
            "decode_cpu_time_nanos",
            Metric.Unit.COUNT,
            "free when CPU is not measured"
        );
        collector.counter(companionIdentity).add(3);
        final Snapshot snapshot = collector.snapshot();
        assertThat(snapshot.get(DECODE_WITH_CPU).cpuNanos(), equalTo(OptionalLong.empty()));
        assertThat(snapshot.get(companionIdentity), equalTo(new CounterMeasurement(companionIdentity, 3)));
    }

    public void testTheLimitCountsACpuTimeCompanionBeforeAnythingIsRegistered() {
        final ProfileCollector collector = TestCollectors.withClocks(() -> 0L, () -> 0L);
        for (int i = 0; i < ProfileCollector.MAX_METRICS - 1; i++) {
            collector.counter(CounterMetric.of("C", "fill", "m" + i, Metric.Unit.COUNT, "filler"));
        }
        expectThrows(IllegalStateException.class, () -> collector.timer(DECODE_WITH_CPU));
        assertThat("neither side was registered", collector.snapshot().get(DECODE.id()), nullValue());
        collector.timer(DECODE);
        assertThat(collector.snapshot().measurements().size(), equalTo(ProfileCollector.MAX_METRICS));
    }

    public void testADetailReservesATimerAndItsCompanionTogether() {
        final ProfileCollector parent = TestCollectors.withClocks(() -> 0L, () -> 0L);
        final List<Long> reserved = new ArrayList<>();
        final ProfileCollector detail = parent.detail(reserved::add);
        detail.timer(DECODE_WITH_CPU);
        detail.counter(BYTES);
        assertThat(
            reserved,
            equalTo(
                List.of(
                    ProfileCollector.DETAIL_BYTES,
                    ProfileCollector.TIMER_BYTES + ProfileCollector.CPU_TIME_BYTES,
                    ProfileCollector.COUNTER_BYTES
                )
            )
        );
    }

    public void testARefusedReservationLeavesTheDetailWithoutTheMetricAndTheParentWithIt() {
        final ProfileCollector parent = TestCollectors.withClocks(() -> 0L, () -> 0L);
        final int[] reservations = { 0 };
        final ProfileCollector detail = parent.detail(bytes -> {
            if (reservations[0]++ > 0) {
                throw new IllegalStateException("refused");
            }
        });
        expectThrows(IllegalStateException.class, () -> detail.timer(DECODE_WITH_CPU));
        expectThrows(IllegalStateException.class, () -> detail.counter(BYTES));
        assertTrue(detail.isEmpty());
        assertThat(
            parent.snapshot().get(DECODE_WITH_CPU),
            equalTo(new TimerMeasurement(DECODE_WITH_CPU, 0, OptionalLong.of(0), 0, 0, false))
        );
        assertThat(parent.snapshot().get(BYTES), equalTo(new CounterMeasurement(BYTES, 0)));
        final IllegalStateException e = expectThrows(IllegalStateException.class, () -> parent.detail(bytes -> {}).detail(bytes -> {}));
        assertThat(e.getMessage(), containsString("cannot have details of its own"));
    }
}
