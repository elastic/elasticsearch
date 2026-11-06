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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class SnapshotTests extends ESTestCase {

    private static final CounterMetric READ = CounterMetric.of("ColumNAR", "blocks", "read", Metric.Unit.COUNT, "blocks fetched");
    private static final TimerMetric READ_TIME = TimerMetric.of(
        "ColumNAR",
        "blocks",
        "read_time_nanos",
        TimingPolicy.ADAPTIVE,
        "fetch time"
    ).withCpuTime("read_cpu_time_nanos", "fetch CPU time");
    private static final TimerMetric OPEN_TIME = TimerMetric.of("ColumNAR", "readers", "open_time_nanos", TimingPolicy.EXACT, "open time");
    private static final CounterMetric OTHER = CounterMetric.of("Numeric", "pages", "read", Metric.Unit.COUNT, "pages read");

    public void testMeasurementsAreOrderedByMetricWhateverTheyAreGivenIn() {
        final List<Measurement> measurements = new ArrayList<>(
            List.of(new CounterMeasurement(OTHER, 1), new CounterMeasurement(READ, 2), exact(OPEN_TIME, 3, 1), timer(READ_TIME, 4, 5, 1, 2))
        );
        Collections.shuffle(measurements, random());
        final Snapshot snapshot = Snapshot.of(measurements);
        final List<Metric.Id> ids = snapshot.measurements().stream().map(Measurement::id).toList();
        assertThat(ids, equalTo(List.of(READ.id(), READ_TIME.id(), OPEN_TIME.id(), OTHER.id())));
        assertThat(
            TestCollectors.toKeyedValues(snapshot),
            equalTo(
                Map.of(
                    "ColumNAR.blocks.read",
                    2L,
                    "ColumNAR.blocks.read_time_nanos",
                    4L,
                    "ColumNAR.blocks.read_cpu_time_nanos",
                    5L,
                    "ColumNAR.readers.open_time_nanos",
                    3L,
                    "Numeric.pages.read",
                    1L
                )
            )
        );
        assertThat(snapshot.get(OPEN_TIME), equalTo(exact(OPEN_TIME, 3, 1)));
        assertThat(
            "a companion is part of its timer's measurement, not one of its own",
            snapshot.get(READ_TIME.cpuTime().id()),
            nullValue()
        );
    }

    public void testATypedLookupRefusesAnotherDefinitionOfTheIdentity() {
        final Snapshot snapshot = Snapshot.of(List.of(new CounterMeasurement(READ, 2)));
        assertThat(snapshot.get(READ), equalTo(new CounterMeasurement(READ, 2)));
        final CounterMetric redefined = CounterMetric.of(READ.id(), Metric.Unit.COUNT, "another meaning");
        expectThrows(IllegalStateException.class, () -> snapshot.get(redefined));
        assertThat(snapshot.get(OTHER), nullValue());
    }

    public void testASnapshotIsImmutable() {
        final Snapshot snapshot = Snapshot.of(List.of(new CounterMeasurement(READ, 2)));
        expectThrows(UnsupportedOperationException.class, () -> snapshot.measurements().add(new CounterMeasurement(OTHER, 1)));
        expectThrows(UnsupportedOperationException.class, () -> TestCollectors.toKeyedValues(snapshot).put("x", 1L));

        final ProfileCollector collector = TestCollectors.countersOnly();
        final Counter counter = collector.counter(READ);
        counter.add(3);
        final Snapshot taken = collector.snapshot();
        counter.add(4);
        assertThat("later work does not change a taken snapshot", taken.get(READ).value(), equalTo(3L));
        assertThat(collector.snapshot().get(READ).value(), equalTo(7L));
    }

    public void testAnIdentityIsMeasuredOnceInASnapshotCountingCompanions() {
        expectThrows(
            IllegalArgumentException.class,
            () -> Snapshot.of(List.of(new CounterMeasurement(READ, 1), new CounterMeasurement(READ, 2)))
        );
        final CounterMetric namedLikeTheCompanion = CounterMetric.of(READ_TIME.cpuTime().id(), Metric.Unit.COUNT, "a counter");
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> Snapshot.of(List.of(timer(READ_TIME, 1, 1, 1, 1), new CounterMeasurement(namedLikeTheCompanion, 1)))
        );
        assertThat(e.getMessage(), containsString("read_cpu_time_nanos"));
    }

    public void testAMeasurementRefusesAnInconsistentState() {
        expectThrows(IllegalArgumentException.class, () -> new CounterMeasurement(READ, -1));
        expectThrows(IllegalArgumentException.class, () -> new TimerMeasurement(OPEN_TIME, -1, OptionalLong.empty(), 1, 1, false));
        expectThrows(IllegalArgumentException.class, () -> new TimerMeasurement(OPEN_TIME, 1, OptionalLong.empty(), 2, 1, true));
        expectThrows(IllegalArgumentException.class, () -> new TimerMeasurement(OPEN_TIME, 1, OptionalLong.empty(), 1, 2, false));
        final IllegalArgumentException orphan = expectThrows(
            IllegalArgumentException.class,
            () -> new TimerMeasurement(OPEN_TIME, 1, OptionalLong.of(1), 1, 1, false)
        );
        assertThat(orphan.getMessage(), containsString("no CPU time companion"));
        expectThrows(IllegalArgumentException.class, () -> new TimerMeasurement(READ_TIME, 1, OptionalLong.of(-1), 1, 1, false));
        new TimerMeasurement(READ_TIME, 1, OptionalLong.of(0), 1, 1, false);
        new TimerMeasurement(OPEN_TIME, 1, OptionalLong.empty(), 1, 1, true);
    }

    public void testCountsAndCallsAreSummedAndHoldAtTheLargestValue() {
        final Snapshot left = Snapshot.of(List.of(new CounterMeasurement(READ, Long.MAX_VALUE - 1), timer(READ_TIME, 100, 70, 12, 40)));
        final Snapshot right = Snapshot.of(List.of(new CounterMeasurement(READ, 5), timer(READ_TIME, 50, 30, 3, 3)));
        final Snapshot merged = Snapshot.merge(List.of(left, right));
        assertThat(merged.get(READ).value(), equalTo(Long.MAX_VALUE));
        assertThat(merged.get(READ_TIME), equalTo(timer(READ_TIME, 150, 100, 15, 43)));
    }

    public void testAnEstimateStaysAnEstimateWhenSaturatedCountsCannotShowIt() {
        final TimerMeasurement sampled = new TimerMeasurement(
            OPEN_TIME,
            1,
            OptionalLong.empty(),
            Long.MAX_VALUE - 10,
            Long.MAX_VALUE - 1,
            true
        );
        final TimerMeasurement exact = exact(OPEN_TIME, 1, 100);
        final TimerMeasurement merged = Snapshot.merge(List.of(Snapshot.of(List.of(sampled)), Snapshot.of(List.of(exact)))).get(OPEN_TIME);
        assertThat(merged.sampledCalls(), equalTo(merged.totalCalls()));
        assertTrue("a part was sampled, so the sum is an estimate", merged.estimated());
        assertFalse(Snapshot.merge(List.of(Snapshot.of(List.of(exact)), Snapshot.of(List.of(exact)))).get(OPEN_TIME).estimated());
    }

    public void testTwoDefinitionsOfOneIdentityDoNotMerge() {
        final TimerMeasurement base = timer(READ_TIME, 1, 1, 1, 1);
        final List<Measurement> others = List.of(
            new CounterMeasurement(
                CounterMetric.of(READ_TIME.id().group(), READ_TIME.id().component(), READ_TIME.id().name(), Metric.Unit.COUNT, "c"),
                1
            ),
            new TimerMeasurement(
                TimerMetric.of(READ_TIME.id(), TimingPolicy.EXACT, READ_TIME.description()),
                1,
                OptionalLong.empty(),
                1,
                1,
                false
            ),
            new TimerMeasurement(
                TimerMetric.of(READ_TIME.id(), TimingPolicy.ADAPTIVE, "another meaning"),
                1,
                OptionalLong.empty(),
                1,
                1,
                false
            ),
            new TimerMeasurement(
                TimerMetric.of(READ_TIME.id(), TimingPolicy.ADAPTIVE, READ_TIME.description()),
                1,
                OptionalLong.empty(),
                1,
                1,
                false
            )
        );
        for (Measurement other : others) {
            expectThrows(
                IllegalStateException.class,
                () -> Snapshot.merge(List.of(Snapshot.of(List.of(base)), Snapshot.of(List.of(other))))
            );
        }
        final CounterMetric bytes = CounterMetric.of("ColumNAR", "blocks", "payload_bytes", Metric.Unit.BYTES, "bytes");
        final CounterMetric counted = CounterMetric.of("ColumNAR", "blocks", "payload_bytes", Metric.Unit.COUNT, "bytes");
        final ProfileCollector one = TestCollectors.countersOnly();
        final ProfileCollector two = TestCollectors.countersOnly();
        one.counter(bytes).add(1);
        two.counter(counted).add(1);
        final IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> Snapshot.merge(List.of(one.snapshot(), two.snapshot()))
        );
        assertThat("the units of two collectors are compared, not only the identities", e.getMessage(), containsString("payload_bytes"));
    }

    public void testACpuTimeIsKeptOnlyWhenEveryPartWithItsTimerHasIt() {
        final Snapshot full = Snapshot.of(List.of(timer(READ_TIME, 100, 60, 4, 8)));
        final Snapshot unrelated = Snapshot.of(List.of(new CounterMeasurement(OTHER, 1)));
        final Snapshot timerWithoutCpu = Snapshot.of(List.of(new TimerMeasurement(READ_TIME, 100, OptionalLong.empty(), 4, 8, true)));

        final Snapshot kept = Snapshot.merge(List.of(full, unrelated, full));
        assertThat("a part that did none of the work does not count", kept.get(READ_TIME), equalTo(timer(READ_TIME, 200, 120, 8, 16)));
        assertThat(kept.get(OTHER), equalTo(new CounterMeasurement(OTHER, 1)));

        final Snapshot dropped = Snapshot.merge(List.of(full, unrelated, timerWithoutCpu));
        assertThat(
            "a CPU total of part of the work is never shown",
            dropped.get(READ_TIME),
            equalTo(new TimerMeasurement(READ_TIME, 200, OptionalLong.empty(), 8, 16, true))
        );
    }

    public void testMergingIsCommutativeAssociativeAndIndependentOfGrouping() {
        final List<Snapshot> parts = new ArrayList<>();
        final int count = between(1, 8);
        for (int i = 0; i < count; i++) {
            final List<Measurement> measurements = new ArrayList<>();
            if (randomBoolean()) {
                measurements.add(new CounterMeasurement(READ, randomLongBetween(0, 1000)));
            }
            if (randomBoolean()) {
                final long total = randomLongBetween(0, 1000);
                final long sampled = randomLongBetween(0, total);
                measurements.add(
                    new TimerMeasurement(
                        READ_TIME,
                        randomLongBetween(0, 10_000),
                        randomBoolean() ? OptionalLong.of(randomLongBetween(0, 10_000)) : OptionalLong.empty(),
                        sampled,
                        total,
                        sampled < total || randomBoolean()
                    )
                );
            }
            parts.add(Snapshot.of(measurements));
        }
        final Snapshot merged = Snapshot.merge(parts);
        final List<Snapshot> shuffled = new ArrayList<>(parts);
        Collections.shuffle(shuffled, random());
        assertThat(Snapshot.merge(shuffled), equalTo(merged));
        final int cut = between(0, parts.size());
        final Snapshot grouped = Snapshot.merge(
            List.of(Snapshot.merge(parts.subList(0, cut)), Snapshot.merge(parts.subList(cut, parts.size())))
        );
        assertThat(grouped, equalTo(merged));
        assertThat("the empty snapshot is the identity", Snapshot.merge(List.of(merged, Snapshot.EMPTY)), equalTo(merged));
        assertThat(Snapshot.merge(List.of()), equalTo(Snapshot.EMPTY));
    }

    private static TimerMeasurement exact(TimerMetric definition, long value, long calls) {
        return new TimerMeasurement(definition, value, OptionalLong.empty(), calls, calls, false);
    }

    private static TimerMeasurement timer(TimerMetric definition, long elapsed, long cpu, long sampled, long total) {
        return new TimerMeasurement(definition, elapsed, OptionalLong.of(cpu), sampled, total, sampled < total);
    }
}
