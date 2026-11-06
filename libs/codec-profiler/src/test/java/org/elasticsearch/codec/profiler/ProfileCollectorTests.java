/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.codec.profiler;

import org.elasticsearch.codec.profiler.Scopes.Binding;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class ProfileCollectorTests extends ESTestCase {

    private static final CounterMetric EVENTS = CounterMetric.of("columnar", "test", "events", Metric.Unit.COUNT, "events that happened");
    private static final CounterMetric PAYLOAD_BYTES = CounterMetric.of(
        "columnar",
        "test",
        "payload_bytes",
        Metric.Unit.BYTES,
        "bytes moved"
    );
    private static final TimerMetric WORK_TIME = TimerMetric.of(
        "columnar",
        "test",
        "work_time_nanos",
        TimingPolicy.EXACT,
        "time spent working"
    );
    private static final TimerMetric WORK_TIME_WITH_CPU = WORK_TIME.withCpuTime("work_cpu_time_nanos", "thread CPU time spent working");

    @After
    public void assertNothingIsLeftBound() {
        assertFalse("a test left a scope bound on its thread", Scopes.isBound());
    }

    public void testTheModeDecidesWhatIsMeasured() {
        final ProfileCollector counters = ProfileCollector.create(ProfileCollector.Mode.COUNTERS);
        assertSame("counters read no clock and register no timer", Timer.INERT, counters.timer(WORK_TIME));
        assertTrue(counters.isEmpty());
        for (ProfileCollector.Mode mode : List.of(ProfileCollector.Mode.ELAPSED, ProfileCollector.Mode.ELAPSED_AND_CPU)) {
            final ProfileCollector collector = ProfileCollector.create(mode);
            collector.counter(EVENTS).add(3);
            final Timer timer = collector.timer(WORK_TIME_WITH_CPU);
            assertNotNull(mode + " measures elapsed time", timer);
            timer.start();
            timer.stop();
            assertThat(collector.snapshot().get(WORK_TIME_WITH_CPU).totalCalls(), equalTo(1L));
        }
        assertFalse(TestCollectors.measuresThreadCpuTime(ProfileCollector.create(ProfileCollector.Mode.ELAPSED)));

        final long[] cpuNow = { 0 };
        final ProfileCollector cpu = TestCollectors.withClocks(() -> 0, () -> cpuNow[0]);
        assertTrue(TestCollectors.measuresThreadCpuTime(cpu));
        cpu.timer(WORK_TIME_WITH_CPU);
        assertThat(TestCollectors.toKeyedValues(cpu.snapshot()), hasKey("columnar.test.work_cpu_time_nanos"));
        final ProfileCollector elapsed = TestCollectors.withClock(() -> 0);
        elapsed.timer(WORK_TIME_WITH_CPU);
        assertThat(TestCollectors.toKeyedValues(elapsed.snapshot()), not(hasKey("columnar.test.work_cpu_time_nanos")));
        assertThat(TestCollectors.toKeyedValues(elapsed.snapshot()), hasKey("columnar.test.work_time_nanos"));
    }

    public void testClocksThatContradictTheModeAreRefused() {
        expectThrows(IllegalArgumentException.class, () -> ProfileCollector.create(ProfileCollector.Mode.COUNTERS, () -> 0, null));
        expectThrows(IllegalArgumentException.class, () -> ProfileCollector.create(ProfileCollector.Mode.COUNTERS, null, () -> 0));
        expectThrows(NullPointerException.class, () -> ProfileCollector.create(ProfileCollector.Mode.ELAPSED, null, null));
        expectThrows(IllegalArgumentException.class, () -> ProfileCollector.create(ProfileCollector.Mode.ELAPSED, () -> 0, () -> 0));
        expectThrows(NullPointerException.class, () -> ProfileCollector.create(ProfileCollector.Mode.ELAPSED_AND_CPU, null, () -> 0));
    }

    public void testCollectorsShareNoState() {
        final ProfileCollector first = TestCollectors.withClock(() -> 0);
        final ProfileCollector second = TestCollectors.withClock(() -> 0);
        first.counter(EVENTS).add(5);
        assertTrue(second.isEmpty());
        assertThat(TestCollectors.toKeyedValues(first.snapshot()), equalTo(Map.of("columnar.test.events", 5L)));
    }

    public void testAnUnsupportedCpuClockFallsBackToElapsedOnlyProfiling() {
        final ProfileCollector stats = TestCollectors.cpuUnsupported(() -> 0);
        assertFalse("the fallback is discoverable", TestCollectors.measuresThreadCpuTime(stats));
        final Timer timer = stats.timer(WORK_TIME_WITH_CPU);
        assertNotNull("elapsed profiling continues", timer);
        timer.start();
        timer.stop();
        assertThat(TestCollectors.toKeyedValues(stats.snapshot()), hasKey("columnar.test.work_time_nanos"));
        assertThat(
            "the CPU metric is absent, never zero",
            TestCollectors.toKeyedValues(stats.snapshot()),
            not(hasKey("columnar.test.work_cpu_time_nanos"))
        );
    }

    public void testCountersAccumulateAndAreReusedByMetric() {
        final ProfileCollector stats = TestCollectors.elapsed();
        final Counter first = stats.counter(EVENTS);
        first.add(3);
        stats.counter(EVENTS).add(4);
        assertThat(stats.counter(EVENTS), sameInstance(first));
        assertThat(first.value(), equalTo(7L));
        assertThat(TestCollectors.toKeyedValues(stats.snapshot()), equalTo(Map.of("columnar.test.events", 7L)));
    }

    public void testARegisteredMetricIsReportedWhileItsCountIsZero() {
        final ProfileCollector stats = TestCollectors.elapsed();
        assertTrue(stats.isEmpty());
        stats.counter(EVENTS);
        stats.counter(PAYLOAD_BYTES);
        assertFalse(stats.isEmpty());
        assertThat(
            TestCollectors.toKeyedValues(stats.snapshot()),
            equalTo(Map.of("columnar.test.events", 0L, "columnar.test.payload_bytes", 0L))
        );
    }

    public void testSnapshotIsACopyOrderedByName() {
        final ProfileCollector stats = TestCollectors.elapsed();
        stats.counter(PAYLOAD_BYTES).add(10);
        final Counter events = stats.counter(EVENTS);
        events.add(1);
        final Map<String, Long> before = TestCollectors.toKeyedValues(stats.snapshot());
        events.add(5);
        assertThat(before.get("columnar.test.events"), equalTo(1L));
        assertThat(new ArrayList<>(before.keySet()), equalTo(List.of("columnar.test.events", "columnar.test.payload_bytes")));
        expectThrows(UnsupportedOperationException.class, () -> before.put("other", 1L));
    }

    public void testAMetricCannotBeRegisteredUnderADifferentDefinition() {
        final ProfileCollector stats = TestCollectors.elapsed();
        stats.counter(EVENTS);
        final CounterMetric clash = CounterMetric.of("columnar", "test", "events", Metric.Unit.COUNT, "something else entirely");
        expectThrows(IllegalStateException.class, () -> stats.counter(clash));
    }

    public void testACountSaturatesInsteadOfWrappingToANegativeValue() {
        final ProfileCollector stats = TestCollectors.elapsed();
        final Counter counter = stats.counter(EVENTS);
        counter.add(Long.MAX_VALUE - 1);
        counter.add(5);
        assertThat(counter.value(), equalTo(Long.MAX_VALUE));
        counter.add(1);
        assertThat(TestCollectors.toKeyedValues(stats.snapshot()).get("columnar.test.events"), equalTo(Long.MAX_VALUE));
    }

    public void testACountCanOnlyGrow() {
        final Counter counter = TestCollectors.elapsed().counter(EVENTS);
        counter.add(0);
        assertThat(counter.value(), equalTo(0L));
        expectThrows(AssertionError.class, () -> counter.add(-1));
        assertThat(counter.value(), equalTo(0L));
    }

    public void testRegisteringAgainNeverEvictsAMetricOrCountsItTwice() {
        final ProfileCollector stats = TestCollectors.elapsed();
        final List<CounterMetric> metrics = new ArrayList<>();
        for (int i = 0; i < ProfileCollector.MAX_METRICS; i++) {
            final CounterMetric metric = CounterMetric.of("columnar", "test", "metric_" + i, Metric.Unit.COUNT, "metric " + i);
            metrics.add(metric);
            stats.counter(metric).add(i + 1);
        }
        for (CounterMetric metric : metrics) {
            stats.counter(metric);
        }
        expectThrows(
            IllegalStateException.class,
            () -> stats.counter(CounterMetric.of("columnar", "test", "one_more", Metric.Unit.COUNT, "extra"))
        );
        final Map<String, Long> snapshot = TestCollectors.toKeyedValues(stats.snapshot());
        assertThat(snapshot.size(), equalTo(ProfileCollector.MAX_METRICS));
        for (int i = 0; i < metrics.size(); i++) {
            assertThat(snapshot.get("columnar.test.metric_" + i), equalTo((long) i + 1));
        }
    }

    public void testAMetricOfAnotherFeatureNeedsNoChangeToTheCollector() {
        final CounterMetric foreign = CounterMetric.of(
            "columnar",
            "test",
            "foreign_feature_events",
            Metric.Unit.COUNT,
            "events of a feature"
        );
        final ProfileCollector stats = TestCollectors.elapsed();
        stats.counter(EVENTS).add(1);
        stats.counter(foreign).add(2);
        assertThat(
            TestCollectors.toKeyedValues(stats.snapshot()),
            equalTo(Map.of("columnar.test.events", 1L, "columnar.test.foreign_feature_events", 2L))
        );
    }

    public void testANullMetricIsRejected() {
        expectThrows(NullPointerException.class, () -> CounterMetric.of("columnar", null, "events", Metric.Unit.COUNT, "d"));
        expectThrows(NullPointerException.class, () -> CounterMetric.of("columnar", "test", null, Metric.Unit.COUNT, "d"));
        expectThrows(NullPointerException.class, () -> CounterMetric.of("columnar", "test", "events", null, "d"));
        expectThrows(NullPointerException.class, () -> CounterMetric.of("columnar", "test", "events", Metric.Unit.COUNT, null));
        expectThrows(NullPointerException.class, () -> TestCollectors.elapsed().counter(null));
    }

    public void testASameDefinitionIsRegisteredIdempotentlyAndADifferentOneIsRefused() {
        final ProfileCollector stats = TestCollectors.elapsed();
        stats.counter(PAYLOAD_BYTES).add(5);
        stats.counter(CounterMetric.of("columnar", "test", "payload_bytes", Metric.Unit.BYTES, "bytes moved")).add(5);
        assertThat(TestCollectors.toKeyedValues(stats.snapshot()).get("columnar.test.payload_bytes"), equalTo(10L));
        expectThrows(
            IllegalStateException.class,
            () -> stats.counter(
                CounterMetric.of("columnar", "test", "payload_bytes", Metric.Unit.BYTES, "bytes moved, described differently")
            )
        );
    }

    public void testNoScopeLeaksOntoAReusedExecutorThread() throws Exception {
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            executor.submit(() -> {
                try (Binding outer = Scopes.bind(Scopes.eager(TestCollectors.elapsed()))) {
                    try (Binding inner = Scopes.bind(Scopes.eager(TestCollectors.elapsed()))) {
                        assertTrue(Scopes.isBound());
                    }
                    assertTrue(Scopes.isBound());
                }
            }).get();
            assertFalse(executor.submit(Scopes::isBound).get());
            assertNull(executor.submit(() -> Scopes.current("codec", "_0", "f", Map.of())).get());
        } finally {
            terminate(executor);
        }
    }

    public void testACollectorAcceptsAtMostTheLimitOfMetrics() {
        final ProfileCollector stats = TestCollectors.elapsed();
        for (int i = 0; i < ProfileCollector.MAX_METRICS; i++) {
            stats.counter(CounterMetric.of("columnar", "test", "metric_" + i, Metric.Unit.COUNT, "metric " + i));
        }
        expectThrows(
            IllegalStateException.class,
            () -> stats.counter(CounterMetric.of("columnar", "test", "one_too_many", Metric.Unit.COUNT, "past the limit"))
        );
        stats.counter(CounterMetric.of("columnar", "test", "metric_0", Metric.Unit.COUNT, "metric 0")).add(1);
        assertThat(TestCollectors.toKeyedValues(stats.snapshot()).size(), equalTo(ProfileCollector.MAX_METRICS));
    }

    public void testNothingIsCurrentOutsideABinding() {
        assertThat(Scopes.current("codec", "_0", "f", Map.of()), nullValue());
        assertFalse(Scopes.isBound());
    }

    public void testAReaderOpenedInAScopeGetsThatScopesCollector() {
        final ProfileCollector stats = TestCollectors.elapsed();
        try (Binding ignored = Scopes.bind(Scopes.eager(stats))) {
            assertThat(Scopes.current("codec", "_0", "f", Map.of()), sameInstance(stats));
        }
        assertThat(Scopes.current("codec", "_0", "f", Map.of()), nullValue());
    }

    public void testNestedScopesRestoreTheOuterOneAndNeverLeakInward() {
        final ProfileCollector outer = TestCollectors.elapsed();
        final ProfileCollector inner = TestCollectors.elapsed();
        try (Binding ignoredOuter = Scopes.bind(Scopes.eager(outer))) {
            try (Binding ignoredInner = Scopes.bind(Scopes.eager(inner))) {
                assertThat(Scopes.current("codec", "_0", "f", Map.of()), sameInstance(inner));
            }
            assertThat(Scopes.current("codec", "_0", "f", Map.of()), sameInstance(outer));
        }
        assertThat(Scopes.current("codec", "_0", "f", Map.of()), nullValue());
    }

    public void testTheScopeIsRestoredWhenTheBoundWorkThrows() {
        final ProfileCollector outer = TestCollectors.elapsed();
        try (Binding ignoredOuter = Scopes.bind(Scopes.eager(outer))) {
            expectThrows(IllegalStateException.class, () -> {
                try (Binding ignoredInner = Scopes.bind(Scopes.eager(TestCollectors.elapsed()))) {
                    throw new IllegalStateException("boom");
                }
            });
            assertThat(Scopes.current("codec", "_0", "f", Map.of()), sameInstance(outer));
        }
    }

    public void testClosingABindingTwiceRestoresOnlyOnce() {
        final ProfileCollector outer = TestCollectors.elapsed();
        final ProfileCollector inner = TestCollectors.elapsed();
        try (Binding ignoredOuter = Scopes.bind(Scopes.eager(outer))) {
            final Binding binding = Scopes.bind(Scopes.eager(inner));
            binding.close();
            assertThat(Scopes.current("codec", "_0", "f", Map.of()), sameInstance(outer));
            try (Binding ignoredAgain = Scopes.bind(Scopes.eager(inner))) {
                binding.close();
                assertThat(Scopes.current("codec", "_0", "f", Map.of()), sameInstance(inner));
            }
        }
    }

    public void testABindingIsClosedInnermostFirst() {
        final Binding outer = Scopes.bind(Scopes.eager(TestCollectors.elapsed()));
        final Binding inner = Scopes.bind(Scopes.eager(TestCollectors.elapsed()));
        final AssertionError e = expectThrows(AssertionError.class, outer::close);
        assertThat(e.getMessage(), equalTo("bindings are closed innermost first"));
        inner.close();
        outer.close();
        assertFalse(Scopes.isBound());
    }

    public void testABindingIsClosedOnTheThreadThatBoundIt() throws Exception {
        final Binding binding = Scopes.bind(Scopes.eager(TestCollectors.elapsed()));
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            final Throwable failure = executor.submit(() -> {
                try {
                    binding.close();
                    return null;
                } catch (AssertionError e) {
                    return e;
                }
            }).get();
            assertThat(failure.getMessage(), equalTo("a binding is closed on the thread that bound it"));
        } finally {
            terminate(executor);
        }
        assertTrue("the owner's binding is untouched", Scopes.isBound());
        binding.close();
    }

    public void testTheFactsOfAReaderReachItsScope() {
        final AtomicReference<Map<String, Boolean>> seen = new AtomicReference<>();
        final ProfileCollector collector = TestCollectors.elapsed();
        try (Binding ignored = Scopes.bind((codec, segment, field, facts) -> {
            seen.set(facts);
            return collector;
        })) {
            assertThat(Scopes.current("codec", "_0", "f", Map.of("skip_index", true)), sameInstance(collector));
        }
        assertThat(seen.get(), equalTo(Map.of("skip_index", true)));
    }

    public void testALazyScopeCreatesItsCollectorOnlyWhenAReaderIsOpened() {
        final AtomicInteger created = new AtomicInteger();
        final AtomicReference<ProfileCollector> collector = new AtomicReference<>();
        final Scope lazy = (codec, segment, field, facts) -> collector.updateAndGet(existing -> {
            if (existing != null) {
                return existing;
            }
            created.incrementAndGet();
            return TestCollectors.elapsed();
        });
        try (Binding ignored = Scopes.bind(lazy)) {
            assertThat("binding alone allocates nothing", created.get(), equalTo(0));
        }
        try (Binding ignored = Scopes.bind(lazy)) {
            final MetricRecorder first = Scopes.current("codec", "_0", "f", Map.of());
            final MetricRecorder second = Scopes.current("codec", "_0", "f", Map.of());
            assertThat(first, sameInstance(second));
        }
        assertThat(created.get(), equalTo(1));
    }

    public void testAScopeBoundOnOneThreadIsInvisibleToAnother() throws Exception {
        final ProfileCollector stats = TestCollectors.elapsed();
        final AtomicReference<MetricRecorder> seenElsewhere = new AtomicReference<>(stats);
        try (Binding ignored = Scopes.bind(Scopes.eager(stats))) {
            final Thread other = new Thread(() -> seenElsewhere.set(Scopes.current("codec", "_0", "f", Map.of())));
            other.start();
            other.join();
        }
        assertThat(seenElsewhere.get(), nullValue());
    }

    public void testAMetricNameMustBeLowerCaseWordsJoinedByUnderscores() {
        for (String bad : List.of("", "Events", "1events", "events-total", "events total", "_events")) {
            expectThrows(IllegalArgumentException.class, () -> CounterMetric.of("columnar", "test", bad, Metric.Unit.COUNT, "described"));
        }
        CounterMetric.of("columnar", "test", "a", Metric.Unit.COUNT, "single letter");
        CounterMetric.of("columnar", "test", "chunks_decompressed_2", Metric.Unit.COUNT, "digits after the first character");
    }

    public void testTheUnitHasToShowInTheName() {
        expectThrows(
            IllegalArgumentException.class,
            () -> CounterMetric.of("columnar", "test", "chunk_size", Metric.Unit.BYTES, "described")
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> CounterMetric.of("columnar", "test", "skipped_span", Metric.Unit.DOC_IDS, "described")
        );
        CounterMetric.of("columnar", "chunks", "bytes_inflated", Metric.Unit.BYTES, "described");
        CounterMetric.of("columnar", "test", "skip_docid_span", Metric.Unit.DOC_IDS, "described");
    }

    public void testAMetricNeedsADescription() {
        expectThrows(IllegalArgumentException.class, () -> CounterMetric.of("columnar", "test", "events", Metric.Unit.COUNT, "  "));
    }
}
