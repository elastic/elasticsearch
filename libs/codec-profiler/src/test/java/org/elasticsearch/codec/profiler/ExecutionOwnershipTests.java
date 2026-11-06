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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.LongSupplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

// NOTE: These tests use a synthetic codec, not ColumNAR. A producer and its block cache are shared by every execution,
// as a segment's are. Only the caller-owned views and the helpers they create keep an execution's handles.
public class ExecutionOwnershipTests extends ESTestCase {

    private static final CounterMetric LOOKUPS = CounterMetric.of("test_codec", "blocks", "lookups", Metric.Unit.COUNT, "lookups");
    private static final CounterMetric HITS = CounterMetric.of("test_codec", "blocks", "cache_hits", Metric.Unit.COUNT, "hits");
    private static final CounterMetric TERMS = CounterMetric.of("test_codec", "terms", "visited", Metric.Unit.COUNT, "terms");
    private static final TimerMetric LOOKUP_TIME = TimerMetric.of(
        "test_codec",
        "blocks",
        "lookup_time_nanos",
        TimingPolicy.EXACT,
        "lookup time"
    );

    private static final class SharedCache {
        private final Map<Integer, byte[]> blocks = new ConcurrentHashMap<>();

        boolean lookup(int block) {
            return blocks.putIfAbsent(block, new byte[] { (byte) block }) != null;
        }
    }

    private static final class Producer {
        private final SharedCache cache = new SharedCache();

        View open() {
            return new View(cache, Scopes.current("test_codec", "_0", "f", Map.of()));
        }
    }

    private static final class View {
        private final SharedCache cache;
        @Nullable
        private final MetricRecorder recorder;
        @Nullable
        private final Counter lookups;
        @Nullable
        private final Counter hits;
        private final Timer lookupTime;
        private Terms terms;

        View(SharedCache cache, @Nullable MetricRecorder recorder) {
            this.cache = cache;
            this.recorder = recorder;
            this.lookups = recorder == null ? null : recorder.counter(LOOKUPS);
            this.hits = recorder == null ? null : recorder.counter(HITS);
            this.lookupTime = recorder == null ? Timer.INERT : recorder.timer(LOOKUP_TIME);
        }

        void read(int block) {
            lookupTime.start();
            try {
                final boolean hit = cache.lookup(block);
                if (lookups != null) {
                    lookups.add(1);
                    if (hit) {
                        hits.add(1);
                    }
                }
            } finally {
                lookupTime.stop();
            }
        }

        Terms terms() {
            if (terms == null) {
                terms = new Terms(recorder);
            }
            return terms;
        }
    }

    private static final class Terms {
        @Nullable
        private final Counter visited;

        Terms(@Nullable MetricRecorder recorder) {
            this.visited = recorder == null ? null : recorder.counter(TERMS);
        }

        void next() {
            if (visited != null) {
                visited.add(1);
            }
        }
    }

    private static final class TickingClock implements LongSupplier {
        private long now;

        @Override
        public long getAsLong() {
            return now++;
        }
    }

    public void testAViewReportsToItsOwnerAfterTheBindingClosesAndWhileAnotherScopeIsBound() {
        final Producer producer = new Producer();
        final ProfileCollector a = TestCollectors.countersOnly();
        final ProfileCollector b = TestCollectors.countersOnly();

        final View unprofiledBefore = producer.open();
        final View viewA;
        try (Binding ignored = TestCollectors.bind(a)) {
            viewA = producer.open();
        }
        final View unprofiledBetween = producer.open();
        final View viewB;
        try (Binding ignored = TestCollectors.bind(b)) {
            viewB = producer.open();
        }

        viewA.read(1);
        viewA.read(2);
        unprofiledBefore.read(1);
        try (Binding ignored = TestCollectors.bind(b)) {
            viewA.read(3);
        }
        final Snapshot aBeforeB = a.snapshot();
        for (int block = 1; block <= 5; block++) {
            viewB.read(block);
        }
        unprofiledBetween.read(9);

        assertThat(TestCollectors.toKeyedValues(a.snapshot()).get("test_codec.blocks.lookups"), equalTo(3L));
        assertThat(TestCollectors.toKeyedValues(b.snapshot()).get("test_codec.blocks.lookups"), equalTo(5L));
        assertThat("an older snapshot does not change", a.snapshot(), equalTo(aBeforeB));
        assertFalse(TestCollectors.isBound());
    }

    public void testASharedCacheWarmedWithoutProfilingKeepsNoRecorderAndHidesNoLaterHit() {
        final Producer producer = new Producer();
        final int blocks = between(1, 20);
        final View warmer = producer.open();
        for (int block = 0; block < blocks; block++) {
            warmer.read(block);
        }
        final ProfileCollector a = TestCollectors.countersOnly();
        final ProfileCollector b = TestCollectors.countersOnly();
        final View viewA;
        try (Binding ignored = TestCollectors.bind(a)) {
            viewA = producer.open();
        }
        final View viewB;
        try (Binding ignored = TestCollectors.bind(b)) {
            viewB = producer.open();
        }
        for (int block = 0; block < blocks; block++) {
            viewA.read(block);
        }
        viewB.read(0);

        assertThat(TestCollectors.toKeyedValues(a.snapshot()).get("test_codec.blocks.cache_hits"), equalTo((long) blocks));
        assertThat(TestCollectors.toKeyedValues(b.snapshot()).get("test_codec.blocks.cache_hits"), equalTo(1L));
        assertThat(TestCollectors.toKeyedValues(b.snapshot()).get("test_codec.blocks.lookups"), equalTo(1L));
    }

    public void testAHelperCreatedLaterInheritsItsViewsRecorderWhateverScopeIsBound() {
        final Producer producer = new Producer();
        final ProfileCollector owner = TestCollectors.countersOnly();
        final ProfileCollector other = TestCollectors.countersOnly();
        final View view;
        try (Binding ignored = TestCollectors.bind(owner)) {
            view = producer.open();
        }
        final View unprofiled = producer.open();
        final int visits = between(1, 10);
        try (Binding ignored = TestCollectors.bind(other)) {
            for (int term = 0; term < visits; term++) {
                view.terms().next();
                unprofiled.terms().next();
            }
        }

        assertThat(TestCollectors.toKeyedValues(owner.snapshot()).get("test_codec.terms.visited"), equalTo((long) visits));
        assertThat("the other scope saw no view open", other.snapshot().isEmpty(), equalTo(true));
    }

    public void testExecutionsReadingTheSharedCacheConcurrentlyKeepExactCountsAndTimes() throws Exception {
        final Producer producer = new Producer();
        final int blocks = between(1, 50);
        final View warmer = producer.open();
        for (int block = 0; block < blocks; block++) {
            warmer.read(block);
        }
        final int executions = between(2, 4);
        final int reads = between(1, 500);
        final List<ProfileCollector> collectors = new ArrayList<>();
        for (int execution = 0; execution < executions; execution++) {
            collectors.add(TestCollectors.withClock(new TickingClock()));
        }
        final CyclicBarrier opened = new CyclicBarrier(executions + 1);
        final Thread[] threads = new Thread[executions + 1];
        for (int t = 0; t <= executions; t++) {
            final ProfileCollector collector = t < executions ? collectors.get(t) : null;
            threads[t] = new Thread(() -> {
                final View view;
                if (collector == null) {
                    view = producer.open();
                } else {
                    try (Binding ignored = TestCollectors.bind(collector)) {
                        view = producer.open();
                    }
                }
                try {
                    opened.await();
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
                for (int read = 0; read < reads; read++) {
                    view.read(read % blocks);
                }
            });
        }
        startInParallel(threads);

        final List<Snapshot> parts = new ArrayList<>();
        for (ProfileCollector collector : collectors) {
            final Map<String, Long> values = TestCollectors.toKeyedValues(collector.snapshot());
            assertThat(values.get("test_codec.blocks.lookups"), equalTo((long) reads));
            assertThat("every block was warm", values.get("test_codec.blocks.cache_hits"), equalTo((long) reads));
            assertThat(
                "each call reads its own clock once at start and once at stop",
                values.get("test_codec.blocks.lookup_time_nanos"),
                equalTo((long) reads)
            );
            parts.add(collector.snapshot());
        }
        final Map<String, Long> merged = TestCollectors.toKeyedValues(Snapshot.merge(parts));
        assertThat(merged.get("test_codec.blocks.lookups"), equalTo((long) reads * executions));
        assertThat(merged.get("test_codec.blocks.lookup_time_nanos"), equalTo((long) reads * executions));
    }

    public void testAFailedOpenOnAReusedThreadLeavesTheOuterOwnerAndNoScopeForTheNextTask() throws Exception {
        final Producer producer = new Producer();
        final ProfileCollector outer = TestCollectors.countersOnly();
        final ProfileCollector inner = TestCollectors.countersOnly();
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            executor.submit(() -> {
                try (Binding ignoredOuter = TestCollectors.bind(outer)) {
                    expectThrows(IllegalStateException.class, () -> {
                        try (Binding ignoredInner = TestCollectors.bind(inner)) {
                            producer.open().read(1);
                            throw new IllegalStateException("the read failed");
                        }
                    });
                    producer.open().read(2);
                }
            }).get();
            final View next = executor.submit(producer::open).get();
            next.read(3);
            assertThat("the next task on the thread opens an unprofiled view", next.recorder, nullValue());
        } finally {
            terminate(executor);
        }
        assertThat(TestCollectors.toKeyedValues(inner.snapshot()).get("test_codec.blocks.lookups"), equalTo(1L));
        assertThat(TestCollectors.toKeyedValues(outer.snapshot()).get("test_codec.blocks.lookups"), equalTo(1L));
    }
}
