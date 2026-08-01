/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.derivedmetrics;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.datastreams.derivedmetrics.CompiledDerivedMetrics.CompiledMetric;
import org.elasticsearch.datastreams.derivedmetrics.CompiledDerivedMetrics.Interval;
import org.elasticsearch.datastreams.derivedmetrics.CompiledDerivedMetrics.Reduction;
import org.elasticsearch.datastreams.derivedmetrics.CompiledDerivedMetrics.Source;
import org.elasticsearch.datastreams.derivedmetrics.CompiledDerivedMetrics.Trigger;
import org.elasticsearch.datastreams.derivedmetrics.DerivedMetricsBuffer.Drained;
import org.elasticsearch.datastreams.derivedmetrics.DerivedMetricsBuffer.TableKey;
import org.elasticsearch.datastreams.derivedmetrics.DerivedMetricsDimensionCodec.Scratch;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class DerivedMetricsBufferTests extends ESTestCase {

    private static final Interval TEN_SECONDS = new Interval("10s", 10_000L);

    private BigArrays bigArrays;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // MockBigArrays fails the test if anything we allocate is not released, which is exactly the leak we care about
        bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
    }

    public void testBucketsAreAlignedToTheEpoch() {
        assertEquals(0L, DerivedMetricsBuffer.bucketStart(9_999, 10_000));
        assertEquals(10_000L, DerivedMetricsBuffer.bucketStart(10_000, 10_000));
        assertEquals(10_000L, DerivedMetricsBuffer.bucketStart(19_999, 10_000));
        assertEquals(60_000L, DerivedMetricsBuffer.bucketStart(119_999, 60_000));
    }

    public void testAccumulationReducesEveryWay() {
        try (DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(bigArrays, 10)) {
            TableKey key = key(Reduction.SUM, 0L);
            for (double value : new double[] { 4.0, 1.0, 7.0 }) {
                assertTrue(record(buffer, key, "checkout", value));
            }
            var drained = buffer.drainAll();
            try {
                DerivedMetricsSeriesTable table = drained.get(0).table();
                assertEquals(1L, table.size());
                assertEquals(3L, table.countOf(0));
                assertEquals(12.0, table.reduce(0, Reduction.SUM, 10_000L), 0.0);
                assertEquals(1.0, table.reduce(0, Reduction.MIN, 10_000L), 0.0);
                assertEquals(7.0, table.reduce(0, Reduction.MAX, 10_000L), 0.0);
                // 12 observations worth of value spread over a ten second interval
                assertEquals(1.2, table.reduce(0, Reduction.RATE, 10_000L), 0.0);
                // an avg gauge emits its sum, and the count travels alongside it
                assertEquals(12.0, table.reduce(0, Reduction.AVG, 10_000L), 0.0);
            } finally {
                drained.forEach(d -> d.table().close());
            }
        }
    }

    public void testDistinctDimensionValuesAreDistinctSeries() {
        try (DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(bigArrays, 10)) {
            TableKey key = key(Reduction.SUM, 0L);
            assertTrue(record(buffer, key, "checkout", 1.0));
            assertTrue(record(buffer, key, "search", 2.0));
            assertTrue(record(buffer, key, "checkout", 3.0));
            assertEquals(2, buffer.size());

            var drained = buffer.drainAll();
            try {
                DerivedMetricsSeriesTable table = drained.get(0).table();
                assertEquals(2L, table.size());
                BytesRef spare = new BytesRef();
                assertEquals("checkout", table.dimensionsOf(0, 1, spare)[0]);
                assertEquals(4.0, table.reduce(0, Reduction.SUM, 10_000L), 0.0);
                assertEquals("search", table.dimensionsOf(1, 1, spare)[0]);
                assertEquals(2.0, table.reduce(1, Reduction.SUM, 10_000L), 0.0);
            } finally {
                drained.forEach(d -> d.table().close());
            }
        }
    }

    public void testDrainClosedOnlyReturnsBucketsPastTheGracePeriod() {
        try (DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(bigArrays, 10)) {
            record(buffer, key(Reduction.SUM, 0L), "checkout", 1.0);
            record(buffer, key(Reduction.SUM, 10_000L), "checkout", 2.0);

            // the first bucket ends at 10s, so with a 5s grace period it only closes at 15s
            assertTrue(buffer.drainClosed(14_999, 5_000).isEmpty());

            var closed = buffer.drainClosed(15_000, 5_000);
            try {
                assertEquals(1, closed.size());
                assertEquals(0L, closed.get(0).key().bucketStartMillis());
            } finally {
                closed.forEach(d -> d.table().close());
            }
            assertEquals(1, buffer.size());
        }
    }

    public void testNewSeriesAreDroppedOnceTheCapIsReached() {
        try (DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(bigArrays, 2)) {
            TableKey key = key(Reduction.SUM, 0L);
            assertTrue(record(buffer, key, "a", 1.0));
            assertTrue(record(buffer, key, "b", 1.0));
            assertFalse(record(buffer, key, "c", 1.0));
            assertEquals(2, buffer.size());
            assertEquals(1L, buffer.droppedSeries());

            // series that are already tracked keep accumulating even once the cap is reached
            assertTrue(record(buffer, key, "a", 1.0));
        }
    }

    /**
     * Without a per-stream ceiling the node budget is first-come-first-served, so a stream that churns through dimension values can
     * consume all of it and silently starve every other stream on the node.
     */
    public void testOneStreamCannotConsumeAnotherStreamsBudget() {
        try (DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(bigArrays, 100, 2)) {
            TableKey noisy = key("logs-noisy-default", Reduction.SUM, 0L);
            TableKey quiet = key("logs-quiet-default", Reduction.SUM, 0L);

            assertTrue(record(buffer, noisy, "a", 1.0));
            assertTrue(record(buffer, noisy, "b", 1.0));
            // the noisy stream has spent its share
            assertFalse(record(buffer, noisy, "c", 1.0));

            // a quiet stream is unaffected, even though the node as a whole has plenty of room left
            assertTrue(record(buffer, quiet, "a", 1.0));
            assertTrue(record(buffer, quiet, "b", 1.0));
            assertEquals(2, buffer.seriesFor("logs-noisy-default"));
            assertEquals(2, buffer.seriesFor("logs-quiet-default"));
        }
    }

    public void testDrainingReturnsBudgetToTheStream() {
        try (DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(bigArrays, 100, 2)) {
            TableKey noisy = key("logs-noisy-default", Reduction.SUM, 0L);
            assertTrue(record(buffer, noisy, "a", 1.0));
            assertTrue(record(buffer, noisy, "b", 1.0));
            assertFalse(record(buffer, noisy, "c", 1.0));

            buffer.drainAll().forEach(d -> d.table().close());
            assertEquals(0, buffer.seriesFor("logs-noisy-default"));
            assertEquals(0, buffer.size());
            assertTrue(record(buffer, noisy, "c", 1.0));
        }
    }

    /**
     * A bucket flushed early is emitted more than once. A time series _id is derived from the tsid and the timestamp, so the partials
     * have to be numbered or the second would collide with the first and be rejected.
     */
    public void testEachEarlyFlushOfABucketIsANewPartial() {
        try (DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(bigArrays, 10)) {
            TableKey key = key(Reduction.SUM, 0L);

            assertTrue(record(buffer, key, "checkout", 1.0));
            assertEquals(0, drainForPressure(buffer, key).partial());

            assertTrue(record(buffer, key, "checkout", 2.0));
            assertEquals(1, drainForPressure(buffer, key).partial());

            // the bucket then closes normally, and its final emission continues the numbering
            assertTrue(record(buffer, key, "checkout", 3.0));
            var closed = buffer.drainClosed(20_000, 0);
            try {
                assertEquals(2, closed.get(0).partial());
                // each partial carries only what was collected since the previous one; consumers sum them back together
                assertEquals(3.0, closed.get(0).table().reduce(0, Reduction.SUM, 10_000L), 0.0);
            } finally {
                closed.forEach(d -> d.table().close());
            }
        }
    }

    /**
     * The partial counter lives only in heap, so a node that restarts inside a bucket it had already emitted for would start again at
     * offset zero — same tsid, same timestamp, same _id, silently rejected by op_type=create. Seeding the counter per service instance is
     * what keeps the post-restart partial distinguishable from the pre-crash one.
     */
    public void testPartialOffsetsFromDifferentInstancesDoNotCollide() {
        TableKey key = key(Reduction.SUM, 0L);
        int before;
        int after;
        try (DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(bigArrays, 10, 10, 8, 17)) {
            assertTrue(record(buffer, key, "checkout", 1.0));
            before = drainForPressure(buffer, key).partial();
        }
        // a second instance stands for the node coming back up inside the same bucket
        try (DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(bigArrays, 10, 10, 8, 42)) {
            assertTrue(record(buffer, key, "checkout", 1.0));
            after = drainForPressure(buffer, key).partial();
        }
        assertEquals(17, before);
        assertEquals(42, after);
    }

    /**
     * A partial is stamped at bucketStart plus its number, so the number has to stay inside the interval. Past that the document would
     * land in the following bucket, which is worse than shedding it.
     */
    public void testPartialsStopBeingIssuedAtTheEndOfTheInterval() {
        try (DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(bigArrays, 10, 10, 8, (int) TEN_SECONDS.millis() - 2)) {
            TableKey key = key(Reduction.SUM, 0L);
            assertTrue(record(buffer, key, "checkout", 1.0));

            // the seed leaves exactly one offset, so the first early flush is issued and the next is refused
            assertNotNull(drainForPressure(buffer, key));
            assertTrue(record(buffer, key, "checkout", 1.0));
            assertNull(drainForPressure(buffer, key));
        }
    }

    /**
     * A bucket flushed early and then never written to again would otherwise leave its partial counter behind forever, since nothing
     * drains a table that no longer exists.
     */
    public void testPartialCountersAreSweptOnceTheBucketCloses() {
        try (DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(bigArrays, 10)) {
            TableKey key = key(Reduction.SUM, 0L);
            assertTrue(record(buffer, key, "checkout", 1.0));
            drainForPressure(buffer, key);
            assertEquals(1, buffer.partialsTracked());

            buffer.drainClosed(20_000, 0);
            assertEquals(0, buffer.partialsTracked());
        }
    }

    /**
     * Draining a bucket that is still open leaves the writers free to carry on, which is the whole point of flushing early. A writer that
     * held the drained table must not record into it, since nothing will ever emit it again.
     */
    public void testRecordingAfterAnEarlyFlushStartsAFreshBucket() {
        try (DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(bigArrays, 10)) {
            TableKey key = key(Reduction.SUM, 0L);
            assertTrue(record(buffer, key, "checkout", 1.0));
            drainForPressure(buffer, key);
            assertEquals(0, buffer.size());

            assertTrue(record(buffer, key, "checkout", 5.0));
            var drained = buffer.drainAll();
            try {
                assertEquals(1L, drained.get(0).table().size());
                assertEquals(5.0, drained.get(0).table().reduce(0, Reduction.SUM, 10_000L), 0.0);
            } finally {
                drained.forEach(d -> d.table().close());
            }
        }
    }

    /**
     * Relieving pressure runs on the indexing thread, inside the shard's operation permit, so it has to touch exactly the bucket that
     * refused the observation and nothing else. Draining every bucket the node holds would be unbounded work in the one place that cannot
     * afford it.
     */
    public void testAnEarlyFlushTakesOnlyTheBucketThatRefused() {
        try (DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(bigArrays, 100)) {
            TableKey refused = key("logs-refused-default", Reduction.SUM, 0L);
            TableKey untouched = key("logs-untouched-default", Reduction.SUM, 0L);
            assertTrue(record(buffer, refused, "a", 1.0));
            assertTrue(record(buffer, untouched, "a", 1.0));

            drainForPressure(buffer, refused);

            assertEquals(0, buffer.seriesFor("logs-refused-default"));
            assertEquals(1, buffer.seriesFor("logs-untouched-default"));
        }
    }

    /**
     * Flushing early is what turns a full buffer from lost observations into extra documents: the series that had been refused fits once
     * the bucket has been emitted.
     */
    public void testAnEarlyFlushMakesRoomForARefusedSeries() {
        try (DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(bigArrays, 2)) {
            TableKey key = key(Reduction.SUM, 0L);
            assertTrue(record(buffer, key, "a", 1.0));
            assertTrue(record(buffer, key, "b", 1.0));
            assertFalse(record(buffer, key, "c", 1.0));

            drainForPressure(buffer, key);
            assertTrue(record(buffer, key, "c", 1.0));
        }
    }

    /**
     * A histogram series keeps the whole distribution rather than a handful of primitives, and it is charged against the same breaker as
     * everything else. Closing the table has to give all of it back, which is the part that is easy to get wrong: the accumulators are
     * objects held in an array rather than array memory the buffer released on its own.
     */
    public void testHistogramSeriesAccumulateAndReleaseTheirMemory() {
        CircuitBreakerService breakerService = LimitedBreaker.service(DerivedMetricsService.BREAKER_NAME, ByteSizeValue.ofMb(64));
        CircuitBreaker breaker = breakerService.getBreaker(DerivedMetricsService.BREAKER_NAME);
        BigArrays accounted = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), breakerService).withCircuitBreaking();

        try (DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(accounted, 10)) {
            TableKey key = key(Reduction.HISTOGRAM, 0L);
            for (int value = 1; value <= 100; value++) {
                assertTrue(record(buffer, key, "checkout", value));
            }
            assertThat("a histogram series should have taken real memory", breaker.getUsed(), greaterThan(0L));

            var drained = buffer.drainAll();
            try {
                try (var histogram = drained.get(0).table().histogramOf(0)) {
                    assertEquals(100L, histogram.valueCount());
                    assertEquals(5050.0, histogram.sum(), 1e-6);
                    assertEquals(1.0, histogram.min(), 1e-6);
                    assertEquals(100.0, histogram.max(), 1e-6);
                }
            } finally {
                drained.forEach(d -> d.table().close());
            }
        }
        assertEquals("closing the buffer must give every byte back", 0L, breaker.getUsed());
    }

    /**
     * Pins what a series of each kind costs, because these numbers are what the circuit breaker's budget is planned against and a
     * regression in them would otherwise only show up as a node running out of room sooner than expected.
     *
     * <p>The bounds are deliberately loose — the exact figure depends on how the underlying arrays round up — but the order of magnitude
     * is the point: a scalar series is tens of bytes and a histogram series is thousands, which is why the two cannot share a budget
     * assumption.
     */
    public void testWhatASeriesOfEachKindCosts() {
        long scalar = bytesPerSeries(Reduction.SUM, 1000);
        long histogram = bytesPerSeries(Reduction.HISTOGRAM, 50);
        logger.info("bytes per series: scalar [{}], histogram [{}]", scalar, histogram);

        assertThat("a scalar series is a handful of primitives in shared arrays", scalar, lessThan(200L));
        assertThat("a histogram series keeps a whole distribution", histogram, greaterThan(1000L));
        assertThat("but it must still be bounded by its bucket count", histogram, lessThan(20_000L));
    }

    /**
     * Fills a table with the given number of distinct series and returns the accounted bytes each one cost.
     */
    private long bytesPerSeries(Reduction reduction, int series) {
        CircuitBreakerService breakerService = LimitedBreaker.service(DerivedMetricsService.BREAKER_NAME, ByteSizeValue.ofMb(512));
        CircuitBreaker breaker = breakerService.getBreaker(DerivedMetricsService.BREAKER_NAME);
        BigArrays accounted = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), breakerService).withCircuitBreaking();
        try (DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(accounted, series * 2)) {
            TableKey key = key(reduction, 0L);
            for (int i = 0; i < series; i++) {
                assertTrue(record(buffer, key, "service-" + i, i + 1.0));
            }
            return breaker.getUsed() / series;
        }
    }

    /**
     * The property the whole design exists for: hold the budget fixed and raise both the number of distinct series offered and the number
     * of observations by an order of magnitude, and the memory actually taken must not follow. It is bounded by the cap, not by the load.
     */
    public void testMemoryStaysFlatAsSeriesAndWritesGrow() {
        long small = peakBytesFor(50, 100);
        long large = peakBytesFor(500, 1_000);

        assertThat("nothing was accounted at all, so the measurement proves nothing", small, greaterThan(0L));
        // an order of magnitude more series offered and ten times the writes, for the same bounded footprint
        assertThat(large, equalTo(small));
    }

    /**
     * Offers {@code series} distinct dimension values, {@code writesPerSeries} times each, to a buffer capped at ten series, and returns
     * the bytes the circuit breaker had accounted at the end.
     */
    private long peakBytesFor(int series, int writesPerSeries) {
        CircuitBreakerService breakerService = LimitedBreaker.service(DerivedMetricsService.BREAKER_NAME, ByteSizeValue.ofMb(64));
        BigArrays accounted = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), breakerService).withCircuitBreaking();
        try (DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(accounted, 10)) {
            TableKey key = key(Reduction.SUM, 0L);
            for (int write = 0; write < writesPerSeries; write++) {
                for (int i = 0; i < series; i++) {
                    record(buffer, key, "service-" + i, 1.0);
                }
            }
            return breakerService.getBreaker(DerivedMetricsService.BREAKER_NAME).getUsed();
        }
    }

    private static DerivedMetricsBuffer.Drained drainForPressure(DerivedMetricsBuffer buffer, TableKey key) {
        DerivedMetricsBuffer.Drained drained = buffer.drainForPressure(key);
        if (drained != null) {
            drained.table().close();
        }
        return drained;
    }

    /**
     * Every write thread touching one metric on a node serialises through that table's monitor, and until now nothing had ever run two
     * threads through it. This asserts the part that would break silently: that concurrent recording loses no observations and leaves the
     * series budget exactly where it started.
     */
    public void testConcurrentRecordingLosesNothing() throws Exception {
        int threads = 8;
        int perThread = 2000;
        int services = 16;
        try (DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(bigArrays, 1000)) {
            TableKey key = key(Reduction.SUM, 0L);
            // one scratch per thread, as on the write path, since a scratch buffer is explicitly not shareable
            runInParallel(threads, thread -> {
                Scratch scratch = new Scratch();
                for (int i = 0; i < perThread; i++) {
                    buffer.record(key, new String[] { "service-" + (i % services) }, scratch, 1.0);
                }
            });

            var drained = buffer.drainAll();
            try {
                DerivedMetricsSeriesTable table = drained.get(0).table();
                assertEquals("every distinct dimension value should be one series", services, (int) table.size());
                double total = 0;
                for (long ordinal = 0; ordinal < table.size(); ordinal++) {
                    total += table.reduce(ordinal, Reduction.SUM, 10_000L);
                }
                assertEquals("no observation may be lost or double counted", (double) threads * perThread, total, 0.0);
            } finally {
                drained.forEach(d -> d.table().close());
            }
            assertEquals("draining must return the whole budget", 0, buffer.size());
        }
    }

    /**
     * The nastier interleaving: a drain seals a table while writers are still recording into it. A writer that finds its table sealed has
     * to notice and start again on the replacement, or its observation vanishes.
     */
    public void testRecordingRacingADrainLosesNothing() throws Exception {
        int threads = 6;
        int perThread = 1500;
        try (DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(bigArrays, 1000)) {
            TableKey key = key(Reduction.SUM, 0L);
            java.util.concurrent.atomic.AtomicReference<Double> drainedTotal = new java.util.concurrent.atomic.AtomicReference<>(0.0);

            runInParallel(threads + 1, worker -> {
                if (worker == threads) {
                    // the drainer, taking the bucket out from under the writers repeatedly
                    for (int i = 0; i < 40; i++) {
                        for (var entry : buffer.drainAll()) {
                            try {
                                for (long ordinal = 0; ordinal < entry.table().size(); ordinal++) {
                                    double value = entry.table().reduce(ordinal, Reduction.SUM, 10_000L);
                                    drainedTotal.updateAndGet(current -> current + value);
                                }
                            } finally {
                                entry.table().close();
                            }
                        }
                    }
                    return;
                }
                Scratch scratch = new Scratch();
                for (int i = 0; i < perThread; i++) {
                    buffer.record(key, new String[] { "checkout" }, scratch, 1.0);
                }
            });

            // whatever the drainer did not take is still buffered; the two together must account for every observation
            double remaining = 0;
            var drained = buffer.drainAll();
            try {
                for (var entry : drained) {
                    for (long ordinal = 0; ordinal < entry.table().size(); ordinal++) {
                        remaining += entry.table().reduce(ordinal, Reduction.SUM, 10_000L);
                    }
                }
            } finally {
                drained.forEach(d -> d.table().close());
            }
            assertEquals((double) threads * perThread, drainedTotal.get() + remaining, 0.0);
            assertEquals(0, buffer.size());
        }
    }

    /**
     * The accounting invariant the whole budget rests on: whatever the buffer took, it gives back exactly, so a node that has been under
     * breaker pressure still knows how much room it has.
     *
     * <p>This used to fail. The series table interned a dimension tuple before it grew the columns behind it, so a refusal in between
     * left a tuple in the hash that the buffer had never counted. On drain the table reported its hash size, the buffer subtracted that,
     * and the node-wide count drifted below zero — which then let it exceed the very cap the count exists to enforce.
     */
    public void testTheSeriesCountReturnsToZeroEvenWhenTheBreakerRefusedSeriesAlongTheWay() {
        // small enough that growing the columns fails partway through, which is the window the bug lived in
        CircuitBreakerService breakerService = LimitedBreaker.service(DerivedMetricsService.BREAKER_NAME, ByteSizeValue.ofKb(16));
        BigArrays accounted = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), breakerService).withCircuitBreaking();

        try (DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(accounted, 100_000)) {
            TableKey key = key(Reduction.SUM, 0L);
            int accepted = 0;
            for (int i = 0; i < 2_000; i++) {
                if (record(buffer, key, "service-" + i, 1.0)) {
                    accepted++;
                }
            }
            assertThat("the breaker should have refused something, or this test proves nothing", accepted, lessThan(2_000));
            assertThat("a refusal should have retired the table it poisoned", buffer.tablesRetired(), greaterThan(0L));

            var drained = buffer.drainAll();
            try {
                // a retired table is handed to the next flush rather than dropped, so every series the buffer accepted still comes out
                long emitted = drained.stream().mapToLong(d -> d.table().size()).sum();
                assertEquals("every accepted series should still be emitted", accepted, (int) emitted);
            } finally {
                drained.forEach(d -> d.table().close());
            }
            assertEquals("draining everything must return the count to exactly zero", 0, buffer.size());
        }
    }

    /**
     * A series the breaker refused must leave no trace at all. If it were interned without its columns, emission would walk ordinals up
     * to the hash size and read past the end of those columns, turning one refused observation into a flush that throws.
     */
    public void testARefusedSeriesIsNotLeftBehindForEmissionToTripOver() {
        CircuitBreakerService breakerService = LimitedBreaker.service(DerivedMetricsService.BREAKER_NAME, ByteSizeValue.ofKb(16));
        BigArrays accounted = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), breakerService).withCircuitBreaking();

        try (DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(accounted, 100_000)) {
            TableKey key = key(Reduction.SUM, 0L);
            for (int i = 0; i < 2_000; i++) {
                record(buffer, key, "service-" + i, 1.0);
            }
            var drained = buffer.drainAll();
            try {
                for (var entry : drained) {
                    DerivedMetricsSeriesTable table = entry.table();
                    // reading every ordinal the table claims to have is exactly what emission does
                    for (long ordinal = 0; ordinal < table.size(); ordinal++) {
                        assertEquals(1.0, table.reduce(ordinal, Reduction.SUM, TEN_SECONDS.millis()), 0.0);
                    }
                }
            } finally {
                drained.forEach(d -> d.table().close());
            }
        }
    }

    /**
     * Two projects may each have a data stream of the same name. They are different streams, so they must not share a budget — otherwise
     * one tenant's cardinality silently refuses another tenant's series.
     */
    public void testTwoProjectsWithTheSameStreamNameDoNotShareABudget() {
        try (DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(bigArrays, 100, 1)) {
            ProjectId other = ProjectId.fromId("other-project");
            TableKey mine = key(Reduction.SUM, 0L);
            TableKey theirs = key(other, "logs-my_app-default", Reduction.SUM, 0L);

            assertTrue(record(buffer, mine, "checkout", 1.0));
            // the per-stream cap is one, and the default project has now spent it — the other project's identically named stream has not
            assertTrue("a second project must have its own budget", record(buffer, theirs, "checkout", 1.0));

            assertEquals(1, buffer.seriesFor(ProjectId.DEFAULT, "logs-my_app-default"));
            assertEquals(1, buffer.seriesFor(other, "logs-my_app-default"));
        }
    }

    /**
     * Knowing that something was refused is not enough to act on: raising the node budget and finding the one noisy stream are different
     * responses, so the two caps are counted apart.
     */
    public void testTheNodeCapAndTheStreamCapAreCountedSeparately() {
        try (DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(bigArrays, 100, 1)) {
            TableKey key = key(Reduction.SUM, 0L);
            assertTrue(record(buffer, key, "checkout", 1.0));
            assertFalse(record(buffer, key, "search", 1.0));

            assertEquals(1L, buffer.droppedSeriesAtStreamCap());
            assertEquals(0L, buffer.droppedSeriesAtNodeCap());
        }

        try (DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(bigArrays, 1, 100)) {
            TableKey key = key(Reduction.SUM, 0L);
            assertTrue(record(buffer, key, "checkout", 1.0));
            assertFalse(record(buffer, key, "search", 1.0));

            assertEquals(1L, buffer.droppedSeriesAtNodeCap());
            assertEquals(0L, buffer.droppedSeriesAtStreamCap());
        }
    }

    /**
     * Series counts cannot rank one metric against another, because a histogram series is worth roughly thirty scalar ones. Bytes can,
     * which is what makes it possible to give up the table that is actually filling the node rather than whichever one asked last.
     */
    public void testATableReportsTheBytesItHolds() {
        try (DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(bigArrays, 1000)) {
            TableKey scalar = key("logs-scalar-default", Reduction.SUM, 0L);
            TableKey histogram = key("logs-histogram-default", Reduction.HISTOGRAM, 0L);
            for (int i = 0; i < 20; i++) {
                assertTrue(record(buffer, scalar, "service-" + i, i));
                assertTrue(record(buffer, histogram, "service-" + i, i));
            }

            var drained = buffer.drainAll();
            try {
                long scalarBytes = 0;
                long histogramBytes = 0;
                for (var entry : drained) {
                    long bytes = entry.table().bytesHeld();
                    assertThat("every table holds something", bytes, greaterThan(0L));
                    if (entry.key().metric().reduction().isHistogram()) {
                        histogramBytes = bytes;
                    } else {
                        scalarBytes = bytes;
                    }
                }
                // the same number of series either way, so any difference is the distributions themselves
                assertThat(
                    "a histogram table of the same cardinality must report far more than a scalar one",
                    histogramBytes,
                    greaterThan(scalarBytes * 5)
                );
            } finally {
                drained.forEach(d -> d.table().close());
            }
        }
    }

    /**
     * Under pressure the node should give up whatever is actually filling it, not whichever bucket happened to ask last. Flushing a
     * bucket early loses nothing — partials sum at query time — so this is a choice about which metric pays in extra documents.
     */
    public void testTheLargestBucketIsTheOneGivenUp() {
        try (DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(bigArrays, 1000)) {
            TableKey small = key("logs-small-default", Reduction.SUM, 0L);
            TableKey large = key("logs-large-default", Reduction.SUM, 0L);
            assertTrue(record(buffer, small, "only-one", 1.0));
            for (int i = 0; i < 200; i++) {
                assertTrue(record(buffer, large, "service-" + i, 1.0));
            }

            Drained given = buffer.drainLargest(null);
            try {
                assertNotNull(given);
                assertEquals("the bucket holding the most is the one to give up", "logs-large-default", given.key().sourceDataStream());
            } finally {
                given.table().close();
            }
        }
    }

    /**
     * When it was the per-stream cap that refused, freeing another stream's memory gives the refused stream none of its share back, so
     * the choice has to be made within that stream even if a bigger bucket exists elsewhere.
     */
    public void testGivingUpMemoryForAStreamStaysWithinThatStream() {
        try (DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(bigArrays, 1000)) {
            TableKey mine = key("logs-mine-default", Reduction.SUM, 0L);
            TableKey bigger = key("logs-someone-elses-default", Reduction.SUM, 0L);
            assertTrue(record(buffer, mine, "a", 1.0));
            for (int i = 0; i < 200; i++) {
                assertTrue(record(buffer, bigger, "service-" + i, 1.0));
            }

            Drained given = buffer.drainLargest(buffer.streamOf(mine));
            try {
                assertNotNull(given);
                assertEquals(
                    "only the refusing stream's own buckets may be considered",
                    "logs-mine-default",
                    given.key().sourceDataStream()
                );
            } finally {
                given.table().close();
            }
        }
    }

    /**
     * Cost and importance are not the same thing. A metric may say it would rather keep its memory, and that has to be able to outrank
     * being the biggest — otherwise the busiest metric is always the one sacrificed.
     */
    public void testAPreferenceCanOutrankBeingTheLargest() {
        try (DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(bigArrays, 1000)) {
            TableKey guarded = keyWithPreference("logs-guarded-default", 10_000);
            TableKey ordinary = key("logs-ordinary-default", Reduction.SUM, 0L);
            // the guarded stream is by far the bigger of the two
            for (int i = 0; i < 200; i++) {
                assertTrue(record(buffer, guarded, "service-" + i, 1.0));
            }
            for (int i = 0; i < 20; i++) {
                assertTrue(record(buffer, ordinary, "service-" + i, 1.0));
            }

            Drained given = buffer.drainLargest(null);
            try {
                assertNotNull(given);
                assertEquals(
                    "a metric that asked to be kept should not be sacrificed for being busy",
                    "logs-ordinary-default",
                    given.key().sourceDataStream()
                );
            } finally {
                given.table().close();
            }
        }
    }

    private static TableKey keyWithPreference(String sourceDataStream, int preference) {
        CompiledMetric metric = new CompiledMetric(
            "ingest.docs.count",
            Trigger.SUCCESS,
            Reduction.SUM,
            DerivedMetricsPredicate.MATCH_ALL,
            new Source.Constant(1.0),
            List.of("service.name"),
            new int[] { 0 },
            0,
            TEN_SECONDS,
            preference
        );
        return new TableKey(ProjectId.DEFAULT, sourceDataStream, metric, 0L, TEN_SECONDS.millis());
    }

    private static boolean record(DerivedMetricsBuffer buffer, TableKey key, String service, double value) {
        return buffer.record(key, new String[] { service }, new Scratch(), value).recorded();
    }

    private static TableKey key(Reduction reduction, long bucketStart) {
        return key("logs-my_app-default", reduction, bucketStart);
    }

    private static TableKey key(ProjectId project, String sourceDataStream, Reduction reduction, long bucketStart) {
        return new TableKey(project, sourceDataStream, metric(reduction), bucketStart, TEN_SECONDS.millis());
    }

    private static TableKey key(String sourceDataStream, Reduction reduction, long bucketStart) {
        return key(ProjectId.DEFAULT, sourceDataStream, reduction, bucketStart);
    }

    private static CompiledMetric metric(Reduction reduction) {
        return new CompiledMetric(
            "ingest.docs.count",
            Trigger.SUCCESS,
            reduction,
            DerivedMetricsPredicate.MATCH_ALL,
            new Source.Constant(1.0),
            List.of("service.name"),
            new int[] { 0 },
            0,
            TEN_SECONDS
        );
    }
}
