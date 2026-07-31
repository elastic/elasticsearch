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
import org.elasticsearch.datastreams.derivedmetrics.DerivedMetricsBuffer.TableKey;
import org.elasticsearch.datastreams.derivedmetrics.DerivedMetricsDimensionCodec.Scratch;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

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
                assertEquals(4.0, table.reduce(0, Reduction.FIRST, 10_000L), 0.0);
                assertEquals(7.0, table.reduce(0, Reduction.LAST, 10_000L), 0.0);
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
            assertEquals(0, drainForPressure(buffer).get(0).partial());

            assertTrue(record(buffer, key, "checkout", 2.0));
            assertEquals(1, drainForPressure(buffer).get(0).partial());

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
     * A bucket flushed early and then never written to again would otherwise leave its partial counter behind forever, since nothing
     * drains a table that no longer exists.
     */
    public void testPartialCountersAreSweptOnceTheBucketCloses() {
        try (DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(bigArrays, 10)) {
            TableKey key = key(Reduction.SUM, 0L);
            assertTrue(record(buffer, key, "checkout", 1.0));
            drainForPressure(buffer);
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
            drainForPressure(buffer);
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
     * Flushing early is what turns a full buffer from lost observations into extra documents: the series that had been refused fits once
     * the bucket has been emitted.
     */
    public void testAnEarlyFlushMakesRoomForARefusedSeries() {
        try (DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(bigArrays, 2)) {
            TableKey key = key(Reduction.SUM, 0L);
            assertTrue(record(buffer, key, "a", 1.0));
            assertTrue(record(buffer, key, "b", 1.0));
            assertFalse(record(buffer, key, "c", 1.0));

            drainForPressure(buffer);
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

    private static List<DerivedMetricsBuffer.Drained> drainForPressure(DerivedMetricsBuffer buffer) {
        List<DerivedMetricsBuffer.Drained> drained = buffer.drainForPressure();
        drained.forEach(d -> d.table().close());
        return drained;
    }

    private static boolean record(DerivedMetricsBuffer buffer, TableKey key, String service, double value) {
        return buffer.record(key, new String[] { service }, new Scratch(), value);
    }

    private static TableKey key(Reduction reduction, long bucketStart) {
        return key("logs-my_app-default", reduction, bucketStart);
    }

    private static TableKey key(String sourceDataStream, Reduction reduction, long bucketStart) {
        CompiledMetric metric = new CompiledMetric(
            "ingest.docs.count",
            Trigger.SUCCESS,
            reduction,
            DerivedMetricsPredicate.MATCH_ALL,
            new Source.Constant(1.0),
            List.of("service.name"),
            TEN_SECONDS
        );
        return new TableKey(ProjectId.DEFAULT, sourceDataStream, metric, bucketStart, TEN_SECONDS.millis());
    }
}
