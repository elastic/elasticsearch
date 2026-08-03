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
import java.util.concurrent.atomic.LongAdder;

import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.startsWith;

public class DerivedMetricsBufferTests extends ESTestCase {

    private static final Interval TEN_SECONDS = new Interval("10s", 10_000L);

    private BigArrays bigArrays;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // MockBigArrays fails the test if anything we allocate is not released, which is exactly the leak we care about
        bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
    }

    /**
     * The property the whole feature rests on: output is a function of series and interval, never of write rate. A producer whose data
     * arrives at more distinct moments than the metric has slots is the one case that could break it, since every observation would open a
     * bucket. Dropping the stalest bucket is what keeps the output bounded, and this pins that it stays bounded rather than climbing with
     * the observations.
     */
    public void testAnErraticProducerCostsBoundedOutput() {
        int slots = 4;
        try (DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(bigArrays, 10_000, 10_000, 8, 0, 1, 0, 1_000, slots)) {
            CompiledMetric metric = metric(Reduction.SUM);
            int observations = 2_000;
            for (int i = 0; i < observations; i++) {
                // sixteen moments interleaved, four times the slots, which is the shape a fleet of producers at unrelated lags produces
                long bucketStart = (i * 7L % 16L) * TEN_SECONDS.millis();
                buffer.record(
                    new TableKey(ProjectId.DEFAULT, "logs-my_app-default", metric, bucketStart, TEN_SECONDS.millis()),
                    new String[] { "checkout" },
                    new Scratch(),
                    1.0
                );
            }

            var drained = buffer.drainAll();
            try {
                assertThat("what is held can never exceed the slots", drained.size(), lessThanOrEqualTo(slots));
                assertThat("and the loss is counted rather than silent", buffer.bucketsDropped(), greaterThan(0L));
            } finally {
                drained.forEach(d -> d.table().close());
            }
        }
    }

    /**
     * Ordered data is the case that must not pay for the above, and does not: a producer moving through its own time holds one moment at a
     * time, so each bucket idles out and is written normally however far behind the producer is running.
     *
     * <p>The qualifier is speed rather than lateness. A bucket idles out on the wall clock, so a producer replaying <em>faster</em> than
     * real time can outrun that and give up buckets it had finished with. That is the accepted cost of bounding the output, it is counted,
     * and {@code max_interval_buckets} is what buys more room for it.
     */
    public void testAProducerRunningBehindDropsNothing() {
        try (DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(bigArrays, 10_000, 10_000, 8, 0, 1, 0, 1_000, 2)) {
            CompiledMetric metric = metric(Reduction.SUM);
            for (int bucket = 0; bucket < 360; bucket++) {
                for (int i = 0; i < 5; i++) {
                    buffer.record(
                        new TableKey(ProjectId.DEFAULT, "logs-my_app-default", metric, bucket * TEN_SECONDS.millis(), TEN_SECONDS.millis()),
                        new String[] { "checkout" },
                        new Scratch(),
                        1.0
                    );
                }
                // the flush the node runs anyway, which retires each bucket once the producer has moved past it
                buffer.drainClosed((bucket + 2) * TEN_SECONDS.millis(), 0).forEach(d -> d.table().close());
            }
            assertEquals("an hour of ordered history costs nothing", 0L, buffer.bucketsDropped());
        }
    }

    /**
     * The accepted cost, pinned so that it is a decision rather than a surprise: a backlog replayed faster than buckets can idle out gives
     * up the ones it has finished with, and says how many so an operator can size {@code max_interval_buckets} against their own replay.
     */
    public void testABacklogReplayedFasterThanRealTimeGivesUpBuckets() {
        try (DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(bigArrays, 10_000, 10_000, 8, 0, 1, 0, 1_000, 2)) {
            CompiledMetric metric = metric(Reduction.SUM);
            for (int bucket = 0; bucket < 120; bucket++) {
                buffer.record(
                    new TableKey(ProjectId.DEFAULT, "logs-my_app-default", metric, bucket * TEN_SECONDS.millis(), TEN_SECONDS.millis()),
                    new String[] { "checkout" },
                    new Scratch(),
                    1.0
                );
                // twenty minutes of history drained in two seconds, so nothing has been quiet long enough to be written out
                buffer.drainClosed(bucket * 16L, 5_000).forEach(d -> d.table().close());
            }
            assertThat("the backlog outran the flush", buffer.bucketsDropped(), greaterThan(0L));
            assertThat("and it is reported as how short of slots it came", buffer.maxBucketsDroppedInACycle(), greaterThan(0L));
        }
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

    /**
     * The node-wide refusal counters say the node ran out of budget; these say which stream spent it, which is the only form of the answer
     * that names something an operator can change. Without this, "series were dropped" on a node serving hundreds of streams sends whoever
     * reads it looking through all of them.
     */
    public void testRefusalsAreAttributedToTheStreamThatSufferedThem() {
        try (DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(bigArrays, 100, 2)) {
            TableKey noisy = key("logs-noisy-default", Reduction.SUM, 0L);
            TableKey quiet = key("logs-quiet-default", Reduction.SUM, 0L);

            assertTrue(record(buffer, noisy, "a", 1.0));
            assertTrue(record(buffer, noisy, "b", 1.0));
            assertFalse(record(buffer, noisy, "c", 1.0));
            assertFalse(record(buffer, noisy, "d", 1.0));
            assertTrue(record(buffer, quiet, "a", 1.0));

            List<DerivedMetricsBuffer.StreamRefusals> refusals = buffer.streamRefusals();
            // only the stream that was actually refused appears: a stream inside its budget has nothing to report
            assertEquals(1, refusals.size());
            DerivedMetricsBuffer.StreamRefusals refused = refusals.get(0);
            assertEquals("logs-noisy-default", refused.sourceDataStream());
            assertEquals(ProjectId.DEFAULT, refused.project());
            assertEquals(2L, refused.atStreamCap());
            // which budget refused is the whole point: this one says go and find the stream, not raise the node's cap
            assertEquals(0L, refused.atNodeCap());
            assertEquals(0L, refused.atBreaker());
            assertEquals(0L, refused.atHistogramCap());
            assertEquals(2L, buffer.droppedSeriesAtStreamCap());
        }
    }

    /** A refusal at the node cap is attributed to the stream that suffered it, but named as the node's problem rather than the stream's. */
    public void testNodeCapRefusalsSayTheNodeIsFullRatherThanTheStream() {
        try (DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(bigArrays, 2)) {
            TableKey key = key("logs-my_app-default", Reduction.SUM, 0L);
            assertTrue(record(buffer, key, "a", 1.0));
            assertTrue(record(buffer, key, "b", 1.0));
            assertFalse(record(buffer, key, "c", 1.0));

            DerivedMetricsBuffer.StreamRefusals refused = buffer.streamRefusals().get(0);
            assertEquals(1L, refused.atNodeCap());
            assertEquals(0L, refused.atStreamCap());
        }
    }

    /**
     * What a metric is holding has to be readable per metric rather than only as a node total, because "this node holds 10,000 series" does
     * not say which of them to reduce. The numbers come from the same figures the shedding decision already keeps, so reporting them costs
     * a walk of the live buckets and nothing on the write path.
     */
    public void testMetricSnapshotsReportWhatEachMetricIsHolding() {
        try (DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(bigArrays, 100)) {
            TableKey busy = key("logs-busy-default", Reduction.SUM, 0L);
            TableKey quiet = key("logs-quiet-default", Reduction.SUM, 0L);
            assertTrue(record(buffer, busy, "a", 1.0));
            assertTrue(record(buffer, busy, "b", 1.0));
            assertTrue(record(buffer, busy, "c", 1.0));
            assertTrue(record(buffer, quiet, "a", 1.0));
            // a second bucket of the same metric, which a flush has not caught up with yet: the metric's cost is both of them together
            assertTrue(record(buffer, key("logs-busy-default", Reduction.SUM, 10_000L), "d", 1.0));

            List<DerivedMetricsBuffer.MetricSnapshot> snapshots = buffer.metricSnapshots();
            assertEquals(2, snapshots.size());
            DerivedMetricsBuffer.MetricSnapshot busiest = snapshots.stream()
                .filter(snapshot -> snapshot.sourceDataStream().equals("logs-busy-default"))
                .findFirst()
                .orElseThrow();
            assertEquals("ingest.docs.count", busiest.metric());
            assertEquals("10s", busiest.interval());
            assertEquals(4L, busiest.seriesHeld());
            assertFalse(busiest.histogram());
            assertThat(busiest.bytesHeld(), greaterThan(0L));

            DerivedMetricsBuffer.MetricSnapshot quietest = snapshots.stream()
                .filter(snapshot -> snapshot.sourceDataStream().equals("logs-quiet-default"))
                .findFirst()
                .orElseThrow();
            assertEquals(1L, quietest.seriesHeld());
            assertThat(busiest.bytesHeld(), greaterThan(quietest.bytesHeld()));

            // draining is what makes a bucket stop costing anything, so the snapshot has to follow it down
            buffer.drainAll().forEach(drained -> drained.table().close());
            assertEquals(List.of(), buffer.metricSnapshots());
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
     * The counter is what keeps the ids of a bucket's results apart, so it has to outlive the bucket. A late observation reopens a bucket
     * already written out, and an offset starting over would give the new result the id of the one already there.
     */
    public void testAPartialCounterOutlivesTheBucketItCounts() {
        try (DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(bigArrays, 10)) {
            TableKey key = key(Reduction.SUM, 0L);
            assertTrue(record(buffer, key, "checkout", 1.0));
            drainForPressure(buffer, key);
            assertEquals(1, buffer.partialsTracked());

            buffer.drainClosed(20_000, 0);
            assertEquals("the bucket is gone but a late observation could still reach it", 1, buffer.partialsTracked());
        }
    }

    /**
     * It cannot be kept forever either. Once the metric's own data has moved far enough on that nothing could reach back to the bucket, the
     * counter is dropped — measured against the metric's progression, since a replaying producer's data is nowhere near this node's clock.
     */
    public void testAPartialCounterIsDroppedOnceTheDataHasMovedOn() {
        try (DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(bigArrays, 10)) {
            TableKey key = key(Reduction.SUM, 0L);
            assertTrue(record(buffer, key, "checkout", 1.0));
            drainForPressure(buffer, key);
            assertEquals(1, buffer.partialsTracked());

            // twenty intervals later, well past the ten this remembers for
            assertTrue(record(buffer, key(Reduction.SUM, 200_000L), "checkout", 1.0));
            buffer.drainClosed(200_000, 0);
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
        long scalar = bytesPerSeries(Reduction.SUM, 1000, 1);
        long histogram = bytesPerSeries(Reduction.HISTOGRAM, 50, 1);
        logger.info("bytes per series: scalar [{}], histogram [{}]", scalar, histogram);

        assertThat("a scalar series is a handful of primitives in shared arrays", scalar, lessThan(200L));
        // Expressed against the scalar cost rather than as an absolute, because the absolute moves whenever the histogram storage gets
        // cheaper — it has twice already — while the thing worth guarding is that a distribution is in a different class of cost from a
        // handful of primitives. These series see one observation each, so they never grow their buckets towards capacity; the busy case
        // is measured separately by testABusyHistogramSeriesCostsMoreThanAnIdleOne.
        assertThat("a histogram series keeps a whole distribution", histogram, greaterThan(scalar * 5));
        assertThat("but it must still be bounded by its bucket count", histogram, lessThan(20_000L));
    }

    /**
     * A histogram series costs more once it is busy than when it is idle, and capacity planning has to use the busy number.
     *
     * <p>The generator buffers raw values and only folds them into an accumulating histogram once that buffer fills, which happens after
     * as many observations as the series has buckets. A series quiet enough never to fill it therefore never pays for the accumulator —
     * and every series in a real workload does.
     */
    public void testABusyHistogramSeriesCostsMoreThanAnIdleOne() {
        int buckets = DerivedMetricsBuffer.DEFAULT_HISTOGRAM_BUCKETS;
        long idle = bytesPerSeries(Reduction.HISTOGRAM, 20, 1);
        long busy = bytesPerSeries(Reduction.HISTOGRAM, 20, buckets * 3);
        logger.info("histogram bytes per series: idle [{}], busy [{}]", idle, busy);

        assertThat("a series that has folded its values away holds more than one that has not", busy, greaterThan(idle));
        // the point of the bucket capacity is that this is bounded, however many observations arrive
        assertThat("but it is still bounded by the bucket count", busy, lessThan(20_000L));
    }

    /**
     * Fills a table with the given number of distinct series and returns the accounted bytes each one cost.
     */
    private long bytesPerSeries(Reduction reduction, int series, int observationsPerSeries) {
        CircuitBreakerService breakerService = LimitedBreaker.service(DerivedMetricsService.BREAKER_NAME, ByteSizeValue.ofMb(512));
        CircuitBreaker breaker = breakerService.getBreaker(DerivedMetricsService.BREAKER_NAME);
        BigArrays accounted = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), breakerService).withCircuitBreaking();
        try (DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(accounted, series * 2)) {
            TableKey key = key(reduction, 0L);
            for (int i = 0; i < series; i++) {
                for (int observation = 0; observation < observationsPerSeries; observation++) {
                    assertTrue(record(buffer, key, "service-" + i, observation + 1.0));
                }
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
     * The same race against a bucket that is striped per thread rather than shared. Striping adds two ways to lose an observation that a
     * single table does not have: a thread can open a stripe in a bucket that has just been sealed, and the stripes have to be folded
     * together before emission. Both would look like a slightly low count rather than a failure, so this counts every observation twice —
     * once as it is accepted, once as it comes out — and requires the two to agree.
     */
    public void testConcurrentRecordingIntoAStripedBucketLosesNothing() throws Exception {
        int threads = 8;
        int perThread = 4000;
        int services = 4;
        try (DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(bigArrays, 10_000, 10_000, 8, 0, threads)) {
            TableKey key = key(Reduction.SUM, 0L);
            LongAdder accepted = new LongAdder();
            LongAdder emitted = new LongAdder();

            runInParallel(threads + 1, worker -> {
                if (worker == threads) {
                    // the drainer, sealing and merging the stripes out from under the writers over and over
                    for (int i = 0; i < 50; i++) {
                        for (Drained entry : buffer.drainAll()) {
                            try {
                                for (long ordinal = 0; ordinal < entry.table().size(); ordinal++) {
                                    emitted.add(entry.table().countOf(ordinal));
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
                    if (buffer.record(key, new String[] { "service-" + (i % services) }, scratch, 1.0).recorded()) {
                        accepted.increment();
                    }
                }
            });

            var drained = buffer.drainAll();
            try {
                for (Drained entry : drained) {
                    for (long ordinal = 0; ordinal < entry.table().size(); ordinal++) {
                        emitted.add(entry.table().countOf(ordinal));
                    }
                }
            } finally {
                drained.forEach(d -> d.table().close());
            }

            assertEquals(
                "the budget is far wider than this test needs, so nothing may have been refused",
                threads * perThread,
                accepted.sum()
            );
            assertEquals("every observation must come back out of exactly one stripe", accepted.sum(), emitted.sum());
            assertEquals("draining must return the whole budget", 0, buffer.size());

            // and the bucket really was striped: four dimension values is well below the threshold, so the next one is too
            TableKey next = key(Reduction.SUM, 10_000L);
            assertTrue(record(buffer, next, "service-0", 1.0));
            assertThat(buffer.stripesOf(next), greaterThan(1));
        }
    }

    /**
     * Merging the stripes has to reduce them the same way one shared table would have. A sum that is merely added up would hide a min or a
     * max that was taken from one stripe instead of across all of them, so this checks each reduction with values that only agree if every
     * stripe was folded in.
     */
    public void testMergingStripesReducesAcrossAllOfThem() throws Exception {
        int threads = 4;
        try (DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(bigArrays, 100, 100, 8, 0, threads)) {
            TableKey key = key(Reduction.SUM, 0L);
            // one value per thread, so whichever stripes they land on, the extremes live in different ones unless they collided
            runInParallel(threads, thread -> buffer.record(key, new String[] { "checkout" }, new Scratch(), thread + 1.0));

            var drained = buffer.drainAll();
            try {
                DerivedMetricsSeriesTable table = drained.get(0).table();
                assertEquals("the same dimension tuple in every stripe is one series once merged", 1L, table.size());
                assertEquals(threads, table.countOf(0));
                assertEquals(10.0, table.reduce(0, Reduction.SUM, 10_000L), 0.0);
                assertEquals(1.0, table.reduce(0, Reduction.MIN, 10_000L), 0.0);
                assertEquals(4.0, table.reduce(0, Reduction.MAX, 10_000L), 0.0);
            } finally {
                drained.forEach(d -> d.table().close());
            }
            assertEquals(0, buffer.size());
        }
    }

    /**
     * The decision striping rests on. A metric with few series is replicated per thread because a copy of it is free and its monitor is
     * otherwise nearly the whole of an observation; a metric with many series keeps one shared table, because by then the observations are
     * already spread across the series and replicating them would cost real memory. Which one a bucket gets is decided from what the
     * metric's cardinality turned out to be last time, and a metric nobody has seen yet starts striped because a metric starts small.
     */
    public void testOnlyALowCardinalityMetricIsStriped() {
        int stripes = 8;
        try (DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(bigArrays, 10_000, 10_000, 8, 0, stripes)) {
            TableKey low = key("logs-low-default", Reduction.SUM, 0L);
            TableKey high = key("logs-high-default", Reduction.SUM, 0L);

            assertTrue(record(buffer, low, "checkout", 1.0));
            assertEquals("a metric with no history starts striped", stripes, buffer.stripesOf(low));
            for (int i = 0; i <= DerivedMetricsBuffer.STRIPE_SERIES_THRESHOLD; i++) {
                assertTrue(record(buffer, high, "service-" + i, 1.0));
            }
            // the first bucket of either is striped; only what it turned out to hold can change that
            assertEquals(stripes, buffer.stripesOf(high));
            buffer.drainClosed(20_000, 0).forEach(d -> d.table().close());

            TableKey lowNext = key("logs-low-default", Reduction.SUM, 10_000L);
            TableKey highNext = key("logs-high-default", Reduction.SUM, 10_000L);
            assertTrue(record(buffer, lowNext, "checkout", 1.0));
            assertTrue(record(buffer, highNext, "service-0", 1.0));

            assertEquals("one series is nothing to replicate", stripes, buffer.stripesOf(lowNext));
            assertEquals("past the threshold a bucket goes back to one shared table", 1, buffer.stripesOf(highNext));
        }
    }

    /**
     * A distribution costs roughly thirty times what a scalar series does, which puts it on the wrong side of the trade striping rests on:
     * the whole point is that the buckets cheap enough to replicate are the ones that contend.
     */
    public void testAHistogramMetricIsNeverStriped() {
        try (DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(bigArrays, 100, 100, 8, 0, 8)) {
            TableKey key = key(Reduction.HISTOGRAM, 0L);
            assertTrue(record(buffer, key, "checkout", 1.0));
            assertEquals(1, buffer.stripesOf(key));
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

    /**
     * The failure this exists to prevent: one dimension with a value per document consumes the node budget and every other metric
     * starves. Instead the metric gives up that dimension's breakdown, stays bounded, and keeps counting every observation.
     */
    public void testARunawayDimensionCollapsesAndTheMetricKeepsWorking() {
        try (DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(bigArrays, 10_000, 10_000, 8, 0, 1, 100)) {
            TableKey key = key(Reduction.SUM, 0L);
            for (int i = 0; i < 500; i++) {
                assertTrue(record(buffer, key, "user-" + i, 1.0));
            }
            assertEquals("the dimension should have been given up exactly once", 1L, buffer.dimensionsCollapsed());
            int bounded = buffer.size();
            assertThat("the runaway must have been stopped well short of the values offered", bounded, lessThan(400));

            // every further value now lands on the placeholder series, so the metric's cost stops growing entirely
            for (int i = 500; i < 5_000; i++) {
                assertTrue(record(buffer, key, "user-" + i, 1.0));
            }
            assertEquals("a collapsed dimension must stop the series count growing at all", bounded, buffer.size());

            var drained = buffer.drainAll();
            try {
                DerivedMetricsSeriesTable table = drained.get(0).table();
                BytesRef spare = new BytesRef();
                long observations = 0;
                boolean sawPlaceholder = false;
                for (long ordinal = 0; ordinal < table.size(); ordinal++) {
                    observations += table.countOf(ordinal);
                    if (DerivedMetricsDimensionCodec.COLLAPSED_VALUE.equals(table.dimensionsOf(ordinal, 1, spare)[0])) {
                        sawPlaceholder = true;
                    }
                }
                assertTrue("the collapsed breakdown has to be visible in what is emitted", sawPlaceholder);
                assertEquals("no observation may be lost by collapsing; only the breakdown is", 5_000L, observations);
            } finally {
                drained.forEach(d -> d.table().close());
            }
        }
    }

    /**
     * Collapsing has to be per dimension rather than per metric, or a metric with one bad dimension would lose the good ones with it —
     * which is the same all-or-nothing failure the series caps already have.
     */
    public void testOnlyTheRunawayDimensionOfAMetricCollapses() {
        try (DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(bigArrays, 10_000, 10_000, 8, 0, 1, 100)) {
            TableKey key = twoDimensionKey();
            for (int i = 0; i < 2_000; i++) {
                assertTrue(buffer.record(key, new String[] { "service-" + (i % 4), "user-" + i }, new Scratch(), 1.0).recorded());
            }

            var reported = buffer.dimensionCardinalities();
            assertEquals(2, reported.size());
            var service = reported.stream().filter(d -> d.dimension().equals("service.name")).findFirst().orElseThrow();
            var user = reported.stream().filter(d -> d.dimension().equals("user.id")).findFirst().orElseThrow();
            assertFalse("a four-valued dimension is nowhere near its budget", service.collapsed());
            assertTrue("a dimension with a value per document is far past it", user.collapsed());
            assertEquals(1L, buffer.dimensionsCollapsed());

            var drained = buffer.drainAll();
            try {
                DerivedMetricsSeriesTable table = drained.get(0).table();
                BytesRef spare = new BytesRef();
                for (long ordinal = table.size() - 1; ordinal >= table.size() - 4; ordinal--) {
                    String[] dimensions = table.dimensionsOf(ordinal, 2, spare);
                    assertThat("the surviving dimension keeps its real values", dimensions[0], startsWith("service-"));
                    assertEquals(DerivedMetricsDimensionCodec.COLLAPSED_VALUE, dimensions[1]);
                }
            } finally {
                drained.forEach(d -> d.table().close());
            }
        }
    }

    /**
     * The estimate is what answers "which dimension is spending the budget", so it has to be roughly right — and it has to be reported
     * against the stream and metric it belongs to, since a node holds many of both.
     */
    public void testDimensionCardinalityIsReportedPerStreamAndMetric() {
        try (DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(bigArrays, 10_000, 10_000, 8, 0, 1, 0)) {
            TableKey mine = key("logs-mine-default", Reduction.SUM, 0L);
            TableKey theirs = key("logs-theirs-default", Reduction.SUM, 0L);
            for (int i = 0; i < 1_000; i++) {
                assertTrue(record(buffer, mine, "user-" + i, 1.0));
            }
            assertTrue(record(buffer, theirs, "checkout", 1.0));

            var reported = buffer.dimensionCardinalities();
            assertEquals(2, reported.size());
            var busy = reported.stream().filter(d -> d.sourceDataStream().equals("logs-mine-default")).findFirst().orElseThrow();
            var quiet = reported.stream().filter(d -> d.sourceDataStream().equals("logs-theirs-default")).findFirst().orElseThrow();
            assertEquals("ingest.docs.count", busy.metric());
            assertEquals("service.name", busy.dimension());
            logger.info("estimated [{}] distinct values for a dimension that really had 1000", busy.estimatedValues());
            assertThat(busy.estimatedValues(), both(greaterThan(800L)).and(lessThan(1_200L)));
            assertEquals(1L, quiet.estimatedValues());
            assertFalse("a zero budget counts without ever collapsing", busy.collapsed());
        }
    }

    /**
     * The sketches outlive every bucket, so nothing else would ever release them. MockBigArrays would catch a leaked array, but not the
     * breaker accounting behind it, which is what this asserts.
     */
    public void testDimensionSketchesGiveTheirBytesBack() {
        CircuitBreakerService breakerService = LimitedBreaker.service(DerivedMetricsService.BREAKER_NAME, ByteSizeValue.ofMb(64));
        CircuitBreaker breaker = breakerService.getBreaker(DerivedMetricsService.BREAKER_NAME);
        BigArrays accounted = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), breakerService).withCircuitBreaking();

        try (DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(accounted, 10_000, 10_000, 8, 0, 1, 100)) {
            TableKey key = key(Reduction.SUM, 0L);
            for (int i = 0; i < 2_000; i++) {
                assertTrue(record(buffer, key, "user-" + i, 1.0));
            }
            assertThat("the sketches should have taken real memory", breaker.getUsed(), greaterThan(0L));
            var drained = buffer.drainAll();
            drained.forEach(d -> d.table().close());
            assertThat(
                "draining the tables does not release the sketches, which are not part of a bucket",
                breaker.getUsed(),
                greaterThan(0L)
            );
        }
        assertEquals("closing the buffer must give every byte back, sketches included", 0L, breaker.getUsed());
    }

    private static TableKey twoDimensionKey() {
        CompiledMetric metric = new CompiledMetric(
            "ingest.docs.count",
            Trigger.SUCCESS,
            Reduction.SUM,
            DerivedMetricsPredicate.MATCH_ALL,
            new Source.Constant(1.0),
            List.of("service.name", "user.id"),
            new int[] { 0, 1 },
            0,
            TEN_SECONDS
        );
        return new TableKey(ProjectId.DEFAULT, "logs-my_app-default", metric, 0L, TEN_SECONDS.millis());
    }

    /**
     * A histogram series costs about forty times a scalar one, so the general series budget cannot protect a small node from
     * distributions: ten thousand of them would ask for more memory than the whole circuit breaker allows on a node with a small heap.
     * They therefore have a budget of their own, and spend both.
     */
    public void testHistogramSeriesHaveTheirOwnBudget() {
        // a generous general budget, so that anything refused here was refused by the histogram budget and nothing else
        try (DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(bigArrays, 10_000, 10_000, 8, 0, 1, 0, 3)) {
            TableKey histogram = key("logs-histogram-default", Reduction.HISTOGRAM, 0L);
            for (int i = 0; i < 3; i++) {
                assertTrue(record(buffer, histogram, "service-" + i, 1.0));
            }
            assertFalse("the fourth distribution is past the histogram budget", record(buffer, histogram, "service-3", 1.0));
            assertEquals(1L, buffer.droppedSeriesAtHistogramCap());
            assertEquals(0L, buffer.droppedSeriesAtNodeCap());
            assertEquals(3, buffer.histogramSeries());

            // scalar series are unaffected: they are cheap, and the budget they share is nowhere near spent
            TableKey scalar = key("logs-scalar-default", Reduction.SUM, 0L);
            for (int i = 0; i < 50; i++) {
                assertTrue("a scalar series must not be refused by the histogram budget", record(buffer, scalar, "service-" + i, 1.0));
            }
            assertEquals("scalar series are not distributions", 3, buffer.histogramSeries());
        }
    }

    /**
     * The histogram budget has to be given back when the distributions holding it are emitted, or a node would stop accepting them after
     * one interval's worth however little it was actually holding.
     */
    public void testDrainingDistributionsReturnsTheirBudget() {
        try (DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(bigArrays, 10_000, 10_000, 8, 0, 1, 0, 3)) {
            TableKey histogram = key(Reduction.HISTOGRAM, 0L);
            for (int i = 0; i < 3; i++) {
                assertTrue(record(buffer, histogram, "service-" + i, 1.0));
            }
            assertEquals(3, buffer.histogramSeries());

            buffer.drainAll().forEach(d -> d.table().close());
            assertEquals("draining must return the histogram budget exactly", 0, buffer.histogramSeries());

            // and the node accepts distributions again
            assertTrue(record(buffer, key(Reduction.HISTOGRAM, 10_000L), "service-0", 1.0));
        }
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
