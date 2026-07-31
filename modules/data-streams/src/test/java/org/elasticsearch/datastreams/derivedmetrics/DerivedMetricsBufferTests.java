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
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.datastreams.derivedmetrics.CompiledDerivedMetrics.CompiledMetric;
import org.elasticsearch.datastreams.derivedmetrics.CompiledDerivedMetrics.Interval;
import org.elasticsearch.datastreams.derivedmetrics.CompiledDerivedMetrics.Reduction;
import org.elasticsearch.datastreams.derivedmetrics.CompiledDerivedMetrics.Source;
import org.elasticsearch.datastreams.derivedmetrics.CompiledDerivedMetrics.Trigger;
import org.elasticsearch.datastreams.derivedmetrics.DerivedMetricsBuffer.TableKey;
import org.elasticsearch.datastreams.derivedmetrics.DerivedMetricsDimensionCodec.Scratch;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

public class DerivedMetricsBufferTests extends ESTestCase {

    private static final Interval TEN_SECONDS = new Interval("10s", 10_000L);

    private BigArrays bigArrays;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // MockBigArrays fails the test if anything we allocate is not released, which is exactly the leak we care about
        bigArrays = new MockBigArrays(
            new MockPageCacheRecycler(org.elasticsearch.common.settings.Settings.EMPTY),
            new NoneCircuitBreakerService()
        );
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
                DerivedMetricsSeriesTable table = drained.get(0).getValue();
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
                drained.forEach(entry -> entry.getValue().close());
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
                DerivedMetricsSeriesTable table = drained.get(0).getValue();
                assertEquals(2L, table.size());
                BytesRef spare = new BytesRef();
                assertEquals("checkout", table.dimensionsOf(0, 1, spare)[0]);
                assertEquals(4.0, table.reduce(0, Reduction.SUM, 10_000L), 0.0);
                assertEquals("search", table.dimensionsOf(1, 1, spare)[0]);
                assertEquals(2.0, table.reduce(1, Reduction.SUM, 10_000L), 0.0);
            } finally {
                drained.forEach(entry -> entry.getValue().close());
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
                assertEquals(0L, closed.get(0).getKey().bucketStartMillis());
            } finally {
                closed.forEach(entry -> entry.getValue().close());
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

            buffer.drainAll().forEach(entry -> entry.getValue().close());
            assertEquals(0, buffer.seriesFor("logs-noisy-default"));
            assertEquals(0, buffer.size());
            assertTrue(record(buffer, noisy, "c", 1.0));
        }
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
