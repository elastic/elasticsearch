/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.derivedmetrics;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.datastreams.derivedmetrics.CompiledDerivedMetrics.Reduction;
import org.elasticsearch.datastreams.derivedmetrics.DerivedMetricsBuffer.Accumulator;
import org.elasticsearch.datastreams.derivedmetrics.DerivedMetricsBuffer.BucketKey;
import org.elasticsearch.datastreams.derivedmetrics.DerivedMetricsBuffer.SeriesKey;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;

public class DerivedMetricsBufferTests extends ESTestCase {

    public void testBucketsAreAlignedToTheEpoch() {
        assertEquals(0L, DerivedMetricsBuffer.bucketStart(9_999, 10_000));
        assertEquals(10_000L, DerivedMetricsBuffer.bucketStart(10_000, 10_000));
        assertEquals(10_000L, DerivedMetricsBuffer.bucketStart(19_999, 10_000));
        assertEquals(60_000L, DerivedMetricsBuffer.bucketStart(119_999, 60_000));
    }

    public void testAccumulationReducesEveryWay() {
        DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(10);
        BucketKey key = key(Reduction.SUM, 0L, 10_000L);
        for (double value : new double[] { 4.0, 1.0, 7.0 }) {
            assertTrue(buffer.record(key, value));
        }
        Accumulator accumulator = buffer.drainAll().get(0).getValue();
        assertEquals(3L, accumulator.count());
        assertEquals(12.0, accumulator.reduce(Reduction.SUM, 10_000L), 0.0);
        assertEquals(1.0, accumulator.reduce(Reduction.MIN, 10_000L), 0.0);
        assertEquals(7.0, accumulator.reduce(Reduction.MAX, 10_000L), 0.0);
        assertEquals(4.0, accumulator.reduce(Reduction.AVG, 10_000L), 0.0);
        assertEquals(4.0, accumulator.reduce(Reduction.FIRST, 10_000L), 0.0);
        assertEquals(7.0, accumulator.reduce(Reduction.LAST, 10_000L), 0.0);
        // 12 observations worth of value spread over a ten second interval
        assertEquals(1.2, accumulator.reduce(Reduction.RATE, 10_000L), 0.0);
    }

    public void testObservationsOfDifferentBucketsDoNotMix() {
        DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(10);
        buffer.record(key(Reduction.SUM, 0L, 10_000L), 1.0);
        buffer.record(key(Reduction.SUM, 10_000L, 10_000L), 2.0);
        assertEquals(2, buffer.size());
    }

    public void testDrainClosedOnlyReturnsBucketsPastTheGracePeriod() {
        DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(10);
        buffer.record(key(Reduction.SUM, 0L, 10_000L), 1.0);
        buffer.record(key(Reduction.SUM, 10_000L, 10_000L), 2.0);

        // the first bucket ends at 10s, so with a 5s grace period it only closes at 15s
        assertTrue(buffer.drainClosed(14_999, 5_000).isEmpty());

        List<Map.Entry<BucketKey, Accumulator>> closed = buffer.drainClosed(15_000, 5_000);
        assertEquals(1, closed.size());
        assertEquals(0L, closed.get(0).getKey().bucketStartMillis());
        assertEquals(1, buffer.size());
    }

    public void testDrainAllReturnsOpenBucketsToo() {
        DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(10);
        buffer.record(key(Reduction.SUM, 0L, 10_000L), 1.0);
        assertEquals(1, buffer.drainAll().size());
        assertEquals(0, buffer.size());
    }

    public void testNewSeriesAreDroppedOnceTheCapIsReached() {
        DerivedMetricsBuffer buffer = new DerivedMetricsBuffer(2);
        assertTrue(buffer.record(key(Reduction.SUM, 0L, 10_000L, "a"), 1.0));
        assertTrue(buffer.record(key(Reduction.SUM, 0L, 10_000L, "b"), 1.0));
        assertFalse(buffer.record(key(Reduction.SUM, 0L, 10_000L, "c"), 1.0));
        assertEquals(2, buffer.size());
        assertEquals(1L, buffer.droppedSeries());

        // series that are already tracked keep accumulating even once the cap is reached
        assertTrue(buffer.record(key(Reduction.SUM, 0L, 10_000L, "a"), 1.0));
    }

    private static BucketKey key(Reduction reduction, long bucketStart, long intervalMillis) {
        return key(reduction, bucketStart, intervalMillis, "checkout");
    }

    private static BucketKey key(Reduction reduction, long bucketStart, long intervalMillis, String dimensionValue) {
        SeriesKey series = new SeriesKey(
            ProjectId.DEFAULT,
            "logs-my_app-default",
            "ingest.docs.count",
            "10s",
            reduction,
            List.of("service.name"),
            List.of(dimensionValue)
        );
        return new BucketKey(series, bucketStart, intervalMillis);
    }
}
