/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.apache.lucene.internal.hppc.IntArrayList;
import org.apache.lucene.internal.hppc.IntDoubleHashMap;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;

public class AggregateCounterFieldDownsamplerTests extends ESTestCase {

    /**
     * Monotonically increasing counter with no resets within a single bucket.
     * Downsampled doc: 1
     */
    public void testAggregateCounter() throws IOException {
        CounterResetDataPoints resetDataPoints = new CounterResetDataPoints();
        NumericMetricFieldDownsampler.AggregateCounter producer = new NumericMetricFieldDownsampler.AggregateCounter("my-counter", null);
        IntArrayList docIdBuffer = IntArrayList.from(6, 5, 4, 3, 2, 1, 0);
        long[] timeValues = new long[] { 70, 60, 50, 40, 30, 20, 10 };
        SortedNumericDoubleValues counterValues = createNumericValuesInstance(docIdBuffer, 64, 32, 16, 8, 4, 2, 1);
        producer.collect(counterValues, timeValues, docIdBuffer);
        producer.updateResetDataPoints(resetDataPoints);
        assertThat(producer.downsampledValue, equalTo(1.0));
        assertThat(resetDataPoints.isEmpty(), equalTo(true));
        producer.reset();
        assertThat(producer.downsampledValue, equalTo(Double.NaN));
        assertThat(producer.previousValue, equalTo(1.0));
        assertThat(producer.lastTimestamp, equalTo(-1L));
        producer.tsidReset();
        assertThat(producer.previousValue, equalTo(Double.NaN));
    }

    /**
     * Single reset within a bucket. The last-before-reset value (16 at t=50) and the after-reset
     * value (5 at t=60) are both stored as reset data points.
     * Downsampled doc: 1
     * Reset docs: 16 at 50, 5 at 60
     */
    public void testAggregateCounterWithReset() throws IOException {
        CounterResetDataPoints resetDataPoints = new CounterResetDataPoints();
        NumericMetricFieldDownsampler.AggregateCounter producer = new NumericMetricFieldDownsampler.AggregateCounter("my-counter", null);
        IntArrayList docIdBuffer = IntArrayList.from(6, 5, 4, 3, 2, 1, 0);
        long[] timeValues = new long[] { 70, 60, 50, 40, 30, 20, 10 };
        SortedNumericDoubleValues counterValues = createNumericValuesInstance(docIdBuffer, 8, 5, 16, 8, 4, 2, 1);
        producer.collect(counterValues, timeValues, docIdBuffer);
        producer.updateResetDataPoints(resetDataPoints);
        assertThat(producer.downsampledValue, equalTo(1.0));
        assertThat(resetDataPoints.countResetDocuments(), equalTo(2));
        resetDataPoints.processDataPoints((timestamp, dataPoints) -> {
            assertThat(timestamp, anyOf(equalTo(60L), equalTo(50L)));
            if (timestamp == 60L) {
                assertThat(dataPoints, equalTo(List.of(Tuple.tuple("my-counter", 5.0))));
            }
            if (timestamp == 50L) {
                assertThat(dataPoints, equalTo(List.of(Tuple.tuple("my-counter", 16.0))));
            }
        });
        producer.reset();
        assertThat(producer.downsampledValue, equalTo(Double.NaN));
        assertThat(producer.previousValue, equalTo(1.0));
        assertThat(producer.lastTimestamp, equalTo(-1L));
        producer.tsidReset();
        assertThat(producer.previousValue, equalTo(Double.NaN));
    }

    /**
     * Counter with a reset where the last-before-reset value (1) is also the earliest value in
     * the bucket and equals the downsampled value. Only the after-reset value (0 at t=20) is
     * stored as a reset data point; the before-reset value is not duplicated.
     * Downsampled doc: 1
     * Reset docs: 0 at 20
     */
    public void testAggregateCounterDoesNotDuplicateFirstValue() throws IOException {
        CounterResetDataPoints resetDataPoints = new CounterResetDataPoints();
        NumericMetricFieldDownsampler.AggregateCounter producer = new NumericMetricFieldDownsampler.AggregateCounter("my-counter", null);
        IntArrayList docIdBuffer = IntArrayList.from(2, 1, 0);
        long[] timeValues = new long[] { 30, 20, 10 };
        SortedNumericDoubleValues counterValues = createNumericValuesInstance(docIdBuffer, 7, 0, 1);
        producer.collect(counterValues, timeValues, docIdBuffer);
        producer.updateResetDataPoints(resetDataPoints);
        assertThat(producer.downsampledValue, equalTo(1.0));
        assertThat(resetDataPoints.countResetDocuments(), equalTo(1));
        resetDataPoints.processDataPoints((timestamp, dataPoints) -> {
            assertThat(timestamp, equalTo(20L));
            assertThat(dataPoints, equalTo(List.of(Tuple.tuple("my-counter", 0.0))));
        });
        producer.reset();
        assertThat(producer.downsampledValue, equalTo(Double.NaN));
        assertThat(producer.previousValue, equalTo(1.0));
        assertThat(producer.lastTimestamp, equalTo(-1L));
        producer.tsidReset();
        assertThat(producer.previousValue, equalTo(Double.NaN));
    }

    /**
     * Two resets within a single bucket where the last-before-reset value of the earlier reset (8)
     * is larger than the last-before-reset value of the later reset (5, which is also the most
     * recently persisted reset point). This means the after-reset value (3) is redundant and does
     * NOT get stored as a separate reset data point.
     * Downsampled doc: 1
     * Reset docs: 8 at 40, 5 at 60, 2 at 70
     */
    public void testAggregateCounterWithMultipleResetsLastBeforeResetLarger() throws IOException {
        CounterResetDataPoints resetDataPoints = new CounterResetDataPoints();
        NumericMetricFieldDownsampler.AggregateCounter producer = new NumericMetricFieldDownsampler.AggregateCounter("my-counter", null);
        IntArrayList docIdBuffer = IntArrayList.from(7, 6, 5, 4, 3, 2, 1, 0);
        long[] timeValues = new long[] { 80, 70, 60, 50, 40, 30, 20, 10 };
        SortedNumericDoubleValues counterValues = createNumericValuesInstance(docIdBuffer, 4, 2, 5, 3, 8, 4, 2, 1);
        producer.collect(counterValues, timeValues, docIdBuffer);
        producer.updateResetDataPoints(resetDataPoints);
        assertThat(producer.downsampledValue, equalTo(1.0));
        assertThat(resetDataPoints.countResetDocuments(), equalTo(3));
        resetDataPoints.processDataPoints((timestamp, dataPoints) -> {
            assertThat(timestamp, anyOf(equalTo(40L), equalTo(60L), equalTo(70L)));
            if (timestamp == 40L) {
                assertThat(dataPoints, equalTo(List.of(Tuple.tuple("my-counter", 8.0))));
            }
            if (timestamp == 60L) {
                assertThat(dataPoints, equalTo(List.of(Tuple.tuple("my-counter", 5.0))));
            }
            if (timestamp == 70L) {
                assertThat(dataPoints, equalTo(List.of(Tuple.tuple("my-counter", 2.0))));
            }
        });
        producer.reset();
        assertThat(producer.downsampledValue, equalTo(Double.NaN));
        assertThat(producer.previousValue, equalTo(1.0));
        assertThat(producer.lastTimestamp, equalTo(-1L));
        producer.tsidReset();
        assertThat(producer.previousValue, equalTo(Double.NaN));
    }

    /**
     * Two resets within a single bucket where the last-before-reset value of the earlier reset (4)
     * is smaller than the last-before-reset value of the later reset (5, which is also the most
     * recently persisted reset point). This means the after-reset value (3) is NOT redundant and
     * gets stored as a separate reset data point.
     * Downsampled doc: 1
     * Reset docs: 4 at 30, 3 at 40, 5 at 50, 2 at 60
     */
    public void testAggregateCounterWithMultipleResetsLastBeforeResetSmaller() throws IOException {
        CounterResetDataPoints resetDataPoints = new CounterResetDataPoints();
        NumericMetricFieldDownsampler.AggregateCounter producer = new NumericMetricFieldDownsampler.AggregateCounter("my-counter", null);
        IntArrayList docIdBuffer = IntArrayList.from(6, 5, 4, 3, 2, 1, 0);
        long[] timeValues = new long[] { 70, 60, 50, 40, 30, 20, 10 };
        SortedNumericDoubleValues counterValues = createNumericValuesInstance(docIdBuffer, 4, 2, 5, 3, 4, 2, 1);
        producer.collect(counterValues, timeValues, docIdBuffer);
        producer.updateResetDataPoints(resetDataPoints);
        assertThat(producer.downsampledValue, equalTo(1.0));
        assertThat(resetDataPoints.countResetDocuments(), equalTo(4));
        resetDataPoints.processDataPoints((timestamp, dataPoints) -> {
            assertThat(timestamp, anyOf(equalTo(30L), equalTo(40L), equalTo(50L), equalTo(60L)));
            if (timestamp == 30L) {
                assertThat(dataPoints, equalTo(List.of(Tuple.tuple("my-counter", 4.0))));
            }
            if (timestamp == 40L) {
                assertThat(dataPoints, equalTo(List.of(Tuple.tuple("my-counter", 3.0))));
            }
            if (timestamp == 50L) {
                assertThat(dataPoints, equalTo(List.of(Tuple.tuple("my-counter", 5.0))));
            }
            if (timestamp == 60L) {
                assertThat(dataPoints, equalTo(List.of(Tuple.tuple("my-counter", 2.0))));
            }
        });
        producer.reset();
        assertThat(producer.downsampledValue, equalTo(Double.NaN));
        assertThat(producer.previousValue, equalTo(1.0));
        assertThat(producer.lastTimestamp, equalTo(-1L));
        producer.tsidReset();
        assertThat(producer.previousValue, equalTo(Double.NaN));
    }

    /**
     * Two buckets processed in reverse time order. Bucket #2 (t=50-70) has monotonically
     * increasing values 4, 5, 6 with no resets. Bucket #1 (t=10-40) has values 7, 8, 0, 2
     * with a reset at t=30. Both the last-before-reset value (8 at t=20) and the after-reset
     * value (0 at t=30) are added as there is no other bucket information for the same tsid.
     * Downsampled docs: 7, 4
     * Reset docs: 8 at 20, 0 at 30
     */
    public void testAggregateCounterDoesNotAddNotRedundantValue() throws IOException {
        CounterResetDataPoints resetDataPoints = new CounterResetDataPoints();
        NumericMetricFieldDownsampler.AggregateCounter producer = new NumericMetricFieldDownsampler.AggregateCounter("my-counter", null);
        // Bucket #2
        IntArrayList docIdBuffer = IntArrayList.from(6, 5, 4);
        long[] timeValues = new long[] { 70, 60, 50 };
        SortedNumericDoubleValues counterValues = createNumericValuesInstance(docIdBuffer, 6, 5, 4);
        producer.collect(counterValues, timeValues, docIdBuffer);
        producer.updateResetDataPoints(resetDataPoints);
        assertThat(producer.downsampledValue, equalTo(4.0));
        assertThat(resetDataPoints.isEmpty(), equalTo(true));
        producer.reset();

        // Bucket #1
        docIdBuffer = IntArrayList.from(3, 2, 1, 0);
        timeValues = new long[] { 40, 30, 20, 10 };
        counterValues = createNumericValuesInstance(docIdBuffer, 2, 0, 8, 7);
        producer.collect(counterValues, timeValues, docIdBuffer);
        resetDataPoints = new CounterResetDataPoints();
        producer.updateResetDataPoints(resetDataPoints);
        assertThat(producer.downsampledValue, equalTo(7.0));
        assertThat(resetDataPoints.countResetDocuments(), equalTo(1));
        resetDataPoints.processDataPoints((timestamp, dataPoints) -> {
            assertThat(timestamp, equalTo(20L));
            assertThat(dataPoints, equalTo(List.of(Tuple.tuple("my-counter", 8.0))));
        });
        producer.reset();
        assertThat(producer.downsampledValue, equalTo(Double.NaN));
        assertThat(producer.previousValue, equalTo(7.0));
        assertThat(producer.lastTimestamp, equalTo(-1L));
        producer.tsidReset();
        assertThat(producer.previousValue, equalTo(Double.NaN));
    }

    /**
     * Two buckets with 2 different tsids. Bucket tsid_2 has monotonically increasing values
     * with no resets. Bucket tsid_2 has values 7, 8, 0, 2 with a reset at t=30. Only the
     * last-before-reset value (8 at t=20) is stored as a reset data point; the after-reset
     * value (0 at t=30) is not added as it would be redundant.
     * Downsampled docs: 7, 4
     * Reset docs: 8 at 20, 0 at 30
     */
    public void testAggregateCounterResetsWhenTsidChanges() throws IOException {
        CounterResetDataPoints resetDataPoints = new CounterResetDataPoints();
        NumericMetricFieldDownsampler.AggregateCounter producer = new NumericMetricFieldDownsampler.AggregateCounter("my-counter", null);
        // Bucket tsid_2
        IntArrayList docIdBuffer = IntArrayList.from(6, 5, 4);
        long[] timeValues = new long[] { 40, 20, 10 };
        SortedNumericDoubleValues counterValues = createNumericValuesInstance(docIdBuffer, 6, 5, 4);
        producer.collect(counterValues, timeValues, docIdBuffer);
        producer.updateResetDataPoints(resetDataPoints);
        assertThat(producer.downsampledValue, equalTo(4.0));
        assertThat(resetDataPoints.isEmpty(), equalTo(true));
        producer.tsidReset();

        // Bucket tsid_1
        docIdBuffer = IntArrayList.from(3, 2, 1, 0);
        timeValues = new long[] { 40, 30, 20, 10 };
        counterValues = createNumericValuesInstance(docIdBuffer, 2, 0, 8, 7);
        producer.collect(counterValues, timeValues, docIdBuffer);
        resetDataPoints = new CounterResetDataPoints();
        producer.updateResetDataPoints(resetDataPoints);
        assertThat(producer.downsampledValue, equalTo(7.0));
        assertThat(resetDataPoints.countResetDocuments(), equalTo(2));
        resetDataPoints.processDataPoints((timestamp, dataPoints) -> {
            assertThat(timestamp, anyOf(equalTo(20L), equalTo(30L)));
            if (timestamp == 20L) {
                assertThat(dataPoints, equalTo(List.of(Tuple.tuple("my-counter", 8.0))));
            }
            if (timestamp == 30L) {
                assertThat(dataPoints, equalTo(List.of(Tuple.tuple("my-counter", 0.0))));
            }
        });
        producer.reset();
        assertThat(producer.downsampledValue, equalTo(Double.NaN));
        assertThat(producer.previousValue, equalTo(7.0));
        assertThat(producer.lastTimestamp, equalTo(-1L));
        producer.tsidReset();
        assertThat(producer.previousValue, equalTo(Double.NaN));
    }

    static SortedNumericDoubleValues createNumericValuesInstance(IntArrayList docIdBuffer, double... values) {
        return new SortedNumericDoubleValues() {

            final IntDoubleHashMap docIdToValue = IntDoubleHashMap.from(docIdBuffer.toArray(), values);

            int currentDocId = -1;

            @Override
            public boolean advanceExact(int target) {
                currentDocId = target;
                return docIdToValue.containsKey(target);
            }

            @Override
            public double nextValue() {
                return docIdToValue.get(currentDocId);
            }

            @Override
            public int docValueCount() {
                return 1;
            }
        };
    }
}
