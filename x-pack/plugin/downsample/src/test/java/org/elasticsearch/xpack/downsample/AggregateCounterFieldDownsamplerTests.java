/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.internal.hppc.IntArrayList;
import org.apache.lucene.internal.hppc.LongArrayList;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.downsample.SortedNumericDoubleValuesTestUtils.DocValuesType;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.downsample.SortedNumericDoubleValuesTestUtils.trackingWithDocIdIterator;
import static org.elasticsearch.xpack.downsample.SortedNumericDoubleValuesTestUtils.withDocIdIterator;
import static org.elasticsearch.xpack.downsample.SortedNumericDoubleValuesTestUtils.withoutDocIdIterator;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class AggregateCounterFieldDownsamplerTests extends ESTestCase {

    private final DocValuesType docValuesType;

    public AggregateCounterFieldDownsamplerTests(DocValuesType docValuesType) {
        this.docValuesType = docValuesType;
    }

    @ParametersFactory(shuffle = false)
    public static List<Object[]> iteratorTypes() {
        return List.of(new Object[] { DocValuesType.WITH_ITERATOR }, new Object[] { DocValuesType.WITHOUT_ITERATOR });
    }

    private SortedNumericDoubleValues getIterator(IntArrayList docIdsWithValues, double... values) {
        return switch (docValuesType) {
            case WITH_ITERATOR -> withDocIdIterator(docIdsWithValues, values);
            case WITHOUT_ITERATOR -> withoutDocIdIterator(docIdsWithValues, values);
        };
    }

    /**
     * Monotonically increasing counter with no resets within a single bucket.
     * Downsampled doc: 1
     */
    public void testAggregateCounter() throws IOException {
        ResetDataPoints resetDataPoints = new ResetDataPoints();
        NumericMetricFieldDownsampler.AggregateCounter producer = new NumericMetricFieldDownsampler.AggregateCounter("my-counter", null);
        IntArrayList docIdBuffer = IntArrayList.from(0, 1, 2, 3, 4, 5, 6);
        LongArrayList timeValues = LongArrayList.from(70, 60, 50, 40, 30, 20, 10);
        SortedNumericDoubleValues counterValues = getIterator(docIdBuffer, 64, 32, 16, 8, 4, 2, 1);
        producer.collect(counterValues, timeValues, docIdBuffer, randomFrom(Temporality.DEFAULT, Temporality.CUMULATIVE));
        assertThat(producer.delegateCollector(), instanceOf(NumericMetricFieldDownsampler.AggregateCounter.CumulativeCollector.class));
        producer.updateResetDataPoints(resetDataPoints);
        assertThat(producer.downsampledValue(), equalTo(1.0));
        assertThat(resetDataPoints.isEmpty(), equalTo(true));
        producer.reset();
        assertThat(producer.delegateCollector(), instanceOf(NumericMetricFieldDownsampler.AggregateCounter.CumulativeCollector.class));
        NumericMetricFieldDownsampler.AggregateCounter.CumulativeCollector collector =
            (NumericMetricFieldDownsampler.AggregateCounter.CumulativeCollector) producer.delegateCollector();
        assertThat(collector.downsampledValue(), equalTo(Double.NaN));
        assertThat(collector.previousValue, equalTo(1.0));
        assertThat(collector.lastTimestamp, equalTo(-1L));
        assertThat(producer.isDone(), equalTo(false));
        producer.tsidReset();
        assertThat(collector.previousValue, equalTo(Double.NaN));
        assertThat(producer.delegateCollector(), nullValue());
    }

    /**
     * Single reset within a bucket. The last-before-reset value (16 at t=50) and the after-reset
     * value (5 at t=60) are both stored as reset data points.
     * Downsampled doc: 1
     * Reset docs: 16 at 50, 5 at 60
     */
    public void testAggregateCounterWithReset() throws IOException {
        ResetDataPoints resetDataPoints = new ResetDataPoints();
        NumericMetricFieldDownsampler.AggregateCounter producer = new NumericMetricFieldDownsampler.AggregateCounter("my-counter", null);
        IntArrayList docIdBuffer = IntArrayList.from(0, 1, 2, 3, 4, 5, 6);
        LongArrayList timeValues = LongArrayList.from(70, 60, 50, 40, 30, 20, 10);
        SortedNumericDoubleValues counterValues = getIterator(docIdBuffer, 8, 5, 16, 8, 4, 2, 1);
        producer.collect(counterValues, timeValues, docIdBuffer, randomFrom(Temporality.DEFAULT, Temporality.CUMULATIVE));
        producer.updateResetDataPoints(resetDataPoints);
        assertThat(producer.downsampledValue(), equalTo(1.0));
        assertThat(resetDataPoints.countResetDocuments(), equalTo(2));
        assertThat(producer.isDone(), equalTo(false));
        resetDataPoints.processDataPoints((timestamp, dataPoints) -> {
            assertThat(timestamp, anyOf(equalTo(60L), equalTo(50L)));
            if (timestamp == 60L) {
                assertThat(dataPoints, equalTo(List.of(Tuple.tuple("my-counter", new ResetDataPoints.CounterResetValue(5.0)))));
            }
            if (timestamp == 50L) {
                assertThat(dataPoints, equalTo(List.of(Tuple.tuple("my-counter", new ResetDataPoints.CounterResetValue(16.0)))));
            }
        });
        producer.reset();
        assertThat(producer.downsampledValue(), equalTo(Double.NaN));
        assertThat(producer.delegateCollector(), instanceOf(NumericMetricFieldDownsampler.AggregateCounter.CumulativeCollector.class));
        NumericMetricFieldDownsampler.AggregateCounter.CumulativeCollector collector =
            (NumericMetricFieldDownsampler.AggregateCounter.CumulativeCollector) producer.delegateCollector();
        assertThat(collector.previousValue, equalTo(1.0));
        assertThat(collector.lastTimestamp, equalTo(-1L));
        producer.tsidReset();
        assertThat(collector.previousValue, equalTo(Double.NaN));
        assertThat(producer.delegateCollector(), nullValue());
    }

    /**
     * Counter with a reset where the last-before-reset value (1) is also the earliest value in
     * the bucket and equals the downsampled value. Only the after-reset value (0 at t=20) is
     * stored as a reset data point; the before-reset value is not duplicated.
     * Downsampled doc: 1
     * Reset docs: 0 at 20
     */
    public void testAggregateCounterDoesNotDuplicateFirstValue() throws IOException {
        ResetDataPoints resetDataPoints = new ResetDataPoints();
        NumericMetricFieldDownsampler.AggregateCounter producer = new NumericMetricFieldDownsampler.AggregateCounter("my-counter", null);
        IntArrayList docIdBuffer = IntArrayList.from(0, 1, 2);
        LongArrayList timeValues = LongArrayList.from(30, 20, 10);
        SortedNumericDoubleValues counterValues = getIterator(docIdBuffer, 7, 0, 1);
        producer.collect(counterValues, timeValues, docIdBuffer, randomFrom(Temporality.DEFAULT, Temporality.CUMULATIVE));
        producer.updateResetDataPoints(resetDataPoints);
        assertThat(producer.downsampledValue(), equalTo(1.0));
        assertThat(resetDataPoints.countResetDocuments(), equalTo(1));
        resetDataPoints.processDataPoints((timestamp, dataPoints) -> {
            assertThat(timestamp, equalTo(20L));
            assertThat(dataPoints, equalTo(List.of(Tuple.tuple("my-counter", new ResetDataPoints.CounterResetValue(0.0)))));
        });
        producer.reset();
        assertThat(producer.downsampledValue(), equalTo(Double.NaN));
        assertThat(producer.delegateCollector(), instanceOf(NumericMetricFieldDownsampler.AggregateCounter.CumulativeCollector.class));
        NumericMetricFieldDownsampler.AggregateCounter.CumulativeCollector collector =
            (NumericMetricFieldDownsampler.AggregateCounter.CumulativeCollector) producer.delegateCollector();
        assertThat(collector.previousValue, equalTo(1.0));
        assertThat(collector.lastTimestamp, equalTo(-1L));
        producer.tsidReset();
        assertThat(collector.previousValue, equalTo(Double.NaN));
        assertThat(producer.delegateCollector(), nullValue());
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
        ResetDataPoints resetDataPoints = new ResetDataPoints();
        NumericMetricFieldDownsampler.AggregateCounter producer = new NumericMetricFieldDownsampler.AggregateCounter("my-counter", null);
        IntArrayList docIdBuffer = IntArrayList.from(0, 1, 2, 3, 4, 5, 6, 7);
        LongArrayList timeValues = LongArrayList.from(80, 70, 60, 50, 40, 30, 20, 10);
        SortedNumericDoubleValues counterValues = getIterator(docIdBuffer, 4, 2, 5, 3, 8, 4, 2, 1);
        producer.collect(counterValues, timeValues, docIdBuffer, randomFrom(Temporality.DEFAULT, Temporality.CUMULATIVE));
        producer.updateResetDataPoints(resetDataPoints);
        assertThat(producer.downsampledValue(), equalTo(1.0));
        assertThat(resetDataPoints.countResetDocuments(), equalTo(3));
        resetDataPoints.processDataPoints((timestamp, dataPoints) -> {
            assertThat(timestamp, anyOf(equalTo(40L), equalTo(60L), equalTo(70L)));
            if (timestamp == 40L) {
                assertThat(dataPoints, equalTo(List.of(Tuple.tuple("my-counter", new ResetDataPoints.CounterResetValue(8.0)))));
            }
            if (timestamp == 60L) {
                assertThat(dataPoints, equalTo(List.of(Tuple.tuple("my-counter", new ResetDataPoints.CounterResetValue(5.0)))));
            }
            if (timestamp == 70L) {
                assertThat(dataPoints, equalTo(List.of(Tuple.tuple("my-counter", new ResetDataPoints.CounterResetValue(2.0)))));
            }
        });
        producer.reset();
        assertThat(producer.downsampledValue(), equalTo(Double.NaN));
        assertThat(producer.delegateCollector(), instanceOf(NumericMetricFieldDownsampler.AggregateCounter.CumulativeCollector.class));
        NumericMetricFieldDownsampler.AggregateCounter.CumulativeCollector collector =
            (NumericMetricFieldDownsampler.AggregateCounter.CumulativeCollector) producer.delegateCollector();
        assertThat(collector.previousValue, equalTo(1.0));
        assertThat(collector.lastTimestamp, equalTo(-1L));
        producer.tsidReset();
        assertThat(collector.previousValue, equalTo(Double.NaN));
        assertThat(producer.delegateCollector(), nullValue());
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
        ResetDataPoints resetDataPoints = new ResetDataPoints();
        NumericMetricFieldDownsampler.AggregateCounter producer = new NumericMetricFieldDownsampler.AggregateCounter("my-counter", null);
        IntArrayList docIdBuffer = IntArrayList.from(0, 1, 2, 3, 4, 5, 6);
        LongArrayList timeValues = LongArrayList.from(70, 60, 50, 40, 30, 20, 10);
        SortedNumericDoubleValues counterValues = getIterator(docIdBuffer, 4, 2, 5, 3, 4, 2, 1);
        producer.collect(counterValues, timeValues, docIdBuffer, randomFrom(Temporality.DEFAULT, Temporality.CUMULATIVE));
        producer.updateResetDataPoints(resetDataPoints);
        assertThat(producer.downsampledValue(), equalTo(1.0));
        assertThat(resetDataPoints.countResetDocuments(), equalTo(4));
        resetDataPoints.processDataPoints((timestamp, dataPoints) -> {
            assertThat(timestamp, anyOf(equalTo(30L), equalTo(40L), equalTo(50L), equalTo(60L)));
            if (timestamp == 30L) {
                assertThat(dataPoints, equalTo(List.of(Tuple.tuple("my-counter", new ResetDataPoints.CounterResetValue(4.0)))));
            }
            if (timestamp == 40L) {
                assertThat(dataPoints, equalTo(List.of(Tuple.tuple("my-counter", new ResetDataPoints.CounterResetValue(3.0)))));
            }
            if (timestamp == 50L) {
                assertThat(dataPoints, equalTo(List.of(Tuple.tuple("my-counter", new ResetDataPoints.CounterResetValue(5.0)))));
            }
            if (timestamp == 60L) {
                assertThat(dataPoints, equalTo(List.of(Tuple.tuple("my-counter", new ResetDataPoints.CounterResetValue(2.0)))));
            }
        });
        producer.reset();
        assertThat(producer.downsampledValue(), equalTo(Double.NaN));
        assertThat(producer.delegateCollector(), instanceOf(NumericMetricFieldDownsampler.AggregateCounter.CumulativeCollector.class));
        NumericMetricFieldDownsampler.AggregateCounter.CumulativeCollector collector =
            (NumericMetricFieldDownsampler.AggregateCounter.CumulativeCollector) producer.delegateCollector();
        assertThat(collector.previousValue, equalTo(1.0));
        assertThat(collector.lastTimestamp, equalTo(-1L));
        producer.tsidReset();
        assertThat(collector.previousValue, equalTo(Double.NaN));
        assertThat(producer.delegateCollector(), nullValue());
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
        ResetDataPoints resetDataPoints = new ResetDataPoints();
        NumericMetricFieldDownsampler.AggregateCounter producer = new NumericMetricFieldDownsampler.AggregateCounter("my-counter", null);
        // Bucket #2
        IntArrayList docIdBuffer = IntArrayList.from(0, 1, 2);
        LongArrayList timeValues = LongArrayList.from(70, 60, 50);
        SortedNumericDoubleValues counterValues = getIterator(docIdBuffer, 6, 5, 4);
        producer.collect(counterValues, timeValues, docIdBuffer, randomFrom(Temporality.DEFAULT, Temporality.CUMULATIVE));
        producer.updateResetDataPoints(resetDataPoints);
        assertThat(producer.downsampledValue(), equalTo(4.0));
        assertThat(resetDataPoints.isEmpty(), equalTo(true));
        producer.reset();

        // Bucket #1
        docIdBuffer = IntArrayList.from(3, 4, 5, 6);
        timeValues = LongArrayList.from(40, 30, 20, 10);
        counterValues = getIterator(docIdBuffer, 2, 0, 8, 7);
        producer.collect(counterValues, timeValues, docIdBuffer, randomFrom(Temporality.DEFAULT, Temporality.CUMULATIVE));
        resetDataPoints = new ResetDataPoints();
        producer.updateResetDataPoints(resetDataPoints);
        assertThat(producer.downsampledValue(), equalTo(7.0));
        assertThat(resetDataPoints.countResetDocuments(), equalTo(1));
        resetDataPoints.processDataPoints((timestamp, dataPoints) -> {
            assertThat(timestamp, equalTo(20L));
            assertThat(dataPoints, equalTo(List.of(Tuple.tuple("my-counter", new ResetDataPoints.CounterResetValue(8.0)))));
        });
        producer.reset();
        assertThat(producer.downsampledValue(), equalTo(Double.NaN));
        assertThat(producer.delegateCollector(), instanceOf(NumericMetricFieldDownsampler.AggregateCounter.CumulativeCollector.class));
        NumericMetricFieldDownsampler.AggregateCounter.CumulativeCollector collector =
            (NumericMetricFieldDownsampler.AggregateCounter.CumulativeCollector) producer.delegateCollector();
        assertThat(collector.previousValue, equalTo(7.0));
        assertThat(collector.lastTimestamp, equalTo(-1L));
        producer.tsidReset();
        assertThat(collector.previousValue, equalTo(Double.NaN));
        assertThat(producer.delegateCollector(), nullValue());
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
        ResetDataPoints resetDataPoints = new ResetDataPoints();
        NumericMetricFieldDownsampler.AggregateCounter producer = new NumericMetricFieldDownsampler.AggregateCounter("my-counter", null);
        // Bucket tsid_2
        IntArrayList docIdBuffer = IntArrayList.from(0, 1, 2);
        LongArrayList timeValues = LongArrayList.from(40, 20, 10);
        SortedNumericDoubleValues counterValues = getIterator(docIdBuffer, 6, 5, 4);
        producer.collect(counterValues, timeValues, docIdBuffer, randomFrom(Temporality.DEFAULT, Temporality.CUMULATIVE));
        producer.updateResetDataPoints(resetDataPoints);
        assertThat(producer.downsampledValue(), equalTo(4.0));
        assertThat(resetDataPoints.isEmpty(), equalTo(true));
        producer.tsidReset();

        // Bucket tsid_1
        docIdBuffer = IntArrayList.from(3, 4, 5, 6);
        timeValues = LongArrayList.from(40, 30, 20, 10);
        counterValues = getIterator(docIdBuffer, 2, 0, 8, 7);
        producer.collect(counterValues, timeValues, docIdBuffer, randomFrom(Temporality.DEFAULT, Temporality.CUMULATIVE));
        resetDataPoints = new ResetDataPoints();
        producer.updateResetDataPoints(resetDataPoints);
        assertThat(producer.downsampledValue(), equalTo(7.0));
        assertThat(resetDataPoints.countResetDocuments(), equalTo(2));
        resetDataPoints.processDataPoints((timestamp, dataPoints) -> {
            assertThat(timestamp, anyOf(equalTo(20L), equalTo(30L)));
            if (timestamp == 20L) {
                assertThat(dataPoints, equalTo(List.of(Tuple.tuple("my-counter", new ResetDataPoints.CounterResetValue(8.0)))));
            }
            if (timestamp == 30L) {
                assertThat(dataPoints, equalTo(List.of(Tuple.tuple("my-counter", new ResetDataPoints.CounterResetValue(0.0)))));
            }
        });
        producer.reset();
        assertThat(producer.downsampledValue(), equalTo(Double.NaN));
        assertThat(producer.delegateCollector(), instanceOf(NumericMetricFieldDownsampler.AggregateCounter.CumulativeCollector.class));
        NumericMetricFieldDownsampler.AggregateCounter.CumulativeCollector collector =
            (NumericMetricFieldDownsampler.AggregateCounter.CumulativeCollector) producer.delegateCollector();
        assertThat(collector.previousValue, equalTo(7.0));
        assertThat(collector.lastTimestamp, equalTo(-1L));
        producer.tsidReset();
        assertThat(collector.previousValue, equalTo(Double.NaN));
        assertThat(producer.delegateCollector(), nullValue());
    }

    /**
     * Delta temporality: values represent increments and are summed within a bucket.
     * No reset data points are produced regardless of value patterns.
     */
    public void testDeltaCounterSumsValues() throws IOException {
        ResetDataPoints resetDataPoints = new ResetDataPoints();
        NumericMetricFieldDownsampler.AggregateCounter producer = new NumericMetricFieldDownsampler.AggregateCounter("my-counter", null);
        IntArrayList docIdBuffer = IntArrayList.from(0, 1, 2, 3, 4, 5, 6);
        LongArrayList timeValues = LongArrayList.from(70, 60, 50, 40, 30, 20, 10);
        // Values that would trigger reset detection in cumulative mode (5 > 3, 8 > 2), but delta just sums them
        SortedNumericDoubleValues counterValues = getIterator(docIdBuffer, 8, 5, 16, 8, 4, 2, 1);
        producer.collect(counterValues, timeValues, docIdBuffer, Temporality.DELTA);
        producer.updateResetDataPoints(resetDataPoints);
        assertThat(producer.downsampledValue(), equalTo(44.0));
        assertThat(resetDataPoints.isEmpty(), equalTo(true));

        // Reset and collect a second bucket
        producer.reset();
        assertThat(producer.downsampledValue(), equalTo(0.0));
        assertThat(producer.delegateCollector(), instanceOf(NumericMetricFieldDownsampler.AggregateCounter.DeltaCollector.class));
        docIdBuffer = IntArrayList.from(7, 8, 9);
        timeValues = LongArrayList.from(100, 90, 80);
        counterValues = getIterator(docIdBuffer, 3, 7, 10);
        producer.collect(counterValues, timeValues, docIdBuffer, Temporality.DELTA);
        resetDataPoints = new ResetDataPoints();
        producer.updateResetDataPoints(resetDataPoints);
        assertThat(producer.downsampledValue(), equalTo(20.0));
        assertThat(resetDataPoints.isEmpty(), equalTo(true));
    }

    /**
     * Mixed temporality across tsid changes: delta and cumulative tsids are handled independently.
     */
    public void testDeltaCounterWithTsidChange() throws IOException {
        NumericMetricFieldDownsampler.AggregateCounter producer = new NumericMetricFieldDownsampler.AggregateCounter("my-counter", null);
        ResetDataPoints resetDataPoints;

        // tsid_1: delta — values are summed
        IntArrayList docIdBuffer = IntArrayList.from(0, 1, 2);
        LongArrayList timeValues = LongArrayList.from(30, 20, 10);
        SortedNumericDoubleValues counterValues = getIterator(docIdBuffer, 5, 3, 2);
        producer.collect(counterValues, timeValues, docIdBuffer, Temporality.DELTA);
        assertThat(producer.downsampledValue(), equalTo(10.0));
        resetDataPoints = new ResetDataPoints();
        producer.updateResetDataPoints(resetDataPoints);
        assertThat(resetDataPoints.isEmpty(), equalTo(true));
        assertThat(producer.delegateCollector(), instanceOf(NumericMetricFieldDownsampler.AggregateCounter.DeltaCollector.class));
        producer.tsidReset();
        assertThat(producer.isEmpty(), equalTo(true));
        assertThat(producer.delegateCollector(), nullValue());

        // tsid_2: delta — starts fresh, values are summed
        docIdBuffer = IntArrayList.from(3, 4, 5);
        timeValues = LongArrayList.from(30, 20, 10);
        counterValues = getIterator(docIdBuffer, 100, 200, 300);
        producer.collect(counterValues, timeValues, docIdBuffer, Temporality.DELTA);
        assertThat(producer.delegateCollector(), instanceOf(NumericMetricFieldDownsampler.AggregateCounter.DeltaCollector.class));
        assertThat(producer.downsampledValue(), equalTo(600.0));
        resetDataPoints = new ResetDataPoints();
        producer.updateResetDataPoints(resetDataPoints);
        assertThat(resetDataPoints.isEmpty(), equalTo(true));
        producer.tsidReset();
        assertThat(producer.isEmpty(), equalTo(true));
        assertThat(producer.delegateCollector(), nullValue());

        // tsid_3: cumulative with a reset — oldest value kept, reset data points produced
        docIdBuffer = IntArrayList.from(6, 7, 8, 9);
        timeValues = LongArrayList.from(40, 30, 20, 10);
        counterValues = getIterator(docIdBuffer, 2, 0, 8, 7);
        producer.collect(counterValues, timeValues, docIdBuffer, randomFrom(Temporality.DEFAULT, Temporality.CUMULATIVE));
        assertThat(producer.downsampledValue(), equalTo(7.0));
        resetDataPoints = new ResetDataPoints();
        producer.updateResetDataPoints(resetDataPoints);
        assertThat(resetDataPoints.countResetDocuments(), equalTo(2));
        assertThat(producer.delegateCollector(), instanceOf(NumericMetricFieldDownsampler.AggregateCounter.CumulativeCollector.class));
        producer.tsidReset();
        assertThat(producer.isEmpty(), equalTo(true));
        assertThat(producer.delegateCollector(), nullValue());

        // tsid_4: delta again — fully independent from the cumulative tsid
        docIdBuffer = IntArrayList.from(10, 11, 12);
        timeValues = LongArrayList.from(30, 20, 10);
        counterValues = getIterator(docIdBuffer, 7, 3, 1);
        producer.collect(counterValues, timeValues, docIdBuffer, Temporality.DELTA);
        assertThat(producer.downsampledValue(), equalTo(11.0));
        resetDataPoints = new ResetDataPoints();
        producer.updateResetDataPoints(resetDataPoints);
        assertThat(resetDataPoints.isEmpty(), equalTo(true));
        assertThat(producer.delegateCollector(), instanceOf(NumericMetricFieldDownsampler.AggregateCounter.DeltaCollector.class));
    }

    /**
     * Only docs 1 and 4 in a six-doc buffer carry values; the rest are sparse.
     * The DocIdSetIterator skips the missing docs, collecting only the two that exist.
     * No reset is detected because the values increase monotonically going back in time
     * (8 at t=50, then 5 at t=20).
     * Downsampled value: 5.0 (oldest)
     * Reset docs: none
     */
    public void testCounterSkipsSparseDocsWithDocIdIterator() throws IOException {
        assumeTrue("relevant only to downsampling with doc id iterator", docValuesType == DocValuesType.WITH_ITERATOR);
        ResetDataPoints resetDataPoints = new ResetDataPoints();
        NumericMetricFieldDownsampler.AggregateCounter producer = new NumericMetricFieldDownsampler.AggregateCounter("my-counter", null);
        IntArrayList docIdBuffer = IntArrayList.from(0, 1, 2, 3, 4, 5);
        LongArrayList timeValues = LongArrayList.from(60, 50, 40, 30, 20, 10);
        SortedNumericDoubleValues counterValues = withDocIdIterator(IntArrayList.from(1, 4), 8.0, 5.0);
        producer.collect(counterValues, timeValues, docIdBuffer, Temporality.CUMULATIVE);
        producer.updateResetDataPoints(resetDataPoints);
        assertThat(producer.downsampledValue(), equalTo(5.0));
        assertThat(resetDataPoints.isEmpty(), equalTo(true));
    }

    /**
     * After a leaf's DocIdSetIterator is exhausted (no matching doc found in the first bucket),
     * subsequent collect calls with the same iterator instance are no-ops; the iterator is not
     * advanced again.
     */
    public void testCounterSkipsExhaustedLeafAfterBucketReset() throws IOException {
        assumeTrue("relevant only to downsampling with doc id iterator", docValuesType == DocValuesType.WITH_ITERATOR);
        NumericMetricFieldDownsampler.AggregateCounter producer = new NumericMetricFieldDownsampler.AggregateCounter("my-counter", null);
        var counterValues = trackingWithDocIdIterator(IntArrayList.from(1), 12.2);

        // The only doc with a value (doc 1) is below the buffer range [2, 3]; the iterator exhausts.
        producer.collect(counterValues, LongArrayList.from(30, 20), IntArrayList.from(2, 3), Temporality.CUMULATIVE);
        assertThat(producer.isEmpty(), equalTo(true));
        assertThat(counterValues.advanceCalls(), equalTo(1));
        assertThat(counterValues.advanceExactCalls(), equalTo(0));

        producer.reset();
        // Same exhausted iterator — should be skipped entirely with no new advance calls.
        producer.collect(counterValues, LongArrayList.from(50, 40), IntArrayList.from(4, 5), Temporality.CUMULATIVE);
        assertThat(producer.isEmpty(), equalTo(true));
        assertThat(counterValues.advanceCalls(), equalTo(1));
        assertThat(counterValues.advanceExactCalls(), equalTo(0));
    }

    /**
     * A new leaf iterator (different object instance) clears the exhaustion state, allowing
     * collection to proceed from the start of the new leaf.
     */
    public void testCounterClearsExhaustionForNewLeafIterator() throws IOException {
        assumeTrue("relevant only to downsampling with doc id iterator", docValuesType == DocValuesType.WITH_ITERATOR);
        NumericMetricFieldDownsampler.AggregateCounter producer = new NumericMetricFieldDownsampler.AggregateCounter("my-counter", null);
        var firstLeafValues = trackingWithDocIdIterator(IntArrayList.from(2), 12.2);
        producer.collect(firstLeafValues, LongArrayList.from(50, 40), IntArrayList.from(2, 3), Temporality.CUMULATIVE);
        assertThat(producer.isEmpty(), equalTo(false));
        assertThat(producer.isDone(), equalTo(false));
        assertThat(firstLeafValues.advanceCalls(), equalTo(2));
        assertThat(firstLeafValues.advanceExactCalls(), equalTo(0));

        // Continues processing the bucket on the next leaf and updates seen value
        var secondLeafValues = trackingWithDocIdIterator(IntArrayList.from(4), 55.0);
        producer.collect(secondLeafValues, LongArrayList.from(30, 10), IntArrayList.from(4, 5), Temporality.CUMULATIVE);
        assertThat(producer.isEmpty(), equalTo(false));
        assertThat(producer.downsampledValue(), equalTo(55.0));
        assertThat(secondLeafValues.advanceCalls(), equalTo(2));
        assertThat(secondLeafValues.advanceExactCalls(), equalTo(0));
    }

}
