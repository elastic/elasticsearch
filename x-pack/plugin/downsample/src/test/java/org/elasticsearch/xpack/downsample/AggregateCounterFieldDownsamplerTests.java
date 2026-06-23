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
        CounterResetDataPoints resetDataPoints = new CounterResetDataPoints();
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
        CounterResetDataPoints resetDataPoints = new CounterResetDataPoints();
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
                assertThat(dataPoints, equalTo(List.of(Tuple.tuple("my-counter", 5.0))));
            }
            if (timestamp == 50L) {
                assertThat(dataPoints, equalTo(List.of(Tuple.tuple("my-counter", 16.0))));
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
        CounterResetDataPoints resetDataPoints = new CounterResetDataPoints();
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
            assertThat(dataPoints, equalTo(List.of(Tuple.tuple("my-counter", 0.0))));
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
        CounterResetDataPoints resetDataPoints = new CounterResetDataPoints();
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
        CounterResetDataPoints resetDataPoints = new CounterResetDataPoints();
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
        CounterResetDataPoints resetDataPoints = new CounterResetDataPoints();
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
        resetDataPoints = new CounterResetDataPoints();
        producer.updateResetDataPoints(resetDataPoints);
        assertThat(producer.downsampledValue(), equalTo(7.0));
        assertThat(resetDataPoints.countResetDocuments(), equalTo(1));
        resetDataPoints.processDataPoints((timestamp, dataPoints) -> {
            assertThat(timestamp, equalTo(20L));
            assertThat(dataPoints, equalTo(List.of(Tuple.tuple("my-counter", 8.0))));
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
        CounterResetDataPoints resetDataPoints = new CounterResetDataPoints();
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
        resetDataPoints = new CounterResetDataPoints();
        producer.updateResetDataPoints(resetDataPoints);
        assertThat(producer.downsampledValue(), equalTo(7.0));
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
     * Verifies that when the same {@link SortedNumericDoubleValues} instance is passed to two
     * consecutive collect calls — simulating a doc-ID buffer that fills up mid-leaf and flushes
     * twice within the same bucket — the iterator is resumed from its current position rather
     * than rewound.
     * <p>
     * With the DocIdSetIterator path the iterator advances forward-only; {@link
     * NumericMetricFieldDownsampler#resetLeafIteratorStateIfNeeded} reads {@code docID()} to
     * pick up from where the previous call left off. With the advanceExact path each doc is
     * probed independently, so resume is implicit.
     * <p>
     * Both paths must produce the same final downsampled value.
     */
    public void testIteratorResumedAcrossConsecutiveBuffers() throws IOException {
        var producer = new NumericMetricFieldDownsampler.AggregateCounter("my-counter", null);
        // Docs 0–5 all carry the counter; they belong to a single bucket whose buffer fills after
        // the first 3 docs, triggering two consecutive collect calls with the same doc-values instance.
        var allDocsWithValues = IntArrayList.from(0, 1, 2, 3, 4, 5);
        var counterValues = getIterator(allDocsWithValues, 6.0, 5.0, 4.0, 3.0, 2.0, 1.0);

        // First flush: docs 0–2 (most recent timestamps)
        producer.collect(counterValues, LongArrayList.from(60, 50, 40), IntArrayList.from(0, 1, 2), Temporality.CUMULATIVE);
        // Second flush: docs 3–5, same SortedNumericDoubleValues — iterator must resume, not restart
        producer.collect(counterValues, LongArrayList.from(30, 20, 10), IntArrayList.from(3, 4, 5), Temporality.CUMULATIVE);

        // Monotonically increasing counter (values 1→6 over ascending time), no resets.
        // Oldest doc (doc 5, t=10, v=1.0) is the downsampled value for the bucket.
        assertThat(producer.downsampledValue(), equalTo(1.0));
        var resetDataPoints = new CounterResetDataPoints();
        producer.updateResetDataPoints(resetDataPoints);
        assertThat(resetDataPoints.isEmpty(), equalTo(true));
    }

    /**
     * Verifies that docs absent from the counter field are correctly skipped regardless of
     * iteration strategy. For the DocIdSetIterator path the iterator jumps directly over missing
     * docs; for the advanceExact path {@code advanceExact} returns false for them.
     */
    public void testIteratorSkipsMissingDocs() throws IOException {
        var producer = new NumericMetricFieldDownsampler.AggregateCounter("my-counter", null);
        // Counter is only present on docs 1 and 3; docs 0, 2, and 4 are absent.
        var counterValues = getIterator(IntArrayList.from(1, 3), 4.0, 2.0);

        producer.collect(counterValues, LongArrayList.from(50, 40, 30, 20, 10), IntArrayList.from(0, 1, 2, 3, 4), Temporality.CUMULATIVE);

        // doc 1 (t=40, v=4.0) and doc 3 (t=20, v=2.0): counter grows 2→4, no reset.
        // Oldest contributing doc (doc 3, v=2.0) is the downsampled value.
        assertThat(producer.downsampledValue(), equalTo(2.0));
        var resetDataPoints = new CounterResetDataPoints();
        producer.updateResetDataPoints(resetDataPoints);
        assertThat(resetDataPoints.isEmpty(), equalTo(true));
    }

    /**
     * Verifies that once the DocIdSetIterator is exhausted during a collect call (the counter
     * field ends before the buffer does), a subsequent call with the same
     * {@link SortedNumericDoubleValues} for higher doc IDs is a no-op: the exhaustion flag is
     * preserved across calls and the downsampled value remains unchanged.
     * <p>
     * Skipped for the advanceExact path, which has no shared exhaustion state.
     */
    public void testExhaustedIteratorSkipsSubsequentCall() throws IOException {
        assumeTrue("only relevant for the DocIdSetIterator path", docValuesType == DocValuesType.WITH_ITERATOR);

        var producer = new NumericMetricFieldDownsampler.AggregateCounter("my-counter", null);
        // Counter covers docs 0–2 only; doc 3 is absent.
        var counterValues = getIterator(IntArrayList.from(0, 1, 2), 3.0, 2.0, 1.0);

        // First call: buffer 0–3. Docs 0–2 are collected; advancing past doc 2 to reach doc 3
        // exhausts the iterator (returns NO_MORE_DOCS).
        producer.collect(counterValues, LongArrayList.from(40, 30, 20, 10), IntArrayList.from(0, 1, 2, 3), Temporality.CUMULATIVE);
        assertThat(producer.downsampledValue(), equalTo(1.0));

        // Second call: same SortedNumericDoubleValues, docs 4–5. The iterator is already
        // exhausted so this call must return immediately without issuing any advance call
        // or changing the downsampled value.
        producer.collect(counterValues, LongArrayList.from(80, 70), IntArrayList.from(4, 5), Temporality.CUMULATIVE);

        assertThat(producer.downsampledValue(), equalTo(1.0));
        var resetDataPoints = new CounterResetDataPoints();
        producer.updateResetDataPoints(resetDataPoints);
        assertThat(resetDataPoints.isEmpty(), equalTo(true));
    }

    /**
     * Delta temporality: values represent increments and are summed within a bucket.
     * No reset data points are produced regardless of value patterns.
     */
    public void testDeltaCounterSumsValues() throws IOException {
        CounterResetDataPoints resetDataPoints = new CounterResetDataPoints();
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
        resetDataPoints = new CounterResetDataPoints();
        producer.updateResetDataPoints(resetDataPoints);
        assertThat(producer.downsampledValue(), equalTo(20.0));
        assertThat(resetDataPoints.isEmpty(), equalTo(true));
    }

    /**
     * Mixed temporality across tsid changes: delta and cumulative tsids are handled independently.
     */
    public void testDeltaCounterWithTsidChange() throws IOException {
        NumericMetricFieldDownsampler.AggregateCounter producer = new NumericMetricFieldDownsampler.AggregateCounter("my-counter", null);
        CounterResetDataPoints resetDataPoints;

        // tsid_1: delta — values are summed
        IntArrayList docIdBuffer = IntArrayList.from(0, 1, 2);
        LongArrayList timeValues = LongArrayList.from(30, 20, 10);
        SortedNumericDoubleValues counterValues = getIterator(docIdBuffer, 5, 3, 2);
        producer.collect(counterValues, timeValues, docIdBuffer, Temporality.DELTA);
        assertThat(producer.downsampledValue(), equalTo(10.0));
        resetDataPoints = new CounterResetDataPoints();
        producer.updateResetDataPoints(resetDataPoints);
        assertThat(resetDataPoints.isEmpty(), equalTo(true));
        assertThat(producer.delegateCollector(), instanceOf(NumericMetricFieldDownsampler.AggregateCounter.DeltaCollector.class));
        producer.tsidReset();
        assertThat(producer.delegateCollector(), nullValue());

        // tsid_2: delta — starts fresh, values are summed
        docIdBuffer = IntArrayList.from(3, 4, 5);
        timeValues = LongArrayList.from(30, 20, 10);
        counterValues = getIterator(docIdBuffer, 100, 200, 300);
        producer.collect(counterValues, timeValues, docIdBuffer, Temporality.DELTA);
        assertThat(producer.delegateCollector(), instanceOf(NumericMetricFieldDownsampler.AggregateCounter.DeltaCollector.class));
        assertThat(producer.downsampledValue(), equalTo(600.0));
        resetDataPoints = new CounterResetDataPoints();
        producer.updateResetDataPoints(resetDataPoints);
        assertThat(resetDataPoints.isEmpty(), equalTo(true));
        producer.tsidReset();
        assertThat(producer.delegateCollector(), nullValue());

        // tsid_3: cumulative with a reset — oldest value kept, reset data points produced
        docIdBuffer = IntArrayList.from(6, 7, 8, 9);
        timeValues = LongArrayList.from(40, 30, 20, 10);
        counterValues = getIterator(docIdBuffer, 2, 0, 8, 7);
        producer.collect(counterValues, timeValues, docIdBuffer, randomFrom(Temporality.DEFAULT, Temporality.CUMULATIVE));
        assertThat(producer.downsampledValue(), equalTo(7.0));
        resetDataPoints = new CounterResetDataPoints();
        producer.updateResetDataPoints(resetDataPoints);
        assertThat(resetDataPoints.countResetDocuments(), equalTo(2));
        assertThat(producer.delegateCollector(), instanceOf(NumericMetricFieldDownsampler.AggregateCounter.CumulativeCollector.class));
        producer.tsidReset();
        assertThat(producer.delegateCollector(), nullValue());

        // tsid_4: delta again — fully independent from the cumulative tsid
        docIdBuffer = IntArrayList.from(10, 11, 12);
        timeValues = LongArrayList.from(30, 20, 10);
        counterValues = getIterator(docIdBuffer, 7, 3, 1);
        producer.collect(counterValues, timeValues, docIdBuffer, Temporality.DELTA);
        assertThat(producer.downsampledValue(), equalTo(11.0));
        resetDataPoints = new CounterResetDataPoints();
        producer.updateResetDataPoints(resetDataPoints);
        assertThat(resetDataPoints.isEmpty(), equalTo(true));
        assertThat(producer.delegateCollector(), instanceOf(NumericMetricFieldDownsampler.AggregateCounter.DeltaCollector.class));
    }

}
