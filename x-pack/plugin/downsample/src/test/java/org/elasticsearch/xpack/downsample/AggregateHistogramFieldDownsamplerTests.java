/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.apache.lucene.internal.hppc.IntArrayList;
import org.apache.lucene.internal.hppc.LongArrayList;
import org.apache.lucene.search.DocIdSetIterator;
import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramCircuitBreaker;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramMerger;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.exponentialhistogram.fielddata.ExponentialHistogramValuesReader;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class AggregateHistogramFieldDownsamplerTests extends ESTestCase {

    /**
     * Delta temporality (default for histograms): all histograms are merged together.
     */
    public void testDeltaHistogramMergesValues() throws IOException {
        var producer = new ExponentialHistogramFieldDownsampler.AggregateHistogram("my-histogram", null);
        IntArrayList docIdBuffer = IntArrayList.from(2, 1, 0);
        LongArrayList timeValues = LongArrayList.from(30, 20, 10);

        ExponentialHistogram h1 = ExponentialHistogram.create(320, ExponentialHistogramCircuitBreaker.noop(), 1.0, 2.0, 3.0);
        ExponentialHistogram h2 = ExponentialHistogram.create(320, ExponentialHistogramCircuitBreaker.noop(), 4.0, 5.0);
        ExponentialHistogram h3 = ExponentialHistogram.create(320, ExponentialHistogramCircuitBreaker.noop(), 6.0);

        ExponentialHistogramValuesReader values = createHistogramValues(docIdBuffer, h1, h2, h3);
        producer.collect(values, timeValues, docIdBuffer, randomFrom(Temporality.DEFAULT, Temporality.DELTA));
        assertThat(producer.delegateCollector(), instanceOf(ExponentialHistogramFieldDownsampler.AggregateHistogram.DeltaCollector.class));

        ExponentialHistogram result = producer.downsampledValue();
        assertThat(result.valueCount(), equalTo(6L));
        assertThat(result.sum(), closeTo(21.0, 0.001));

        ResetDataPoints resetDataPoints = new ResetDataPoints();
        producer.updateResetDataPoints(resetDataPoints);
        assertThat(resetDataPoints.isEmpty(), equalTo(true));
    }

    /**
     * Cumulative temporality without resets: the oldest (last-iterated) value is kept.
     * Data is in descending time order, so the oldest value is the last one collected.
     */
    public void testCumulativeHistogramKeepsOldestValue() throws IOException {
        var producer = new ExponentialHistogramFieldDownsampler.AggregateHistogram("my-histogram", null);
        IntArrayList docIdBuffer = IntArrayList.from(2, 1, 0);
        LongArrayList timeValues = LongArrayList.from(30, 20, 10);

        // Cumulative: each snapshot includes all prior values
        ExponentialHistogram h10 = ExponentialHistogram.create(320, ExponentialHistogramCircuitBreaker.noop(), 1.0, 2.0, 3.0);
        ExponentialHistogram h20 = ExponentialHistogram.create(320, ExponentialHistogramCircuitBreaker.noop(), 1.0, 2.0);
        ExponentialHistogram h30 = ExponentialHistogram.create(320, ExponentialHistogramCircuitBreaker.noop(), 1.0);

        ExponentialHistogramValuesReader values = createHistogramValues(docIdBuffer, h10, h20, h30);
        producer.collect(values, timeValues, docIdBuffer, Temporality.CUMULATIVE);
        assertThat(
            producer.delegateCollector(),
            instanceOf(ExponentialHistogramFieldDownsampler.AggregateHistogram.CumulativeCollector.class)
        );

        // Oldest value (at t=10) should be preserved
        ExponentialHistogram result = producer.downsampledValue();
        assertThat(result.valueCount(), equalTo(1L));
        assertThat(result.sum(), closeTo(1.0, 0.001));

        ResetDataPoints resetDataPoints = new ResetDataPoints();
        producer.updateResetDataPoints(resetDataPoints);
        assertThat(resetDataPoints.isEmpty(), equalTo(true));

        producer.reset();
        assertThat(producer.downsampledValue(), nullValue());
        var collector = (ExponentialHistogramFieldDownsampler.AggregateHistogram.CumulativeCollector) producer.delegateCollector();
        assertThat(collector.lastTimestamp, equalTo(-1L));
        producer.tsidReset();
        assertThat(producer.isEmpty(), equalTo(true));
        assertThat(collector.previousValueHolder, nullValue());
        assertThat(producer.delegateCollector(), nullValue());
    }

    /**
     * Cumulative temporality with a reset: the oldest value is kept, and reset boundary
     * data points are recorded.
     */
    public void testCumulativeHistogramWithReset() throws IOException {
        var producer = new ExponentialHistogramFieldDownsampler.AggregateHistogram("my-histogram", null);

        // Data in descending time order:
        // t=40: count=2 (post-reset, growing)
        // t=30: count=1 (post-reset, smallest)
        // t=20: count=5 (pre-reset, largest)
        // t=10: count=3 (pre-reset, growing)
        IntArrayList docIdBuffer = IntArrayList.from(3, 2, 1, 0);
        LongArrayList timeValues = LongArrayList.from(40, 30, 20, 10);

        ExponentialHistogram h40 = ExponentialHistogram.create(320, ExponentialHistogramCircuitBreaker.noop(), 10.0, 20.0);
        ExponentialHistogram h30 = ExponentialHistogram.create(320, ExponentialHistogramCircuitBreaker.noop(), 10.0);
        ExponentialHistogram h20 = merge(
            h30,
            ExponentialHistogram.create(320, ExponentialHistogramCircuitBreaker.noop(), 1.0, 2.0, 3.0, 4.0)
        );
        ExponentialHistogram h10 = merge(h30, ExponentialHistogram.create(320, ExponentialHistogramCircuitBreaker.noop(), 1.0, 2.0));

        ExponentialHistogramValuesReader values = createHistogramValues(docIdBuffer, h40, h30, h20, h10);
        producer.collect(values, timeValues, docIdBuffer, Temporality.CUMULATIVE);

        // Oldest value (at t=10) should be the downsampled value
        ExponentialHistogram result = producer.downsampledValue();
        assertThat(result.valueCount(), equalTo(h10.valueCount()));

        ResetDataPoints resetDataPoints = new ResetDataPoints();
        producer.updateResetDataPoints(resetDataPoints);
        // Reset detected between t=30 and t=20 (going backward: h20.valueCount()=5 > h30.valueCount()=1)
        assertThat(resetDataPoints.countResetDocuments(), equalTo(2));
        resetDataPoints.processDataPoints((timestamp, dataPoints) -> {
            assertThat(timestamp, anyOf(equalTo(20L), equalTo(30L)));
            if (timestamp == 20L) {
                assertThat(dataPoints.size(), equalTo(1));
                var histogramValue = (ResetDataPoints.HistogramResetValue) dataPoints.get(0).v2();
                assertThat(histogramValue.value().valueCount(), equalTo(h20.valueCount()));
            }
            if (timestamp == 30L) {
                assertThat(dataPoints.size(), equalTo(1));
                var histogramValue = (ResetDataPoints.HistogramResetValue) dataPoints.get(0).v2();
                assertThat(histogramValue.value().valueCount(), equalTo(h30.valueCount()));
            }
        });

        producer.reset();
        assertThat(producer.downsampledValue(), nullValue());
        var collector = (ExponentialHistogramFieldDownsampler.AggregateHistogram.CumulativeCollector) producer.delegateCollector();
        assertThat(collector.lastTimestamp, equalTo(-1L));
        producer.tsidReset();
        assertThat(producer.isEmpty(), equalTo(true));
        assertThat(collector.previousValueHolder, nullValue());
        assertThat(producer.delegateCollector(), nullValue());
    }

    /**
     * Cumulative temporality across bucket boundaries: two buckets processed in reverse time order
     * (later bucket first). No resets should be detected for normal cumulative growth.
     */
    public void testCumulativeHistogramAcrossBuckets() throws IOException {
        var producer = new ExponentialHistogramFieldDownsampler.AggregateHistogram("my-histogram", null);

        // Bucket 2 (later, t=40-60): cumulative histograms with 4-6 values
        IntArrayList docIdBuffer = IntArrayList.from(5, 4, 3);
        LongArrayList timeValues = LongArrayList.from(60, 50, 40);

        ExponentialHistogram h60 = ExponentialHistogram.create(
            320,
            ExponentialHistogramCircuitBreaker.noop(),
            1.0,
            2.0,
            3.0,
            4.0,
            5.0,
            6.0
        );
        ExponentialHistogram h50 = ExponentialHistogram.create(320, ExponentialHistogramCircuitBreaker.noop(), 1.0, 2.0, 3.0, 4.0, 5.0);
        ExponentialHistogram h40 = ExponentialHistogram.create(320, ExponentialHistogramCircuitBreaker.noop(), 1.0, 2.0, 3.0, 4.0);

        ExponentialHistogramValuesReader values = createHistogramValues(docIdBuffer, h60, h50, h40);
        producer.collect(values, timeValues, docIdBuffer, Temporality.CUMULATIVE);

        assertThat(producer.downsampledValue().valueCount(), equalTo(4L));

        ResetDataPoints resetDataPoints = new ResetDataPoints();
        producer.updateResetDataPoints(resetDataPoints);
        assertThat(resetDataPoints.isEmpty(), equalTo(true));

        producer.reset();

        // Bucket 1 (earlier, t=10-30): cumulative histograms with 1-3 values
        docIdBuffer = IntArrayList.from(2, 1, 0);
        timeValues = LongArrayList.from(30, 20, 10);

        ExponentialHistogram h30 = ExponentialHistogram.create(320, ExponentialHistogramCircuitBreaker.noop(), 1.0, 2.0, 3.0);
        ExponentialHistogram h20 = ExponentialHistogram.create(320, ExponentialHistogramCircuitBreaker.noop(), 1.0, 2.0);
        ExponentialHistogram h10 = ExponentialHistogram.create(320, ExponentialHistogramCircuitBreaker.noop(), 1.0);

        values = createHistogramValues(docIdBuffer, h30, h20, h10);
        producer.collect(values, timeValues, docIdBuffer, Temporality.CUMULATIVE);

        assertThat(producer.downsampledValue().valueCount(), equalTo(1L));

        resetDataPoints = new ResetDataPoints();
        producer.updateResetDataPoints(resetDataPoints);
        assertThat(resetDataPoints.isEmpty(), equalTo(true));

        producer.reset();
        assertThat(producer.downsampledValue(), nullValue());
        var collector = (ExponentialHistogramFieldDownsampler.AggregateHistogram.CumulativeCollector) producer.delegateCollector();
        assertThat(collector.lastTimestamp, equalTo(-1L));
        producer.tsidReset();
        assertThat(producer.isEmpty(), equalTo(true));
        assertThat(collector.previousValueHolder, nullValue());
        assertThat(producer.delegateCollector(), nullValue());
    }

    /**
     * Mixed temporality across tsid changes: delta and cumulative tsids are handled independently.
     */
    public void testMixedTemporalityAcrossTsids() throws IOException {
        var producer = new ExponentialHistogramFieldDownsampler.AggregateHistogram("my-histogram", null);

        // tsid_1: delta — histograms are merged
        IntArrayList docIdBuffer = IntArrayList.from(1, 0);
        LongArrayList timeValues = LongArrayList.from(20, 10);
        ExponentialHistogram h1 = ExponentialHistogram.create(320, ExponentialHistogramCircuitBreaker.noop(), 1.0, 2.0);
        ExponentialHistogram h2 = ExponentialHistogram.create(320, ExponentialHistogramCircuitBreaker.noop(), 3.0);
        ExponentialHistogramValuesReader values = createHistogramValues(docIdBuffer, h1, h2);
        producer.collect(values, timeValues, docIdBuffer, Temporality.DELTA);
        assertThat(producer.delegateCollector(), instanceOf(ExponentialHistogramFieldDownsampler.AggregateHistogram.DeltaCollector.class));
        assertThat(producer.downsampledValue().valueCount(), equalTo(3L));

        ResetDataPoints resetDataPoints = new ResetDataPoints();
        producer.updateResetDataPoints(resetDataPoints);
        assertThat(resetDataPoints.isEmpty(), equalTo(true));

        producer.tsidReset();
        assertThat(producer.isEmpty(), equalTo(true));
        assertThat(producer.delegateCollector(), nullValue());

        // tsid_2: cumulative — oldest value kept
        docIdBuffer = IntArrayList.from(3, 2);
        timeValues = LongArrayList.from(20, 10);
        ExponentialHistogram h3 = ExponentialHistogram.create(320, ExponentialHistogramCircuitBreaker.noop(), 1.0, 2.0, 3.0);
        ExponentialHistogram h4 = ExponentialHistogram.create(320, ExponentialHistogramCircuitBreaker.noop(), 1.0);
        values = createHistogramValues(docIdBuffer, h3, h4);
        producer.collect(values, timeValues, docIdBuffer, Temporality.CUMULATIVE);
        assertThat(
            producer.delegateCollector(),
            instanceOf(ExponentialHistogramFieldDownsampler.AggregateHistogram.CumulativeCollector.class)
        );
        assertThat(producer.downsampledValue().valueCount(), equalTo(1L));

        resetDataPoints = new ResetDataPoints();
        producer.updateResetDataPoints(resetDataPoints);
        assertThat(resetDataPoints.isEmpty(), equalTo(true));

        producer.tsidReset();
        assertThat(producer.isEmpty(), equalTo(true));
        assertThat(producer.delegateCollector(), nullValue());

        // tsid_3: delta again — fully independent
        docIdBuffer = IntArrayList.from(5, 4);
        timeValues = LongArrayList.from(20, 10);
        ExponentialHistogram h5 = ExponentialHistogram.create(320, ExponentialHistogramCircuitBreaker.noop(), 10.0);
        ExponentialHistogram h6 = ExponentialHistogram.create(320, ExponentialHistogramCircuitBreaker.noop(), 20.0);
        values = createHistogramValues(docIdBuffer, h5, h6);
        producer.collect(values, timeValues, docIdBuffer, Temporality.DELTA);
        assertThat(producer.delegateCollector(), instanceOf(ExponentialHistogramFieldDownsampler.AggregateHistogram.DeltaCollector.class));
        assertThat(producer.downsampledValue().valueCount(), equalTo(2L));

        resetDataPoints = new ResetDataPoints();
        producer.updateResetDataPoints(resetDataPoints);
        assertThat(resetDataPoints.isEmpty(), equalTo(true));
    }

    /**
     * Delta temporality with an empty bucket between two non-empty buckets:
     * the empty bucket should produce a null downsampled value.
     */
    public void testDeltaHistogramEmptyBucketBetweenNonEmpty() throws IOException {
        var producer = new ExponentialHistogramFieldDownsampler.AggregateHistogram("my-histogram", null);

        // Bucket 1: two histograms merged
        IntArrayList docIdBuffer = IntArrayList.from(1, 0);
        LongArrayList timeValues = LongArrayList.from(20, 10);
        ExponentialHistogram h1 = ExponentialHistogram.create(320, ExponentialHistogramCircuitBreaker.noop(), 1.0, 2.0);
        ExponentialHistogram h2 = ExponentialHistogram.create(320, ExponentialHistogramCircuitBreaker.noop(), 3.0);
        ExponentialHistogramValuesReader values = createHistogramValues(docIdBuffer, h1, h2);
        producer.collect(values, timeValues, docIdBuffer, Temporality.DELTA);
        assertThat(producer.downsampledValue().valueCount(), equalTo(3L));

        producer.reset();

        // Bucket 2: empty (no documents in this time range)
        assertThat(producer.isEmpty(), equalTo(true));
        assertThat(producer.downsampledValue(), nullValue());

        producer.reset();

        // Bucket 3: one histogram
        docIdBuffer = IntArrayList.from(2);
        timeValues = LongArrayList.from(50);
        ExponentialHistogram h3 = ExponentialHistogram.create(320, ExponentialHistogramCircuitBreaker.noop(), 4.0, 5.0);
        values = createHistogramValues(docIdBuffer, h3);
        producer.collect(values, timeValues, docIdBuffer, Temporality.DELTA);
        assertThat(producer.downsampledValue().valueCount(), equalTo(2L));
    }

    public void testIsAggregateDownsamplerConsistentWithCreate() {
        assertThat(ExponentialHistogramFieldDownsampler.isAggregateDownsampler(DownsampleConfig.SamplingMethod.AGGREGATE), equalTo(true));
        assertThat(
            ExponentialHistogramFieldDownsampler.create("test", null, DownsampleConfig.SamplingMethod.AGGREGATE),
            instanceOf(ExponentialHistogramFieldDownsampler.AggregateHistogram.class)
        );

        assertThat(ExponentialHistogramFieldDownsampler.isAggregateDownsampler(DownsampleConfig.SamplingMethod.LAST_VALUE), equalTo(false));
        assertThat(
            ExponentialHistogramFieldDownsampler.create("test", null, DownsampleConfig.SamplingMethod.LAST_VALUE),
            instanceOf(ExponentialHistogramFieldDownsampler.LastValueProducer.class)
        );
    }

    /**
     * Default temporality for histograms should behave like delta (merge).
     */
    public void testDefaultTemporalityBehavesLikeDelta() throws IOException {
        var producer = new ExponentialHistogramFieldDownsampler.AggregateHistogram("my-histogram", null);
        IntArrayList docIdBuffer = IntArrayList.from(1, 0);
        LongArrayList timeValues = LongArrayList.from(20, 10);
        ExponentialHistogram h1 = ExponentialHistogram.create(320, ExponentialHistogramCircuitBreaker.noop(), 1.0, 2.0);
        ExponentialHistogram h2 = ExponentialHistogram.create(320, ExponentialHistogramCircuitBreaker.noop(), 3.0);
        ExponentialHistogramValuesReader values = createHistogramValues(docIdBuffer, h1, h2);
        producer.collect(values, timeValues, docIdBuffer, Temporality.DEFAULT);
        assertThat(producer.delegateCollector(), instanceOf(ExponentialHistogramFieldDownsampler.AggregateHistogram.DeltaCollector.class));

        ExponentialHistogram result = producer.downsampledValue();
        assertThat(result.valueCount(), equalTo(3L));
        assertThat(result.sum(), closeTo(6.0, 0.001));
    }

    private static ExponentialHistogram merge(ExponentialHistogram... histograms) {
        try (ExponentialHistogramMerger merger = ExponentialHistogramMerger.create(ExponentialHistogramCircuitBreaker.noop())) {
            for (ExponentialHistogram h : histograms) {
                merger.add(h);
            }
            return merger.get();
        }
    }

    static ExponentialHistogramValuesReader createHistogramValues(IntArrayList docIdBuffer, ExponentialHistogram... histograms) {
        assert docIdBuffer.size() == histograms.length;
        Map<Integer, ExponentialHistogram> docIdToHistogram = new HashMap<>();
        for (int i = 0; i < docIdBuffer.size(); i++) {
            docIdToHistogram.put(docIdBuffer.get(i), histograms[i]);
        }
        return new ExponentialHistogramValuesReader() {
            int currentDocId = -1;

            @Override
            public boolean advanceExact(int docId) {
                currentDocId = docId;
                return docIdToHistogram.containsKey(docId);
            }

            @Override
            public ExponentialHistogram histogramValue() {
                return docIdToHistogram.get(currentDocId);
            }

            @Override
            public long valuesCountValue() {
                return docIdToHistogram.get(currentDocId).valueCount();
            }

            @Override
            public double sumValue() {
                return docIdToHistogram.get(currentDocId).sum();
            }

            @Override
            public double minValue() {
                return docIdToHistogram.get(currentDocId).min();
            }

            @Override
            public double maxValue() {
                return docIdToHistogram.get(currentDocId).max();
            }

            @Override
            public DocIdSetIterator docIdIterator() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
