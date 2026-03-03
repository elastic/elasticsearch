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
