/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.apache.lucene.internal.hppc.IntArrayList;
import org.apache.lucene.internal.hppc.IntDoubleHashMap;
import org.apache.lucene.internal.hppc.IntLongHashMap;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.fielddata.SortedNumericLongValues;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;

public class AggregateCounterFieldDownsamplerTests extends ESTestCase {

    public void testAggregateCounter() throws IOException {
        CounterResetDataPoints resetDataPoints = new CounterResetDataPoints();
        NumericMetricFieldDownsampler.AggregateCounterFieldDownsampler producer =
            new NumericMetricFieldDownsampler.AggregateCounterFieldDownsampler("my-counter", null, resetDataPoints);
        IntArrayList docIdBuffer = IntArrayList.from(6, 5, 4, 3, 2, 1, 0);
        SortedNumericLongValues timeValues = createTimestampValuesInstance(docIdBuffer, 70, 60, 50, 40, 30, 20, 10);
        SortedNumericDoubleValues counterValues = createNumericValuesInstance(docIdBuffer, 64, 32, 16, 8, 4, 2, 1);
        producer.collect(counterValues, timeValues, docIdBuffer);
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
        NumericMetricFieldDownsampler.AggregateCounterFieldDownsampler producer =
            new NumericMetricFieldDownsampler.AggregateCounterFieldDownsampler("my-counter", null, resetDataPoints);
        IntArrayList docIdBuffer = IntArrayList.from(6, 5, 4, 3, 2, 1, 0);
        SortedNumericLongValues timeValues = createTimestampValuesInstance(docIdBuffer, 70, 60, 50, 40, 30, 20, 10);
        SortedNumericDoubleValues counterValues = createNumericValuesInstance(docIdBuffer, 8, 5, 16, 8, 4, 2, 1);
        producer.collect(counterValues, timeValues, docIdBuffer);
        assertThat(producer.downsampledValue, equalTo(1.0));
        assertThat(resetDataPoints.isEmpty(), equalTo(false));
        resetDataPoints.processDataPoints((timestamp, dataPoints) -> {
            assertThat(timestamp, anyOf(equalTo(60L), equalTo(50L)));
            if (timestamp == 60L) {
                assertThat(dataPoints, equalTo(Map.of("my-counter", 5.0)));
            }
            if (timestamp == 50L) {
                assertThat(dataPoints, equalTo(Map.of("my-counter", 16.0)));
            }
        });
        producer.reset();
        assertThat(producer.downsampledValue, equalTo(Double.NaN));
        assertThat(producer.previousValue, equalTo(1.0));
        assertThat(producer.lastTimestamp, equalTo(-1L));
        producer.tsidReset();
        assertThat(producer.previousValue, equalTo(Double.NaN));
        resetDataPoints.reset();
        assertThat(resetDataPoints.isEmpty(), equalTo(true));
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

    static SortedNumericLongValues createTimestampValuesInstance(IntArrayList docIdBuffer, long... values) {
        return new SortedNumericLongValues() {

            final IntLongHashMap docIdToValue = IntLongHashMap.from(docIdBuffer.toArray(), values);

            int currentDocId = -1;

            @Override
            public boolean advanceExact(int target) {
                currentDocId = target;
                return docIdToValue.containsKey(target);
            }

            @Override
            public long nextValue() {
                return docIdToValue.get(currentDocId);
            }

            @Override
            public int docValueCount() {
                return 1;
            }
        };
    }

}
