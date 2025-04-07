/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.apache.lucene.internal.hppc.IntArrayList;
import org.apache.lucene.internal.hppc.IntDoubleHashMap;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;

public class MetricFieldProducerTests extends AggregatorTestCase {

    public void testMinCountMetric() throws IOException {
        var instance = new MetricFieldProducer.GaugeMetricFieldProducer(randomAlphaOfLength(10));
        assertEquals(Double.MAX_VALUE, instance.min, 0);
        var docIdBuffer = IntArrayList.from(0, 1, 2, 3);
        var values = createValuesInstance(docIdBuffer, 40, 5.5, 12.2, 55);
        instance.collect(values, docIdBuffer);
        assertEquals(5.5, instance.min, 0);
        instance.reset();
        assertEquals(Double.MAX_VALUE, instance.min, 0);
    }

    public void testMaxCountMetric() throws IOException {
        var instance = new MetricFieldProducer.GaugeMetricFieldProducer(randomAlphaOfLength(10));
        assertEquals(-Double.MAX_VALUE, instance.max, 0);
        var docIdBuffer = IntArrayList.from(0, 1, 2);
        var values = createValuesInstance(docIdBuffer, 5.5, 12.2, 55);
        instance.collect(values, docIdBuffer);
        assertEquals(55d, instance.max, 0);
        instance.reset();
        assertEquals(-Double.MAX_VALUE, instance.max, 0);
    }

    public void testSumCountMetric() throws IOException {
        var instance = new MetricFieldProducer.GaugeMetricFieldProducer(randomAlphaOfLength(10));
        assertEquals(0, instance.sum.value(), 0);
        var docIdBuffer = IntArrayList.from(0, 1, 2);
        var values = createValuesInstance(docIdBuffer, 5.5, 12.2, 55);
        instance.collect(values, docIdBuffer);
        assertEquals(72.7, instance.sum.value(), 0);
        instance.reset();
        assertEquals(0, instance.sum.value(), 0);
    }

    /**
     * Testing summation accuracy.
     * Tests stolen from SumAggregatorTests#testSummationAccuracy
     */
    public void testSummationAccuracy() throws IOException {
        var instance = new MetricFieldProducer.GaugeMetricFieldProducer(randomAlphaOfLength(10));
        assertEquals(0, instance.sum.value(), 0);
        var docIdBuffer = IntArrayList.from(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16);
        // Summing up a normal array and expect an accurate value
        var values = createValuesInstance(docIdBuffer, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7);
        instance.collect(values, docIdBuffer);
        assertEquals(15.3, instance.sum.value(), Double.MIN_NORMAL);

        // Summing up an array which contains NaN and infinities and expect a result same as naive summation
        instance.reset();
        int n = randomIntBetween(5, 10);
        docIdBuffer = new IntArrayList(n);
        double[] valueArray = new double[n];
        double sum = 0;
        for (int i = 0; i < n; i++) {
            docIdBuffer.add(i);
            double d = frequently()
                ? randomFrom(Double.NaN, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY)
                : randomDoubleBetween(Double.MIN_VALUE, Double.MAX_VALUE, true);
            valueArray[i] = d;
            sum += d;
        }
        values = createValuesInstance(docIdBuffer, valueArray);
        instance.collect(values, docIdBuffer);
        assertEquals(sum, instance.sum.value(), 1e-10);

        // Summing up some big double values and expect infinity result
        instance.reset();
        n = randomIntBetween(5, 10);
        docIdBuffer = new IntArrayList(n);
        valueArray = new double[n];
        for (int i = 0; i < n; i++) {
            docIdBuffer.add(i);
            valueArray[i] = Double.MAX_VALUE;
        }
        values = createValuesInstance(docIdBuffer, valueArray);
        instance.collect(values, docIdBuffer);
        assertEquals(Double.POSITIVE_INFINITY, instance.sum.value(), 0d);

        instance.reset();
        n = randomIntBetween(5, 10);
        docIdBuffer = new IntArrayList(n);
        valueArray = new double[n];
        for (int i = 0; i < n; i++) {
            docIdBuffer.add(i);
            valueArray[i] = -Double.MAX_VALUE;
        }
        values = createValuesInstance(docIdBuffer, valueArray);
        instance.collect(values, docIdBuffer);
        assertEquals(Double.NEGATIVE_INFINITY, instance.sum.value(), 0d);
    }

    public void testValueCountMetric() throws IOException {
        var instance = new MetricFieldProducer.GaugeMetricFieldProducer(randomAlphaOfLength(10));
        assertEquals(0, instance.count);
        var docIdBuffer = IntArrayList.from(0, 1, 2);
        var values = createValuesInstance(docIdBuffer, 40, 30, 20);
        instance.collect(values, docIdBuffer);
        assertEquals(3L, instance.count);
        instance.reset();
        assertEquals(0, instance.count);
    }

    public void testLastValueMetric() throws IOException {
        var instance = new MetricFieldProducer.CounterMetricFieldProducer(randomAlphaOfLength(10));
        assertEquals(Double.MIN_VALUE, instance.lastValue, 0);
        var docIdBuffer = IntArrayList.from(0, 1, 2);
        var values = createValuesInstance(docIdBuffer, 40, 30, 20);
        instance.collect(values, docIdBuffer);
        assertEquals(40, instance.lastValue, 0);
        instance.reset();
        assertEquals(Double.MIN_VALUE, instance.lastValue, 0);
    }

    public void testCounterMetricFieldProducer() throws IOException {
        final String field = "field";
        var producer = new MetricFieldProducer.CounterMetricFieldProducer(field);
        assertTrue(producer.isEmpty());
        var docIdBuffer = IntArrayList.from(0, 1, 2);
        var valuesInstance = createValuesInstance(docIdBuffer, 55.0, 12.2, 5.5);

        producer.collect(valuesInstance, docIdBuffer);

        assertFalse(producer.isEmpty());
        assertEquals(55.0, producer.lastValue, 0);
        assertEquals("field", producer.name());

        XContentBuilder builder = JsonXContent.contentBuilder().startObject();
        producer.write(builder);
        builder.endObject();
        assertEquals("{\"field\":55.0}", Strings.toString(builder));
    }

    public void testGaugeMetricFieldProducer() throws IOException {
        final String field = "field";
        MetricFieldProducer producer = new MetricFieldProducer.GaugeMetricFieldProducer(field);
        assertTrue(producer.isEmpty());
        var docIdBuffer = IntArrayList.from(0, 1, 2);
        var valuesInstance = createValuesInstance(docIdBuffer, 55.0, 12.2, 5.5);
        producer.collect(valuesInstance, docIdBuffer);

        assertFalse(producer.isEmpty());

        XContentBuilder builder = JsonXContent.contentBuilder().startObject();
        producer.write(builder);
        builder.endObject();
        assertEquals("{\"field\":{\"min\":5.5,\"max\":55.0,\"sum\":72.7,\"value_count\":3}}", Strings.toString(builder));

        assertEquals(field, producer.name());
    }

    static SortedNumericDoubleValues createValuesInstance(IntArrayList docIdBuffer, double... values) {
        return new SortedNumericDoubleValues() {

            final IntDoubleHashMap docIdToValue = IntDoubleHashMap.from(docIdBuffer.toArray(), values);

            int currentDocId = -1;

            @Override
            public boolean advanceExact(int target) throws IOException {
                currentDocId = target;
                return docIdToValue.containsKey(target);
            }

            @Override
            public double nextValue() throws IOException {
                return docIdToValue.get(currentDocId);
            }

            @Override
            public int docValueCount() {
                return 1;
            }
        };
    }
}
