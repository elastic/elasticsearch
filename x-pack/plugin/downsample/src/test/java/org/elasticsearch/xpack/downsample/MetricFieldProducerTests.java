/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.apache.lucene.internal.hppc.IntArrayList;
import org.apache.lucene.internal.hppc.IntDoubleHashMap;
import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;

import static org.elasticsearch.xpack.downsample.LastValueFieldProducerTests.createValuesInstance;

public class MetricFieldProducerTests extends AggregatorTestCase {

    public void testMinCountMetric() throws IOException {
        var instance = MetricFieldProducer.createFieldProducerForGauge(randomAlphaOfLength(10), DownsampleConfig.SamplingMethod.AGGREGATE);
        var aggregateMetricFieldProducer = (MetricFieldProducer.AggregateGaugeMetricFieldProducer) instance;
        assertEquals(Double.MAX_VALUE, aggregateMetricFieldProducer.min, 0);
        var docIdBuffer = IntArrayList.from(0, 1, 2, 3);
        var numericValues = createNumericValuesInstance(docIdBuffer, 40, 5.5, 12.2, 55);
        aggregateMetricFieldProducer.collect(numericValues, docIdBuffer);
        assertEquals(5.5, aggregateMetricFieldProducer.min, 0);
        aggregateMetricFieldProducer.reset();
        assertEquals(Double.MAX_VALUE, aggregateMetricFieldProducer.min, 0);

        instance = MetricFieldProducer.createFieldProducerForGauge(randomAlphaOfLength(10), DownsampleConfig.SamplingMethod.LAST_VALUE);
        var lastValueProducer = (LastValueFieldProducer) instance;
        assertNull(lastValueProducer.lastValue());
        docIdBuffer = IntArrayList.from(0, 1, 2, 3);
        var values = createValuesInstance(docIdBuffer, new Double[] { 40D, 5.5, 12.2, 55D });
        lastValueProducer.collect(values, docIdBuffer);
        assertEquals(40.0, (double) lastValueProducer.lastValue(), 0);
        lastValueProducer.reset();
        assertNull(lastValueProducer.lastValue());
    }

    public void testMaxCountMetric() throws IOException {
        var instance = MetricFieldProducer.createFieldProducerForGauge(randomAlphaOfLength(10), DownsampleConfig.SamplingMethod.AGGREGATE);
        var aggregateMetricFieldProducer = (MetricFieldProducer.AggregateGaugeMetricFieldProducer) instance;
        assertEquals(-Double.MAX_VALUE, aggregateMetricFieldProducer.max, 0);
        var docIdBuffer = IntArrayList.from(0, 1, 2);
        var numericValues = createNumericValuesInstance(docIdBuffer, 5.5, 12.2, 55);
        aggregateMetricFieldProducer.collect(numericValues, docIdBuffer);
        assertEquals(55d, aggregateMetricFieldProducer.max, 0);
        aggregateMetricFieldProducer.reset();
        assertEquals(-Double.MAX_VALUE, aggregateMetricFieldProducer.max, 0);

        instance = MetricFieldProducer.createFieldProducerForGauge(randomAlphaOfLength(10), DownsampleConfig.SamplingMethod.LAST_VALUE);
        var lastValueProducer = (LastValueFieldProducer) instance;
        assertNull(lastValueProducer.lastValue());
        docIdBuffer = IntArrayList.from(0, 1, 2);
        var values = createValuesInstance(docIdBuffer, new Double[] { 5.5, 12.2, 55D });
        lastValueProducer.collect(values, docIdBuffer);
        assertEquals(5.5, (double) lastValueProducer.lastValue(), 0);
        lastValueProducer.reset();
        assertNull(lastValueProducer.lastValue());
    }

    public void testSumCountMetric() throws IOException {
        var instance = MetricFieldProducer.createFieldProducerForGauge(randomAlphaOfLength(10), DownsampleConfig.SamplingMethod.AGGREGATE);
        var aggregateMetricFieldProducer = (MetricFieldProducer.AggregateGaugeMetricFieldProducer) instance;
        assertEquals(0, aggregateMetricFieldProducer.sum.value(), 0);
        var docIdBuffer = IntArrayList.from(0, 1, 2);
        var numericValues = createNumericValuesInstance(docIdBuffer, 5.5, 12.2, 55);
        aggregateMetricFieldProducer.collect(numericValues, docIdBuffer);
        assertEquals(72.7, aggregateMetricFieldProducer.sum.value(), 0);
        aggregateMetricFieldProducer.reset();
        assertEquals(0, aggregateMetricFieldProducer.sum.value(), 0);

        instance = MetricFieldProducer.createFieldProducerForGauge(randomAlphaOfLength(10), DownsampleConfig.SamplingMethod.LAST_VALUE);
        var lastValueProducer = (LastValueFieldProducer) instance;
        assertNull(lastValueProducer.lastValue());
        docIdBuffer = IntArrayList.from(0, 1, 2);
        var values = createValuesInstance(docIdBuffer, new Double[] { 5.5, 12.2, 55D });
        lastValueProducer.collect(values, docIdBuffer);
        assertEquals(5.5, (double) lastValueProducer.lastValue(), 0);
        lastValueProducer.reset();
        assertNull(lastValueProducer.lastValue());
    }

    /**
     * Testing summation accuracy.
     * Tests stolen from SumAggregatorTests#testSummationAccuracy
     */
    public void testSummationAccuracy() throws IOException {
        var instance = new MetricFieldProducer.AggregateGaugeMetricFieldProducer(randomAlphaOfLength(10));
        assertEquals(0, instance.sum.value(), 0);
        var docIdBuffer = IntArrayList.from(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16);
        // Summing up a normal array and expect an accurate value
        var values = createNumericValuesInstance(
            docIdBuffer,
            0.1,
            0.2,
            0.3,
            0.4,
            0.5,
            0.6,
            0.7,
            0.8,
            0.9,
            1.0,
            1.1,
            1.2,
            1.3,
            1.4,
            1.5,
            1.6,
            1.7
        );
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
        values = createNumericValuesInstance(docIdBuffer, valueArray);
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
        values = createNumericValuesInstance(docIdBuffer, valueArray);
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
        values = createNumericValuesInstance(docIdBuffer, valueArray);
        instance.collect(values, docIdBuffer);
        assertEquals(Double.NEGATIVE_INFINITY, instance.sum.value(), 0d);
    }

    public void testValueCountMetric() throws IOException {
        var instance = MetricFieldProducer.createFieldProducerForGauge(randomAlphaOfLength(10), DownsampleConfig.SamplingMethod.AGGREGATE);
        var aggregateMetricFieldProducer = (MetricFieldProducer.AggregateGaugeMetricFieldProducer) instance;
        assertEquals(0, aggregateMetricFieldProducer.count);
        var docIdBuffer = IntArrayList.from(0, 1, 2);
        var numericValues = createNumericValuesInstance(docIdBuffer, 40, 30, 20);
        aggregateMetricFieldProducer.collect(numericValues, docIdBuffer);
        assertEquals(3L, aggregateMetricFieldProducer.count);
        instance.reset();
        assertEquals(0, aggregateMetricFieldProducer.count);

        instance = MetricFieldProducer.createFieldProducerForGauge(randomAlphaOfLength(10), DownsampleConfig.SamplingMethod.LAST_VALUE);
        var lastValueProducer = (LastValueFieldProducer) instance;
        assertNull(lastValueProducer.lastValue());
        docIdBuffer = IntArrayList.from(0, 1, 2);
        var values = createValuesInstance(docIdBuffer, new Integer[] { 40, 30, 20 });
        lastValueProducer.collect(values, docIdBuffer);
        assertEquals(40, (int) lastValueProducer.lastValue(), 0);
        lastValueProducer.reset();
        assertNull(lastValueProducer.lastValue());
    }

    public void testCounterMetricFieldProducer() throws IOException {
        final String field = "field";
        var producer = LastValueFieldProducer.createForMetric(field);
        assertTrue(producer.isEmpty());
        var docIdBuffer = IntArrayList.from(0, 1, 2);
        var valuesInstance = createValuesInstance(docIdBuffer, new Double[] { 55.0, 12.2, 5.5 });

        producer.collect(valuesInstance, docIdBuffer);

        assertFalse(producer.isEmpty());
        assertEquals(55.0, (double) producer.lastValue(), 0);
        assertEquals("field", producer.name());

        XContentBuilder builder = JsonXContent.contentBuilder().startObject();
        producer.write(builder);
        builder.endObject();
        assertEquals("{\"field\":55.0}", Strings.toString(builder));
    }

    public void testGaugeMetricFieldProducer() throws IOException {
        final String field = "field";
        MetricFieldProducer producer = new MetricFieldProducer.AggregateGaugeMetricFieldProducer(field);
        assertTrue(producer.isEmpty());
        var docIdBuffer = IntArrayList.from(0, 1, 2);
        var valuesInstance = createNumericValuesInstance(docIdBuffer, 55.0, 12.2, 5.5);
        producer.collect(valuesInstance, docIdBuffer);

        assertFalse(producer.isEmpty());

        XContentBuilder builder = JsonXContent.contentBuilder().startObject();
        producer.write(builder);
        builder.endObject();
        assertEquals("{\"field\":{\"min\":5.5,\"max\":55.0,\"sum\":72.7,\"value_count\":3}}", Strings.toString(builder));

        assertEquals(field, producer.name());
    }

    static SortedNumericDoubleValues createNumericValuesInstance(IntArrayList docIdBuffer, double... values) {
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
