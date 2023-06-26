/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.common.Strings;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;

public class MetricFieldProducerTests extends AggregatorTestCase {

    public void testMinCountMetric() {
        MetricFieldProducer.Metric metric = new MetricFieldProducer.Min();
        assertNull(metric.get());
        metric.collect(40);
        metric.collect(5.5);
        metric.collect(12.2);
        metric.collect(55);
        assertEquals(5.5, metric.get());
        metric.reset();
        assertNull(metric.get());
    }

    public void testMaxCountMetric() {
        MetricFieldProducer.Metric metric = new MetricFieldProducer.Max();
        assertNull(metric.get());
        metric.collect(5.5);
        metric.collect(12.2);
        metric.collect(55);
        assertEquals(55d, metric.get());
        metric.reset();
        assertNull(metric.get());
    }

    public void testSumCountMetric() {
        MetricFieldProducer.Metric metric = new MetricFieldProducer.Sum();
        assertEquals(0d, metric.get());
        metric.collect(5.5);
        metric.collect(12.2);
        metric.collect(55);
        assertEquals(72.7, metric.get());
        metric.reset();
        assertEquals(0d, metric.get());
    }

    /**
     * Testing summation accuracy.
     * Tests stolen from SumAggregatorTests#testSummationAccuracy
     */
    public void testSummationAccuracy() {
        MetricFieldProducer.Metric metric = new MetricFieldProducer.Sum();
        // Summing up a normal array and expect an accurate value
        double[] values = new double[] { 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7 };
        for (int i = 0; i < values.length; i++) {
            metric.collect(values[i]);
        }
        assertEquals(15.3, metric.get().doubleValue(), Double.MIN_NORMAL);

        // Summing up an array which contains NaN and infinities and expect a result same as naive summation
        metric.reset();
        int n = randomIntBetween(5, 10);
        double sum = 0;
        for (int i = 0; i < n; i++) {
            double d = frequently()
                ? randomFrom(Double.NaN, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY)
                : randomDoubleBetween(Double.MIN_VALUE, Double.MAX_VALUE, true);
            sum += d;
            metric.collect(d);
        }
        assertEquals(sum, metric.get().doubleValue(), 1e-10);

        // Summing up some big double values and expect infinity result
        metric.reset();
        n = randomIntBetween(5, 10);
        for (int i = 0; i < n; i++) {
            metric.collect(Double.MAX_VALUE);
        }
        assertEquals(Double.POSITIVE_INFINITY, metric.get().doubleValue(), 0d);

        metric.reset();
        for (int i = 0; i < n; i++) {
            metric.collect(-Double.MAX_VALUE);
        }
        assertEquals(Double.NEGATIVE_INFINITY, metric.get().doubleValue(), 0d);
    }

    public void testValueCountMetric() {
        MetricFieldProducer.Metric metric = new MetricFieldProducer.ValueCount();
        assertEquals(0L, metric.get());
        metric.collect(40);
        metric.collect(30);
        metric.collect(20);
        assertEquals(3L, metric.get());
        metric.reset();
        assertEquals(0L, metric.get());
    }

    public void testLastValueMetric() {
        MetricFieldProducer.Metric metric = new MetricFieldProducer.LastValue();
        assertNull(metric.get());
        metric.collect(40);
        metric.collect(30);
        metric.collect(20);
        assertEquals(40, metric.get());
        metric.reset();
        assertNull(metric.get());
    }

    public void testCounterMetricFieldProducer() throws IOException {
        final String field = "field";
        var producer = new MetricFieldProducer.CounterMetricFieldProducer(field);
        assertTrue(producer.isEmpty());
        producer.collect(55.0);
        producer.collect(12.2);
        producer.collect(5.5);

        assertFalse(producer.isEmpty());
        Object o = producer.value();
        assertEquals(55.0, o);
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
        producer.collect(55.0);
        producer.collect(12.2);
        producer.collect(5.5);

        assertFalse(producer.isEmpty());

        XContentBuilder builder = JsonXContent.contentBuilder().startObject();
        producer.write(builder);
        builder.endObject();
        assertEquals("{\"field\":{\"min\":5.5,\"max\":55.0,\"sum\":72.7,\"value_count\":3}}", Strings.toString(builder));

        assertEquals(field, producer.name());
    }
}
