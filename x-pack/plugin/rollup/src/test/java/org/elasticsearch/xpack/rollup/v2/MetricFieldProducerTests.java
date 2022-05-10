/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rollup.v2;

import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesParams;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.aggregations.AggregatorTestCase;

import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;

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
        assertEquals(40.0, metric.get());
        metric.reset();
        assertNull(metric.get());
    }

    public void testCounterMetricFieldProducer() {
        MetricFieldProducer producer = new MetricFieldProducer.CounterMetricFieldProducer("field");
        assertTrue(producer.isEmpty());
        producer.collectMetric(55.0);
        producer.collectMetric(12.2);
        producer.collectMetric(5.5);

        assertFalse(producer.isEmpty());
        Object o = producer.value();
        assertEquals(55.0, o);
        assertEquals("field", producer.field());
    }

    public void testGaugeMetricFieldProducer() {
        MetricFieldProducer producer = new MetricFieldProducer.GaugeMetricFieldProducer("field");
        assertTrue(producer.isEmpty());
        producer.collectMetric(55.0);
        producer.collectMetric(12.2);
        producer.collectMetric(5.5);

        assertFalse(producer.isEmpty());
        Object o = producer.value();
        if (o instanceof Map) {
            Map<?, ?> m = (Map<?, ?>) o;
            assertMap(m, matchesMap().entry("min", 5.5).entry("max", 55.0).entry("value_count", 3L).entry("sum", 72.7));
            assertEquals(4, m.size());
        } else {
            fail("Value is not a Map");
        }
        assertEquals("field", producer.field());
    }

    public void testBuildMetricProducers() {
        final Map<String, MappedFieldType> provideMappedFieldType = Map.of(
            "gauge_field",
            new NumberFieldMapper.NumberFieldType(
                "gauge_field",
                NumberFieldMapper.NumberType.DOUBLE,
                true,
                true,
                true,
                true,
                null,
                emptyMap(),
                null,
                false,
                TimeSeriesParams.MetricType.gauge
            ),
            "counter_field",
            new NumberFieldMapper.NumberFieldType(
                "counter_field",
                NumberFieldMapper.NumberType.DOUBLE,
                true,
                true,
                true,
                true,
                null,
                emptyMap(),
                null,
                false,
                TimeSeriesParams.MetricType.counter
            )
        );

        IndexSettings settings = createIndexSettings();
        SearchExecutionContext searchExecutionContext = new SearchExecutionContext(
            0,
            0,
            settings,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            () -> 0L,
            null,
            null,
            () -> true,
            null,
            emptyMap()
        ) {
            @Override
            public MappedFieldType getFieldType(String name) {
                return provideMappedFieldType.get(name);
            }
        };

        Map<String, MetricFieldProducer> producers = MetricFieldProducer.buildMetricFieldProducers(
            searchExecutionContext,
            new String[] { "gauge_field", "counter_field" }
        );
        assertTrue(producers.get("gauge_field") instanceof MetricFieldProducer.GaugeMetricFieldProducer);
        assertTrue(producers.get("counter_field") instanceof MetricFieldProducer.CounterMetricFieldProducer);
    }
}
