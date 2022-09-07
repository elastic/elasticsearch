/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.aggregations.metrics.CompensatedSum;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Class that collects all raw values for a metric field and computes its aggregate (downsampled)
 * values. Based on the supported metric types, the subclasses of this class compute values for
 * gauge and metric types.
 */
abstract class MetricFieldProducer extends AbstractRollupFieldProducer<Number> {
    /**
     * a list of metrics that will be computed for the field
     */
    private final List<Metric> metrics;

    MetricFieldProducer(String name, List<Metric> metrics) {
        super(name);
        this.metrics = metrics;
    }

    /**
     * Reset all values collected for the field
     */
    public void reset() {
        for (Metric metric : metrics) {
            metric.reset();
        }
        isEmpty = true;
    }

    public String name() {
        return name;
    }

    /** return the list of metrics that are computed for the field */
    public List<Metric> metrics() {
        return metrics;
    }

    /** Collect the value of a raw field and compute all downsampled metrics */
    @Override
    public void collect(Number value) {
        for (MetricFieldProducer.Metric metric : metrics) {
            metric.collect(value);
        }
        isEmpty = false;
    }

    /**
     * Return the downsampled value as computed after collecting all raw values.
     * @return
     */
    public abstract Object value();

    abstract static class Metric {
        final String name;

        /**
         * Abstract class that defines the how a metric is computed.
         * @param name
         */
        protected Metric(String name) {
            this.name = name;
        }

        abstract void collect(Number number);

        abstract Number get();

        abstract void reset();
    }

    /**
     * Metric implementation that computes the maximum of all values of a field
     */
    static class Max extends Metric {
        private Double max;

        Max() {
            super("max");
        }

        @Override
        void collect(Number value) {
            this.max = max != null ? Math.max(value.doubleValue(), max) : value.doubleValue();
        }

        @Override
        Number get() {
            return max;
        }

        @Override
        void reset() {
            max = null;
        }
    }

    /**
     * Metric implementation that computes the minimum of all values of a field
     */
    static class Min extends Metric {
        private Double min;

        Min() {
            super("min");
        }

        @Override
        void collect(Number value) {
            this.min = min != null ? Math.min(value.doubleValue(), min) : value.doubleValue();
        }

        @Override
        Number get() {
            return min;
        }

        @Override
        void reset() {
            min = null;
        }
    }

    /**
     * Metric implementation that computes the sum of all values of a field
     */
    static class Sum extends Metric {
        private final CompensatedSum kahanSummation = new CompensatedSum();

        Sum() {
            super("sum");
        }

        @Override
        void collect(Number value) {
            kahanSummation.add(value.doubleValue());
        }

        @Override
        Number get() {
            return kahanSummation.value();
        }

        @Override
        void reset() {
            kahanSummation.reset(0, 0);
        }
    }

    /**
     * Metric implementation that counts all values collected for a metric field
     */
    static class ValueCount extends Metric {
        private long count;

        ValueCount() {
            super("value_count");
        }

        @Override
        void collect(Number value) {
            count++;
        }

        @Override
        Number get() {
            return count;
        }

        @Override
        void reset() {
            count = 0;
        }
    }

    /**
     * Metric implementation that stores the last value over time for a metric. This implementation
     * assumes that field values are collected sorted by descending order by time. In this case,
     * it assumes that the last value of the time is the first value collected. Eventually,
     * the implementation of this class end up storing the first value it is empty and then
     * ignoring everything else.
     */
    static class LastValue extends Metric {
        private Number lastValue;

        LastValue() {
            super("last_value");
        }

        @Override
        void collect(Number value) {
            if (lastValue == null) {
                lastValue = value.doubleValue();
            }
        }

        @Override
        Number get() {
            return lastValue;
        }

        @Override
        void reset() {
            lastValue = null;
        }
    }

    /**
     * {@link MetricFieldProducer} implementation for a counter metric field
     */
    static class CounterMetricFieldProducer extends MetricFieldProducer {

        CounterMetricFieldProducer(String name) {
            super(name, List.of(new LastValue()));
        }

        @Override
        public Object value() {
            assert metrics().size() == 1 : "Counters have only one metric";
            return metrics().get(0).get();
        }
    }

    /**
     * {@link MetricFieldProducer} implementation for a gauge metric field
     */
    static class GaugeMetricFieldProducer extends MetricFieldProducer {

        GaugeMetricFieldProducer(String name) {
            super(name, List.of(new Min(), new Max(), new Sum(), new ValueCount()));
        }

        @Override
        public Object value() {
            Map<String, Object> metricValues = new HashMap<>();
            for (MetricFieldProducer.Metric metric : metrics()) {
                if (metric.get() != null) {
                    metricValues.put(metric.name, metric.get());
                }
            }
            return Collections.unmodifiableMap(metricValues);
        }
    }

    /**
     * Produce a collection of metric field producers based on the metric_type mapping parameter in the field
     * mapping.
     */
    static Map<String, MetricFieldProducer> buildMetricFieldProducers(SearchExecutionContext context, String[] metricFields) {
        final Map<String, MetricFieldProducer> fields = new LinkedHashMap<>();
        for (String field : metricFields) {
            MappedFieldType fieldType = context.getFieldType(field);
            assert fieldType.getMetricType() != null;

            MetricFieldProducer producer = switch (fieldType.getMetricType()) {
                case gauge -> new GaugeMetricFieldProducer(field);
                case counter -> new CounterMetricFieldProducer(field);
                default -> throw new IllegalArgumentException("Unsupported metric type [" + fieldType.getMetricType() + "]");
            };

            fields.put(field, producer);
        }
        return Collections.unmodifiableMap(fields);
    }
}
