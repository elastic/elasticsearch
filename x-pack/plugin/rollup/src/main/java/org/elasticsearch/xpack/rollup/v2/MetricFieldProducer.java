/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rollup.v2;

import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

abstract class MetricFieldProducer {
    private final String field;
    private final List<Metric> metrics;
    private boolean isEmpty = true;

    MetricFieldProducer(String field, List<Metric> metrics) {
        this.field = field;
        this.metrics = metrics;
    }

    void reset() {
        for (Metric metric : metrics) {
            metric.reset();
        }
        isEmpty = true;
    }

    public String field() {
        return field;
    }

    public List<Metric> metrics() {
        return metrics;
    }

    public void collectMetric(Double value) {
        for (MetricFieldProducer.Metric metric : metrics) {
            metric.collect(value);
        }
        isEmpty = false;
    }

    public boolean isEmpty() {
        return isEmpty;
    }

    public abstract Object value();

    abstract static class Metric {
        final String name;

        protected Metric(String name) {
            this.name = name;
        }

        abstract void collect(double number);

        abstract Number get();

        abstract void reset();
    }

    static class Max extends Metric {
        private Double max;

        Max() {
            super("max");
        }

        @Override
        void collect(double value) {
            this.max = max != null ? Math.max(value, max) : value;
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

    static class Min extends Metric {
        private Double min;

        Min() {
            super("min");
        }

        @Override
        void collect(double value) {
            this.min = min != null ? Math.min(value, min) : value;
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

    static class Sum extends Metric {
        private double sum = 0;

        Sum() {
            super("sum");
        }

        @Override
        void collect(double value) {
            // TODO: switch to Kahan summation ?
            this.sum += value;
        }

        @Override
        Number get() {
            return sum;
        }

        @Override
        void reset() {
            sum = 0;
        }
    }

    static class ValueCount extends Metric {
        private long count;

        ValueCount() {
            super("value_count");
        }

        @Override
        void collect(double value) {
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

    static class LastValue extends Metric {
        private Number lastValue;

        LastValue() {
            super("last_value");
        }

        @Override
        void collect(double value) {
            if (lastValue == null) {
                lastValue = value;
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

    static class CounterMetricFieldProducer extends MetricFieldProducer {

        CounterMetricFieldProducer(String field) {
            super(field, List.of(new LastValue()));

        }

        @Override
        public Object value() {
            assert metrics().size() == 1 : "Counters have only one metric";
            return metrics().get(0).get();
        }
    }

    static class GaugeMetricFieldProducer extends MetricFieldProducer {

        GaugeMetricFieldProducer(String field) {
            super(field, List.of(new Min(), new Max(), new Sum(), new ValueCount()));
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
