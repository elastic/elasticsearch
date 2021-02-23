/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rollup.v2;

import org.elasticsearch.xpack.core.rollup.job.MetricConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class FieldMetricsProducer {
    final String fieldName;
    final List<Metric> metrics;

    FieldMetricsProducer(String fieldName, List<Metric> metrics) {
        this.fieldName = fieldName;
        this.metrics = metrics;
    }

    void reset() {
        for (Metric metric : metrics) {
            metric.reset();
        }
    }

    abstract static class Metric {
        final String name;

        protected Metric(String name) {
            this.name = name;
        }

        abstract void collect(double number);

        abstract Number get();

        abstract void reset();
    }

    private static class Max extends Metric {
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

    private static class Min extends Metric {
        private Double min;

        private Min() {
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

    private static class Sum extends Metric {
        private double sum = 0;

        private Sum() {
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

    private static class ValueCount extends Metric {
        private long count;

        private ValueCount() {
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

    static List<FieldMetricsProducer> buildMetrics(List<MetricConfig> metricsConfigs) {
        final List<FieldMetricsProducer> fields = new ArrayList<>();
        if (metricsConfigs != null) {
            for (MetricConfig metricConfig : metricsConfigs) {
                final List<String> normalizedMetrics = normalizeMetrics(metricConfig.getMetrics());
                final List<Metric> list = new ArrayList<>();
                if (normalizedMetrics.isEmpty() == false) {
                    for (String metricName : normalizedMetrics) {
                        switch (metricName) {
                            case "min":
                                list.add(new Min());
                                break;
                            case "max":
                                list.add(new Max());
                                break;
                            case "sum":
                                list.add(new Sum());
                                break;
                            case "value_count":
                                list.add(new ValueCount());
                                break;
                            default:
                                throw new IllegalArgumentException("Unsupported metric type [" + metricName + "]");
                        }
                    }
                    fields.add(new FieldMetricsProducer(metricConfig.getField(), Collections.unmodifiableList(list)));
                }
            }
        }
        return Collections.unmodifiableList(fields);
    }

    static List<String> normalizeMetrics(List<String> metrics) {
        List<String> newMetrics = new ArrayList<>(metrics);
        // avg = sum + value_count
        if (newMetrics.remove(MetricConfig.AVG.getPreferredName())) {
            if (newMetrics.contains(MetricConfig.VALUE_COUNT.getPreferredName()) == false) {
                newMetrics.add(MetricConfig.VALUE_COUNT.getPreferredName());
            }
            if (newMetrics.contains(MetricConfig.SUM.getPreferredName()) == false) {
                newMetrics.add(MetricConfig.SUM.getPreferredName());
            }
        }
        return newMetrics;
    }
}
