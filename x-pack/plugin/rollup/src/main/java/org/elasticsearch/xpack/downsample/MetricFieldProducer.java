/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.index.fielddata.FormattedDocValues;
import org.elasticsearch.search.aggregations.metrics.CompensatedSum;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Class that collects all raw values for a metric field and computes its aggregate (downsampled)
 * values. Based on the supported metric types, the subclasses of this class compute values for
 * gauge and metric types.
 */
abstract class MetricFieldProducer extends AbstractDownsampleFieldProducer {
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
        metrics().forEach(Metric::reset);
        isEmpty = true;
    }

    /** return the list of metrics that are computed for the field */
    public Collection<Metric> metrics() {
        return metrics;
    }

    /** Collect the value of a raw field and compute all downsampled metrics */
    void collect(Number value) {
        for (MetricFieldProducer.Metric metric : metrics()) {
            metric.collect(value);
        }
        isEmpty = false;
    }

    @Override
    public void collect(FormattedDocValues docValues, int docId) throws IOException {
        if (docValues.advanceExact(docId) == false) {
            return;
        }
        int docValuesCount = docValues.docValueCount();
        for (int i = 0; i < docValuesCount; i++) {
            Number num = (Number) docValues.nextValue();
            collect(num);
        }
    }

    abstract static class Metric {
        final String name;

        /**
         * Abstract class that defines how a metric is computed.
         * @param name the name of the metric as it will be output in the downsampled document
         */
        protected Metric(String name) {
            this.name = name;
        }

        public String name() {
            return name;
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

        Sum(String name) {
            super(name);
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

    /**
     * {@link MetricFieldProducer} implementation for a counter metric field
     */
    static class CounterMetricFieldProducer extends MetricFieldProducer {

        CounterMetricFieldProducer(String name) {
            super(name, Collections.singletonList(new LastValue()));
        }

        @Override
        public void collect(FormattedDocValues docValues, int docId) throws IOException {
            // Counter producers only collect the last_value. Since documents are
            // collected by descending timestamp order, the producer should only
            // process the first value for every tsid. So, it will only collect the
            // field if no value has been set before.
            if (isEmpty()) {
                super.collect(docValues, docId);
            }
        }

        public Object value() {
            assert metrics().size() == 1 : "Single value producers must have only one metric";
            return metrics().iterator().next().get();
        }

        @Override
        public void write(XContentBuilder builder) throws IOException {
            if (isEmpty() == false) {
                builder.field(name(), value());
            }
        }
    }

    /**
     * {@link MetricFieldProducer} implementation for a gauge metric field
     */
    static class GaugeMetricFieldProducer extends MetricFieldProducer {

        GaugeMetricFieldProducer(String name) {
            this(name, List.of(new Min(), new Max(), new Sum(), new ValueCount()));
        }

        GaugeMetricFieldProducer(String name, List<Metric> metrics) {
            super(name, metrics);
        }

        @Override
        public void write(XContentBuilder builder) throws IOException {
            if (isEmpty() == false) {
                builder.startObject(name());
                for (MetricFieldProducer.Metric metric : metrics()) {
                    if (metric.get() != null) {
                        builder.field(metric.name(), metric.get());
                    }
                }
                builder.endObject();
            }
        }
    }
}
