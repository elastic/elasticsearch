/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.apache.lucene.internal.hppc.IntArrayList;
import org.elasticsearch.index.fielddata.FormattedDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.aggregations.metrics.CompensatedSum;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Class that collects all raw values for a metric field and computes its aggregate (downsampled)
 * values. Based on the supported metric types, the subclasses of this class compute values for
 * gauge and metric types.
 */
abstract sealed class MetricFieldProducer extends AbstractDownsampleFieldProducer {
    /**
     * a list of metrics that will be computed for the field
     */
    private final Metric[] metrics;

    MetricFieldProducer(String name, Metric... metrics) {
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

    /** return the list of metrics that are computed for the field */
    public Metric[] metrics() {
        return metrics;
    }

    /** Collect the value of a raw field and compute all downsampled metrics */
    void collect(double value) {
        for (MetricFieldProducer.Metric metric : metrics()) {
            metric.collect(value);
        }
        isEmpty = false;
    }

    @Override
    public void collect(FormattedDocValues docValues, IntArrayList docIdBuffer) throws IOException {
        assert false : "MetricFieldProducer does not support formatted doc values";
        throw new UnsupportedOperationException();
    }

    public void collect(SortedNumericDoubleValues docValues, IntArrayList docIdBuffer) throws IOException {
        for (int i = 0; i < docIdBuffer.size(); i++) {
            int docId = docIdBuffer.get(i);
            if (docValues.advanceExact(docId) == false) {
                continue;
            }
            int docValuesCount = docValues.docValueCount();
            for (int j = 0; j < docValuesCount; j++) {
                double num = docValues.nextValue();
                collect(num);
            }
        }
    }

    abstract static sealed class Metric {
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

        abstract void collect(double number);

        abstract double get();

        abstract void reset();
    }

    /**
     * Metric implementation that computes the maximum of all values of a field
     */
    static final class Max extends Metric {
        private double max = -Double.MAX_VALUE;

        Max() {
            super("max");
        }

        @Override
        void collect(double value) {
            this.max = Math.max(value, max);
        }

        @Override
        double get() {
            return max;
        }

        @Override
        void reset() {
            max = -Double.MAX_VALUE;
        }
    }

    /**
     * Metric implementation that computes the minimum of all values of a field
     */
    static final class Min extends Metric {
        private double min = Double.MAX_VALUE;

        Min() {
            super("min");
        }

        @Override
        void collect(double value) {
            this.min = Math.min(value, min);
        }

        @Override
        double get() {
            return min;
        }

        @Override
        void reset() {
            min = Double.MAX_VALUE;
        }
    }

    /**
     * Metric implementation that computes the sum of all values of a field
     */
    static final class Sum extends Metric {
        private final CompensatedSum kahanSummation = new CompensatedSum();

        Sum() {
            super("sum");
        }

        Sum(String name) {
            super(name);
        }

        @Override
        void collect(double value) {
            kahanSummation.add(value);
        }

        @Override
        double get() {
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
    static final class ValueCount extends Metric {
        private long count;

        ValueCount() {
            super("value_count");
        }

        @Override
        void collect(double value) {
            count++;
        }

        @Override
        double get() {
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
    static final class LastValue extends Metric {
        private double lastValue = Double.MIN_VALUE;

        LastValue() {
            super("last_value");
        }

        @Override
        void collect(double value) {
            if (lastValue == Double.MIN_VALUE) {
                lastValue = value;
            }
        }

        @Override
        double get() {
            return lastValue;
        }

        @Override
        void reset() {
            lastValue = Double.MIN_VALUE;
        }
    }

    /**
     * {@link MetricFieldProducer} implementation for a counter metric field
     */
    static final class CounterMetricFieldProducer extends MetricFieldProducer {

        CounterMetricFieldProducer(String name) {
            super(name, new LastValue());
        }

        @Override
        public void collect(SortedNumericDoubleValues docValues, IntArrayList docIdBuffer) throws IOException {
            // Counter producers only collect the last_value. Since documents are
            // collected by descending timestamp order, the producer should only
            // process the first value for every tsid. So, it will only collect the
            // field if no value has been set before.
            if (isEmpty()) {
                super.collect(docValues, docIdBuffer);
            }
        }

        public Object value() {
            assert metrics().length == 1 : "Single value producers must have only one metric";
            return metrics()[0].get();
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
    static final class GaugeMetricFieldProducer extends MetricFieldProducer {

        GaugeMetricFieldProducer(String name) {
            this(name, new Min(), new Max(), new Sum(), new ValueCount());
        }

        GaugeMetricFieldProducer(String name, Metric... metrics) {
            super(name, metrics);
        }

        @Override
        public void write(XContentBuilder builder) throws IOException {
            if (isEmpty() == false) {
                builder.startObject(name());
                for (MetricFieldProducer.Metric metric : metrics()) {
                    builder.field(metric.name(), metric.get());
                }
                builder.endObject();
            }
        }
    }
}
