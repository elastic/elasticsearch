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
import org.elasticsearch.xpack.aggregatemetric.mapper.AggregateMetricDoubleFieldMapper;

import java.io.IOException;

/**
 * Class that collects all raw values for a metric field and computes its aggregate (downsampled)
 * values. Based on the supported metric types, the subclasses of this class compute values for
 * gauge and metric types.
 */
abstract sealed class MetricFieldProducer extends AbstractDownsampleFieldProducer {

    MetricFieldProducer(String name) {
        super(name);
    }

    @Override
    public void collect(FormattedDocValues docValues, IntArrayList buffer) throws IOException {
        assert false : "MetricFieldProducer does not support formatted doc values";
        throw new UnsupportedOperationException();
    }

    public abstract void collect(SortedNumericDoubleValues docValues, IntArrayList buffer) throws IOException;

    /**
     * {@link MetricFieldProducer} implementation for a counter metric field
     */
    static final class CounterMetricFieldProducer extends MetricFieldProducer {

        static final double NO_VALUE = Double.MIN_VALUE;

        double lastValue = NO_VALUE;

        CounterMetricFieldProducer(String name) {
            super(name);
        }

        @Override
        public void collect(SortedNumericDoubleValues docValues, IntArrayList docIdBuffer) throws IOException {
            if (isEmpty() == false) {
                return;
            }

            for (int i = 0; i < docIdBuffer.size(); i++) {
                int docId = docIdBuffer.get(i);
                if (docValues.advanceExact(docId)) {
                    isEmpty = false;
                    lastValue = docValues.nextValue();
                    return;
                }
            }
        }

        @Override
        public void reset() {
            isEmpty = true;
            lastValue = NO_VALUE;
        }

        @Override
        public void write(XContentBuilder builder) throws IOException {
            if (isEmpty() == false) {
                builder.field(name(), lastValue);
            }
        }
    }

    static final double MAX_NO_VALUE = -Double.MAX_VALUE;
    static final double MIN_NO_VALUE = Double.MAX_VALUE;

    /**
     * {@link MetricFieldProducer} implementation for a gauge metric field
     */
    static final class GaugeMetricFieldProducer extends MetricFieldProducer {

        double max = MAX_NO_VALUE;
        double min = MIN_NO_VALUE;
        final CompensatedSum sum = new CompensatedSum();
        long count;

        GaugeMetricFieldProducer(String name) {
            super(name);
        }

        @Override
        public void collect(SortedNumericDoubleValues docValues, IntArrayList docIdBuffer) throws IOException {
            for (int i = 0; i < docIdBuffer.size(); i++) {
                int docId = docIdBuffer.get(i);
                if (docValues.advanceExact(docId) == false) {
                    continue;
                }
                isEmpty = false;
                int docValuesCount = docValues.docValueCount();
                for (int j = 0; j < docValuesCount; j++) {
                    double value = docValues.nextValue();
                    this.max = Math.max(value, max);
                    this.min = Math.min(value, min);
                    sum.add(value);
                    count++;
                }
            }
        }

        @Override
        public void reset() {
            isEmpty = true;
            max = MAX_NO_VALUE;
            min = MIN_NO_VALUE;
            sum.reset(0, 0);
            count = 0;
        }

        @Override
        public void write(XContentBuilder builder) throws IOException {
            if (isEmpty() == false) {
                builder.startObject(name());
                builder.field("min", min);
                builder.field("max", max);
                builder.field("sum", sum.value());
                builder.field("value_count", count);
                builder.endObject();
            }
        }
    }

    // For downsampling downsampled indices:
    static final class AggregatedGaugeMetricFieldProducer extends MetricFieldProducer {

        final AggregateMetricDoubleFieldMapper.Metric metric;

        double max = MAX_NO_VALUE;
        double min = MIN_NO_VALUE;
        final CompensatedSum sum = new CompensatedSum();
        long count;

        AggregatedGaugeMetricFieldProducer(String name, AggregateMetricDoubleFieldMapper.Metric metric) {
            super(name);
            this.metric = metric;
        }

        @Override
        public void collect(SortedNumericDoubleValues docValues, IntArrayList docIdBuffer) throws IOException {
            for (int i = 0; i < docIdBuffer.size(); i++) {
                int docId = docIdBuffer.get(i);
                if (docValues.advanceExact(docId) == false) {
                    continue;
                }
                isEmpty = false;
                int docValuesCount = docValues.docValueCount();
                for (int j = 0; j < docValuesCount; j++) {
                    double value = docValues.nextValue();
                    switch (metric) {
                        case min -> min = Math.min(value, min);
                        case max -> max = Math.max(value, max);
                        case sum -> sum.add(value);
                        // This is the reason why we can't use GaugeMetricFieldProducer
                        // For downsampled indices aggregate metric double's value count field needs to be summed.
                        // (Note: not using CompensatedSum here should be ok given that value_count is mapped as long)
                        case value_count -> count += Math.round(value);
                    }
                }
            }
        }

        @Override
        public void reset() {
            isEmpty = true;
            max = MAX_NO_VALUE;
            min = MIN_NO_VALUE;
            sum.reset(0, 0);
            count = 0;
        }

        @Override
        public void write(XContentBuilder builder) throws IOException {
            if (isEmpty() == false) {
                builder.startObject(name());
                switch (metric) {
                    case min -> builder.field("min", min);
                    case max -> builder.field("max", max);
                    case sum -> builder.field("sum", sum.value());
                    case value_count -> builder.field("value_count", count);
                }
                builder.endObject();
            }
        }
    }
}
