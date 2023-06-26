/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.index.fielddata.FormattedDocValues;
import org.elasticsearch.index.fielddata.HistogramValue;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.aggregatemetric.mapper.AggregateDoubleMetricFieldMapper.Metric;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Class that produces values for a label field.
 */
abstract class LabelFieldProducer extends AbstractDownsampleFieldProducer {

    LabelFieldProducer(String name) {
        super(name);
    }

    abstract Label label();

    abstract static class Label {
        private final String name;

        /**
         * Abstract class that defines how a label is downsampled.
         * @param name the name of the field as it will be stored in the downsampled document
         */
        protected Label(String name) {
            this.name = name;
        }

        public String name() {
            return name;
        }

        abstract void collect(Object value);

        abstract Object get();

        abstract void reset();
    }

    /**
     * Label implementation that stores the last value over time for a label. This implementation
     * assumes that field values are collected sorted by descending order by time. In this case,
     * it assumes that the last value of the time is the first value collected. Eventually,
     * the implementation of this class end up storing the first value it is empty and then
     * ignoring everything else.
     */
    static class LastValueLabel extends Label {
        private Object lastValue;

        LastValueLabel(String name) {
            super(name);
        }

        LastValueLabel() {
            this("last_value");
        }

        @Override
        Object get() {
            return lastValue;
        }

        @Override
        void reset() {
            lastValue = null;
        }

        void collect(Object value) {
            if (lastValue == null) {
                lastValue = value;
            }
        }
    }

    /**
     * {@link LabelFieldProducer} implementation for a last value label
     */
    static class LabelLastValueFieldProducer extends LabelFieldProducer {
        protected final LastValueLabel label;

        LabelLastValueFieldProducer(String name, LastValueLabel label) {
            super(name);
            this.label = label;
        }

        LabelLastValueFieldProducer(String name) {
            this(name, new LastValueLabel());
        }

        @Override
        Label label() {
            return label;
        }

        @Override
        public void write(XContentBuilder builder) throws IOException {
            if (isEmpty() == false) {
                builder.field(name(), label.get());
            }
        }

        @Override
        public void collect(FormattedDocValues docValues, int docId) throws IOException {
            if (isEmpty() == false) {
                return;
            }
            if (docValues.advanceExact(docId) == false) {
                return;
            }

            int docValuesCount = docValues.docValueCount();
            assert docValuesCount > 0;
            isEmpty = false;
            if (docValuesCount == 1) {
                label.collect(docValues.nextValue());
            } else {
                Object[] values = new Object[docValuesCount];
                for (int i = 0; i < docValuesCount; i++) {
                    values[i] = docValues.nextValue();
                }
                label.collect(values);
            }
        }

        @Override
        public void reset() {
            label.reset();
            isEmpty = true;
        }
    }

    static class AggregateMetricFieldProducer extends LabelLastValueFieldProducer {

        AggregateMetricFieldProducer(String name, Metric metric) {
            super(name, new LastValueLabel(metric.name()));
        }
    }

    public static class HistogramLastLabelFieldProducer extends LabelLastValueFieldProducer {
        HistogramLastLabelFieldProducer(String name) {
            super(name);
        }

        @Override
        public void write(XContentBuilder builder) throws IOException {
            if (isEmpty() == false) {
                final HistogramValue histogramValue = (HistogramValue) label.get();
                final List<Double> values = new ArrayList<>();
                final List<Integer> counts = new ArrayList<>();
                while (histogramValue.next()) {
                    values.add(histogramValue.value());
                    counts.add(histogramValue.count());
                }
                builder.startObject(name()).field("counts", counts).field("values", values).endObject();
            }
        }
    }
}
