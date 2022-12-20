/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.index.fielddata.FormattedDocValues;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.aggregatemetric.mapper.AggregateDoubleMetricFieldMapper;

import java.io.IOException;
import java.util.Set;

/**
 * Class that produces values for a label field.
 */
abstract class LabelFieldProducer extends AbstractRollupFieldProducer {

    LabelFieldProducer(String name) {
        super(name);
    }

    abstract static class Label {
        final String name;

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
        private final LastValueLabel label;

        LabelLastValueFieldProducer(String name) {
            super(name);
            this.label = new LastValueLabel(name);
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

    static class AggregateMetricFieldProducer extends LabelFieldProducer {
        private LastValueLabel[] labels;

        AggregateMetricFieldProducer(
            String name,
            Set<AggregateDoubleMetricFieldMapper.Metric> metrics
        ) {
            super(name);
            labels = new LastValueLabel[metrics.size()];
            int i = 0;
            for (var e : metrics) {
                labels[i++] = new LastValueLabel(e.name());
            }
        }

        @Override
        public void collect(FormattedDocValues docValues, int docId) throws IOException {
            if (isEmpty == false) {
                return;
            }

            if (docValues.advanceExact(docId) == false) {
                return;
            }

            // TODO
            int docValuesCount = docValues.docValueCount();
            assert docValuesCount > 0;
            if (docValuesCount == 1) {
                //label.collect(docValues.nextValue());
            } else {
                Object[] values = new Object[docValuesCount];
                for (int i = 0; i < docValuesCount; i++) {
                    values[i] = docValues.nextValue();
                }
                //label.collect(values);
            }
        }

        @Override
        public void write(XContentBuilder builder) throws IOException {
            if (isEmpty() == false) {
                builder.startObject(name());
                for (Label label : labels) {
                    if (label.get() != null) {
                        builder.field(label.name(), label.get());
                    }
                }
                builder.endObject();
            }
        }

        @Override
        public void reset() {
            for (LastValueLabel l : labels) {
                l.reset();
            }
            isEmpty = true;
        }
    }
}
