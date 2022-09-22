/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.aggregatemetric.mapper.AggregateDoubleMetricFieldMapper;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Class that produces values for a label field.
 */
abstract class LabelFieldProducer extends AbstractRollupFieldProducer<Object> {

    private final Label label;

    LabelFieldProducer(String name, Label label) {
        super(name);
        this.label = label;
    }

    public String name() {
        return name;
    }

    /** Collect the value of a raw field  */
    @Override
    public void collect(String field, Object value) {
        label.collect(value);
        isEmpty = false;
    }

    public Label label() {
        return this.label;
    }

    public void reset() {
        label.reset();
        isEmpty = true;
    }

    /**
     * Return the downsampled value as computed after collecting all raw values.
     * @return
     */
    public abstract Object value();

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
            super("last_value");
        }

        @Override
        void collect(Object value) {
            if (lastValue == null) {
                lastValue = value;
            }
        }

        @Override
        Object get() {
            return lastValue;
        }

        @Override
        void reset() {
            lastValue = null;
        }
    }

    /**
     * {@link LabelFieldProducer} implementation for a last value label
     */
    static class LabelLastValueFieldProducer extends LabelFieldProducer {

        LabelLastValueFieldProducer(String name) {
            super(name, new LastValueLabel());
        }

        @Override
        public Object value() {
            return label().get();
        }

        @Override
        public void write(XContentBuilder builder) throws IOException {
            if (isEmpty() == false) {
                builder.field(name(), value());
            }
        }
    }

    static class AggregateMetricFieldProducer extends LabelFieldProducer {

        private Map<String, Label> labelsByField = new LinkedHashMap<>();

        AggregateMetricFieldProducer(String name) {
            super(name, null);
        }

        public void addLabel(String field, Label label) {
            labelsByField.put(field, label);
        }

        @Override
        public void collect(String field, Object value) {
            labelsByField.get(field).collect(value);
            isEmpty = false;
        }

        @Override
        public void write(XContentBuilder builder) throws IOException {
            if (isEmpty() == false) {
                builder.startObject(name());
                for (Label label : labels()) {
                    if (label.get() != null) {
                        builder.field(label.name(), label.get());
                    }
                }
                builder.endObject();
            }
        }

        public Collection<Label> labels() {
            return labelsByField.values();
        }

        @Override
        public Object value() {
            return labelsByField;
        }

        @Override
        public void reset() {
            labels().forEach(Label::reset);
            isEmpty = true;
        }
    }

    /**
     * Create a collection of label field producers.
     */
    static Map<String, LabelFieldProducer> createLabelFieldProducers(SearchExecutionContext context, String[] labelFields) {
        final Map<String, LabelFieldProducer> fields = new LinkedHashMap<>();
        for (String field : labelFields) {
            MappedFieldType fieldType = context.getFieldType(field);
            assert fieldType != null : "Unknown field type for field: [" + field + "]";

            if (fieldType instanceof AggregateDoubleMetricFieldMapper.AggregateDoubleMetricFieldType aggMetricFieldType) {
                // If the field is an aggregate_metric_double field, we should use the correct subfields
                // for each aggregation. This is a rollup-of-rollup case
                AggregateMetricFieldProducer producer = new AggregateMetricFieldProducer.AggregateMetricFieldProducer(field);
                for (var e : aggMetricFieldType.getMetricFields().entrySet()) {
                    AggregateDoubleMetricFieldMapper.Metric metric = e.getKey();
                    NumberFieldMapper.NumberFieldType metricSubField = e.getValue();
                    producer.addLabel(metricSubField.name(), new LastValueLabel(metric.name()));
                    fields.put(metricSubField.name(), producer);
                }
            } else {
                LabelFieldProducer producer = new LabelLastValueFieldProducer(field);
                fields.put(field, producer);
            }
        }
        return Collections.unmodifiableMap(fields);
    }
}
