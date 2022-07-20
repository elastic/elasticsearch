/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rollup.v2;

import org.elasticsearch.index.query.SearchExecutionContext;

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
    public void collect(Object value) {
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
         * Abstract class that defines the how a label is computed.
         * @param name
         */
        protected Label(String name) {
            this.name = name;
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
    }

    /**
     * Produce a collection of label field producers.
     */
    static Map<String, LabelFieldProducer> buildLabelFieldProducers(SearchExecutionContext context, String[] labelFields) {
        final Map<String, LabelFieldProducer> fields = new LinkedHashMap<>();
        for (String field : labelFields) {
            LabelFieldProducer producer = new LabelLastValueFieldProducer(field);
            fields.put(field, producer);
        }
        return Collections.unmodifiableMap(fields);
    }
}
