/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.index.fielddata.FormattedDocValues;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.aggregatemetric.mapper.AggregateDoubleMetricFieldMapper;

import java.io.IOException;
import java.util.Map;

/**
 * Class that produces values for a label field.
 */
abstract class LabelFieldProducer extends AbstractRollupFieldProducer<Object> {

    LabelFieldProducer(String name) {
        super(name);
    }

    public String name() {
        return name;
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
        private final CheckedFunction<LeafReaderContext, FormattedDocValues, IOException> leaf;
        private Object lastValue;

        LastValueLabel(String name, CheckedFunction<LeafReaderContext, FormattedDocValues, IOException> leaf) {
            super(name);
            this.leaf = leaf;
        }

        LastValueLabel(CheckedFunction<LeafReaderContext, FormattedDocValues, IOException> leaf) {
            this("last_value", leaf);
        }

        LeafCollector leaf(LeafReaderContext ctx) throws IOException {
            final FormattedDocValues docValues = leaf.apply(ctx);
            return docId -> {
                if (lastValue != null) {
                    return;
                }
                if (docValues.advanceExact(docId) == false) {
                    return;
                }

                assert docValues.docValueCount() > 0;
                if (docValues.docValueCount() == 1) {
                    lastValue = docValues.nextValue();
                    return;
                }
                Object[] values = new Object[docValues.docValueCount()];
                for (int i = 0; i < values.length; i++) {
                    values[i] = docValues.nextValue();
                }
                lastValue = values;
            };
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
        private final LastValueLabel label;

        LabelLastValueFieldProducer(String name, CheckedFunction<LeafReaderContext, FormattedDocValues, IOException> leaf) {
            super(name);
            this.label = new LastValueLabel(name, leaf);
        }

        @Override
        public void write(XContentBuilder builder) throws IOException {
            if (isEmpty() == false) {
                builder.field(name(), label.get());
            }
        }

        @Override
        public LeafCollector leaf(LeafReaderContext ctx) throws IOException {
            return label.leaf(ctx);
        }

        @Override
        public void reset() {
            label.reset();
            isEmpty = true;
        }
    }

    static class AggregateMetricFieldProducer extends LabelFieldProducer {
        private LastValueLabel[] labels;

        AggregateMetricFieldProducer(String name, Map<String, CheckedFunction<LeafReaderContext, FormattedDocValues, IOException>> metrics) {
            super(name);
            labels = new LastValueLabel[metrics.size()];
            int i = 0;
            for (var e : metrics.entrySet()) {
                labels[i++] = new LastValueLabel(e.getKey(), e.getValue());
            }
        }

        @Override
        public LeafCollector leaf(LeafReaderContext ctx) throws IOException {
            LeafCollector[] labelLeaves = new LeafCollector[labels.length];
            for (int i = 0; i < labelLeaves.length; i++) {
                labelLeaves[i] = labels[i].leaf(ctx);
            }
            return docId -> {
                for (LeafCollector l : labelLeaves) {
                    l.collect(docId);
                }
            };
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
