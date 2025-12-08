/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.apache.lucene.internal.hppc.IntArrayList;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.aggregations.metrics.CompensatedSum;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.aggregatemetric.mapper.AggregateMetricDoubleFieldMapper;

import java.io.IOException;
import java.util.Collection;

import static org.elasticsearch.xpack.downsample.NumericMetricFieldProducer.MAX_NO_VALUE;
import static org.elasticsearch.xpack.downsample.NumericMetricFieldProducer.MIN_NO_VALUE;

/**
 * A producer that can be used for downsampling ONLY a sub-metric of an aggregate metric double field. This is mainly used
 * when downsampling already downsampled indices.
 */
abstract class AggregateMetricDoubleFieldProducer extends AbstractDownsampleFieldProducer<SortedNumericDoubleValues> {

    protected final AggregateMetricDoubleFieldMapper.Metric metric;

    AggregateMetricDoubleFieldProducer(String name, AggregateMetricDoubleFieldMapper.Metric metric) {
        super(name);
        this.metric = metric;
    }

    static final class Aggregate extends AggregateMetricDoubleFieldProducer {

        private double max = MAX_NO_VALUE;
        private double min = MIN_NO_VALUE;
        private final CompensatedSum sum = new CompensatedSum();
        private long count;

        Aggregate(String name, AggregateMetricDoubleFieldMapper.Metric metric) {
            super(name, metric);
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
                switch (metric) {
                    case min -> builder.field("min", min);
                    case max -> builder.field("max", max);
                    case sum -> builder.field("sum", sum.value());
                    case value_count -> builder.field("value_count", count);
                }
            }
        }
    }

    static final class LastValue extends AggregateMetricDoubleFieldProducer {

        private final boolean supportsMultiValue;
        private Object lastValue = null;

        LastValue(String name, AggregateMetricDoubleFieldMapper.Metric metric, boolean supportsMultiValue) {
            super(name, metric);
            this.supportsMultiValue = supportsMultiValue;
        }

        @Override
        public void collect(SortedNumericDoubleValues docValues, IntArrayList docIdBuffer) throws IOException {
            if (isEmpty() == false) {
                return;
            }

            for (int i = 0; i < docIdBuffer.size(); i++) {
                int docId = docIdBuffer.get(i);
                if (docValues.advanceExact(docId) == false) {
                    continue;
                }
                int docValuesCount = docValues.docValueCount();
                assert docValuesCount > 0;
                isEmpty = false;
                if (docValuesCount == 1 || supportsMultiValue == false) {
                    lastValue = docValues.nextValue();
                } else {
                    var values = new Object[docValuesCount];
                    for (int j = 0; j < docValuesCount; j++) {
                        values[j] = docValues.nextValue();
                    }
                    lastValue = values;
                }
                // Only need to record one label value from one document, within in the same tsid-and-time-interval we only keep the first
                // with downsampling.
                return;
            }
        }

        @Override
        public void reset() {
            isEmpty = true;
            lastValue = null;
        }

        @Override
        public void write(XContentBuilder builder) throws IOException {
            if (isEmpty() == false) {
                builder.field(metric.name(), lastValue);
            }
        }
    }

    /**
     * We use a specialised serializer because we are combining all the available submetric producers.
     */
    static class Serializer implements DownsampleFieldSerializer {
        private final Collection<AbstractDownsampleFieldProducer<?>> producers;
        private final String name;

        /**
         * @param name the name of the aggregate_metric_double field as it will be serialized
         *             in the downsampled index
         * @param producers a collection of {@link AggregateMetricDoubleFieldProducer} instances with the subfields
         *                  of the aggregate_metric_double field.
         */
        Serializer(String name, Collection<AbstractDownsampleFieldProducer<?>> producers) {
            this.name = name;
            this.producers = producers;
        }

        @Override
        public void write(XContentBuilder builder) throws IOException {
            if (isEmpty()) {
                return;
            }

            builder.startObject(name);
            for (AbstractDownsampleFieldProducer<?> fieldProducer : producers) {
                assert name.equals(fieldProducer.name()) : "producer has a different name";
                if (fieldProducer.isEmpty()) {
                    continue;
                }
                if (fieldProducer instanceof AggregateMetricDoubleFieldProducer == false) {
                    throw new IllegalStateException(
                        "Unexpected field producer class: " + fieldProducer.getClass().getSimpleName() + " for " + name + " field"
                    );
                }
                fieldProducer.write(builder);
            }
            builder.endObject();
        }

        private boolean isEmpty() {
            for (AbstractDownsampleFieldProducer<?> p : producers) {
                if (p.isEmpty() == false) {
                    return false;
                }
            }
            return true;
        }
    }
}
