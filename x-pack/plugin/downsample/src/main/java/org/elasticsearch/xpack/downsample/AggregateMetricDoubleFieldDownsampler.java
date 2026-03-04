/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.apache.lucene.internal.hppc.IntArrayList;
import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.aggregations.metrics.CompensatedSum;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.aggregatemetric.mapper.AggregateMetricDoubleFieldMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A downsampler that can be used for downsampling aggregate metric double fields whether is a metric or a label. We need a separate
 * downsampler for each a sub-metric of an aggregate metric double, this means to downsample an aggregate metric double we will need 4.
 * This is mainly used when downsampling already downsampled indices.
 */
abstract sealed class AggregateMetricDoubleFieldDownsampler extends NumericMetricFieldDownsampler {

    protected final AggregateMetricDoubleFieldMapper.Metric metric;

    AggregateMetricDoubleFieldDownsampler(String name, AggregateMetricDoubleFieldMapper.Metric metric, IndexFieldData<?> fieldData) {
        super(name, fieldData);
        this.metric = metric;
    }

    public static boolean supportsFieldType(MappedFieldType fieldType) {
        return AggregateMetricDoubleFieldMapper.CONTENT_TYPE.equals(fieldType.typeName());
    }

    static final class Aggregate extends AggregateMetricDoubleFieldDownsampler {

        private double max = MAX_NO_VALUE;
        private double min = MIN_NO_VALUE;
        private final CompensatedSum sum = new CompensatedSum();
        private long count;

        Aggregate(String name, AggregateMetricDoubleFieldMapper.Metric metric, IndexFieldData<?> fieldData) {
            super(name, metric, fieldData);
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

    /**
     * Important note: This class assumes that field values are collected and sorted by descending order by time
     */
    static final class LastValue extends AggregateMetricDoubleFieldDownsampler {

        private final boolean supportsMultiValue;
        private Object lastValue = null;

        LastValue(String name, AggregateMetricDoubleFieldMapper.Metric metric, IndexFieldData<?> fieldData, boolean supportsMultiValue) {
            super(name, metric, fieldData);
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
     * We use a specialised serializer because we are combining all the available submetric downsamplers.
     */
    static class Serializer implements DownsampleFieldSerializer {
        private final Collection<AbstractFieldDownsampler<?>> downsamplers;
        private final String name;

        /**
         * @param name the name of the aggregate_metric_double field as it will be serialized
         *             in the downsampled index
         * @param downsamplers a collection of {@link AggregateMetricDoubleFieldDownsampler} instances with the subfields
         *                  of the aggregate_metric_double field.
         */
        Serializer(String name, Collection<AbstractFieldDownsampler<?>> downsamplers) {
            this.name = name;
            this.downsamplers = downsamplers;
        }

        @Override
        public void write(XContentBuilder builder) throws IOException {
            if (isEmpty()) {
                return;
            }

            builder.startObject(name);
            for (AbstractFieldDownsampler<?> fieldDownsampler : downsamplers) {
                assert name.equals(fieldDownsampler.name()) : "downsampler has a different name";
                if (fieldDownsampler.isEmpty()) {
                    continue;
                }
                if (fieldDownsampler instanceof AggregateMetricDoubleFieldDownsampler == false) {
                    throw new IllegalStateException(
                        "Unexpected field downsampler class: " + fieldDownsampler.getClass().getSimpleName() + " for " + name + " field"
                    );
                }
                fieldDownsampler.write(builder);
            }
            builder.endObject();
        }

        private boolean isEmpty() {
            for (AbstractFieldDownsampler<?> d : downsamplers) {
                if (d.isEmpty() == false) {
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * For aggregate_metric_double fields we create separate fetchers for each sub-metric. This is usually a downsample-of-downsample case.
     */
    static List<AggregateMetricDoubleFieldDownsampler> create(
        SearchExecutionContext context,
        AggregateMetricDoubleFieldMapper.AggregateMetricDoubleFieldType aggMetricFieldType,
        DownsampleConfig.SamplingMethod samplingMethod
    ) {
        List<AggregateMetricDoubleFieldDownsampler> downsamplers = new ArrayList<>();
        // If the field is an aggregate_metric_double field, we should load all its subfields
        // This is usually a downsample-of-downsample case
        for (var metricField : aggMetricFieldType.getMetricFields().entrySet()) {
            var metric = metricField.getKey();
            var metricSubField = metricField.getValue();
            if (context.fieldExistsInIndex(metricSubField.name())) {
                IndexFieldData<?> fieldData = context.getForField(metricSubField, MappedFieldType.FielddataOperation.SEARCH);
                downsamplers.add(create(aggMetricFieldType, metric, fieldData, samplingMethod));
            }
        }
        return downsamplers;
    }

    private static AggregateMetricDoubleFieldDownsampler create(
        AggregateMetricDoubleFieldMapper.AggregateMetricDoubleFieldType fieldType,
        AggregateMetricDoubleFieldMapper.Metric metric,
        IndexFieldData<?> fieldData,
        DownsampleConfig.SamplingMethod samplingMethod
    ) {
        if (fieldType.getMetricType() == null) {
            return new AggregateMetricDoubleFieldDownsampler.LastValue(fieldType.name(), metric, fieldData, true);
        }
        return switch (samplingMethod) {
            case AGGREGATE -> new AggregateMetricDoubleFieldDownsampler.Aggregate(fieldType.name(), metric, fieldData);
            case LAST_VALUE -> new AggregateMetricDoubleFieldDownsampler.LastValue(fieldType.name(), metric, fieldData, false);
        };
    }
}
