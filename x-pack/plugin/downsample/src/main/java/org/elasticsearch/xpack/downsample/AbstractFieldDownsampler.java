/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.internal.hppc.IntArrayList;
import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.TimeSeriesParams;
import org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xpack.aggregatemetric.mapper.AggregateMetricDoubleFieldMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.TimeSeriesParams.MetricType.POSITION;

/**
 * Base class that reads fields from the source index and produces their downsampled values
 */
abstract class AbstractFieldDownsampler<T> implements DownsampleFieldSerializer {

    private final String name;
    protected boolean isEmpty;
    private final FieldValueFetcher<T> fieldValueFetcher;

    AbstractFieldDownsampler(String name, FieldValueFetcher<T> fieldValueFetcher) {
        this.name = name;
        this.isEmpty = true;
        this.fieldValueFetcher = fieldValueFetcher;
    }

    /**
     * @return the name of the field.
     */
    public String name() {
        return name;
    }

    /**
     * Resets the downsampler to an empty value.
     */
    public abstract void reset();

    /**
     * @return true if the field has not collected any value.
     */
    public boolean isEmpty() {
        return isEmpty;
    }

    /**
     * @return the leaf reader that will retrieve the values per doc for this field.
     */
    public T getLeaf(LeafReaderContext context) throws IOException {
        return fieldValueFetcher.getLeaf(context);
    }

    public abstract void collect(T docValues, IntArrayList docIdBuffer) throws IOException;

    /**
     * Utility class used for fetching field values by reading field data.
     * For fields whose type is multivalued the 'name' matches the parent field
     * name (normally used for indexing data), while the actual multiField
     * name is accessible by means of {@link MappedFieldType#name()}.
     */
    abstract static class FieldValueFetcher<T> {
        protected final String name;
        protected final MappedFieldType fieldType;
        protected final IndexFieldData<?> fieldData;

        FieldValueFetcher(String name, MappedFieldType fieldType, IndexFieldData<?> fieldData) {
            this.name = name;
            this.fieldType = fieldType;
            this.fieldData = fieldData;
        }

        String name() {
            return name;
        }

        abstract T getLeaf(LeafReaderContext context) throws IOException;
    }

    /**
     * Retrieve field value fetchers for a list of fields.
     */
    static List<AbstractFieldDownsampler<?>> create(
        SearchExecutionContext context,
        String[] fields,
        Map<String, String> multiFieldSources,
        DownsampleConfig.SamplingMethod samplingMethod
    ) {
        List<AbstractFieldDownsampler<?>> fetchers = new ArrayList<>();
        for (String field : fields) {
            String sourceField = multiFieldSources.getOrDefault(field, field);
            MappedFieldType fieldType = context.getFieldType(sourceField);
            assert fieldType != null : "Unknown field type for field: [" + sourceField + "]";

            if (fieldType instanceof AggregateMetricDoubleFieldMapper.AggregateMetricDoubleFieldType aggMetricFieldType) {
                fetchers.addAll(AggregateMetricDoubleFieldDownsampler.create(context, aggMetricFieldType, samplingMethod));
            } else {
                if (context.fieldExistsInIndex(field)) {
                    final IndexFieldData<?> fieldData;
                    if (fieldType instanceof FlattenedFieldMapper.RootFlattenedFieldType flattenedFieldType) {
                        var keyedFieldType = flattenedFieldType.getKeyedFieldType();
                        fieldData = context.getForField(keyedFieldType, MappedFieldType.FielddataOperation.SEARCH);
                    } else {
                        fieldData = context.getForField(fieldType, MappedFieldType.FielddataOperation.SEARCH);
                    }
                    fetchers.add(create(field, fieldType, fieldData, samplingMethod));
                }
            }
        }
        return Collections.unmodifiableList(fetchers);
    }

    private static AbstractFieldDownsampler<?> create(
        String fieldName,
        MappedFieldType fieldType,
        IndexFieldData<?> fieldData,
        DownsampleConfig.SamplingMethod samplingMethod
    ) {
        assert AggregateMetricDoubleFieldMapper.CONTENT_TYPE.equals(fieldType.typeName()) == false
            : "Aggregate metric double should be handled by a dedicated FieldValueFetcher";
        if (TDigestHistogramFieldDownsampler.TYPE.equals(fieldType.typeName())) {
            return TDigestHistogramFieldDownsampler.create(fieldName, fieldType, fieldData, samplingMethod);
        }
        if (ExponentialHistogramFieldDownsampler.TYPE.equals(fieldType.typeName())) {
            return ExponentialHistogramFieldDownsampler.create(fieldName, fieldType, fieldData, samplingMethod);
        }
        if (fieldType.getMetricType() != null) {
            // TODO: Support POSITION in downsampling
            if (fieldType.getMetricType() == POSITION) {
                throw new IllegalArgumentException("Unsupported metric type [position] for down-sampling");
            }
            assert fieldType.getMetricType() == TimeSeriesParams.MetricType.GAUGE
                || fieldType.getMetricType() == TimeSeriesParams.MetricType.COUNTER
                : "only gauges and counters accepted, other metrics should have been handled earlier";
            if (samplingMethod == DownsampleConfig.SamplingMethod.AGGREGATE
                && fieldType.getMetricType() == TimeSeriesParams.MetricType.GAUGE) {
                return new NumericMetricFieldDownsampler.AggregateGauge(fieldName, fieldType, fieldData);
            }
            return new NumericMetricFieldDownsampler.LastValue(fieldName, fieldType, fieldData);
        } else {
            // If a field is not a metric, we downsample it as a label
            return LastValueFieldDownsampler.create(fieldName, fieldType, fieldData);
        }
    }
}
