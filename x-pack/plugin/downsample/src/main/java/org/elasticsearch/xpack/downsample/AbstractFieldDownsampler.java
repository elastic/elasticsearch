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
    protected final IndexFieldData<?> fieldData;

    AbstractFieldDownsampler(String name, IndexFieldData<?> fieldData) {
        this.name = name;
        this.isEmpty = true;
        this.fieldData = fieldData;
    }

    /**
     * @return the name of the field.
     */
    public String name() {
        return name;
    }

    /**
     * Resets the downsampler to an empty value. The downsampler should be reset before downsampling a field for a new bucket.
     */
    public abstract void reset();

    /**
     * @return true if the field has not collected any value.
     */
    public boolean isEmpty() {
        return isEmpty;
    }

    /**
     * @return the leaf reader that will retrieve the doc values for this field.
     */
    public abstract T getLeaf(LeafReaderContext context) throws IOException;

    /**
     * Collects the values for this field of the doc ids requested.
     * @param docValues the doc values for this field
     * @param docIdBuffer the doc ids for which we need to retrieve the field values
     * @throws IOException
     */
    public abstract void collect(T docValues, IntArrayList docIdBuffer) throws IOException;

    /**
     * Create field downsamplers for the provided list of fields.
     */
    static List<AbstractFieldDownsampler<?>> create(
        SearchExecutionContext context,
        String[] fields,
        Map<String, String> multiFieldSources,
        DownsampleConfig.SamplingMethod samplingMethod
    ) {
        List<AbstractFieldDownsampler<?>> downsamplers = new ArrayList<>();
        for (String field : fields) {
            String sourceField = multiFieldSources.getOrDefault(field, field);
            MappedFieldType fieldType = context.getFieldType(sourceField);
            assert fieldType != null : "Unknown field type for field: [" + sourceField + "]";

            if (fieldType instanceof AggregateMetricDoubleFieldMapper.AggregateMetricDoubleFieldType aggMetricFieldType) {
                downsamplers.addAll(AggregateMetricDoubleFieldDownsampler.create(context, aggMetricFieldType, samplingMethod));
            } else {
                if (context.fieldExistsInIndex(field)) {
                    final IndexFieldData<?> fieldData;
                    if (fieldType instanceof FlattenedFieldMapper.RootFlattenedFieldType flattenedFieldType) {
                        var keyedFieldType = flattenedFieldType.getKeyedFieldType();
                        fieldData = context.getForField(keyedFieldType, MappedFieldType.FielddataOperation.SEARCH);
                    } else {
                        fieldData = context.getForField(fieldType, MappedFieldType.FielddataOperation.SEARCH);
                    }
                    downsamplers.add(create(field, fieldType, fieldData, samplingMethod));
                }
            }
        }
        return Collections.unmodifiableList(downsamplers);
    }

    /**
     * Create a single field downsamplers for the provided field.
     */
    private static AbstractFieldDownsampler<?> create(
        String fieldName,
        MappedFieldType fieldType,
        IndexFieldData<?> fieldData,
        DownsampleConfig.SamplingMethod samplingMethod
    ) {
        assert AggregateMetricDoubleFieldDownsampler.supportsFieldType(fieldType) == false
            : "Aggregate metric double should be handled by a dedicated downsampler";
        if (TDigestHistogramFieldDownsampler.supportsFieldType(fieldType)) {
            return TDigestHistogramFieldDownsampler.create(fieldName, fieldType, fieldData, samplingMethod);
        }
        if (ExponentialHistogramFieldDownsampler.supportsFieldType(fieldType)) {
            return ExponentialHistogramFieldDownsampler.create(fieldName, fieldData, samplingMethod);
        }
        if (NumericMetricFieldDownsampler.supportsFieldType(fieldType)) {
            return NumericMetricFieldDownsampler.create(fieldName, fieldType, fieldData, samplingMethod);
        }
        // TODO: Support POSITION in downsampling
        if (fieldType.getMetricType() == POSITION) {
            throw new IllegalArgumentException("Unsupported metric type [position] for downsampling");
        }
        // If a field is not a metric, we downsample it as a label
        return LastValueFieldDownsampler.create(fieldName, fieldType, fieldData);
    }
}
