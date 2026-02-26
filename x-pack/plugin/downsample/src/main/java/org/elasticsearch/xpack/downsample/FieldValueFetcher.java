/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.index.fielddata.FormattedDocValues;
import org.elasticsearch.index.fielddata.HistogramValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.LeafHistogramFieldData;
import org.elasticsearch.index.fielddata.LeafNumericFieldData;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.TimeSeriesParams;
import org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.xpack.aggregatemetric.mapper.AggregateMetricDoubleFieldMapper;
import org.elasticsearch.xpack.analytics.mapper.HistogramFieldMapper;
import org.elasticsearch.xpack.core.analytics.mapper.TDigestFieldMapper;
import org.elasticsearch.xpack.core.exponentialhistogram.fielddata.ExponentialHistogramValuesReader;
import org.elasticsearch.xpack.core.exponentialhistogram.fielddata.LeafExponentialHistogramFieldData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.TimeSeriesParams.MetricType.POSITION;

/**
 * Utility class used for fetching field values by reading field data.
 * For fields whose type is multivalued the 'name' matches the parent field
 * name (normally used for indexing data), while the actual multiField
 * name is accessible by means of {@link MappedFieldType#name()}.
 */
class FieldValueFetcher {

    protected final String name;
    protected final MappedFieldType fieldType;
    protected final IndexFieldData<?> fieldData;
    protected final AbstractDownsampleFieldProducer<?> fieldProducer;

    protected FieldValueFetcher(
        String name,
        MappedFieldType fieldType,
        IndexFieldData<?> fieldData,
        DownsampleConfig.SamplingMethod samplingMethod
    ) {
        this.name = name;
        this.fieldType = fieldType;
        this.fieldData = fieldData;
        this.fieldProducer = createFieldProducer(samplingMethod);
    }

    public String name() {
        return name;
    }

    public FormattedDocValues getLeaf(LeafReaderContext context) {
        DocValueFormat format = fieldType.docValueFormat(null, null);
        return fieldData.load(context).getFormattedValues(format);
    }

    public SortedNumericDoubleValues getNumericLeaf(LeafReaderContext context) {
        LeafNumericFieldData numericFieldData = (LeafNumericFieldData) fieldData.load(context);
        return numericFieldData.getDoubleValues();
    }

    public ExponentialHistogramValuesReader getExponentialHistogramLeaf(LeafReaderContext context) throws IOException {
        LeafExponentialHistogramFieldData exponentialHistogramFieldData = (LeafExponentialHistogramFieldData) fieldData.load(context);
        return exponentialHistogramFieldData.getHistogramValues();
    }

    AbstractDownsampleFieldProducer<?> fieldProducer() {
        return fieldProducer;
    }

    public HistogramValues getHistogramLeaf(LeafReaderContext context) throws IOException {
        LeafHistogramFieldData histogramFieldData = (LeafHistogramFieldData) fieldData.load(context);
        return histogramFieldData.getHistogramValues();
    }

    private AbstractDownsampleFieldProducer<?> createFieldProducer(DownsampleConfig.SamplingMethod samplingMethod) {
        assert AggregateMetricDoubleFieldMapper.CONTENT_TYPE.equals(fieldType.typeName()) == false
            : "Aggregate metric double should be handled by a dedicated FieldValueFetcher";
        if (HistogramFieldMapper.CONTENT_TYPE.equals(fieldType.typeName())) {
            return TDigestHistogramFieldProducer.createForLegacyHistogram(name(), samplingMethod);
        }
        if (TDigestFieldMapper.CONTENT_TYPE.equals(fieldType.typeName())) {
            return TDigestHistogramFieldProducer.createForTDigest(name(), samplingMethod);
        }
        if (ExponentialHistogramFieldProducer.TYPE.equals(fieldType.typeName())) {
            return ExponentialHistogramFieldProducer.create(name(), samplingMethod);
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
                return new NumericMetricFieldProducer.AggregateGauge(name());
            }
            return new NumericMetricFieldProducer.LastValue(name());
        } else {
            // If a field is not a metric, we downsample it as a label
            return LastValueFieldProducer.create(name(), fieldType.typeName());
        }
    }

    /**
     * Retrieve field value fetchers for a list of fields.
     */
    static List<FieldValueFetcher> create(
        SearchExecutionContext context,
        String[] fields,
        Map<String, String> multiFieldSources,
        DownsampleConfig.SamplingMethod samplingMethod
    ) {
        List<FieldValueFetcher> fetchers = new ArrayList<>();
        for (String field : fields) {
            String sourceField = multiFieldSources.getOrDefault(field, field);
            MappedFieldType fieldType = context.getFieldType(sourceField);
            assert fieldType != null : "Unknown field type for field: [" + sourceField + "]";

            if (fieldType instanceof AggregateMetricDoubleFieldMapper.AggregateMetricDoubleFieldType aggMetricFieldType) {
                fetchers.addAll(AggregateMetricDoubleFieldValueFetcher.create(context, aggMetricFieldType, samplingMethod));
            } else {
                if (context.fieldExistsInIndex(field)) {
                    final IndexFieldData<?> fieldData;
                    if (fieldType instanceof FlattenedFieldMapper.RootFlattenedFieldType flattenedFieldType) {
                        var keyedFieldType = flattenedFieldType.getKeyedFieldType();
                        fieldData = context.getForField(keyedFieldType, MappedFieldType.FielddataOperation.SEARCH);
                    } else {
                        fieldData = context.getForField(fieldType, MappedFieldType.FielddataOperation.SEARCH);
                    }
                    fetchers.add(new FieldValueFetcher(field, fieldType, fieldData, samplingMethod));
                }
            }
        }
        return Collections.unmodifiableList(fetchers);
    }
}
