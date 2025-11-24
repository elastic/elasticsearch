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
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.LeafNumericFieldData;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.xpack.aggregatemetric.mapper.AggregateMetricDoubleFieldMapper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Utility class used for fetching field values by reading field data.
 * For fields whose type is multivalued the 'name' matches the parent field
 * name (normally used for indexing data), while the actual subfield
 * name is accessible by means of {@link MappedFieldType#name()}.
 */
class FieldValueFetcher {

    protected final String name;
    protected final MappedFieldType fieldType;
    protected final IndexFieldData<?> fieldData;
    protected final AbstractDownsampleFieldProducer fieldProducer;

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

    public AbstractDownsampleFieldProducer fieldProducer() {
        return fieldProducer;
    }

    private AbstractDownsampleFieldProducer createFieldProducer(DownsampleConfig.SamplingMethod samplingMethod) {
        assert "aggregate_metric_double".equals(fieldType.typeName()) == false
            : "Aggregate metric double should be handled by a dedicated FieldValueFetcher";
        if (fieldType.getMetricType() != null) {
            return switch (fieldType.getMetricType()) {
                case GAUGE -> MetricFieldProducer.createFieldProducerForGauge(name(), samplingMethod);
                case COUNTER -> LastValueFieldProducer.createForMetric(name());
                case HISTOGRAM -> throw new IllegalArgumentException("Unsupported metric type [histogram] for downsampling, coming soon");
                // TODO: Support POSITION in downsampling
                case POSITION -> throw new IllegalArgumentException("Unsupported metric type [position] for down-sampling");
            };
        } else {
            // If a field is not a metric, we downsample it as a label
            return LastValueFieldProducer.createForLabel(name(), fieldType.typeName());
        }
    }

    /**
     * Retrieve field value fetchers for a list of fields.
     */
    static List<FieldValueFetcher> create(SearchExecutionContext context, String[] fields, DownsampleConfig.SamplingMethod samplingMethod) {
        List<FieldValueFetcher> fetchers = new ArrayList<>();
        for (String field : fields) {
            MappedFieldType fieldType = context.getFieldType(field);
            assert fieldType != null : "Unknown field type for field: [" + field + "]";

            if (fieldType instanceof AggregateMetricDoubleFieldMapper.AggregateMetricDoubleFieldType aggMetricFieldType) {
                fetchers.addAll(AggregateSubMetricFieldValueFetcher.create(context, aggMetricFieldType, samplingMethod));
            } else {
                if (context.fieldExistsInIndex(field)) {
                    final IndexFieldData<?> fieldData;
                    if (fieldType instanceof FlattenedFieldMapper.RootFlattenedFieldType flattenedFieldType) {
                        var keyedFieldType = flattenedFieldType.getKeyedFieldType();
                        fieldData = context.getForField(keyedFieldType, MappedFieldType.FielddataOperation.SEARCH);
                    } else {
                        fieldData = context.getForField(fieldType, MappedFieldType.FielddataOperation.SEARCH);
                    }
                    final String fieldName = context.isMultiField(field)
                        ? fieldType.name().substring(0, fieldType.name().lastIndexOf('.'))
                        : fieldType.name();
                    fetchers.add(new FieldValueFetcher(fieldName, fieldType, fieldData, samplingMethod));
                }
            }
        }
        return Collections.unmodifiableList(fetchers);
    }
}
