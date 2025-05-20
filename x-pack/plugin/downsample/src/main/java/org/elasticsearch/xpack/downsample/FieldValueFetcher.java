/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.fielddata.FormattedDocValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.LeafNumericFieldData;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.xpack.aggregatemetric.mapper.AggregateMetricDoubleFieldMapper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Utility class used for fetching field values by reading field data.
 * For fields whose type is multi-valued the 'name' matches the parent field
 * name (normally used for indexing data), while the actual sub-field
 * name is accessible by means of {@link MappedFieldType#name()}.
 */
class FieldValueFetcher {

    protected final String name;
    protected final MappedFieldType fieldType;
    protected final IndexFieldData<?> fieldData;
    protected final AbstractDownsampleFieldProducer fieldProducer;

    protected FieldValueFetcher(String name, MappedFieldType fieldType, IndexFieldData<?> fieldData) {
        this.name = name;
        this.fieldType = fieldType;
        this.fieldData = fieldData;
        this.fieldProducer = createFieldProducer();
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

    private AbstractDownsampleFieldProducer createFieldProducer() {
        if (fieldType.getMetricType() != null) {
            return switch (fieldType.getMetricType()) {
                case GAUGE -> new MetricFieldProducer.GaugeMetricFieldProducer(name());
                case COUNTER -> new MetricFieldProducer.CounterMetricFieldProducer(name());
                // TODO: Support POSITION in downsampling
                case POSITION -> throw new IllegalArgumentException("Unsupported metric type [position] for down-sampling");
            };
        } else {
            // If field is not a metric, we downsample it as a label
            if ("histogram".equals(fieldType.typeName())) {
                return new LabelFieldProducer.HistogramLastLabelFieldProducer(name());
            } else if ("flattened".equals(fieldType.typeName())) {
                return new LabelFieldProducer.FlattenedLastValueFieldProducer(name());
            }
            return new LabelFieldProducer.LabelLastValueFieldProducer(name());
        }
    }

    /**
     * Retrieve field value fetchers for a list of fields.
     */
    static List<FieldValueFetcher> create(SearchExecutionContext context, String[] fields) {
        List<FieldValueFetcher> fetchers = new ArrayList<>();
        for (String field : fields) {
            MappedFieldType fieldType = context.getFieldType(field);
            assert fieldType != null : "Unknown field type for field: [" + field + "]";

            if (fieldType instanceof AggregateMetricDoubleFieldMapper.AggregateMetricDoubleFieldType aggMetricFieldType) {
                // If the field is an aggregate_metric_double field, we should load all its subfields
                // This is a downsample-of-downsample case
                for (NumberFieldMapper.NumberFieldType metricSubField : aggMetricFieldType.getMetricFields().values()) {
                    if (context.fieldExistsInIndex(metricSubField.name())) {
                        IndexFieldData<?> fieldData = context.getForField(metricSubField, MappedFieldType.FielddataOperation.SEARCH);
                        fetchers.add(new AggregateMetricFieldValueFetcher(metricSubField, aggMetricFieldType, fieldData));
                    }
                }
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
                    fetchers.add(new FieldValueFetcher(fieldName, fieldType, fieldData));
                }
            }
        }
        return Collections.unmodifiableList(fetchers);
    }
}
