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
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.xpack.aggregatemetric.mapper.AggregateDoubleMetricFieldMapper;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class used for fetching field values by reading field data
 */
class FieldValueFetcher {
    private final String name;
    private final ValueType valueType;
    private final MappedFieldType fieldType;
    private final DocValueFormat format;
    private final IndexFieldData<?> fieldData;

    protected FieldValueFetcher(String name, ValueType valueType, MappedFieldType fieldType, IndexFieldData<?> fieldData) {
        this.name = name;
        this.valueType = valueType;
        this.fieldType = fieldType;
        this.format = fieldType.docValueFormat(null, null);
        this.fieldData = fieldData;
    }

    public String name() {
        return name;
    }

    public ValueType valueType() {
        return valueType;
    }

    public MappedFieldType fieldType() {
        return fieldType;
    }

    public DocValueFormat format() {
        return format;
    }

    public IndexFieldData<?> fieldData() {
        return fieldData;
    }

    FormattedDocValues getLeaf(LeafReaderContext context) {
        return fieldData.load(context).getFormattedValues(format);
    }

    /**
     * Retrieve field value fetchers for a list of fields.
     */
    static FieldValueFetcher[] create(SearchExecutionContext context, String[] metricFields, String[] labelFields) {
        List<FieldValueFetcher> fetchers = new ArrayList<>();
        for (String field : metricFields) {
            MappedFieldType fieldType = context.getFieldType(field);
            assert fieldType != null : "Unknown field type for field: [" + field + "]";

            if (fieldType instanceof AggregateDoubleMetricFieldMapper.AggregateDoubleMetricFieldType aggMetricFieldType) {
                // If the field is an aggregate_metric_double field, we should load all its subfields
                // This is a rollup-of-rollup case
                for (NumberFieldMapper.NumberFieldType metricSubField : aggMetricFieldType.getMetricFields().values()) {
                    if (context.fieldExistsInIndex(metricSubField.name())) {
                        IndexFieldData<?> fieldData = context.getForField(metricSubField, MappedFieldType.FielddataOperation.SEARCH);
                        fetchers.add(new FieldValueFetcher(metricSubField.name(), ValueType.METRIC, fieldType, fieldData));
                    }
                }
            } else {
                if (context.fieldExistsInIndex(field)) {
                    IndexFieldData<?> fieldData = context.getForField(fieldType, MappedFieldType.FielddataOperation.SEARCH);
                    fetchers.add(new FieldValueFetcher(field, ValueType.METRIC, fieldType, fieldData));
                }
            }
        }
        for (String field : labelFields) {
            MappedFieldType fieldType = context.getFieldType(field);
            assert fieldType != null : "Unknown field type for field: [" + field + "]";
            if (context.fieldExistsInIndex(field)) {
                IndexFieldData<?> fieldData = context.getForField(fieldType, MappedFieldType.FielddataOperation.SEARCH);
                fetchers.add(new FieldValueFetcher(field, ValueType.LABEL, fieldType, fieldData));
            }
        }
        return fetchers.toArray(new FieldValueFetcher[0]);
    }

    static DocValueFetcher[] docValuesFetchers(LeafReaderContext ctx, FieldValueFetcher[] fetchers) {
        DocValueFetcher[] docValueFetchers = new DocValueFetcher[fetchers.length];
        for (int i = 0; i < fetchers.length; i++) {
            FieldValueFetcher fetcher = fetchers[i];
            docValueFetchers[i] = new DocValueFetcher(fetcher.name(), fetcher.valueType(), fetcher.getLeaf(ctx));
        }
        return docValueFetchers;
    }

    enum ValueType {
        METRIC, LABEL
    }

    record DocValueFetcher(String name, ValueType valueType,
                           FormattedDocValues formattedDocValues) {
    }
}
