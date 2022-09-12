/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.FormattedDocValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.xpack.aggregatemetric.mapper.AggregateDoubleMetricFieldMapper;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility class used for fetching field values by reading field data
 */
class FieldValueFetcher {
    private final String name;
    private final MappedFieldType fieldType;
    private final DocValueFormat format;
    private final IndexFieldData<?> fieldData;

    protected FieldValueFetcher(String name, MappedFieldType fieldType, IndexFieldData<?> fieldData) {
        this.name = name;
        this.fieldType = fieldType;
        this.format = fieldType.docValueFormat(null, null);
        this.fieldData = fieldData;
    }

    public String name() {
        return name;
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

    Object format(Object value) {
        if (value instanceof Long l) {
            return format.format(l);
        } else if (value instanceof Double d) {
            return format.format(d);
        } else if (value instanceof BytesRef b) {
            return format.format(b);
        } else if (value instanceof String s) {
            return s;
        } else {
            throw new IllegalArgumentException("Invalid type: [" + value.getClass() + "]");
        }
    }

    private static Map<String, FieldValueFetcher> build(SearchExecutionContext context, String field) {
        MappedFieldType fieldType = context.getFieldType(field);
        assert fieldType != null : "Unknown field type for field: [" + field + "]";
        Map<String, FieldValueFetcher> fetchers = new HashMap<>();

        if (fieldType instanceof AggregateDoubleMetricFieldMapper.AggregateDoubleMetricFieldType aggMetricFieldType) {
            for (NumberFieldMapper.NumberFieldType metricSubField : aggMetricFieldType.getMetricFields().values()) {
                IndexFieldData<?> fieldData = context.getForField(metricSubField, MappedFieldType.FielddataOperation.SEARCH);
                fetchers.put(metricSubField.name(), new FieldValueFetcher(metricSubField.name(), fieldType, fieldData));
            }
        } else {
            IndexFieldData<?> fieldData = context.getForField(fieldType, MappedFieldType.FielddataOperation.SEARCH);
            fetchers.put(field, new FieldValueFetcher(field, fieldType, fieldData));
        }
        return Collections.unmodifiableMap(fetchers);
    }

    /**
     * Retrieve field fetchers for a list of fields.
     */
    private static Map<String, FieldValueFetcher> build(SearchExecutionContext context, String[] fields) {
        Map<String, FieldValueFetcher> fetchers = new HashMap<>();
        for (String field : fields) {
            fetchers.putAll(build(context, field));
        }
        return Collections.unmodifiableMap(fetchers);
    }

    static Map<String, FieldValueFetcher> forMetrics(SearchExecutionContext context, String[] metricFields) {
        return build(context, metricFields);
    }

    static Map<String, FieldValueFetcher> forLabels(SearchExecutionContext context, String[] labelFields) {
        return build(context, labelFields);
    }

}
