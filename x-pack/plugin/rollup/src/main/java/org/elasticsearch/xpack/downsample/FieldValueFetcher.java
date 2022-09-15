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

import java.util.Collections;
import java.util.LinkedHashMap;
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

    /**
     * Retrieve field value fetchers for a list of fields.
     */
    static Map<String, FieldValueFetcher> create(SearchExecutionContext context, String[] fields) {
        Map<String, FieldValueFetcher> fetchers = new LinkedHashMap<>();
        for (String field : fields) {
            MappedFieldType fieldType = context.getFieldType(field);
            assert fieldType != null : "Unknown field type for field: [" + field + "]";

            if (fieldType instanceof AggregateDoubleMetricFieldMapper.AggregateDoubleMetricFieldType aggMetricFieldType) {
                // If the field is an aggregate_metric_double field, we should load all its subfields
                // This is a rollup-of-rollup case
                for (NumberFieldMapper.NumberFieldType metricSubField : aggMetricFieldType.getMetricFields().values()) {
                    if (context.fieldExistsInIndex(metricSubField.name())) {
                        IndexFieldData<?> fieldData = context.getForField(metricSubField, MappedFieldType.FielddataOperation.SEARCH);
                        fetchers.put(metricSubField.name(), new FieldValueFetcher(metricSubField.name(), fieldType, fieldData));
                    }
                }
            } else {
                if (context.fieldExistsInIndex(field)) {
                    IndexFieldData<?> fieldData = context.getForField(fieldType, MappedFieldType.FielddataOperation.SEARCH);
                    fetchers.put(field, new FieldValueFetcher(field, fieldType, fieldData));
                }
            }
        }
        return Collections.unmodifiableMap(fetchers);
    }

    static Map<String, FormattedDocValues> docValuesFetchers(LeafReaderContext ctx, Map<String, FieldValueFetcher> fieldValueFetchers) {
        final Map<String, FormattedDocValues> docValuesFetchers = new LinkedHashMap<>(fieldValueFetchers.size());
        for (FieldValueFetcher fetcher : fieldValueFetchers.values()) {
            docValuesFetchers.put(fetcher.name(), fetcher.getLeaf(ctx));
        }
        return Collections.unmodifiableMap(docValuesFetchers);
    }
}
