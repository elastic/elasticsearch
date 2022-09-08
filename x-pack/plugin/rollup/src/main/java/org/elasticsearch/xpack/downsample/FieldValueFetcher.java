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
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.DocValueFormat;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * Utility class used for fetching field values by reading field data
 */
class FieldValueFetcher {

    private static final Set<Class<?>> VALID_METRIC_TYPES = Set.of(
        Long.class,
        Double.class,
        BigInteger.class,
        String.class,
        BytesRef.class
    );
    private static final Set<Class<?>> VALID_LABEL_TYPES = Set.of(
        Long.class,
        Double.class,
        BigInteger.class,
        String.class,
        BytesRef.class,
        Boolean.class
    );

    private final String name;
    private final MappedFieldType fieldType;
    private final DocValueFormat format;
    private final IndexFieldData<?> fieldData;
    private final Function<Object, Object> valueFunc;

    protected FieldValueFetcher(String name, MappedFieldType fieldType, IndexFieldData<?> fieldData, Function<Object, Object> valueFunc) {
        this.name = name;
        this.fieldType = fieldType;
        this.format = fieldType.docValueFormat(null, null);
        this.fieldData = fieldData;
        this.valueFunc = valueFunc;
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

        final FormattedDocValues delegate = fieldData.load(context).getFormattedValues(format);
        return new FormattedDocValues() {
            @Override
            public boolean advanceExact(int docId) throws IOException {
                return delegate.advanceExact(docId);
            }

            @Override
            public int docValueCount() throws IOException {
                return delegate.docValueCount();
            }

            @Override
            public Object nextValue() throws IOException {
                return valueFunc.apply(delegate.nextValue());
            }
        };
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

    /**
     * Retrieve field fetchers for a list of fields.
     */
    private static List<FieldValueFetcher> build(SearchExecutionContext context, String[] fields, Set<Class<?>> validTypes) {
        List<FieldValueFetcher> fetchers = new ArrayList<>(fields.length);
        for (String field : fields) {
            MappedFieldType fieldType = context.getFieldType(field);
            if (fieldType == null) {
                throw new IllegalArgumentException("Unknown field: [" + field + "]");
            }
            IndexFieldData<?> fieldData = context.getForField(fieldType, MappedFieldType.FielddataOperation.SEARCH);
            fetchers.add(new FieldValueFetcher(field, fieldType, fieldData, getValidator(field, validTypes)));
        }
        return Collections.unmodifiableList(fetchers);
    }

    static Function<Object, Object> getValidator(String field, Set<Class<?>> validTypes) {
        return value -> {
            if (validTypes.contains(value.getClass()) == false) {
                throw new IllegalArgumentException(
                    "Expected [" + validTypes + "] for field [" + field + "], " + "got [" + value.getClass() + "]"
                );
            }
            return value;
        };
    }

    static List<FieldValueFetcher> forMetrics(SearchExecutionContext context, String[] metricFields) {
        return build(context, metricFields, VALID_METRIC_TYPES);
    }

    static List<FieldValueFetcher> forLabels(SearchExecutionContext context, String[] labelFields) {
        return build(context, labelFields, VALID_LABEL_TYPES);
    }

}
