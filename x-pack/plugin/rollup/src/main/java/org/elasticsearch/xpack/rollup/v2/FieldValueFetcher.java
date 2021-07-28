/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rollup.v2;

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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

class FieldValueFetcher {
    private static final Set<Class<?>> VALID_TYPES = Collections.unmodifiableSet(
        new HashSet<>(Arrays.asList(Long.class, Double.class, BigInteger.class, String.class, BytesRef.class))
    );

    final String name;
    final MappedFieldType fieldType;
    final DocValueFormat format;
    final IndexFieldData<?> fieldData;
    final Function<Object, Object> valueFunc;

    protected FieldValueFetcher(String name,
                                MappedFieldType fieldType, IndexFieldData<?> fieldData,
                                Function<Object, Object> valueFunc) {
        this.name = name;
        this.fieldType = fieldType;
        this.format = fieldType.docValueFormat(null, null);
        this.fieldData = fieldData;
        this.valueFunc = valueFunc;
    }

    FormattedDocValues getLeaf(LeafReaderContext context) {
        final FormattedDocValues delegate = fieldData.load(context).getFormattedValues(DocValueFormat.RAW);
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
        if (value instanceof Long) {
            return format.format((long) value);
        } else if (value instanceof Double) {
            return format.format((double) value);
        } else if (value instanceof BytesRef) {
            return format.format((BytesRef) value);
        } else if (value instanceof String) {
            return value.toString();
        } else {
            throw new IllegalArgumentException("Invalid type: [" + value.getClass() + "]");
        }
    }

    static List<FieldValueFetcher> build(SearchExecutionContext context, String[] fields) {
        List<FieldValueFetcher> fetchers = new ArrayList<>();
        for (String field : fields) {
            MappedFieldType fieldType = context.getFieldType(field);
            if (fieldType == null) {
                throw new IllegalArgumentException("Unknown field: [" + field + "]");
            }
            IndexFieldData<?> fieldData = context.getForField(fieldType);
            fetchers.add(new FieldValueFetcher(field, fieldType, fieldData, getValidator(field)));
        }
        return Collections.unmodifiableList(fetchers);
    }

    static List<FieldValueFetcher> buildHistograms(SearchExecutionContext context, String[] fields, double interval) {
        List<FieldValueFetcher> fetchers = new ArrayList<>();
        for (String field : fields) {
            MappedFieldType fieldType = context.getFieldType(field);
            if (fieldType == null) {
                throw new IllegalArgumentException("Unknown field: [" + field + "]");
            }
            IndexFieldData<?> fieldData = context.getForField(fieldType);
            fetchers.add(new FieldValueFetcher(field, fieldType, fieldData, getIntervalValueFunc(field, interval)));
        }
        return Collections.unmodifiableList(fetchers);
    }

    static Function<Object, Object> getValidator(String field) {
        return value -> {
            if (VALID_TYPES.contains(value.getClass()) == false) {
                throw new IllegalArgumentException("Expected [" + VALID_TYPES + "] for field [" + field + "], " +
                    "got [" + value.getClass() + "]");
            }
            return value;
        };
    }

    static Function<Object, Object> getIntervalValueFunc(String field, double interval) {
        return value -> {
            if (value instanceof Number == false) {
                throw new IllegalArgumentException("Expected [Number] for field [" + field + "], got [" + value.getClass() + "]");
            }
            double number = ((Number) value).doubleValue();
            return Math.floor(number / interval) * interval;
        };
    }
}
