/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.IOFunction;
import org.elasticsearch.common.CheckedIntFunction;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.fielddata.ColumnarPayloadSortableBinaryDocValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.MultiValuedSortableBinaryDocValues;
import org.elasticsearch.index.fielddata.SortableBinaryDocValues;
import org.elasticsearch.index.fielddata.SortingArrayOrderBinaryDocValues;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Where {@link ReanalyzingTextQuery} and {@link ReanalyzingIntervalsSource} read a document's values from when they
 * analyze them again. A field holding its values in doc values is read there rather than from {@code _source}.
 */
public final class FieldValueFetchers {

    private FieldValueFetchers() {}

    /**
     * A document's values read from its binary doc values.
     *
     * <p>The decoder has to follow the layout: they are not interchangeable, and reading one as another returns wrong
     * values rather than failing, which a phrase query shows as a document that simply does not match.
     */
    public static IOFunction<LeafReaderContext, CheckedIntFunction<List<Object>, IOException>> fromBinaryDocValues(
        String fieldName,
        BinaryDocValuesFormat format
    ) {
        return context -> new CheckedIntFunction<>() {
            SortableBinaryDocValues binaryDocValues;

            @Override
            public List<Object> apply(int docId) throws IOException {
                if (binaryDocValues == null) {
                    binaryDocValues = switch (format) {
                        case COLUMNAR_PAYLOAD -> ColumnarPayloadSortableBinaryDocValues.from(context.reader(), fieldName);
                        case ARRAY_ORDER_INLINE_NULL -> SortingArrayOrderBinaryDocValues.from(context.reader(), fieldName);
                        case SEPARATE_COUNT -> MultiValuedSortableBinaryDocValues.from(context.reader(), fieldName);
                        case PLAIN -> MultiValuedSortableBinaryDocValues.fromPlain(context.reader(), fieldName);
                    };
                }
                return valuesOf(binaryDocValues, docId);
            }
        };
    }

    /** A document's values read through field data, for a field whose doc values are not binary. */
    public static IOFunction<LeafReaderContext, CheckedIntFunction<List<Object>, IOException>> fromFieldData(IndexFieldData<?> fieldData) {
        return context -> {
            SortableBinaryDocValues values = fieldData.load(context).getBytesValues();
            return docId -> valuesOf(values, docId);
        };
    }

    /**
     * A document's values read from the parent of a multi-field, which holds the text the sub-field was built from.
     *
     * @return null where the parent keeps no values either
     */
    @Nullable
    public static IOFunction<LeafReaderContext, CheckedIntFunction<List<Object>, IOException>> fromParent(
        SearchExecutionContext context,
        String fieldName
    ) {
        final String parentName = context.parentPath(fieldName);
        final MappedFieldType parent = context.lookup().fieldType(parentName);
        // A keyword parent keeps a value longer than ignore_above in a fallback field rather than its own.
        if (parent instanceof KeywordFieldMapper.KeywordFieldType keywordParent && keywordParent.ignoreAbove().valuesPotentiallyIgnored()) {
            final String fallbackName = keywordParent.syntheticSourceFallbackFieldName();
            final var fallback = keywordParent.usesBinaryDocValuesForIgnoredFields()
                ? fromBinaryDocValues(fallbackName, BinaryDocValuesFormat.SEPARATE_COUNT)
                : fromStoredFields(fallbackName);
            final var values = valuesOfField(context, parent, parentName);
            return values == null ? null : concat(values, fallback);
        }
        return valuesOfField(context, parent, parentName);
    }

    /** Where {@code fieldType} keeps its values: its stored field, or its doc values. Null if it keeps none. */
    @Nullable
    private static IOFunction<LeafReaderContext, CheckedIntFunction<List<Object>, IOException>> valuesOfField(
        SearchExecutionContext context,
        MappedFieldType fieldType,
        String fieldName
    ) {
        if (fieldType.isStored()) {
            return fromStoredFields(fieldName);
        }
        if (fieldType.hasDocValues()) {
            return fromFieldData(context.getForField(fieldType, MappedFieldType.FielddataOperation.SEARCH));
        }
        return null;
    }

    /** A document's values read from the stored fields {@code fieldNames}. */
    public static IOFunction<LeafReaderContext, CheckedIntFunction<List<Object>, IOException>> fromStoredFields(String... fieldNames) {
        final StoredFieldLoader loader = StoredFieldLoader.create(false, Set.of(fieldNames));
        return context -> {
            final var leafLoader = loader.getLoader(context, null);
            return docId -> {
                leafLoader.advanceTo(docId);
                final var storedFields = leafLoader.storedFields();
                if (fieldNames.length == 1) {
                    return storedFields.get(fieldNames[0]);
                }
                final List<Object> values = new ArrayList<>();
                for (String fieldName : fieldNames) {
                    final var fieldValues = storedFields.get(fieldName);
                    if (fieldValues != null) {
                        values.addAll(fieldValues);
                    }
                }
                return values;
            };
        };
    }

    /** Both fetchers' values together, for a field whose values are split over two places. */
    public static IOFunction<LeafReaderContext, CheckedIntFunction<List<Object>, IOException>> concat(
        IOFunction<LeafReaderContext, CheckedIntFunction<List<Object>, IOException>> first,
        IOFunction<LeafReaderContext, CheckedIntFunction<List<Object>, IOException>> second
    ) {
        return context -> {
            final var firstValues = first.apply(context);
            final var secondValues = second.apply(context);
            return docId -> {
                final List<Object> values = new ArrayList<>();
                final var fromFirst = firstValues.apply(docId);
                if (fromFirst != null) {
                    values.addAll(fromFirst);
                }
                final var fromSecond = secondValues.apply(docId);
                if (fromSecond != null) {
                    values.addAll(fromSecond);
                }
                assert fromFirst != null || fromSecond != null;
                return values;
            };
        };
    }

    /** The values {@code docId} holds, or none where it holds none. */
    public static List<Object> valuesOf(SortableBinaryDocValues docValues, int docId) throws IOException {
        if (docValues.advanceExact(docId) == false) {
            return List.of();
        }
        final List<Object> values = new ArrayList<>(docValues.docValueCount());
        for (int i = 0; i < docValues.docValueCount(); i++) {
            values.add(docValues.nextValue().utf8ToString());
        }
        return values;
    }
}
