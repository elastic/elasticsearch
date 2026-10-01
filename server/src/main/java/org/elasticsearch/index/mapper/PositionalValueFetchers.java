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
import org.elasticsearch.index.fielddata.ColumnarPayloadSortableBinaryDocValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.MultiValuedSortableBinaryDocValues;
import org.elasticsearch.index.fielddata.SortableBinaryDocValues;
import org.elasticsearch.index.fielddata.SortingArrayOrderBinaryDocValues;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Where {@link SourceConfirmedTextQuery} and {@link SourceIntervalsSource} read a document's values from when they
 * confirm positions a field did not index. A field holding its values in doc values is read there rather than from
 * {@code _source}.
 */
public final class PositionalValueFetchers {

    private PositionalValueFetchers() {}

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
