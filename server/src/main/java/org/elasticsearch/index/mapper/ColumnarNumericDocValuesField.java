/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.columnar.numeric.NumericBinaryPayload;

/**
 * The binary doc values a numeric field writes when the ColumNAR codec stores it: one
 * {@link NumericBinaryPayload} per document, holding the document's non-null values in the order they appeared in
 * the source. The field joins a document with its first value, so a document without one writes no payload.
 *
 * <p>As with {@link ColumnarBinaryDocValuesField}, nothing here marks the field as the codec's: which fields the
 * ColumNAR codec stores is settled in {@code PerFieldFormatSupplier}, so this is an ordinary binary doc values field.
 */
public final class ColumnarNumericDocValuesField extends CustomDocValuesField {

    private long[] values = new long[1];
    private int count;
    private final BytesRefBuilder payload = new BytesRefBuilder();

    private ColumnarNumericDocValuesField(String name) {
        super(name);
    }

    /**
     * This document's values as a payload. The bytes are the field's own, so they are valid until the next value is
     * recorded, which is all Lucene needs: it copies the value into the doc values writer as soon as it is handed over.
     */
    @Override
    public BytesRef binaryValue() {
        return NumericBinaryPayload.encode(values, count, payload);
    }

    /** Records a value into the document's payload for {@code fieldName}, after the values already recorded. */
    public static void recordValue(LuceneDocument doc, String fieldName, long value) {
        final ColumnarNumericDocValuesField field = (ColumnarNumericDocValuesField) doc.getOrAddWithKey(fieldName, key -> {
            var created = new ColumnarNumericDocValuesField(fieldName);
            doc.add(created);
            return created;
        });
        field.values = ArrayUtil.grow(field.values, field.count + 1);
        field.values[field.count++] = value;
    }
}
