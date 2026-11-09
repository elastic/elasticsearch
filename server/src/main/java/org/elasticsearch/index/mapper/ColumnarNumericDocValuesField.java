/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.numeric.NumericBinaryPayload;

/**
 * The binary doc values a numeric field writes when the ColumNAR codec stores it: one
 * {@link NumericBinaryPayload} per document, holding the document's slots in the order they appeared in the
 * source, nulls included.
 *
 * <p>Because the payload keeps the array as it was written, these fields need no {@code .offsets} companion:
 * an empty array is a payload of zero slots, and a null keeps its place among the values. Both are still distinct
 * from a document without the field, which writes no payload.
 *
 * <p>As with {@link ColumnarBinaryDocValuesField}, nothing here marks the field as the codec's: which fields the
 * ColumNAR codec stores is settled in {@code PerFieldFormatSupplier}, so this is an ordinary binary doc values field.
 */
public final class ColumnarNumericDocValuesField extends CustomDocValuesField {

    private final NumericBinaryPayload.Builder payload = new NumericBinaryPayload.Builder();

    private ColumnarNumericDocValuesField(String name) {
        super(name);
    }

    /**
     * This document's slots as a payload. The bytes are the builder's own, so they are valid until the next value is
     * recorded, which is all Lucene needs: it copies the value into the doc values writer as soon as it is handed over.
     */
    @Override
    public BytesRef binaryValue() {
        return payload.build();
    }

    /** Records a value into the document's payload for {@code fieldName}, after the slots already recorded. */
    public static void recordValue(LuceneDocument doc, String fieldName, long value) {
        getOrCreate(doc, fieldName).payload.addValue(value);
    }

    /** Records a {@code null} slot, keeping its position among the document's values. */
    public static void recordNull(LuceneDocument doc, String fieldName) {
        getOrCreate(doc, fieldName).payload.addNull();
    }

    /** Records an empty array: a payload of zero slots, unless the document records slots for the field elsewhere. */
    public static void recordEmptyArray(LuceneDocument doc, String fieldName) {
        getOrCreate(doc, fieldName);
    }

    /**
     * Looks up the document's payload for the field, creating it and adding it to the document on first use. The field
     * joins the document as soon as it exists, since its payload describes an empty or all-null array as well as values.
     */
    private static ColumnarNumericDocValuesField getOrCreate(LuceneDocument doc, String fieldName) {
        return (ColumnarNumericDocValuesField) doc.getOrAddWithKey(fieldName, key -> {
            var created = new ColumnarNumericDocValuesField(fieldName);
            doc.add(created);
            return created;
        });
    }
}
