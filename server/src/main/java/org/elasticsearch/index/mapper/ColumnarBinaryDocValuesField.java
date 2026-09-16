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
import org.elasticsearch.columnar.string.StringBinaryPayload;

import java.util.ArrayList;
import java.util.Comparator;

/**
 * The binary doc-values format written for a field whose doc values are stored by the ColumNAR codec:
 *
 * <pre>
 * [vint slotCount] then slotCount slots of:
 *     [vint len+1][bytes]   a value of length len
 *     [vint 0]              a null slot
 * </pre>
 *
 * <p>The slot count travels inside the payload, which is what the other formats put in a {@code .counts}
 * companion. That is forced by where the codec sits: {@code DocValuesConsumer.addBinaryField(field,
 * valuesProducer)} sees a single binary field at flush, so a companion field is out of reach, and without a
 * count in the blob the codec cannot tell one value stored raw from one value that is the whole blob — which
 * is what it needs in order to store a document's values separately rather than as one opaque blob.
 *
 * <p>Carrying the count means the payload can describe every shape a document takes, so <b>no companion field
 * is written at all</b>: an empty array is a count of zero, and an all-null array is a count with nothing but
 * null slots under it. Both are still distinct from a field that is absent, which writes no payload.
 *
 * <p>{@link org.elasticsearch.columnar.string.ColumnarStringBinaryDocValues#binaryValue} rebuilds exactly this
 * format from the stored slots, so every reader of these fields decodes the one format on both sides of the
 * codec.
 *
 * <p>Nothing here marks the field as the codec's: which fields the ColumNAR codec stores, and as what column
 * type, is settled entirely in the codec wiring by {@code PerFieldFormatSupplier}, and readers dispatch on the
 * {@link BinaryDocValuesFormat} the mapping gives them. So this field's type is the ordinary binary doc-values
 * type its siblings use, and the mapper carries no codec convention.
 */
public class ColumnarBinaryDocValuesField extends MultiValuedBinaryDocValuesField {

    /**
     * Encodes this document's payload. Held on the field rather than made per call, so a document is encoded through the buffer this
     * accumulator already owns instead of allocating one and copying out of it.
     */
    private final StringBinaryPayload.Builder payload = new StringBinaryPayload.Builder();

    public ColumnarBinaryDocValuesField(String name, ValueOrdering ordering) {
        super(name, ordering);
    }

    /**
     * Appends a {@code null} slot, preserving its position relative to the surrounding values. Null slots
     * count towards {@link #count()} but carry no bytes.
     */
    public void addNull() {
        values.add(null);
    }

    /**
     * This document's slots as a payload. The bytes are the builder's own, so they are valid until the next call on this field — which
     * is all Lucene needs, since it copies the value into the doc-values writer as soon as it is handed over.
     */
    // TODO: the backing collection is still allocated for every document, where ArrayOrderInlineNull holds a lone slot in a field and
    // only promotes to a list on the second one. A single-valued document is the common shape for these fields, so it is worth the
    // same treatment.
    @Override
    public BytesRef binaryValue() {
        if (ordering == ValueOrdering.SORTED && values instanceof ArrayList<BytesRef> list) {
            list.sort(Comparator.naturalOrder());
        }
        return payload.encode(values);
    }

    /**
     * Records a non-null value into the document's accumulator for {@code fieldName}, in the order the field's
     * {@code ordering} dictates.
     */
    public static void recordValue(LuceneDocument doc, String fieldName, BytesRef value, ValueOrdering ordering) {
        getOrCreate(doc, fieldName, ordering).add(value);
    }

    /** Records a {@code null} slot, preserving its position relative to the surrounding values. */
    public static void recordNull(LuceneDocument doc, String fieldName) {
        getOrCreate(doc, fieldName, ValueOrdering.UNSORTED).addNull();
    }

    /** Records an empty array: a payload holding a count of zero, which no other shape produces. */
    public static void recordEmptyArray(LuceneDocument doc, String fieldName) {
        getOrCreate(doc, fieldName, ValueOrdering.UNSORTED);
    }

    /**
     * Looks up the per-field accumulator on the document, creating it on first use. Unlike the formats that
     * keep their count in a companion field, this one is added to the document as soon as it exists: its
     * payload describes an empty or all-null array just as well as it describes values, so there is nothing
     * to hold back for.
     */
    private static ColumnarBinaryDocValuesField getOrCreate(LuceneDocument doc, String fieldName, ValueOrdering ordering) {
        return (ColumnarBinaryDocValuesField) doc.getOrAddWithKey(fieldName, key -> {
            var field = new ColumnarBinaryDocValuesField(fieldName, ordering);
            doc.add(field);
            return field;
        });
    }

    /**
     * Creates a field already holding {@code value} and registers it in one go, for the common case of a
     * field that turns out to hold a single value.
     */
    public static void recordSingleValue(LuceneDocument doc, String fieldName, BytesRef value, ValueOrdering ordering) {
        var field = new ColumnarBinaryDocValuesField(fieldName, ordering);
        if (doc.putKeyIfAbsent(fieldName, field) == null) {
            field.add(value);
            doc.add(field);
        } else {
            // Safety net (for dotted-field flattening or duplicated field names): a field under the same name
            // has already been registered.
            recordValue(doc, fieldName, value, ordering);
        }
    }
}
