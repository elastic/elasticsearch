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
     * This document's payload, built up as its slots are recorded. Started by the first slot to arrive, so the buffer is sized against
     * a real slot rather than grown from nothing and a field that turns out to hold no slots at all allocates neither.
     *
     * <p>An unsorted field appends into this as it goes and keeps nothing else. A sorted one cannot — it has to see every value before
     * it can place any of them — so it collects into {@link #values} and encodes through this builder at the end instead.
     */
    private StringBinaryPayload.Builder payload;

    /**
     * @param ordering how this document's slots are collected: {@link ValueOrdering#UNSORTED} keeps them in the order they arrive and
     *                 appends them straight into the payload, while the sorted orderings have to collect them first
     */
    public ColumnarBinaryDocValuesField(String name, ValueOrdering ordering) {
        // Only a sorted ordering has a use for the backing collection, so only a sorted ordering allocates one.
        super(name, ordering, ordering != ValueOrdering.UNSORTED);
    }

    @Override
    public void add(BytesRef value) {
        if (values != null) {
            // The collection settles the order; the base class keeps its byte count in step with what it admits.
            super.add(value);
            return;
        }
        appendSlot(value);
    }

    /**
     * Appends a {@code null} slot, preserving its position relative to the surrounding values. Null slots
     * count towards {@link #count()} but carry no bytes.
     */
    public void addNull() {
        if (values != null) {
            values.add(null);
            return;
        }
        appendSlot(null);
    }

    /**
     * Puts one slot into the payload, starting it if this is the document's first. The unsorted arm only: a sorted field's
     * {@link #binaryValue()} encodes from {@link #values} through a {@link StringBinaryPayload.Builder#reset}, which would discard
     * anything appended here — leaving the payload correct and the work done twice, where nothing would report it.
     */
    private void appendSlot(BytesRef value) {
        assert values == null : "sorted ordering [" + ordering + "] collects its slots; appending them would be discarded";
        if (payload == null) {
            payload = new StringBinaryPayload.Builder();
        }
        payload.appendSlot(value);
    }

    @Override
    public int count() {
        if (values != null) {
            return values.size();
        }
        return payload == null ? 0 : payload.slotCount();
    }

    /**
     * This document's slots as a payload. The bytes are the builder's own, so they are valid until the next call on this field — which
     * is all Lucene needs, since it copies the value into the doc-values writer as soon as it is handed over.
     */
    @Override
    public BytesRef binaryValue() {
        if (values == null) {
            // Already appended in arrival order, so all that is left is to write the count in front of them.
            return payload == null ? StringBinaryPayload.EMPTY : payload.build();
        }
        if (ordering == ValueOrdering.SORTED && values instanceof ArrayList<BytesRef> list) {
            list.sort(Comparator.naturalOrder());
        }
        if (payload == null) {
            payload = new StringBinaryPayload.Builder();
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
