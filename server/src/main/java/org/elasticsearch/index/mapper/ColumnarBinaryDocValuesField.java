/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.ColumNARDocValuesFormat;
import org.elasticsearch.columnar.ColumnarFieldType;
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
 */
public class ColumnarBinaryDocValuesField extends MultiValuedBinaryDocValuesField {

    /**
     * Carries the codec's field type, which is how {@code ColumnarFieldType.fromField} resolves the column,
     * and marks the field as one whose blobs are payloads — the signal readers dispatch on.
     */
    public static final FieldType TYPE;
    static {
        FieldType type = new FieldType();
        type.setDocValuesType(DocValuesType.BINARY);
        type.setOmitNorms(true);
        type.putAttribute(ColumNARDocValuesFormat.TYPE_ATTRIBUTE, ColumnarFieldType.STRING.name());
        type.freeze();
        TYPE = type;
    }

    /**
     * Whether {@code fieldName}'s binary doc values in this segment are payloads of this format. Read off the
     * field's own attributes rather than passed down from the mapping, so a reader decodes what the segment
     * actually holds — which is also what keeps a reader honest across segments written under different
     * settings.
     *
     * <p>The column type is checked, not merely the presence of the attribute: the codec also stores
     * {@link ColumnarFieldType#LONG} and {@link ColumnarFieldType#DOUBLE} columns, whose blobs are a numeric
     * payload with no slot count in front of them. Reading one of those as a string payload would decode
     * something rather than fail.
     */
    public static boolean isColumnarStringPayload(LeafReader leafReader, String fieldName) {
        final FieldInfo fieldInfo = leafReader.getFieldInfos().fieldInfo(fieldName);
        return fieldInfo != null && ColumnarFieldType.STRING.name().equals(fieldInfo.getAttribute(ColumNARDocValuesFormat.TYPE_ATTRIBUTE));
    }

    public ColumnarBinaryDocValuesField(String name, ValueOrdering ordering) {
        super(name, ordering);
    }

    @Override
    public IndexableFieldType fieldType() {
        return TYPE;
    }

    /**
     * Appends a {@code null} slot, preserving its position relative to the surrounding values. Null slots
     * count towards {@link #count()} but carry no bytes.
     */
    public void addNull() {
        values.add(null);
    }

    @Override
    public BytesRef binaryValue() {
        if (ordering == ValueOrdering.SORTED && values instanceof ArrayList<BytesRef> list) {
            list.sort(Comparator.naturalOrder());
        }
        return StringBinaryPayload.encode(values);
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
