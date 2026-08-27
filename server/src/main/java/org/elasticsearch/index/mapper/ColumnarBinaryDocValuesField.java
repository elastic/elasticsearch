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
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.ColumNARDocValuesFormat;
import org.elasticsearch.columnar.ColumnarFieldType;
import org.elasticsearch.columnar.string.StringBinaryPayload;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * The binary doc-values format written for a field whose doc values are stored by the ColumNAR codec.
 *
 * <p>It differs from the other formats in exactly one way: the slot count travels inside the payload. That is
 * forced by where the codec sits. {@code DocValuesConsumer.addBinaryField(field, valuesProducer)} sees a single
 * binary field at flush, so it cannot reach the {@code .counts} companion the other formats lean on, and
 * without a count in the blob it cannot tell one value stored raw from one value that is the whole blob — which
 * is what it needs in order to store a document's values separately rather than as one opaque blob.
 *
 * <p>The {@code .counts} companion is still written, unchanged, because it is what every reader of these fields
 * consults. The codec re-encodes a document's slots back into the framing the mapper would have
 * written when it hands them back at the {@code BinaryDocValues} surface, so nothing downstream sees this
 * format at all. Dropping {@code .counts} waits on the query and aggregation pushdowns that make it redundant.
 *
 * <p>The framing to re-encode into is carried to the codec on the field's
 * {@link ColumNARDocValuesFormat#STRING_FRAMING_ATTRIBUTE} attribute.
 */
public class ColumnarBinaryDocValuesField extends MultiValuedBinaryDocValuesField {

    /**
     * Which of the mapper's framings this field's values would have been written in, and so the one the codec
     * re-encodes into. Only {@link StringBinaryPayload.Framing#ARRAY_ORDER} biases a slot's length, so only it
     * can carry an inline null.
     */
    private static final FieldType ARRAY_ORDER_TYPE = fieldType(StringBinaryPayload.Framing.ARRAY_ORDER);
    private static final FieldType SEPARATE_COUNT_TYPE = fieldType(StringBinaryPayload.Framing.SEPARATE_COUNT);

    /**
     * The field type of a single-valued columnar field, whose payload is just the value. Exposed so the
     * columnar batch-mapping path can tag its zero-copy binary column with the same attributes the row path
     * writes, without re-encoding anything.
     */
    public static final FieldType PLAIN_TYPE = fieldType(StringBinaryPayload.Framing.PLAIN);

    /** The field type of an array-order columnar field, for the columnar batch-mapping path's binary column. */
    public static FieldType arrayOrderType() {
        return ARRAY_ORDER_TYPE;
    }

    private static FieldType fieldType(StringBinaryPayload.Framing framing) {
        FieldType type = new FieldType();
        type.setDocValuesType(DocValuesType.BINARY);
        type.setOmitNorms(true);
        type.putAttribute(ColumNARDocValuesFormat.TYPE_ATTRIBUTE, ColumnarFieldType.STRING.name());
        type.putAttribute(ColumNARDocValuesFormat.STRING_FRAMING_ATTRIBUTE, framing.name());
        type.freeze();
        return type;
    }

    private final StringBinaryPayload.Framing framing;

    /** Held so the record helpers can update the count on each slot without re-deriving the companion field. */
    private NumericDocValuesField countField;

    /**
     * Whether any non-null value has arrived. While {@code false} the binary field must NOT be added to the
     * document: an all-null or empty array is carried by {@code .counts} alone, exactly as
     * {@link ArrayOrderInlineNull} does it, so the codec never sees a document with nothing to store.
     */
    private boolean hasNonNullValue;

    public ColumnarBinaryDocValuesField(String name, StringBinaryPayload.Framing framing, ValueOrdering ordering) {
        super(name, ordering);
        this.framing = framing;
    }

    @Override
    public IndexableFieldType fieldType() {
        return switch (framing) {
            case ARRAY_ORDER -> ARRAY_ORDER_TYPE;
            case SEPARATE_COUNT -> SEPARATE_COUNT_TYPE;
            case PLAIN -> PLAIN_TYPE;
        };
    }

    public String countFieldName() {
        return name() + SeparateCount.COUNT_FIELD_SUFFIX;
    }

    public NumericDocValuesField countField() {
        return countField;
    }

    /** Whether at least one non-null value has been accumulated, and so whether a blob is written at all. */
    public boolean hasNonNullValue() {
        return hasNonNullValue;
    }

    @Override
    public void add(BytesRef value) {
        hasNonNullValue = true;
        super.add(value);
    }

    /**
     * Appends a {@code null} slot, preserving its position relative to the surrounding values. Null slots count
     * towards {@link #count()} but carry no bytes, and only the array-order framing can express one.
     */
    public void addNull() {
        assert framing == StringBinaryPayload.Framing.ARRAY_ORDER : "null slot under a framing that cannot express one";
        values.add(null);
    }

    @Override
    public BytesRef binaryValue() {
        assert hasNonNullValue : "a document with no non-null value must not write a binary value";
        if (framing.isSelfDescribing() == false) {
            assert values.size() == 1 : "a single-valued field holds one value per document, got " + values.size();
            return values.iterator().next();
        }
        if (ordering == ValueOrdering.SORTED && values instanceof ArrayList<BytesRef> list) {
            list.sort(Comparator.naturalOrder());
        }
        return StringBinaryPayload.encode(values);
    }

    /**
     * Records a non-null value into the document's accumulator for {@code fieldName}, in the order the field's
     * {@code ordering} dictates. The binary blob is added to the document lazily on the first non-null value,
     * so an all-null or empty array writes the {@code .counts} field alone.
     */
    public static void recordValue(
        LuceneDocument doc,
        String fieldName,
        BytesRef value,
        StringBinaryPayload.Framing framing,
        ValueOrdering ordering
    ) {
        var field = getOrCreate(doc, fieldName, framing, ordering);
        boolean firstNonNullValue = field.hasNonNullValue == false;
        field.add(value);
        if (firstNonNullValue) {
            doc.add(field);
        }
        field.countField.setLongValue(field.count());
    }

    /** Records a {@code null} slot; updates the {@code .counts} field but never adds the binary blob. */
    public static void recordNull(LuceneDocument doc, String fieldName, StringBinaryPayload.Framing framing) {
        var field = getOrCreate(doc, fieldName, framing, ValueOrdering.UNSORTED);
        field.addNull();
        field.countField.setLongValue(field.count());
    }

    /** Records an empty array: ensures the {@code .counts} field exists (value {@code 0}); no blob is written. */
    public static void recordEmptyArray(LuceneDocument doc, String fieldName, StringBinaryPayload.Framing framing) {
        getOrCreate(doc, fieldName, framing, ValueOrdering.UNSORTED);
    }

    /**
     * Looks up the per-field accumulator on the document, creating it on first use. The accumulator is
     * registered by key (without being added to the field list yet) and its always-present {@code .counts}
     * companion is added to the document immediately.
     */
    private static ColumnarBinaryDocValuesField getOrCreate(
        LuceneDocument doc,
        String fieldName,
        StringBinaryPayload.Framing framing,
        ValueOrdering ordering
    ) {
        return (ColumnarBinaryDocValuesField) doc.getOrAddWithKey(fieldName, key -> {
            var field = new ColumnarBinaryDocValuesField(fieldName, framing, ordering);
            field.countField = NumericDocValuesField.indexedField(field.countFieldName(), 0);
            // Only the always-present .counts companion is added here; the blob follows the first non-null value.
            doc.add(field.countField);
            return field;
        });
    }

    /**
     * Records the lone value of a field declared single-valued, with no {@code .counts} companion and no framing at
     * all — matching what {@link DocValuesFieldFactory} writes as a plain {@code BinaryDocValuesField} for such a
     * field. The codec takes the blob as the value and hands it straight back, so a reader sees byte for byte what
     * it sees today.
     */
    public static void recordSingleValuedField(LuceneDocument doc, String fieldName, BytesRef value) {
        var field = new ColumnarBinaryDocValuesField(fieldName, StringBinaryPayload.Framing.PLAIN, ValueOrdering.UNSORTED);
        field.add(value);
        doc.add(field);
    }

    /**
     * Creates a field already holding {@code value} and registers it with its companion in one go, for the
     * common case of a field that turns out to hold a single value.
     */
    public static void recordSingleValue(
        LuceneDocument doc,
        String fieldName,
        BytesRef value,
        StringBinaryPayload.Framing framing,
        ValueOrdering ordering
    ) {
        var field = new ColumnarBinaryDocValuesField(fieldName, framing, ordering);
        if (doc.putKeyIfAbsent(fieldName, field) == null) {
            field.add(value);
            field.countField = NumericDocValuesField.indexedField(field.countFieldName(), 1);
            doc.addAll(List.of(field, field.countField));
        } else {
            // Safety net (for dotted-field flattening or duplicated field names): a field under the same name
            // has already been registered.
            recordValue(doc, fieldName, value, framing, ordering);
        }
    }
}
