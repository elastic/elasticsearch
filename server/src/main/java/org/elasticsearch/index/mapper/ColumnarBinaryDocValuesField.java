/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.InvertableType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableFieldType;
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

    /**
     * The type to report if this field ends the document holding no value, or {@code null} for a field whose values are not indexed.
     * See {@link #fieldType()}.
     */
    IndexableFieldType typeWhenValueless;

    public ColumnarBinaryDocValuesField(String name, ValueOrdering ordering) {
        super(name, ordering);
    }

    /**
     * The type a field of this kind reports for a document that holds no value, given the type its values are indexed with. It is
     * the doc-values type plus the index options, norms and term vectors of {@code indexed} — the three Lucene keeps in a field's
     * {@code FieldInfo} and compares across documents. Tokenized so that {@link #invertableType()} can invert it into nothing;
     * neither that nor {@code stored} reaches {@code FieldInfo}.
     *
     * <p>Meant to be built once per mapper and reused, so documents taking this shape hand Lucene the same frozen type each time.
     */
    public static FieldType typeWhenValueless(FieldType indexed) {
        final FieldType type = new FieldType();
        type.setDocValuesType(DocValuesType.BINARY);
        type.setIndexOptions(indexed.indexOptions());
        type.setOmitNorms(indexed.omitNorms());
        type.setStoreTermVectors(indexed.storeTermVectors());
        if (indexed.storeTermVectors()) {
            type.setStoreTermVectorPositions(indexed.storeTermVectorPositions());
            type.setStoreTermVectorOffsets(indexed.storeTermVectorOffsets());
            type.setStoreTermVectorPayloads(indexed.storeTermVectorPayloads());
        }
        type.setTokenized(true);
        type.setStored(false);
        type.freeze();
        return type;
    }

    /**
     * The doc-values type, or — for a document that turned out to hold no value under an indexed field — that type plus the index
     * options its values would have carried.
     *
     * <p>The payload joins the document as soon as the field appears, so a document whose slots are all null carries the field
     * without having indexed anything. Lucene builds a field's {@code FieldInfo} from the first document that has it and rejects any
     * later document presenting it with different index options, so such a document has to state them itself. Lucene asks for the
     * type while it indexes the document, by which point every value the document had is in, so the question is settled by then.
     */
    @Override
    public IndexableFieldType fieldType() {
        return typeWhenValueless != null && hasValue() == false ? typeWhenValueless : super.fieldType();
    }

    /**
     * Inverted through {@link #tokenStream}, which yields nothing, rather than as the single term the payload bytes would otherwise
     * be read as. The field is only ever inverted when it reports {@link #typeWhenValueless}, where there is nothing to index.
     */
    @Override
    public InvertableType invertableType() {
        return InvertableType.TOKEN_STREAM;
    }

    @Override
    public TokenStream tokenStream(Analyzer analyzer, TokenStream reuse) {
        return new EmptyTokenStream();
    }

    /** Produces no tokens, so the field is inverted into nothing. */
    private static final class EmptyTokenStream extends TokenStream {
        @Override
        public boolean incrementToken() {
            return false;
        }
    }

    /**
     * A field over a payload that is already encoded, for the batch path, which builds the bytes a column at a time rather than a
     * slot at a time. Reports the same type for the same document as the row path would, so the two agree field for field.
     *
     * @param hasValue whether any of the encoded slots holds a value, which the caller knows from building them
     */
    public static ColumnarBinaryDocValuesField encoded(
        String name,
        BytesRef encoded,
        boolean hasValue,
        IndexableFieldType typeWhenValueless
    ) {
        final var field = new Encoded(name, encoded, hasValue);
        field.typeWhenValueless = typeWhenValueless;
        return field;
    }

    private static final class Encoded extends ColumnarBinaryDocValuesField {
        private final BytesRef encoded;
        private final boolean hasValue;

        private Encoded(String name, BytesRef encoded, boolean hasValue) {
            super(name, ValueOrdering.UNSORTED);
            this.encoded = encoded;
            this.hasValue = hasValue;
        }

        @Override
        public BytesRef binaryValue() {
            return encoded;
        }

        @Override
        public boolean hasValue() {
            return hasValue;
        }
    }

    /**
     * Appends a {@code null} slot, preserving its position relative to the surrounding values. Null slots
     * count towards {@link #count()} but carry no bytes.
     */
    public void addNull() {
        values.add(null);
    }

    /**
     * Whether any slot holds a value, as opposed to the document having recorded nothing but nulls for the field. A document with a
     * value indexed a term for it and so already carries the field's index options; one without states them itself, through
     * {@link #fieldType()}.
     *
     * <p>Costs one pass over this document's slots, which is what the document wrote for this field.
     */
    public boolean hasValue() {
        for (BytesRef value : values) {
            if (value != null) {
                return true;
            }
        }
        return false;
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

    /**
     * Records a {@code null} slot, preserving its position relative to the surrounding values.
     *
     * @param typeWhenValueless from {@link #typeWhenValueless(FieldType)}, or {@code null} if the field's values are not indexed.
     *                          Only consulted if the document ends up holding no value; a later value makes it moot.
     */
    public static void recordNull(LuceneDocument doc, String fieldName, IndexableFieldType typeWhenValueless) {
        getOrCreate(doc, fieldName, ValueOrdering.UNSORTED, typeWhenValueless).addNull();
    }

    /**
     * Records an empty array: a payload holding a count of zero, which no other shape produces.
     *
     * @param typeWhenValueless see {@link #recordNull}
     */
    public static void recordEmptyArray(LuceneDocument doc, String fieldName, IndexableFieldType typeWhenValueless) {
        getOrCreate(doc, fieldName, ValueOrdering.UNSORTED, typeWhenValueless);
    }

    /**
     * Looks up the per-field accumulator on the document, creating it on first use. Unlike the formats that
     * keep their count in a companion field, this one is added to the document as soon as it exists: its
     * payload describes an empty or all-null array just as well as it describes values, so there is nothing
     * to hold back for.
     */
    private static ColumnarBinaryDocValuesField getOrCreate(LuceneDocument doc, String fieldName, ValueOrdering ordering) {
        return getOrCreate(doc, fieldName, ordering, null);
    }

    private static ColumnarBinaryDocValuesField getOrCreate(
        LuceneDocument doc,
        String fieldName,
        ValueOrdering ordering,
        IndexableFieldType typeWhenValueless
    ) {
        var field = (ColumnarBinaryDocValuesField) doc.getOrAddWithKey(fieldName, key -> {
            var created = new ColumnarBinaryDocValuesField(fieldName, ordering);
            doc.add(created);
            return created;
        });
        if (typeWhenValueless != null) {
            field.typeWhenValueless = typeWhenValueless;
        }
        return field;
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
