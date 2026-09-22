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
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.string.StringBinaryPayload;
import org.elasticsearch.core.Nullable;

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
 * <p>Carrying the count means <b>no companion field is written at all</b>: an array of nothing but nulls is a count with null slots
 * under it, which is what tells it from a field that is absent.
 *
 * <p>Null slots are kept so that an array reads back as the array it was written as, its nulls where they were, which is what synthetic
 * source rebuilds from. A scalar {@code null} is no array element and an empty array holds none, so both record nothing and read back as
 * a field that is absent. A document holding an array therefore holds this field, however many of its elements are null, which is what
 * {@code exists} answers from.
 *
 * <p>A document whose array holds nothing but nulls is the one shape the payload cannot describe alone. Lucene requires every document
 * holding a field to hold it as the field's schema says, so where the field is also indexed, a payload without the field's postings is
 * rejected. Such a document therefore carries the field once more, with the field's own index options and a token stream that yields
 * nothing: the schema agrees and no term is added, so the document answers no query over the terms.
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
     * The field's own index options, carried by a document that holds no value so that its schema matches the field's, or null where the
     * field is not indexed and nothing has to agree.
     */
    private final IndexableFieldType postingsShim;

    /** Whether any slot holds a value, which decides whether this document needs the shim above. */
    private boolean hasValue;

    /** Held rather than made per call, and per field rather than shared, since a document is indexed by one thread. */
    private EmptyTokenStream emptyTokens;

    public ColumnarBinaryDocValuesField(String name, ValueOrdering ordering) {
        this(name, ordering, null);
    }

    public ColumnarBinaryDocValuesField(String name, ValueOrdering ordering, @Nullable IndexableFieldType postingsShim) {
        super(name, ordering);
        this.postingsShim = postingsShim;
    }

    /**
     * The type a document holding no value writes this field as: the index options of {@code indexed}, so the document's schema matches
     * the field's, alongside the doc values the payload is. Null where {@code indexed} names no postings, since then nothing disagrees.
     *
     * <p>Tokenized, whatever the field is, because that is what has Lucene ask for a token stream — which yields nothing — rather than
     * take the payload's bytes for a term. Lucene records neither tokenization nor doc values in what it compares, so a document written
     * this way is the same shape as one holding values.
     */
    @Nullable
    public static IndexableFieldType postingsShimFor(IndexableFieldType indexed) {
        final FieldType shim = shimOf(indexed);
        if (shim == null) {
            return null;
        }
        shim.setDocValuesType(DocValuesType.BINARY);
        return Mapper.freezeAndDeduplicateFieldType(shim);
    }

    /**
     * The same shim for the batch path, which writes a document's postings and its doc values as two columns rather than as one field,
     * so this one carries no doc values: the payload arrives on the column beside it. Lucene records neither in what it compares, so a
     * document written as two instances is the same shape as one written as a single field.
     */
    @Nullable
    public static IndexableFieldType postingsOnlyShimFor(IndexableFieldType indexed) {
        final FieldType shim = shimOf(indexed);
        return shim == null ? null : Mapper.freezeAndDeduplicateFieldType(shim);
    }

    /** What both shims carry: everything Lucene compares a document's schema by, and nothing else. */
    @Nullable
    private static FieldType shimOf(IndexableFieldType indexed) {
        if (indexed == null || indexed.indexOptions() == IndexOptions.NONE) {
            return null;
        }
        final FieldType shim = new FieldType();
        shim.setIndexOptions(indexed.indexOptions());
        shim.setOmitNorms(indexed.omitNorms());
        shim.setStoreTermVectors(indexed.storeTermVectors());
        shim.setStoreTermVectorPositions(indexed.storeTermVectorPositions());
        shim.setStoreTermVectorOffsets(indexed.storeTermVectorOffsets());
        shim.setStoreTermVectorPayloads(indexed.storeTermVectorPayloads());
        shim.setTokenized(true);
        return shim;
    }

    @Override
    public void add(BytesRef value) {
        super.add(value);
        hasValue = true;
    }

    /** Whether this document needs to carry the field's index options, which is so while it holds no value. */
    private boolean needsShim() {
        return hasValue == false && postingsShim != null;
    }

    @Override
    public IndexableFieldType fieldType() {
        return needsShim() ? postingsShim : super.fieldType();
    }

    @Override
    public InvertableType invertableType() {
        return needsShim() ? InvertableType.TOKEN_STREAM : super.invertableType();
    }

    @Override
    public TokenStream tokenStream(Analyzer analyzer, TokenStream reuse) {
        if (needsShim() == false) {
            return super.tokenStream(analyzer, reuse);
        }
        if (emptyTokens == null) {
            emptyTokens = new EmptyTokenStream();
        }
        return emptyTokens;
    }

    /** Yields no token, so the field is indexed as the schema says while adding no term. */
    static final class EmptyTokenStream extends TokenStream {
        @Override
        public boolean incrementToken() {
            return false;
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
     * Records a non-null value into the document's accumulator for {@code fieldName}, in the order the field's {@code ordering}
     * dictates. {@code postingsShim} is what a document that ends up holding no value carries the field's index options as, and is null
     * for a field that is not indexed; see {@link #postingsShimFor}.
     */
    public static void recordValue(
        LuceneDocument doc,
        String fieldName,
        BytesRef value,
        ValueOrdering ordering,
        @Nullable IndexableFieldType postingsShim
    ) {
        getOrCreate(doc, fieldName, ordering, postingsShim).add(value);
    }

    /**
     * Records a {@code null} slot, preserving its position relative to the surrounding values, which is what tells an array holding a
     * null from a field that is absent.
     */
    public static void recordNull(LuceneDocument doc, String fieldName, @Nullable IndexableFieldType postingsShim) {
        getOrCreate(doc, fieldName, ValueOrdering.UNSORTED, postingsShim).addNull();
    }

    /** As {@link #recordValue(LuceneDocument, String, BytesRef, ValueOrdering, IndexableFieldType)}, for an unindexed field. */
    public static void recordValue(LuceneDocument doc, String fieldName, BytesRef value, ValueOrdering ordering) {
        recordValue(doc, fieldName, value, ordering, null);
    }

    /** As {@link #recordNull(LuceneDocument, String, IndexableFieldType)}, for an unindexed field. */
    public static void recordNull(LuceneDocument doc, String fieldName) {
        recordNull(doc, fieldName, null);
    }

    /** As {@link #recordSingleValue(LuceneDocument, String, BytesRef, ValueOrdering, IndexableFieldType)}, for an unindexed field. */
    public static void recordSingleValue(LuceneDocument doc, String fieldName, BytesRef value, ValueOrdering ordering) {
        recordSingleValue(doc, fieldName, value, ordering, null);
    }

    /**
     * Looks up the per-field accumulator on the document, creating it on first use. Unlike the formats that keep their count in a
     * companion field, this one is added to the document as soon as it exists: its payload describes an array of nothing but nulls
     * as well as it describes values, so there is nothing to hold back for.
     */
    private static ColumnarBinaryDocValuesField getOrCreate(
        LuceneDocument doc,
        String fieldName,
        ValueOrdering ordering,
        @Nullable IndexableFieldType postingsShim
    ) {
        return (ColumnarBinaryDocValuesField) doc.getOrAddWithKey(fieldName, key -> {
            var field = new ColumnarBinaryDocValuesField(fieldName, ordering, postingsShim);
            doc.add(field);
            return field;
        });
    }

    /**
     * Creates a field already holding {@code value} and registers it in one go, for the common case of a
     * field that turns out to hold a single value.
     */
    public static void recordSingleValue(
        LuceneDocument doc,
        String fieldName,
        BytesRef value,
        ValueOrdering ordering,
        @Nullable IndexableFieldType postingsShim
    ) {
        var field = new ColumnarBinaryDocValuesField(fieldName, ordering, postingsShim);
        if (doc.putKeyIfAbsent(fieldName, field) == null) {
            field.add(value);
            doc.add(field);
        } else {
            // Safety net (for dotted-field flattening or duplicated field names): a field under the same name
            // has already been registered.
            recordValue(doc, fieldName, value, ordering, postingsShim);
        }
    }
}
