/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.BinaryDocValuesFormat;

import java.io.IOException;

/**
 * A list of per-document binary values, sorted
 * according to {@link BytesRef#compareTo(BytesRef)}.
 * There might be dups however.
 */
// TODO: Should it expose a count (current approach) or return null when there are no more values?
public abstract class SortedBinaryDocValues {

    @Nullable
    private final DocIdSetIterator docIdIterator;

    /**
     * @param docIdSetIterator, the {@link DocIdSetIterator} that backs this instance.
     */
    public SortedBinaryDocValues(@Nullable DocIdSetIterator docIdSetIterator) {
        this.docIdIterator = docIdSetIterator;
    }

    /**
     * Reads a field's binary doc values with the decoder its framing requires.
     *
     * <p>The framing is not discoverable from the segment: every one of these layouts is
     * {@link org.apache.lucene.index.DocValuesType#BINARY} on the way out, and decoding one as another returns wrong
     * values rather than failing. So it is taken from the mapping that decided how the values were written, and every
     * reader of them routes through here.
     *
     * <p>{@code indexVersion} settles {@link BinaryDocValuesFormat#SEPARATE_COUNT} alone: indices created before
     * {@link IndexVersions#DEPRECATE_INTEGRATED_COUNTS_BINARY_DOC_VALUES} may hold the deprecated integrated-count
     * layout instead, which {@link MultiValuedSortableBinaryDocValues#fromMultiValued} falls back to when the
     * {@code .counts} companion field is absent.
     */
    public static SortableBinaryDocValues forFormat(
        LeafReader leafReader,
        String fieldName,
        IndexVersion indexVersion,
        BinaryDocValuesFormat binaryFormat
    ) throws IOException {
        return switch (binaryFormat) {
            case COLUMNAR_PAYLOAD -> ColumnarPayloadSortableBinaryDocValues.from(leafReader, fieldName);
            case ARRAY_ORDER_INLINE_NULL -> SortingArrayOrderBinaryDocValues.from(leafReader, fieldName);
            case SEPARATE_COUNT -> indexVersion.onOrAfter(IndexVersions.DEPRECATE_INTEGRATED_COUNTS_BINARY_DOC_VALUES)
                ? MultiValuedSortableBinaryDocValues.from(leafReader, fieldName)
                : MultiValuedSortableBinaryDocValues.fromMultiValued(leafReader, fieldName);
            case PLAIN -> MultiValuedSortableBinaryDocValues.fromPlain(leafReader, fieldName);
        };
    }

    /**
     * Advance this instance to the given document id
     * @return true if there is a value for this document
     */
    public abstract boolean advanceExact(int doc) throws IOException;

    /**
     * Retrieves the number of values for the current document.  This must always
     * be greater than zero.
     * It is illegal to call this method after {@link #advanceExact(int)}
     * returned {@code false}.
     */
    public abstract int docValueCount();

    /**
     * @return the doc id set iterator or null when not available.
     */
    @Nullable
    public DocIdSetIterator docIdIterator() {
        return docIdIterator;
    }

    /**
     * Iterates to the next value in the current document. Do not call this more than
     * {@link #docValueCount} times for the document.
     * Note that the returned {@link BytesRef} might be reused across invocations.
     */
    public abstract BytesRef nextValue() throws IOException;

    /**
     * Indicates the sparsity of the values for this field.
     */
    public Sparsity getSparsity() {
        return Sparsity.UNKNOWN;
    }

    /**
     * Indicates the per-document value mode for this field.
     */
    public ValueMode getValueMode() {
        return ValueMode.UNKNOWN;
    }

    /**
     * Describes the sparsity of the values for a field.
     */
    public enum Sparsity {
        /**
         * Not all documents have a value for a field.
         */
        SPARSE,
        /**
         * All documents have at least one value for a field.
         */
        DENSE,
        /**
         * The sparsity is unknown.
         */
        UNKNOWN
    }

    /**
     * The per-document value mode for a field.
     */
    public enum ValueMode {

        /**
         * All documents have at most one value per field.
         */
        SINGLE_VALUED,
        /**
         * At least one document has multiple values per field.
         */
        MULTI_VALUED,
        /**
         * The per-document value mode is unknown.
         */
        UNKNOWN

    }
}
