/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Nullable;

import java.io.IOException;

/**
 * A list of per-document binary values, in the order {@link #getValueOrder()} reports.
 *
 * <p>Most fields sort their values as they are written, so {@link ValueOrder#SORTED} is the default and what
 * a caller gets unless a field says otherwise. A field that keeps the order its values were given in reports
 * {@link ValueOrder#ARRAY}, and a caller that needs the values ordered has to order them itself. There might
 * be dups either way.
 */
// TODO: Should it expose a count (current approach) or return null when there are no more values?
public abstract class SortableBinaryDocValues {

    @Nullable
    private final DocIdSetIterator docIdIterator;

    /**
     * @param docIdSetIterator, the {@link DocIdSetIterator} that backs this instance.
     */
    public SortableBinaryDocValues(@Nullable DocIdSetIterator docIdSetIterator) {
        this.docIdIterator = docIdSetIterator;
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
     * The order {@link #nextValue()} hands a document's values back in.
     */
    public ValueOrder getValueOrder() {
        return ValueOrder.SORTED;
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

    /**
     * The order a document's values come back in.
     *
     * <p>Which one a field uses is settled when its values are written, so nothing is ordered on the way out:
     * a field is either written sorted or read back the way it was given.
     */
    public enum ValueOrder {

        /**
         * Ascending by {@link BytesRef#compareTo(BytesRef)}, duplicates possible. Callers may rely on a
         * document's values arriving in that order - taking the first as the smallest, comparing neighbours
         * to find duplicates, or advancing through sorted ranges without restarting.
         */
        SORTED,
        /**
         * The order the values were given in, which is how the ColumNAR payload stores every field it holds -
         * not a choice a field makes. Callers that need an order have to establish it themselves.
         */
        ARRAY

    }
}
