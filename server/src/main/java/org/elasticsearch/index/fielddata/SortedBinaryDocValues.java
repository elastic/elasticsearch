/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.util.BytesRef;

import java.io.IOException;

/**
 * A list of per-document binary values, sorted
 * according to {@link BytesRef#compareTo(BytesRef)}.
 * There might be dups however.
 */
// TODO: Should it expose a count (current approach) or return null when there are no more values?
public abstract class SortedBinaryDocValues {

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
