/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.util.BytesRef;

import java.io.IOException;

/**
 * Binary doc values that can hand over the string column behind them, so a search matches a term against
 * the column rather than reading a value for every document.
 *
 * <p>Doc values reach a search through whatever wraps them, and what a caller holds is not always the
 * instance the format made. Asking for this rather than for that instance lets anything standing in front
 * of a column offer what the column can do, by implementing this and answering from what it wraps.
 * Doc values that do not offer it are read a document at a time, which every binary doc values answers.
 */
public interface StringColumnSource {

    /** The column behind these values. */
    StringColumnReader reader();

    /**
     * The largest or smallest value the document these values are positioned on holds, or null when it holds none.
     *
     * <p>Here rather than on the column because the column addresses documents by rank, and which rank these values
     * stand on is what the surface knows and the column does not. The returned {@link BytesRef} is only valid until
     * the next call.
     */
    BytesRef extreme(boolean max, BytesRef dst) throws IOException;

    /**
     * How many non-null values the document these values are positioned on holds, capped at two. Capped because the
     * callers are the single-value functions, which want to know whether the arity is nothing, one, or more than one
     * and nothing further.
     *
     * <p>{@code dst} is set to the first non-null value found, so it is the document's only value exactly when the
     * answer is one. Scanning stops on the second, which leaves {@code dst} holding the first of several - no caller
     * reads it then, and the payload route answers the same way.
     *
     * <p>Answered from what the column already records - how many slots the document has, and which of them are null
     * - so nothing is decoded to count.
     */
    int nonNullValues(BytesRef dst) throws IOException;
}
