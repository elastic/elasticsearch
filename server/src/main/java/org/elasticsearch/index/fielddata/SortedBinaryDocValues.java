/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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

}
