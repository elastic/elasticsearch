/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.SortedNumericDocValues;

import java.io.IOException;

/**
 * Clone of {@link SortedNumericDocValues} for double values.
 */
public abstract class SortedNumericDoubleValues {

    /** Sole constructor. (For invocation by subclass
     * constructors, typically implicit.) */
    protected SortedNumericDoubleValues() {}

    /** Advance the iterator to exactly {@code target} and return whether
     *  {@code target} has a value.
     *  {@code target} must be greater than or equal to the current
     *  doc ID and must be a valid doc ID, ie. &ge; 0 and
     *  &lt; {@code maxDoc}.*/
    public abstract boolean advanceExact(int target) throws IOException;

    /**
     * Iterates to the next value in the current document. Do not call this more than
     * {@link #docValueCount} times for the document.
     */
    public abstract double nextValue() throws IOException;

    /**
     * Retrieves the number of values for the current document.  This must always
     * be greater than zero.
     * It is illegal to call this method after {@link #advanceExact(int)}
     * returned {@code false}.
     */
    public abstract int docValueCount();

}
