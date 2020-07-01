/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.elasticsearch.xpack.spatial.common.CartesianPoint;

import java.io.IOException;

/**
 * A stateful lightweight per document set of {@link CartesianPoint} values.
 * To iterate over values in a document use the following pattern:
 * <pre>
 *   MultiPointValues values = ..;
 *   values.advanceExact(docId);
 *   final int numValues = values.docValueCount();
 *   for (int i = 0; i &lt; numValues; i++) {
 *       CartesianPoint value = values.nextValue();
 *       // process value
 *   }
 * </pre>
 * The set of values associated with a document might contain duplicates and
 * comes in a non-specified order.
 */
public abstract class MultiPointValues {

    public static MultiPointValues EMPTY = new MultiPointValues() {

        @Override
        public boolean advanceExact(int doc) {
            return false;
        }

        @Override
        public int docValueCount() {
            return 0;
        }

        @Override
        public CartesianPoint nextValue() {
            throw new UnsupportedOperationException();
        }
    };

    /**
     * Creates a new {@link MultiPointValues} instance
     */
    protected MultiPointValues() {
    }

    /**
     * Advance this instance to the given document id
     * @return true if there is a value for this document
     */
    public abstract boolean advanceExact(int doc) throws IOException;

    /**
     * Return the number of points the current document has.
     */
    public abstract int docValueCount();

    /**
     * Return the next value associated with the current document. This must not be
     * called more than {@link #docValueCount()} times.
     *
     * Note: the returned {@link CartesianPoint} might be shared across invocations.
     *
     * @return the next value for the current docID set to {@link #advanceExact(int)}.
     */
    public abstract CartesianPoint nextValue() throws IOException;

    /**
     * Returns a single-valued view of the {@link MultiPointValues},
     * if it was previously wrapped with {@link SingletonMultiPointValues},
     * or null.
     */
    public static PointValues unwrapSingleton(MultiPointValues values) {
        if (values instanceof SingletonMultiPointValues) {
            return ((SingletonMultiPointValues) values).getPointValues();
        }
        return null;
    }
}
