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
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.SpatialPoint;
import org.elasticsearch.core.Nullable;

import java.io.IOException;

/**
 * A stateful lightweight per document set of {@link SpatialPoint} values.
 */
public abstract class MultiPointValues<T extends SpatialPoint> {

    protected final SortedNumericLongValues numericValues;

    /**
     * Creates a new {@link MultiPointValues} instance
     */
    protected MultiPointValues(SortedNumericLongValues numericValues) {
        this.numericValues = numericValues;
    }

    /**
     * Advance this instance to the given document id
     * @return true if there is a value for this document
     */
    public boolean advanceExact(int doc) throws IOException {
        return numericValues.advanceExact(doc);
    }

    /**
     * Returns the {@link DocIdSetIterator} that works together with {@link #advanceExact(int)}
     * and {@link #nextValue()}.
     *
     * @return the iterator, or null if the underlying values do not expose one.
     */
    @Nullable
    public DocIdSetIterator docIdIterator() {
        return numericValues.docIdIterator();
    }

    /**
     * Return the number of geo points the current document has.
     */
    public int docValueCount() {
        return numericValues.docValueCount();
    }

    /**
     * Return the next value associated with the current document. This must not be
     * called more than {@link #docValueCount()} times.
     *
     * Note: the returned {@link GeoPoint} might be shared across invocations.
     *
     * @return the next value for the current docID set to {@link #advanceExact(int)}.
     */
    public abstract T nextValue() throws IOException;

}
