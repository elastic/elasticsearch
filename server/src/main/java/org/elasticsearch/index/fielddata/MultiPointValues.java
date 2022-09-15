/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.SpatialPoint;

import java.io.IOException;

/**
 * A stateful lightweight per document set of {@link SpatialPoint} values.
 */
public abstract class MultiPointValues<T extends SpatialPoint> {

    protected final SortedNumericDocValues numericValues;

    /**
     * Creates a new {@link MultiPointValues} instance
     */
    protected MultiPointValues(SortedNumericDocValues numericValues) {
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

    /**
     * Returns a single-valued view of the {@link MultiPointValues} if possible, otherwise null.
     */
    protected abstract PointValues<T> getPointValues();
}
