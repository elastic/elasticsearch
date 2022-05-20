/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.common.geo.GeoPoint;

import java.io.IOException;

/**
 * A stateful lightweight per document set of {@link GeoPoint} values.
 * To iterate over values in a document use the following pattern:
 * <pre>
 *   GeoPointValues values = ..;
 *   values.setDocId(docId);
 *   final int numValues = values.count();
 *   for (int i = 0; i &lt; numValues; i++) {
 *       GeoPoint value = values.valueAt(i);
 *       // process value
 *   }
 * </pre>
 * The set of values associated with a document might contain duplicates and
 * comes in a non-specified order.
 */
public final class MultiGeoPointValues {

    private final GeoPoint point = new GeoPoint();
    private final SortedNumericDocValues numericValues;

    /**
     * Creates a new {@link MultiGeoPointValues} instance
     */
    public MultiGeoPointValues(SortedNumericDocValues numericValues) {
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
    public GeoPoint nextValue() throws IOException {
        return point.resetFromEncoded(numericValues.nextValue());
    }

    /**
     * Returns a single-valued view of the {@link MultiGeoPointValues} if possible, otherwise null.
     */
    GeoPointValues getGeoPointValues() {
        final NumericDocValues singleton = DocValues.unwrapSingleton(numericValues);
        return singleton != null ? new GeoPointValues(singleton) : null;
    }

}
