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
 *   MultiGeoPointValues values = ..;
 *   values.advanceExact(docId);
 *   final int numValues = values.docValueCount();
 *   for (int i = 0; i &lt; numValues; i++) {
 *       GeoPoint value = values.nextValue();
 *       // process value
 *   }
 * </pre>
 * The set of values associated with a document might contain duplicates and
 * comes in a non-specified order.
 */
public class MultiGeoPointValues extends MultiPointValues<GeoPoint> {
    private final GeoPoint point = new GeoPoint();

    public MultiGeoPointValues(SortedNumericDocValues numericValues) {
        super(numericValues);
    }

    @Override
    public GeoPoint nextValue() throws IOException {
        return point.resetFromEncoded(numericValues.nextValue());
    }

    /**
     * Returns a single-valued view of the {@link MultiPointValues} if possible, otherwise null.
     */
    @Override
    protected GeoPointValues getPointValues() {
        final NumericDocValues singleton = DocValues.unwrapSingleton(numericValues);
        return singleton != null ? new GeoPointValues(singleton) : null;
    }
}
