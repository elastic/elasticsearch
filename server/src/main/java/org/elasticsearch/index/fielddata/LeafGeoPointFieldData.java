/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.SortedNumericDocValues;

/**
 * {@link LeafFieldData} specialization for geo points.
 */
public abstract class LeafGeoPointFieldData implements LeafFieldData {

    /**
     * Return geo-point values.
     */
    public final MultiGeoPointValues getGeoPointValues() {
        return new MultiGeoPointValues(getSortedNumericDocValues());
    }

    /**
     * Return the internal representation of geo_point doc values as a {@link SortedNumericDocValues}.
     * A point is encoded as a long that can be decoded by using
     * {@link org.elasticsearch.common.geo.GeoPoint#resetFromEncoded(long)}
     */
    public abstract SortedNumericDocValues getSortedNumericDocValues();

}
