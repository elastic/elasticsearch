/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.common.geo.SpatialPoint;

/**
 * {@link LeafFieldData} specialization for geo points and points.
 */
public abstract class LeafPointFieldData<T extends MultiPointValues<? extends SpatialPoint>> implements LeafFieldData {

    /**
     * Return geo-point or point values.
     */
    public abstract T getPointValues();

    /**
     * Return the internal representation of geo_point or point doc values as a {@link SortedNumericDocValues}.
     * A point is encoded as a long that can be decoded by using
     * {@link org.elasticsearch.common.geo.GeoPoint#resetFromEncoded(long)}
     */
    public abstract SortedNumericDocValues getSortedNumericDocValues();

}
