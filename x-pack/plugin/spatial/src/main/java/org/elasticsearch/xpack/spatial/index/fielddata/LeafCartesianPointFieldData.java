/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.xpack.spatial.search.aggregations.support.CartesianPointValuesSource;

/**
 * {@link LeafFieldData} specialization for cartesian points.
 */
public abstract class LeafCartesianPointFieldData implements LeafFieldData {
    /**
     * Return point values.
     */
    public final CartesianPointValuesSource.CartesianPointValues getGeoPointValues() {
        return new CartesianPointValuesSource.CartesianPointValues(getSortedNumericDocValues());
    }

    /**
     * Return the internal representation of geo_point doc values as a {@link SortedNumericDocValues}.
     * A point is encoded as a long that can be decoded by using
     * {@link org.elasticsearch.common.geo.GeoPoint#resetFromEncoded(long)}
     */
    public abstract SortedNumericDocValues getSortedNumericDocValues();
}
