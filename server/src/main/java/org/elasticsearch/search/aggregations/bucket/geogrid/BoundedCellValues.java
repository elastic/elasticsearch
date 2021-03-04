/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;

/**
 * Class representing {@link CellValues} whose values are filtered
 * according to whether they are within the specified {@link GeoBoundingBox}.
 *
 * The specified bounding box is assumed to be bounded.
 */
class BoundedCellValues extends CellValues {

    private final GeoBoundingBox geoBoundingBox;

    protected BoundedCellValues(MultiGeoPointValues geoValues, int precision, CellIdSource.GeoPointLongEncoder encoder,
                                GeoBoundingBox geoBoundingBox) {
        super(geoValues, precision, encoder);
        this.geoBoundingBox = geoBoundingBox;
    }


    @Override
    int advanceValue(org.elasticsearch.common.geo.GeoPoint target, int valuesIdx) {
        if (geoBoundingBox.pointInBounds(target.getLon(), target.getLat())) {
            values[valuesIdx] = encoder.encode(target.getLon(), target.getLat(), precision);
            return valuesIdx + 1;
        }
        return valuesIdx;
    }
}
