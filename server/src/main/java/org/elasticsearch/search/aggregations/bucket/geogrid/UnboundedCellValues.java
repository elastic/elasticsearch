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
 * Class representing {@link CellValues} that are unbounded by any
 * {@link GeoBoundingBox}.
 */
class UnboundedCellValues extends CellValues {

    UnboundedCellValues(MultiGeoPointValues geoValues, int precision, CellIdSource.GeoPointLongEncoder encoder) {
        super(geoValues, precision, encoder);
    }

    @Override
    int advanceValue(org.elasticsearch.common.geo.GeoPoint target, int valuesIdx) {
        values[valuesIdx] = encoder.encode(target.getLon(), target.getLat(), precision);
        return valuesIdx + 1;
    }
}
