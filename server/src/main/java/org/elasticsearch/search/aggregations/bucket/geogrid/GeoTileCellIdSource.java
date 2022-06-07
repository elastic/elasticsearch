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
 * Class to help convert {@link MultiGeoPointValues} to GeoTile {@link CellValues}
 */
public class GeoTileCellIdSource extends CellIdSource {

    public GeoTileCellIdSource(GeoPoint valuesSource, int precision, GeoBoundingBox geoBoundingBox) {
        super(valuesSource, precision, geoBoundingBox);
    }

    @Override
    protected CellValues unboundedCellValues(MultiGeoPointValues values) {
        return new CellValues(values, precision()) {
            @Override
            protected int advanceValue(org.elasticsearch.common.geo.GeoPoint target, int valuesIdx) {
                values[valuesIdx] = GeoTileUtils.longEncode(target.getLon(), target.getLat(), precision);
                return valuesIdx + 1;
            }
        };
    }

    @Override
    protected CellValues boundedCellValues(MultiGeoPointValues values, GeoBoundingBox boundingBox) {
        final GeoTileBoundedPredicate predicate = new GeoTileBoundedPredicate(precision(), boundingBox);
        final long tiles = 1L << precision();
        return new CellValues(values, precision()) {
            @Override
            protected int advanceValue(org.elasticsearch.common.geo.GeoPoint target, int valuesIdx) {
                final int x = GeoTileUtils.getXTile(target.getLon(), tiles);
                final int y = GeoTileUtils.getYTile(target.getLat(), tiles);
                if (predicate.validTile(x, y, precision)) {
                    values[valuesIdx] = GeoTileUtils.longEncodeTiles(precision, x, y);
                    return valuesIdx + 1;
                }
                return valuesIdx;
            }
        };
    }
}
