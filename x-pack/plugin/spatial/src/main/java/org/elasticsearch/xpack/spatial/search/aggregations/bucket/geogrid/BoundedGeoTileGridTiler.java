/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileBoundedPredicate;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;

/**
 * Bounded geotile aggregation. It accepts tiles that intersects the provided bounds.
 */
public class BoundedGeoTileGridTiler extends AbstractGeoTileGridTiler {

    private final GeoTileBoundedPredicate predicate;

    public BoundedGeoTileGridTiler(int precision, GeoBoundingBox bbox) {
        super(precision);
        this.predicate = new GeoTileBoundedPredicate(precision, bbox);
    }

    @Override
    protected boolean validTile(int x, int y, int z) {
        return predicate.validTile(x, y, z);
    }

    @Override
    protected long getMaxCells() {
        return predicate.getMaxTiles();
    }

    @Override
    @SuppressWarnings("HiddenField")
    protected int setValuesForFullyContainedTile(int xTile, int yTile, int zTile, GeoShapeCellValues values, int valuesIndex) {
        // For every level we go down, we half each dimension. The total number of splits is equal to 1 << (levelEnd - levelStart)
        final int splits = 1 << precision - zTile;
        // The start value of a dimension is calculated by multiplying the value of that dimension at the start level
        // by the number of splits. Choose the max value with respect to the bounding box.
        final int minY = Math.max(predicate.minY(), yTile * splits);
        // The end value of a dimension is calculated by adding to the start value the number of splits.
        // Choose the min value with respect to the bounding box.
        final int maxY = Math.min(predicate.maxY(), yTile * splits + splits);
        // Do the same for the X dimension taking into account that the bounding box might cross the dateline.
        if (predicate.crossesDateline()) {
            final int eastMinX = xTile * splits;
            final int westMinX = Math.max(predicate.leftX(), xTile * splits);
            // when the left and right box land in the same tile, we need to make sure we don't count then twice
            final int eastMaxX = Math.min(westMinX, Math.min(predicate.rightX(), xTile * splits + splits));
            final int westMaxX = xTile * splits + splits;
            for (int i = eastMinX; i < eastMaxX; i++) {
                for (int j = minY; j < maxY; j++) {
                    assert predicate.validTile(i, j, precision);
                    values.add(valuesIndex++, GeoTileUtils.longEncodeTiles(precision, i, j));
                }
            }
            for (int i = westMinX; i < westMaxX; i++) {
                for (int j = minY; j < maxY; j++) {
                    assert predicate.validTile(i, j, precision);
                    values.add(valuesIndex++, GeoTileUtils.longEncodeTiles(precision, i, j));
                }
            }
        } else {
            final int _minX = Math.max(predicate.leftX(), xTile * splits);
            final int _maxX = Math.min(predicate.rightX(), xTile * splits + splits);
            for (int i = _minX; i < _maxX; i++) {
                for (int j = minY; j < maxY; j++) {
                    assert predicate.validTile(i, j, precision);
                    values.add(valuesIndex++, GeoTileUtils.longEncodeTiles(precision, i, j));
                }
            }
        }
        return valuesIndex;
    }
}
