/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;

/**
 * Unbounded geotile aggregation. It accepts any tile.
 */
public class UnboundedGeoTileGridTiler extends AbstractGeoTileGridTiler {
    private final long maxTiles;

    public UnboundedGeoTileGridTiler(int precision) {
        super(precision);
        maxTiles = tiles * tiles;
    }

    @Override
    protected boolean validTile(int x, int y, int z) {
        return true;
    }

    @Override
    protected long getMaxCells() {
        return maxTiles;
    }

    @Override
    protected int setValuesForFullyContainedTile(int xTile, int yTile, int zTile, GeoShapeCellValues values, int valuesIndex) {
        // For every level we go down, we half each dimension. The total number of splits is equal to 1 << (levelEnd - levelStart)
        final int splits = 1 << precision - zTile;
        // The start value of a dimension is calculated by multiplying the value of that dimension at the start level
        // by the number of splits
        final int minX = xTile * splits;
        final int minY = yTile * splits;
        // The end value of a dimension is calculated by adding to the start value the number of splits
        final int maxX = minX + splits;
        final int maxY = minY + splits;
        for (int i = minX; i < maxX; i++) {
            for (int j = minY; j < maxY; j++) {
                assert validTile(i, j, precision);
                values.add(valuesIndex++, GeoTileUtils.longEncodeTiles(precision, i, j));
            }
        }
        return valuesIndex;
    }
}
