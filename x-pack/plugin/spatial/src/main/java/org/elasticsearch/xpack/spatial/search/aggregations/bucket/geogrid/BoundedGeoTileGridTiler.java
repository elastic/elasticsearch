/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;

/**
 * Bounded geotile aggregation. It accepts tiles that intersects the provided bounds.
 */
public class BoundedGeoTileGridTiler extends AbstractGeoTileGridTiler {
    private final boolean crossesDateline;
    private final long maxTiles;
    // min values are included, max values are excluded
    private final int minX, maxX, minY, maxY;

    public BoundedGeoTileGridTiler(int precision, GeoBoundingBox bbox) {
        super(precision);
        this.crossesDateline = bbox.right() < bbox.left();
        if (bbox.bottom() > GeoTileUtils.NORMALIZED_LATITUDE_MASK || bbox.top() < GeoTileUtils.NORMALIZED_NEGATIVE_LATITUDE_MASK) {
            // this makes validTile() always return false
            minX = maxX = minY = maxY = -1;
            maxTiles = 0;
        } else {
            // compute minX, minY
            final int minX = GeoTileUtils.getXTile(bbox.left(), this.tiles);
            final int minY = GeoTileUtils.getYTile(bbox.top(), this.tiles);
            final Rectangle minTile = GeoTileUtils.toBoundingBox(minX, minY, precision);
            // touching tiles are excluded, they need to share at least one interior point
            this.minX = minTile.getMaxX() == bbox.left() ? minX + 1 : minX;
            this.minY = minTile.getMinY() == bbox.top() ? minY + 1 : minY;
            // compute maxX, maxY
            final int maxX = GeoTileUtils.getXTile(bbox.right(), this.tiles);
            final int maxY = GeoTileUtils.getYTile(bbox.bottom(), this.tiles);
            final Rectangle maxTile = GeoTileUtils.toBoundingBox(maxX, maxY, precision);
            // touching tiles are excluded, they need to share at least one interior point
            this.maxX = maxTile.getMinX() == bbox.right() ? maxX : maxX + 1;
            this.maxY = maxTile.getMaxY() == bbox.bottom() ? maxY : maxY + 1;
            if (crossesDateline) {
                this.maxTiles = (tiles + this.maxX - this.minX) * (this.maxY - this.minY);
            } else {
                this.maxTiles = (long) (this.maxX - this.minX) * (this.maxY - this.minY);
            }
        }
    }

    @Override
    protected boolean validTile(int x, int y, int z) {
        // compute number of splits at precision
        final int splits = 1 << precision - z;
        final int yMin = y * splits;
        if (maxY > yMin && minY < yMin + splits) {
            final int xMin = x * splits;
            if (crossesDateline) {
                return maxX > xMin || minX < xMin + splits;
            } else {
                return maxX > xMin && minX < xMin + splits;
            }
        }
        return false;
    }

    @Override
    protected long getMaxCells() {
        return maxTiles;
    }

    @Override
    protected int setValuesForFullyContainedTile(int xTile, int yTile, int zTile, GeoShapeCellValues values, int valuesIndex) {
        // For every level we go down, we half each dimension. The total number of splits is equal to 1 << (levelEnd - levelStart)
        final int splits = 1 << precision - zTile;
        // The start value of a dimension is calculated by multiplying the  value of that dimension at the start level
        // by the number of splits. Choose the max value with respect to the bounding box.
        final int minY = Math.max(this.minY, yTile * splits);
        // The end value of a dimension is calculated by adding to the start value the number of splits.
        // Choose the min value with respect to the bounding box.
        final int maxY = Math.min(this.maxY, yTile * splits + splits);
        // Do the same for the X dimension taking into account that the bounding box might cross the dateline.
        if (crossesDateline) {
            final int eastMinX = xTile * splits;
            final int westMinX = Math.max(this.minX, xTile * splits);
            // when the left and right box land in the same tile, we need to make sure we don't count then twice
            final int eastMaxX = Math.min(westMinX, Math.min(this.maxX, xTile * splits + splits));
            final int westMaxX = xTile * splits + splits;
            for (int i = eastMinX; i < eastMaxX; i++) {
                for (int j = minY; j < maxY; j++) {
                    assert validTile(i, j, precision);
                    values.add(valuesIndex++, GeoTileUtils.longEncodeTiles(precision, i, j));
                }
            }
            for (int i = westMinX; i < westMaxX; i++) {
                for (int j = minY; j < maxY; j++) {
                    assert validTile(i, j, precision);
                    values.add(valuesIndex++, GeoTileUtils.longEncodeTiles(precision, i, j));
                }
            }
        } else {
            final int minX = Math.max(this.minX, xTile * splits);
            final int maxX = Math.min(this.maxX, xTile * splits + splits);
            for (int i = minX; i < maxX; i++) {
                for (int j = minY; j < maxY; j++) {
                    assert validTile(i, j, precision);
                    values.add(valuesIndex++, GeoTileUtils.longEncodeTiles(precision, i, j));
                }
            }
        }
        return valuesIndex;
    }
}
