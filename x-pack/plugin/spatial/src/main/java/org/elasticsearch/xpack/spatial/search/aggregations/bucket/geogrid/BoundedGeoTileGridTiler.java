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
            this.maxX = maxTile.getMinX() == bbox.right() ? maxX - 1 : maxX;
            this.maxY = maxTile.getMaxY() == bbox.bottom() ? maxY - 1 : maxY;
            if (crossesDateline) {
                this.maxTiles = (tiles + this.maxX - this.minX + 1) * (this.maxY - this.minY + 1);
            } else {
                this.maxTiles = (long) (this.maxX - this.minX + 1) * (this.maxY - this.minY + 1);
            }
        }
    }

    @Override
    protected boolean validTile(int x, int y, int z) {
        // compute number of splits at precision
        final int splits = 1 << precision - z;
        final int yMin = y * splits;
        if (maxY >= yMin && minY < yMin + splits) {
            final int xMin = x * splits;
            if (crossesDateline) {
                return maxX >= xMin || minX < xMin + splits;
            } else {
                return maxX >= xMin && minX < xMin + splits;
            }
        }
        return false;
    }

    @Override
    protected long getMaxCells() {
        return maxTiles;
    }
}
