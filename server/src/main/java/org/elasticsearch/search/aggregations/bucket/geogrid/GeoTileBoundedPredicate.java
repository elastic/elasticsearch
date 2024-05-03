/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.geometry.Rectangle;

import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitude;

/**
 * Filters out tiles using the provided bounds at the provided precision. In order to be efficient it works on the X/Y coordinates of the
 * geotile scheme.
 */
public class GeoTileBoundedPredicate {

    private final boolean crossesDateline;
    private final long maxTiles;
    private final int precision, leftX, rightX, minY, maxY;

    public GeoTileBoundedPredicate(int precision, GeoBoundingBox bbox) {
        this.crossesDateline = bbox.right() < bbox.left();
        this.precision = precision;
        if (bbox.bottom() > GeoTileUtils.NORMALIZED_LATITUDE_MASK || bbox.top() < GeoTileUtils.NORMALIZED_NEGATIVE_LATITUDE_MASK) {
            // this makes validTile() always return false
            leftX = rightX = minY = maxY = -1;
            maxTiles = 0;
        } else {
            final int tiles = 1 << precision;
            // compute minX, minY
            final int minX = GeoTileUtils.getXTile(bbox.left(), tiles);
            final int minY = GeoTileUtils.getYTile(bbox.top(), tiles);
            final Rectangle minTile = GeoTileUtils.toBoundingBox(minX, minY, precision);
            // touching tiles are excluded, they need to share at least one interior point
            this.leftX = encodeLongitude(minTile.getMaxX()) == encodeLongitude(bbox.left()) ? minX + 1 : minX;
            this.minY = encodeLatitude(minTile.getMinY()) == encodeLatitude(bbox.top()) ? minY + 1 : minY;
            // compute maxX, maxY
            final int maxX = GeoTileUtils.getXTile(bbox.right(), tiles);
            final int maxY = GeoTileUtils.getYTile(bbox.bottom(), tiles);
            final Rectangle maxTile = GeoTileUtils.toBoundingBox(maxX, maxY, precision);
            // touching tiles are excluded, they need to share at least one interior point
            this.rightX = encodeLongitude(maxTile.getMinX()) == encodeLongitude(bbox.right()) ? maxX : maxX + 1;
            this.maxY = encodeLatitude(maxTile.getMaxY()) == encodeLatitude(bbox.bottom()) ? maxY : maxY + 1;
            if (crossesDateline) {
                this.maxTiles = ((long) tiles + this.rightX - this.leftX) * (this.maxY - this.minY);
            } else {
                this.maxTiles = (long) (this.rightX - this.leftX) * (this.maxY - this.minY);
            }
        }
    }

    /** Does the provided bounds crosses the dateline */
    public boolean crossesDateline() {
        return crossesDateline;
    }

    /** The left bound on geotile coordinates */
    public int leftX() {
        return leftX;
    }

    /** The right bound on geotile coordinates */
    public int rightX() {
        return rightX;
    }

    /** The bottom bound on geotile coordinates */
    public int minY() {
        return minY;
    }

    /** The top bound on geotile coordinates */
    public int maxY() {
        return maxY;
    }

    /** Check if the provided tile at the provided level intersects with the provided bounds. The provided precision must be
     * lower or equal to the precision provided in the constructor.
     */
    public boolean validTile(int x, int y, int precision) {
        assert this.precision >= precision : "input precision bigger than this predicate precision";
        // compute number of splits at precision
        final int splits = 1 << this.precision - precision;
        final int yMin = y * splits;
        if (maxY > yMin && minY < yMin + splits) {
            final int xMin = x * splits;
            if (crossesDateline) {
                return rightX > xMin || leftX < xMin + splits;
            } else {
                return rightX > xMin && leftX < xMin + splits;
            }
        }
        return false;
    }

    /**
     * Total number of tiles intersecting this bounds at the precision provided in the constructor.
     */
    public long getMaxTiles() {
        return maxTiles;
    }
}
