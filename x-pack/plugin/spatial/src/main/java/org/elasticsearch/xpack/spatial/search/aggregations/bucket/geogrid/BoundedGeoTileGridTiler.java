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
    private final GeoBoundingBox bbox;
    private final boolean crossesDateline;
    private final long maxTiles;

    public BoundedGeoTileGridTiler(int precision, GeoBoundingBox bbox) {
        super(precision);
        this.bbox = bbox;
        this.crossesDateline = bbox.right() < bbox.left();
        if (crossesDateline) {
            maxTiles =  numTilesFromPrecision(precision, bbox.left(), 180, bbox.bottom(), bbox.top())
                + numTilesFromPrecision(precision, -180, bbox.right(), bbox.bottom(), bbox.top());

        } else {
            maxTiles = numTilesFromPrecision(precision, bbox.left(), bbox.right(), bbox.bottom(), bbox.top());
        }
    }

    @Override
    protected boolean validTile(int x, int y, int z) {
        Rectangle rectangle = GeoTileUtils.toBoundingBox(x, y, z);
        if (bbox.top() >= rectangle.getMinY() && bbox.bottom() <= rectangle.getMaxY()) {
            if (crossesDateline) {
                return bbox.left() <= rectangle.getMaxX() || bbox.right() >= rectangle.getMinX();
            } else {
                return bbox.left() <= rectangle.getMaxX() && bbox.right() >= rectangle.getMinX();
            }
        }
        return false;
    }

    @Override
    protected long getMaxTiles() {
        return maxTiles;
    }
}
