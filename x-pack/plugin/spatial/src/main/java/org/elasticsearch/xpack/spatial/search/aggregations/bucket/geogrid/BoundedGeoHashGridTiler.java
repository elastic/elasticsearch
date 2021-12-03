/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.Geohash;

/**
 * Bounded geotile aggregation. It accepts hashes that intersects the provided bounds.
 */
public class BoundedGeoHashGridTiler extends AbstractGeoHashGridTiler {
    private final GeoBoundingBox bbox;
    private final boolean crossesDateline;
    private final long maxHashes;

    public BoundedGeoHashGridTiler(int precision, GeoBoundingBox bbox) {
        super(precision);
        this.bbox = bbox;
        this.crossesDateline = bbox.right() < bbox.left();
        final long hashesY = (long) Math.ceil(((bbox.top() - bbox.bottom()) / Geohash.latHeightInDegrees(precision)) + 1);
        final long hashesX;
        if (crossesDateline) {
            final long hashesLeft = (long) Math.ceil(((180 - bbox.left()) / Geohash.lonWidthInDegrees(precision)) + 1);
            final long hashesRight = (long) Math.ceil(((bbox.right() + 180) / Geohash.lonWidthInDegrees(precision)) + 1);
            hashesX = hashesLeft + hashesRight;
        } else {
            hashesX = (long) Math.ceil(((bbox.right() - bbox.left()) / Geohash.lonWidthInDegrees(precision)) + 1);
        }
        this.maxHashes = hashesX * hashesY;
    }

    @Override
    protected long getMaxCells() {
        return maxHashes;
    }

    @Override
    protected boolean validHash(String hash) {
        final Rectangle rectangle = Geohash.toBoundingBox(hash);
        // touching hashes are excluded
        if (bbox.top() > rectangle.getMinY() && bbox.bottom() < rectangle.getMaxY()) {
            if (crossesDateline) {
                return bbox.left() < rectangle.getMaxX() || bbox.right() > rectangle.getMinX();
            } else {
                return bbox.left() < rectangle.getMaxX() && bbox.right() > rectangle.getMinX();
            }
        }
        return false;
    }
}
