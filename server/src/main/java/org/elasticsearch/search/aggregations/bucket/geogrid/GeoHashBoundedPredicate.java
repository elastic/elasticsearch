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
import org.elasticsearch.geometry.utils.Geohash;

/**
 * Filters out geohashes using the provided bounds at the provided precision.
 */
public class GeoHashBoundedPredicate {

    private final boolean crossesDateline;
    private final long maxHashes;
    private final GeoBoundingBox bbox;

    public GeoHashBoundedPredicate(int precision, GeoBoundingBox bbox) {
        this.crossesDateline = bbox.right() < bbox.left();
        this.bbox = bbox;
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

    /** Check if the provided geohash intersects with the provided bounds. */
    public boolean validHash(String geohash) {
        final Rectangle rect = Geohash.toBoundingBox(geohash);
        // hashes should not cross in theory the dateline but due to precision
        // errors and normalization computing the hash, it might happen that they actually
        // cross the dateline.
        if (rect.getMaxX() < rect.getMinX()) {
            return intersects(-180, rect.getMaxX(), rect.getMinY(), rect.getMaxY())
                || intersects(rect.getMinX(), 180, rect.getMinY(), rect.getMaxY());
        } else {
            return intersects(rect.getMinX(), rect.getMaxX(), rect.getMinY(), rect.getMaxY());
        }
    }

    private boolean intersects(double minX, double maxX, double minY, double maxY) {
        // touching hashes are excluded
        if (bbox.top() > minY && bbox.bottom() < maxY) {
            if (crossesDateline) {
                return bbox.left() < maxX || bbox.right() > minX;
            } else {
                return bbox.left() < maxX && bbox.right() > minX;
            }
        }
        return false;
    }

    /**
     * upper bounds on count of geohashes intersecting this bounds at the precision provided in the constructor.
     */
    public long getMaxHashes() {
        return maxHashes;
    }
}
