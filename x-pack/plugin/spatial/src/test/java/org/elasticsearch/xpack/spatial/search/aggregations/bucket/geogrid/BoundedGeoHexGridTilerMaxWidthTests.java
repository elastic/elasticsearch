/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.geometry.Rectangle;

import static org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid.GeoHexGridTiler.BoundedGeoHexGridTiler.height;
import static org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid.GeoHexGridTiler.BoundedGeoHexGridTiler.width;

/**
 * The original algorithm inflates using the max size of cells at two corners of the bbox.
 */
public class BoundedGeoHexGridTilerMaxWidthTests extends BoundedGeoHexGridTilerTests {

    @Override
    protected GeoBoundingBox inflateBbox(int precision, GeoBoundingBox bbox, double factor) {
        return GeoHexGridTiler.BoundedGeoHexGridTiler.inflateBbox(precision, bbox, factor);
    }

    @Override
    /* Calculate the bounds of the h3 cell assuming the test bbox is entirely within the cell */
    protected Rectangle getFullBounds(Rectangle bounds, GeoBoundingBox bbox) {
        final double height = height(bounds);
        final double width = width(bounds);
        // inflate the coordinates by the full width and height
        final double minY = Math.max(bbox.bottom() - height, -90d);
        final double maxY = Math.min(bbox.top() + height, 90d);
        final double left = GeoUtils.normalizeLon(bbox.left() - width);
        final double right = GeoUtils.normalizeLon(bbox.right() + width);
        if (2 * width + width(bbox) >= 360d) {
            // if the total width is bigger than the world, then it covers all longitude range.
            return new Rectangle(-180, 180, maxY, minY);
        } else {
            return new Rectangle(left, right, maxY, minY);
        }
    }
}
