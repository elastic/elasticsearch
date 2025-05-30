/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.xpack.esql.common.spatial.H3SphericalUtil;

/**
 * A class that checks if a hexagon intersects with a bounding box.
 * TODO: This class is a copy of the inner class GeoHexPredicate inside GeoHexCellIdSource, we should find a common location for it.
 */
public class GeoHexBoundedPredicate {

    private final boolean crossesDateline;
    private final GeoBoundingBox bbox, scratch;

    GeoHexBoundedPredicate(GeoBoundingBox bbox) {
        this.crossesDateline = bbox.right() < bbox.left();
        // TODO remove this once we get serverless flaky tests to pass
        // assert this.crossesDateline == false;
        this.bbox = bbox;
        scratch = new GeoBoundingBox(new org.elasticsearch.common.geo.GeoPoint(), new org.elasticsearch.common.geo.GeoPoint());
    }

    public boolean validHex(long hex) {
        H3SphericalUtil.computeGeoBounds(hex, scratch);
        if (bbox.top() > scratch.bottom() && bbox.bottom() < scratch.top()) {
            if (scratch.left() > scratch.right()) {
                return intersects(-180, scratch.right()) || intersects(scratch.left(), 180);
            } else {
                return intersects(scratch.left(), scratch.right());
            }
        }
        return false;
    }

    private boolean intersects(double minLon, double maxLon) {
        if (crossesDateline) {
            return bbox.left() < maxLon || bbox.right() > minLon;
        } else {
            return bbox.left() < maxLon && bbox.right() > minLon;
        }
    }
}
