/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.spatial;

import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.SpatialEnvelopeVisitor;
import org.elasticsearch.geometry.utils.SpatialEnvelopeVisitor.WrapLongitude;

class GeoPointEnvelopeVisitor extends SpatialEnvelopeVisitor.GeoPointVisitor {
    GeoPointEnvelopeVisitor() {
        super(WrapLongitude.WRAP);
    }

    void reset() {
        minY = Double.POSITIVE_INFINITY;
        maxY = Double.NEGATIVE_INFINITY;
        minNegX = Double.POSITIVE_INFINITY;
        maxNegX = Double.NEGATIVE_INFINITY;
        minPosX = Double.POSITIVE_INFINITY;
        maxPosX = Double.NEGATIVE_INFINITY;
    }

    double getMinNegX() {
        return minNegX;
    }

    double getMinPosX() {
        return minPosX;
    }

    double getMaxNegX() {
        return maxNegX;
    }

    double getMaxPosX() {
        return maxPosX;
    }

    double getMaxY() {
        return maxY;
    }

    double getMinY() {
        return minY;
    }

    static Rectangle asRectangle(
        double minNegX,
        double minPosX,
        double maxNegX,
        double maxPosX,
        double maxY,
        double minY,
        WrapLongitude wrapLongitude
    ) {
        return SpatialEnvelopeVisitor.GeoPointVisitor.getResult(minNegX, minPosX, maxNegX, maxPosX, maxY, minY, wrapLongitude);
    }
}
