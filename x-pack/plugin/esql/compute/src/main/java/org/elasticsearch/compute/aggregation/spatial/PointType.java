/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.spatial;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.geo.XYEncodingUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.SpatialEnvelopeVisitor;

import java.util.Optional;

enum PointType {
    GEO,
    CARTESIAN;

    double decodeX(int encoded) {
        return switch (this) {
            case GEO -> GeoEncodingUtils.decodeLongitude(encoded);
            case CARTESIAN -> XYEncodingUtils.decode(encoded);
        };
    }

    double decodeY(int encoded) {
        return switch (this) {
            case GEO -> GeoEncodingUtils.decodeLatitude(encoded);
            case CARTESIAN -> XYEncodingUtils.decode(encoded);
        };
    }

    int encodeX(double decoded) {
        return switch (this) {
            case GEO -> GeoEncodingUtils.encodeLongitude(decoded);
            case CARTESIAN -> XYEncodingUtils.encode((float) decoded);
        };
    }

    int encodeY(double decoded) {
        return switch (this) {
            case GEO -> GeoEncodingUtils.encodeLatitude(decoded);
            case CARTESIAN -> XYEncodingUtils.encode((float) decoded);
        };
    }

    // FIXME (gal) document
    public int extractX(long encoded) {
        return switch (this) {
            case GEO -> SpatialAggregationUtils.extractY(encoded);
            case CARTESIAN -> SpatialAggregationUtils.extractX(encoded);
        };
    }

    public int extractY(long encoded) {
        return switch (this) {
            case GEO -> SpatialAggregationUtils.extractX(encoded);
            case CARTESIAN -> SpatialAggregationUtils.extractY(encoded);
        };
    }

    public Optional<Rectangle> computeEnvelope(Geometry geo) {
        return switch (this) {
            case GEO -> SpatialEnvelopeVisitor.visitGeo(geo, false);
            case CARTESIAN -> SpatialEnvelopeVisitor.visitCartesian(geo);
        };
    }
}
