/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.spatial;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.geo.XYEncodingUtils;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.GeometryValidator;
import org.elasticsearch.geometry.utils.SpatialEnvelopeVisitor.WrapLongitude;
import org.elasticsearch.geometry.utils.WellKnownBinary;

class SpatialAggregationUtils {
    private SpatialAggregationUtils() { /* Utility class */ }

    public static Geometry decode(BytesRef wkb) {
        return WellKnownBinary.fromWKB(GeometryValidator.NOOP, false /* coerce */, wkb.bytes, wkb.offset, wkb.length);
    }

    public static Point decodePoint(BytesRef wkb) {
        return (Point) decode(wkb);
    }

    public static double decodeX(long encoded) {
        return XYEncodingUtils.decode(extractFirst(encoded));
    }

    public static int extractFirst(long encoded) {
        return (int) (encoded >>> 32);
    }

    public static double decodeY(long encoded) {
        return XYEncodingUtils.decode(extractSecond(encoded));
    }

    public static int extractSecond(long encoded) {
        return (int) (encoded & 0xFFFFFFFFL);
    }

    public static double decodeLongitude(long encoded) {
        return GeoEncodingUtils.decodeLongitude((int) (encoded & 0xFFFFFFFFL));
    }

    public static double decodeLatitude(long encoded) {
        return GeoEncodingUtils.decodeLatitude((int) (encoded >>> 32));
    }

    public static int encodeNegativeLongitude(double d) {
        return Double.isFinite(d) ? GeoEncodingUtils.encodeLongitude(d) : DEFAULT_NEG;
    }

    public static int encodePositiveLongitude(double d) {
        return Double.isFinite(d) ? GeoEncodingUtils.encodeLongitude(d) : DEFAULT_POS;
    }

    public static Rectangle asRectangle(int minNegX, int minPosX, int maxNegX, int maxPosX, int maxY, int minY) {
        assert minNegX <= 0 == maxNegX <= 0;
        assert minPosX >= 0 == maxPosX >= 0;
        return GeoPointEnvelopeVisitor.asRectangle(
            minNegX <= 0 ? decodeLongitude(minNegX) : Double.POSITIVE_INFINITY,
            minPosX >= 0 ? decodeLongitude(minPosX) : Double.POSITIVE_INFINITY,
            maxNegX <= 0 ? decodeLongitude(maxNegX) : Double.NEGATIVE_INFINITY,
            maxPosX >= 0 ? decodeLongitude(maxPosX) : Double.NEGATIVE_INFINITY,
            GeoEncodingUtils.decodeLatitude(maxY),
            GeoEncodingUtils.decodeLatitude(minY),
            WrapLongitude.WRAP
        );
    }

    public static int maxNeg(int a, int b) {
        return a <= 0 && b <= 0 ? Math.max(a, b) : Math.min(a, b);
    }

    public static int minPos(int a, int b) {
        return a >= 0 && b >= 0 ? Math.min(a, b) : Math.max(a, b);
    }

    // The default values are intentionally non-negative/non-positive, so we can mark unassigned values.
    public static final int DEFAULT_POS = -1;
    public static final int DEFAULT_NEG = 1;
}
