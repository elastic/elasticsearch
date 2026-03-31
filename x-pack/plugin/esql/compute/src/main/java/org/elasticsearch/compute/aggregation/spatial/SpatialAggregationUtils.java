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
import org.elasticsearch.geometry.utils.GeometryValidator;
import org.elasticsearch.geometry.utils.WellKnownBinary;

public class SpatialAggregationUtils {
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

    public static int encodeLongitude(double d) {
        return Double.isFinite(d) ? GeoEncodingUtils.encodeLongitude(d) : encodeInfinity(d);
    }

    private static int encodeInfinity(double d) {
        return d == Double.NEGATIVE_INFINITY ? Integer.MIN_VALUE : Integer.MAX_VALUE;
    }

    public static int maxNeg(int a, int b) {
        return a <= 0 && b <= 0 ? Math.max(a, b) : Math.min(a, b);
    }

    public static int minPos(int a, int b) {
        return a >= 0 && b >= 0 ? Math.min(a, b) : Math.max(a, b);
    }
}
