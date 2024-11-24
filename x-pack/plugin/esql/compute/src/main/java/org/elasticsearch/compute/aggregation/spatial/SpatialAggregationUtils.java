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
import org.elasticsearch.geometry.utils.WellKnownBinary;

class SpatialAggregationUtils {
    public static Geometry decode(BytesRef wkb) {
        return WellKnownBinary.fromWKB(GeometryValidator.NOOP, false, wkb.bytes, wkb.offset, wkb.length);
    }

    public static Point decodePoint(BytesRef wkb) {
        return (Point) decode(wkb);
    }

    public static Rectangle decodeRectangle(BytesRef wkb) {
        return (Rectangle) decode(wkb);
    }

    public static double decodeX(long encoded) {
        return XYEncodingUtils.decode((int) (encoded >>> 32));
    }

    public static double decodeY(long encoded) {
        return XYEncodingUtils.decode((int) (encoded & 0xFFFFFFFFL));
    }

    public static double decodeLongitude(long encoded) {
        return GeoEncodingUtils.decodeLongitude((int) (encoded & 0xFFFFFFFFL));
    }

    public static double decodeLatitude(long encoded) {
        return GeoEncodingUtils.decodeLatitude((int) (encoded >>> 32));
    }

    private SpatialAggregationUtils() { /* Utility class */ }
}
