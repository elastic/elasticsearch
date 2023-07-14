/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.util;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.utils.GeometryValidator;
import org.elasticsearch.geometry.utils.WellKnownText;

import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitude;

public class SpatialUtils {

    public static GeoPoint longAsGeoPoint(long encoded) {
        return new GeoPoint(GeoEncodingUtils.decodeLatitude((int) (encoded >>> 32)), GeoEncodingUtils.decodeLongitude((int) encoded));
    }

    public static long geoPointAsLong(GeoPoint point) {
        int latitudeEncoded = encodeLatitude(point.lat());
        int longitudeEncoded = encodeLongitude(point.lon());
        return (((long) latitudeEncoded) << 32) | (longitudeEncoded & 0xFFFFFFFFL);
    }

    public static String geoPointAsString(GeoPoint point) {
        return WellKnownText.toWKT(new Point(point.getX(), point.getY()));
    }

    public static GeoPoint stringAsGeoPoint(String string) {
        try {
            Geometry geometry = WellKnownText.fromWKT(GeometryValidator.NOOP, false, string);
            if (geometry instanceof Point point) {
                return new GeoPoint(point.getY(), point.getX());
            } else {
                throw new IllegalArgumentException("Unsupported geometry type " + geometry.type());
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse WKT: " + e.getMessage(), e);
        }
    }

    public static Geometry stringAsGeometry(String string) {
        try {
            Geometry geometry = WellKnownText.fromWKT(GeometryValidator.NOOP, false, string);
            // TODO map to appropriate type for ESQL
            return geometry;
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse WKT: " + e.getMessage(), e);
        }
    }
}
