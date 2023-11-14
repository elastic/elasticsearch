/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.util;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.SpatialPoint;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.utils.GeometryValidator;
import org.elasticsearch.geometry.utils.WellKnownText;

import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitude;

public enum SpatialCoordinateTypes {
    Geo {
        public SpatialPoint longAsPoint(long encoded) {
            return new GeoPoint(GeoEncodingUtils.decodeLatitude((int) (encoded >>> 32)), GeoEncodingUtils.decodeLongitude((int) encoded));
        }

        public long pointAsLong(double x, double y) {
            int latitudeEncoded = encodeLatitude(y);
            int longitudeEncoded = encodeLongitude(x);
            return (((long) latitudeEncoded) << 32) | (longitudeEncoded & 0xFFFFFFFFL);
        }

        public SpatialPoint pointAsPoint(Point point) {
            return new GeoPoint(point.getY(), point.getX());
        }
    };

    public abstract SpatialPoint longAsPoint(long encoded);

    public long pointAsLong(SpatialPoint point) {
        return pointAsLong(point.getX(), point.getY());
    }

    public abstract long pointAsLong(double x, double y);

    public String pointAsString(SpatialPoint point) {
        return WellKnownText.toWKT(new Point(point.getX(), point.getY()));
    }

    public SpatialPoint stringAsPoint(String string) {
        try {
            Geometry geometry = WellKnownText.fromWKT(GeometryValidator.NOOP, false, string);
            if (geometry instanceof Point point) {
                return pointAsPoint(point);
            } else {
                throw new IllegalArgumentException("Unsupported geometry type " + geometry.type());
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse WKT: " + e.getMessage(), e);
        }
    }

    public abstract SpatialPoint pointAsPoint(Point point);
}
