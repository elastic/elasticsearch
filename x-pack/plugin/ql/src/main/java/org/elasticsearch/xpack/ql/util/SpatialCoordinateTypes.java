/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.util;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.geo.XYEncodingUtils;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.SpatialPoint;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.utils.GeometryValidator;
import org.elasticsearch.geometry.utils.WellKnownBinary;
import org.elasticsearch.geometry.utils.WellKnownText;

import java.nio.ByteOrder;

import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitude;

public enum SpatialCoordinateTypes {
    GEO {
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

        public SpatialPoint pointAsPoint(SpatialPoint point) {
            return new GeoPoint(point);
        }
    },
    CARTESIAN {
        public SpatialPoint longAsPoint(long encoded) {
            try {
                final double x = XYEncodingUtils.decode((int) (encoded >>> 32));
                final double y = XYEncodingUtils.decode((int) (encoded & 0xFFFFFFFF));
                return new SpatialPoint(x, y);
            } catch (Error e) {
                throw new IllegalArgumentException("Failed to convert invalid encoded value to cartesian point");
            }
        }

        public long pointAsLong(double x, double y) {
            final long xi = XYEncodingUtils.encode((float) x);
            final long yi = XYEncodingUtils.encode((float) y);
            return (yi & 0xFFFFFFFFL) | xi << 32;
        }

        public SpatialPoint pointAsPoint(Point point) {
            return new SpatialPoint(point.getX(), point.getY());
        }
    };

    public abstract SpatialPoint longAsPoint(long encoded);

    public long pointAsLong(SpatialPoint point) {
        return pointAsLong(point.getX(), point.getY());
    }

    public abstract long pointAsLong(double x, double y);

    public String pointAsString(SpatialPoint point) {
        return point.toWKT();
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
            throw new IllegalArgumentException("Failed to parse WKT: " + e.getMessage(), e);
        }
    }

    public abstract SpatialPoint pointAsPoint(Point point);

    /**
     * Convert point to the correct class for the upper column type. For example, create a GeoPoint from a cartesian point.
     */
    public SpatialPoint pointAsPoint(SpatialPoint point) {
        return point;
    }

    public BytesRef stringAsWKB(String string) {
        try {
            Geometry geometry = WellKnownText.fromWKT(GeometryValidator.NOOP, false, string);
            return new BytesRef(WellKnownBinary.toWKB(geometry, ByteOrder.LITTLE_ENDIAN));
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse WKT: " + e.getMessage(), e);
        }
    }

    public BytesRef pointAsWKB(SpatialPoint point) {
        return new BytesRef(WellKnownBinary.toWKB(new Point(point.getX(), point.getY()), ByteOrder.LITTLE_ENDIAN));
    }

    public BytesRef longAsWKB(long encoded) {
        return pointAsWKB(longAsPoint(encoded));
    }

    public long wkbAsLong(BytesRef wkb) {
        Geometry geometry = WellKnownBinary.fromWKB(GeometryValidator.NOOP, false, wkb.bytes, wkb.offset, wkb.length);
        if (geometry instanceof Point point) {
            return pointAsLong(point.getX(), point.getY());
        } else {
            throw new IllegalArgumentException("Unsupported geometry: " + geometry.type());
        }
    }

    public String wkbAsString(BytesRef wkb) {
        Geometry geometry = WellKnownBinary.fromWKB(GeometryValidator.NOOP, false, wkb.bytes, wkb.offset, wkb.length);
        if (geometry instanceof Point point) {
            // WellKnownText.toWKT renders points slightly differently to SpatialPoint.toWKT
            return pointAsString(pointAsPoint(point));
        }
        return WellKnownText.toWKT(geometry);
    }

    public SpatialPoint wkbAsPoint(BytesRef wkb) {
        Geometry geometry = WellKnownBinary.fromWKB(GeometryValidator.NOOP, false, wkb.bytes, wkb.offset, wkb.length);
        if (geometry instanceof Point point) {
            return pointAsPoint(point);
        } else {
            throw new IllegalArgumentException("Unsupported geometry: " + geometry.type());
        }
    }
}
