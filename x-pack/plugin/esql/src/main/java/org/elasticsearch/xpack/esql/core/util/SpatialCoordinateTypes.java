/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.util;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.geo.XYEncodingUtils;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.GeographyValidator;
import org.elasticsearch.geometry.utils.GeometryValidator;
import org.elasticsearch.geometry.utils.WellKnownBinary;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;

import java.nio.ByteOrder;

import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitude;

public enum SpatialCoordinateTypes {
    GEO {
        @Override
        public double decodeX(long encoded) {
            return GeoEncodingUtils.decodeLongitude((int) encoded);
        }

        @Override
        public double decodeY(long encoded) {
            return GeoEncodingUtils.decodeLatitude((int) (encoded >>> 32));
        }

        @Override
        public long pointAsLong(double x, double y) {
            int latitudeEncoded = encodeLatitude(y);
            int longitudeEncoded = encodeLongitude(x);
            return (((long) latitudeEncoded) << 32) | (longitudeEncoded & 0xFFFFFFFFL);
        }

        @Override
        public GeometryValidator validator() {
            // We validate the lat/lon values, and ignore any z values
            return GeographyValidator.instance(true);
        }
    },
    CARTESIAN {

        private static final int MAX_VAL_ENCODED = XYEncodingUtils.encode((float) XYEncodingUtils.MAX_VAL_INCL);
        private static final int MIN_VAL_ENCODED = XYEncodingUtils.encode((float) XYEncodingUtils.MIN_VAL_INCL);

        @Override
        public double decodeX(long encoded) {
            final int x = checkCoordinate((int) (encoded >>> 32));
            return XYEncodingUtils.decode(x);
        }

        @Override
        public double decodeY(long encoded) {
            final int y = checkCoordinate((int) (encoded & 0xFFFFFFFF));
            return XYEncodingUtils.decode(y);
        }

        private int checkCoordinate(int i) {
            if (i > MAX_VAL_ENCODED || i < MIN_VAL_ENCODED) {
                throw new IllegalArgumentException("Failed to convert invalid encoded value to cartesian point");
            }
            return i;
        }

        @Override
        public long pointAsLong(double x, double y) {
            final long xi = XYEncodingUtils.encode((float) x);
            final long yi = XYEncodingUtils.encode((float) y);
            return (yi & 0xFFFFFFFFL) | xi << 32;
        }
    },
    UNSPECIFIED {
        @Override
        public double decodeX(long encoded) {
            throw new UnsupportedOperationException("Cannot convert long to point without specifying coordinate type");
        }

        @Override
        public double decodeY(long encoded) {
            throw new UnsupportedOperationException("Cannot convert long to point without specifying coordinate type");
        }

        @Override
        public long pointAsLong(double x, double y) {
            throw new UnsupportedOperationException("Cannot convert point to long without specifying coordinate type");
        }
    };

    protected GeometryValidator validator() {
        return GeometryValidator.NOOP;
    }

    public abstract double decodeX(long encoded);

    public abstract double decodeY(long encoded);

    public Point longAsPoint(long encoded) {
        return new Point(decodeX(encoded), decodeY(encoded));
    }

    public abstract long pointAsLong(double x, double y);

    public long wkbAsLong(BytesRef wkb) {
        Point point = wkbAsPoint(wkb);
        return pointAsLong(point.getX(), point.getY());
    }

    public Point wkbAsPoint(BytesRef wkb) {
        Geometry geometry = WellKnownBinary.fromWKB(validator(), false, wkb.bytes, wkb.offset, wkb.length);
        if (geometry instanceof Point point) {
            return point;
        } else {
            throw new IllegalArgumentException("Unsupported geometry: " + geometry.type());
        }
    }

    public BytesRef longAsWkb(long encoded) {
        return asWkb(longAsPoint(encoded));
    }

    public String asWkt(Geometry geometry) {
        return WellKnownText.toWKT(geometry);
    }

    public BytesRef asWkb(Geometry geometry) {
        return new BytesRef(WellKnownBinary.toWKB(geometry, ByteOrder.LITTLE_ENDIAN));
    }

    public BytesRef wktToWkb(String wkt) {
        // TODO: we should be able to transform WKT to WKB without building the geometry
        // we should as well use different validator for cartesian and geo?
        try {
            Geometry geometry = WellKnownText.fromWKT(validator(), false, wkt);
            return new BytesRef(WellKnownBinary.toWKB(geometry, ByteOrder.LITTLE_ENDIAN));
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse WKT: " + e.getMessage(), e);
        }
    }

    public String wkbToWkt(BytesRef wkb) {
        return WellKnownText.fromWKB(wkb.bytes, wkb.offset, wkb.length);
    }

    public Geometry wkbToGeometry(BytesRef wkb) {
        return WellKnownBinary.fromWKB(validator(), false, wkb.bytes, wkb.offset, wkb.length);
    }

    public org.locationtech.jts.geom.Geometry wkbToJtsGeometry(BytesRef wkb) throws ParseException, IllegalArgumentException {
        String wkt = wkbToWkt(wkb);
        if (wkt.startsWith("BBOX")) {
            Geometry geometry = WellKnownBinary.fromWKB(GeometryValidator.NOOP, true, wkb.bytes, wkb.offset, wkb.length);
            if (geometry instanceof Rectangle rect) {
                var bottomLeft = new Coordinate(rect.getMinX(), rect.getMinY());
                var bottomRight = new Coordinate(rect.getMaxX(), rect.getMinY());
                var topRight = new Coordinate(rect.getMaxX(), rect.getMaxY());
                var topLeft = new Coordinate(rect.getMinX(), rect.getMaxY());

                var coordinates = new Coordinate[] { bottomLeft, bottomRight, topRight, topLeft, bottomLeft };
                var geomFactory = new GeometryFactory();
                var linearRing = new LinearRing(new CoordinateArraySequence(coordinates), geomFactory);
                return new Polygon(linearRing, null, geomFactory);
            }
        }
        WKTReader reader = new WKTReader();
        return reader.read(wkt);
    }

    public BytesRef jtsGeometryToWkb(org.locationtech.jts.geom.Geometry jtsGeometry) {
        WKTWriter writer = new WKTWriter();
        String wkt = writer.write(jtsGeometry);
        return wktToWkb(wkt);
    }

}
