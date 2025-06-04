/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.geo;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.util.BitUtil;
import org.elasticsearch.common.geo.GeoUtils.EffectivePoint;
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.ShapeType;
import org.elasticsearch.geometry.utils.GeographyValidator;
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Locale;

public final class GeoPoint implements SpatialPoint, ToXContentFragment {

    private double lat;
    private double lon;

    public GeoPoint() {}

    /**
     * Create a new Geopoint from a string. This String must either be a geohash
     * or a lat-lon tuple.
     *
     * @param value String to create the point from
     */
    public GeoPoint(String value) {
        this.resetFromString(value);
    }

    public GeoPoint(double lat, double lon) {
        this.lat = lat;
        this.lon = lon;
    }

    public GeoPoint(SpatialPoint template) {
        this(template.getY(), template.getX());
    }

    public GeoPoint reset(double lat, double lon) {
        this.lat = lat;
        this.lon = lon;
        return this;
    }

    public GeoPoint resetLat(double lat) {
        this.lat = lat;
        return this;
    }

    public GeoPoint resetLon(double lon) {
        this.lon = lon;
        return this;
    }

    public GeoPoint resetFromString(String value) {
        return resetFromString(value, false, EffectivePoint.BOTTOM_LEFT);
    }

    public GeoPoint resetFromString(String value, final boolean ignoreZValue, EffectivePoint effectivePoint) {
        if (value.toLowerCase(Locale.ROOT).contains("point")) {
            return resetFromWKT(value, ignoreZValue);
        } else if (value.contains(",")) {
            return resetFromCoordinates(value, ignoreZValue);
        }
        return parseGeoHash(value, effectivePoint);
    }

    public GeoPoint resetFromCoordinates(String value, final boolean ignoreZValue) {
        String[] vals = value.split(",");
        if (vals.length > 3) {
            throw new ElasticsearchParseException("failed to parse [{}], expected 2 or 3 coordinates " + "but found: [{}]", vals.length);
        }
        final double lat;
        final double lon;
        try {
            lat = Double.parseDouble(vals[0].trim());
        } catch (NumberFormatException ex) {
            throw new ElasticsearchParseException("latitude must be a number");
        }
        try {
            lon = Double.parseDouble(vals[1].trim());
        } catch (NumberFormatException ex) {
            throw new ElasticsearchParseException("longitude must be a number");
        }
        if (vals.length > 2) {
            GeoPoint.assertZValue(ignoreZValue, Double.parseDouble(vals[2].trim()));
        }
        return reset(lat, lon);
    }

    private GeoPoint resetFromWKT(String value, boolean ignoreZValue) {
        Geometry geometry;
        try {
            geometry = WellKnownText.fromWKT(GeographyValidator.instance(ignoreZValue), false, value);
        } catch (Exception e) {
            throw new ElasticsearchParseException("Invalid WKT format", e);
        }
        if (geometry.type() != ShapeType.POINT) {
            throw new ElasticsearchParseException(
                "[geo_point] supports only POINT among WKT primitives, " + "but found " + geometry.type()
            );
        }
        Point point = (Point) geometry;
        return reset(point.getY(), point.getX());
    }

    GeoPoint parseGeoHash(String geohash, EffectivePoint effectivePoint) {
        if (effectivePoint == EffectivePoint.BOTTOM_LEFT) {
            return resetFromGeoHash(geohash);
        } else {
            Rectangle rectangle = Geohash.toBoundingBox(geohash);
            return switch (effectivePoint) {
                case TOP_LEFT -> reset(rectangle.getMaxY(), rectangle.getMinX());
                case TOP_RIGHT -> reset(rectangle.getMaxY(), rectangle.getMaxX());
                case BOTTOM_RIGHT -> reset(rectangle.getMinY(), rectangle.getMaxX());
                default -> throw new IllegalArgumentException("Unsupported effective point " + effectivePoint);
            };
        }
    }

    public GeoPoint resetFromIndexHash(long hash) {
        lon = Geohash.decodeLongitude(hash);
        lat = Geohash.decodeLatitude(hash);
        return this;
    }

    public GeoPoint resetFromGeoHash(String geohash) {
        final long hash;
        try {
            hash = Geohash.mortonEncode(geohash);
        } catch (IllegalArgumentException ex) {
            throw new ElasticsearchParseException(ex.getMessage(), ex);
        }
        return this.reset(Geohash.decodeLatitude(hash), Geohash.decodeLongitude(hash));
    }

    public GeoPoint resetFromGeoHash(long geohashLong) {
        final int level = (int) (12 - (geohashLong & 15));
        return this.resetFromIndexHash(BitUtil.flipFlop((geohashLong >>> 4) << ((level * 5) + 2)));
    }

    public double lat() {
        return this.lat;
    }

    public double getLat() {
        return this.lat;
    }

    public double lon() {
        return this.lon;
    }

    public double getLon() {
        return this.lon;
    }

    @Override
    public double getX() {
        return this.lon;
    }

    @Override
    public double getY() {
        return this.lat;
    }

    public String geohash() {
        return Geohash.stringEncode(lon, lat);
    }

    public String getGeohash() {
        return Geohash.stringEncode(lon, lat);
    }

    /** Return the point in Lucene encoded format used to stored points as doc values */
    public long getEncoded() {
        final int latitudeEncoded = GeoEncodingUtils.encodeLatitude(this.lat);
        final int longitudeEncoded = GeoEncodingUtils.encodeLongitude(this.lon);
        return (((long) latitudeEncoded) << 32) | (longitudeEncoded & 0xFFFFFFFFL);
    }

    /** reset the point using Lucene encoded format used to stored points as doc values */
    public GeoPoint resetFromEncoded(long encoded) {
        return resetFromEncoded((int) (encoded >>> 32), (int) encoded);
    }

    /** reset the point using Lucene encoded format for lat and lon */
    private GeoPoint resetFromEncoded(int encodedLat, int encodedLon) {
        return reset(GeoEncodingUtils.decodeLatitude(encodedLat), GeoEncodingUtils.decodeLongitude(encodedLon));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GeoPoint geoPoint = (GeoPoint) o;

        if (Double.compare(geoPoint.lat, lat) != 0) return false;
        if (Double.compare(geoPoint.lon, lon) != 0) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = lat != +0.0d ? Double.doubleToLongBits(lat) : 0L;
        result = Long.hashCode(temp);
        temp = lon != +0.0d ? Double.doubleToLongBits(lon) : 0L;
        result = 31 * result + Long.hashCode(temp);
        return result;
    }

    @Override
    public String toString() {
        return lat + ", " + lon;
    }

    public static GeoPoint fromGeohash(String geohash) {
        return new GeoPoint().resetFromGeoHash(geohash);
    }

    public static GeoPoint fromGeohash(long geohashLong) {
        return new GeoPoint().resetFromGeoHash(geohashLong);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.latlon(lat, lon);
    }

    public static double assertZValue(final boolean ignoreZValue, double zValue) {
        if (ignoreZValue == false) {
            throw new ElasticsearchParseException(
                "Exception parsing coordinates: found Z value [{}] but [ignore_z_value] " + "parameter is [{}]",
                zValue,
                ignoreZValue
            );
        }
        return zValue;
    }
}
