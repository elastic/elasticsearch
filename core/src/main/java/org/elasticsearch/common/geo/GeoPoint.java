/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.geo;

import org.apache.lucene.spatial.geopoint.document.GeoPointField;
import org.apache.lucene.util.BitUtil;

import static org.elasticsearch.common.geo.GeoHashUtils.mortonEncode;
import static org.elasticsearch.common.geo.GeoHashUtils.stringEncode;

/**
 *
 */
public final class GeoPoint {

    private double lat;
    private double lon;

    public GeoPoint() {
    }

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

    public GeoPoint(GeoPoint template) {
        this(template.getLat(), template.getLon());
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
        int comma = value.indexOf(',');
        if (comma != -1) {
            lat = Double.parseDouble(value.substring(0, comma).trim());
            lon = Double.parseDouble(value.substring(comma + 1).trim());
        } else {
            resetFromGeoHash(value);
        }
        return this;
    }

    public GeoPoint resetFromIndexHash(long hash) {
        lon = GeoPointField.decodeLongitude(hash);
        lat = GeoPointField.decodeLatitude(hash);
        return this;
    }

    public GeoPoint resetFromGeoHash(String geohash) {
        final long hash = mortonEncode(geohash);
        return this.reset(GeoPointField.decodeLatitude(hash), GeoPointField.decodeLongitude(hash));
    }

    public GeoPoint resetFromGeoHash(long geohashLong) {
        final int level = (int)(12 - (geohashLong&15));
        return this.resetFromIndexHash(BitUtil.flipFlop((geohashLong >>> 4) << ((level * 5) + 2)));
    }

    public final double lat() {
        return this.lat;
    }

    public final double getLat() {
        return this.lat;
    }

    public final double lon() {
        return this.lon;
    }

    public final double getLon() {
        return this.lon;
    }

    public final String geohash() {
        return stringEncode(lon, lat);
    }

    public final String getGeohash() {
        return stringEncode(lon, lat);
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

    public static GeoPoint parseFromLatLon(String latLon) {
        GeoPoint point = new GeoPoint(latLon);
        return point;
    }

    public static GeoPoint fromGeohash(String geohash) {
        return new GeoPoint().resetFromGeoHash(geohash);
    }

    public static GeoPoint fromGeohash(long geohashLong) {
        return new GeoPoint().resetFromGeoHash(geohashLong);
    }
}
