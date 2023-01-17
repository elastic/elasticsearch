/*
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * This project is based on a modification of https://github.com/uber/h3 which is licensed under the Apache 2.0 License.
 *
 * Copyright 2016-2021 Uber Technologies, Inc.
 */
package org.elasticsearch.h3;

import java.util.Objects;

/** pair of latitude/longitude */
public final class LatLng {

    /** Minimum Angular resolution. */
    public static final double MINIMUM_ANGULAR_RESOLUTION = Math.PI * 1.0e-12; // taken from lucene's spatial3d

    // lat / lon in radians
    private final double lon;
    private final double lat;

    LatLng(double lat, double lon) {
        this.lon = lon;
        this.lat = lat;
    }

    /** Returns latitude in radians */
    public double getLatRad() {
        return lat;
    }

    /** Returns longitude in radians */
    public double getLonRad() {
        return lon;
    }

    /** Returns latitude in degrees */
    public double getLatDeg() {
        return Math.toDegrees(getLatRad());
    }

    /** Returns longitude in degrees */
    public double getLonDeg() {
        return Math.toDegrees(getLonRad());
    }

    /**
     * Determines the azimuth to the provided LatLng in radians.
     *
     * @param lat The latitude in radians.
     * @param lon The longitude in radians.
     * @return The azimuth in radians.
     */
    double geoAzimuthRads(double lat, double lon) {
        final double cosLat = FastMath.cos(lat);
        return FastMath.atan2(
            cosLat * FastMath.sin(lon - this.lon),
            FastMath.cos(this.lat) * FastMath.sin(lat) - FastMath.sin(this.lat) * cosLat * FastMath.cos(lon - this.lon)
        );
    }

    /**
     * Determines the maximum latitude of the great circle defined by this LatLng to the provided LatLng.
     *
     * @param latLng The LatLng.
     * @return The maximum latitude of the great circle in radians.
     */
    public double greatCircleMaxLatitude(LatLng latLng) {
        if (isNumericallyIdentical(latLng)) {
            return latLng.lat;
        }
        return latLng.lat > this.lat ? greatCircleMaxLatitude(latLng, this) : greatCircleMaxLatitude(this, latLng);
    }

    private static double greatCircleMaxLatitude(LatLng latLng1, LatLng latLng2) {
        // we compute the max latitude using Clairaut's formula (https://streckenflug.at/download/formeln.pdf)
        assert latLng1.lat >= latLng2.lat;
        final double az = latLng1.geoAzimuthRads(latLng2.lat, latLng2.lon);
        // the great circle contains the maximum latitude only if the azimuth is between -90 and 90 degrees.
        if (Math.abs(az) < Math.PI / 2) {
            return FastMath.acos(Math.abs(FastMath.sin(az) * FastMath.cos(latLng1.lat)));
        }
        return latLng1.lat;
    }

    /**
     * Determines the minimum latitude of the great circle defined by this LatLng to the provided LatLng.
     *
     * @param latLng The LatLng.
     * @return The minimum latitude of the great circle in radians.
     */
    public double greatCircleMinLatitude(LatLng latLng) {
        if (isNumericallyIdentical(latLng)) {
            return latLng.lat;
        }
        return latLng.lat < this.lat ? greatCircleMinLatitude(latLng, this) : greatCircleMinLatitude(this, latLng);
    }

    private static double greatCircleMinLatitude(LatLng latLng1, LatLng latLng2) {
        assert latLng1.lat <= latLng2.lat;
        // we compute the min latitude using Clairaut's formula (https://streckenflug.at/download/formeln.pdf)
        final double az = latLng1.geoAzimuthRads(latLng2.lat, latLng2.lon);
        // the great circle contains the minimum latitude only if the azimuth is not between -90 and 90 degrees.
        if (Math.abs(az) > Math.PI / 2) {
            // note the sign
            return -FastMath.acos(Math.abs(FastMath.sin(az) * FastMath.cos(latLng1.lat)));
        }
        return latLng1.lat;
    }

    private boolean isNumericallyIdentical(LatLng latLng) {
        return Math.abs(this.lat - latLng.lat) < MINIMUM_ANGULAR_RESOLUTION && Math.abs(this.lon - latLng.lon) < MINIMUM_ANGULAR_RESOLUTION;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final LatLng latLng = (LatLng) o;
        return Double.compare(latLng.lon, lon) == 0 && Double.compare(latLng.lat, lat) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(lon, lat);
    }
}
