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

/** pair of latitude/longitude */
public final class LatLng {

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
        return Math.atan2(
            Math.cos(lat) * Math.sin(lon - this.lon),
            Math.cos(this.lat) * Math.sin(lat) - Math.sin(this.lat) * Math.cos(lat) * Math.cos(lon - this.lon)
        );
    }
}
