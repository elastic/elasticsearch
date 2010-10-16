/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.search.geo;

import org.elasticsearch.common.unit.DistanceUnit;

/**
 * @author kimchy (shay.banon)
 */
public class LatLng {

    private double lat;
    private double lng;
    private boolean normalized;

    public LatLng(double lat, double lng) {
        if (lat > 90.0 || lat < -90.0) {
            throw new IllegalArgumentException("Illegal latitude value " + lat);
        }
        this.lat = lat;
        this.lng = lng;
    }

    public boolean isNormalized() {
        return normalized || ((lng >= -180) && (lng <= 180));
    }

    public LatLng normalize() {
        if (isNormalized()) {
            return this;
        }

        double delta = 0;
        if (lng < 0) {
            delta = 360;
        } else if (lng >= 0) {
            delta = -360;
        }

        double newLng = lng;
        while (newLng <= -180 || newLng >= 180) {
            newLng += delta;
        }

        LatLng ret = new LatLng(lat, newLng);
        ret.normalized = true;
        return ret;
    }

    public double getLat() {
        return this.lat;
    }

    public double getLng() {
        return this.lng;
    }

    public boolean equals(LatLng other) {
        return lat == other.getLat() && lng == other.getLng();
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof LatLng && equals((LatLng) other);
    }

    /**
     * Calculates the distance between two lat/lng's in miles.
     * Imported from mq java client.
     *
     * @param ll2 Second lat,lng position to calculate distance to.
     * @return Returns the distance in miles.
     */
    public double arcDistance(LatLng ll2) {
        return arcDistance(ll2, DistanceUnit.MILES);
    }

    /**
     * Calculates the distance between two lat/lng's in miles or meters.
     * Imported from mq java client.  Variable references changed to match.
     *
     * @param ll2    Second lat,lng position to calculate distance to.
     * @param lUnits Units to calculate distace, defaults to miles
     * @return Returns the distance in meters or miles.
     */
    public double arcDistance(LatLng ll2, DistanceUnit lUnits) {
        LatLng ll1 = normalize();
        ll2 = ll2.normalize();

        double lat1 = ll1.getLat(), lng1 = ll1.getLng();
        double lat2 = ll2.getLat(), lng2 = ll2.getLng();

        // Check for same position
        if (lat1 == lat2 && lng1 == lng2)
            return 0.0;

        // Get the m_dLongitude diffeernce. Don't need to worry about
        // crossing 180 since cos(x) = cos(-x)
        double dLon = lng2 - lng1;

        double a = radians(90.0 - lat1);
        double c = radians(90.0 - lat2);
        double cosB = (Math.cos(a) * Math.cos(c))
                + (Math.sin(a) * Math.sin(c) * Math.cos(radians(dLon)));

        double radius = (lUnits == DistanceUnit.MILES) ? 3963.205/* MILERADIUSOFEARTH */
                : 6378.160187/* KMRADIUSOFEARTH */;

        // Find angle subtended (with some bounds checking) in radians and
        // multiply by earth radius to find the arc distance
        if (cosB < -1.0)
            return 3.14159265358979323846/* PI */ * radius;
        else if (cosB >= 1.0)
            return 0;
        else
            return Math.acos(cosB) * radius;
    }

    private double radians(double a) {
        return a * 0.01745329251994;
    }
}
