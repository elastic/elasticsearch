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

package org.elasticsearch.common.lucene.geo;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.unit.DistanceUnit;

/**
 * Geo distance calculation.
 *
 * @author kimchy (shay.banon)
 */
public enum GeoDistance {
    /**
     * Calculates distance as points on a plane. Faster, but less accurate than {@link #ARC}.
     */
    PLANE() {
        private final static double EARTH_CIRCUMFERENCE_MILES = 24901;
        private final static double DISTANCE_PER_DEGREE = EARTH_CIRCUMFERENCE_MILES / 360;

        @Override public double calculate(double sourceLatitude, double sourceLongitude, double targetLatitude, double targetLongitude, DistanceUnit unit) {
            double px = targetLongitude - sourceLongitude;
            double py = targetLatitude - sourceLatitude;
            double distanceMiles = Math.sqrt(px * px + py * py) * DISTANCE_PER_DEGREE;
            return DistanceUnit.convert(distanceMiles, DistanceUnit.MILES, unit);
        }
    },
    /**
     * Calculates distance as points in a globe.
     */
    ARC() {
        @Override public double calculate(double sourceLatitude, double sourceLongitude, double targetLatitude, double targetLongitude, DistanceUnit unit) {
            LatLng sourcePoint = new LatLng(sourceLatitude, sourceLongitude);
            LatLng targetPoint = new LatLng(targetLatitude, targetLongitude);
            return DistanceUnit.convert(sourcePoint.arcDistance(targetPoint, DistanceUnit.MILES), DistanceUnit.MILES, unit);
        }};

    public abstract double calculate(double sourceLatitude, double sourceLongitude, double targetLatitude, double targetLongitude, DistanceUnit unit);

    public static GeoDistance fromString(String s) {
        if ("plane".equals(s)) {
            return PLANE;
        } else if ("arc".equals(s)) {
            return ARC;
        }
        throw new ElasticSearchIllegalArgumentException("No geo distance for [" + s + "]");
    }
}
