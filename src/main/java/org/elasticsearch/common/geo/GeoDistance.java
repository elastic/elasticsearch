/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.common.geo;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.unit.DistanceUnit;

/**
 * Geo distance calculation.
 */
public enum GeoDistance {
    /**
     * Calculates distance as points on a plane. Faster, but less accurate than {@link #ARC}.
     */
    PLANE() {
        @Override
        public double calculate(double sourceLatitude, double sourceLongitude, double targetLatitude, double targetLongitude, DistanceUnit unit) {
            double px = targetLongitude - sourceLongitude;
            double py = targetLatitude - sourceLatitude;
            return Math.sqrt(px * px + py * py) * unit.getDistancePerDegree();
        }

        @Override
        public double normalize(double distance, DistanceUnit unit) {
            return distance;
        }

        @Override
        public FixedSourceDistance fixedSourceDistance(double sourceLatitude, double sourceLongitude, DistanceUnit unit) {
            return new PlaneFixedSourceDistance(sourceLatitude, sourceLongitude, unit);
        }
    },
    /**
     * Calculates distance factor.
     */
    FACTOR() {
        @Override
        public double calculate(double sourceLatitude, double sourceLongitude, double targetLatitude, double targetLongitude, DistanceUnit unit) {
            double longitudeDifference = targetLongitude - sourceLongitude;
            double a = Math.toRadians(90D - sourceLatitude);
            double c = Math.toRadians(90D - targetLatitude);
            return (Math.cos(a) * Math.cos(c)) + (Math.sin(a) * Math.sin(c) * Math.cos(Math.toRadians(longitudeDifference)));
        }

        @Override
        public double normalize(double distance, DistanceUnit unit) {
            return Math.cos(distance / unit.getEarthRadius());
        }

        @Override
        public FixedSourceDistance fixedSourceDistance(double sourceLatitude, double sourceLongitude, DistanceUnit unit) {
            return new FactorFixedSourceDistance(sourceLatitude, sourceLongitude, unit);
        }
    },
    /**
     * Calculates distance as points in a globe.
     */
    ARC() {
        @Override
        public double calculate(double sourceLatitude, double sourceLongitude, double targetLatitude, double targetLongitude, DistanceUnit unit) {
            double longitudeDifference = targetLongitude - sourceLongitude;
            double a = Math.toRadians(90D - sourceLatitude);
            double c = Math.toRadians(90D - targetLatitude);
            double factor = (Math.cos(a) * Math.cos(c)) + (Math.sin(a) * Math.sin(c) * Math.cos(Math.toRadians(longitudeDifference)));

            if (factor < -1D) {
                return Math.PI * unit.getEarthRadius();
            } else if (factor >= 1D) {
                return 0;
            } else {
                return Math.acos(factor) * unit.getEarthRadius();
            }
        }

        @Override
        public double normalize(double distance, DistanceUnit unit) {
            return distance;
        }

        @Override
        public FixedSourceDistance fixedSourceDistance(double sourceLatitude, double sourceLongitude, DistanceUnit unit) {
            return new ArcFixedSourceDistance(sourceLatitude, sourceLongitude, unit);
        }
    };

    public abstract double normalize(double distance, DistanceUnit unit);

    public abstract double calculate(double sourceLatitude, double sourceLongitude, double targetLatitude, double targetLongitude, DistanceUnit unit);

    public abstract FixedSourceDistance fixedSourceDistance(double sourceLatitude, double sourceLongitude, DistanceUnit unit);

    private static final double MIN_LAT = Math.toRadians(-90d);  // -PI/2
    private static final double MAX_LAT = Math.toRadians(90d);   //  PI/2
    private static final double MIN_LON = Math.toRadians(-180d); // -PI
    private static final double MAX_LON = Math.toRadians(180d);  //  PI

    public static DistanceBoundingCheck distanceBoundingCheck(double sourceLatitude, double sourceLongitude, double distance, DistanceUnit unit) {
        // angular distance in radians on a great circle
        double radDist = distance / unit.getEarthRadius();

        double radLat = Math.toRadians(sourceLatitude);
        double radLon = Math.toRadians(sourceLongitude);

        double minLat = radLat - radDist;
        double maxLat = radLat + radDist;

        double minLon, maxLon;
        if (minLat > MIN_LAT && maxLat < MAX_LAT) {
            double deltaLon = Math.asin(Math.sin(radDist) / Math.cos(radLat));
            minLon = radLon - deltaLon;
            if (minLon < MIN_LON) minLon += 2d * Math.PI;
            maxLon = radLon + deltaLon;
            if (maxLon > MAX_LON) maxLon -= 2d * Math.PI;
        } else {
            // a pole is within the distance
            minLat = Math.max(minLat, MIN_LAT);
            maxLat = Math.min(maxLat, MAX_LAT);
            minLon = MIN_LON;
            maxLon = MAX_LON;
        }

        GeoPoint topLeft = new GeoPoint(Math.toDegrees(maxLat), Math.toDegrees(minLon));
        GeoPoint bottomRight = new GeoPoint(Math.toDegrees(minLat), Math.toDegrees(maxLon));
        if (minLon > maxLon) {
            return new Meridian180DistanceBoundingCheck(topLeft, bottomRight);
        }
        return new SimpleDistanceBoundingCheck(topLeft, bottomRight);
    }

    public static GeoDistance fromString(String s) {
        if ("plane".equals(s)) {
            return PLANE;
        } else if ("arc".equals(s)) {
            return ARC;
        } else if ("factor".equals(s)) {
            return FACTOR;
        }
        throw new ElasticSearchIllegalArgumentException("No geo distance for [" + s + "]");
    }

    public static interface FixedSourceDistance {

        double calculate(double targetLatitude, double targetLongitude);
    }

    public static interface DistanceBoundingCheck {

        boolean isWithin(double targetLatitude, double targetLongitude);

        GeoPoint topLeft();

        GeoPoint bottomRight();
    }

    public static AlwaysDistanceBoundingCheck ALWAYS_INSTANCE = new AlwaysDistanceBoundingCheck();

    private static class AlwaysDistanceBoundingCheck implements DistanceBoundingCheck {
        @Override
        public boolean isWithin(double targetLatitude, double targetLongitude) {
            return true;
        }

        @Override
        public GeoPoint topLeft() {
            return null;
        }

        @Override
        public GeoPoint bottomRight() {
            return null;
        }
    }

    public static class Meridian180DistanceBoundingCheck implements DistanceBoundingCheck {

        private final GeoPoint topLeft;
        private final GeoPoint bottomRight;

        public Meridian180DistanceBoundingCheck(GeoPoint topLeft, GeoPoint bottomRight) {
            this.topLeft = topLeft;
            this.bottomRight = bottomRight;
        }

        @Override
        public boolean isWithin(double targetLatitude, double targetLongitude) {
            return (targetLatitude >= bottomRight.lat() && targetLatitude <= topLeft.lat()) &&
                    (targetLongitude >= topLeft.lon() || targetLongitude <= bottomRight.lon());
        }

        @Override
        public GeoPoint topLeft() {
            return topLeft;
        }

        @Override
        public GeoPoint bottomRight() {
            return bottomRight;
        }
    }

    public static class SimpleDistanceBoundingCheck implements DistanceBoundingCheck {
        private final GeoPoint topLeft;
        private final GeoPoint bottomRight;

        public SimpleDistanceBoundingCheck(GeoPoint topLeft, GeoPoint bottomRight) {
            this.topLeft = topLeft;
            this.bottomRight = bottomRight;
        }

        @Override
        public boolean isWithin(double targetLatitude, double targetLongitude) {
            return (targetLatitude >= bottomRight.lat() && targetLatitude <= topLeft.lat()) &&
                    (targetLongitude >= topLeft.lon() && targetLongitude <= bottomRight.lon());
        }

        @Override
        public GeoPoint topLeft() {
            return topLeft;
        }

        @Override
        public GeoPoint bottomRight() {
            return bottomRight;
        }
    }

    public static class PlaneFixedSourceDistance implements FixedSourceDistance {

        private final double sourceLatitude;
        private final double sourceLongitude;
        private final double distancePerDegree;

        public PlaneFixedSourceDistance(double sourceLatitude, double sourceLongitude, DistanceUnit unit) {
            this.sourceLatitude = sourceLatitude;
            this.sourceLongitude = sourceLongitude;
            this.distancePerDegree = unit.getDistancePerDegree();
        }

        @Override
        public double calculate(double targetLatitude, double targetLongitude) {
            double px = targetLongitude - sourceLongitude;
            double py = targetLatitude - sourceLatitude;
            return Math.sqrt(px * px + py * py) * distancePerDegree;
        }
    }

    public static class FactorFixedSourceDistance implements FixedSourceDistance {

        private final double sourceLatitude;
        private final double sourceLongitude;
        private final double earthRadius;

        private final double a;
        private final double sinA;
        private final double cosA;

        public FactorFixedSourceDistance(double sourceLatitude, double sourceLongitude, DistanceUnit unit) {
            this.sourceLatitude = sourceLatitude;
            this.sourceLongitude = sourceLongitude;
            this.earthRadius = unit.getEarthRadius();
            this.a = Math.toRadians(90D - sourceLatitude);
            this.sinA = Math.sin(a);
            this.cosA = Math.cos(a);
        }

        @Override
        public double calculate(double targetLatitude, double targetLongitude) {
            double longitudeDifference = targetLongitude - sourceLongitude;
            double c = Math.toRadians(90D - targetLatitude);
            return (cosA * Math.cos(c)) + (sinA * Math.sin(c) * Math.cos(Math.toRadians(longitudeDifference)));
        }
    }


    public static class ArcFixedSourceDistance implements FixedSourceDistance {

        private final double sourceLatitude;
        private final double sourceLongitude;
        private final double earthRadius;

        private final double a;
        private final double sinA;
        private final double cosA;

        public ArcFixedSourceDistance(double sourceLatitude, double sourceLongitude, DistanceUnit unit) {
            this.sourceLatitude = sourceLatitude;
            this.sourceLongitude = sourceLongitude;
            this.earthRadius = unit.getEarthRadius();
            this.a = Math.toRadians(90D - sourceLatitude);
            this.sinA = Math.sin(a);
            this.cosA = Math.cos(a);
        }

        @Override
        public double calculate(double targetLatitude, double targetLongitude) {
            double longitudeDifference = targetLongitude - sourceLongitude;
            double c = Math.toRadians(90D - targetLatitude);
            double factor = (cosA * Math.cos(c)) + (sinA * Math.sin(c) * Math.cos(Math.toRadians(longitudeDifference)));

            if (factor < -1D) {
                return Math.PI * earthRadius;
            } else if (factor >= 1D) {
                return 0;
            } else {
                return Math.acos(factor) * earthRadius;
            }
        }
    }
}
