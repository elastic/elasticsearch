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

import org.apache.lucene.util.Bits;
import org.apache.lucene.util.SloppyMath;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.GeoPointValues;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.fielddata.SortingNumericDoubleValues;

import java.util.Locale;

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
     * Calculates distance as points on a globe.
     */
    ARC() {
        @Override
        public double calculate(double sourceLatitude, double sourceLongitude, double targetLatitude, double targetLongitude, DistanceUnit unit) {
            double x1 = sourceLatitude * Math.PI / 180D;
            double x2 = targetLatitude * Math.PI / 180D;
            double h1 = 1D - Math.cos(x1 - x2);
            double h2 = 1D - Math.cos((sourceLongitude - targetLongitude) * Math.PI / 180D);
            double h = (h1 + Math.cos(x1) * Math.cos(x2) * h2) / 2;
            double averageLatitude = (x1 + x2) / 2;
            double diameter = GeoUtils.earthDiameter(averageLatitude);
            return unit.fromMeters(diameter * Math.asin(Math.min(1, Math.sqrt(h))));
        }

        @Override
        public double normalize(double distance, DistanceUnit unit) {
            return distance;
        }

        @Override
        public FixedSourceDistance fixedSourceDistance(double sourceLatitude, double sourceLongitude, DistanceUnit unit) {
            return new ArcFixedSourceDistance(sourceLatitude, sourceLongitude, unit);
        }
    },
    /**
     * Calculates distance as points on a globe in a sloppy way. Close to the pole areas the accuracy
     * of this function decreases.
     */
    SLOPPY_ARC() {

        @Override
        public double normalize(double distance, DistanceUnit unit) {
            return distance;
        }

        @Override
        public double calculate(double sourceLatitude, double sourceLongitude, double targetLatitude, double targetLongitude, DistanceUnit unit) {
            return unit.fromMeters(SloppyMath.haversin(sourceLatitude, sourceLongitude, targetLatitude, targetLongitude) * 1000.0);
        }

        @Override
        public FixedSourceDistance fixedSourceDistance(double sourceLatitude, double sourceLongitude, DistanceUnit unit) {
            return new SloppyArcFixedSourceDistance(sourceLatitude, sourceLongitude, unit);
        }
    };

    /**
     * Default {@link GeoDistance} function. This method should be used, If no specific function has been selected.
     * This is an alias for <code>SLOPPY_ARC</code>
     */
    public static final GeoDistance DEFAULT = SLOPPY_ARC; 
    
    public abstract double normalize(double distance, DistanceUnit unit);

    public abstract double calculate(double sourceLatitude, double sourceLongitude, double targetLatitude, double targetLongitude, DistanceUnit unit);

    public abstract FixedSourceDistance fixedSourceDistance(double sourceLatitude, double sourceLongitude, DistanceUnit unit);

    private static final double MIN_LAT = Math.toRadians(-90d);  // -PI/2
    private static final double MAX_LAT = Math.toRadians(90d);   //  PI/2
    private static final double MIN_LON = Math.toRadians(-180d); // -PI
    private static final double MAX_LON = Math.toRadians(180d);  //  PI

    public static DistanceBoundingCheck distanceBoundingCheck(double sourceLatitude, double sourceLongitude, double distance, DistanceUnit unit) {
        // angular distance in radians on a great circle
        // assume worst-case: use the minor axis
        double radDist = unit.toMeters(distance) / GeoUtils.EARTH_SEMI_MINOR_AXIS;

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

    /**
     * Get a {@link GeoDistance} according to a given name. Valid values are
     * 
     * <ul>
     *     <li><b>plane</b> for <code>GeoDistance.PLANE</code></li>
     *     <li><b>sloppy_arc</b> for <code>GeoDistance.SLOPPY_ARC</code></li>
     *     <li><b>factor</b> for <code>GeoDistance.FACTOR</code></li>
     *     <li><b>arc</b> for <code>GeoDistance.ARC</code></li>
     * </ul>
     * 
     * @param name name of the {@link GeoDistance}
     * @return a {@link GeoDistance}
     */
    public static GeoDistance fromString(String name) {
        name = name.toLowerCase(Locale.ROOT);
        if ("plane".equals(name)) {
            return PLANE;
        } else if ("arc".equals(name)) {
            return ARC;
        } else if ("sloppy_arc".equals(name)) {
            return SLOPPY_ARC;
        } else if ("factor".equals(name)) {
            return FACTOR;
        }
        throw new IllegalArgumentException("No geo distance for [" + name + "]");
    }

    public static interface FixedSourceDistance {

        double calculate(double targetLatitude, double targetLongitude);
    }

    public static interface DistanceBoundingCheck {

        boolean isWithin(double targetLatitude, double targetLongitude);

        GeoPoint topLeft();

        GeoPoint bottomRight();
    }

    public static final AlwaysDistanceBoundingCheck ALWAYS_INSTANCE = new AlwaysDistanceBoundingCheck();

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

        private final double sourceLongitude;

        private final double a;
        private final double sinA;
        private final double cosA;

        public FactorFixedSourceDistance(double sourceLatitude, double sourceLongitude, DistanceUnit unit) {
            this.sourceLongitude = sourceLongitude;
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

    /**
     * Basic implementation of {@link FixedSourceDistance}. This class keeps the basic parameters for a distance
     * functions based on a fixed source. Namely latitude, longitude and unit. 
     */
    public static abstract class FixedSourceDistanceBase implements FixedSourceDistance {
        protected final double sourceLatitude;
        protected final double sourceLongitude;
        protected final DistanceUnit unit;

        public FixedSourceDistanceBase(double sourceLatitude, double sourceLongitude, DistanceUnit unit) {
            this.sourceLatitude = sourceLatitude;
            this.sourceLongitude = sourceLongitude;
            this.unit = unit;
        }
    }
    
    public static class ArcFixedSourceDistance extends FixedSourceDistanceBase {

        public ArcFixedSourceDistance(double sourceLatitude, double sourceLongitude, DistanceUnit unit) {
            super(sourceLatitude, sourceLongitude, unit);
        }

        @Override
        public double calculate(double targetLatitude, double targetLongitude) {
            return ARC.calculate(sourceLatitude, sourceLongitude, targetLatitude, targetLongitude, unit);
        }

    }

    public static class SloppyArcFixedSourceDistance extends FixedSourceDistanceBase {

        public SloppyArcFixedSourceDistance(double sourceLatitude, double sourceLongitude, DistanceUnit unit) {
            super(sourceLatitude, sourceLongitude, unit);
        }

        @Override
        public double calculate(double targetLatitude, double targetLongitude) {
            return SLOPPY_ARC.calculate(sourceLatitude, sourceLongitude, targetLatitude, targetLongitude, unit);
        }
    }


    /**
     * Return a {@link SortedNumericDoubleValues} instance that returns the distances to a list of geo-points for each document.
     */
    public static SortedNumericDoubleValues distanceValues(final MultiGeoPointValues geoPointValues, final FixedSourceDistance... distances) {
        final GeoPointValues singleValues = FieldData.unwrapSingleton(geoPointValues);
        if (singleValues != null && distances.length == 1) {
            final Bits docsWithField = FieldData.unwrapSingletonBits(geoPointValues);
            return FieldData.singleton(new NumericDoubleValues() {

                @Override
                public double get(int docID) {
                    if (docsWithField != null && !docsWithField.get(docID)) {
                        return 0d;
                    }
                    final GeoPoint point = singleValues.get(docID);
                    return distances[0].calculate(point.lat(), point.lon());
                }

            }, docsWithField);
        } else {
            return new SortingNumericDoubleValues() {

                @Override
                public void setDocument(int doc) {
                    geoPointValues.setDocument(doc);
                    resize(geoPointValues.count() * distances.length);
                    int valueCounter = 0;
                    for (FixedSourceDistance distance : distances) {
                        for (int i = 0; i < geoPointValues.count(); ++i) {
                            final GeoPoint point = geoPointValues.valueAt(i);
                            values[valueCounter] = distance.calculate(point.lat(), point.lon());
                            valueCounter++;
                        }
                    }
                    sort();
                }
            };
        }
    }
}
