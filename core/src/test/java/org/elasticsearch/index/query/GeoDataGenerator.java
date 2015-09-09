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

package org.elasticsearch.index.query;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.test.ESTestCase;

/** Helper class for generating geo related data. */
public class GeoDataGenerator {

    /** Generate a valid random geo point. */
    public static GeoPoint randomGeoPoint() {
        double lat = ESTestCase.randomDoubleBetween(GeoUtils.MIN_LAT, GeoUtils.MAX_LAT, true);
        double lon = ESTestCase.randomDoubleBetween(GeoUtils.MIN_LON, GeoUtils.MAX_LON, true);
        return new GeoPoint(lat, lon);
    }

    /** Generate a valid random bounding box. */
    public static BoundingBox randomBoundingBox() {
        double bottom = ESTestCase.randomDoubleBetween(GeoUtils.MIN_LAT, GeoUtils.MAX_LAT, true);
        double top = ESTestCase.randomDoubleBetween(bottom, GeoUtils.MAX_LAT, true);
        double left = ESTestCase.randomDoubleBetween(GeoUtils.MIN_LON, GeoUtils.MAX_LON, true);
        double right = ESTestCase.randomDoubleBetween(left, GeoUtils.MAX_LON, true);

        if (ESTestCase.rarely()) {
            // Rarely check everything works as expected also when hitting the lat/lon boundaries
            if (ESTestCase.randomBoolean()) {
                top = GeoUtils.MAX_LAT;
            }
            if (ESTestCase.randomBoolean()) {
                right = GeoUtils.MAX_LON;
            }
        }
        return new BoundingBox(top, left, bottom, right);
    }

    /** Generate a double that lies outside the given interval, includes NaN and pos/neg infinity. */
    public static double invalidRandomDouble(double validStart, double validEnd) {
        Double[] values = new Double[6];
        values[0] = Double.NaN;
        values[1] = Double.POSITIVE_INFINITY;
        values[2] = Double.NEGATIVE_INFINITY;
        values[3] = ESTestCase.randomDoubleBetween(-Double.MAX_VALUE, validStart, true);
        values[4] = ESTestCase.randomDoubleBetween(validEnd, Double.MAX_VALUE, false);
        values[5] = Double.MAX_VALUE;
        return ESTestCase.randomFrom(values);
    }

    public static class BoundingBox {
        public GeoPoint topLeft;
        public GeoPoint bottomRight;

        public BoundingBox(double top, double left, double bottom, double right) {
            this.topLeft = new GeoPoint(top, left);
            this.bottomRight = new GeoPoint(bottom, right);
        }
        public double top() {
            return topLeft.getLat();
        }

        public double left() {
            return topLeft.getLon();
        }

        public double bottom() {
            return bottomRight.getLat();
        }

        public double right() {
            return bottomRight.getLon();
        }
    }
}
