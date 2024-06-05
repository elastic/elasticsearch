/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.geometry.utils;

import org.elasticsearch.geometry.Circle;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.Polygon;

/**
 * Utility class for storing different helpful re-usable spatial functions
 */
public class CircleUtils {

    static final int CIRCLE_TO_POLYGON_MINIMUM_NUMBER_OF_SIDES = 4;
    static final int CIRCLE_TO_POLYGON_MAXIMUM_NUMBER_OF_SIDES = 1000;

    private CircleUtils() {}

    /**
     * Makes an n-gon, centered at the provided circle's center, and each vertex approximately
     * {@link Circle#getRadiusMeters()} away from the center.
     *
     * It throws an IllegalArgumentException if the circle contains a pole.
     *
     * This does not split the polygon across the date-line. Relies on org.elasticsearch.index.mapper.GeoShapeIndexer to
     * split prepare polygon for indexing.
     *
     * Adapted from from org.apache.lucene.tests.geo.GeoTestUtil
     * */
    public static Polygon createRegularGeoShapePolygon(Circle circle, int gons) {
        if (slowHaversin(circle.getLat(), circle.getLon(), 90, 0) < circle.getRadiusMeters()) {
            throw new IllegalArgumentException(
                "circle [" + circle.toString() + "] contains the north pole. " + "It cannot be translated to a polygon"
            );
        }
        if (slowHaversin(circle.getLat(), circle.getLon(), -90, 0) < circle.getRadiusMeters()) {
            throw new IllegalArgumentException(
                "circle [" + circle.toString() + "] contains the south pole. " + "It cannot be translated to a polygon"
            );
        }
        double[][] result = new double[2][];
        result[0] = new double[gons + 1];
        result[1] = new double[gons + 1];
        for (int i = 0; i < gons; i++) {
            // make sure we do not start at angle 0 or we have issues at the poles
            double angle = i * (360.0 / gons);
            double x = Math.cos(Math.toRadians(angle));
            double y = Math.sin(Math.toRadians(angle));
            double factor = 2.0;
            double step = 1.0;
            int last = 0;

            // Iterate out along one spoke until we hone in on the point that's nearly exactly radiusMeters from the center:
            while (true) {
                double lat = circle.getLat() + y * factor;
                double lon = circle.getLon() + x * factor;
                double distanceMeters = slowHaversin(circle.getLat(), circle.getLon(), lat, lon);

                if (Math.abs(distanceMeters - circle.getRadiusMeters()) < 0.1) {
                    // Within 10 cm: close enough!
                    // lon/lat are left de-normalized so that indexing can properly detect dateline crossing.
                    result[0][i] = lon;
                    result[1][i] = lat;
                    break;
                }

                if (distanceMeters > circle.getRadiusMeters()) {
                    // too big
                    factor -= step;
                    if (last == 1) {
                        step /= 2.0;
                    }
                    last = -1;
                } else if (distanceMeters < circle.getRadiusMeters()) {
                    // too small
                    factor += step;
                    if (last == -1) {
                        step /= 2.0;
                    }
                    last = 1;
                }
            }
        }

        // close poly
        result[0][gons] = result[0][0];
        result[1][gons] = result[1][0];
        return new Polygon(new LinearRing(result[0], result[1]));
    }

    /**
     * Makes an n-gon, centered at the provided circle's center. This assumes
     * distance measured in cartesian geometry.
     **/
    public static Polygon createRegularShapePolygon(Circle circle, int gons) {
        double[][] result = new double[2][];
        result[0] = new double[gons + 1];
        result[1] = new double[gons + 1];
        for (int i = 0; i < gons; i++) {
            double angle = i * (360.0 / gons);
            double x = circle.getRadiusMeters() * Math.cos(Math.toRadians(angle));
            double y = circle.getRadiusMeters() * Math.sin(Math.toRadians(angle));

            result[0][i] = x + circle.getX();
            result[1][i] = y + circle.getY();
        }
        // close poly
        result[0][gons] = result[0][0];
        result[1][gons] = result[1][0];
        return new Polygon(new LinearRing(result[0], result[1]));
    }

    public static int circleToPolygonNumSides(double radiusMeters, double errorDistance) {
        int val = (int) Math.ceil(2 * Math.PI / Math.acos(1 - errorDistance / radiusMeters));
        return Math.min(CIRCLE_TO_POLYGON_MAXIMUM_NUMBER_OF_SIDES, Math.max(CIRCLE_TO_POLYGON_MINIMUM_NUMBER_OF_SIDES, val));
    }

    // simple incorporation of the wikipedia formula
    // Improved method implementation for better performance can be found in `SloppyMath.haversinMeters` which
    // is included in `org.apache.lucene:lucene-core`.
    // Do not use this method for performance critical cases
    private static double slowHaversin(double lat1, double lon1, double lat2, double lon2) {
        double h1 = (1 - StrictMath.cos(StrictMath.toRadians(lat2) - StrictMath.toRadians(lat1))) / 2;
        double h2 = (1 - StrictMath.cos(StrictMath.toRadians(lon2) - StrictMath.toRadians(lon1))) / 2;
        double h = h1 + StrictMath.cos(StrictMath.toRadians(lat1)) * StrictMath.cos(StrictMath.toRadians(lat2)) * h2;
        return 2 * 6371008.7714 * StrictMath.asin(Math.min(1, Math.sqrt(h)));
    }
}
