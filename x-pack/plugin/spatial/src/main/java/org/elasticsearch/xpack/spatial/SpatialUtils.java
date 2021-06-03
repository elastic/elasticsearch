/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial;

import org.apache.lucene.util.SloppyMath;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.geometry.Circle;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.Polygon;

/**
 * Utility class for storing different helpful re-usable spatial functions
 */
public class SpatialUtils {

    private SpatialUtils() {}

    /**
     * Makes an n-gon, centered at the provided circle's center, and each vertex approximately
     * {@link Circle#getRadiusMeters()} away from the center.
     *
     * It throws an IllegalArgumentException if the circle contains a pole.
     *
     * This does not split the polygon across the date-line. Relies on {@link GeoShapeIndexer} to
     * split prepare polygon for indexing.
     *
     * Adapted from from org.apache.lucene.geo.GeoTestUtil
     * */
    public static Polygon createRegularGeoShapePolygon(Circle circle, int gons) {
        if (SloppyMath.haversinMeters(circle.getLat(), circle.getLon(), 90, 0) < circle.getRadiusMeters()) {
            throw new IllegalArgumentException("circle [" + circle.toString() + "] contains the north pole. " +
                "It cannot be translated to a polygon");
        }
        if (SloppyMath.haversinMeters(circle.getLat(), circle.getLon(), -90, 0) < circle.getRadiusMeters()) {
            throw new IllegalArgumentException("circle [" + circle.toString() + "] contains the south pole. " +
                "It cannot be translated to a polygon");
        }
        double[][] result = new double[2][];
        result[0] = new double[gons+1];
        result[1] = new double[gons+1];
        for(int i=0; i<gons; i++) {
            // make sure we do not start at angle 0 or we have issues at the poles
            double angle = i * (360.0 / gons);
            double x = Math.cos(SloppyMath.toRadians(angle));
            double y = Math.sin(SloppyMath.toRadians(angle));
            double factor = 2.0;
            double step = 1.0;
            int last = 0;

            // Iterate out along one spoke until we hone in on the point that's nearly exactly radiusMeters from the center:
            while (true) {
                double lat = circle.getLat() + y * factor;
                double lon = circle.getLon() + x * factor;
                double distanceMeters = SloppyMath.haversinMeters(circle.getLat(), circle.getLon(), lat, lon);

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
        result[0] = new double[gons+1];
        result[1] = new double[gons+1];
        for(int i=0; i<gons; i++) {
            double angle = i * (360.0 / gons);
            double x = circle.getRadiusMeters() * Math.cos(SloppyMath.toRadians(angle));
            double y = circle.getRadiusMeters() * Math.sin(SloppyMath.toRadians(angle));

            result[0][i] = x + circle.getX();
            result[1][i] = y + circle.getY();
        }
        // close poly
        result[0][gons] = result[0][0];
        result[1][gons] = result[1][0];
        return new Polygon(new LinearRing(result[0], result[1]));
    }
}
