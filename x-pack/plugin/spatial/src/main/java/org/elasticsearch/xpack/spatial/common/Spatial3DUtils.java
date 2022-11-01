/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.common;

import org.apache.lucene.spatial3d.geom.GeoPoint;
import org.apache.lucene.spatial3d.geom.PlanetModel;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.h3.CellBoundary;
import org.elasticsearch.h3.LatLng;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.distance.GeodesicSphereDistCalc;
import org.locationtech.spatial4j.shape.impl.PointImpl;

/**
 * Utility methods for working with Lucenes spatial3d geometries, or using spatial3d algebra to work with ES geometries.
 */
public class Spatial3DUtils {
    private Spatial3DUtils() {}

    /**
     * Taking two points in mercator space, convert them to spatial3d coordinates and interpolate between them.
     * This will avoid mercator distortions in the interpolation, so the new points will line on the same great
     * circle as the original two points. The factor passed will decide where to place the point. Zero means at
     * the inside point. 1.0 means at the border point. Values greater than 1.0 will place it past the border.
     *
     * @param inside starting point
     * @param border ending point
     * @param factor factor along the inside->newly great circle to place the point
     * @return newly generated point along the same great circle
     */
    public static Point pointInterpolation(Point inside, Point border, double factor) {
        GeoPoint inside3d = new GeoPoint(PlanetModel.SPHERE, Math.toRadians(inside.getLat()), Math.toRadians(inside.getLon()));
        GeoPoint border3d = new GeoPoint(PlanetModel.SPHERE, Math.toRadians(border.getLat()), Math.toRadians(border.getLon()));
        GeoPoint point = pointInterpolation(inside3d, border3d, factor);
        return new Point(Math.toDegrees(point.getLongitude()), Math.toDegrees(point.getLatitude()));
    }

    /**
     * Taking two points in mercator space, convert them to spatial3d coordinates and interpolate between them.
     * This will avoid mercator distortions in the interpolation, so the new points will line on the same great
     * circle as the original two points. The factor passed will decide where to place the point. Zero means at
     * the inside point. 1.0 means at the border point. Values greater than 1.0 will place it past the border.
     *
     * @param inside3d starting point
     * @param border3d ending point
     * @param factor factor along the inside->newly great circle to place the point
     * @return newly generated point along the same great circle
     */
    public static GeoPoint pointInterpolation(GeoPoint inside3d, GeoPoint border3d, double factor) {
        double dX = border3d.x - inside3d.x;
        double dY = border3d.y - inside3d.y;
        double dZ = border3d.z - inside3d.z;
        double newX = inside3d.x + dX * factor;
        double newY = inside3d.y + dY * factor;
        double newZ = inside3d.z + dZ * factor;
        return new GeoPoint(newX, newY, newZ);
    }

    /**
     * Calculate the distance over the surface of the earth
     */
    public static double distance(Point from, Point to) {
        PointImpl a = new PointImpl(from.getX(), from.getY(), SpatialContext.GEO);
        PointImpl b = new PointImpl(to.getX(), to.getY(), SpatialContext.GEO);
        return new GeodesicSphereDistCalc.Haversine().distance(a, b);
    }

    /**
     * Given an H3 cell boundary, calculate the centroid
     */
    public static Point calculateCentroid(CellBoundary boundary) {
        GeoPoint centroid3D = calculateCentroid3d(boundary);
        return new Point(Math.toDegrees(centroid3D.getLongitude()), Math.toDegrees(centroid3D.getLatitude()));
    }

    /**
     * Given an H3 cell boundary, calculate the centroid
     */
    public static GeoPoint calculateCentroid3d(CellBoundary boundary) {
        double centroidX = 0;
        double centroidY = 0;
        double centroidZ = 0;
        for (int i = 0; i < boundary.numPoints(); i++) {
            LatLng vertexLatLng = boundary.getLatLon(i);
            GeoPoint vertex = new GeoPoint(PlanetModel.SPHERE, vertexLatLng.getLatRad(), vertexLatLng.getLonRad());
            centroidX += vertex.x;
            centroidY += vertex.y;
            centroidZ += vertex.z;
        }
        centroidX /= boundary.numPoints();
        centroidY /= boundary.numPoints();
        centroidZ /= boundary.numPoints();
        return new GeoPoint(centroidX, centroidY, centroidZ);
    }

    /**
     * Given a collection of GeoPoint, calculate the centroid
     */
    public static GeoPoint calculateCentroid(GeoPoint[] boundary) {
        double centroidX = 0;
        double centroidY = 0;
        double centroidZ = 0;
        for (GeoPoint vertex : boundary) {
            centroidX += vertex.x;
            centroidY += vertex.y;
            centroidZ += vertex.z;
        }
        centroidX /= boundary.length;
        centroidY /= boundary.length;
        centroidZ /= boundary.length;
        return new GeoPoint(centroidX, centroidY, centroidZ);
    }
}
