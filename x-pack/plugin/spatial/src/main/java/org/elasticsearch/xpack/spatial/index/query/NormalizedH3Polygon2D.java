/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.query;

import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.index.PointValues;
import org.elasticsearch.h3.CellBoundary;
import org.elasticsearch.h3.LatLng;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;

import java.util.ArrayList;

import static org.elasticsearch.core.Strings.format;

/**
 * This Component2D implementation represents a single simple polygon of an H3 cell that crosses the DateLine.
 * The Polygon will be split into two polygons, one on each side of the DateLine.
 */
public class NormalizedH3Polygon2D implements Component2D {
    public static final int NORTH = 1;
    public static final int SOUTH = -1;
    public static final int NEITHER = 0;
    private Component2D component;
    private double offset;
    private final double latitudeThreshold;
    private final int pole;
    private final Listener listener;  // option callback for capturing actual polygon

    /**
     * Construct using CCW ordered coordinates, with all longitude values within -180 to 180.
     */
    NormalizedH3Polygon2D(double[] lats, double[] lons, int pole) {
        this(lats, lons, pole, GeoTileUtils.LATITUDE_MASK, null);
    }

    NormalizedH3Polygon2D(double[] lats, double[] lons, int pole, double latitudeThreshold, Listener listener) {
        this.latitudeThreshold = latitudeThreshold;
        this.listener = listener;
        this.pole = pole;
        validate(lats, lons);
        initialize(lats, lons);
    }

    /**
     * Construct using an H3 cell identifier
     */
    static NormalizedH3Polygon2D fromH3Boundary(CellBoundary boundary, int pole, double latitudeThreshold, Listener listener) {
        double[] lats = new double[boundary.numPoints() + 1];
        double[] lons = new double[boundary.numPoints() + 1];
        for (int i = 0; i < boundary.numPoints(); i++) {
            LatLng vertex = boundary.getLatLon(i);
            lats[i] = vertex.getLatDeg();
            lons[i] = vertex.getLonDeg();
        }
        lats[lats.length - 1] = lats[0];
        lons[lons.length - 1] = lons[0];
        return new NormalizedH3Polygon2D(lats, lons, pole, latitudeThreshold, listener);
    }

    private static void validate(double[] lats, double[] lons) {
        if (lats.length != lons.length) {
            throw new IllegalArgumentException("Latitudes and longitudes must have same array length");
        }
        if (lons.length < 4) {
            throw new IllegalArgumentException("Expect at least four points in polygon coordinate array");
        }
        if (lats[0] != lats[lats.length - 1] || lons[0] != lons[lons.length - 1]) {
            throw new IllegalArgumentException(
                format(
                    "First and last polygon coordinates must be the same equal: %s != %s",
                    pointAsString(lats, lons, 0),
                    pointAsString(lats, lons, lons.length - 1)
                )
            );
        }
        for (double lat : lats) {
            if (lat < -90 || lat > 90) {
                throw new IllegalArgumentException("Illegal latitude: " + lat);
            }
        }
        for (double lon : lons) {
            if (lon < -180 || lon > 180) {
                throw new IllegalArgumentException("Illegal longitude: " + lon);
            }
        }
    }

    private static double offsetFromDateline(double[] lons) {
        // Look for dateline crossing
        double min = Float.MAX_VALUE;
        double max = -Float.MAX_VALUE;
        for (double lon : lons) {
            min = Math.min(min, lon);
            max = Math.max(max, lon);
        }
        if (max - min > 180) {
            // Candidate for crossing the dateline, determine optimal offset
            return normalizedAverage(min, max);
        } else {
            return 0;
        }
    }

    private static double normalizedAverage(double a, double b) {
        while (a < 0) {
            a += 360;
        }
        while (b < 0) {
            b += 360;
        }
        return n(-(a + b) / 2);
    }

    private static double offsetFromPolarTruncation(double[] lats, double[] lons, int pole, double latitudeThreshold) {
        // When we have a polar cell that has already been truncated, then find the two truncated points
        // and make sure they are offset so that the entire cell is within the -180:180 range.
        int startOfTruncation = -1;
        int endOfTruncation = -1;
        latitudeThreshold = latitudeThreshold * pole;
        for (int i = 0; i < lons.length; i++) {
            if (lats[i] == latitudeThreshold) {
                if (startOfTruncation < 0) {
                    startOfTruncation = i;
                } else {
                    endOfTruncation = i;
                    break;
                }
            }
        }
        if (startOfTruncation < 0 || endOfTruncation < 0) {
            throw new IllegalStateException("Failed to find truncation of polar cell - it is really polar?");
        }
        if (startOfTruncation == 0 && endOfTruncation != 1) {
            // We looped, so swap coordinates
            int newStart = endOfTruncation;
            endOfTruncation = startOfTruncation;
            startOfTruncation = newStart;
        }
        double startLon = lons[startOfTruncation];
        double endLon = lons[endOfTruncation];
        if (pole == NORTH) {
            while (startLon < endLon) {
                startLon += 360;
            }
        } else {
            while (startLon > endLon) {
                endLon += 360;
            }
        }
        return n(-(startLon + endLon) / 2);
    }

    public int pole() {
        return pole;
    }

    private static String pointAsString(double[] lats, double[] lons, int index) {
        return "(" + lons[index] + "," + lats[index] + ")";
    }

    private void initialize(double[] lats, double[] lons) {
        ArrayList<Integer> edges = new ArrayList<>();
        int crosses = 0;
        for (int i = 0; i < lons.length - 1; i++) {
            if (illegalLatitude(lats[i])) {
                if (illegalLatitude(lats[i + 1]) == false) {
                    // We are entering legal space
                    crosses++;
                    edges.add(i);
                }
            } else {
                // Current point is legal, add it to the list
                edges.add(i);
            }
        }
        if (edges.size() == 0) {
            throw new IllegalArgumentException(
                "No legal edges detected, polygon is likely entirely north of 85 degrees or south of -85 degrees"
            );
        }
        component = buildComponent(edges, lats, lons, crosses);
    }

    private boolean illegalLatitude(double latitude) {
        return latitude > latitudeThreshold || latitude < -latitudeThreshold;
    }

    private Component2D buildComponent(ArrayList<Integer> edges, double[] externalLats, double[] externalLons, int extra) {
        double[] internalLats = new double[edges.size() + extra + 1];
        double[] internalLons = new double[edges.size() + extra + 1];
        int index = 0;
        for (int edge : edges) {
            internalLats[index] = externalLats[edge];
            internalLons[index] = externalLons[edge];
            if (illegalLatitude(internalLats[index])) {
                // Next point is on correct side, so replace this point with an interpolated one
                latitudeInterpolation(internalLons, internalLats, index, edge, edge + 1, externalLats, externalLons);
            }
            if (illegalLatitude(externalLats[edge + 1])) {
                // Next point is illegal, so add an extra interpolated point
                index++;
                latitudeInterpolation(internalLons, internalLats, index, edge, edge + 1, externalLats, externalLons);
            }
            index++;
        }
        internalLats[internalLats.length - 1] = internalLats[0];
        internalLons[internalLons.length - 1] = internalLons[0];
        if (pole == NEITHER) {
            offset = offsetFromDateline(internalLons);
        } else {
            offset = offsetFromPolarTruncation(internalLats, internalLons, pole, latitudeThreshold);
        }
        for (int i = 0; i < internalLons.length; i++) {
            internalLons[i] = o(internalLons[i]);
        }
        if (listener != null) {
            listener.polygon(internalLats, internalLons, offset);
        }
        return LatLonGeometry.create(new Polygon(internalLats, internalLons));
    }

    public void latitudeInterpolation(
        double[] internalLons,
        double[] internalLats,
        int i,
        int edgeA,
        int edgeB,
        double[] externalLats,
        double[] externalLons
    ) {
        boolean north = externalLats[edgeA] > 0;
        double threshold = north ? latitudeThreshold : -latitudeThreshold;
        double width = n(externalLons[edgeB] - externalLons[edgeA]);
        double height = (externalLats[edgeB] - externalLats[edgeA]);
        double factor = (threshold - externalLats[edgeA]) / height;
        internalLats[i] = threshold;
        internalLons[i] = n(externalLons[edgeA] + factor * width);
    }

    @Override
    public double getMinX() {
        return r(component.getMinX());
    }

    @Override
    public double getMaxX() {
        return r(component.getMaxX());
    }

    @Override
    public double getMinY() {
        return component.getMinY();
    }

    @Override
    public double getMaxY() {
        return component.getMaxY();
    }

    @Override
    public boolean contains(double x, double y) {
        return component.contains(o(x), y);
    }

    public boolean any(PointValues.Relation expected, PointValues.Relation... others) {
        for (PointValues.Relation other : others) {
            if (other == expected) {
                return true;
            }
        }
        return false;
    }

    public boolean all(PointValues.Relation expected, PointValues.Relation... others) {
        for (PointValues.Relation other : others) {
            if (other != expected) {
                return false;
            }
        }
        return true;
    }

    @Override
    public PointValues.Relation relate(double minX, double maxX, double minY, double maxY) {
        minX = o(minX);
        maxX = o(maxX);
        if (minX > maxX) {
            PointValues.Relation right = component.relate(minX, 180, minY, maxY);
            PointValues.Relation left = component.relate(-180, maxX, minY, maxY);
            if (all(PointValues.Relation.CELL_INSIDE_QUERY, left, right)) {
                return PointValues.Relation.CELL_INSIDE_QUERY;
            }
            if (all(PointValues.Relation.CELL_OUTSIDE_QUERY, left, right)) {
                return PointValues.Relation.CELL_OUTSIDE_QUERY;
            }
            return PointValues.Relation.CELL_CROSSES_QUERY;
        } else {
            return component.relate(minX, maxX, minY, maxY);
        }
    }

    @Override
    public boolean intersectsLine(double minX, double maxX, double minY, double maxY, double aX, double aY, double bX, double bY) {
        return component.intersectsLine(o(minX), o(maxX), minY, maxY, o(aX), aY, o(bX), bY);
    }

    @Override
    public boolean intersectsLine(double aX, double aY, double bX, double bY) {
        return component.intersectsLine(o(aX), aY, o(bX), bY);
    }

    @Override
    public boolean intersectsTriangle(
        double minX,
        double maxX,
        double minY,
        double maxY,
        double aX,
        double aY,
        double bX,
        double bY,
        double cX,
        double cY
    ) {
        return component.intersectsTriangle(o(minX), o(maxX), minY, maxY, o(aX), aY, o(bX), bY, o(cX), cY);
    }

    @Override
    public boolean intersectsTriangle(double aX, double aY, double bX, double bY, double cX, double cY) {
        return component.intersectsTriangle(o(aX), aY, o(bX), bY, o(cX), cY);
    }

    @Override
    public boolean containsLine(double minX, double maxX, double minY, double maxY, double aX, double aY, double bX, double bY) {
        return component.containsLine(o(minX), o(maxX), minY, maxY, o(aX), aY, o(bX), bY);
    }

    @Override
    public boolean containsLine(double aX, double aY, double bX, double bY) {
        return component.containsLine(o(aX), aY, o(bX), bY);
    }

    @Override
    public boolean containsTriangle(
        double minX,
        double maxX,
        double minY,
        double maxY,
        double aX,
        double aY,
        double bX,
        double bY,
        double cX,
        double cY
    ) {
        return component.containsTriangle(o(minX), o(maxX), minY, maxY, o(aX), aY, o(bX), bY, o(cX), cY);
    }

    @Override
    public boolean containsTriangle(double aX, double aY, double bX, double bY, double cX, double cY) {
        return component.containsTriangle(o(aX), aY, o(bX), bY, o(cX), cY);
    }

    @Override
    public WithinRelation withinPoint(double x, double y) {
        return component.withinPoint(o(x), y);
    }

    @Override
    public WithinRelation withinLine(
        double minX,
        double maxX,
        double minY,
        double maxY,
        double aX,
        double aY,
        boolean ab,
        double bX,
        double bY
    ) {
        return component.withinLine(o(minX), o(maxX), minY, maxY, o(aX), aY, ab, o(bX), bY);
    }

    @Override
    public WithinRelation withinLine(double aX, double aY, boolean ab, double bX, double bY) {
        return component.withinLine(o(aX), aY, ab, o(bX), bY);
    }

    @Override
    public WithinRelation withinTriangle(
        double minX,
        double maxX,
        double minY,
        double maxY,
        double aX,
        double aY,
        boolean ab,
        double bX,
        double bY,
        boolean bc,
        double cX,
        double cY,
        boolean ca
    ) {
        return component.withinTriangle(o(minX), o(maxX), minY, maxY, o(aX), aY, ab, o(bX), bY, bc, o(cX), cY, ca);
    }

    @Override
    public WithinRelation withinTriangle(
        double aX,
        double aY,
        boolean ab,
        double bX,
        double bY,
        boolean bc,
        double cX,
        double cY,
        boolean ca
    ) {
        return component.withinTriangle(o(aX), aY, ab, o(bX), bY, bc, o(cX), cY, ca);
    }

    /**
     * Add the offset to the longitude to move it into the internal coordinate range that does not cross the dateline
     */
    private double o(double x) {
        return n(x + offset);
    }

    /**
     * Remove the offset from the internal longitude to move it into the external coordinate range
     */
    private double r(double x) {
        return n(x - offset);
    }

    /**
     * Normalize longitude to be within -180 to 180. This is useful when performing arithmetic on longitudes.
     */
    static double n(double x) {
        while (x <= -180) {
            x += 360;
        }
        while (x > 180) {
            x -= 360;
        }
        return x;
    }

    /**
     * This interface allows calling code to receive feedback on the inner workings.
     * It is particularly useful for testing without having to make parts of the class public.
     */
    interface Listener {
        /**
         * Information on the internal polygon coordinates.
         * Note that these will have been adjusted with the longitude shifted by the offset to ensure that they do not cross the dateline.
         * To get the original external polygon coordinates, subtract the offset from all longitude values.
         */
        void polygon(double[] lats, double[] lons, double offset);
    }
}
