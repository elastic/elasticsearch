/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.geom;

import org.apache.lucene.spatial3d.geom.Bounds;
import org.apache.lucene.spatial3d.geom.DistanceStyle;
import org.apache.lucene.spatial3d.geom.GeoAreaShape;
import org.apache.lucene.spatial3d.geom.GeoPoint;
import org.apache.lucene.spatial3d.geom.GeoShape;
import org.apache.lucene.spatial3d.geom.Membership;
import org.apache.lucene.spatial3d.geom.Plane;
import org.apache.lucene.spatial3d.geom.PlanetModel;
import org.apache.lucene.spatial3d.geom.SerializableObject;
import org.apache.lucene.spatial3d.geom.SidedPlane;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

/**
 * Fast implementation of a simple convex polygon, for example an S2 cell, or an H3 cell. There are no checks validating
 * that points are convex therefore users must provide points in CCW or the logic will fail. The last point should not
 * be a copy of the first point.
 *
 * The existing Lucene class GeoS2Shape could extend this class, and in fact this class was created as a simple generalization
 * of the GeoS2Shape class.
 *
 * TODO: Once Lucene makes GeoBaseAreaShape and/or GeoBasePolygon public, we can stop extending LuceneGeoBaseAreaShape and delete that
 * and instead extend GeoBasePolygon from Lucene directly.
 *
 * TODO: Consider moving this class and its tests into Lucene spatial3d library itself.
 */
public class GeoRegularConvexPolygon extends LuceneGeoBaseAreaShape {

    protected final GeoPoint[] points;
    protected final SidedPlane[] planes;
    protected final GeoPoint[][] notablePlanePoints;
    protected final GeoPoint[] edgePoints;

    /**
     * It builds from N>=3 points given in CCW. It must be convex or logic will fail.
     * The last point should not be a copy of the first point.
     *
     * @param planetModel is the planet model.
     * @param points an array of at least three points in CCW orientation.
     */
    GeoRegularConvexPolygon(final PlanetModel planetModel, GeoPoint... points) {
        super(planetModel);
        assert points.length >= 3;
        this.points = points;

        // Now build the N planes
        this.planes = new SidedPlane[points.length];
        this.notablePlanePoints = new GeoPoint[points.length][];
        for (int i = 0; i < points.length; i++) {
            int prev = (i < 1) ? points.length - 1 : i - 1;
            int next = (i == points.length - 1) ? 0 : i + 1;
            this.planes[i] = new SidedPlane(points[prev], points[i], points[next]);
            this.notablePlanePoints[i] = new GeoPoint[] { points[i], points[next] };
        }
        this.edgePoints = new GeoPoint[] { points[0] };
    }

    /**
     * Constructor for deserialization.
     *
     * @param planetModel is the planet model.
     * @param inputStream is the input stream.
     */
    GeoRegularConvexPolygon(final PlanetModel planetModel, final InputStream inputStream) throws IOException {
        this(planetModel, readGeoPointsFromStream(inputStream));
    }

    private static GeoPoint[] readGeoPointsFromStream(final InputStream inputStream) throws IOException {
        int length = SerializableObject.readInt(inputStream);
        GeoPoint[] points = new GeoPoint[length];
        for (int i = 0; i < length; i++) {
            points[i] = (GeoPoint) SerializableObject.readObject(inputStream);
        }
        return points;
    }

    @Override
    public void write(final OutputStream outputStream) throws IOException {
        SerializableObject.writeInt(outputStream, points.length);
        for (GeoPoint point : points) {
            SerializableObject.writeObject(outputStream, point);
        }
    }

    @Override
    public boolean isWithin(final double x, final double y, final double z) {
        boolean ans = true;
        for (SidedPlane plane : planes) {
            ans &= plane.isWithin(x, y, z);
        }
        return ans;
    }

    @Override
    public GeoPoint[] getEdgePoints() {
        return edgePoints;
    }

    @Override
    public boolean intersects(final Plane p, final GeoPoint[] notablePoints, final Membership... bounds) {
        boolean ans = false;
        for (int i = 0; i < planes.length; i++) {
            int prev = (i < 1) ? points.length - 1 : i - 1;
            int next = (i == points.length - 1) ? 0 : i + 1;
            ans |= p.intersects(planetModel, planes[i], notablePoints, notablePlanePoints[i], bounds, planes[next], planes[prev]);
        }
        return ans;
    }

    @Override
    public boolean intersects(GeoShape geoShape) {
        boolean ans = false;
        for (int i = 0; i < planes.length; i++) {
            int prev = (i < 1) ? points.length - 1 : i - 1;
            int next = (i == points.length - 1) ? 0 : i + 1;
            ans |= geoShape.intersects(planes[i], notablePlanePoints[i], planes[next], planes[prev]);
        }
        return ans;
    }

    @Override
    public void getBounds(Bounds bounds) {
        super.getBounds(bounds);
        for (int i = 0; i < planes.length; i++) {
            int prev = (i < 1) ? points.length - 1 : i - 1;
            int next = (i == points.length - 1) ? 0 : i + 1;
            bounds = bounds.addPlane(planetModel, planes[i], planes[next], planes[prev]);
            bounds = bounds.addPoint(points[i]);
        }
    }

    @Override
    public double outsideDistance(DistanceStyle distanceStyle, double x, double y, double z) {
        double distance = Double.MAX_VALUE;
        for (int i = 0; i < planes.length; i++) {
            int prev = (i < 1) ? points.length - 1 : i - 1;
            int next = (i == points.length - 1) ? 0 : i + 1;
            distance = Math.min(distance, distanceStyle.computeDistance(planetModel, planes[i], x, y, z, planes[next], planes[prev]));
            distance = Math.min(distance, distanceStyle.computeDistance(points[i], x, y, z));
        }
        assert distance != Double.MAX_VALUE;
        return distance;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof GeoRegularConvexPolygon other) {
            if (other.points.length != this.points.length) {
                return false;
            }
            boolean ans = super.equals(other);
            for (int i = 0; i < points.length; i++) {
                ans &= other.points[i].equals(points[i]);
            }
            return ans;
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        for (GeoPoint point : points) {
            result = 31 * result + point.hashCode();
        }
        return result;
    }

    @Override
    public String toString() {
        return "GeoRegularConvexPolygon: {planetmodel=" + planetModel + ", points=" + Arrays.toString(points) + "}";
    }

    /**
     * Test if any of the points comprising this convex polygon are inside the provided shape
     */
    public boolean anyPointInside(GeoAreaShape shape) {
        for (GeoPoint point : points) {
            if (shape.isWithin(point)) {
                return true;
            }
        }
        return false;
    }
}
