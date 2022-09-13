/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.query;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.document.ShapeField;
import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial3d.geom.GeoPoint;
import org.apache.lucene.spatial3d.geom.PlanetModel;
import org.apache.lucene.store.Directory;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.h3.CellBoundary;
import org.elasticsearch.h3.H3;
import org.elasticsearch.h3.LatLng;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.distance.GeodesicSphereDistCalc;
import org.locationtech.spatial4j.shape.impl.PointImpl;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assume.assumeThat;

public class H3LatLonGeometryTests extends ESTestCase {

    private static final String FIELD_NAME = "field";

    public void testIndexPoints() throws Exception {
        Point queryPoint = GeometryTestUtils.randomPoint();
        String[] hexes = new String[H3.MAX_H3_RES + 1];
        for (int res = 0; res < hexes.length; res++) {
            hexes[res] = H3.geoToH3Address(queryPoint.getLat(), queryPoint.getLon(), res);
        }
        IndexWriterConfig iwc = newIndexWriterConfig();
        // Else seeds may not reproduce:
        iwc.setMergeScheduler(new SerialMergeScheduler());
        // Else we can get O(N^2) merging:
        iwc.setMaxBufferedDocs(10);
        Directory dir = newDirectory();
        // RandomIndexWriter is too slow here:
        int[] counts = new int[H3.MAX_H3_RES + 1];
        IndexWriter w = new IndexWriter(dir, iwc);
        for (String hex : hexes) {
            CellBoundary cellBoundary = H3.h3ToGeoBoundary(hex);
            for (int i = 0; i < cellBoundary.numPoints(); i++) {
                Document doc = new Document();
                LatLng latLng = cellBoundary.getLatLon(i);
                doc.add(new LatLonPoint(FIELD_NAME, latLng.getLatDeg(), latLng.getLonDeg()));
                w.addDocument(doc);
                computeCounts(hexes, latLng.getLonDeg(), latLng.getLatDeg(), counts);
            }

        }
        final int numDocs = randomIntBetween(1000, 2000);
        for (int id = 0; id < numDocs; id++) {
            Document doc = new Document();
            Point point = GeometryTestUtils.randomPoint();
            doc.add(new LatLonPoint(FIELD_NAME, point.getLat(), point.getLon()));
            w.addDocument(doc);
            computeCounts(hexes, point.getLon(), point.getLat(), counts);
        }

        if (random().nextBoolean()) {
            w.forceMerge(1);
        }
        final IndexReader r = DirectoryReader.open(w);
        w.close();

        IndexSearcher s = newSearcher(r);
        for (int i = 0; i < H3.MAX_H3_RES + 1; i++) {
            H3LatLonGeometry geometry = new H3LatLonGeometry(hexes[i]);
            Query indexQuery = LatLonPoint.newGeometryQuery(FIELD_NAME, ShapeField.QueryRelation.INTERSECTS, geometry);
            assertEquals(counts[i], s.count(indexQuery));
        }
        IOUtils.close(r, dir);
    }

    public void testOriginLevelZero() {
        doTestLevelAndPoint(0, new Point(0, 0));
    }

    public void testOriginLevelOne() {
        doTestLevelAndPoint(1, new Point(0, 0));
    }

    public void testSpecificPointLevelZero() {
        doTestLevelAndPoint(0, new Point(25, 25));
    }

    public void testSpecificPointLevelOne() {
        doTestLevelAndPoint(1, new Point(25, 25));
    }

    public void testHighLatitudePointLevelZero() {
        doTestLevelAndPoint(0, new Point(22.946810341965456, 80.96342330588482));
    }

    public void testTwoHighLatitudeCells() {
        Point point = new Point(22.946810341965456, 80.96342330588482);
        long h3a = 576495936675512319L;
        long h3b = 576566305419689983L;
        final StringBuilder sb = new StringBuilder();
        for (long h3 : new long[] { h3a, h3b }) {
            H3LatLonGeometry h3geom = new H3LatLonGeometry(H3.h3ToString(h3));
            addH3Polygon(sb, h3geom.toComponent2D(), point, false, false);
        }
        sb.append(")");
        //System.out.println(sb);
    }

    public void testRandomPointAllLevels() {
        Point point = GeometryTestUtils.randomPoint();
        for (int level = 0; level < H3.MAX_H3_RES; level++) {
            doTestLevelAndPoint(level, point);
        }
    }

    private void debugH3Polygon2D(Component2D component, Point origin, boolean boxPoint) {
        final StringBuilder sb = new StringBuilder();
        addH3Polygon(sb, component, origin, true, boxPoint);
        sb.append(")");
        // Uncomment this line to get WKT printout of H3 cell and origin point
        //System.out.println(sb);
    }

    private void addH3Polygon(StringBuilder sb, Component2D component, Point origin, boolean bbox, boolean boxPoint) {
        final double BBOX_EDGE_DELTA = 1e-4;
        if (component instanceof H3LatLonGeometry.H3Polygon2D h3Polygon) {
            h3Polygon.inspect((h3, res, minX, maxX, minY, maxY, boundary) -> {
                //System.out.println("H3 Cell: " + H3.h3ToString(h3));
                if (sb.length() == 0) {
                    sb.append("GEOMETRYCOLLECTION(POLYGON((");
                } else {
                    sb.append(", POLYGON((");
                }
                for (int i = 0; i < boundary.size(); i++) {
                    final Point point = boundary.get(i);
                    if (i > 0) sb.append(", ");
                    double x = point.getX();
                    if (x < -160) x += 360;
                    sb.append(x);
                    sb.append(" ");
                    sb.append(point.getY());
                }
                sb.append("))");
                if (bbox) {
                    addBox(sb, minX, maxX, minY, maxY);
                }
                if (boxPoint) {
                    addBox(
                        sb,
                        origin.getX() - BBOX_EDGE_DELTA,
                        origin.getX() + BBOX_EDGE_DELTA,
                        origin.getY() - BBOX_EDGE_DELTA,
                        origin.getY() + BBOX_EDGE_DELTA
                    );
                }
                sb.append(",POINT(");
                sb.append(origin.getX()).append(" ").append(origin.getY());
                sb.append(")");
            });
        }
    }

    private void addBox(StringBuilder sb, double minX, double maxX, double minY, double maxY) {
        sb.append(",POLYGON((");
        sb.append(minX).append(" ").append(minY).append(", ");
        sb.append(maxX).append(" ").append(minY).append(", ");
        sb.append(maxX).append(" ").append(maxY).append(", ");
        sb.append(minX).append(" ").append(maxY).append(", ");
        sb.append(minX).append(" ").append(minY);
        sb.append("))");
    }

    private void doTestLevelAndPoint(int level, Point point) {
        long h3 = H3.geoToH3(point.getLat(), point.getLon(), level);
        H3LatLonGeometry h3geom = new H3LatLonGeometry(H3.h3ToString(h3));
        Component2D component = h3geom.toComponent2D();
        debugH3Polygon2D(component, point, false);
        CellBoundary boundary = H3.h3ToGeoBoundary(h3);
        String cellName = "H3[l" + level + ":b" + boundary.numPoints() + "]";
        assumeThat("We only test convex hexagons for now", boundary.numPoints(), lessThanOrEqualTo(6));
        assertThat(h3geom.toString(), containsString(H3.h3ToString(h3)));
        assertThat(
            "Expect the point from which the " + cellName + " cell was created to match the cell",
            component.relate(point.getX(), point.getX(), point.getY(), point.getY()),
            either(equalTo(PointValues.Relation.CELL_INSIDE_QUERY)).or(equalTo(PointValues.Relation.CELL_CROSSES_QUERY))
        );
        assertThat(
            "Expect the point from which the " + cellName + " cell was created to match the cell",
            component.contains(point.getX(), point.getY()),
            equalTo(true)
        );

        // Now walk around the boundary of the hexagon and test each vertex point, as well as points inside/outside the hexagon
        Point centroid = calculateCentroid(boundary);
        Point[] inside = new Point[boundary.numPoints()];
        Point[] outside = new Point[boundary.numPoints()];
        for (int i = 0; i < boundary.numPoints(); i++) {
            LatLng vertexLatLng = boundary.getLatLon(i);
            Point vertex = new Point(vertexLatLng.getLonDeg(), vertexLatLng.getLatDeg());
            // Some vertex points will be seen as contained by adjacent cells, but all are related to the current cell
            assertThat(
                "Vertex[" + i + "] intersects " + cellName + " cell",
                component.relate(vertex.getX(), vertex.getX(), vertex.getY(), vertex.getY()),
                equalTo(PointValues.Relation.CELL_CROSSES_QUERY)
            );
            // Create points inside and outside the cell at the specific vertex and test them
            inside[i] = pointInterpolation(centroid, vertex, 0.9);
            outside[i] = pointInterpolation(centroid, vertex, 1.1);
            assertPointAndLine(cellName, component, point, inside[i], true);
            assertPointAndLine(cellName, component, point, outside[i], false);
            assertPointAndLine(cellName, component, inside[i], outside[i], false);
        }

        // For each adjacent pair of inside/outside points, test the triangles composed of those points and the inner centroid point
        for (int i = 1; i < inside.length; i++) {
            assertLineAndTriangle(cellName, component, centroid, inside[i - 1], inside[i], true);
            assertLineAndTriangle(cellName, component, centroid, outside[i - 1], outside[i], false);
        }

        // For points on opposite sides of the hexagon, test the lines going through the hexagon
        for (int i = 0; i < outside.length / 2; i++) {
            Point a = outside[i];
            Point b = outside[i + outside.length / 2];
            // Construct a point outside the hexagon such that the three points form a triangle that does not intersect
            Point o = pointInterpolation(outside[i + 1], outside[i + 2], 0.5);
            Point pointOutside = pointInterpolation(centroid, o, 2);
            assertLineAndTriangleOutsideHexagon(cellName, component, a, b, true, pointOutside);
            assertLineAndTriangleOutsideHexagon(cellName, component, a, b, false, pointOutside);
        }
    }

    private Point calculateCentroid(CellBoundary boundary) {
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
        GeoPoint centroid3D = new GeoPoint(centroidX, centroidY, centroidZ);
        return new Point(Math.toDegrees(centroid3D.getLongitude()), Math.toDegrees(centroid3D.getLatitude()));
    }

    private void assertPointAndLine(String cellName, Component2D component, Point origin, Point point, boolean inside) {
        // Test that point intersects hexagon
        String name = inside ? "Inside" : "Outside";
        assertThat(
            name + " " + point + " intersects " + cellName + " cell",
            component.contains(point.getX(), point.getY()),
            equalTo(inside)
        );

        // Test that relationship between point and hexagon is as expected
        PointValues.Relation expected = inside ? PointValues.Relation.CELL_INSIDE_QUERY : PointValues.Relation.CELL_OUTSIDE_QUERY;
        assertThat(
            name + " " + point + " should relate to " + cellName + " cell with " + expected,
            component.relate(point.getX(), point.getX(), point.getY(), point.getY()),
            either(equalTo(expected)).or(equalTo(PointValues.Relation.CELL_CROSSES_QUERY))
        );

        // Test that the line from the origin (INSIDE hexagon) and point (either INSIDE or OUTSIDE) is as expected
        double minX = Math.min(origin.getX(), point.getX());
        double maxX = Math.max(origin.getX(), point.getX());
        double minY = Math.min(origin.getY(), point.getY());
        double maxY = Math.max(origin.getY(), point.getY());
        assertThat(
            "Line from inner " + point + " to " + name + " point should relate with NOTWITHIN " + cellName,
            component.withinLine(minX, maxX, minY, maxY, origin.getX(), origin.getY(), true, point.getX(), point.getY()),
            equalTo(Component2D.WithinRelation.NOTWITHIN)
        );
        assertThat(
            "Hexagon.containsLine(Origin," + name + ") for " + cellName,
            component.containsLine(minX, maxX, minY, maxY, origin.getX(), origin.getY(), point.getX(), point.getY()),
            equalTo(inside)
        );
    }

    private void assertLineAndTriangle(String cellName, Component2D component, Point origin, Point a, Point b, boolean inside) {
        // Make sure this method is only called with origin inside, and points inside/outside the hexagon
        assertThat(origin + " in " + cellName, component.contains(origin.getX(), origin.getY()), equalTo(true));
        assertThat(a + " in " + cellName, component.contains(a.getX(), a.getY()), equalTo(inside));
        assertThat(b + " in " + cellName, component.contains(b.getX(), b.getY()), equalTo(inside));

        // First test the line composed of the two points
        String lineName = inside ? "inner" : "outer";
        // TODO verify this relation
        Component2D.WithinRelation withinLine = inside ? Component2D.WithinRelation.NOTWITHIN : Component2D.WithinRelation.DISJOINT;
        for (boolean partOfShape : new boolean[] { true, false }) {
            assertThat(
                "Line between two " + lineName + " points, and is " + (partOfShape ? "part" : "not part") + " of the shape",
                component.withinLine(a.getX(), a.getY(), partOfShape, b.getX(), b.getY()),
                equalTo(withinLine)
            );
        }
        assertThat(
            "Line between two " + lineName + " points (" + a + " and " + b + ")",
            component.withinLine(a.getX(), a.getY(), false, b.getX(), b.getY()),
            equalTo(withinLine)
        );

        // Now test the triangle composed of those two points and the origin
        String triangleName = inside ? "three inner points" : "one inner point and two outer points";
        assertThat(
            "Triangle with " + triangleName + " should intersect " + cellName,
            component.intersectsTriangle(origin.getX(), origin.getY(), a.getX(), a.getY(), b.getX(), b.getY()),
            equalTo(true)
        );
        Component2D.WithinRelation withinTriangle = Component2D.WithinRelation.NOTWITHIN;
        assertThat(
            "Triangle with " + triangleName + " should have withinTriangle relation for " + cellName,
            component.withinTriangle(origin.getX(), origin.getY(), false, a.getX(), a.getY(), true, b.getX(), b.getY(), false),
            equalTo(withinTriangle)
        );
        assertThat(
            "Triangle with " + triangleName + " should " + (inside ? "be" : "not be") + " contained within the " + cellName,
            component.containsTriangle(origin.getX(), origin.getY(), a.getX(), a.getY(), b.getX(), b.getY()),
            equalTo(inside)
        );

        // Construct a point outside the hexagon such that if the two points are outside, the entire triangle is outside
        Point ab = pointInterpolation(a, b, 0.5);
        Point outside = pointInterpolation(origin, ab, 2);
        lineName = inside ? "a point inside and a point outside" : "two points outside and not intersecting";
        // Line from midpoint to outside will cross hexagon if midpoint is inside
        for (boolean partOfShape : new boolean[] { true, false }) {
            assertThat(
                "Line made of " + lineName + ", and is " + (partOfShape ? "part" : "not part") + " of the shape for " + cellName,
                component.withinLine(ab.getX(), ab.getY(), partOfShape, outside.getX(), outside.getY()),
                equalTo(withinLine)
            );
        }
        // Now test the triangle that is either entirely outside or crossing the hexagon
        triangleName = inside ? "two inner points and one outer point" : "three outer points";
        assertThat(
            "Triangle with " + triangleName + " should " + (inside ? "intersect" : "not intersect") + " " + cellName,
            component.intersectsTriangle(outside.getX(), outside.getY(), b.getX(), b.getY(), a.getX(), a.getY()),
            equalTo(inside)
        );
        withinTriangle = inside ? Component2D.WithinRelation.NOTWITHIN : Component2D.WithinRelation.DISJOINT;
        assertThat(
            "Triangle with " + triangleName + " should have withinTriangle relation for " + cellName,
            component.withinTriangle(outside.getX(), outside.getY(), false, b.getX(), b.getY(), true, a.getX(), a.getY(), false),
            equalTo(withinTriangle)
        );
        assertThat(
            "Triangle with " + triangleName + " should not be contained within " + cellName,
            component.containsTriangle(outside.getX(), outside.getY(), b.getX(), b.getY(), a.getX(), a.getY()),
            equalTo(false)
        );
    }

    private void assertLineAndTriangleOutsideHexagon(
        String cellName,
        Component2D component,
        Point a,
        Point b,
        boolean partOfShape,
        Point outside
    ) {
        // Make sure this method is only called with points outside the hexagon
        assertThat(a + " should not be contained in " + cellName, component.contains(a.getX(), a.getY()), equalTo(false));
        assertThat(b + " should not be contained in " + cellName, component.contains(b.getX(), b.getY()), equalTo(false));

        // If the line is part of the original shape, we know the hexagon is not within the original shape
        Component2D.WithinRelation withinLine = partOfShape ? Component2D.WithinRelation.NOTWITHIN : Component2D.WithinRelation.DISJOINT;
        String lineName = partOfShape ? "part of shape" : "not part of shape";
        assertThat(
            "Line between two outside points that are " + lineName,
            component.withinLine(a.getX(), a.getY(), partOfShape, b.getX(), b.getY()),
            equalTo(withinLine)
        );

        // Test a triangle of three points all outside the hexagon, but with the ab line crossing the hexagon
        String triangleName = "Triangle with three inner points and one line crossing which is "
            + (partOfShape ? "part of the shape" : "not part of the shape");
        assertThat(
            triangleName + " should intersect " + cellName,
            component.intersectsTriangle(outside.getX(), outside.getY(), a.getX(), a.getY(), b.getX(), b.getY()),
            equalTo(true)
        );
        Component2D.WithinRelation withinTriangle = partOfShape
            ? Component2D.WithinRelation.NOTWITHIN
            : Component2D.WithinRelation.CANDIDATE;
        assertThat(
            triangleName + " should have withinTriangle relation with " + cellName,
            component.withinTriangle(outside.getX(), outside.getY(), false, a.getX(), a.getY(), partOfShape, b.getX(), b.getY(), false),
            equalTo(withinTriangle)
        );
        assertThat(
            triangleName + " should not be contained within " + cellName,
            component.containsTriangle(outside.getX(), outside.getY(), a.getX(), a.getY(), b.getX(), b.getY()),
            equalTo(false)
        );
    }

    public void testPointInterpolation() {
        Point origin = new Point(2, 2);
        for (int ix = -1; ix <= 1; ix++) {
            for (int iy = -1; iy <= 1; iy++) {
                assertPointInterpolation(origin, new Point(10 * ix, 10 * iy));
            }
        }
    }

    public void testPointInterpolationAcrossDateline() {
        Point origin = new Point(-172, 1);
        String h3 = H3.geoToH3Address(origin.getLat(), origin.getLon(), 0);
        CellBoundary boundary = H3.h3ToGeoBoundary(h3);
        for (int i = 0; i < boundary.numPoints(); i++) {
            LatLng latLng = boundary.getLatLon(i);
            assertPointInterpolation(origin, new Point(latLng.getLonDeg(), latLng.getLatDeg()));
        }
    }

    private void assertPointInterpolation(Point origin, Point point) {
        Point midpoint = pointInterpolation(origin, point, 0.5);
        double distance = distance(origin, point);
        double innerDist = distance(origin, midpoint);
        double outerDist = distance(midpoint, point);
        assertThat("Distance to midpoint", innerDist, closeTo(outerDist, 1e-12));
        assertThat("Distance to midpoint", distance, closeTo(innerDist * 2, 1e-12));
        Point onEdge = pointInterpolation(origin, point, 1.0);
        assertThat(onEdge, matchesPoint(point));
        Point inside = pointInterpolation(origin, point, 0.99);
        Point outside = pointInterpolation(origin, point, 1.01);
        double shortDistance = distance(origin, inside);
        double longDistance = distance(origin, outside);
        assertThat("Inside " + inside + " should be closer than " + point + " to " + origin, shortDistance, lessThan(distance));
        assertThat("Outside " + outside + " should be closer than " + point + " to " + origin, longDistance, greaterThan(distance));
    }

    private double distance(Point from, Point to) {
        PointImpl a = new PointImpl(from.getX(), from.getY(), SpatialContext.GEO);
        PointImpl b = new PointImpl(to.getX(), to.getY(), SpatialContext.GEO);
        return new GeodesicSphereDistCalc.Haversine().distance(a, b);
    }

    private Point pointInterpolation(Point inside, Point border, double factor) {
        GeoPoint inside3d = new GeoPoint(PlanetModel.SPHERE, Math.toRadians(inside.getLat()), Math.toRadians(inside.getLon()));
        GeoPoint border3d = new GeoPoint(PlanetModel.SPHERE, Math.toRadians(border.getLat()), Math.toRadians(border.getLon()));
        double dX = border3d.x - inside3d.x;
        double dY = border3d.y - inside3d.y;
        double dZ = border3d.z - inside3d.z;
        double newX = inside3d.x + dX * factor;
        double newY = inside3d.y + dY * factor;
        double newZ = inside3d.z + dZ * factor;
        GeoPoint point = new GeoPoint(newX, newY, newZ);
        return new Point(Math.toDegrees(point.getLongitude()), Math.toDegrees(point.getLatitude()));
    }

    private static Matcher<Point> matchesPoint(Point point) {
        return new PointMatcher(point);
    }

    private static class PointMatcher extends BaseMatcher<Point> {
        public static final double THRESHOLD = 1e-10;
        private final Point point;

        PointMatcher(Point point) {
            this.point = point;
        }

        @Override
        public boolean matches(Object actual) {
            if (actual instanceof Point other) {
                return (Math.abs(other.getX() - point.getX()) < THRESHOLD) && (Math.abs(other.getY() - point.getY()) < THRESHOLD);
            }
            return false;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("Comparing " + point);
        }
    }

    private void computeCounts(String[] hexes, double lon, double lat, int[] counts) {
        double qLat = GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(lat));
        double qLon = GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(lon));
        for (int res = 0; res < hexes.length; res++) {
            if (hexes[res].equals(H3.geoToH3Address(qLat, qLon, res))) {
                counts[res]++;
            }
        }
    }
}
