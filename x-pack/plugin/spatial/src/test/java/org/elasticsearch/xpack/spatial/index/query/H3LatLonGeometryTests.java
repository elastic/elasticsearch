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
import org.apache.lucene.store.Directory;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.h3.CellBoundary;
import org.elasticsearch.h3.H3;
import org.elasticsearch.h3.LatLng;
import org.elasticsearch.test.ESTestCase;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.distance.GeodesicSphereDistCalc;
import org.locationtech.spatial4j.shape.impl.PointImpl;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

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

    public void testRandomPointAllLevels() {
        Point point = GeometryTestUtils.randomPoint();
        for (int level = 0; level < H3.MAX_H3_RES; level++) {
            doTestLevelAndPoint(level, point);
        }
    }

    private void doTestLevelAndPoint(int level, Point point) {
        long h3 = H3.geoToH3(point.getLat(), point.getLon(), level);
        H3LatLonGeometry h3geom = new H3LatLonGeometry(H3.h3ToString(h3));
        Component2D component = h3geom.toComponent2D();
        CellBoundary boundary = H3.h3ToGeoBoundary(h3);
        String cellName = "H3[l" + level + ":b" + boundary.numPoints() + "]";
        assertThat(h3geom.toString(), containsString(H3.h3ToString(h3)));
        assertThat(
            "Expect the point from which the " + cellName + " cell was created to match the cell",
            component.relate(point.getX(), point.getX(), point.getY(), point.getY()),
            equalTo(PointValues.Relation.CELL_INSIDE_QUERY)
        );

        // Now walk around the boundary of the hexagon and test each vertex point, as well as points inside/outside the hexagon
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
            inside[i] = pointInterpolation(point, vertex, 0.9);
            outside[i] = pointInterpolation(point, vertex, 1.1);
            assertPoint(cellName, component, inside[i], true);
            assertPoint(cellName, component, outside[i], false);
        }
    }

    private void assertPoint(String cellName, Component2D component, Point point, boolean inside) {
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
            equalTo(expected)
        );
    }

    public void testPointInterpolation() {
        Point origin = new Point(2, 2);
        for (int ix = -1; ix <= 1; ix++) {
            for (int iy = -1; iy <= 1; iy++) {
                Point point = new Point(10 * ix, 10 * iy);
                double distance = distance(origin, point);
                Point onEdge = pointInterpolation(origin, point, 1.0);
                assertThat(onEdge, equalTo(point));
                Point inside = pointInterpolation(origin, point, 0.99);
                Point outside = pointInterpolation(origin, point, 1.01);
                double shortDistance = distance(origin, inside);
                double longDistance = distance(origin, outside);
                assertThat(shortDistance, lessThan(distance));
                assertThat(longDistance, greaterThan(distance));
            }
        }
    }

    private double distance(Point from, Point to) {
        PointImpl a = new PointImpl(from.getX(), from.getY(), SpatialContext.GEO);
        PointImpl b = new PointImpl(to.getX(), to.getY(), SpatialContext.GEO);
        return new GeodesicSphereDistCalc.Haversine().distance(a, b);
    }

    private Point pointInterpolation(Point inside, Point border, double factor) {
        double dX = border.getX() - inside.getX();
        double dY = border.getY() - inside.getY();
        double newX = inside.getX() + dX * factor;
        double newY = inside.getY() + dY * factor;
        return new Point(newX, newY);
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
