/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.geo;

import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.geo.XYGeometry;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.MultiLine;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.ShapeType;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class LuceneGeometriesUtilTests extends ESTestCase {

    public void testLatLonPoint() {
        Point point = GeometryTestUtils.randomPoint();
        LatLonGeometry[] geometries = LuceneGeometriesUtil.toLatLonGeometry(point, false, t -> assertEquals(ShapeType.POINT, t));
        assertEquals(1, geometries.length);
        assertLatLonPoint(point, geometries[0]);
    }

    public void testLatLonMultiPoint() {
        MultiPoint multiPoint = GeometryTestUtils.randomMultiPoint(randomBoolean());
        int[] counter = new int[] { 0 };
        LatLonGeometry[] geometries = LuceneGeometriesUtil.toLatLonGeometry(multiPoint, false, t -> {
            if (counter[0]++ == 0) {
                assertEquals(ShapeType.MULTIPOINT, t);
            } else {
                assertEquals(ShapeType.POINT, t);
            }
        });
        assertEquals(multiPoint.size(), geometries.length);
        for (int i = 0; i < multiPoint.size(); i++) {
            assertLatLonPoint(multiPoint.get(i), geometries[i]);
        }
    }

    private void assertLatLonPoint(Point point, LatLonGeometry geometry) {
        assertThat(geometry, instanceOf(org.apache.lucene.geo.Point.class));
        org.apache.lucene.geo.Point lalonPoint = (org.apache.lucene.geo.Point) geometry;
        assertThat(lalonPoint.getLon(), equalTo(point.getLon()));
        assertThat(lalonPoint.getLat(), equalTo(point.getLat()));
        assertThat(geometry, equalTo(LuceneGeometriesUtil.toLucenePoint(point)));
    }

    public void testXYPoint() {
        Point point = GeometryTestUtils.randomPoint();
        XYGeometry[] geometries = LuceneGeometriesUtil.toXYGeometry(point, t -> assertEquals(ShapeType.POINT, t));
        assertEquals(1, geometries.length);
        assertXYPoint(point, geometries[0]);
        assertThat(geometries[0], instanceOf(org.apache.lucene.geo.XYPoint.class));
    }

    public void testXYMultiPoint() {
        MultiPoint multiPoint = GeometryTestUtils.randomMultiPoint(randomBoolean());
        int[] counter = new int[] { 0 };
        XYGeometry[] geometries = LuceneGeometriesUtil.toXYGeometry(multiPoint, t -> {
            if (counter[0]++ == 0) {
                assertEquals(ShapeType.MULTIPOINT, t);
            } else {
                assertEquals(ShapeType.POINT, t);
            }
        });
        assertEquals(multiPoint.size(), geometries.length);
        for (int i = 0; i < multiPoint.size(); i++) {
            assertXYPoint(multiPoint.get(i), geometries[i]);
        }
    }

    private void assertXYPoint(Point point, XYGeometry geometry) {
        assertThat(geometry, instanceOf(org.apache.lucene.geo.XYPoint.class));
        org.apache.lucene.geo.XYPoint xyPoint = (org.apache.lucene.geo.XYPoint) geometry;
        assertThat(xyPoint.getX(), equalTo((float) point.getX()));
        assertThat(xyPoint.getY(), equalTo((float) point.getY()));
        assertThat(geometry, equalTo(LuceneGeometriesUtil.toLuceneXYPoint(point)));
    }

    public void testLatLonLine() {
        Line line = GeometryTestUtils.randomLine(randomBoolean());
        LatLonGeometry[] geometries = LuceneGeometriesUtil.toLatLonGeometry(line, false, t -> assertEquals(ShapeType.LINESTRING, t));
        assertEquals(1, geometries.length);
        assertLatLonLine(line, geometries[0]);
    }

    public void testLatLonMultiLine() {
        MultiLine multiLine = GeometryTestUtils.randomMultiLine(randomBoolean());
        int[] counter = new int[] { 0 };
        LatLonGeometry[] geometries = LuceneGeometriesUtil.toLatLonGeometry(multiLine, false, t -> {
            if (counter[0]++ == 0) {
                assertEquals(ShapeType.MULTILINESTRING, t);
            } else {
                assertEquals(ShapeType.LINESTRING, t);
            }
        });
        assertEquals(multiLine.size(), geometries.length);
        for (int i = 0; i < multiLine.size(); i++) {
            assertLatLonLine(multiLine.get(i), geometries[i]);
        }
    }

    private void assertLatLonLine(Line line, LatLonGeometry geometry) {
        assertThat(geometry, instanceOf(org.apache.lucene.geo.Line.class));
        org.apache.lucene.geo.Line lalonLine = (org.apache.lucene.geo.Line) geometry;
        assertThat(lalonLine.getLons(), equalTo(line.getLons()));
        assertThat(lalonLine.getLats(), equalTo(line.getLats()));
        assertThat(geometry, equalTo(LuceneGeometriesUtil.toLuceneLine(line)));
    }

    public void testXYLine() {
        Line line = GeometryTestUtils.randomLine(randomBoolean());
        XYGeometry[] geometries = LuceneGeometriesUtil.toXYGeometry(line, t -> assertEquals(ShapeType.LINESTRING, t));
        assertEquals(1, geometries.length);
        assertXYLine(line, geometries[0]);
    }

    public void testXYMultiLine() {
        MultiLine multiLine = GeometryTestUtils.randomMultiLine(randomBoolean());
        int[] counter = new int[] { 0 };
        XYGeometry[] geometries = LuceneGeometriesUtil.toXYGeometry(multiLine, t -> {
            if (counter[0]++ == 0) {
                assertEquals(ShapeType.MULTILINESTRING, t);
            } else {
                assertEquals(ShapeType.LINESTRING, t);
            }
        });
        assertEquals(multiLine.size(), geometries.length);
        for (int i = 0; i < multiLine.size(); i++) {
            assertXYLine(multiLine.get(i), geometries[i]);
        }
    }

    private void assertXYLine(Line line, XYGeometry geometry) {
        assertThat(geometry, instanceOf(org.apache.lucene.geo.XYLine.class));
        org.apache.lucene.geo.XYLine xyLine = (org.apache.lucene.geo.XYLine) geometry;
        assertThat(xyLine.getX(), equalTo(LuceneGeometriesUtil.doubleArrayToFloatArray(line.getLons())));
        assertThat(xyLine.getY(), equalTo(LuceneGeometriesUtil.doubleArrayToFloatArray(line.getLats())));
        assertThat(geometry, equalTo(LuceneGeometriesUtil.toLuceneXYLine(line)));
    }

    public void testLatLonPolygon() {
        Polygon polygon = validRandomPolygon(randomBoolean());
        LatLonGeometry[] geometries = LuceneGeometriesUtil.toLatLonGeometry(polygon, false, t -> assertEquals(ShapeType.POLYGON, t));
        if (geometries.length == 2) {
            // this might happen if the polygon has been split on the dateline.
            // we cannot check forward but I think is ok.
            return;
        }
        assertEquals(1, geometries.length);
        assertLatLonPolygon(polygon, geometries[0]);
    }

    public void testLatLonMultiPolygon() {
        MultiPolygon multiPolygon = validRandomMultiPolygon(randomBoolean());
        int[] counter = new int[] { 0 };
        LatLonGeometry[] geometries = LuceneGeometriesUtil.toLatLonGeometry(multiPolygon, false, t -> {
            if (counter[0]++ == 0) {
                assertEquals(ShapeType.MULTIPOLYGON, t);
            } else {
                assertEquals(ShapeType.POLYGON, t);
            }
        });
        if (geometries.length > multiPolygon.size()) {
            // this might happen if the polygon has been split on the dateline.
            // we cannot check forward but I think is ok.
            return;
        }
        assertEquals(multiPolygon.size(), geometries.length);
        for (int i = 0; i < multiPolygon.size(); i++) {
            assertLatLonPolygon(multiPolygon.get(i), geometries[i]);
        }
    }

    private void assertLatLonPolygon(Polygon polygon, LatLonGeometry geometry) {
        assertThat(geometry, instanceOf(org.apache.lucene.geo.Polygon.class));
        org.apache.lucene.geo.Polygon lalonPolygon = (org.apache.lucene.geo.Polygon) geometry;
        assertThat(lalonPolygon.getPolyLons(), equalTo(polygon.getPolygon().getLons()));
        assertThat(lalonPolygon.getPolyLats(), equalTo(polygon.getPolygon().getLats()));
        assertThat(geometry, equalTo(LuceneGeometriesUtil.toLucenePolygon(polygon)));
    }

    public void testXYPolygon() {
        Polygon polygon = GeometryTestUtils.randomPolygon(randomBoolean());
        XYGeometry[] geometries = LuceneGeometriesUtil.toXYGeometry(polygon, t -> assertEquals(ShapeType.POLYGON, t));
        assertEquals(1, geometries.length);
        assertXYPolygon(polygon, geometries[0]);
    }

    public void testXYMultiPolygon() {
        MultiPolygon multiPolygon = GeometryTestUtils.randomMultiPolygon(randomBoolean());
        int[] counter = new int[] { 0 };
        XYGeometry[] geometries = LuceneGeometriesUtil.toXYGeometry(multiPolygon, t -> {
            if (counter[0]++ == 0) {
                assertEquals(ShapeType.MULTIPOLYGON, t);
            } else {
                assertEquals(ShapeType.POLYGON, t);
            }
        });
        assertEquals(multiPolygon.size(), geometries.length);
        for (int i = 0; i < multiPolygon.size(); i++) {
            assertXYPolygon(multiPolygon.get(i), geometries[i]);
        }
    }

    private void assertXYPolygon(Polygon polygon, XYGeometry geometry) {
        assertThat(geometry, instanceOf(org.apache.lucene.geo.XYPolygon.class));
        org.apache.lucene.geo.XYPolygon xyPolygon = (org.apache.lucene.geo.XYPolygon) geometry;
        assertThat(xyPolygon.getPolyX(), equalTo(LuceneGeometriesUtil.doubleArrayToFloatArray(polygon.getPolygon().getX())));
        assertThat(xyPolygon.getPolyY(), equalTo(LuceneGeometriesUtil.doubleArrayToFloatArray(polygon.getPolygon().getY())));
        assertThat(geometry, equalTo(LuceneGeometriesUtil.toLuceneXYPolygon(polygon)));
    }

    public void testLatLonGeometryCollection() {
        boolean hasZ = randomBoolean();
        Point point = GeometryTestUtils.randomPoint(hasZ);
        Line line = GeometryTestUtils.randomLine(hasZ);
        Polygon polygon = validRandomPolygon(hasZ);
        GeometryCollection<Geometry> geometryCollection = new GeometryCollection<>(List.of(point, line, polygon));
        int[] counter = new int[] { 0 };
        LatLonGeometry[] geometries = LuceneGeometriesUtil.toLatLonGeometry(geometryCollection, false, t -> {
            if (counter[0] == 0) {
                assertEquals(ShapeType.GEOMETRYCOLLECTION, t);
            } else if (counter[0] == 1) {
                assertEquals(ShapeType.POINT, t);
            } else if (counter[0] == 2) {
                assertEquals(ShapeType.LINESTRING, t);
            } else if (counter[0] == 3) {
                assertEquals(ShapeType.POLYGON, t);
            } else {
                fail("Unexpected counter value");
            }
            counter[0]++;
        });
        if (geometries.length > geometryCollection.size()) {
            // this might happen if the polygon has been split on the dateline.
            // we cannot check forward but I think is ok.
            return;
        }
        assertEquals(geometryCollection.size(), geometries.length);
        assertLatLonPoint(point, geometries[0]);
        assertLatLonLine(line, geometries[1]);
        assertLatLonPolygon(polygon, geometries[2]);
    }

    public void testXYGeometryCollection() {
        boolean hasZ = randomBoolean();
        Point point = GeometryTestUtils.randomPoint(hasZ);
        Line line = GeometryTestUtils.randomLine(hasZ);
        Polygon polygon = GeometryTestUtils.randomPolygon(hasZ);
        GeometryCollection<Geometry> geometryCollection = new GeometryCollection<>(List.of(point, line, polygon));
        int[] counter = new int[] { 0 };
        XYGeometry[] geometries = LuceneGeometriesUtil.toXYGeometry(geometryCollection, t -> {
            if (counter[0] == 0) {
                assertEquals(ShapeType.GEOMETRYCOLLECTION, t);
            } else if (counter[0] == 1) {
                assertEquals(ShapeType.POINT, t);
            } else if (counter[0] == 2) {
                assertEquals(ShapeType.LINESTRING, t);
            } else if (counter[0] == 3) {
                assertEquals(ShapeType.POLYGON, t);
            } else {
                fail("Unexpected counter value");
            }
            counter[0]++;
        });
        assertEquals(geometryCollection.size(), geometries.length);
        assertXYPoint(point, geometries[0]);
        assertXYLine(line, geometries[1]);
        assertXYPolygon(polygon, geometries[2]);
    }

    private Polygon validRandomPolygon(boolean hasLat) {
        return randomValueOtherThanMany(
            polygon -> GeometryNormalizer.needsNormalize(Orientation.CCW, polygon),
            () -> GeometryTestUtils.randomPolygon(hasLat)
        );
    }

    private MultiPolygon validRandomMultiPolygon(boolean hasLat) {
        return randomValueOtherThanMany(
            multiPolygon -> GeometryNormalizer.needsNormalize(Orientation.CCW, multiPolygon),
            () -> GeometryTestUtils.randomMultiPolygon(hasLat)
        );
    }
}
