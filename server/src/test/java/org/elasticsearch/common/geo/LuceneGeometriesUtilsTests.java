/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.geo;

import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.geo.XYGeometry;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geo.ShapeTestUtils;
import org.elasticsearch.geometry.Circle;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiLine;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.ShapeType;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class LuceneGeometriesUtilsTests extends ESTestCase {

    public void testLatLonPoint() {
        Point point = GeometryTestUtils.randomPoint();
        {
            LatLonGeometry[] geometries = LuceneGeometriesUtils.toLatLonGeometry(point, false, t -> assertEquals(ShapeType.POINT, t));
            assertEquals(1, geometries.length);
            assertLatLonPoint(point, geometries[0]);
        }
        {
            LatLonGeometry[] geometries = LuceneGeometriesUtils.toLatLonGeometry(point, true, t -> assertEquals(ShapeType.POINT, t));
            assertEquals(1, geometries.length);
            assertLatLonPoint(quantize(point), geometries[0]);
        }
    }

    public void testLatLonMultiPoint() {
        MultiPoint multiPoint = GeometryTestUtils.randomMultiPoint(randomBoolean());
        {
            int[] counter = new int[] { 0 };
            LatLonGeometry[] geometries = LuceneGeometriesUtils.toLatLonGeometry(multiPoint, false, t -> {
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
        {
            int[] counter = new int[] { 0 };
            LatLonGeometry[] geometries = LuceneGeometriesUtils.toLatLonGeometry(multiPoint, true, t -> {
                if (counter[0]++ == 0) {
                    assertEquals(ShapeType.MULTIPOINT, t);
                } else {
                    assertEquals(ShapeType.POINT, t);
                }
            });
            assertEquals(multiPoint.size(), geometries.length);
            for (int i = 0; i < multiPoint.size(); i++) {
                assertLatLonPoint(quantize(multiPoint.get(i)), geometries[i]);
            }
        }
    }

    private void assertLatLonPoint(Point point, LatLonGeometry geometry) {
        assertThat(geometry, instanceOf(org.apache.lucene.geo.Point.class));
        org.apache.lucene.geo.Point lalonPoint = (org.apache.lucene.geo.Point) geometry;
        assertThat(lalonPoint.getLon(), equalTo(point.getLon()));
        assertThat(lalonPoint.getLat(), equalTo(point.getLat()));
        assertThat(geometry, equalTo(LuceneGeometriesUtils.toLatLonPoint(point)));
    }

    public void testXYPoint() {
        Point point = ShapeTestUtils.randomPoint();
        XYGeometry[] geometries = LuceneGeometriesUtils.toXYGeometry(point, t -> assertEquals(ShapeType.POINT, t));
        assertEquals(1, geometries.length);
        assertXYPoint(point, geometries[0]);
        assertThat(geometries[0], instanceOf(org.apache.lucene.geo.XYPoint.class));
    }

    public void testXYMultiPoint() {
        MultiPoint multiPoint = ShapeTestUtils.randomMultiPoint(randomBoolean());
        int[] counter = new int[] { 0 };
        XYGeometry[] geometries = LuceneGeometriesUtils.toXYGeometry(multiPoint, t -> {
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
        assertThat(geometry, equalTo(LuceneGeometriesUtils.toXYPoint(point)));
    }

    public void testLatLonLine() {
        Line line = GeometryTestUtils.randomLine(randomBoolean());
        {
            LatLonGeometry[] geometries = LuceneGeometriesUtils.toLatLonGeometry(line, false, t -> assertEquals(ShapeType.LINESTRING, t));
            assertEquals(1, geometries.length);
            assertLatLonLine(line, geometries[0]);
        }
        {
            LatLonGeometry[] geometries = LuceneGeometriesUtils.toLatLonGeometry(line, true, t -> assertEquals(ShapeType.LINESTRING, t));
            assertEquals(1, geometries.length);
            assertLatLonLine(quantize(line), geometries[0]);
        }
    }

    public void testLatLonMultiLine() {
        MultiLine multiLine = GeometryTestUtils.randomMultiLine(randomBoolean());
        {
            int[] counter = new int[] { 0 };
            LatLonGeometry[] geometries = LuceneGeometriesUtils.toLatLonGeometry(multiLine, false, t -> {
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
        {
            int[] counter = new int[] { 0 };
            LatLonGeometry[] geometries = LuceneGeometriesUtils.toLatLonGeometry(multiLine, true, t -> {
                if (counter[0]++ == 0) {
                    assertEquals(ShapeType.MULTILINESTRING, t);
                } else {
                    assertEquals(ShapeType.LINESTRING, t);
                }
            });
            assertEquals(multiLine.size(), geometries.length);
            for (int i = 0; i < multiLine.size(); i++) {
                assertLatLonLine(quantize(multiLine.get(i)), geometries[i]);
            }
        }
    }

    private void assertLatLonLine(Line line, LatLonGeometry geometry) {
        assertThat(geometry, instanceOf(org.apache.lucene.geo.Line.class));
        org.apache.lucene.geo.Line lalonLine = (org.apache.lucene.geo.Line) geometry;
        assertThat(lalonLine.getLons(), equalTo(line.getLons()));
        assertThat(lalonLine.getLats(), equalTo(line.getLats()));
        assertThat(geometry, equalTo(LuceneGeometriesUtils.toLatLonLine(line)));
    }

    public void testXYLine() {
        Line line = ShapeTestUtils.randomLine(randomBoolean());
        XYGeometry[] geometries = LuceneGeometriesUtils.toXYGeometry(line, t -> assertEquals(ShapeType.LINESTRING, t));
        assertEquals(1, geometries.length);
        assertXYLine(line, geometries[0]);
    }

    public void testXYMultiLine() {
        MultiLine multiLine = ShapeTestUtils.randomMultiLine(randomBoolean());
        int[] counter = new int[] { 0 };
        XYGeometry[] geometries = LuceneGeometriesUtils.toXYGeometry(multiLine, t -> {
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
        assertThat(xyLine.getX(), equalTo(LuceneGeometriesUtils.doubleArrayToFloatArray(line.getLons())));
        assertThat(xyLine.getY(), equalTo(LuceneGeometriesUtils.doubleArrayToFloatArray(line.getLats())));
        assertThat(geometry, equalTo(LuceneGeometriesUtils.toXYLine(line)));
    }

    public void testLatLonPolygon() {
        Polygon polygon = validRandomPolygon(randomBoolean());
        {
            LatLonGeometry[] geometries = LuceneGeometriesUtils.toLatLonGeometry(polygon, false, t -> assertEquals(ShapeType.POLYGON, t));
            assertEquals(1, geometries.length);
            assertLatLonPolygon(polygon, geometries[0]);
        }
        {
            LatLonGeometry[] geometries = LuceneGeometriesUtils.toLatLonGeometry(polygon, true, t -> assertEquals(ShapeType.POLYGON, t));
            assertEquals(1, geometries.length);
            assertLatLonPolygon(quantize(polygon), geometries[0]);
        }
    }

    public void testLatLonMultiPolygon() {
        MultiPolygon multiPolygon = validRandomMultiPolygon(randomBoolean());
        {
            int[] counter = new int[] { 0 };
            LatLonGeometry[] geometries = LuceneGeometriesUtils.toLatLonGeometry(multiPolygon, false, t -> {
                if (counter[0]++ == 0) {
                    assertEquals(ShapeType.MULTIPOLYGON, t);
                } else {
                    assertEquals(ShapeType.POLYGON, t);
                }
            });
            assertEquals(multiPolygon.size(), geometries.length);
            for (int i = 0; i < multiPolygon.size(); i++) {
                assertLatLonPolygon(multiPolygon.get(i), geometries[i]);
            }
        }
        {
            int[] counter = new int[] { 0 };
            LatLonGeometry[] geometries = LuceneGeometriesUtils.toLatLonGeometry(multiPolygon, true, t -> {
                if (counter[0]++ == 0) {
                    assertEquals(ShapeType.MULTIPOLYGON, t);
                } else {
                    assertEquals(ShapeType.POLYGON, t);
                }
            });
            assertEquals(multiPolygon.size(), geometries.length);
            for (int i = 0; i < multiPolygon.size(); i++) {
                assertLatLonPolygon(quantize(multiPolygon.get(i)), geometries[i]);
            }
        }
    }

    private void assertLatLonPolygon(Polygon polygon, LatLonGeometry geometry) {
        assertThat(geometry, instanceOf(org.apache.lucene.geo.Polygon.class));
        org.apache.lucene.geo.Polygon lalonPolygon = (org.apache.lucene.geo.Polygon) geometry;
        assertThat(lalonPolygon.getPolyLons(), equalTo(polygon.getPolygon().getLons()));
        assertThat(lalonPolygon.getPolyLats(), equalTo(polygon.getPolygon().getLats()));
        assertThat(geometry, equalTo(LuceneGeometriesUtils.toLatLonPolygon(polygon)));
    }

    public void testXYPolygon() {
        Polygon polygon = ShapeTestUtils.randomPolygon(randomBoolean());
        XYGeometry[] geometries = LuceneGeometriesUtils.toXYGeometry(polygon, t -> assertEquals(ShapeType.POLYGON, t));
        assertEquals(1, geometries.length);
        assertXYPolygon(polygon, geometries[0]);
    }

    public void testXYMultiPolygon() {
        MultiPolygon multiPolygon = ShapeTestUtils.randomMultiPolygon(randomBoolean());
        int[] counter = new int[] { 0 };
        XYGeometry[] geometries = LuceneGeometriesUtils.toXYGeometry(multiPolygon, t -> {
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
        assertThat(xyPolygon.getPolyX(), equalTo(LuceneGeometriesUtils.doubleArrayToFloatArray(polygon.getPolygon().getX())));
        assertThat(xyPolygon.getPolyY(), equalTo(LuceneGeometriesUtils.doubleArrayToFloatArray(polygon.getPolygon().getY())));
        assertThat(geometry, equalTo(LuceneGeometriesUtils.toXYPolygon(polygon)));
    }

    public void testLatLonGeometryCollection() {
        boolean hasZ = randomBoolean();
        Point point = GeometryTestUtils.randomPoint(hasZ);
        Line line = GeometryTestUtils.randomLine(hasZ);
        Polygon polygon = validRandomPolygon(hasZ);
        GeometryCollection<Geometry> geometryCollection = new GeometryCollection<>(List.of(point, line, polygon));
        {
            int[] counter = new int[] { 0 };
            LatLonGeometry[] geometries = LuceneGeometriesUtils.toLatLonGeometry(geometryCollection, false, t -> {
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
            assertLatLonPoint(point, geometries[0]);
            assertLatLonLine(line, geometries[1]);
            assertLatLonPolygon(polygon, geometries[2]);
        }
        {
            int[] counter = new int[] { 0 };
            LatLonGeometry[] geometries = LuceneGeometriesUtils.toLatLonGeometry(geometryCollection, true, t -> {
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
            assertLatLonPoint(quantize(point), geometries[0]);
            assertLatLonLine(quantize(line), geometries[1]);
            assertLatLonPolygon(quantize(polygon), geometries[2]);
        }
    }

    public void testXYGeometryCollection() {
        boolean hasZ = randomBoolean();
        Point point = ShapeTestUtils.randomPoint(hasZ);
        Line line = ShapeTestUtils.randomLine(hasZ);
        Polygon polygon = ShapeTestUtils.randomPolygon(hasZ);
        GeometryCollection<Geometry> geometryCollection = new GeometryCollection<>(List.of(point, line, polygon));
        int[] counter = new int[] { 0 };
        XYGeometry[] geometries = LuceneGeometriesUtils.toXYGeometry(geometryCollection, t -> {
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

    public void testLatLonRectangle() {
        Rectangle rectangle = GeometryTestUtils.randomRectangle();
        LatLonGeometry[] geometries = LuceneGeometriesUtils.toLatLonGeometry(rectangle, false, t -> assertEquals(ShapeType.ENVELOPE, t));
        assertEquals(1, geometries.length);
        assertLatLonRectangle(rectangle, geometries[0]);
    }

    private void assertLatLonRectangle(Rectangle rectangle, LatLonGeometry geometry) {
        assertThat(geometry, instanceOf(org.apache.lucene.geo.Rectangle.class));
        org.apache.lucene.geo.Rectangle lalonRectangle = (org.apache.lucene.geo.Rectangle) geometry;
        assertThat(lalonRectangle.maxLon, equalTo(rectangle.getMaxLon()));
        assertThat(lalonRectangle.minLon, equalTo(rectangle.getMinLon()));
        assertThat(lalonRectangle.maxLat, equalTo(rectangle.getMaxLat()));
        assertThat(lalonRectangle.minLat, equalTo(rectangle.getMinLat()));
        assertThat(geometry, equalTo(LuceneGeometriesUtils.toLatLonRectangle(rectangle)));
    }

    public void testXYRectangle() {
        Rectangle rectangle = ShapeTestUtils.randomRectangle();
        XYGeometry[] geometries = LuceneGeometriesUtils.toXYGeometry(rectangle, t -> assertEquals(ShapeType.ENVELOPE, t));
        assertEquals(1, geometries.length);
        assertXYRectangle(rectangle, geometries[0]);
    }

    private void assertXYRectangle(Rectangle rectangle, XYGeometry geometry) {
        assertThat(geometry, instanceOf(org.apache.lucene.geo.XYRectangle.class));
        org.apache.lucene.geo.XYRectangle xyRectangle = (org.apache.lucene.geo.XYRectangle) geometry;
        assertThat(xyRectangle.maxX, equalTo((float) rectangle.getMaxX()));
        assertThat(xyRectangle.minX, equalTo((float) rectangle.getMinX()));
        assertThat(xyRectangle.maxY, equalTo((float) rectangle.getMaxY()));
        assertThat(xyRectangle.minY, equalTo((float) rectangle.getMinY()));
        assertThat(geometry, equalTo(LuceneGeometriesUtils.toXYRectangle(rectangle)));
    }

    public void testLatLonCircle() {
        Circle circle = GeometryTestUtils.randomCircle(randomBoolean());
        LatLonGeometry[] geometries = LuceneGeometriesUtils.toLatLonGeometry(circle, false, t -> assertEquals(ShapeType.CIRCLE, t));
        assertEquals(1, geometries.length);
        assertLatLonCircle(circle, geometries[0]);
    }

    private void assertLatLonCircle(Circle circle, LatLonGeometry geometry) {
        assertThat(geometry, instanceOf(org.apache.lucene.geo.Circle.class));
        org.apache.lucene.geo.Circle lalonCircle = (org.apache.lucene.geo.Circle) geometry;
        assertThat(lalonCircle.getLon(), equalTo(circle.getLon()));
        assertThat(lalonCircle.getLat(), equalTo(circle.getLat()));
        assertThat(lalonCircle.getRadius(), equalTo(circle.getRadiusMeters()));
        assertThat(geometry, equalTo(LuceneGeometriesUtils.toLatLonCircle(circle)));
    }

    public void testXYCircle() {
        Circle circle = ShapeTestUtils.randomCircle(randomBoolean());
        XYGeometry[] geometries = LuceneGeometriesUtils.toXYGeometry(circle, t -> assertEquals(ShapeType.CIRCLE, t));
        assertEquals(1, geometries.length);
        assertXYCircle(circle, geometries[0]);
    }

    private void assertXYCircle(Circle circle, XYGeometry geometry) {
        assertThat(geometry, instanceOf(org.apache.lucene.geo.XYCircle.class));
        org.apache.lucene.geo.XYCircle xyCircle = (org.apache.lucene.geo.XYCircle) geometry;
        assertThat(xyCircle.getX(), equalTo((float) circle.getX()));
        assertThat(xyCircle.getY(), equalTo((float) circle.getY()));
        assertThat(xyCircle.getRadius(), equalTo((float) circle.getRadiusMeters()));
        assertThat(geometry, equalTo(LuceneGeometriesUtils.toXYCircle(circle)));
    }

    private MultiPolygon validRandomMultiPolygon(boolean hasLat) {
        // make sure we don't generate a polygon that gets splitted across the dateline
        return randomValueOtherThanMany(
            multiPolygon -> GeometryNormalizer.needsNormalize(Orientation.CCW, multiPolygon),
            () -> GeometryTestUtils.randomMultiPolygon(hasLat)
        );
    }

    private Point quantize(Point point) {
        return new Point(GeoUtils.quantizeLon(point.getLon()), GeoUtils.quantizeLat(point.getLat()));
    }

    private Line quantize(Line line) {
        return new Line(
            LuceneGeometriesUtils.LATLON_QUANTIZER.quantizeLons(line.getLons()),
            LuceneGeometriesUtils.LATLON_QUANTIZER.quantizeLats(line.getLats())
        );
    }

    private Polygon quantize(Polygon polygon) {
        List<LinearRing> holes = new ArrayList<>(polygon.getNumberOfHoles());
        for (int i = 0; i < polygon.getNumberOfHoles(); i++) {
            holes.add(quantize(polygon.getHole(i)));
        }
        return new Polygon(quantize(polygon.getPolygon()), holes);
    }

    private LinearRing quantize(LinearRing linearRing) {
        return new LinearRing(
            LuceneGeometriesUtils.LATLON_QUANTIZER.quantizeLons(linearRing.getLons()),
            LuceneGeometriesUtils.LATLON_QUANTIZER.quantizeLats(linearRing.getLats())
        );
    }
}
