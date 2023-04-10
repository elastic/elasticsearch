/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.apache.lucene.document.ShapeField.QueryRelation;
import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.geo.Line;
import org.apache.lucene.geo.Point;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.tests.geo.GeoTestUtil;
import org.elasticsearch.common.geo.GeometryNormalizer;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.spatial.util.GeoTestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.lucene.geo.GeoEncodingUtils.decodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.decodeLongitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitude;
import static org.hamcrest.Matchers.equalTo;

public class LatLonGeometryRelationVisitorTests extends ESTestCase {

    public void testPoint() throws Exception {
        doTestShapes(GeoTestUtil::nextPoint);
    }

    public void testLine() throws Exception {
        doTestShapes(GeoTestUtil::nextLine);
    }

    public void testPolygon() throws Exception {
        doTestShapes(GeoTestUtil::nextPolygon);
    }

    // Specific tests for known troublesome points from https://github.com/elastic/elasticsearch/issues/92151
    public void testTroublesomePoints() throws Exception {
        ArrayList<org.elasticsearch.geometry.Point> points = new ArrayList<>();
        points.add(new org.elasticsearch.geometry.Point(-6.6957112520498185, 5.337277253715181E-129));
        points.add(new org.elasticsearch.geometry.Point(0.0, 1.6938947866910307E-202));
        points.add(new org.elasticsearch.geometry.Point(-114.40977624485328, -37.484381576244864));
        points.add(new org.elasticsearch.geometry.Point(49.1828546738179, 23.813793855174865));
        points.add(new org.elasticsearch.geometry.Point(60.5683489131913, 90.0));
        points.add(new org.elasticsearch.geometry.Point(-79.65717776327665, -39.5));
        points.add(new org.elasticsearch.geometry.Point(1.401298464324817E-45, 0.0));
        MultiPoint geometry = new MultiPoint(points);
        GeoShapeValues.GeoShapeValue geoShapeValue = GeoTestUtils.geoShapeValue(geometry);
        GeometryDocValueReader reader = GeoTestUtils.geometryDocValueReader(geometry, CoordinateEncoder.GEO);
        Point[] troublesome = new Point[] {
            new Point(-6.404126347681029E-213, 0.0),    // This point will match the last point in the multipoint
            new Point(0.0, 0.0)                         // this point will match the second point in the multipoint
        };
        for (Point point : troublesome) {
            doTestShape(geometry, geoShapeValue, reader, quantize(point));
        }
    }

    public void testIdenticalPoint() throws Exception {
        double x = quantizeLon(1);
        double y = quantizeLat(1);
        org.elasticsearch.geometry.Point shape = new org.elasticsearch.geometry.Point(x, y);
        Point latLonGeometry = new Point(shape.getLat(), shape.getLon());

        GeoShapeValues.GeoShapeValue geoShapeValue = GeoTestUtils.geoShapeValue(shape);
        GeometryDocValueReader reader = GeoTestUtils.geometryDocValueReader(shape, CoordinateEncoder.GEO);
        GeoRelation relation = geoShapeValue.relate(latLonGeometry);
        assertThat("Identical points", relation, equalTo(GeoRelation.QUERY_INSIDE));
        doTestShape(shape, reader, latLonGeometry, relation, true);
    }

    public void testVeryFlatPolygonDoesNotContainIntersectingLine() throws Exception {
        double[] x = new double[] { -0.001, -0.001, 0.001, 0.001, -0.001 };
        double[] y = new double[] { 1e-10, 0, -1e-10, 0, 1e-10 };
        Geometry geometry = new org.elasticsearch.geometry.Polygon(new LinearRing(x, y));
        GeoShapeValues.GeoShapeValue geoShapeValue = GeoTestUtils.geoShapeValue(geometry);
        GeometryDocValueReader reader = GeoTestUtils.geometryDocValueReader(geometry, CoordinateEncoder.GEO);
        double[] lons = new double[] { 0.0, 0.0 };
        double[] lats = new double[] { 0.0, 0.001 };
        Line line = new Line(lats, lons);
        doTestShape(geometry, geoShapeValue, reader, line);
    }

    public void testContainedPolygons() throws Exception {
        // Create simple rectangular polygon
        double[] x = new double[] { -1, 1, 1, -1, -1 };
        double[] y = new double[] { -1, -1, 1, 1, -1 };
        quantize(y, x);
        org.elasticsearch.geometry.Polygon shape = new org.elasticsearch.geometry.Polygon(new LinearRing(x, y));

        // Setup tests for contains, identical and within
        LinkedHashMap<Double, GeoRelation> tests = new LinkedHashMap<>();
        tests.put(0.5, GeoRelation.QUERY_INSIDE);
        tests.put(1.0, GeoRelation.QUERY_CONTAINS);
        tests.put(2.0, GeoRelation.QUERY_CONTAINS);
        for (Map.Entry<Double, GeoRelation> entry : tests.entrySet()) {
            double factor = entry.getKey();
            GeoRelation expected = entry.getValue();
            double[] lats = new double[y.length];
            double[] lons = new double[x.length];
            for (int i = 0; i < x.length; i++) {
                lats[i] = quantizeLat(y[i] * factor);
                lons[i] = quantizeLon(x[i] * factor);
            }
            Polygon latLonGeometry = new Polygon(lats, lons);
            boolean identical = factor == 1.0;
            // Assert that polygons are identical
            if (identical) {
                for (int i = 0; i < latLonGeometry.numPoints(); i++) {
                    assertThat("Latitude[" + i + "]", latLonGeometry.getPolyLat(i), equalTo(shape.getPolygon().getLat(i)));
                    assertThat("Longitude[" + i + "]", latLonGeometry.getPolyLon(i), equalTo(shape.getPolygon().getLon(i)));
                }
            }

            GeoShapeValues.GeoShapeValue geoShapeValue = GeoTestUtils.geoShapeValue(shape);
            GeometryDocValueReader reader = GeoTestUtils.geometryDocValueReader(shape, CoordinateEncoder.GEO);
            GeoRelation relation = geoShapeValue.relate(latLonGeometry);
            assertThat("Polygon scaled by " + factor, relation, equalTo(expected));
            doTestShape(shape, reader, latLonGeometry, relation, false);
        }
    }

    private <T extends LatLonGeometry> void doTestShapes(Supplier<T> supplier) throws Exception {
        Geometry geometry = GeometryNormalizer.apply(Orientation.CCW, GeometryTestUtils.randomGeometryWithoutCircle(0, false));
        GeoShapeValues.GeoShapeValue geoShapeValue = GeoTestUtils.geoShapeValue(geometry);
        GeometryDocValueReader reader = GeoTestUtils.geometryDocValueReader(geometry, CoordinateEncoder.GEO);
        for (int i = 0; i < 1000; i++) {
            LatLonGeometry latLonGeometry = quantize(supplier.get());
            doTestShape(geometry, geoShapeValue, reader, latLonGeometry);
        }
    }

    private void doTestShape(
        Geometry geometry,
        GeoShapeValues.GeoShapeValue geoShapeValue,
        GeometryDocValueReader reader,
        LatLonGeometry latLonGeometry
    ) throws Exception {
        doTestShape(geometry, reader, latLonGeometry, geoShapeValue.relate(latLonGeometry));
    }

    private void doTestShape(Geometry geometry, GeometryDocValueReader reader, LatLonGeometry latLonGeometry, GeoRelation relation)
        throws Exception {
        doTestShape(geometry, reader, latLonGeometry, relation, isIdenticalPoint(geometry, latLonGeometry));
    }

    private boolean isIdenticalPoint(Geometry geometry, LatLonGeometry latLonGeometry) {
        if (latLonGeometry instanceof Point latLonPoint) {
            if (geometry instanceof org.elasticsearch.geometry.Point point) {
                return encodeLatitude(point.getLat()) == encodeLatitude(latLonPoint.getLat())
                    && encodeLongitude(point.getLon()) == encodeLongitude(latLonPoint.getLon());
            } else if (geometry instanceof org.elasticsearch.geometry.Line line) {
                for (int i = 0; i < line.length(); i++) {
                    if (encodeLatitude(line.getLat(i)) != encodeLatitude(latLonPoint.getLat())
                        || encodeLongitude(line.getLon(i)) != encodeLongitude(latLonPoint.getLon())) {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    private boolean pointsOnly(Geometry geometry) {
        return geometry instanceof org.elasticsearch.geometry.Point || geometry instanceof org.elasticsearch.geometry.MultiPoint;
    }

    private boolean pointsOnly(LatLonGeometry geometry) {
        return geometry instanceof Point;
    }

    private void doTestShape(
        Geometry geometry,
        GeometryDocValueReader reader,
        LatLonGeometry latLonGeometry,
        GeoRelation relation,
        boolean identicalPoint  // When both geometries are points and identical, then CONTAINS==WITHIN
    ) throws Exception {
        boolean pointsOnly = pointsOnly(geometry) && pointsOnly(latLonGeometry);
        String description = "Geometry " + latLonGeometry + " relates to shape " + geometry.getClass().getSimpleName() + ": " + relation;
        Component2D component2D = LatLonGeometry.create(latLonGeometry);
        Component2DVisitor contains = visitQueryRelation(component2D, QueryRelation.CONTAINS, reader);
        Component2DVisitor intersects = visitQueryRelation(component2D, QueryRelation.INTERSECTS, reader);
        Component2DVisitor disjoint = visitQueryRelation(component2D, QueryRelation.DISJOINT, reader);
        Component2DVisitor within = visitQueryRelation(component2D, QueryRelation.WITHIN, reader);
        if (relation == GeoRelation.QUERY_INSIDE) {
            assertThat("CONTAINS/" + relation + ": " + description, contains.matches(), equalTo(true));
            assertThat("INTERSECTS/" + relation + ": " + description, intersects.matches(), equalTo(true));
            assertThat("DISJOINT/" + relation + ": " + description, disjoint.matches(), equalTo(false));
            assertThat("WITHIN/" + relation + ": " + description, within.matches(), equalTo(identicalPoint));
        } else if (relation == GeoRelation.QUERY_CROSSES) {
            if (pointsOnly == false) {
                // When we have point comparisons, CROSSES can also allow CONTAINS
                assertThat("CONTAINS/" + relation + ": " + description, contains.matches(), equalTo(false));
            }
            assertThat("INTERSECTS/" + relation + ": " + description, intersects.matches(), equalTo(true));
            assertThat("DISJOINT/" + relation + ": " + description, disjoint.matches(), equalTo(false));
            assertThat("WITHIN/" + relation + ": " + description, within.matches(), equalTo(false));
        } else if (relation == GeoRelation.QUERY_CONTAINS) {
            assertThat("CONTAINS/" + relation + ": " + description, contains.matches(), equalTo(identicalPoint));
            assertThat("INTERSECTS/" + relation + ": " + description, intersects.matches(), equalTo(true));
            assertThat("DISJOINT/" + relation + ": " + description, disjoint.matches(), equalTo(false));
            assertThat("WITHIN/" + relation + ": " + description, within.matches(), equalTo(true));
        } else {
            assertThat("CONTAINS/" + relation + ": " + description, contains.matches(), equalTo(false));
            assertThat("INTERSECTS/" + relation + ": " + description, intersects.matches(), equalTo(false));
            assertThat("DISJOINT/" + relation + ": " + description, disjoint.matches(), equalTo(true));
            assertThat("WITHIN/" + relation + ": " + description, within.matches(), equalTo(false));
        }
    }

    private Component2DVisitor visitQueryRelation(Component2D component2D, QueryRelation queryRelation, GeometryDocValueReader reader)
        throws IOException {
        Component2DVisitor contains = Component2DVisitor.getVisitor(component2D, queryRelation, CoordinateEncoder.GEO);
        reader.visit(contains);
        return contains;
    }

    private LatLonGeometry quantize(LatLonGeometry geometry) {
        if (geometry instanceof Point point) {
            return quantize(point);
        } else if (geometry instanceof Line line) {
            return quantize(line);
        } else if (geometry instanceof Polygon polygon) {
            return quantize(polygon);
        } else {
            throw new IllegalArgumentException("Unimplemented: quantize(" + geometry.getClass().getSimpleName() + ")");
        }
    }

    private Point quantize(Point point) {
        return new Point(quantizeLat(point.getLat()), quantizeLon(point.getLon()));
    }

    private Line quantize(Line line) {
        double[] lons = line.getLons();
        double[] lats = line.getLats();
        quantize(lats, lons);
        return new Line(lats, lons);
    }

    private Polygon quantize(Polygon polygon) {
        Polygon[] holes = polygon.getHoles();
        for (int i = 0; i < holes.length; i++) {
            holes[i] = quantize(holes[i]);
        }
        double[] lats = polygon.getPolyLats();
        double[] lons = polygon.getPolyLons();
        quantize(lats, lons);
        return new Polygon(lats, lons, holes);
    }

    private void quantize(double[] lats, double[] lons) {
        for (int i = 0; i < lons.length; i++) {
            lats[i] = quantizeLat(lats[i]);
            lons[i] = quantizeLon(lons[i]);
        }
    }

    private double quantizeLat(double lat) {
        return decodeLatitude(encodeLatitude(lat));
    }

    private double quantizeLon(double lon) {
        return decodeLongitude(encodeLongitude(lon));
    }
}
