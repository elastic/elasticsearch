/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.common;

import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.tests.geo.GeoTestUtil;
import org.elasticsearch.common.geo.GeometryNormalizer;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.h3.CellBoundary;
import org.elasticsearch.h3.H3;
import org.elasticsearch.h3.LatLng;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoRelation;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues;
import org.elasticsearch.xpack.spatial.util.GeoTestUtils;
import org.hamcrest.Matchers;

import java.io.IOException;

public class H3CartesianUtilTests extends ESTestCase {

    public void testLevel1() throws IOException {
        for (int i = 0; i < 10000; i++) {
            Point point = GeometryTestUtils.randomPoint();
            GeoShapeValues.GeoShapeValue geoValue = GeoTestUtils.geoShapeValue(point);
            boolean inside = false;
            for (long h3 : H3.getLongRes0Cells()) {
                if (geoValue.relate(H3CartesianUtil.getLatLonGeometry(h3)) != GeoRelation.QUERY_DISJOINT) {
                    inside = true;
                    break;
                }
            }
            if (inside == false) {
                fail(
                    "failing matching point: " + WellKnownText.toWKT(new org.elasticsearch.geometry.Point(point.getLon(), point.getLat()))
                );
            }
        }
    }

    public void testLevel2() throws IOException {
        for (int i = 0; i < 10000; i++) {
            Point point = GeometryTestUtils.randomPoint();
            GeoShapeValues.GeoShapeValue geoValue = GeoTestUtils.geoShapeValue(point);
            boolean inside = false;
            for (long res0Cell : H3.getLongRes0Cells()) {
                for (long h3 : H3.h3ToChildren(res0Cell)) {
                    if (geoValue.relate(H3CartesianUtil.getLatLonGeometry(h3)) != GeoRelation.QUERY_DISJOINT) {
                        inside = true;
                        break;
                    }
                }
            }
            if (inside == false) {
                fail(
                    "failing matching point: " + WellKnownText.toWKT(new org.elasticsearch.geometry.Point(point.getLon(), point.getLat()))
                );
            }
        }
    }

    public void testNorthPole() throws IOException {
        for (int res = 0; res <= H3.MAX_H3_RES; res++) {
            final long h3 = H3.geoToH3(90, 0, res);
            final LatLonGeometry latLonGeometry = H3CartesianUtil.getLatLonGeometry(h3);
            final double lon = GeoTestUtil.nextLongitude();
            {
                GeoShapeValues.GeoShapeValue geoValue = GeoTestUtils.geoShapeValue(new Point(lon, 90));
                assertThat(geoValue.relate(latLonGeometry), Matchers.equalTo(GeoRelation.QUERY_CONTAINS));
            }
            {
                final double bound = H3CartesianUtil.getNorthPolarBound(res);
                final double lat = randomValueOtherThanMany(l -> l > bound, GeoTestUtil::nextLatitude);
                GeoShapeValues.GeoShapeValue geoValue = GeoTestUtils.geoShapeValue(new Point(lon, lat));
                assertThat(geoValue.relate(latLonGeometry), Matchers.equalTo(GeoRelation.QUERY_DISJOINT));
            }
        }
    }

    public void testSouthPole() throws IOException {
        for (int res = 0; res <= H3.MAX_H3_RES; res++) {
            final long h3 = H3.geoToH3(-90, 0, res);
            final LatLonGeometry latLonGeometry = H3CartesianUtil.getLatLonGeometry(h3);
            final double lon = GeoTestUtil.nextLongitude();
            {
                GeoShapeValues.GeoShapeValue geoValue = GeoTestUtils.geoShapeValue(new Point(lon, -90));
                assertThat(geoValue.relate(latLonGeometry), Matchers.equalTo(GeoRelation.QUERY_CONTAINS));
            }
            {
                final double bound = H3CartesianUtil.getSouthPolarBound(res);
                final double lat = randomValueOtherThanMany(l -> l < bound, GeoTestUtil::nextLatitude);
                GeoShapeValues.GeoShapeValue geoValue = GeoTestUtils.geoShapeValue(new Point(lon, lat));
                assertThat(geoValue.relate(latLonGeometry), Matchers.equalTo(GeoRelation.QUERY_DISJOINT));
            }
        }
    }

    public void testDateline() throws IOException {
        final long h3 = H3.geoToH3(0, 180, 0);
        final LatLonGeometry latLonGeometry = H3CartesianUtil.getLatLonGeometry(h3);
        // points
        {
            GeoShapeValues.GeoShapeValue geoValue = GeoTestUtils.geoShapeValue(new org.elasticsearch.geometry.Point(0, 0));
            assertThat(geoValue.relate(latLonGeometry), Matchers.equalTo(GeoRelation.QUERY_DISJOINT));
            geoValue = GeoTestUtils.geoShapeValue(new org.elasticsearch.geometry.Point(180, 0));
            assertThat(geoValue.relate(latLonGeometry), Matchers.equalTo(GeoRelation.QUERY_CONTAINS));
            geoValue = GeoTestUtils.geoShapeValue(new org.elasticsearch.geometry.Point(-180, 0));
            assertThat(geoValue.relate(latLonGeometry), Matchers.equalTo(GeoRelation.QUERY_CONTAINS));
            geoValue = GeoTestUtils.geoShapeValue(new org.elasticsearch.geometry.Point(179, 0));
            assertThat(geoValue.relate(latLonGeometry), Matchers.equalTo(GeoRelation.QUERY_CONTAINS));
            geoValue = GeoTestUtils.geoShapeValue(new org.elasticsearch.geometry.Point(-179, 0));
            assertThat(geoValue.relate(latLonGeometry), Matchers.equalTo(GeoRelation.QUERY_CONTAINS));
        }
        // lines
        {
            GeoShapeValues.GeoShapeValue geoValue = GeoTestUtils.geoShapeValue(
                new org.elasticsearch.geometry.Line(new double[] { 0, 0 }, new double[] { -1, 1 })
            );
            assertThat(geoValue.relate(latLonGeometry), Matchers.equalTo(GeoRelation.QUERY_DISJOINT));
            geoValue = GeoTestUtils.geoShapeValue(new org.elasticsearch.geometry.Line(new double[] { 180, 180 }, new double[] { -1, 1 }));
            assertThat(geoValue.relate(latLonGeometry), Matchers.equalTo(GeoRelation.QUERY_CONTAINS));
            geoValue = GeoTestUtils.geoShapeValue(new org.elasticsearch.geometry.Line(new double[] { -180, -180 }, new double[] { -1, 1 }));
            assertThat(geoValue.relate(latLonGeometry), Matchers.equalTo(GeoRelation.QUERY_CONTAINS));
            geoValue = GeoTestUtils.geoShapeValue(new org.elasticsearch.geometry.Line(new double[] { 179, 179 }, new double[] { -1, 1 }));
            assertThat(geoValue.relate(latLonGeometry), Matchers.equalTo(GeoRelation.QUERY_CONTAINS));
            geoValue = GeoTestUtils.geoShapeValue(new org.elasticsearch.geometry.Line(new double[] { -179, -179 }, new double[] { -1, 1 }));
            assertThat(geoValue.relate(latLonGeometry), Matchers.equalTo(GeoRelation.QUERY_CONTAINS));
            geoValue = GeoTestUtils.geoShapeValue(new org.elasticsearch.geometry.Line(new double[] { -179, 179 }, new double[] { -1, 1 }));
            assertThat(geoValue.relate(latLonGeometry), Matchers.equalTo(GeoRelation.QUERY_CROSSES));
        }
        // polygons
        {
            GeoShapeValues.GeoShapeValue geoValue = GeoTestUtils.geoShapeValue(
                new org.elasticsearch.geometry.Polygon(new LinearRing(new double[] { 0, 0, 1, 0 }, new double[] { -1, 1, 1, -1 }))
            );
            assertThat(geoValue.relate(latLonGeometry), Matchers.equalTo(GeoRelation.QUERY_DISJOINT));
            geoValue = GeoTestUtils.geoShapeValue(
                new org.elasticsearch.geometry.Polygon(new LinearRing(new double[] { 180, 180, 179, 180 }, new double[] { -1, 1, 1, -1 }))
            );
            assertThat(geoValue.relate(latLonGeometry), Matchers.equalTo(GeoRelation.QUERY_CONTAINS));
            geoValue = GeoTestUtils.geoShapeValue(
                new org.elasticsearch.geometry.Polygon(
                    new LinearRing(new double[] { -180, -180, -179, -180 }, new double[] { -1, 1, 1, -1 })
                )
            );
            assertThat(geoValue.relate(latLonGeometry), Matchers.equalTo(GeoRelation.QUERY_CONTAINS));
            geoValue = GeoTestUtils.geoShapeValue(
                new org.elasticsearch.geometry.Polygon(new LinearRing(new double[] { 179, 179, 179.5, 179 }, new double[] { -1, 1, 1, -1 }))
            );
            assertThat(geoValue.relate(latLonGeometry), Matchers.equalTo(GeoRelation.QUERY_CONTAINS));
            geoValue = GeoTestUtils.geoShapeValue(
                new org.elasticsearch.geometry.Polygon(
                    new LinearRing(new double[] { -179, -179, -179.5, -179 }, new double[] { -1, 1, 1, -1 })
                )
            );
            assertThat(geoValue.relate(latLonGeometry), Matchers.equalTo(GeoRelation.QUERY_CONTAINS));
            geoValue = GeoTestUtils.geoShapeValue(
                new org.elasticsearch.geometry.Polygon(
                    new LinearRing(new double[] { -179, 179, -178, -179 }, new double[] { -1, 1, 1, -1 })
                )
            );
            assertThat(geoValue.relate(latLonGeometry), Matchers.equalTo(GeoRelation.QUERY_CROSSES));
        }
    }

    public void testRandomBasic() throws IOException {
        for (int res = 0; res < H3.MAX_H3_RES; res++) {
            final long h3 = H3.geoToH3(0, 0, res);
            final GeoShapeValues.GeoShapeValue geoValue = GeoTestUtils.geoShapeValue(H3CartesianUtil.getNormalizeGeometry(h3));
            final long[] children = H3.h3ToChildren(h3);
            assertThat(geoValue.relate(getComponent(children[0])), Matchers.equalTo(GeoRelation.QUERY_INSIDE));
            for (int i = 1; i < children.length; i++) {
                assertThat(geoValue.relate(getComponent(children[i])), Matchers.equalTo(GeoRelation.QUERY_CROSSES));
            }
            for (long noChild : H3.h3ToNoChildrenIntersecting(h3)) {
                assertThat(geoValue.relate(getComponent(noChild)), Matchers.equalTo(GeoRelation.QUERY_CROSSES));
            }
        }
    }

    public void testRandomDateline() throws IOException {
        for (int res = 0; res < H3.MAX_H3_RES; res++) {
            final long h3 = H3.geoToH3(0, 180, res);
            final GeoShapeValues.GeoShapeValue geoValue = GeoTestUtils.geoShapeValue(H3CartesianUtil.getNormalizeGeometry(h3));
            final long[] children = H3.h3ToChildren(h3);
            final Component2D component2D = getComponent(children[0]);
            // this is a current limitation because we break polygons around the dateline.
            final GeoRelation expected = component2D.getMaxX() - component2D.getMinX() == 360d
                ? GeoRelation.QUERY_CROSSES
                : GeoRelation.QUERY_INSIDE;
            assertThat(geoValue.relate(component2D), Matchers.equalTo(expected));
            for (int i = 1; i < children.length; i++) {
                assertThat(geoValue.relate(getComponent(children[i])), Matchers.equalTo(GeoRelation.QUERY_CROSSES));
            }
            for (long noChild : H3.h3ToNoChildrenIntersecting(h3)) {
                assertThat(geoValue.relate(getComponent(noChild)), Matchers.equalTo(GeoRelation.QUERY_CROSSES));
            }
        }
    }

    private static Component2D getComponent(long h3) {
        return LatLonGeometry.create(H3CartesianUtil.getLatLonGeometry(h3));
    }

    public void testBoundingBox() {
        final double[] xs = new double[H3CartesianUtil.MAX_ARRAY_SIZE];
        final double[] ys = new double[H3CartesianUtil.MAX_ARRAY_SIZE];
        for (int res = 0; res <= H3.MAX_H3_RES; res++) {
            final long h3 = H3.geoToH3(GeoTestUtil.nextLatitude(), GeoTestUtil.nextLongitude(), res);
            final Rectangle rectangle = H3CartesianUtil.toBoundingBox(h3);
            final int points = H3CartesianUtil.computePoints(h3, xs, ys);
            for (int i = 0; i < points; i++) {
                final double lat = ys[i];
                final double lon = xs[i];
                assertTrue(rectangle.getMaxY() >= lat && rectangle.getMinY() <= lat);
                if (rectangle.getMinX() > rectangle.getMaxX()) {
                    assertTrue(rectangle.getMaxX() >= lon || rectangle.getMinX() <= lon);
                } else {
                    assertTrue(rectangle.getMaxX() >= lon && rectangle.getMinX() <= lon);
                }
            }
        }
    }

    public void testWronglyNormaliseGeometry() {
        // this bin fails normalising in CCW, so we added hack to normalise it CW. If this test fails
        // it might mean we fixed the bug in the GeometryNormaliser.
        CellBoundary cellBoundary = H3.h3ToGeoBoundary(576531121047601151L);
        double[] xs = new double[cellBoundary.numPoints() + 1];
        double[] ys = new double[cellBoundary.numPoints() + 1];
        for (int i = 0; i < cellBoundary.numPoints(); i++) {
            final LatLng latLng = cellBoundary.getLatLon(i);
            xs[i] = GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(latLng.getLonDeg()));
            ys[i] = GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(latLng.getLatDeg()));
        }
        xs[cellBoundary.numPoints()] = xs[0];
        ys[cellBoundary.numPoints()] = ys[0];
        Polygon polygon = new Polygon(new LinearRing(xs, ys));
        assertThat(GeometryNormalizer.apply(Orientation.CCW, polygon), Matchers.instanceOf(Polygon.class));
        assertThat(GeometryNormalizer.apply(Orientation.CW, polygon), Matchers.instanceOf(MultiPolygon.class));
    }

    public void testNormaliseGeometry() throws IOException {
        Point point = GeometryTestUtils.randomPoint();
        for (int res = 0; res <= H3.MAX_H3_RES; res++) {
            long h3 = H3.geoToH3(point.getLat(), point.getLon(), res);
            assertNotNull(GeoTestUtils.geoShapeValue(H3CartesianUtil.getNormalizeGeometry(h3)));
        }
    }
}
