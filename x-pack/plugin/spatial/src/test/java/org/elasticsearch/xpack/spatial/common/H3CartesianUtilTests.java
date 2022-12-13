/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.common;

import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.Point;
import org.apache.lucene.tests.geo.GeoTestUtil;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.h3.H3;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoRelation;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues;
import org.elasticsearch.xpack.spatial.util.GeoTestUtils;
import org.hamcrest.Matchers;

import java.io.IOException;

public class H3CartesianUtilTests extends ESTestCase {

    public void testLevel1() {
        for (int i = 0; i < 10000; i++) {
            Point point = GeoTestUtil.nextPoint();
            boolean inside = false;
            for (long h3 : H3.getLongRes0Cells()) {
                if (H3CartesianUtil.getComponent(h3).contains(point.getLon(), point.getLat())) {
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

    public void testLevel2() {
        for (int i = 0; i < 10000; i++) {
            Point point = GeoTestUtil.nextPoint();
            boolean inside = false;
            for (long res0Cell : H3.getLongRes0Cells()) {
                for (long h3 : H3.h3ToChildren(res0Cell)) {
                    if (H3CartesianUtil.getComponent(h3).contains(point.getLon(), point.getLat())) {
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

    public void testNorthPole() {
        for (int res = 0; res <= H3.MAX_H3_RES; res++) {
            final long h3 = H3.geoToH3(90, 0, res);
            final Component2D component2D = H3CartesianUtil.getComponent(h3);
            final double lon = GeoTestUtil.nextLongitude();
            assertThat(component2D.contains(lon, 90), Matchers.equalTo(true));
            final double bound = H3CartesianUtil.getNorthPolarBound(res);
            final double lat = randomValueOtherThanMany(l -> l > bound, GeoTestUtil::nextLatitude);
            assertThat(component2D.contains(lon, lat), Matchers.equalTo(false));
        }
    }

    public void testSouthPole() {
        for (int res = 0; res <= H3.MAX_H3_RES; res++) {
            final long h3 = H3.geoToH3(-90, 0, res);
            final Component2D component2D = H3CartesianUtil.getComponent(h3);
            final double lon = GeoTestUtil.nextLongitude();
            assertThat(component2D.contains(lon, -90), Matchers.equalTo(true));
            final double bound = H3CartesianUtil.getSouthPolarBound(res);
            final double lat = randomValueOtherThanMany(l -> l < bound, GeoTestUtil::nextLatitude);
            assertThat(component2D.contains(lon, lat), Matchers.equalTo(false));
        }
    }

    public void testDateline() throws IOException {
        final long h3 = H3.geoToH3(0, 180, 0);
        final Component2D component2D = H3CartesianUtil.getComponent(h3);
        // points
        {
            GeoShapeValues.GeoShapeValue geoValue = GeoTestUtils.geoShapeValue(new org.elasticsearch.geometry.Point(0, 0));
            assertThat(geoValue.relate(component2D), Matchers.equalTo(GeoRelation.QUERY_DISJOINT));
            geoValue = GeoTestUtils.geoShapeValue(new org.elasticsearch.geometry.Point(180, 0));
            assertThat(geoValue.relate(component2D), Matchers.equalTo(GeoRelation.QUERY_CONTAINS));
            geoValue = GeoTestUtils.geoShapeValue(new org.elasticsearch.geometry.Point(-180, 0));
            assertThat(geoValue.relate(component2D), Matchers.equalTo(GeoRelation.QUERY_CONTAINS));
            geoValue = GeoTestUtils.geoShapeValue(new org.elasticsearch.geometry.Point(179, 0));
            assertThat(geoValue.relate(component2D), Matchers.equalTo(GeoRelation.QUERY_CONTAINS));
            geoValue = GeoTestUtils.geoShapeValue(new org.elasticsearch.geometry.Point(-179, 0));
            assertThat(geoValue.relate(component2D), Matchers.equalTo(GeoRelation.QUERY_CONTAINS));
        }
        // lines
        {
            GeoShapeValues.GeoShapeValue geoValue = GeoTestUtils.geoShapeValue(
                new org.elasticsearch.geometry.Line(new double[] { 0, 0 }, new double[] { -1, 1 })
            );
            assertThat(geoValue.relate(component2D), Matchers.equalTo(GeoRelation.QUERY_DISJOINT));
            geoValue = GeoTestUtils.geoShapeValue(new org.elasticsearch.geometry.Line(new double[] { 180, 180 }, new double[] { -1, 1 }));
            assertThat(geoValue.relate(component2D), Matchers.equalTo(GeoRelation.QUERY_CONTAINS));
            geoValue = GeoTestUtils.geoShapeValue(new org.elasticsearch.geometry.Line(new double[] { -180, -180 }, new double[] { -1, 1 }));
            assertThat(geoValue.relate(component2D), Matchers.equalTo(GeoRelation.QUERY_CONTAINS));
            geoValue = GeoTestUtils.geoShapeValue(new org.elasticsearch.geometry.Line(new double[] { 179, 179 }, new double[] { -1, 1 }));
            assertThat(geoValue.relate(component2D), Matchers.equalTo(GeoRelation.QUERY_CONTAINS));
            geoValue = GeoTestUtils.geoShapeValue(new org.elasticsearch.geometry.Line(new double[] { -179, -179 }, new double[] { -1, 1 }));
            assertThat(geoValue.relate(component2D), Matchers.equalTo(GeoRelation.QUERY_CONTAINS));
            geoValue = GeoTestUtils.geoShapeValue(new org.elasticsearch.geometry.Line(new double[] { -179, 179 }, new double[] { -1, 1 }));
            assertThat(geoValue.relate(component2D), Matchers.equalTo(GeoRelation.QUERY_CROSSES));
        }
        // polygons
        {
            GeoShapeValues.GeoShapeValue geoValue = GeoTestUtils.geoShapeValue(
                new org.elasticsearch.geometry.Polygon(new LinearRing(new double[] { 0, 0, 1, 0 }, new double[] { -1, 1, 1, -1 }))
            );
            assertThat(geoValue.relate(component2D), Matchers.equalTo(GeoRelation.QUERY_DISJOINT));
            geoValue = GeoTestUtils.geoShapeValue(
                new org.elasticsearch.geometry.Polygon(new LinearRing(new double[] { 180, 180, 179, 180 }, new double[] { -1, 1, 1, -1 }))
            );
            assertThat(geoValue.relate(component2D), Matchers.equalTo(GeoRelation.QUERY_CONTAINS));
            geoValue = GeoTestUtils.geoShapeValue(
                new org.elasticsearch.geometry.Polygon(
                    new LinearRing(new double[] { -180, -180, -179, -180 }, new double[] { -1, 1, 1, -1 })
                )
            );
            assertThat(geoValue.relate(component2D), Matchers.equalTo(GeoRelation.QUERY_CONTAINS));
            geoValue = GeoTestUtils.geoShapeValue(
                new org.elasticsearch.geometry.Polygon(new LinearRing(new double[] { 179, 179, 179.5, 179 }, new double[] { -1, 1, 1, -1 }))
            );
            assertThat(geoValue.relate(component2D), Matchers.equalTo(GeoRelation.QUERY_CONTAINS));
            geoValue = GeoTestUtils.geoShapeValue(
                new org.elasticsearch.geometry.Polygon(
                    new LinearRing(new double[] { -179, -179, -179.5, -179 }, new double[] { -1, 1, 1, -1 })
                )
            );
            assertThat(geoValue.relate(component2D), Matchers.equalTo(GeoRelation.QUERY_CONTAINS));
            geoValue = GeoTestUtils.geoShapeValue(
                new org.elasticsearch.geometry.Polygon(
                    new LinearRing(new double[] { -179, 179, -178, -179 }, new double[] { -1, 1, 1, -1 })
                )
            );
            assertThat(geoValue.relate(component2D), Matchers.equalTo(GeoRelation.QUERY_CROSSES));
        }
    }

    public void testRandomBasic() throws IOException {
        for (int res = 0; res < H3.MAX_H3_RES; res++) {
            final long h3 = H3.geoToH3(0, 0, res);
            final GeoShapeValues.GeoShapeValue geoValue = GeoTestUtils.geoShapeValue(H3CartesianUtil.getGeometry(h3));
            final long[] children = H3.h3ToChildren(h3);
            assertThat(geoValue.relate(H3CartesianUtil.getComponent(children[0])), Matchers.equalTo(GeoRelation.QUERY_INSIDE));
            for (int i = 1; i < children.length; i++) {
                assertThat(geoValue.relate(H3CartesianUtil.getComponent(children[i])), Matchers.equalTo(GeoRelation.QUERY_CROSSES));
            }
            for (long noChild : H3.h3ToNoChildrenIntersecting(h3)) {
                assertThat(geoValue.relate(H3CartesianUtil.getComponent(noChild)), Matchers.equalTo(GeoRelation.QUERY_CROSSES));
            }
        }
    }

    public void testRandomDateline() throws IOException {
        for (int res = 0; res < H3.MAX_H3_RES; res++) {
            final long h3 = H3.geoToH3(0, 180, res);
            final GeoShapeValues.GeoShapeValue geoValue = GeoTestUtils.geoShapeValue(H3CartesianUtil.getGeometry(h3));
            final long[] children = H3.h3ToChildren(h3);
            final Component2D component2D = H3CartesianUtil.getComponent(children[0]);
            // this is a current limitation because we break polygons around the dateline.
            final GeoRelation expected = component2D.getMaxX() - component2D.getMinX() == 360d
                ? GeoRelation.QUERY_CROSSES
                : GeoRelation.QUERY_INSIDE;
            assertThat(geoValue.relate(component2D), Matchers.equalTo(expected));
            for (int i = 1; i < children.length; i++) {
                assertThat(geoValue.relate(H3CartesianUtil.getComponent(children[i])), Matchers.equalTo(GeoRelation.QUERY_CROSSES));
            }
            for (long noChild : H3.h3ToNoChildrenIntersecting(h3)) {
                assertThat(geoValue.relate(H3CartesianUtil.getComponent(noChild)), Matchers.equalTo(GeoRelation.QUERY_CROSSES));
            }
        }
    }
}
