/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.util;

import org.apache.lucene.geo.XShapeTestUtil;
import org.apache.lucene.geo.XYPolygon;
import org.apache.lucene.geo.XYRectangle;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class ShapeUtilTests extends ESTestCase {
    public void testBox() {
        XYRectangle geom = XShapeTestUtil.nextBox();
        assertThat("Geometry minX should be less than maxX", geom.minX, lessThan(geom.maxX));
        assertThat("Geometry minY should be less than maxY", geom.minY, lessThan(geom.maxY));
    }

    public void testPolygon() {
        XYPolygon geom = XShapeTestUtil.nextPolygon();
        assertThat("Geometry minX should be less than maxX", geom.minX, lessThan(geom.maxX));
        assertThat("Geometry minY should be less than maxY", geom.minY, lessThan(geom.maxY));
        assertThat("Geometry area should be non-zero", ShapeTestUtils.area(geom), greaterThan(0.0));
    }

    public void testFlatRectangle() {
        XYPolygon geom = new XYPolygon(
            new float[] { 54.69f, 54.69f, 180.0f, 180.0f, 54.69f },
            new float[] { -2.80E-33f, 5.85E-33f, 5.85E-33f, -2.80E-33f, -2.80E-33f }
        );
        assertThat("Geometry minX should be less than maxX", geom.minX, lessThan(geom.maxX));
        assertThat("Geometry minY should be less than maxY", geom.minY, lessThan(geom.maxY));
        assertThat(
            "This flat rectangle has area less than allowed threshold",
            ShapeTestUtils.area(geom),
            lessThan(ShapeTestUtils.MIN_VALID_AREA)
        );
    }
}
