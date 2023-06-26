/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.MultiLine;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.xpack.spatial.util.GeoTestUtils;

import java.io.IOException;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;

public class GeoCentroidCalculatorTests extends CentroidCalculatorTests {
    protected Point randomPoint() {
        return GeometryTestUtils.randomPoint(false);
    }

    protected MultiPoint randomMultiPoint() {
        return GeometryTestUtils.randomMultiPoint(false);
    }

    protected Line randomLine() {
        return GeometryTestUtils.randomLine(false);
    }

    protected MultiLine randomMultiLine() {
        return GeometryTestUtils.randomMultiLine(false);
    }

    protected Polygon randomPolygon() {
        return GeometryTestUtils.randomPolygon(false);
    }

    protected MultiPolygon randomMultiPolygon() {
        return GeometryTestUtils.randomMultiPolygon(false);
    }

    protected Rectangle randomRectangle() {
        return GeometryTestUtils.randomRectangle();
    }

    protected double randomY() {
        return GeometryTestUtils.randomLat();
    }

    protected double randomX() {
        return GeometryTestUtils.randomLon();
    }

    @Override
    protected boolean ignoreAreaErrors() {
        // Tests that calculate polygon areas with very large double values can have very large errors for flat polygons
        // This would not happen in the tightly bounded case of geo-data, but for cartesian test data it happens a lot.
        return false;
    }

    public void testRoundingErrorAndNormalization() throws IOException {
        double lonA = randomX();
        double latA = randomY();
        double lonB = randomValueOtherThanMany((l) -> Math.abs(l - lonA) <= GeoUtils.TOLERANCE, this::randomX);
        double latB = randomValueOtherThanMany((l) -> Math.abs(l - latA) <= GeoUtils.TOLERANCE, this::randomY);
        {
            Line line = new Line(new double[] { 180.0, 180.0 }, new double[] { latA, latB });
            GeoShapeValues.GeoShapeValue value = GeoTestUtils.geoShapeValue(line);
            assertThat(value.getX(), anyOf(equalTo(179.99999991618097), equalTo(-180.0)));
        }

        {
            Line line = new Line(new double[] { -180.0, -180.0 }, new double[] { latA, latB });
            GeoShapeValues.GeoShapeValue value = GeoTestUtils.geoShapeValue(line);
            assertThat(value.getX(), anyOf(equalTo(179.99999991618097), equalTo(-180.0)));
        }

        {
            Line line = new Line(new double[] { lonA, lonB }, new double[] { 90.0, 90.0 });
            GeoShapeValues.GeoShapeValue value = GeoTestUtils.geoShapeValue(line);
            assertThat(value.getY(), equalTo(89.99999995809048));
        }

        {
            Line line = new Line(new double[] { lonA, lonB }, new double[] { -90.0, -90.0 });
            GeoShapeValues.GeoShapeValue value = GeoTestUtils.geoShapeValue(line);
            assertThat(value.getY(), equalTo(-90.0));
        }
    }
}
