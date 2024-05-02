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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.spatial.util.GeoTestUtils;

import java.io.IOException;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;

/**
 * When the GeoCentroidCalculatorTests were moved to server, as part of the work to move the doc-values components needed by ESQL to server,
 * this test class was split in two, most moving to server, but one test remaining in xpack.spatial because it depends on GeoShapeValues.
 */
public class GeoCentroidCalculatorExtraTests extends ESTestCase {
    protected double randomY() {
        return GeometryTestUtils.randomLat();
    }

    protected double randomX() {
        return GeometryTestUtils.randomLon();
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
