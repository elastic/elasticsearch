/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.spatial;

import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.MultiLine;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;

/**
 * The GeoCentroidCalculatorTests were moved to server, as part of the work to move the doc-values components needed by ESQL to server,
 * this test class was split in two, most moving to server, but one test remaining in xpack.spatial because it depends on GeoShapeValues.
 * See GeoCentroidCalculatorExtraTests.java for that.
 */
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
}
