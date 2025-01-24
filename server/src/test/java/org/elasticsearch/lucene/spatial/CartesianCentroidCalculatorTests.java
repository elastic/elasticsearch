/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.spatial;

import org.elasticsearch.geo.ShapeTestUtils;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.MultiLine;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;

public class CartesianCentroidCalculatorTests extends CentroidCalculatorTests {
    protected Point randomPoint() {
        return ShapeTestUtils.randomPoint(false);
    }

    protected MultiPoint randomMultiPoint() {
        return ShapeTestUtils.randomMultiPoint(false);
    }

    protected Line randomLine() {
        return ShapeTestUtils.randomLine(false);
    }

    protected MultiLine randomMultiLine() {
        return ShapeTestUtils.randomMultiLine(false);
    }

    protected Polygon randomPolygon() {
        return ShapeTestUtils.randomPolygon(false);
    }

    protected MultiPolygon randomMultiPolygon() {
        return ShapeTestUtils.randomMultiPolygon(false);
    }

    protected Rectangle randomRectangle() {
        return ShapeTestUtils.randomRectangle();
    }

    protected double randomY() {
        return ShapeTestUtils.randomValue();
    }

    protected double randomX() {
        return ShapeTestUtils.randomValue();
    }

    @Override
    protected boolean ignoreAreaErrors() {
        // Tests that calculate polygon areas with very large double values can have very large errors for flat polygons
        // This would not happen in the tightly bounded case of geo-data, but for cartesian test data it happens a lot.
        return true;
    }
}
