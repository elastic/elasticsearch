/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.util;

import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geo.ShapeTestUtils;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.test.ESTestCase;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.closeTo;

public class SpatialCoordinateTypesTests extends ESTestCase {

    private static final Map<SpatialCoordinateTypes, TestTypeFunctions> types = new LinkedHashMap<>();
    static {
        types.put(SpatialCoordinateTypes.GEO, new TestTypeFunctions(GeometryTestUtils::randomPoint, v -> 1e-5));
        types.put(
            SpatialCoordinateTypes.CARTESIAN,
            new TestTypeFunctions(ShapeTestUtils::randomPoint, SpatialCoordinateTypesTests::cartesianError)
        );
    }

    private static double cartesianError(double v) {
        double abs = Math.abs(v);
        return (abs < 1) ? 1e-5 : abs / 1e7;
    }

    record TestTypeFunctions(Supplier<Point> randomPoint, Function<Double, Double> error) {}

    public void testEncoding() {
        for (var type : types.entrySet()) {
            for (int i = 0; i < 10; i++) {
                SpatialCoordinateTypes coordType = type.getKey();
                Point original = type.getValue().randomPoint().get();
                var error = type.getValue().error;
                Point point = coordType.longAsPoint(coordType.pointAsLong(original.getX(), original.getY()));
                assertThat(coordType + ": Y[" + i + "]", point.getY(), closeTo(original.getY(), error.apply(original.getY())));
                assertThat(coordType + ": X[" + i + "]", point.getX(), closeTo(original.getX(), error.apply(original.getX())));
            }
        }
    }

    public void testParsing() {
        for (var type : types.entrySet()) {
            for (int i = 0; i < 10; i++) {
                SpatialCoordinateTypes coordType = type.getKey();
                Point point = type.getValue().randomPoint.get();
                assertEquals(coordType.wkbToWkt(coordType.asWkb(point)), coordType.asWkt(point));
            }
        }
    }
}
