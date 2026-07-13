/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.util;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geo.ShapeTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.utils.GeometryValidator;
import org.elasticsearch.geometry.utils.WellKnownBinary;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.test.ESTestCase;
import org.locationtech.jts.operation.union.UnaryUnionOp;

import java.nio.ByteOrder;
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

    public void testWkbToJtsGeometryCollection() throws Exception {
        // id=0 data from multivalue_geometries.csv
        String shapeWkt = "GEOMETRYCOLLECTION (POLYGON ((-10 -10, 0 -10, 0 0, -10 0, -10 -10)), POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0)))";
        String smallerWkt = "GEOMETRYCOLLECTION (POLYGON ((-9 -9, -1 -9, -1 -1, -9 -1, -9 -9)), POLYGON ((1 1, 9 1, 9 9, 1 9, 1 1)))";
        Geometry shapeEsGeom = WellKnownText.fromWKT(GeometryValidator.NOOP, false, shapeWkt);
        Geometry smallerEsGeom = WellKnownText.fromWKT(GeometryValidator.NOOP, false, smallerWkt);
        byte[] shapeWkb = WellKnownBinary.toWKB(shapeEsGeom, ByteOrder.LITTLE_ENDIAN);
        byte[] smallerWkb = WellKnownBinary.toWKB(smallerEsGeom, ByteOrder.LITTLE_ENDIAN);
        BytesRef shapeRef = new BytesRef(shapeWkb);
        BytesRef smallerRef = new BytesRef(smallerWkb);

        String intermediateWkt = SpatialCoordinateTypes.UNSPECIFIED.wkbToWkt(shapeRef);
        logger.info("Intermediate WKT for GEOMETRYCOLLECTION shape: {}", intermediateWkt);

        org.locationtech.jts.geom.Geometry shapeJts = SpatialCoordinateTypes.UNSPECIFIED.wkbToJtsGeometry(shapeRef);
        assertNotNull("wkbToJtsGeometry should not return null for GEOMETRYCOLLECTION", shapeJts);
        assertEquals(2, shapeJts.getNumGeometries());

        org.locationtech.jts.geom.Geometry smallerJts = SpatialCoordinateTypes.UNSPECIFIED.wkbToJtsGeometry(smallerRef);
        assertNotNull("wkbToJtsGeometry should not return null for smaller GEOMETRYCOLLECTION", smallerJts);

        // JTS binary overlay operations reject heterogeneous GeometryCollection; pre-flatten with UnaryUnionOp
        org.locationtech.jts.geom.Geometry shapeFlat = UnaryUnionOp.union(shapeJts);
        org.locationtech.jts.geom.Geometry smallerFlat = UnaryUnionOp.union(smallerJts);
        logger.info("Flattened shape type: {}", shapeFlat.getClass().getSimpleName());
        org.locationtech.jts.geom.Geometry unionResult = shapeFlat.union(smallerFlat);
        logger.info("Union result type: {}, WKT: {}", unionResult.getClass().getSimpleName(), unionResult.toText());

        BytesRef resultWkb = SpatialCoordinateTypes.UNSPECIFIED.jtsGeometryToWkb(unionResult);
        assertNotNull("jtsGeometryToWkb should not return null for union result", resultWkb);
        String resultWkt = SpatialCoordinateTypes.UNSPECIFIED.wkbToWkt(resultWkb);
        logger.info("Round-trip WKT: {}", resultWkt);
    }
}
