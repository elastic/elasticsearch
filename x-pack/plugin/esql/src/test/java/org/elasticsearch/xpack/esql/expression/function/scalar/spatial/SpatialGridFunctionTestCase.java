/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.List;
import java.util.function.BiFunction;

import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_SHAPE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.UNSPECIFIED;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assume.assumeNotNull;

public abstract class SpatialGridFunctionTestCase extends AbstractScalarFunctionTestCase {

    @FunctionalInterface
    protected interface TriFunction<T, U, V, R> {
        R apply(T t, U u, V v);
    }

    private static String getFunctionClassName() {
        Class<?> testClass = getTestClass();
        String testClassName = testClass.getSimpleName();
        return testClassName.replace("Tests", "");
    }

    protected static void addTestCaseSuppliers(
        List<TestCaseSupplier> suppliers,
        DataType[] dataTypes,
        BiFunction<BytesRef, Integer, Long> expectedValue,
        TriFunction<BytesRef, Integer, GeoBoundingBox, Long> expectedValueWithBounds
    ) {
        for (DataType spatialType : dataTypes) {
            TestCaseSupplier.TypedDataSupplier geometrySupplier = testCaseSupplier(spatialType, true);
            for (boolean literalPrecision : List.of(true)) {
                // TODO: add 'false' case once we support non-literal precision
                String testName = spatialType.typeName() + (literalPrecision ? " with literal precision" : " with precision");
                suppliers.add(new TestCaseSupplier(testName, List.of(spatialType, INTEGER), () -> {
                    TestCaseSupplier.TypedData geoTypedData = geometrySupplier.get();
                    BytesRef geometry = (BytesRef) geoTypedData.data();
                    int precision = between(1, 8);
                    TestCaseSupplier.TypedData precisionData = new TestCaseSupplier.TypedData(precision, INTEGER, "precision");
                    String evaluatorName = "FromFieldAndLiteralEvaluator[in=Attribute[channel=0], precision=Attribute[channel=1]";
                    if (literalPrecision) {
                        precisionData = precisionData.forceLiteral();
                        evaluatorName = "FromFieldAndLiteralEvaluator[wkbBlock=Attribute[channel=0], precision=" + precision + "]";
                    }
                    return new TestCaseSupplier.TestCase(
                        List.of(geoTypedData, precisionData),
                        getFunctionClassName() + evaluatorName,
                        LONG,
                        equalTo(expectedValue.apply(geometry, precision))
                    );
                }));
                // Test with bounds
                String boundsTestName = testName + " and bounds";
                suppliers.add(new TestCaseSupplier(boundsTestName, List.of(spatialType, INTEGER, GEO_SHAPE), () -> {
                    TestCaseSupplier.TypedData geoTypedData = geometrySupplier.get();
                    BytesRef geometry = (BytesRef) geoTypedData.data();
                    int precision = between(1, 8);
                    TestCaseSupplier.TypedData precisionData = new TestCaseSupplier.TypedData(precision, INTEGER, "precision");
                    Rectangle bounds = new Rectangle(-180, 180, 90, -90);
                    String evaluatorName = "FromFieldAndLiteralAndLiteralEvaluator[in=Attribute[channel=0], bounds=[";
                    if (literalPrecision) {
                        precisionData = precisionData.forceLiteral();
                        evaluatorName = "FromFieldAndLiteralAndLiteralEvaluator[in=Attribute[channel=0], bounds=[";
                    }
                    TestCaseSupplier.TypedData boundsData;
                    // Create a rectangle as bounds
                    BytesRef boundsBytesRef = GEO.asWkb(bounds);
                    boundsData = new TestCaseSupplier.TypedData(boundsBytesRef, GEO_SHAPE, "bounds").forceLiteral();
                    return new TestCaseSupplier.TestCase(
                        List.of(geoTypedData, precisionData, boundsData),
                        startsWith(getFunctionClassName() + evaluatorName),
                        LONG,
                        equalTo(expectedValueWithBounds.apply(geometry, precision, SpatialGridFunction.asGeoBoundingBox(bounds)))
                    );
                }));
            }
        }
    }

    public static TestCaseSupplier.TypedDataSupplier testCaseSupplier(DataType dataType, boolean pointsOnly) {
        if (pointsOnly) {
            return switch (dataType.esType()) {
                case "geo_point" -> TestCaseSupplier.geoPointCases(() -> false).getFirst();
                case "cartesian_point" -> TestCaseSupplier.cartesianPointCases(() -> false).getFirst();
                default -> throw new IllegalArgumentException("Unsupported datatype for " + functionName() + ": " + dataType);
            };
        } else {
            return switch (dataType.esType()) {
                case "geo_point" -> TestCaseSupplier.geoPointCases(() -> false).getFirst();
                case "geo_shape" -> TestCaseSupplier.geoShapeCases(() -> false).getFirst();
                case "cartesian_point" -> TestCaseSupplier.cartesianPointCases(() -> false).getFirst();
                case "cartesian_shape" -> TestCaseSupplier.cartesianShapeCases(() -> false).getFirst();
                default -> throw new IllegalArgumentException("Unsupported datatype for " + functionName() + ": " + dataType);
            };
        }
    }

    protected Long process(int precision, BiFunction<BytesRef, Integer, Long> expectedValue) {
        Object spatialObj = this.testCase.getDataValues().getFirst();
        assumeNotNull(spatialObj);
        assumeTrue("Expected a BytesRef, but got " + spatialObj.getClass(), spatialObj instanceof BytesRef);
        BytesRef wkb = (BytesRef) spatialObj;
        try (
            EvalOperator.ExpressionEvaluator eval = evaluator(
                build(Source.EMPTY, List.of(new Literal(Source.EMPTY, wkb, GEO_POINT), new Literal(Source.EMPTY, precision, INTEGER)))
            ).get(driverContext());
            Block block = eval.eval(row(List.of(wkb, precision)))
        ) {
            return block.isNull(0) ? null : expectedValue.apply(wkb, precision);
        }
    }

    protected Long process(BytesRef bounds, BiFunction<BytesRef, BytesRef, Long> expectedValue) {
        Object spatialObj = this.testCase.getDataValues().getFirst();
        assumeNotNull(spatialObj);
        assumeTrue("Expected a BytesRef, but got " + spatialObj.getClass(), spatialObj instanceof BytesRef);
        BytesRef wkb = (BytesRef) spatialObj;
        try (
            EvalOperator.ExpressionEvaluator eval = evaluator(
                build(
                    Source.EMPTY,
                    List.of(
                        new Literal(Source.EMPTY, wkb, GEO_POINT),
                        new Literal(Source.EMPTY, 1, INTEGER),
                        new Literal(Source.EMPTY, bounds, GEO_SHAPE)
                    )
                )
            ).get(driverContext());
            Block block = eval.eval(row(List.of(wkb, 1, bounds)))
        ) {
            return block.isNull(0) ? null : expectedValue.apply(wkb, bounds);
        }
    }

    public void testInvalidBounds() {
        BytesRef point = UNSPECIFIED.asWkb(new Point(0, 0));
        BytesRef polygon = UNSPECIFIED.wktToWkb("POLYGON((0.0 30.0, 12.0 30.0, 12.0 60.0, 0.0 60.0, 0.0 30.0))");
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> process(point, (p, b) -> -1L));
        assertThat(ex.getMessage(), containsString("Bounds geometry type 'Point' is not an envelope"));
        ex = expectThrows(IllegalArgumentException.class, () -> process(polygon, (p, b) -> -1L));
        assertThat(ex.getMessage(), containsString("Bounds geometry type 'Polygon' is not an envelope"));
    }
}
