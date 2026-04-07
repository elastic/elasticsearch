/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matchers;
import org.junit.AssumptionViolatedException;
import org.locationtech.jts.geom.Geometry;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.core.type.DataType.CARTESIAN_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.CARTESIAN_SHAPE;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_SHAPE;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.UNSPECIFIED;

/**
 * Base test case for spatial functions that transform a geometry with a double parameter
 * (e.g. ST_SIMPLIFY, ST_SIMPLIFYPRESERVETOPOLOGY, ST_BUFFER).
 */
public abstract class AbstractSpatialGeometryTransformTestCase extends AbstractScalarFunctionTestCase {

    protected AbstractSpatialGeometryTransformTestCase(Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    /** The JTS operation to apply for computing expected results. */
    protected abstract BiFunction<Geometry, Double, Geometry> jtsOperation();

    /** The evaluator class name prefix, e.g. "StSimplify" or "StBuffer". */
    protected abstract String evaluatorPrefix();

    /** The name of the second parameter in the evaluator, e.g. "tolerance" or "distance". */
    protected abstract String secondParameterName();

    /** The expected return type for the given input spatial type. */
    protected DataType expectedReturnType(DataType spatialType) {
        return spatialType;
    }

    /** Minimum value for the randomly generated second parameter. */
    protected double randomParameterMin() {
        return 0;
    }

    /** Maximum value for the randomly generated second parameter. */
    protected double randomParameterMax() {
        return 100;
    }

    public static TestCaseSupplier.TypedDataSupplier testCaseSupplier(DataType dataType) {
        return switch (dataType) {
            case GEO_POINT -> TestCaseSupplier.geoPointCases(() -> false).getFirst();
            case GEO_SHAPE -> TestCaseSupplier.geoShapeCases(() -> false).getFirst();
            case CARTESIAN_POINT -> TestCaseSupplier.cartesianPointCases(() -> false).getFirst();
            case CARTESIAN_SHAPE -> TestCaseSupplier.cartesianShapeCases(() -> false).getFirst();
            default -> throw new IllegalArgumentException("Unsupported datatype: " + dataType);
        };
    }

    protected static List<TestCaseSupplier.TypedDataSupplier> hardcodedSuppliers() {
        return List.of(
            new TestCaseSupplier.TypedDataSupplier("geo point", () -> UNSPECIFIED.wktToWkb("POINT(3.141592 -3.141592)"), GEO_POINT),
            new TestCaseSupplier.TypedDataSupplier("cartesian point", () -> UNSPECIFIED.wktToWkb("POINT(3.141592 500)"), CARTESIAN_POINT),
            new TestCaseSupplier.TypedDataSupplier(
                "geo shape",
                () -> UNSPECIFIED.wktToWkb("POLYGON ((-73.97 40.78, -73.98 40.75, -73.95 40.74, -73.93 40.76, -73.97 40.78))"),
                GEO_SHAPE
            ),
            new TestCaseSupplier.TypedDataSupplier(
                "cartesian shape",
                () -> UNSPECIFIED.wktToWkb("POLYGON ((2 3, 4 8, 7 6, 6 2, 2 3))"),
                CARTESIAN_SHAPE
            )
        );
    }

    protected static Iterable<Object[]> buildParameters(
        String evaluatorPrefix,
        String paramName,
        BiFunction<Geometry, Double, Geometry> jtsOp,
        BiFunction<DataType, DataType, DataType> expectedTypeFunction,
        double paramMin,
        double paramMax
    ) {
        final List<TestCaseSupplier> suppliers = new ArrayList<>();
        for (DataType spatialType : new DataType[] { GEO_POINT, CARTESIAN_POINT, GEO_SHAPE, CARTESIAN_SHAPE }) {
            addTestCaseSuppliers(
                suppliers,
                spatialType,
                testCaseSupplier(spatialType),
                evaluatorPrefix,
                paramName,
                jtsOp,
                expectedTypeFunction,
                paramMin,
                paramMax
            );
        }
        var hardcoded = hardcodedSuppliers();
        DataType[] types = { GEO_POINT, CARTESIAN_POINT, GEO_SHAPE, CARTESIAN_SHAPE };
        for (int i = 0; i < types.length; i++) {
            addTestCaseSuppliers(
                suppliers,
                types[i],
                hardcoded.get(i),
                evaluatorPrefix,
                paramName,
                jtsOp,
                expectedTypeFunction,
                paramMin,
                paramMax
            );
        }

        var testSuppliers = anyNullIsNull(
            randomizeBytesRefsOffset(suppliers),
            (nullPosition, nullValueDataType, original) -> nullValueDataType == DataType.NULL ? DataType.NULL : original.expectedType(),
            (nullPosition, nullData, original) -> nullData.isForceLiteral() ? Matchers.equalTo("LiteralsEvaluator[lit=null]") : original
        );

        return parameterSuppliersFromTypedData(testSuppliers);
    }

    private static void addTestCaseSuppliers(
        List<TestCaseSupplier> suppliers,
        DataType spatialType,
        TestCaseSupplier.TypedDataSupplier geometrySupplier,
        String evaluatorPrefix,
        String paramName,
        BiFunction<Geometry, Double, Geometry> jtsOp,
        BiFunction<DataType, DataType, DataType> expectedTypeFunction,
        double paramMin,
        double paramMax
    ) {
        for (DataType paramType : new DataType[] { DataType.DOUBLE, DataType.FLOAT, DataType.LONG, DataType.INTEGER }) {
            String testName = spatialType.typeName() + " with " + paramName + "[" + paramType.typeName() + "].";
            suppliers.add(new TestCaseSupplier(testName, List.of(spatialType, paramType), () -> {
                TestCaseSupplier.TypedData geoTypedData = geometrySupplier.get();
                BytesRef geometry = (BytesRef) geoTypedData.data();
                double paramValue = paramType.isWholeNumber()
                    ? randomDoubleBetween(paramMin, paramMax, true)
                    : randomIntBetween((int) paramMin, (int) paramMax);
                TestCaseSupplier.TypedData paramData = new TestCaseSupplier.TypedData(paramValue, paramType, paramName).forceLiteral();
                String evaluatorName = evaluatorPrefix
                    + "NonFoldableGeometryAndFoldable"
                    + capitalize(paramName)
                    + "Evaluator[geometry=Attribute[channel=0], "
                    + paramName
                    + "="
                    + paramValue
                    + "]";
                DataType expectedType = expectedTypeFunction.apply(spatialType, paramType);
                var expectedResult = valueOf(geometry, paramValue, jtsOp);

                return new TestCaseSupplier.TestCase(
                    List.of(geoTypedData, paramData),
                    evaluatorName,
                    expectedType,
                    Matchers.equalTo(expectedResult)
                );
            }));
        }
    }

    private static BytesRef valueOf(BytesRef wkb, double parameter, BiFunction<Geometry, Double, Geometry> jtsOp) {
        if (wkb == null) {
            return null;
        }
        try {
            Geometry jtsGeometry = UNSPECIFIED.wkbToJtsGeometry(wkb);
            Geometry result = jtsOp.apply(jtsGeometry, parameter);
            return UNSPECIFIED.jtsGeometryToWkb(result);
        } catch (Exception e) {
            throw new AssumptionViolatedException("Skipping invalid test case");
        }
    }

    private static String capitalize(String s) {
        return s.substring(0, 1).toUpperCase(java.util.Locale.ROOT) + s.substring(1);
    }
}
