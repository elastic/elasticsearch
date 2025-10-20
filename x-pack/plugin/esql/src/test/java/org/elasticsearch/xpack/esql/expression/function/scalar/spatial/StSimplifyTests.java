/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matchers;
import org.junit.AssumptionViolatedException;
import org.locationtech.jts.simplify.DouglasPeuckerSimplifier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.core.type.DataType.CARTESIAN_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.CARTESIAN_SHAPE;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_SHAPE;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.UNSPECIFIED;

@FunctionName("st_simplify")
public class StSimplifyTests extends AbstractScalarFunctionTestCase {
    public StSimplifyTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        final List<TestCaseSupplier> suppliers = new ArrayList<>();
        addTestCaseSuppliers(suppliers, new DataType[] { GEO_POINT }, GEO_SHAPE, StSimplifyTests::valueOf);
        addTestCaseSuppliers(suppliers, new DataType[] { CARTESIAN_POINT }, GEO_SHAPE, StSimplifyTests::valueOf);
        addTestCaseSuppliers(suppliers, new DataType[] { CARTESIAN_SHAPE }, GEO_SHAPE, StSimplifyTests::valueOf);
        addTestCaseSuppliers(suppliers, new DataType[] { GEO_SHAPE }, GEO_SHAPE, StSimplifyTests::valueOf);
        return parameterSuppliersFromTypedDataWithDefaultChecksNoErrors(true, suppliers);
    }

    public static TestCaseSupplier.TypedDataSupplier testCaseSupplier(DataType dataType) {
        return switch (dataType) {
            case GEO_POINT -> TestCaseSupplier.geoPointCases(() -> false).getFirst();
            case GEO_SHAPE -> TestCaseSupplier.geoShapeCases(() -> false).getFirst();
            case CARTESIAN_POINT -> TestCaseSupplier.cartesianPointCases(() -> false).getFirst();
            case CARTESIAN_SHAPE -> TestCaseSupplier.cartesianShapeCases(() -> false).getFirst();
            default -> throw new IllegalArgumentException("Unsupported datatype for " + functionName() + ": " + dataType);
        };
    }

    private static String getFunctionClassName() {
        Class<?> testClass = getTestClass();
        String testClassName = testClass.getSimpleName();
        return testClassName.replace("Tests", "");
    }

    protected static void addTestCaseSuppliers(
        List<TestCaseSupplier> suppliers,
        DataType[] dataTypes,
        DataType gridType,
        BiFunction<BytesRef, Double, BytesRef> expectedValue
    ) {
        for (DataType spatialType : dataTypes) {
            TestCaseSupplier.TypedDataSupplier geometrySupplier = testCaseSupplier(spatialType);
            String testName = spatialType.typeName() + " with tolerance.";

            suppliers.add(new TestCaseSupplier(testName, List.of(spatialType, DOUBLE), () -> {
                TestCaseSupplier.TypedData geoTypedData = geometrySupplier.get();
                BytesRef geometry = (BytesRef) geoTypedData.data();
                double tolerance = randomDoubleBetween(0, 100, true);
                TestCaseSupplier.TypedData toleranceData = new TestCaseSupplier.TypedData(tolerance, DOUBLE, "tolerance");
                toleranceData = toleranceData.forceLiteral();
                String evaluatorName = "NonFoldableGeoAndConstantToleranceEvaluator[inputGeometry=Attribute[channel=0], inputTolerance="
                    + tolerance
                    + "]";
                var expectedResult = expectedValue.apply(geometry, tolerance);

                return new TestCaseSupplier.TestCase(
                    List.of(geoTypedData, toleranceData),
                    getFunctionClassName() + evaluatorName,
                    gridType,
                    Matchers.equalTo(expectedResult)
                );
            }));
        }
    }

    private static BytesRef valueOf(BytesRef wkb, double tolerance) {
        if (wkb == null) {
            return null;
        }
        try {
            org.locationtech.jts.geom.Geometry jtsGeometry = UNSPECIFIED.wkbToJtsGeometry(wkb);
            org.locationtech.jts.geom.Geometry simplifiedGeometry = DouglasPeuckerSimplifier.simplify(jtsGeometry, tolerance);
            return UNSPECIFIED.jtsGeometryToWkb(simplifiedGeometry);
        } catch (Exception e) {
            throw new AssumptionViolatedException("Skipping invalid test case");
        }
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new StSimplify(source, args.get(0), args.get(1));
    }
}
