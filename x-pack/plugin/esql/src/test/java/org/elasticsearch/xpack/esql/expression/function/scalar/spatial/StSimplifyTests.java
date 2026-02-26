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
import org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matchers;
import org.junit.AssumptionViolatedException;
import org.locationtech.jts.simplify.DouglasPeuckerSimplifier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.core.type.DataType.CARTESIAN_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.CARTESIAN_SHAPE;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_SHAPE;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.CARTESIAN;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.UNSPECIFIED;

@FunctionName("st_simplify")
public class StSimplifyTests extends AbstractScalarFunctionTestCase {
    public StSimplifyTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        var geoPoint = new TestCaseSupplier.TypedDataSupplier(
            "geo point",
            () -> UNSPECIFIED.wktToWkb("POINT(3.141592 -3.141592)"),
            GEO_POINT
        );
        var cartesianPoint = new TestCaseSupplier.TypedDataSupplier(
            "geo point",
            () -> UNSPECIFIED.wktToWkb("POINT(3.141592 500)"),
            CARTESIAN_POINT
        );
        var geoShape = new TestCaseSupplier.TypedDataSupplier(
            "geo shape",
            () -> UNSPECIFIED.wktToWkb("POLYGON ((-73.97 40.78, -73.98 40.75, -73.95 40.74, -73.93 40.76, -73.97 40.78))"),
            GEO_SHAPE
        );
        var cartesianShape = new TestCaseSupplier.TypedDataSupplier(
            "cartesian shape",
            () -> UNSPECIFIED.wktToWkb("POLYGON ((2 3, 4 8, 7 6, 6 2, 2 3))"),
            CARTESIAN_SHAPE
        );

        final List<TestCaseSupplier> suppliers = new ArrayList<>();
        // Random test case suppliers
        addTestCaseSuppliers(suppliers, GEO_POINT, testCaseSupplier(GEO_POINT));
        addTestCaseSuppliers(suppliers, CARTESIAN_POINT, testCaseSupplier(CARTESIAN_POINT));
        addTestCaseSuppliers(suppliers, GEO_SHAPE, testCaseSupplier(GEO_SHAPE));
        addTestCaseSuppliers(suppliers, CARTESIAN_SHAPE, testCaseSupplier(CARTESIAN_SHAPE));
        // Adds hardcoded test cases so we avoid failing if the above none of the test cases were valid for a specific typed data
        addTestCaseSuppliers(suppliers, GEO_POINT, geoPoint);
        addTestCaseSuppliers(suppliers, CARTESIAN_POINT, cartesianPoint);
        addTestCaseSuppliers(suppliers, GEO_SHAPE, geoShape);
        addTestCaseSuppliers(suppliers, CARTESIAN_SHAPE, cartesianShape);

        var testSuppliers = anyNullIsNull(
            randomizeBytesRefsOffset(suppliers),
            (nullPosition, nullValueDataType, original) -> nullValueDataType == DataType.NULL ? DataType.NULL : original.expectedType(),
            (nullPosition, nullData, original) -> nullData.isForceLiteral() ? Matchers.equalTo("LiteralsEvaluator[lit=null]") : original
        );

        return parameterSuppliersFromTypedData(testSuppliers);
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

    protected static void addTestCaseSuppliers(
        List<TestCaseSupplier> suppliers,
        DataType spatialType,
        TestCaseSupplier.TypedDataSupplier geometrySupplier
    ) {
        String testName = spatialType.typeName() + " with tolerance.";
        String evaluatorPrefix = evaluatorPrefix(spatialType);

        suppliers.add(new TestCaseSupplier(testName, List.of(spatialType, DOUBLE), () -> {
            TestCaseSupplier.TypedData geoTypedData = geometrySupplier.get();
            BytesRef geometry = (BytesRef) geoTypedData.data();
            double tolerance = randomDoubleBetween(0, 100, true);
            TestCaseSupplier.TypedData toleranceData = new TestCaseSupplier.TypedData(tolerance, DOUBLE, "tolerance");
            toleranceData = toleranceData.forceLiteral();
            String evaluatorName = evaluatorPrefix + "[geometry=Attribute[channel=0], tolerance=" + tolerance + "]";
            var expectedResult = valueOf(geometry, tolerance, spatialType);

            return new TestCaseSupplier.TestCase(
                List.of(geoTypedData, toleranceData),
                evaluatorName,
                spatialType,
                Matchers.equalTo(expectedResult)
            );
        }));
    }

    private static String evaluatorPrefix(DataType spatialType) {
        return DataType.isSpatialGeo(spatialType)
            ? "StSimplifyNonFoldableGeoShapeAndFoldableToleranceEvaluator"
            : "StSimplifyNonFoldableCartesianShapeAndFoldableToleranceEvaluator";
    }

    private static BytesRef valueOf(BytesRef wkb, double tolerance, DataType spatialType) {
        if (wkb == null) {
            return null;
        }
        try {
            org.locationtech.jts.geom.Geometry jtsGeometry = UNSPECIFIED.wkbToJtsGeometry(wkb);
            SpatialCoordinateTypes coordType = DataType.isSpatialGeo(spatialType) ? GEO : CARTESIAN;
            jtsGeometry.apply((org.locationtech.jts.geom.CoordinateFilter) coord -> {
                long encoded = coordType.pointAsLong(coord.x, coord.y);
                coord.x = coordType.decodeX(encoded);
                coord.y = coordType.decodeY(encoded);
            });
            jtsGeometry.geometryChanged();
            org.locationtech.jts.geom.Geometry simplifiedGeometry = DouglasPeuckerSimplifier.simplify(jtsGeometry, tolerance);
            return UNSPECIFIED.jtsGeometryToWkb(simplifiedGeometry);
        } catch (Exception e) {
            throw new AssumptionViolatedException("Skipping invalid test case");
        }
    }

    @Override
    public void testFold() {
        // The evaluator quantizes coordinates for indexed fields, but fold() handles literals
        // where quantization doesn't apply. Recompute without quantization for the fold test.
        var data = testCase.getData();
        if (testCase.getExpectedTypeError() != null || data.size() < 2) {
            super.testFold();
            return;
        }
        Object geomData = data.get(0).data();
        Object tolData = data.get(1).data();
        if (geomData instanceof BytesRef wkb && tolData instanceof Number tol) {
            BytesRef nonQuantized = valueOfNoQuantize(wkb, tol.doubleValue());
            if (nonQuantized != null) {
                testCase = new TestCaseSupplier.TestCase(
                    data,
                    testCase.evaluatorToString().toString(),
                    testCase.expectedType(),
                    Matchers.equalTo(nonQuantized)
                );
            }
        }
        super.testFold();
    }

    private static BytesRef valueOfNoQuantize(BytesRef wkb, double tolerance) {
        try {
            org.locationtech.jts.geom.Geometry jtsGeometry = UNSPECIFIED.wkbToJtsGeometry(wkb);
            org.locationtech.jts.geom.Geometry simplifiedGeometry = DouglasPeuckerSimplifier.simplify(jtsGeometry, tolerance);
            return UNSPECIFIED.jtsGeometryToWkb(simplifiedGeometry);
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new StSimplify(source, args.get(0), args.get(1));
    }
}
