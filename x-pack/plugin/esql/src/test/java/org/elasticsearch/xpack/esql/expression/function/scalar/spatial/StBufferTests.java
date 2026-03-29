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
import org.locationtech.jts.operation.buffer.BufferOp;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.core.type.DataType.CARTESIAN_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.CARTESIAN_SHAPE;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_SHAPE;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.UNSPECIFIED;

@FunctionName("st_buffer")
public class StBufferTests extends AbstractScalarFunctionTestCase {
    public StBufferTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
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
            "cartesian point",
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

    protected static void addTestCaseSuppliers(
        List<TestCaseSupplier> suppliers,
        DataType spatialType,
        TestCaseSupplier.TypedDataSupplier geometrySupplier
    ) {
        DataType expectedType = DataType.isSpatialGeo(spatialType) ? GEO_SHAPE : CARTESIAN_SHAPE;
        for (DataType distanceType : new DataType[] { DataType.DOUBLE, DataType.FLOAT, DataType.LONG, DataType.INTEGER }) {
            String testName = spatialType.typeName() + " with distance[" + distanceType.typeName() + "].";
            suppliers.add(new TestCaseSupplier(testName, List.of(spatialType, distanceType), () -> {
                TestCaseSupplier.TypedData geoTypedData = geometrySupplier.get();
                BytesRef geometry = (BytesRef) geoTypedData.data();
                double distance = distanceType.isWholeNumber() ? randomDoubleBetween(0.1, 10, true) : randomIntBetween(1, 10);
                TestCaseSupplier.TypedData distanceData = new TestCaseSupplier.TypedData(distance, distanceType, "distance");
                distanceData = distanceData.forceLiteral();
                String evaluatorName = "StBufferNonFoldableGeometryAndFoldableDistanceEvaluator[geometry=Attribute[channel=0], distance="
                    + distance
                    + "]";
                var expectedResult = valueOf(geometry, distance);

                return new TestCaseSupplier.TestCase(
                    List.of(geoTypedData, distanceData),
                    evaluatorName,
                    expectedType,
                    Matchers.equalTo(expectedResult)
                );
            }));
        }
    }

    private static BytesRef valueOf(BytesRef wkb, double distance) {
        if (wkb == null) {
            return null;
        }
        try {
            org.locationtech.jts.geom.Geometry jtsGeometry = UNSPECIFIED.wkbToJtsGeometry(wkb);
            org.locationtech.jts.geom.Geometry bufferedGeometry = BufferOp.bufferOp(jtsGeometry, distance);
            return UNSPECIFIED.jtsGeometryToWkb(bufferedGeometry);
        } catch (Exception e) {
            throw new AssumptionViolatedException("Skipping invalid test case");
        }
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new StBuffer(source, args.get(0), args.get(1));
    }
}
