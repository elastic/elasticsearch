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

import java.util.List;
import java.util.function.BiFunction;

import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.isSpatialGeo;
import static org.hamcrest.Matchers.equalTo;

public abstract class SpatialGridFunctionTestCase extends AbstractScalarFunctionTestCase {

    private static String getFunctionClassName() {
        Class<?> testClass = getTestClass();
        String testClassName = testClass.getSimpleName();
        return testClassName.replace("Tests", "");
    }

    protected static void addTestCaseSuppliers(
        List<TestCaseSupplier> suppliers,
        DataType[] dataTypes,
        BiFunction<BytesRef, Integer, Long> expectedValue
    ) {
        for (DataType spatialType : dataTypes) {
            DataType shapeType = isSpatialGeo(spatialType) ? DataType.GEO_SHAPE : DataType.CARTESIAN_SHAPE;
            TestCaseSupplier.TypedDataSupplier geometrySupplier = testCaseSupplier(spatialType, true);
            suppliers.add(new TestCaseSupplier("with precision", List.of(spatialType, DataType.INTEGER), () -> {
                TestCaseSupplier.TypedData geoTypedData = geometrySupplier.get();
                BytesRef geometry = (BytesRef) geoTypedData.data();
                int precision = between(1, 8);
                TestCaseSupplier.TypedData precisionData = new TestCaseSupplier.TypedData(precision, DataType.INTEGER, "precision");
                return new TestCaseSupplier.TestCase(
                    List.of(geoTypedData, precisionData),
                    getFunctionClassName() + "FromFieldAndFieldEvaluator[in=Attribute[channel=0], precision=Attribute[channel=1]]",
                    LONG,
                    equalTo(expectedValue.apply(geometry, precision))
                );
            }));
            /*
            TODO: Implement non-foldable bounds
            suppliers.add(new TestCaseSupplier("with precision and bounds", List.of(spatialType, DataType.INTEGER, shapeType), () -> {
                TestCaseSupplier.TypedData geoTypedData = geometrySupplier.get();
                BytesRef geometry = (BytesRef) geoTypedData.data();
                int precision = between(1, 8);
                TestCaseSupplier.TypedData precisionData = new TestCaseSupplier.TypedData(precision, DataType.INTEGER, "precision");
                Rectangle bounds = new Rectangle(-30, 30, 30, -30);
                BytesRef boundsBytesRef = GEO.asWkb(bounds);
                TestCaseSupplier.TypedData boundsData = new TestCaseSupplier.TypedData(boundsBytesRef, shapeType, "bounds");
                return new TestCaseSupplier.TestCase(
                    List.of(geoTypedData, precisionData, boundsData),
                    getFunctionClassName()
                        + "FromFieldAndFieldAndLiteralEvaluator["
                        + "in=Attribute[channel=0], precision=Attribute[channel=1], bounds=Attribute[channel=2]]",
                    LONG,
                    equalTo(expectedValue.apply(geometry, precision))
                );
            }));
             */
        }
    }

    public static TestCaseSupplier.TypedDataSupplier testCaseSupplier(DataType dataType, boolean pointsOnly) {
        if (pointsOnly) {
            return switch (dataType.esType()) {
                case "geo_point" -> TestCaseSupplier.geoPointCases(() -> false).get(0);
                case "cartesian_point" -> TestCaseSupplier.cartesianPointCases(() -> false).get(0);
                default -> throw new IllegalArgumentException("Unsupported datatype for " + functionName() + ": " + dataType);
            };
        } else {
            return switch (dataType.esType()) {
                case "geo_point" -> TestCaseSupplier.geoPointCases(() -> false).get(0);
                case "geo_shape" -> TestCaseSupplier.geoShapeCases(() -> false).get(0);
                case "cartesian_point" -> TestCaseSupplier.cartesianPointCases(() -> false).get(0);
                case "cartesian_shape" -> TestCaseSupplier.cartesianShapeCases(() -> false).get(0);
                default -> throw new IllegalArgumentException("Unsupported datatype for " + functionName() + ": " + dataType);
            };
        }
    }
}
