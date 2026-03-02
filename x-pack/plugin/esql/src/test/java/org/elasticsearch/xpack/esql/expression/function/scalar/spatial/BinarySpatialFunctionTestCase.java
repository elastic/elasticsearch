/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.xpack.esql.core.type.DataType.GEOHASH;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEOHEX;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEOTILE;
import static org.elasticsearch.xpack.esql.core.type.DataType.isSpatialGeo;
import static org.elasticsearch.xpack.esql.core.type.DataType.isSpatialOrGrid;
import static org.elasticsearch.xpack.esql.core.type.DataType.isString;
import static org.hamcrest.Matchers.equalTo;

public abstract class BinarySpatialFunctionTestCase extends AbstractScalarFunctionTestCase {

    private static String getFunctionClassName() {
        Class<?> testClass = getTestClass();
        String testClassName = testClass.getSimpleName();
        return testClassName.replace("Tests", "");
    }

    protected static Class<?> getSpatialRelatesFunctionClass() throws ClassNotFoundException {
        String functionClassName = getFunctionClassName();
        return Class.forName("org.elasticsearch.xpack.esql.expression.function.scalar.spatial." + functionClassName);
    }

    public static TestCaseSupplier.TypedDataSupplier testCaseSupplier(DataType dataType, boolean pointsOnly) {
        if (pointsOnly) {
            return switch (dataType) {
                case GEO_POINT -> TestCaseSupplier.geoPointCases(() -> false).get(0);
                case CARTESIAN_POINT -> TestCaseSupplier.cartesianPointCases(() -> false).get(0);
                default -> throw new IllegalArgumentException("Unsupported datatype for " + functionName() + ": " + dataType);
            };
        } else {
            return switch (dataType) {
                case GEO_POINT -> TestCaseSupplier.geoPointCases(() -> false).get(0);
                case GEO_SHAPE -> TestCaseSupplier.geoShapeCases(() -> false).get(0);
                case CARTESIAN_POINT -> TestCaseSupplier.cartesianPointCases(() -> false).get(0);
                case CARTESIAN_SHAPE -> TestCaseSupplier.cartesianShapeCases(() -> false).get(0);
                case GEOHASH -> TestCaseSupplier.geoGridCases(GEOHASH, () -> false).get(0);
                case GEOTILE -> TestCaseSupplier.geoGridCases(GEOTILE, () -> false).get(0);
                case GEOHEX -> TestCaseSupplier.geoGridCases(GEOHEX, () -> false).get(0);
                default -> throw new IllegalArgumentException("Unsupported datatype for " + functionName() + ": " + dataType);
            };
        }
    }

    /**
     * Binary spatial functions that take two spatial arguments
     * should use this to generate combinations of test cases.
     */
    protected static void addSpatialCombinations(
        List<TestCaseSupplier> suppliers,
        DataType[] dataTypes,
        DataType returnType,
        boolean pointsOnly
    ) {
        for (DataType leftType : dataTypes) {
            TestCaseSupplier.TypedDataSupplier leftDataSupplier = testCaseSupplier(leftType, pointsOnly);
            for (DataType rightType : dataTypes) {
                if (typeCompatible(leftType, rightType)) {
                    TestCaseSupplier.TypedDataSupplier rightDataSupplier = testCaseSupplier(rightType, pointsOnly);
                    suppliers.add(
                        TestCaseSupplier.testCaseSupplier(
                            leftDataSupplier,
                            rightDataSupplier,
                            BinarySpatialFunctionTestCase::spatialEvaluatorString,
                            returnType,
                            (l, r) -> expected(l, leftType, r, rightType)
                        )
                    );
                }
            }
        }
    }

    /**
     * Binary spatial functions that take two spatial arguments, one of which is a gridId,
     * should use this to generate combinations of test cases.
     */
    protected static void addSpatialGridCombinations(List<TestCaseSupplier> suppliers, DataType[] dataTypes, DataType returnType) {
        for (DataType leftType : dataTypes) {
            TestCaseSupplier.TypedDataSupplier leftDataSupplier = testCaseSupplier(leftType, true);
            for (DataType rightType : new DataType[] { DataType.GEOHASH, DataType.GEOTILE, DataType.GEOHEX }) {
                if (typeCompatible(leftType, rightType)) {
                    TestCaseSupplier.TypedDataSupplier rightDataSupplier = testCaseSupplier(rightType, false);
                    suppliers.add(
                        TestCaseSupplier.testCaseSupplier(
                            leftDataSupplier,
                            rightDataSupplier,
                            BinarySpatialFunctionTestCase::spatialEvaluatorString,
                            returnType,
                            (l, r) -> expected(l, leftType, r, rightType)
                        )
                    );
                    suppliers.add(
                        TestCaseSupplier.testCaseSupplier(
                            rightDataSupplier,
                            leftDataSupplier,
                            BinarySpatialFunctionTestCase::spatialEvaluatorString,
                            returnType,
                            (l, r) -> expected(l, rightType, r, leftType)
                        )
                    );
                }
            }
        }
    }

    protected static Object expected(Object left, DataType leftType, Object right, DataType rightType) {
        if (typeCompatible(leftType, rightType) == false) {
            return null;
        }
        BinarySpatialFunction.BinarySpatialComparator<?> spatialRelations = spatialRelations(left, leftType, right, rightType);
        try {
            if (DataType.isGeoGrid(leftType)) {
                return expectedGrid(spatialRelations, asGeometryWKB(right, rightType), left, leftType);
            } else if (DataType.isGeoGrid(rightType)) {
                return expectedGrid(spatialRelations, asGeometryWKB(left, leftType), right, rightType);
            } else {
                BytesRef leftWKB = asGeometryWKB(left, leftType);
                BytesRef rightWKB = asGeometryWKB(right, rightType);
                return spatialRelations.compare(leftWKB, rightWKB);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean expectedGrid(
        BinarySpatialFunction.BinarySpatialComparator<?> spatialRelations,
        BytesRef wkb,
        Object grid,
        DataType gridType
    ) {
        Geometry geometry = SpatialCoordinateTypes.UNSPECIFIED.wkbToGeometry(wkb);
        long gridId = Long.parseLong(grid.toString());
        return ((SpatialRelatesFunction.SpatialRelations) spatialRelations).compareGeometryAndGrid(geometry, gridId, gridType);
    }

    /**
     * When two spatial arguments are processed and then compared with a third argument,
     * we need to process this argument too, before producing the final result.
     */
    protected static Object expected(
        Object left,
        DataType leftType,
        Object right,
        DataType rightType,
        Object arg,
        BinaryOperator<Object> argProcessor
    ) {
        Object result = expected(left, leftType, right, rightType);
        if (result == null) {
            return null;
        }
        return argProcessor.apply(result, arg);
    }

    private static BinarySpatialFunction.BinarySpatialComparator<?> getRelationsField(String name) {
        try {
            Field field = getSpatialRelatesFunctionClass().getField(name);
            Object value = field.get(null);
            return (BinarySpatialFunction.BinarySpatialComparator<?>) value;
        } catch (NoSuchFieldException | ClassNotFoundException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private static BinarySpatialFunction.BinarySpatialComparator<?> spatialRelations(
        Object left,
        DataType leftType,
        Object right,
        DataType rightType
    ) {
        if (isSpatialGeo(leftType) || isSpatialGeo(rightType)) {
            return getRelationsField("GEO");
        } else if (isSpatialOrGrid(leftType) || isSpatialOrGrid(rightType)) {
            return getRelationsField("CARTESIAN");
        } else {
            throw new IllegalArgumentException(
                "Unsupported left and right types: left["
                    + leftType.esType()
                    + ":"
                    + left.getClass().getSimpleName()
                    + "] right["
                    + rightType.esType()
                    + ":"
                    + right.getClass().getSimpleName()
                    + "]"
            );
        }
    }

    protected static BytesRef asGeometryWKB(Object object, DataType dataType) {
        if (isString(dataType)) {
            return SpatialCoordinateTypes.UNSPECIFIED.wktToWkb(object.toString());
        } else if (object instanceof BytesRef wkb) {
            return wkb;
        } else {
            throw new IllegalArgumentException("Invalid geometry base type for " + dataType + ": " + object.getClass().getSimpleName());
        }
    }

    protected static boolean typeCompatible(DataType leftType, DataType rightType) {
        if (isSpatialOrGrid(leftType) && isSpatialOrGrid(rightType)) {
            // Both must be GEO_* or both must be CARTESIAN_*
            return countGeo(leftType, rightType) != 1;
        }
        return true;
    }

    private static DataType pickSpatialType(DataType leftType, DataType rightType) {
        if (isSpatialOrGrid(leftType)) {
            return leftType;
        } else if (isSpatialOrGrid(rightType)) {
            return rightType;
        } else {
            throw new IllegalArgumentException("Invalid spatial types: " + leftType + " and " + rightType);
        }
    }

    private static Matcher<String> spatialEvaluatorString(DataType leftType, DataType rightType) {
        String crsType = isSpatialGeo(pickSpatialType(leftType, rightType)) ? "Geo" : "Cartesian";
        if (DataType.isGeoGrid(leftType) || DataType.isGeoGrid(rightType)) {
            int[] c = DataType.isGeoGrid(leftType) ? new int[] { 1, 0 } : new int[] { 0, 1 };
            DataType gridType = DataType.isGeoGrid(leftType) ? leftType : rightType;
            String channelText = "wkb=Attribute[channel=" + c[0] + "], gridId=Attribute[channel=" + c[1] + "], gridType=" + gridType;
            return equalTo(getFunctionClassName() + crsType + "SourceAndSourceGridEvaluator[" + channelText + "]");
        }
        String channels = channelsText("left", "right");
        return equalTo(getFunctionClassName() + crsType + "SourceAndSourceEvaluator[" + channels + "]");
    }

    private static String channelsText(String... args) {
        return IntStream.range(0, args.length).mapToObj(i -> args[i] + "=Attribute[channel=" + i + "]").collect(Collectors.joining(", "));
    }

    private static int countGeo(DataType... types) {
        int count = 0;
        for (DataType type : types) {
            if (isSpatialGeo(type)) {
                count++;
            }
        }
        return count;
    }

    @Override
    protected Expression serializeDeserializeExpression(Expression expression) {
        // TODO: Functions inheriting from this superclass don't serialize the Source, and must be fixed.
        return expression;
    }
}
