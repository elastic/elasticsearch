/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import joptsimple.internal.Strings;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.xpack.esql.core.type.DataType.GEOHASH;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEOHEX;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEOTILE;
import static org.elasticsearch.xpack.esql.core.type.DataType.isSpatialAndGrid;
import static org.elasticsearch.xpack.esql.core.type.DataType.isSpatialGeo;
import static org.elasticsearch.xpack.esql.core.type.DataType.isString;
import static org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialRelatesFunction.compatibleTypeNames;
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
                case "geohash" -> TestCaseSupplier.geoGridCases(GEOHASH, () -> false).get(0);
                case "geotile" -> TestCaseSupplier.geoGridCases(GEOTILE, () -> false).get(0);
                case "geohex" -> TestCaseSupplier.geoGridCases(GEOHEX, () -> false).get(0);
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

    /**
     * Build the expected error message for an invalid type signature.
     * For two args, this assumes they are both spatial.
     * For three args, we assume two spatial and one additional numerical argument, treated differently.
     */
    protected static String typeErrorMessage(
        boolean includeOrdinal,
        List<Set<DataType>> validPerPosition,
        List<DataType> types,
        boolean pointsOnly,
        boolean supportsGrid
    ) {
        boolean argInvalid = false;
        List<Integer> badArgPositions = new ArrayList<>();
        for (int i = 0; i < types.size(); i++) {
            if (validPerPosition.get(i).contains(types.get(i)) == false) {
                if (i == 2) {
                    argInvalid = true;
                } else {
                    badArgPositions.add(i);
                }
            }
        }
        if (badArgPositions.isEmpty() && types.get(0) != DataType.NULL && types.get(1) != DataType.NULL) {
            // First two arguments are valid spatial types, but it is still possible they are incompatible
            var leftCrs = BinarySpatialFunction.SpatialCrsType.fromDataType(types.get(0));
            var rightCrs = BinarySpatialFunction.SpatialCrsType.fromDataType(types.get(1));
            if (leftCrs != rightCrs) {
                badArgPositions.add(1);
            }
        }
        if (badArgPositions.size() == 1) {
            int badArgPosition = badArgPositions.get(0);
            int goodArgPosition = badArgPosition == 0 ? 1 : 0;
            if (DataType.isGeoGrid(types.get(goodArgPosition))) {
                // When the valid position is a grid, the other type can only be points
                return oneInvalid(badArgPosition, goodArgPosition, includeOrdinal, types, true, supportsGrid);
            } else if (isSpatialAndGrid(types.get(goodArgPosition)) == false) {
                return oneInvalid(badArgPosition, -1, includeOrdinal, types, pointsOnly, supportsGrid);
            } else {
                return oneInvalid(badArgPosition, goodArgPosition, includeOrdinal, types, pointsOnly, supportsGrid);
            }
        } else if (argInvalid && badArgPositions.size() != 2) {
            return invalidArg(types.get(2));
        } else if (supportsGrid && DataType.isGeoGrid(types.get(0))) {
            return invalidGrid(1, types, true);
        } else if (supportsGrid && DataType.isGeoGrid(types.get(1))) {
            return invalidGrid(0, types, true);
        } else {
            return oneInvalid(0, -1, includeOrdinal, types, pointsOnly, supportsGrid);
        }
    }

    private static String invalidArg(DataType invalidType) {
        return String.format(
            Locale.ROOT,
            "%s argument of [%s] must be [%s], found value [%s] type [%s]",
            TypeResolutions.ParamOrdinal.fromIndex(2).toString().toLowerCase(Locale.ROOT),
            "source",
            "double",
            invalidType.typeName(),
            invalidType.typeName()
        );
    }

    private static String invalidGrid(int badArgPosition, List<DataType> types, boolean pointsOnly) {
        String expectedType = pointsOnly ? "geo_point" : "geo_point or geo_shape";
        String ordinal = TypeResolutions.ParamOrdinal.fromIndex(badArgPosition).name().toLowerCase(Locale.ROOT) + " ";
        String name = types.get(badArgPosition).typeName();
        return ordinal + "argument of [source] must be [" + expectedType + "], found value [" + name + "] type [" + name + "]";
    }

    private static String oneInvalid(
        int badArgPosition,
        int goodArgPosition,
        boolean includeOrdinal,
        List<DataType> types,
        boolean pointsOnly,
        boolean supportsGrid
    ) {
        String expected = pointsOnly
            ? "geo_point or cartesian_point"
            : (supportsGrid
                ? "geo_point, cartesian_point, geo_shape, cartesian_shape, geohash, geotile or geohex"
                : "geo_point, cartesian_point, geo_shape or cartesian_shape");
        String ordinal = includeOrdinal ? TypeResolutions.ParamOrdinal.fromIndex(badArgPosition).name().toLowerCase(Locale.ROOT) + " " : "";
        String expectedType = goodArgPosition >= 0 ? compatibleTypes(types.get(goodArgPosition)) : expected;
        String name = types.get(badArgPosition).typeName();
        return ordinal + "argument of [source] must be [" + expectedType + "], found value [" + name + "] type [" + name + "]";
    }

    private static String compatibleTypes(DataType spatialDataType) {
        return Strings.join(compatibleTypeNames(spatialDataType), " or ");
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
        } else if (isSpatialAndGrid(leftType) || isSpatialAndGrid(rightType)) {
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
        if (isSpatialAndGrid(leftType) && isSpatialAndGrid(rightType)) {
            // Both must be GEO_* or both must be CARTESIAN_*
            return countGeo(leftType, rightType) != 1;
        }
        return true;
    }

    private static DataType pickSpatialType(DataType leftType, DataType rightType) {
        if (isSpatialAndGrid(leftType)) {
            return leftType;
        } else if (isSpatialAndGrid(rightType)) {
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
