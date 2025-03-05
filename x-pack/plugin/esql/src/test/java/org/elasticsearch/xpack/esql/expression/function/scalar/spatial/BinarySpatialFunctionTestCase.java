/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import joptsimple.internal.Strings;

import org.apache.lucene.util.BytesRef;
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

import static org.elasticsearch.xpack.esql.core.type.DataType.isSpatial;
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
     * Build the expected error message for an invalid type signature.
     * For two args, this assumes they are both spatial.
     * For three args, we assume two spatial and one additional numerical argument, treated differently.
     */
    protected static String typeErrorMessage(
        boolean includeOrdinal,
        List<Set<DataType>> validPerPosition,
        List<DataType> types,
        boolean pointsOnly
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
            if (isSpatial(types.get(goodArgPosition)) == false) {
                return oneInvalid(badArgPosition, -1, includeOrdinal, types, pointsOnly);
            } else {
                return oneInvalid(badArgPosition, goodArgPosition, includeOrdinal, types, pointsOnly);
            }
        } else if (argInvalid && badArgPositions.size() != 2) {
            return invalidArg(types.get(2));
        } else {
            return oneInvalid(0, -1, includeOrdinal, types, pointsOnly);
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

    private static String oneInvalid(
        int badArgPosition,
        int goodArgPosition,
        boolean includeOrdinal,
        List<DataType> types,
        boolean pointsOnly
    ) {
        String expected = pointsOnly ? "geo_point or cartesian_point" : "geo_point, cartesian_point, geo_shape or cartesian_shape";
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
        // TODO cast objects to right type and check intersection
        BytesRef leftWKB = asGeometryWKB(left, leftType);
        BytesRef rightWKB = asGeometryWKB(right, rightType);
        BinarySpatialFunction.BinarySpatialComparator<?> spatialRelations = spatialRelations(left, leftType, right, rightType);
        try {
            return spatialRelations.compare(leftWKB, rightWKB);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
        } else if (isSpatial(leftType) || isSpatial(rightType)) {
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
        if (isSpatial(leftType) && isSpatial(rightType)) {
            // Both must be GEO_* or both must be CARTESIAN_*
            return countGeo(leftType, rightType) != 1;
        }
        return true;
    }

    private static DataType pickSpatialType(DataType leftType, DataType rightType) {
        if (isSpatial(leftType)) {
            return leftType;
        } else if (isSpatial(rightType)) {
            return rightType;
        } else {
            throw new IllegalArgumentException("Invalid spatial types: " + leftType + " and " + rightType);
        }
    }

    private static Matcher<String> spatialEvaluatorString(DataType leftType, DataType rightType) {
        String crsType = isSpatialGeo(pickSpatialType(leftType, rightType)) ? "Geo" : "Cartesian";
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
