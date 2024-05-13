/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import joptsimple.internal.Strings;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.ql.expression.TypeResolutions;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.util.SpatialCoordinateTypes;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialRelatesFunction.compatibleTypeNames;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.isSpatial;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.isSpatialGeo;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.isString;
import static org.hamcrest.Matchers.equalTo;

public abstract class SpatialRelatesFunctionTestCase extends AbstractFunctionTestCase {

    private static String getFunctionClassName() {
        Class<?> testClass = getTestClass();
        String testClassName = testClass.getSimpleName();
        return testClassName.replace("Tests", "");
    }

    private static Class<?> getSpatialRelatesFunctionClass() throws ClassNotFoundException {
        String functionClassName = getFunctionClassName();
        return Class.forName("org.elasticsearch.xpack.esql.expression.function.scalar.spatial." + functionClassName);
    }

    private static SpatialRelatesFunction.SpatialRelations getRelationsField(String name) {
        try {
            Field field = getSpatialRelatesFunctionClass().getField(name);
            Object value = field.get(null);
            return (SpatialRelatesFunction.SpatialRelations) value;
        } catch (NoSuchFieldException | ClassNotFoundException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    protected static void addSpatialCombinations(List<TestCaseSupplier> suppliers, DataType[] dataTypes) {
        for (DataType leftType : dataTypes) {
            TestCaseSupplier.TypedDataSupplier leftDataSupplier = testCaseSupplier(leftType);
            for (DataType rightType : dataTypes) {
                if (typeCompatible(leftType, rightType)) {
                    TestCaseSupplier.TypedDataSupplier rightDataSupplier = testCaseSupplier(rightType);
                    suppliers.add(
                        TestCaseSupplier.testCaseSupplier(
                            leftDataSupplier,
                            rightDataSupplier,
                            SpatialRelatesFunctionTestCase::spatialEvaluatorString,
                            DataTypes.BOOLEAN,
                            (l, r) -> expected(l, leftType, r, rightType)
                        )
                    );
                }
            }
        }
    }

    /**
     * Build the expected error message for an invalid type signature.
     */
    protected static String typeErrorMessage(boolean includeOrdinal, List<Set<DataType>> validPerPosition, List<DataType> types) {
        List<Integer> badArgPositions = new ArrayList<>();
        for (int i = 0; i < types.size(); i++) {
            if (validPerPosition.get(i).contains(types.get(i)) == false) {
                badArgPositions.add(i);
            }
        }
        if (badArgPositions.isEmpty()) {
            return oneInvalid(1, 0, includeOrdinal, types);
        } else if (badArgPositions.size() == 1) {
            int badArgPosition = badArgPositions.get(0);
            int goodArgPosition = badArgPosition == 0 ? 1 : 0;
            if (isSpatial(types.get(goodArgPosition)) == false) {
                return oneInvalid(badArgPosition, -1, includeOrdinal, types);
            } else {
                return oneInvalid(badArgPosition, goodArgPosition, includeOrdinal, types);
            }
        } else {
            return oneInvalid(0, -1, includeOrdinal, types);
        }
    }

    private static String oneInvalid(int badArgPosition, int goodArgPosition, boolean includeOrdinal, List<DataType> types) {
        String ordinal = includeOrdinal ? TypeResolutions.ParamOrdinal.fromIndex(badArgPosition).name().toLowerCase(Locale.ROOT) + " " : "";
        String expectedType = goodArgPosition >= 0
            ? compatibleTypes(types.get(goodArgPosition))
            : "geo_point, cartesian_point, geo_shape or cartesian_shape";
        String name = types.get(badArgPosition).typeName();
        return ordinal + "argument of [] must be [" + expectedType + "], found value [" + name + "] type [" + name + "]";
    }

    private static String compatibleTypes(DataType spatialDataType) {
        return Strings.join(compatibleTypeNames(spatialDataType), " or ");
    }

    private static TestCaseSupplier.TypedDataSupplier testCaseSupplier(DataType dataType) {
        return switch (dataType.esType()) {
            case "geo_point" -> TestCaseSupplier.geoPointCases(() -> false).get(0);
            case "geo_shape" -> TestCaseSupplier.geoShapeCases(() -> false).get(0);
            case "cartesian_point" -> TestCaseSupplier.cartesianPointCases(() -> false).get(0);
            case "cartesian_shape" -> TestCaseSupplier.cartesianShapeCases(() -> false).get(0);
            default -> throw new IllegalArgumentException("Unsupported datatype for " + functionName() + ": " + dataType);
        };
    }

    private static Object expected(Object left, DataType leftType, Object right, DataType rightType) {
        if (typeCompatible(leftType, rightType) == false) {
            return null;
        }
        // TODO cast objects to right type and check intersection
        BytesRef leftWKB = asGeometryWKB(left, leftType);
        BytesRef rightWKB = asGeometryWKB(right, rightType);
        SpatialRelatesFunction.SpatialRelations spatialRelations = spatialRelations(left, leftType, right, rightType);
        try {
            return spatialRelations.geometryRelatesGeometry(leftWKB, rightWKB);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static SpatialRelatesFunction.SpatialRelations spatialRelations(
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

    private static BytesRef asGeometryWKB(Object object, DataType dataType) {
        if (isString(dataType)) {
            return SpatialCoordinateTypes.UNSPECIFIED.wktToWkb(object.toString());
        } else if (object instanceof BytesRef wkb) {
            return wkb;
        } else {
            throw new IllegalArgumentException("Invalid geometry base type for " + dataType + ": " + object.getClass().getSimpleName());
        }
    }

    private static boolean typeCompatible(DataType leftType, DataType rightType) {
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
        return equalTo(
            getFunctionClassName() + crsType + "SourceAndSourceEvaluator[leftValue=Attribute[channel=0], rightValue=Attribute[channel=1]]"
        );
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
}
