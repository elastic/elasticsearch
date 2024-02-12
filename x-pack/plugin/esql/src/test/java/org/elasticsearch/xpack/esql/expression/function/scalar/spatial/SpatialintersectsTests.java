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
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.TypeResolutions;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.util.SpatialCoordinateTypes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.isSpatial;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.isSpatialGeo;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.isString;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;

public class SpatialintersectsTests extends AbstractFunctionTestCase {
    public SpatialintersectsTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        DataType[] geoDataTypes = { KEYWORD, EsqlDataTypes.GEO_POINT, EsqlDataTypes.GEO_SHAPE };
        addSpatialCombinations(suppliers, geoDataTypes);
        DataType[] cartesianDataTypes = { KEYWORD, EsqlDataTypes.CARTESIAN_POINT, EsqlDataTypes.CARTESIAN_SHAPE };
        addSpatialCombinations(suppliers, cartesianDataTypes);
        return parameterSuppliersFromTypedData(
            errorsForCasesWithoutExamples(anyNullIsNull(true, suppliers), SpatialintersectsTests::typeErrorMessage)
        );
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new SpatialIntersects(source, args.get(0), args.get(1));
    }

    private static void addSpatialCombinations(List<TestCaseSupplier> suppliers, DataType[] dataTypes) {
        for (DataType leftType : dataTypes) {
            TestCaseSupplier.TypedDataSupplier leftDataSupplier = testCaseSupplier(leftType);
            for (DataType rightType : dataTypes) {
                if (typeCompatible(leftType, rightType)) {
                    TestCaseSupplier.TypedDataSupplier rightDataSupplier = testCaseSupplier(rightType);
                    suppliers.add(
                        TestCaseSupplier.testCaseSupplier(
                            leftDataSupplier,
                            rightDataSupplier,
                            SpatialintersectsTests::spatialEvaluatorString,
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
        if (badArgPositions.size() == 0) {
            return oneInvalid(1, 0, includeOrdinal, types);
        } else if (badArgPositions.size() == 1) {
            int badArgPosition = badArgPositions.get(0);
            int goodArgPosition = badArgPosition == 0 ? 1 : 0;
            if (isSpatial(types.get(goodArgPosition)) == false) {
                return bothInvalid(types.get(0), types.get(1));
            } else {
                return oneInvalid(badArgPosition, goodArgPosition, includeOrdinal, types);
            }
        } else {
            return bothInvalid(types.get(0), types.get(1));
        }
    }

    private static String oneInvalid(int badArgPosition, int goodArgPosition, boolean includeOrdinal, List<DataType> types) {
        String ordinal = includeOrdinal ? TypeResolutions.ParamOrdinal.fromIndex(badArgPosition).name().toLowerCase(Locale.ROOT) + " " : "";
        String expectedType = types.get(goodArgPosition).esType() + ", keyword or text";
        String name = types.get(badArgPosition).typeName();
        return ordinal + "argument of [] must be [" + expectedType + "], found value [" + name + "] type [" + name + "]";
    }

    private static String bothInvalid(DataType leftType, DataType rightType) {
        return String.format(
            Locale.ROOT,
            "when neither arguments of [] are [%s], both must be [%s], found value [%s] type [%s] and value [%s] type [%s]",
            "geo_point or geo_shape or cartesian_point or cartesian_shape",
            "keyword or text",
            leftType.typeName(),
            leftType.typeName(),
            rightType.typeName(),
            rightType.typeName()
        );
    }

    private static TestCaseSupplier.TypedDataSupplier testCaseSupplier(DataType dataType) {
        return switch (dataType.esType()) {
            case "geo_point" -> TestCaseSupplier.geoPointCases(() -> false).get(0);
            case "geo_shape" -> TestCaseSupplier.geoShapeCases(() -> false).get(0);
            case "cartesian_point" -> TestCaseSupplier.cartesianPointCases(() -> false).get(0);
            case "cartesian_shape" -> TestCaseSupplier.cartesianShapeCases(() -> false).get(0);
            case "keyword", "text" -> TestCaseSupplier.textShapeCases(() -> false).get(0);
            default -> throw new IllegalArgumentException("Unsupported datatype for ST_INTERSECTS: " + dataType);
        };
    }

    private static Object expected(Object left, DataType leftType, Object right, DataType rightType) {
        if (typeCompatible(leftType, rightType) == false) {
            return null;
        }
        // TODO cast objects to right type and check intersection
        BytesRef leftWKB = asGeometryWKB(left, leftType);
        BytesRef rightWKB = asGeometryWKB(right, rightType);
        SpatialRelatesFunction.SpatialRelations spatialIntersects = spatialRelations(left, leftType, right, rightType);
        try {
            return spatialIntersects.geometryRelatesGeometry(leftWKB, rightWKB);
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
            return SpatialIntersects.GEO;
        } else if (isSpatial(leftType) || isSpatial(rightType)) {
            return SpatialIntersects.CARTESIAN;
        } else if (isString(leftType) && isString(rightType)) {
            return SpatialIntersects.CARTESIAN;
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
            return EsqlDataTypes.CARTESIAN_SHAPE;
        }
    }

    private static String spatialEvaluatorString(DataType leftType, DataType rightType) {
        String crsType = isSpatialGeo(pickSpatialType(leftType, rightType)) ? "Geo" : "Cartesian";
        String left = (leftType == KEYWORD) ? "String" : "Source";
        String right = (rightType == KEYWORD) ? "String" : "Source";
        if (left.equals("String") && right.equals("String")) {
            return makeEvaluatorName("", left, right, "0", "1");
        } else if (left.equals("String")) {
            return makeEvaluatorName(crsType, right, left, "1", "0");
        } else {
            return makeEvaluatorName(crsType, left, right, "0", "1");
        }
    }

    private static String makeEvaluatorName(String crsType, String left, String right, String leftId, String rightId) {
        return "SpatialIntersects"
            + crsType
            + left
            + "And"
            + right
            + "Evaluator[leftValue=Attribute[channel="
            + leftId
            + "], rightValue=Attribute[channel="
            + rightId
            + "]]";
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
