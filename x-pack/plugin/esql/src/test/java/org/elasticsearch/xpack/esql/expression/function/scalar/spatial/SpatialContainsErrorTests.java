/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import joptsimple.internal.Strings;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.ErrorsForCasesWithoutExamplesTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.elasticsearch.xpack.esql.core.type.DataType.isSpatialOrGrid;
import static org.elasticsearch.xpack.esql.expression.function.scalar.spatial.BinarySpatialFunction.compatibleTypeNames;
import static org.hamcrest.Matchers.equalTo;

public class SpatialContainsErrorTests extends ErrorsForCasesWithoutExamplesTestCase {
    @Override
    protected List<TestCaseSupplier> cases() {
        return paramsToSuppliers(SpatialContainsTests.parameters());
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new SpatialContains(source, args.get(0), args.get(1));
    }

    @Override
    protected Matcher<String> expectedTypeErrorMatcher(List<Set<DataType>> validPerPosition, List<DataType> signature) {
        return equalTo(typeErrorMessage(true, validPerPosition, signature, false, false));
    }

    /**
     * Build the expected error message for an invalid type signature.
     * For two args, this assumes they are both spatial.
     * For three args, we assume two spatial and one additional numerical argument, treated differently.
     */
    protected static String typeErrorMessage(
        boolean includeOrdinal,
        List<Set<DataType>> validPerPosition,
        List<DataType> signature,
        boolean pointsOnly,
        boolean supportsGrid
    ) {
        boolean argInvalid = false;
        List<Integer> badArgPositions = new ArrayList<>();
        for (int i = 0; i < signature.size(); i++) {
            if (validPerPosition.get(i).contains(signature.get(i)) == false) {
                if (i == 2) {
                    argInvalid = true;
                } else {
                    badArgPositions.add(i);
                }
            }
        }
        if (badArgPositions.isEmpty() && signature.get(0) != DataType.NULL && signature.get(1) != DataType.NULL) {
            // First two arguments are valid spatial types, but it is still possible they are incompatible
            var leftCrs = BinarySpatialFunction.SpatialCrsType.fromDataType(signature.get(0));
            var rightCrs = BinarySpatialFunction.SpatialCrsType.fromDataType(signature.get(1));
            if (leftCrs != rightCrs) {
                badArgPositions.add(1);
            }
        }
        if (badArgPositions.size() == 1) {
            int badArgPosition = badArgPositions.get(0);
            int goodArgPosition = badArgPosition == 0 ? 1 : 0;
            if (DataType.isGeoGrid(signature.get(goodArgPosition))) {
                // When the valid position is a grid, the other type can only be points
                return oneInvalid(badArgPosition, goodArgPosition, includeOrdinal, signature, true, supportsGrid);
            } else if (isSpatialOrGrid(signature.get(goodArgPosition)) == false) {
                return oneInvalid(badArgPosition, -1, includeOrdinal, signature, pointsOnly, supportsGrid);
            } else {
                return oneInvalid(badArgPosition, goodArgPosition, includeOrdinal, signature, pointsOnly, supportsGrid);
            }
        } else if (argInvalid && badArgPositions.size() != 2) {
            return invalidArg(signature.get(2), signature);
        } else if (supportsGrid && DataType.isGeoGrid(signature.get(0))) {
            return invalidGrid(1, signature, true);
        } else if (supportsGrid && DataType.isGeoGrid(signature.get(1))) {
            return invalidGrid(0, signature, true);
        } else {
            return oneInvalid(0, -1, includeOrdinal, signature, pointsOnly, supportsGrid);
        }
    }

    private static String invalidArg(DataType invalidType, List<DataType> signature) {
        return String.format(
            Locale.ROOT,
            "second argument of [%s] must be [%s], found value [] type [%s]",
            sourceForSignature(signature),
            "double",
            invalidType.typeName()
        );
    }

    private static String invalidGrid(int badArgPosition, List<DataType> signature, boolean pointsOnly) {
        String expectedType = pointsOnly ? "geo_point" : "geo_point or geo_shape";
        String ordinal = TypeResolutions.ParamOrdinal.fromIndex(badArgPosition).name().toLowerCase(Locale.ROOT) + " ";
        String name = signature.get(badArgPosition).typeName();
        return ordinal
            + "argument of ["
            + sourceForSignature(signature)
            + "] must be ["
            + expectedType
            + "], found value [] type ["
            + name
            + "]";
    }

    private static String oneInvalid(
        int badArgPosition,
        int goodArgPosition,
        boolean includeOrdinal,
        List<DataType> signature,
        boolean pointsOnly,
        boolean supportsGrid
    ) {
        String expected = pointsOnly
            ? "geo_point or cartesian_point"
            : (supportsGrid
                ? "geo_point, cartesian_point, geo_shape, cartesian_shape, geohash, geotile or geohex"
                : "geo_point, cartesian_point, geo_shape or cartesian_shape");
        String ordinal = includeOrdinal ? TypeResolutions.ParamOrdinal.fromIndex(badArgPosition).name().toLowerCase(Locale.ROOT) + " " : "";
        String expectedType = goodArgPosition >= 0 ? compatibleTypes(signature.get(goodArgPosition)) : expected;
        String name = signature.get(badArgPosition).typeName();
        return ordinal
            + "argument of ["
            + sourceForSignature(signature)
            + "] must be ["
            + expectedType
            + "], found value [] type ["
            + name
            + "]";
    }

    private static String compatibleTypes(DataType spatialDataType) {
        return Strings.join(compatibleTypeNames(spatialDataType), " or ");
    }
}
