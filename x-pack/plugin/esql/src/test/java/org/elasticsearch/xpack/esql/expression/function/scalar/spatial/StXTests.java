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
import org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.CARTESIAN;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.UNSPECIFIED;

@FunctionName("st_x")
public class StXTests extends AbstractScalarFunctionTestCase {
    public StXTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        String expectedGeo = "StXFromGeoWKBEvaluator[in=Attribute[channel=0]]";
        String expectedCartesian = "StXFromCartesianWKBEvaluator[in=Attribute[channel=0]]";
        final List<TestCaseSupplier> suppliers = new ArrayList<>();
        TestCaseSupplier.forUnaryGeoPoint(suppliers, expectedGeo, DOUBLE, StXTests::valueOfGeo, List.of());
        TestCaseSupplier.forUnaryCartesianPoint(suppliers, expectedCartesian, DOUBLE, StXTests::valueOfCartesian, List.of());
        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    private static double quantize(double value, SpatialCoordinateTypes type) {
        long encoded = type.pointAsLong(value, 0);
        return type.decodeX(encoded);
    }

    private static double valueOfGeo(BytesRef wkb) {
        return quantize(UNSPECIFIED.wkbAsPoint(wkb).getX(), GEO);
    }

    private static double valueOfCartesian(BytesRef wkb) {
        return quantize(UNSPECIFIED.wkbAsPoint(wkb).getX(), CARTESIAN);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new StX(source, args.get(0));
    }
}
