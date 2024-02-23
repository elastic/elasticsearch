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
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.ql.type.DataTypes.DOUBLE;
import static org.elasticsearch.xpack.ql.util.SpatialCoordinateTypes.CARTESIAN;
import static org.elasticsearch.xpack.ql.util.SpatialCoordinateTypes.GEO;

public class StYTests extends AbstractFunctionTestCase {
    public StYTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        final List<TestCaseSupplier> suppliers = new ArrayList<>();
        TestCaseSupplier.forUnaryGeoPoint(suppliers, expectedEvaluator("Geo"), DOUBLE, StYTests::geoValue, List.of());
        TestCaseSupplier.forUnaryCartesianPoint(suppliers, expectedEvaluator("Cartesian"), DOUBLE, StYTests::cartesianValue, List.of());
        return parameterSuppliersFromTypedData(errorsForCasesWithoutExamples(anyNullIsNull(true, suppliers)));
    }

    private static double geoValue(BytesRef wkb) {
        return GEO.wkbAsPoint(wkb).getY();
    }

    private static double cartesianValue(BytesRef wkb) {
        return CARTESIAN.wkbAsPoint(wkb).getY();
    }

    private static String expectedEvaluator(String type) {
        return "StYFrom" + type + "PointEvaluator[field=Attribute[channel=0]]";
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new StY(source, args.get(0));
    }
}
