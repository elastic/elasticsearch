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
import org.elasticsearch.geometry.utils.GeometryPointCountVisitor;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.UNSPECIFIED;

@FunctionName("st_npoints")
public class StNPointsTests extends AbstractScalarFunctionTestCase {
    public StNPointsTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    private static final GeometryPointCountVisitor pointCounter = new GeometryPointCountVisitor();

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        String evaluatorName = "StNPointsFromWKBEvaluator[wkbBlock=Attribute[channel=0]]";
        final List<TestCaseSupplier> suppliers = new ArrayList<>();
        TestCaseSupplier.forUnaryGeoPoint(suppliers, evaluatorName, INTEGER, StNPointsTests::valueOf, List.of());
        TestCaseSupplier.forUnaryCartesianPoint(suppliers, evaluatorName, INTEGER, StNPointsTests::valueOf, List.of());
        TestCaseSupplier.forUnaryGeoShape(suppliers, evaluatorName, INTEGER, StNPointsTests::valueOf, List.of());
        TestCaseSupplier.forUnaryCartesianShape(suppliers, evaluatorName, INTEGER, StNPointsTests::valueOf, List.of());
        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    private static int valueOf(BytesRef wkb) {
        var geometry = UNSPECIFIED.wkbToGeometry(wkb);
        return geometry.isEmpty() ? 0 : geometry.visit(pointCounter);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new StNPoints(source, args.getFirst());
    }
}
