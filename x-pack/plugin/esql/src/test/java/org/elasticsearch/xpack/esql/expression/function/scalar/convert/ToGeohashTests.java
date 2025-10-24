/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

@FunctionName("to_geohash")
public class ToGeohashTests extends AbstractScalarFunctionTestCase {
    public ToGeohashTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        final String attribute = "Attribute[channel=0]";
        final String evaluator = "ToGeohashFromStringEvaluator[in=Attribute[channel=0]]";
        final List<TestCaseSupplier> suppliers = new ArrayList<>();

        TestCaseSupplier.forUnaryGeoGrid(suppliers, attribute, DataType.GEOHASH, DataType.GEOHASH, v -> v, List.of());
        TestCaseSupplier.forUnaryGeoGrid(suppliers, attribute, DataType.LONG, DataType.GEOHASH, v -> v, List.of());
        TestCaseSupplier.forUnaryGeoGrid(suppliers, evaluator, DataType.KEYWORD, DataType.GEOHASH, ToGeohashTests::valueOf, List.of());
        TestCaseSupplier.forUnaryGeoGrid(suppliers, evaluator, DataType.TEXT, DataType.GEOHASH, ToGeohashTests::valueOf, List.of());

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    private static long valueOf(Object gridAddress) {
        assert gridAddress instanceof BytesRef;
        return Geohash.longEncode(((BytesRef) gridAddress).utf8ToString());
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToGeohash(source, args.get(0));
    }
}
