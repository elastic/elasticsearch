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
import org.elasticsearch.h3.H3;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

@FunctionName("to_geohex")
public class ToGeohexTests extends AbstractScalarFunctionTestCase {
    public ToGeohexTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        final String attribute = "Attribute[channel=0]";
        final String evaluator = "ToGeohexFromStringEvaluator[in=Attribute[channel=0]]";
        final List<TestCaseSupplier> suppliers = new ArrayList<>();

        TestCaseSupplier.forUnaryGeoGrid(suppliers, attribute, DataType.GEOHEX, DataType.GEOHEX, v -> v, List.of());
        TestCaseSupplier.forUnaryGeoGrid(suppliers, attribute, DataType.LONG, DataType.GEOHEX, v -> v, List.of());
        TestCaseSupplier.forUnaryGeoGrid(suppliers, evaluator, DataType.KEYWORD, DataType.GEOHEX, ToGeohexTests::valueOf, List.of());
        TestCaseSupplier.forUnaryGeoGrid(suppliers, evaluator, DataType.TEXT, DataType.GEOHEX, ToGeohexTests::valueOf, List.of());

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    private static long valueOf(Object gridAddress) {
        assert gridAddress instanceof BytesRef;
        return H3.stringToH3(((BytesRef) gridAddress).utf8ToString());
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToGeohex(source, args.get(0));
    }
}
