/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.UnaryTestCaseHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.unary;

public class SinhTests extends AbstractScalarFunctionTestCase {
    public SinhTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        UnaryTestCaseHelper helper = unary().evaluatorToString("SinhEvaluator[val=%0]").expectedFromDouble(Math::sinh);
        helper.castingToDouble(-710d, 710d, true, suppliers);

        // Out of range cases
        UnaryTestCaseHelper overflow = helper.expectNullAndWarnings(o -> List.of("Line 1:1: java.lang.ArithmeticException: sinh overflow"));
        overflow.castingToDouble(Double.NEGATIVE_INFINITY, -711d, false, suppliers);
        overflow.castingToDouble(711d, Double.POSITIVE_INFINITY, false, suppliers);
        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Sinh(source, args.get(0));
    }
}
