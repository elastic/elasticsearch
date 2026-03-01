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
import static org.hamcrest.Matchers.closeTo;

public class AcoshTests extends AbstractScalarFunctionTestCase {

    private static final double ACOSH_OF_2 = 1.3169578969248166;
    private static final double ACOSH_OF_10 = 2.993222846126381;

    public AcoshTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        UnaryTestCaseHelper helper = unary().evaluatorToString("AcoshEvaluator[val=%0]");

        helper.expectedFromDouble(d -> 0.0).castingToDouble(1, 1, false, suppliers);
        helper.expectedFromDouble(d -> closeTo(ACOSH_OF_2, Math.ulp(ACOSH_OF_2))).castingToDouble(2, 2, false, suppliers);
        helper.expectedFromDouble(d -> closeTo(ACOSH_OF_10, Math.ulp(ACOSH_OF_10))).castingToDouble(10, 10, false, suppliers);

        // Out of range (x < 1)
        helper.expectNullAndWarnings(o -> List.of("Line 1:1: java.lang.ArithmeticException: Acosh input out of range"))
            .castingToDouble(Double.NEGATIVE_INFINITY, Math.nextDown(1), false, suppliers);
        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Acosh(source, args.getFirst());
    }
}
