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

import java.util.List;
import java.util.function.Supplier;

public class AcoshTests extends AbstractScalarFunctionTestCase {
    public AcoshTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        // acosh(x) = ln(x + sqrt(x^2 - 1)), valid for x >= 1
        List<TestCaseSupplier> suppliers = TestCaseSupplier.forUnaryCastingToDouble(
            "AcoshEvaluator",
            "val",
            v -> Math.log(v + Math.sqrt(v * v - 1)),
            1d,
            Double.MAX_VALUE,
            List.of()
        );

        // Out of range cases (x < 1)
        suppliers.addAll(
            TestCaseSupplier.forUnaryCastingToDouble(
                "AcoshEvaluator",
                "val",
                k -> null,
                Double.NEGATIVE_INFINITY,
                0.999999d,
                List.of(
                    "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                    "Line 1:1: java.lang.ArithmeticException: Acosh input out of range"
                )
            )
        );
        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Acosh(source, args.get(0));
    }
}
