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

public class CoshTests extends AbstractScalarFunctionTestCase {
    public CoshTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = TestCaseSupplier.forUnaryCastingToDouble(
            "CoshEvaluator",
            "val",
            Math::cosh,
            -710d,
            710d,  // Hyperbolic Cosine grows extremely fast. Values outside this range return Double.POSITIVE_INFINITY
            List.of()
        );

        // Out of range cases
        suppliers.addAll(
            TestCaseSupplier.forUnaryCastingToDouble(
                "CoshEvaluator",
                "val",
                k -> null,
                Double.NEGATIVE_INFINITY,
                -711d,
                List.of(
                    "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                    "Line -1:-1: java.lang.ArithmeticException: cosh overflow"
                )
            )
        );
        suppliers.addAll(
            TestCaseSupplier.forUnaryCastingToDouble(
                "CoshEvaluator",
                "val",
                k -> null,
                711d,
                Double.POSITIVE_INFINITY,
                List.of(
                    "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                    "Line -1:-1: java.lang.ArithmeticException: cosh overflow"
                )
            )
        );
        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers, (v, p) -> "numeric");
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Cosh(source, args.get(0));
    }
}
