/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.function.Supplier;

public class SinhTests extends AbstractFunctionTestCase {
    public SinhTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = TestCaseSupplier.forUnaryCastingToDouble(
            "SinhEvaluator",
            "val",
            Math::sinh,
            -710d,
            710d,  // Hyperbolic sine grows extremely fast. Values outside this range return Double.POSITIVE_INFINITY
            List.of()
        );

        // Out of range cases
        suppliers.addAll(
            TestCaseSupplier.forUnaryCastingToDouble(
                "SinhEvaluator",
                "val",
                k -> null,
                Double.NEGATIVE_INFINITY,
                -711d,
                List.of(
                    "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                    "Line -1:-1: java.lang.ArithmeticException: sinh overflow"
                )
            )
        );
        suppliers.addAll(
            TestCaseSupplier.forUnaryCastingToDouble(
                "SinhEvaluator",
                "val",
                k -> null,
                711d,
                Double.POSITIVE_INFINITY,
                List.of(
                    "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                    "Line -1:-1: java.lang.ArithmeticException: sinh overflow"
                )
            )
        );
        return parameterSuppliersFromTypedData(errorsForCasesWithoutExamples(anyNullIsNull(true, suppliers)));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Sinh(source, args.get(0));
    }
}
