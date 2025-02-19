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

public class AsinTests extends AbstractScalarFunctionTestCase {
    public AsinTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        // values in range
        List<TestCaseSupplier> suppliers = TestCaseSupplier.forUnaryCastingToDouble("AsinEvaluator", "val", Math::asin, -1d, 1d, List.of());
        suppliers = anyNullIsNull(true, suppliers);

        // Values out of range
        suppliers.addAll(
            TestCaseSupplier.forUnaryCastingToDouble(
                "AsinEvaluator",
                "val",
                k -> null,
                Double.NEGATIVE_INFINITY,
                Math.nextDown(-1d),
                List.of(
                    "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                    "Line 1:1: java.lang.ArithmeticException: Asin input out of range"
                )
            )
        );
        suppliers.addAll(
            TestCaseSupplier.forUnaryCastingToDouble(
                "AsinEvaluator",
                "val",
                k -> null,
                Math.nextUp(1d),
                Double.POSITIVE_INFINITY,
                List.of(
                    "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                    "Line 1:1: java.lang.ArithmeticException: Asin input out of range"
                )
            )
        );
        return parameterSuppliersFromTypedDataWithDefaultChecksNoErrors(true, suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Asin(source, args.get(0));
    }
}
