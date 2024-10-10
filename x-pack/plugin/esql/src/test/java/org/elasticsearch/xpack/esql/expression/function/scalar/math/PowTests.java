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
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.List;
import java.util.function.Supplier;

public class PowTests extends AbstractScalarFunctionTestCase {
    public PowTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        // Positive number bases
        List<TestCaseSupplier> suppliers = TestCaseSupplier.forBinaryCastingToDouble(
            "PowEvaluator",
            "base",
            "exponent",
            Math::pow,
            // 143^143 is still representable, but 144^144 is infinite
            1d,
            143d,
            -143d,
            143d,
            List.of()
        );
        // Anything to 0 is 1
        suppliers.addAll(
            TestCaseSupplier.forBinaryCastingToDouble(
                "PowEvaluator",
                "base",
                "exponent",
                (b, e) -> 1d,
                // 143^143 is still representable, but 144^144 is infinite
                TestCaseSupplier.castToDoubleSuppliersFromRange(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY),
                List.of(
                    new TestCaseSupplier.TypedDataSupplier("<0 double>", () -> 0d, DataType.DOUBLE),
                    new TestCaseSupplier.TypedDataSupplier("<-0 double>", () -> -0d, DataType.DOUBLE)
                ),
                List.of()
            )
        );

        // Add null cases before the rest of the error cases, so messages are correct.
        suppliers = anyNullIsNull(true, suppliers);

        // Overflow should be null
        suppliers.addAll(
            TestCaseSupplier.forBinaryCastingToDouble(
                "PowEvaluator",
                "base",
                "exponent",
                (b, e) -> null,
                // 143^143 is still representable, but 144^144 is infinite
                144d,
                Double.POSITIVE_INFINITY,
                144d,
                Double.POSITIVE_INFINITY,
                List.of(
                    "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                    "Line -1:-1: java.lang.ArithmeticException: not a finite double number: Infinity"
                )
            )
        );
        return parameterSuppliersFromTypedData(errorsForCasesWithoutExamples(suppliers, (v, p) -> "numeric"));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Pow(source, args.get(0), args.get(1));
    }

}
