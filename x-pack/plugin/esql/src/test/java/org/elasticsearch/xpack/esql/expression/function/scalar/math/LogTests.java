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

public class LogTests extends AbstractScalarFunctionTestCase {
    public LogTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        // Positive base > 1, value >= 1,
        List<TestCaseSupplier> suppliers = TestCaseSupplier.forBinaryCastingToDouble(
            "LogEvaluator",
            "base",
            "value",
            (b, l) -> Math.log10(l) / Math.log10(b),
            2d,
            Double.POSITIVE_INFINITY,
            1d,
            Double.POSITIVE_INFINITY,
            List.of()
        );

        // Positive natural logarithm
        suppliers.addAll(
            TestCaseSupplier.forUnaryCastingToDouble(
                "LogConstantEvaluator",
                "value",
                Math::log,
                Math.nextUp(1d),
                Double.POSITIVE_INFINITY,
                List.of()
            )
        );

        TestCaseSupplier.forUnaryDouble(
            suppliers,
            "LogConstantEvaluator[value=Attribute[channel=0]]",
            DataType.DOUBLE,
            Math::log,
            Math.nextUp(0d),
            Math.nextDown(1d),
            List.of()
        );

        // Positive 0 < base < 1, 0 < value < 1
        suppliers.addAll(
            TestCaseSupplier.forBinaryCastingToDouble(
                "LogEvaluator",
                "base",
                "value",
                (b, l) -> Math.log10(l) / Math.log10(b),
                List.of(
                    new TestCaseSupplier.TypedDataSupplier("<gt0 double>", () -> Math.nextUp(0d), DataType.DOUBLE),
                    new TestCaseSupplier.TypedDataSupplier("<lt1 double>", () -> Math.nextDown(1d), DataType.DOUBLE)
                ),
                List.of(
                    new TestCaseSupplier.TypedDataSupplier("<gt0 double>", () -> Math.nextUp(0d), DataType.DOUBLE),
                    new TestCaseSupplier.TypedDataSupplier("<lt1 double>", () -> Math.nextDown(1d), DataType.DOUBLE)
                ),
                List.of()
            )
        );

        // Negative base <=0,
        suppliers.addAll(
            TestCaseSupplier.forBinaryCastingToDouble(
                "LogEvaluator",
                "base",
                "value",
                (b, l) -> null,
                Double.NEGATIVE_INFINITY,
                0d,
                1d,
                Double.POSITIVE_INFINITY,
                List.of(
                    "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                    "Line -1:-1: java.lang.ArithmeticException: Log of non-positive number"
                )
            )
        );

        // Negative value <=0,
        suppliers.addAll(
            TestCaseSupplier.forBinaryCastingToDouble(
                "LogEvaluator",
                "base",
                "value",
                (b, l) -> null,
                2d,
                Double.POSITIVE_INFINITY,
                Double.NEGATIVE_INFINITY,
                0d,
                List.of(
                    "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                    "Line -1:-1: java.lang.ArithmeticException: Log of non-positive number"
                )
            )
        );

        // Negative base = 1
        suppliers.addAll(
            TestCaseSupplier.forBinaryCastingToDouble(
                "LogEvaluator",
                "base",
                "value",
                (b, l) -> null,
                1d,
                1d,
                1d,
                Double.POSITIVE_INFINITY,
                List.of(
                    "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                    "Line -1:-1: java.lang.ArithmeticException: Log of base 1"
                )
            )
        );

        // Negative 0 < base < 1, value > 1
        suppliers.addAll(
            TestCaseSupplier.forBinaryCastingToDouble(
                "LogEvaluator",
                "base",
                "value",
                (b, l) -> Math.log10(l) / Math.log10(b),
                List.of(
                    new TestCaseSupplier.TypedDataSupplier("<gt0 double>", () -> Math.nextUp(0d), DataType.DOUBLE),
                    new TestCaseSupplier.TypedDataSupplier("<lt1 double>", () -> Math.nextDown(1d), DataType.DOUBLE)
                ),
                List.of(
                    new TestCaseSupplier.TypedDataSupplier("<gt1 double>", () -> Math.nextUp(1d), DataType.DOUBLE),
                    new TestCaseSupplier.TypedDataSupplier("<largest double>", () -> Double.MAX_VALUE, DataType.DOUBLE)
                ),
                List.of()
            )
        );

        // Negative base > 1, 0 < value < 1
        suppliers.addAll(
            TestCaseSupplier.forBinaryCastingToDouble(
                "LogEvaluator",
                "base",
                "value",
                (b, l) -> Math.log10(l) / Math.log10(b),
                List.of(
                    new TestCaseSupplier.TypedDataSupplier("<gt0 double>", () -> Math.nextUp(1d), DataType.DOUBLE),
                    new TestCaseSupplier.TypedDataSupplier("<lt1 double>", () -> Double.MAX_VALUE, DataType.DOUBLE)
                ),
                List.of(
                    new TestCaseSupplier.TypedDataSupplier("<gt1 double>", () -> Math.nextUp(0d), DataType.DOUBLE),
                    new TestCaseSupplier.TypedDataSupplier("<largest double>", () -> Math.nextDown(1d), DataType.DOUBLE)
                ),
                List.of()
            )
        );

        // Negative Unary value <=0
        suppliers.addAll(
            TestCaseSupplier.forUnaryCastingToDouble(
                "LogConstantEvaluator",
                "value",
                v -> null,
                Double.NEGATIVE_INFINITY,
                0d,
                List.of(
                    "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                    "Line -1:-1: java.lang.ArithmeticException: Log of non-positive number"
                )
            )
        );

        // Add null cases before the rest of the error cases, so messages are correct.
        suppliers = anyNullIsNull(true, suppliers);

        // Negative cases
        return parameterSuppliersFromTypedData(errorsForCasesWithoutExamples(suppliers, (v, p) -> "numeric"));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Log(source, args.get(0), args.size() > 1 ? args.get(1) : null);
    }
}
