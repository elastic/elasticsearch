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
import org.elasticsearch.xpack.esql.expression.function.UnaryTestCaseHelper;

import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.unary;

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
        UnaryTestCaseHelper helper = unary().expectedOutputType(DataType.DOUBLE).evaluatorToString("LogConstantEvaluator[value=%0]");
        helper.expectedFromDouble(Math::log).castingToDouble(Math.nextUp(1d), Double.POSITIVE_INFINITY, false, suppliers);
        helper.expectedFromDouble(Math::log).doubles(Math.nextUp(0d), Math.nextDown(1d)).build(suppliers);

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
                    "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                    "Line 1:1: java.lang.ArithmeticException: Log of non-positive number"
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
                    "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                    "Line 1:1: java.lang.ArithmeticException: Log of non-positive number"
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
                    "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                    "Line 1:1: java.lang.ArithmeticException: Log of base 1"
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
        helper.expectNullAndWarnings(o -> List.of("Line 1:1: java.lang.ArithmeticException: Log of non-positive number"))
            .castingToDouble(Double.NEGATIVE_INFINITY, 0d, true, suppliers);

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Log(source, args.get(0), args.size() > 1 ? args.get(1) : null);
    }
}
