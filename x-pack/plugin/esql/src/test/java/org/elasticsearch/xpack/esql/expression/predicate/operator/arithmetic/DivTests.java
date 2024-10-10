/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class DivTests extends AbstractScalarFunctionTestCase {
    public DivTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        suppliers.addAll(
            TestCaseSupplier.forBinaryWithWidening(
                new TestCaseSupplier.NumericTypeTestConfigs<Number>(
                    new TestCaseSupplier.NumericTypeTestConfig<>(
                        (Integer.MIN_VALUE >> 1) - 1,
                        (Integer.MAX_VALUE >> 1) - 1,
                        (l, r) -> l.intValue() / r.intValue(),
                        "DivIntsEvaluator"
                    ),
                    new TestCaseSupplier.NumericTypeTestConfig<>(
                        (Long.MIN_VALUE >> 1) - 1,
                        (Long.MAX_VALUE >> 1) - 1,
                        (l, r) -> l.longValue() / r.longValue(),
                        "DivLongsEvaluator"
                    ),
                    new TestCaseSupplier.NumericTypeTestConfig<>(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, (l, r) -> {
                        double v = l.doubleValue() / r.doubleValue();
                        if (Double.isFinite(v)) {
                            return v;
                        }
                        return null;
                    }, "DivDoublesEvaluator")
                ),
                "lhs",
                "rhs",
                (lhs, rhs) -> {
                    if (lhs.type() != DataType.DOUBLE || rhs.type() != DataType.DOUBLE) {
                        return List.of();
                    }
                    double v = ((Double) lhs.getValue()) / ((Double) rhs.getValue());
                    if (Double.isFinite(v)) {
                        return List.of();
                    }
                    return List.of(
                        "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                        "Line -1:-1: java.lang.ArithmeticException: / by zero"
                    );
                },
                false
            )
        );
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                "DivUnsignedLongsEvaluator",
                "lhs",
                "rhs",
                (l, r) -> (((BigInteger) l).divide((BigInteger) r)),
                DataType.UNSIGNED_LONG,
                TestCaseSupplier.ulongCases(BigInteger.ZERO, BigInteger.valueOf(Long.MAX_VALUE), true),
                TestCaseSupplier.ulongCases(BigInteger.ONE, BigInteger.valueOf(Long.MAX_VALUE), true),
                List.of(),
                false
            )
        );

        suppliers = errorsForCasesWithoutExamples(anyNullIsNull(true, suppliers), DivTests::divErrorMessageString);

        // Divide by zero cases - all of these should warn and return null
        TestCaseSupplier.NumericTypeTestConfigs<Number> typeStuff = new TestCaseSupplier.NumericTypeTestConfigs<>(
            new TestCaseSupplier.NumericTypeTestConfig<>(
                (Integer.MIN_VALUE >> 1) - 1,
                (Integer.MAX_VALUE >> 1) - 1,
                (l, r) -> null,
                "DivIntsEvaluator"
            ),
            new TestCaseSupplier.NumericTypeTestConfig<>(
                (Long.MIN_VALUE >> 1) - 1,
                (Long.MAX_VALUE >> 1) - 1,
                (l, r) -> null,
                "DivLongsEvaluator"
            ),
            new TestCaseSupplier.NumericTypeTestConfig<>(
                Double.NEGATIVE_INFINITY,
                Double.POSITIVE_INFINITY,
                (l, r) -> null,
                "DivDoublesEvaluator"
            )
        );
        List<DataType> numericTypes = List.of(DataType.INTEGER, DataType.LONG, DataType.DOUBLE);

        for (DataType lhsType : numericTypes) {
            for (DataType rhsType : numericTypes) {
                DataType expected = TestCaseSupplier.widen(lhsType, rhsType);
                TestCaseSupplier.NumericTypeTestConfig<Number> expectedTypeStuff = typeStuff.get(expected);
                BiFunction<DataType, DataType, Matcher<String>> evaluatorToString = (lhs, rhs) -> equalTo(
                    expectedTypeStuff.evaluatorName()
                        + "["
                        + "lhs"
                        + "="
                        + TestCaseSupplier.getCastEvaluator("Attribute[channel=0]", lhs, expected)
                        + ", "
                        + "rhs"
                        + "="
                        + TestCaseSupplier.getCastEvaluator("Attribute[channel=1]", rhs, expected)
                        + "]"
                );
                TestCaseSupplier.casesCrossProduct(
                    (l1, r1) -> expectedTypeStuff.expected().apply((Number) l1, (Number) r1),
                    TestCaseSupplier.getSuppliersForNumericType(lhsType, expectedTypeStuff.min(), expectedTypeStuff.max(), true),
                    TestCaseSupplier.getSuppliersForNumericType(rhsType, 0, 0, true),
                    evaluatorToString,
                    (lhs, rhs) -> List.of(
                        "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                        "Line -1:-1: java.lang.ArithmeticException: / by zero"
                    ),
                    suppliers,
                    expected,
                    false
                );
            }
        }

        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                "DivUnsignedLongsEvaluator",
                "lhs",
                "rhs",
                (l, r) -> null,
                DataType.UNSIGNED_LONG,
                TestCaseSupplier.ulongCases(BigInteger.ZERO, BigInteger.valueOf(Long.MAX_VALUE), true),
                TestCaseSupplier.ulongCases(BigInteger.ZERO, BigInteger.ZERO, true),
                List.of(
                    "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                    "Line -1:-1: java.lang.ArithmeticException: / by zero"
                ),
                false
            )
        );

        return parameterSuppliersFromTypedData(suppliers);
    }

    private static String divErrorMessageString(boolean includeOrdinal, List<Set<DataType>> validPerPosition, List<DataType> types) {
        try {
            return typeErrorMessage(includeOrdinal, validPerPosition, types, (a, b) -> "numeric");
        } catch (IllegalStateException e) {
            // This means all the positional args were okay, so the expected error is from the combination
            return "[/] has arguments with incompatible types [" + types.get(0).typeName() + "] and [" + types.get(1).typeName() + "]";

        }
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Div(source, args.get(0), args.get(1));
    }
}
