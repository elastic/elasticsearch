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

public class ModTests extends AbstractScalarFunctionTestCase {
    public ModTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
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
                        (l, r) -> l.intValue() % r.intValue(),
                        "ModIntsEvaluator"
                    ),
                    new TestCaseSupplier.NumericTypeTestConfig<>(
                        (Long.MIN_VALUE >> 1) - 1,
                        (Long.MAX_VALUE >> 1) - 1,
                        (l, r) -> l.longValue() % r.longValue(),
                        "ModLongsEvaluator"
                    ),
                    new TestCaseSupplier.NumericTypeTestConfig<>(
                        Double.NEGATIVE_INFINITY,
                        Double.POSITIVE_INFINITY,
                        (l, r) -> l.doubleValue() % r.doubleValue(),
                        "ModDoublesEvaluator"
                    )
                ),
                "lhs",
                "rhs",
                (lhs, rhs) -> List.of(),
                false
            )
        );
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                "ModUnsignedLongsEvaluator",
                "lhs",
                "rhs",
                (l, r) -> (((BigInteger) l).mod((BigInteger) r)),
                DataType.UNSIGNED_LONG,
                TestCaseSupplier.ulongCases(BigInteger.ZERO, BigInteger.valueOf(Long.MAX_VALUE), true),
                TestCaseSupplier.ulongCases(BigInteger.ONE, BigInteger.valueOf(Long.MAX_VALUE), true),
                List.of(),
                false
            )
        );

        // Divide by zero cases - all of these should warn and return null
        TestCaseSupplier.NumericTypeTestConfigs<Number> typeStuff = new TestCaseSupplier.NumericTypeTestConfigs<>(
            new TestCaseSupplier.NumericTypeTestConfig<>(
                (Integer.MIN_VALUE >> 1) - 1,
                (Integer.MAX_VALUE >> 1) - 1,
                (l, r) -> null,
                "ModIntsEvaluator"
            ),
            new TestCaseSupplier.NumericTypeTestConfig<>(
                (Long.MIN_VALUE >> 1) - 1,
                (Long.MAX_VALUE >> 1) - 1,
                (l, r) -> null,
                "ModLongsEvaluator"
            ),
            new TestCaseSupplier.NumericTypeTestConfig<>(
                Double.NEGATIVE_INFINITY,
                Double.POSITIVE_INFINITY,
                (l, r) -> null,
                "ModDoublesEvaluator"
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
                        "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                        "Line 1:1: java.lang.ArithmeticException: / by zero"
                    ),
                    suppliers,
                    expected,
                    false
                );
            }
        }

        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                "ModUnsignedLongsEvaluator",
                "lhs",
                "rhs",
                (l, r) -> null,
                DataType.UNSIGNED_LONG,
                TestCaseSupplier.ulongCases(BigInteger.ZERO, BigInteger.valueOf(Long.MAX_VALUE), true),
                TestCaseSupplier.ulongCases(BigInteger.ZERO, BigInteger.ZERO, true),
                List.of(
                    "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                    "Line 1:1: java.lang.ArithmeticException: / by zero"
                ),
                false
            )
        );

        suppliers = errorsForCasesWithoutExamples(anyNullIsNull(true, suppliers), ModTests::modErrorMessageString);

        // Cannot use parameterSuppliersFromTypedDataWithDefaultChecks as error messages are non-trivial
        return parameterSuppliersFromTypedData(suppliers);
    }

    private static String modErrorMessageString(boolean includeOrdinal, List<Set<DataType>> validPerPosition, List<DataType> types) {
        try {
            return typeErrorMessage(includeOrdinal, validPerPosition, types, (a, b) -> "numeric");
        } catch (IllegalStateException e) {
            // This means all the positional args were okay, so the expected error is from the combination
            return "[%] has arguments with incompatible types [" + types.get(0).typeName() + "] and [" + types.get(1).typeName() + "]";

        }
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Mod(source, args.get(0), args.get(1));
    }
}
