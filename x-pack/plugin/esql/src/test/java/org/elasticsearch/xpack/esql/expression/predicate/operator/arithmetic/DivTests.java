/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DataTypeConverter;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.core.type.DataType.DENSE_VECTOR;
import static org.elasticsearch.xpack.esql.core.type.DataType.FLOAT;
import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.randomDenseVector;
import static org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.DenseVectorTestCaseHelper.denseVectorScalarCases;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

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
                    double rhsVal = (Double) rhs.getValue();
                    double v = ((Double) lhs.getValue()) / rhsVal;
                    if (Double.isFinite(v)) {
                        return List.of();
                    }
                    return List.of(
                        "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                        rhsVal == 0.0
                            ? "Line 1:1: java.lang.ArithmeticException: / by zero"
                            : "Line 1:1: java.lang.ArithmeticException: not a finite double number: " + v
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
                "DivUnsignedLongsEvaluator",
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

        suppliers.add(new TestCaseSupplier(List.of(DENSE_VECTOR, DENSE_VECTOR), () -> {
            int dimensions = between(64, 128);
            List<Float> left = randomDenseVector(dimensions);
            List<Float> right = randomDenseVector(dimensions);
            List<Float> expected = divVectors(left, right);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(left, DENSE_VECTOR, "vector1"),
                    new TestCaseSupplier.TypedData(right, DENSE_VECTOR, "vector2")
                ),
                "DenseVectorsEvaluator[lhs=Attribute[channel=0], rhs=Attribute[channel=1], opName=Div]",
                DENSE_VECTOR,
                equalTo(expected)
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DENSE_VECTOR, DENSE_VECTOR), () -> {
            List<Float> left = randomDenseVector(64);
            List<Float> right = randomDenseVector(128);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(left, DENSE_VECTOR, "vector1"),
                    new TestCaseSupplier.TypedData(right, DENSE_VECTOR, "vector2")
                ),
                "DenseVectorsEvaluator[lhs=Attribute[channel=0], rhs=Attribute[channel=1], opName=Div]",
                DENSE_VECTOR,
                equalTo(null)
            ).withWarning("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.")
                .withWarning("Line 1:1: java.lang.IllegalArgumentException: dense_vector dimensions do not match");
        }));

        suppliers.add(new TestCaseSupplier(List.of(DENSE_VECTOR, DENSE_VECTOR), () -> {
            List<Float> left = randomDenseVector(64);
            List<Float> right = randomDenseVector(64);
            left.set(32, -Float.MAX_VALUE);
            right.set(32, 0.0f);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(left, DENSE_VECTOR, "vector1"),
                    new TestCaseSupplier.TypedData(right, DENSE_VECTOR, "vector2")
                ),
                "DenseVectorsEvaluator[lhs=Attribute[channel=0], rhs=Attribute[channel=1], opName=Div]",
                DENSE_VECTOR,
                equalTo(null)
            ).withWarning("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.")
                .withWarning("Line 1:1: java.lang.ArithmeticException: / by zero");
        }));

        suppliers.addAll(denseVectorScalarCases("Div", (v, s) -> {
            Float sf = (Float) DataTypeConverter.convert(s, FLOAT);
            List<Float> result = v.stream().map(f -> f / sf).toList();
            // If the scalar denominator is zero or any element overflows to Infinity/NaN,
            // the evaluator throws ArithmeticException and returns null for the whole vector
            return result.stream().anyMatch(f -> Float.isInfinite(f) || Float.isNaN(f)) ? null : result;
        }, (s, v) -> {
            Float sf = (Float) DataTypeConverter.convert(s, FLOAT);
            List<Float> result = v.stream().map(f -> sf / f).toList();
            // Large scalars (e.g. randomInt/randomLong) divided by near-zero vector elements
            // can overflow to Infinity; the evaluator throws and returns null for the whole vector
            return result.stream().anyMatch(f -> Float.isInfinite(f) || Float.isNaN(f)) ? null : result;
        },
            List.of(
                "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                "Line 1:1: java.lang.ArithmeticException: / by zero"
            )
        ));

        // Overflows
        suppliers.addAll(
            List.of(
                new TestCaseSupplier(
                    List.of(DataType.DOUBLE, DataType.DOUBLE),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(Double.MAX_VALUE, DataType.DOUBLE, "lhs"),
                            new TestCaseSupplier.TypedData(0.1, DataType.DOUBLE, "rhs")
                        ),
                        "DivDoublesEvaluator[lhs=Attribute[channel=0], rhs=Attribute[channel=1]]",
                        DataType.DOUBLE,
                        equalTo(null)
                    ).withWarning("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.")
                        .withWarning("Line 1:1: java.lang.ArithmeticException: not a finite double number: Infinity")
                ),
                new TestCaseSupplier(
                    List.of(DataType.DOUBLE, DataType.DOUBLE),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(-Double.MAX_VALUE, DataType.DOUBLE, "lhs"),
                            new TestCaseSupplier.TypedData(0.1, DataType.DOUBLE, "rhs")
                        ),
                        "DivDoublesEvaluator[lhs=Attribute[channel=0], rhs=Attribute[channel=1]]",
                        DataType.DOUBLE,
                        equalTo(null)
                    ).withWarning("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.")
                        .withWarning("Line 1:1: java.lang.ArithmeticException: not a finite double number: -Infinity")
                )
            )
        );

        // Constant-divisor fast path: when the right-hand side is a foldable scalar,
        // Div#toEvaluator dispatches to DivXxxByConstantEvaluator instead of the binary
        // evaluator. These cases verify the dispatch + Cast hookup for each numeric
        // common type, including the two widening combinations. Zero / unsigned_long
        // divisors fall back to the binary path and are covered by the upstream
        // forBinaryWithWidening cases above and the CSV-spec by-zero tests.
        suppliers.add(new TestCaseSupplier("Int / literal Int", List.of(DataType.INTEGER, DataType.INTEGER), () -> {
            int lhs = randomInt();
            int rhs = randomValueOtherThan(0, ESTestCase::randomInt);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(lhs, DataType.INTEGER, "lhs"),
                    new TestCaseSupplier.TypedData(rhs, DataType.INTEGER, "rhs").forceLiteral()
                ),
                startsWith("DivIntsByConstantEvaluator[lhs=Attribute[channel=0], rhs=" + rhs + "]"),
                DataType.INTEGER,
                equalTo(lhs / rhs)
            );
        }));
        suppliers.add(new TestCaseSupplier("Long / literal Long", List.of(DataType.LONG, DataType.LONG), () -> {
            long lhs = randomLong();
            long rhs = randomValueOtherThan(0L, ESTestCase::randomLong);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(lhs, DataType.LONG, "lhs"),
                    new TestCaseSupplier.TypedData(rhs, DataType.LONG, "rhs").forceLiteral()
                ),
                startsWith("DivLongsByConstantEvaluator[lhs=Attribute[channel=0], rhs=" + rhs + "]"),
                DataType.LONG,
                equalTo(lhs / rhs)
            );
        }));
        suppliers.add(new TestCaseSupplier("Long / literal Int", List.of(DataType.LONG, DataType.INTEGER), () -> {
            long lhs = randomLong();
            int rhs = randomValueOtherThan(0, ESTestCase::randomInt);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(lhs, DataType.LONG, "lhs"),
                    new TestCaseSupplier.TypedData(rhs, DataType.INTEGER, "rhs").forceLiteral()
                ),
                startsWith("DivLongsByConstantEvaluator[lhs=Attribute[channel=0], rhs=" + (long) rhs + "]"),
                DataType.LONG,
                equalTo(lhs / (long) rhs)
            );
        }));
        suppliers.add(new TestCaseSupplier("Int / literal Long", List.of(DataType.INTEGER, DataType.LONG), () -> {
            int lhs = randomInt();
            long rhs = randomValueOtherThan(0L, ESTestCase::randomLong);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(lhs, DataType.INTEGER, "lhs"),
                    new TestCaseSupplier.TypedData(rhs, DataType.LONG, "rhs").forceLiteral()
                ),
                startsWith("DivLongsByConstantEvaluator[lhs=CastIntToLongEvaluator[v=Attribute[channel=0]], rhs=" + rhs + "]"),
                DataType.LONG,
                equalTo((long) lhs / rhs)
            );
        }));
        suppliers.add(new TestCaseSupplier("Double / literal Double", List.of(DataType.DOUBLE, DataType.DOUBLE), () -> {
            double lhs = randomDoubleBetween(-1e9, 1e9, true);
            double rhs = randomValueOtherThan(0.0d, () -> randomDoubleBetween(-1e6, 1e6, false));
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(lhs, DataType.DOUBLE, "lhs"),
                    new TestCaseSupplier.TypedData(rhs, DataType.DOUBLE, "rhs").forceLiteral()
                ),
                startsWith("DivDoublesByConstantEvaluator[lhs=Attribute[channel=0], rhs=" + rhs + "]"),
                DataType.DOUBLE,
                equalTo(lhs / rhs)
            );
        }));

        suppliers = anyNullIsNull(true, suppliers);

        // Cannot use parameterSuppliersFromTypedDataWithDefaultChecks as error messages are non-trivial
        return parameterSuppliersFromTypedData(suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Div(source, args.get(0), args.get(1));
    }

    private static List<Float> divVectors(List<Float> left, List<Float> right) {
        List<Float> result = new ArrayList<>();
        for (int i = 0; i < left.size(); i++) {
            result.add(left.get(i) / right.get(i));
        }
        return result;
    }
}
