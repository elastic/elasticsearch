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
import org.elasticsearch.xpack.esql.core.type.DataTypeConverter;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.core.type.DataType.DENSE_VECTOR;
import static org.elasticsearch.xpack.esql.core.type.DataType.FLOAT;
import static org.elasticsearch.xpack.esql.core.util.NumericUtils.asLongUnsigned;
import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.randomDenseVector;
import static org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.DenseVectorTestCaseHelper.denseVectorScalarCases;
import static org.hamcrest.Matchers.equalTo;

public class MulTests extends AbstractScalarFunctionTestCase {
    public MulTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        suppliers.addAll(
            TestCaseSupplier.forBinaryWithWidening(
                new TestCaseSupplier.NumericTypeTestConfigs<Number>(
                    new TestCaseSupplier.NumericTypeTestConfig<>(-255, 255, (l, r) -> l.intValue() * r.intValue(), "MulIntsEvaluator"),
                    new TestCaseSupplier.NumericTypeTestConfig<>(
                        -1024L,
                        1024L,
                        (l, r) -> l.longValue() * r.longValue(),
                        "MulLongsEvaluator"
                    ),
                    new TestCaseSupplier.NumericTypeTestConfig<>(
                        -1024D,
                        1024D,
                        (l, r) -> l.doubleValue() * r.doubleValue(),
                        "MulDoublesEvaluator"
                    )
                ),
                "lhs",
                "rhs",
                (lhs, rhs) -> List.of(),
                true
            )
        );

        // Double
        suppliers.addAll(List.of(new TestCaseSupplier("Double * Double", List.of(DataType.DOUBLE, DataType.DOUBLE), () -> {
            double rhs = randomDouble();
            double lhs = randomDouble();
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(lhs, DataType.DOUBLE, "lhs"),
                    new TestCaseSupplier.TypedData(rhs, DataType.DOUBLE, "rhs")
                ),
                "MulDoublesEvaluator[lhs=Attribute[channel=0], rhs=Attribute[channel=1]]",
                DataType.DOUBLE,
                equalTo(lhs * rhs)
            );
        }),

            // Overflows
            new TestCaseSupplier(
                List.of(DataType.DOUBLE, DataType.DOUBLE),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(Double.MAX_VALUE, DataType.DOUBLE, "lhs"),
                        new TestCaseSupplier.TypedData(Double.MAX_VALUE, DataType.DOUBLE, "rhs")
                    ),
                    "MulDoublesEvaluator[lhs=Attribute[channel=0], rhs=Attribute[channel=1]]",
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
                        new TestCaseSupplier.TypedData(Double.MAX_VALUE, DataType.DOUBLE, "rhs")
                    ),
                    "MulDoublesEvaluator[lhs=Attribute[channel=0], rhs=Attribute[channel=1]]",
                    DataType.DOUBLE,
                    equalTo(null)
                ).withWarning("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.")
                    .withWarning("Line 1:1: java.lang.ArithmeticException: not a finite double number: -Infinity")
            )
        ));

        suppliers.add(
            arithmeticExceptionOverflowCase(
                DataType.INTEGER,
                () -> randomBoolean() ? Integer.MIN_VALUE : Integer.MAX_VALUE,
                () -> randomIntBetween(2, Integer.MAX_VALUE),
                "MulIntsEvaluator"
            )
        );
        suppliers.add(
            arithmeticExceptionOverflowCase(
                DataType.LONG,
                () -> randomBoolean() ? Long.MIN_VALUE : Long.MAX_VALUE,
                () -> randomLongBetween(2L, Long.MAX_VALUE),
                "MulLongsEvaluator"
            )
        );
        suppliers.add(
            arithmeticExceptionOverflowCase(
                DataType.UNSIGNED_LONG,
                () -> asLongUnsigned(UNSIGNED_LONG_MAX),
                () -> asLongUnsigned(randomLongBetween(-Long.MAX_VALUE, Long.MAX_VALUE)),
                "MulUnsignedLongsEvaluator"
            )
        );

        suppliers.add(new TestCaseSupplier(List.of(DENSE_VECTOR, DENSE_VECTOR), () -> {
            int dimensions = between(64, 128);
            List<Float> left = randomDenseVector(dimensions);
            List<Float> right = randomDenseVector(dimensions);
            List<Float> expected = mulVectors(left, right);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(left, DENSE_VECTOR, "vector1"),
                    new TestCaseSupplier.TypedData(right, DENSE_VECTOR, "vector2")
                ),
                "DenseVectorsEvaluator[lhs=Attribute[channel=0], rhs=Attribute[channel=1], opName=Mul]",
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
                "DenseVectorsEvaluator[lhs=Attribute[channel=0], rhs=Attribute[channel=1], opName=Mul]",
                DENSE_VECTOR,
                equalTo(null)
            ).withWarning("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.")
                .withWarning("Line 1:1: java.lang.IllegalArgumentException: dense_vector dimensions do not match");
        }));

        suppliers.add(new TestCaseSupplier(List.of(DENSE_VECTOR, DENSE_VECTOR), () -> {
            List<Float> left = randomDenseVector(64);
            List<Float> right = randomDenseVector(64);
            left.set(32, Float.MAX_VALUE);
            right.set(32, Float.MAX_VALUE);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(left, DENSE_VECTOR, "vector1"),
                    new TestCaseSupplier.TypedData(right, DENSE_VECTOR, "vector2")
                ),
                "DenseVectorsEvaluator[lhs=Attribute[channel=0], rhs=Attribute[channel=1], opName=Mul]",
                DENSE_VECTOR,
                equalTo(null)
            ).withWarning("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.")
                .withWarning("Line 1:1: java.lang.ArithmeticException: not a finite float number: Infinity");
        }));

        suppliers.addAll(
            denseVectorScalarCases(
                "Mul",
                (v, s) -> v.stream().map(f -> f * (Float) DataTypeConverter.convert(s, FLOAT)).toList(),
                (s, v) -> v.stream().map(f -> (Float) DataTypeConverter.convert(s, FLOAT) * f).toList()
            )
        );

        suppliers = errorsForCasesWithoutExamples(anyNullIsNull(true, suppliers), MulTests::mulErrorMessageString);

        // Cannot use parameterSuppliersFromTypedDataWithDefaultChecks as error messages are non-trivial
        return parameterSuppliersFromTypedData(suppliers);
    }

    private static String mulErrorMessageString(boolean includeOrdinal, List<Set<DataType>> validPerPosition, List<DataType> types) {
        try {
            return typeErrorMessage(includeOrdinal, validPerPosition, types, (a, b) -> "numeric or dense_vector");
        } catch (IllegalStateException e) {
            // This means all the positional args were okay, so the expected error is from the combination
            return "[*] has arguments with incompatible types [" + types.get(0).typeName() + "] and [" + types.get(1).typeName() + "]";

        }
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Mul(source, args.get(0), args.get(1));
    }

    private static List<Float> mulVectors(List<Float> left, List<Float> right) {
        List<Float> result = new ArrayList<>();
        for (int i = 0; i < left.size(); i++) {
            result.add(left.get(i) * right.get(i));
        }
        return result;
    }
}
