/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.math.Maths;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class RoundTests extends AbstractScalarFunctionTestCase {
    public RoundTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        suppliers.add(
            supplier(
                "<double>",
                DataType.DOUBLE,
                () -> 1 / randomDouble(),
                "RoundDoubleNoDecimalsEvaluator[val=Attribute[channel=0]]",
                d -> Maths.round(d, 0)
            )
        );
        suppliers.add(
            supplier(
                "<double>, <integer>",
                DataType.DOUBLE,
                () -> 1 / randomDouble(),
                DataType.INTEGER,
                () -> between(-30, 30),
                "RoundDoubleEvaluator[val=Attribute[channel=0], decimals=CastIntToLongEvaluator[v=Attribute[channel=1]]]",
                Maths::round
            )
        );
        // TODO randomized cases for more types
        // TODO errorsForCasesWithoutExamples
        suppliers = anyNullIsNull(
            suppliers,
            (nullPosition, nullValueDataType, original) -> nullPosition == 0 ? nullValueDataType : original.expectedType(),
            (nullPosition, nullData, original) -> original
        );

        suppliers.add(new TestCaseSupplier("two doubles", List.of(DataType.DOUBLE, DataType.INTEGER), () -> {
            double number1 = 1 / randomDouble();
            double number2 = 1 / randomDouble();
            int precision = between(-30, 30);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(List.of(number1, number2), DataType.DOUBLE, "number"),
                    new TestCaseSupplier.TypedData(precision, DataType.INTEGER, "decimals")
                ),
                "RoundDoubleEvaluator[val=Attribute[channel=0], decimals=CastIntToLongEvaluator[v=Attribute[channel=1]]]",
                DataType.DOUBLE,
                is(nullValue())
            ).withWarning("Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.")
                .withWarning("Line -1:-1: java.lang.IllegalArgumentException: single-value function encountered multi-value");
        }));

        // Integer or Long without a decimals parameter is a noop
        suppliers.add(supplier("<integer>", DataType.INTEGER, ESTestCase::randomInt, "Attribute[channel=0]", Function.identity()));
        suppliers.add(supplier("<long>", DataType.LONG, ESTestCase::randomLong, "Attribute[channel=0]", Function.identity()));
        suppliers.add(
            supplier(
                "<unsigned_long>",
                DataType.UNSIGNED_LONG,
                ESTestCase::randomLong,
                "Attribute[channel=0]",
                NumericUtils::unsignedLongAsBigInteger
            )
        );

        suppliers.add(supplier(0, 0));
        suppliers.add(supplier(123.45, 123));
        suppliers.add(supplier(0, 0, 0));
        suppliers.add(supplier(123.45, 0, 123));
        suppliers.add(supplier(123.45, 1, 123.5));
        suppliers.add(supplier(999.0, -1, 1000.0));
        suppliers.add(supplier(12350.0, -2, 12400.0));
        suppliers.add(supplier(12349.0, -2, 12300.0));
        suppliers.add(supplier(-12350.0, -2, -12400.0));
        suppliers.add(supplier(-123.45, -1, -120.0));
        suppliers.add(supplier(-123.45, 1, -123.5));
        suppliers.add(supplier(-123.5, 0, -124.0));
        suppliers.add(supplier(-123.45, -123.0));
        suppliers.add(supplier(123.456, Integer.MAX_VALUE, 123.456));
        suppliers.add(supplier(123.456, Integer.MIN_VALUE, 0.0));

        suppliers.add(supplier(123L, 0, 123));
        suppliers.add(supplier(123L, 5, 123));
        suppliers.add(supplier(123L, -1, 120));
        suppliers.add(supplier(123L, -2, 100));
        suppliers.add(supplier(123L, -3, 0));
        suppliers.add(supplier(123L, -100, 0));
        suppliers.add(supplier(999L, -1, 1000));
        suppliers.add(supplier(-123L, -2, -100));
        suppliers.add(supplier(125L, -1, 130));
        suppliers.add(supplier(12350L, -2, 12400));
        suppliers.add(supplier(-12349L, -2, -12300));
        suppliers.add(supplier(-12350L, -2, -12400));
        suppliers.add(supplier(Long.MAX_VALUE, 5, Long.MAX_VALUE));
        suppliers.add(supplier(Long.MIN_VALUE, 5, Long.MIN_VALUE));

        suppliers.add(supplier(0, 0, 0));
        suppliers.add(supplier(123, 2, 123));
        suppliers.add(supplier(123, -1, 120));
        return parameterSuppliersFromTypedData(suppliers);
    }

    private static TestCaseSupplier supplier(double v, double expected) {
        return supplier(
            "round(" + v + ") -> " + expected,
            DataType.DOUBLE,
            () -> v,
            "RoundDoubleNoDecimalsEvaluator[val=Attribute[channel=0]]",
            value -> expected
        );
    }

    private static TestCaseSupplier supplier(double v, int decimals, double expected) {
        return supplier(
            "round(" + v + ", " + decimals + ") -> " + expected,
            DataType.DOUBLE,
            () -> v,
            DataType.INTEGER,
            () -> decimals,
            "RoundDoubleEvaluator[val=Attribute[channel=0], decimals=CastIntToLongEvaluator[v=Attribute[channel=1]]]",
            (value, de) -> expected
        );
    }

    private static TestCaseSupplier supplier(long v, int decimals, long expected) {
        return supplier(
            "round(" + v + "L, " + decimals + ") -> " + expected,
            DataType.LONG,
            () -> v,
            DataType.INTEGER,
            () -> decimals,
            "RoundLongEvaluator[val=Attribute[channel=0], decimals=CastIntToLongEvaluator[v=Attribute[channel=1]]]",
            (value, de) -> expected
        );
    }

    private static TestCaseSupplier supplier(int v, int decimals, int expected) {
        return supplier(
            "round(" + v + ", " + decimals + ") -> " + expected,
            DataType.INTEGER,
            () -> v,
            DataType.INTEGER,
            () -> decimals,
            "RoundIntEvaluator[val=Attribute[channel=0], decimals=CastIntToLongEvaluator[v=Attribute[channel=1]]]",
            (value, de) -> expected
        );
    }

    private static <N> TestCaseSupplier supplier(
        String name,
        DataType numberType,
        Supplier<N> numberSupplier,
        String expectedEvaluatorName,
        Function<N, ? extends Number> expected
    ) {
        return new TestCaseSupplier(name, List.of(numberType), () -> {
            N number = numberSupplier.get();
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(number, numberType, "number")),
                expectedEvaluatorName,
                numberType,
                equalTo(expected.apply(number))
            );
        });
    }

    private static <N, D> TestCaseSupplier supplier(
        String name,
        DataType numberType,
        Supplier<N> numberSupplier,
        DataType decimalsType,
        Supplier<D> decimalsSupplier,
        String expectedEvaluatorName,
        BiFunction<N, D, ? extends Number> expected
    ) {
        return new TestCaseSupplier(name, List.of(numberType, decimalsType), () -> {
            N number = numberSupplier.get();
            D decimals = decimalsSupplier.get();
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(number, numberType, "number"),
                    new TestCaseSupplier.TypedData(decimals, decimalsType, "decimals")
                ),
                expectedEvaluatorName,
                numberType,
                equalTo(expected.apply(number, decimals))
            );
        });
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Round(source, args.get(0), args.size() < 2 ? null : args.get(1));
    }
}
