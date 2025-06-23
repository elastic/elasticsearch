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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class RoundTests extends AbstractScalarFunctionTestCase {
    public RoundTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        // Double field
        suppliers.add(
            supplier(
                "<double>",
                DataType.DOUBLE,
                () -> randomDoubleBetween(-Double.MAX_VALUE, Double.MAX_VALUE, true),
                "RoundDoubleNoDecimalsEvaluator[val=Attribute[channel=0]]",
                d -> Maths.round(d, 0)
            )
        );
        suppliers.add(
            supplier(
                "<double>, <integer>",
                DataType.DOUBLE,
                () -> randomDoubleBetween(-Double.MAX_VALUE, Double.MAX_VALUE, true),
                DataType.INTEGER,
                () -> between(-30, 30),
                "RoundDoubleEvaluator[val=Attribute[channel=0], decimals=CastIntToLongEvaluator[v=Attribute[channel=1]]]",
                Maths::round
            )
        );
        suppliers.add(
            supplier(
                "<double>, <long>",
                DataType.DOUBLE,
                () -> randomDoubleBetween(-Double.MAX_VALUE, Double.MAX_VALUE, true),
                DataType.LONG,
                () -> randomLongBetween(-30, 30),
                "RoundDoubleEvaluator[val=Attribute[channel=0], decimals=Attribute[channel=1]]",
                Maths::round
            )
        );

        // Long decimals
        suppliers.add(
            supplier(
                "<integer>, <long>",
                DataType.INTEGER,
                ESTestCase::randomInt,
                DataType.LONG,
                ESTestCase::randomLong,
                "RoundIntEvaluator[val=Attribute[channel=0], decimals=Attribute[channel=1]]",
                (n, d) -> Maths.round((Number) n, d)
            )
        );
        suppliers.add(
            supplier(
                "<long>, <long>",
                DataType.LONG,
                ESTestCase::randomLong,
                DataType.LONG,
                ESTestCase::randomLong,
                "RoundLongEvaluator[val=Attribute[channel=0], decimals=Attribute[channel=1]]",
                (n, d) -> Maths.round((Number) n, d)
            )
        );
        suppliers.add(
            supplier(
                "<unsigned_long>, <long>",
                DataType.UNSIGNED_LONG,
                ESTestCase::randomLong,
                DataType.LONG,
                // Safe negative integer to not trigger an exception and not slow down the test
                () -> randomLongBetween(-10_000, Long.MAX_VALUE),
                "RoundUnsignedLongEvaluator[val=Attribute[channel=0], decimals=Attribute[channel=1]]",
                (n, d) -> Maths.round(NumericUtils.unsignedLongAsBigInteger(n), d)
            )
        );

        // Integer decimals
        suppliers.add(
            supplier(
                "<integer>, <integer>",
                DataType.INTEGER,
                ESTestCase::randomInt,
                DataType.INTEGER,
                ESTestCase::randomInt,
                "RoundIntEvaluator[val=Attribute[channel=0], decimals=CastIntToLongEvaluator[v=Attribute[channel=1]]]",
                (n, d) -> Maths.round((Number) n, d)
            )
        );
        suppliers.add(
            supplier(
                "<long>, <integer>",
                DataType.LONG,
                ESTestCase::randomLong,
                DataType.INTEGER,
                ESTestCase::randomInt,
                "RoundLongEvaluator[val=Attribute[channel=0], decimals=CastIntToLongEvaluator[v=Attribute[channel=1]]]",
                (n, d) -> Maths.round((Number) n, d)
            )
        );
        suppliers.add(
            supplier(
                "<unsigned_long>, <integer>",
                DataType.UNSIGNED_LONG,
                ESTestCase::randomLong,
                DataType.INTEGER,
                // Safe negative integer to not trigger an exception and not slow down the test
                () -> randomIntBetween(-10_000, Integer.MAX_VALUE),
                "RoundUnsignedLongEvaluator[val=Attribute[channel=0], decimals=CastIntToLongEvaluator[v=Attribute[channel=1]]]",
                (n, d) -> Maths.round(NumericUtils.unsignedLongAsBigInteger(n), d)
            )
        );

        // Unsigned long errors
        suppliers.add(
            new TestCaseSupplier(
                "<big unsigned_long>, <negative long out of integer range>",
                List.of(DataType.UNSIGNED_LONG, DataType.LONG),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(new BigInteger("18446744073709551615"), DataType.UNSIGNED_LONG, "number"),
                        new TestCaseSupplier.TypedData(-9223372036854775808L, DataType.LONG, "decimals")
                    ),
                    "RoundUnsignedLongEvaluator[val=Attribute[channel=0], decimals=Attribute[channel=1]]",
                    DataType.UNSIGNED_LONG,
                    equalTo(BigInteger.ZERO)
                )
            )
        );
        suppliers.add(
            new TestCaseSupplier(
                "<max unsigned_long>, <negative long in integer range>",
                List.of(DataType.UNSIGNED_LONG, DataType.LONG),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(new BigInteger("18446744073709551615"), DataType.UNSIGNED_LONG, "number"),
                        new TestCaseSupplier.TypedData(-2147483647L, DataType.LONG, "decimals")
                    ),
                    "RoundUnsignedLongEvaluator[val=Attribute[channel=0], decimals=Attribute[channel=1]]",
                    DataType.UNSIGNED_LONG,
                    equalTo(BigInteger.ZERO)
                )
            )
        );
        suppliers.add(
            new TestCaseSupplier(
                "<max unsigned_long>, <-20>",
                List.of(DataType.UNSIGNED_LONG, DataType.LONG),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(new BigInteger("18446744073709551615"), DataType.UNSIGNED_LONG, "number"),
                        new TestCaseSupplier.TypedData(-20L, DataType.LONG, "decimals")
                    ),
                    "RoundUnsignedLongEvaluator[val=Attribute[channel=0], decimals=Attribute[channel=1]]",
                    DataType.UNSIGNED_LONG,
                    equalTo(BigInteger.ZERO)
                )
            )
        );
        suppliers.add(
            new TestCaseSupplier(
                "<max unsigned_long>, <-19>",
                List.of(DataType.UNSIGNED_LONG, DataType.LONG),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(new BigInteger("18446744073709551615"), DataType.UNSIGNED_LONG, "number"),
                        new TestCaseSupplier.TypedData(-19L, DataType.LONG, "decimals")
                    ),
                    "RoundUnsignedLongEvaluator[val=Attribute[channel=0], decimals=Attribute[channel=1]]",
                    DataType.UNSIGNED_LONG,
                    equalTo(null)
                ).withWarning("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.")
                    .withWarning("Line 1:1: java.lang.ArithmeticException: unsigned_long overflow")
            )
        );
        suppliers.add(
            new TestCaseSupplier(
                "<big unsigned_long>, <-19>",
                List.of(DataType.UNSIGNED_LONG, DataType.LONG),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(new BigInteger("14446744073709551615"), DataType.UNSIGNED_LONG, "number"),
                        new TestCaseSupplier.TypedData(-19L, DataType.LONG, "decimals")
                    ),
                    "RoundUnsignedLongEvaluator[val=Attribute[channel=0], decimals=Attribute[channel=1]]",
                    DataType.UNSIGNED_LONG,
                    equalTo(new BigInteger("10000000000000000000"))
                )
            )
        );

        // Max longs and overflows
        suppliers.add(
            new TestCaseSupplier(
                "<max long>, <-20>",
                List.of(DataType.LONG, DataType.LONG),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(Long.MAX_VALUE, DataType.LONG, "number"),
                        new TestCaseSupplier.TypedData(-20L, DataType.LONG, "decimals")
                    ),
                    "RoundLongEvaluator[val=Attribute[channel=0], decimals=Attribute[channel=1]]",
                    DataType.LONG,
                    equalTo(0L)
                )
            )
        );
        suppliers.add(
            new TestCaseSupplier(
                "<max long>, <-19>",
                List.of(DataType.LONG, DataType.LONG),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(Long.MAX_VALUE, DataType.LONG, "number"),
                        new TestCaseSupplier.TypedData(-19L, DataType.LONG, "decimals")
                    ),
                    "RoundLongEvaluator[val=Attribute[channel=0], decimals=Attribute[channel=1]]",
                    DataType.LONG,
                    equalTo(0L)
                )
            )
        );
        suppliers.add(
            new TestCaseSupplier(
                "<big long>, <-18>",
                List.of(DataType.LONG, DataType.LONG),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(Long.MAX_VALUE, DataType.LONG, "number"),
                        new TestCaseSupplier.TypedData(-18L, DataType.LONG, "decimals")
                    ),
                    "RoundLongEvaluator[val=Attribute[channel=0], decimals=Attribute[channel=1]]",
                    DataType.LONG,
                    equalTo(9000000000000000000L)
                )
            )
        );
        // Max integers and overflows
        suppliers.add(
            new TestCaseSupplier(
                "<max integer>, <-10>",
                List.of(DataType.INTEGER, DataType.LONG),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(Integer.MAX_VALUE, DataType.INTEGER, "number"),
                        new TestCaseSupplier.TypedData(-10L, DataType.LONG, "decimals")
                    ),
                    "RoundIntEvaluator[val=Attribute[channel=0], decimals=Attribute[channel=1]]",
                    DataType.INTEGER,
                    equalTo(0)
                )
            )
        );
        suppliers.add(
            new TestCaseSupplier(
                "<max integer>, <-9>",
                List.of(DataType.INTEGER, DataType.LONG),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(Integer.MAX_VALUE, DataType.INTEGER, "number"),
                        new TestCaseSupplier.TypedData(-9L, DataType.LONG, "decimals")
                    ),
                    "RoundIntEvaluator[val=Attribute[channel=0], decimals=Attribute[channel=1]]",
                    DataType.INTEGER,
                    equalTo(2000000000)
                )
            )
        );

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

        return parameterSuppliersFromTypedDataWithDefaultChecksNoErrors(
            (nullPosition, nullValueDataType, original) -> nullPosition == 0 ? nullValueDataType : original.expectedType(),
            (nullPosition, nullData, original) -> original,
            suppliers
        );
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
