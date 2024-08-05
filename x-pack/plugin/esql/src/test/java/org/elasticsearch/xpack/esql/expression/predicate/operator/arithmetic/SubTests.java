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

import java.time.Duration;
import java.time.Period;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.core.type.DateUtils.asDateTime;
import static org.elasticsearch.xpack.esql.core.type.DateUtils.asMillis;
import static org.elasticsearch.xpack.esql.core.util.NumericUtils.ZERO_AS_UNSIGNED_LONG;
import static org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.AbstractArithmeticTestCase.arithmeticExceptionOverflowCase;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class SubTests extends AbstractScalarFunctionTestCase {
    public SubTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {

        List<TestCaseSupplier> suppliers = TestCaseSupplier.forBinaryWithWidening(
            new TestCaseSupplier.NumericTypeTestConfigs<Number>(
                new TestCaseSupplier.NumericTypeTestConfig<>(
                    (Integer.MIN_VALUE >> 1) - 1,
                    (Integer.MAX_VALUE >> 1) - 1,
                    (l, r) -> l.intValue() - r.intValue(),
                    "SubIntsEvaluator"
                ),
                new TestCaseSupplier.NumericTypeTestConfig<>(
                    (Long.MIN_VALUE >> 1) - 1,
                    (Long.MAX_VALUE >> 1) - 1,
                    (l, r) -> l.longValue() - r.longValue(),
                    "SubLongsEvaluator"
                ),
                new TestCaseSupplier.NumericTypeTestConfig<>(
                    Double.NEGATIVE_INFINITY,
                    Double.POSITIVE_INFINITY,
                    (l, r) -> l.doubleValue() - r.doubleValue(),
                    "SubDoublesEvaluator"
                )
            ),
            "lhs",
            "rhs",
            (lhs, rhs) -> List.of(),
            true
        );

        /* new TestCaseSupplier("ULong - ULong", () -> {
            // Ensure we don't have an overflow
            // TODO: we should be able to test values over Long.MAX_VALUE too...
            long rhs = randomLongBetween(0, (Long.MAX_VALUE >> 1) - 1);
            long lhs = randomLongBetween(0, (Long.MAX_VALUE >> 1) - 1);
            BigInteger lhsBI = unsignedLongAsBigInteger(lhs);
            BigInteger rhsBI = unsignedLongAsBigInteger(rhs);
            return new TestCase(
                Source.EMPTY,
                List.of(new TypedData(lhs, DataType.UNSIGNED_LONG, "lhs"), new TypedData(rhs, DataType.UNSIGNED_LONG, "rhs")),
                "SubUnsignedLongsEvaluator[lhs=Attribute[channel=0], rhs=Attribute[channel=1]]",
                equalTo(asLongUnsigned(lhsBI.subtract(rhsBI).longValue()))
            );
          }) */

        // Double overflows
        suppliers.addAll(
            List.of(
                new TestCaseSupplier(
                    List.of(DataType.DOUBLE, DataType.DOUBLE),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(Double.MAX_VALUE, DataType.DOUBLE, "lhs"),
                            new TestCaseSupplier.TypedData(-Double.MAX_VALUE, DataType.DOUBLE, "rhs")
                        ),
                        "SubDoublesEvaluator[lhs=Attribute[channel=0], rhs=Attribute[channel=1]]",
                        DataType.DOUBLE,
                        equalTo(null)
                    ).withWarning("Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.")
                        .withWarning("Line -1:-1: java.lang.ArithmeticException: not a finite double number: Infinity")
                ),
                new TestCaseSupplier(
                    List.of(DataType.DOUBLE, DataType.DOUBLE),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(-Double.MAX_VALUE, DataType.DOUBLE, "lhs"),
                            new TestCaseSupplier.TypedData(Double.MAX_VALUE, DataType.DOUBLE, "rhs")
                        ),
                        "SubDoublesEvaluator[lhs=Attribute[channel=0], rhs=Attribute[channel=1]]",
                        DataType.DOUBLE,
                        equalTo(null)
                    ).withWarning("Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.")
                        .withWarning("Line -1:-1: java.lang.ArithmeticException: not a finite double number: -Infinity")
                )
            )
        );

        suppliers.add(new TestCaseSupplier("Datetime - Period", () -> {
            long lhs = (Long) randomLiteral(DataType.DATETIME).value();
            Period rhs = (Period) randomLiteral(DataType.DATE_PERIOD).value();
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(lhs, DataType.DATETIME, "lhs"),
                    new TestCaseSupplier.TypedData(rhs, DataType.DATE_PERIOD, "rhs")
                ),
                "SubDatetimesEvaluator[lhs=Attribute[channel=0], rhs=Attribute[channel=1]]",
                DataType.DATETIME,
                equalTo(asMillis(asDateTime(lhs).minus(rhs)))
            );
        }));
        suppliers.add(new TestCaseSupplier("Period - Period", () -> {
            Period lhs = (Period) randomLiteral(DataType.DATE_PERIOD).value();
            Period rhs = (Period) randomLiteral(DataType.DATE_PERIOD).value();
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(lhs, DataType.DATE_PERIOD, "lhs"),
                    new TestCaseSupplier.TypedData(rhs, DataType.DATE_PERIOD, "rhs")
                ),
                "Only folding possible, so there's no evaluator",
                DataType.DATE_PERIOD,
                equalTo(lhs.minus(rhs))
            );
        }));
        suppliers.add(new TestCaseSupplier("Datetime - Duration", () -> {
            long lhs = (Long) randomLiteral(DataType.DATETIME).value();
            Duration rhs = (Duration) randomLiteral(DataType.TIME_DURATION).value();
            TestCaseSupplier.TestCase testCase = new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(lhs, DataType.DATETIME, "lhs"),
                    new TestCaseSupplier.TypedData(rhs, DataType.TIME_DURATION, "rhs")
                ),
                "SubDatetimesEvaluator[lhs=Attribute[channel=0], rhs=Attribute[channel=1]]",
                DataType.DATETIME,
                equalTo(asMillis(asDateTime(lhs).minus(rhs)))
            );
            return testCase;
        }));
        suppliers.add(new TestCaseSupplier("Duration - Duration", () -> {
            Duration lhs = (Duration) randomLiteral(DataType.TIME_DURATION).value();
            Duration rhs = (Duration) randomLiteral(DataType.TIME_DURATION).value();
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(lhs, DataType.TIME_DURATION, "lhs"),
                    new TestCaseSupplier.TypedData(rhs, DataType.TIME_DURATION, "rhs")
                ),
                "Only folding possible, so there's no evaluator",
                DataType.TIME_DURATION,
                equalTo(lhs.minus(rhs))
            );
        }));
        suppliers.add(new TestCaseSupplier("MV", () -> {
            // Ensure we don't have an overflow
            int rhs = randomIntBetween((Integer.MIN_VALUE >> 1) - 1, (Integer.MAX_VALUE >> 1) - 1);
            int lhs = randomIntBetween((Integer.MIN_VALUE >> 1) - 1, (Integer.MAX_VALUE >> 1) - 1);
            int lhs2 = randomIntBetween((Integer.MIN_VALUE >> 1) - 1, (Integer.MAX_VALUE >> 1) - 1);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(List.of(lhs, lhs2), DataType.INTEGER, "lhs"),
                    new TestCaseSupplier.TypedData(rhs, DataType.INTEGER, "rhs")
                ),
                "SubIntsEvaluator[lhs=Attribute[channel=0], rhs=Attribute[channel=1]]",
                DataType.INTEGER,
                is(nullValue())
            ).withWarning("Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.")
                .withWarning("Line -1:-1: java.lang.IllegalArgumentException: single-value function encountered multi-value");
        }));
        // exact math arithmetic exceptions
        suppliers.add(
            arithmeticExceptionOverflowCase(
                DataType.INTEGER,
                () -> Integer.MIN_VALUE,
                () -> randomIntBetween(1, Integer.MAX_VALUE),
                "SubIntsEvaluator"
            )
        );
        suppliers.add(
            arithmeticExceptionOverflowCase(
                DataType.INTEGER,
                () -> randomIntBetween(Integer.MIN_VALUE, -2),
                () -> Integer.MAX_VALUE,
                "SubIntsEvaluator"
            )
        );
        suppliers.add(
            arithmeticExceptionOverflowCase(
                DataType.LONG,
                () -> Long.MIN_VALUE,
                () -> randomLongBetween(1L, Long.MAX_VALUE),
                "SubLongsEvaluator"
            )
        );
        suppliers.add(
            arithmeticExceptionOverflowCase(
                DataType.LONG,
                () -> randomLongBetween(Long.MIN_VALUE, -2L),
                () -> Long.MAX_VALUE,
                "SubLongsEvaluator"
            )
        );
        suppliers.add(
            arithmeticExceptionOverflowCase(
                DataType.UNSIGNED_LONG,
                () -> ZERO_AS_UNSIGNED_LONG,
                () -> randomLongBetween(-Long.MAX_VALUE, Long.MAX_VALUE),
                "SubUnsignedLongsEvaluator"
            )
        );

        return parameterSuppliersFromTypedData(suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Sub(source, args.get(0), args.get(1));
    }
}
