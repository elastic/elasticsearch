/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matchers;

import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAmount;
import java.util.List;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import java.util.function.ToLongBiFunction;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.randomLiteral;
import static org.elasticsearch.xpack.esql.core.util.DateUtils.asDateTime;
import static org.elasticsearch.xpack.esql.core.util.DateUtils.asMillis;
import static org.elasticsearch.xpack.esql.core.util.NumericUtils.ZERO_AS_UNSIGNED_LONG;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

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
                        "SubDoublesEvaluator[lhs=Attribute[channel=0], rhs=Attribute[channel=1]]",
                        DataType.DOUBLE,
                        equalTo(null)
                    ).withWarning("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.")
                        .withWarning("Line 1:1: java.lang.ArithmeticException: not a finite double number: -Infinity")
                )
            )
        );

        suppliers.add(new TestCaseSupplier("Datetime - Period", List.of(DataType.DATETIME, DataType.DATE_PERIOD), () -> {
            long lhs = (Long) randomLiteral(DataType.DATETIME).value();
            Period rhs = (Period) randomLiteral(DataType.DATE_PERIOD).value();
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(lhs, DataType.DATETIME, "lhs"),
                    new TestCaseSupplier.TypedData(rhs, DataType.DATE_PERIOD, "rhs").forceLiteral()
                ),
                Matchers.startsWith("SubDatetimesEvaluator[datetime=Attribute[channel=0], temporalAmount="),
                DataType.DATETIME,
                equalTo(asMillis(asDateTime(lhs).minus(rhs)))
            );
        }));

        BinaryOperator<Object> nanosResult = (lhs, rhs) -> {
            try {
                return subtractDatesAndTemporalAmount(lhs, rhs, SubTests::subtractNanos);
            } catch (ArithmeticException e) {
                return null;
            }
        };
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                nanosResult,
                DataType.DATE_NANOS,
                TestCaseSupplier.dateNanosCases(Instant.parse("1985-01-01T00:00:00Z"), DateUtils.MAX_NANOSECOND_INSTANT),
                TestCaseSupplier.datePeriodCases(0, 0, 0, 10, 13, 32),
                startsWith("SubDateNanosEvaluator[dateNanos=Attribute[channel=0], temporalAmount="),
                (l, r) -> List.of(),
                true
            )
        );
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                nanosResult,
                DataType.DATE_NANOS,
                TestCaseSupplier.dateNanosCases(Instant.parse("1985-01-01T00:00:00Z"), DateUtils.MAX_NANOSECOND_INSTANT),
                TestCaseSupplier.timeDurationCases(0, 604800000L),
                startsWith("SubDateNanosEvaluator[dateNanos=Attribute[channel=0], temporalAmount="),
                (l, r) -> List.of(),
                true
            )
        );

        suppliers.add(new TestCaseSupplier("Period - Period", List.of(DataType.DATE_PERIOD, DataType.DATE_PERIOD), () -> {
            Period lhs = (Period) randomLiteral(DataType.DATE_PERIOD).value();
            Period rhs = (Period) randomLiteral(DataType.DATE_PERIOD).value();
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(lhs, DataType.DATE_PERIOD, "lhs"),
                    new TestCaseSupplier.TypedData(rhs, DataType.DATE_PERIOD, "rhs").forceLiteral()
                ),
                "Only folding possible, so there's no evaluator",
                DataType.DATE_PERIOD,
                equalTo(lhs.minus(rhs))
            ).withoutEvaluator();
        }));
        suppliers.add(new TestCaseSupplier("Datetime - Duration", List.of(DataType.DATETIME, DataType.TIME_DURATION), () -> {
            long lhs = (Long) randomLiteral(DataType.DATETIME).value();
            Duration rhs = (Duration) randomLiteral(DataType.TIME_DURATION).value();
            TestCaseSupplier.TestCase testCase = new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(lhs, DataType.DATETIME, "lhs"),
                    new TestCaseSupplier.TypedData(rhs, DataType.TIME_DURATION, "rhs").forceLiteral()
                ),
                Matchers.startsWith("SubDatetimesEvaluator[datetime=Attribute[channel=0], temporalAmount="),
                DataType.DATETIME,
                equalTo(asMillis(asDateTime(lhs).minus(rhs)))
            );
            return testCase;
        }));
        suppliers.add(new TestCaseSupplier("Duration - Duration", List.of(DataType.TIME_DURATION, DataType.TIME_DURATION), () -> {
            Duration lhs = (Duration) randomLiteral(DataType.TIME_DURATION).value();
            Duration rhs = (Duration) randomLiteral(DataType.TIME_DURATION).value();
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(lhs, DataType.TIME_DURATION, "lhs"),
                    new TestCaseSupplier.TypedData(rhs, DataType.TIME_DURATION, "rhs").forceLiteral()
                ),
                "Only folding possible, so there's no evaluator",
                DataType.TIME_DURATION,
                equalTo(lhs.minus(rhs))
            ).withoutEvaluator();
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

        suppliers = errorsForCasesWithoutExamples(anyNullIsNull(suppliers, (nullPosition, nullValueDataType, original) -> {
            if (nullValueDataType == DataType.NULL) {
                return original.getData().get(nullPosition == 0 ? 1 : 0).type();
            }
            return original.expectedType();
        }, (nullPosition, nullData, original) -> {
            if (DataType.isTemporalAmount(nullData.type())) {
                return equalTo("LiteralsEvaluator[lit=null]");
            }
            return original;
        }), SubTests::subErrorMessageString);

        // Cannot use parameterSuppliersFromTypedDataWithDefaultChecks as error messages are non-trivial
        return parameterSuppliersFromTypedData(suppliers);
    }

    private static String subErrorMessageString(boolean includeOrdinal, List<Set<DataType>> validPerPosition, List<DataType> types) {
        if (types.get(1) == DataType.DATETIME) {
            if (types.get(0).isNumeric() || DataType.isMillisOrNanos(types.get(0))) {
                return "[-] has arguments with incompatible types [" + types.get(0).typeName() + "] and [datetime]";
            }
            if (DataType.isNull(types.get(0))) {
                return "[-] arguments are in unsupported order: cannot subtract a [DATETIME] value [datetime] from a [NULL] amount [null]";
            }
        }

        try {
            return typeErrorMessage(includeOrdinal, validPerPosition, types, (a, b) -> "date_nanos, datetime or numeric");
        } catch (IllegalStateException e) {
            // This means all the positional args were okay, so the expected error is from the combination
            return "[-] has arguments with incompatible types [" + types.get(0).typeName() + "] and [" + types.get(1).typeName() + "]";
        }
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Sub(source, args.get(0), args.get(1));
    }

    private static Object subtractDatesAndTemporalAmount(Object lhs, Object rhs, ToLongBiFunction<Instant, TemporalAmount> subtract) {
        // this weird casting dance makes the expected value lambda symmetric
        Instant date;
        TemporalAmount period;
        if (lhs instanceof Instant) {
            date = (Instant) lhs;
            period = (TemporalAmount) rhs;
        } else {
            date = (Instant) rhs;
            period = (TemporalAmount) lhs;
        }
        return subtract.applyAsLong(date, period);
    }

    private static long subtractNanos(Instant date, TemporalAmount period) {
        return DateUtils.toLong(
            Instant.from(ZonedDateTime.ofInstant(date, org.elasticsearch.xpack.esql.core.util.DateUtils.UTC).minus(period))
        );
    }
}
