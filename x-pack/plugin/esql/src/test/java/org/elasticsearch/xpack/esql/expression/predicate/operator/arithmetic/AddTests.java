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
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractConfigurationFunctionTestCase;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;

import static org.elasticsearch.test.ReadableMatchers.matchesDateMillis;
import static org.elasticsearch.test.ReadableMatchers.matchesDateNanos;
import static org.elasticsearch.xpack.esql.core.type.DataType.DENSE_VECTOR;
import static org.elasticsearch.xpack.esql.core.util.DateUtils.asDateTime;
import static org.elasticsearch.xpack.esql.core.util.DateUtils.asMillis;
import static org.elasticsearch.xpack.esql.core.util.NumericUtils.asLongUnsigned;
import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.TEST_SOURCE;
import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.randomDenseVector;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class AddTests extends AbstractConfigurationFunctionTestCase {
    public AddTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        suppliers.addAll(
            TestCaseSupplier.forBinaryWithWidening(
                new TestCaseSupplier.NumericTypeTestConfigs<>(
                    new TestCaseSupplier.NumericTypeTestConfig<>(
                        (Integer.MIN_VALUE >> 1) - 1,
                        (Integer.MAX_VALUE >> 1) - 1,
                        (l, r) -> l.intValue() + r.intValue(),
                        "AddIntsEvaluator"
                    ),
                    new TestCaseSupplier.NumericTypeTestConfig<>(
                        (Long.MIN_VALUE >> 1) - 1,
                        (Long.MAX_VALUE >> 1) - 1,
                        (l, r) -> l.longValue() + r.longValue(),
                        "AddLongsEvaluator"
                    ),
                    new TestCaseSupplier.NumericTypeTestConfig<>(
                        Double.NEGATIVE_INFINITY,
                        Double.POSITIVE_INFINITY,
                        (l, r) -> l.doubleValue() + r.doubleValue(),
                        "AddDoublesEvaluator"
                    )
                ),
                "lhs",
                "rhs",
                (lhs, rhs) -> List.of(),
                true
            )
        );

        // Double overflows
        suppliers.addAll(
            List.of(
                new TestCaseSupplier(
                    List.of(DataType.DOUBLE, DataType.DOUBLE),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(Double.MAX_VALUE, DataType.DOUBLE, "lhs"),
                            new TestCaseSupplier.TypedData(Double.MAX_VALUE, DataType.DOUBLE, "rhs")
                        ),
                        "AddDoublesEvaluator[lhs=Attribute[channel=0], rhs=Attribute[channel=1]]",
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
                            new TestCaseSupplier.TypedData(-Double.MAX_VALUE, DataType.DOUBLE, "rhs")
                        ),
                        "AddDoublesEvaluator[lhs=Attribute[channel=0], rhs=Attribute[channel=1]]",
                        DataType.DOUBLE,
                        equalTo(null)
                    ).withWarning("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.")
                        .withWarning("Line 1:1: java.lang.ArithmeticException: not a finite double number: -Infinity")
                )
            )
        );

        // Unsigned Long cases
        // TODO: These should be integrated into the type cross product above, but are currently broken
        // see https://github.com/elastic/elasticsearch/issues/102935
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                "AddUnsignedLongsEvaluator",
                "lhs",
                "rhs",
                (l, r) -> (((BigInteger) l).add((BigInteger) r)),
                DataType.UNSIGNED_LONG,
                TestCaseSupplier.ulongCases(BigInteger.ZERO, BigInteger.valueOf(Long.MAX_VALUE), true),
                TestCaseSupplier.ulongCases(BigInteger.ZERO, BigInteger.valueOf(Long.MAX_VALUE), true),
                List.of(),
                true
            )
        );

        // Datetime, Period/Duration Cases

        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                (lhs, rhs) -> ((Period) lhs).plus((Period) rhs),
                DataType.DATE_PERIOD,
                TestCaseSupplier.datePeriodCases(),
                TestCaseSupplier.datePeriodCases(),
                startsWith("LiteralsEvaluator[lit="),  // lhs and rhs have to be literals, so we fold into a literal
                (lhs, rhs) -> List.of(),
                true
            )
        );
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                (lhs, rhs) -> ((Duration) lhs).plus((Duration) rhs),
                DataType.TIME_DURATION,
                TestCaseSupplier.timeDurationCases(),
                TestCaseSupplier.timeDurationCases(),
                startsWith("LiteralsEvaluator[lit="), // lhs and rhs have to be literals, so we fold into a literal
                (lhs, rhs) -> List.of(),
                true
            )
        );

        BinaryOperator<Object> result = (lhs, rhs) -> {
            try {
                return addDatesAndTemporalAmount(lhs, rhs, ZoneOffset.UTC, AddTests::addMillis);
            } catch (ArithmeticException e) {
                return null;
            }
        };
        BiFunction<TestCaseSupplier.TypedData, TestCaseSupplier.TypedData, List<String>> warnings = (lhs, rhs) -> {
            try {
                addDatesAndTemporalAmount(lhs.getValue(), rhs.getValue(), ZoneOffset.UTC, AddTests::addMillis);
                return List.of();
            } catch (ArithmeticException e) {
                return List.of(
                    "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                    "Line 1:1: java.lang.ArithmeticException: long overflow"
                );
            }
        };
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                result,
                DataType.DATETIME,
                TestCaseSupplier.dateCases(),
                TestCaseSupplier.datePeriodCases(),
                startsWith("AddDatetimesEvaluator[datetime=Attribute[channel=0], temporalAmount="),
                warnings,
                true
            )
        );
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                result,
                DataType.DATETIME,
                TestCaseSupplier.dateCases(),
                TestCaseSupplier.timeDurationCases(),
                startsWith("AddDatetimesEvaluator[datetime=Attribute[channel=0], temporalAmount="),
                warnings,
                true
            )
        );

        BinaryOperator<Object> nanosResult = (lhs, rhs) -> {
            try {
                assert (lhs instanceof Instant) || (rhs instanceof Instant);
                return addDatesAndTemporalAmount(lhs, rhs, ZoneOffset.UTC, AddTests::addNanos);
            } catch (ArithmeticException e) {
                return null;
            }
        };
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                nanosResult,
                DataType.DATE_NANOS,
                TestCaseSupplier.dateNanosCases(),
                TestCaseSupplier.datePeriodCases(0, 0, 0, 10, 13, 32),
                startsWith("AddDateNanosEvaluator[dateNanos=Attribute[channel=0], temporalAmount="),
                warnings,
                true
            )
        );
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                nanosResult,
                DataType.DATE_NANOS,
                TestCaseSupplier.dateNanosCases(),
                TestCaseSupplier.timeDurationCases(0, 604800000L),
                startsWith("AddDateNanosEvaluator[dateNanos=Attribute[channel=0], temporalAmount="),
                warnings,
                true
            )
        );

        suppliers.addAll(TestCaseSupplier.dateCases().stream().<TestCaseSupplier>mapMulti((tds, consumer) -> {
            consumer.accept(
                new TestCaseSupplier(
                    List.of(DataType.DATETIME, DataType.NULL),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(tds.get(), TestCaseSupplier.TypedData.NULL),
                        "LiteralsEvaluator[lit=null]",
                        DataType.DATETIME,
                        nullValue()
                    )
                )
            );
            consumer.accept(
                new TestCaseSupplier(
                    List.of(DataType.NULL, DataType.DATETIME),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(TestCaseSupplier.TypedData.NULL, tds.get()),
                        "LiteralsEvaluator[lit=null]",
                        DataType.DATETIME,
                        nullValue()
                    )
                )
            );
        }).toList());

        // Exact math arithmetic exceptions
        suppliers.add(
            arithmeticExceptionOverflowCase(
                DataType.INTEGER,
                () -> randomIntBetween(1, Integer.MAX_VALUE),
                () -> Integer.MAX_VALUE,
                "AddIntsEvaluator"
            )
        );
        suppliers.add(
            arithmeticExceptionOverflowCase(
                DataType.INTEGER,
                () -> randomIntBetween(Integer.MIN_VALUE, -1),
                () -> Integer.MIN_VALUE,
                "AddIntsEvaluator"
            )
        );
        suppliers.add(
            arithmeticExceptionOverflowCase(
                DataType.LONG,
                () -> randomLongBetween(1L, Long.MAX_VALUE),
                () -> Long.MAX_VALUE,
                "AddLongsEvaluator"
            )
        );
        suppliers.add(
            arithmeticExceptionOverflowCase(
                DataType.LONG,
                () -> randomLongBetween(Long.MIN_VALUE, -1L),
                () -> Long.MIN_VALUE,
                "AddLongsEvaluator"
            )
        );
        suppliers.add(
            arithmeticExceptionOverflowCase(
                DataType.UNSIGNED_LONG,
                () -> asLongUnsigned(randomBigInteger()),
                () -> asLongUnsigned(UNSIGNED_LONG_MAX),
                "AddUnsignedLongsEvaluator"
            )
        );

        suppliers.add(new TestCaseSupplier(List.of(DENSE_VECTOR, DENSE_VECTOR), () -> {
            int dimensions = between(64, 128);
            List<Float> left = randomDenseVector(dimensions);
            List<Float> right = randomDenseVector(dimensions);
            List<Float> expected = addVectors(left, right);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(left, DENSE_VECTOR, "vector1"),
                    new TestCaseSupplier.TypedData(right, DENSE_VECTOR, "vector2")
                ),
                "AddDenseVectorsEvaluator[lhs=Attribute[channel=0], rhs=Attribute[channel=1]]",
                DENSE_VECTOR,
                equalTo(expected)
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DENSE_VECTOR, DENSE_VECTOR), () -> {
            int dimensions = between(64, 128);
            List<Float> left = randomDenseVector(dimensions);
            List<Float> right = randomDenseVector(randomValueOtherThan(dimensions, () -> between(64, 128)));
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(left, DENSE_VECTOR, "vector1"),
                    new TestCaseSupplier.TypedData(right, DENSE_VECTOR, "vector2")
                ),
                "AddDenseVectorsEvaluator[lhs=Attribute[channel=0], rhs=Attribute[channel=1]]",
                DENSE_VECTOR,
                equalTo(null)
            ).withWarning("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.")
                .withWarning("Line 1:1: java.lang.IllegalArgumentException: dense_vector dimensions do not match");
        }));

        suppliers.add(new TestCaseSupplier(List.of(DENSE_VECTOR, DENSE_VECTOR), () -> {
            int dimensions = between(64, 128);
            List<Float> left = randomDenseVector(dimensions);
            List<Float> right = randomDenseVector(dimensions);
            left.set(32, Float.MAX_VALUE);
            right.set(32, Float.MAX_VALUE);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(left, DENSE_VECTOR, "vector1"),
                    new TestCaseSupplier.TypedData(right, DENSE_VECTOR, "vector2")
                ),
                "AddDenseVectorsEvaluator[lhs=Attribute[channel=0], rhs=Attribute[channel=1]]",
                DENSE_VECTOR,
                equalTo(null)
            ).withWarning("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.")
                .withWarning("Line 1:1: java.lang.ArithmeticException: not a finite double number: Infinity");
        }));

        // Set the timezone to UTC for test cases up to here
        suppliers = TestCaseSupplier.mapTestCases(
            suppliers,
            tc -> tc.withConfiguration(TEST_SOURCE, configurationForTimezone(ZoneOffset.UTC))
        );

        // Date tests with timezones
        // -5 to -4 at 2025-03-09T02:00:00-05:00
        suppliers.addAll(suppliersForDate("2025-03-09T01:00:00-05:00", Period.ofDays(1), "Z", "2025-03-10T01:00:00-05:00"));
        suppliers.addAll(suppliersForDate("2025-03-09T01:00:00-05:00", Period.ofDays(1), "America/New_York", "2025-03-10T01:00:00-04:00"));
        // 24h should do nothing for timezones
        suppliers.addAll(
            suppliersForDate("2025-03-09T01:00:00-05:00", Duration.ofHours(24), "America/New_York", "2025-03-10T01:00:00-05:00")
        );

        // Datetime tests are split in two, depending on their permissiveness of null-injection, which cannot happen "automatically" for
        // Datetime + Period/Duration, since the expression will take the non-null arg's type.
        suppliers = anyNullIsNull(
            suppliers,
            (nullPosition, nullType, original) -> original.expectedType(),
            (nullPosition, nullData, original) -> nullData.isForceLiteral() ? equalTo("LiteralsEvaluator[lit=null]") : original
        );
        suppliers = errorsForCasesWithoutExamples(suppliers, AddTests::addErrorMessageString);

        // Cannot use parameterSuppliersFromTypedDataWithDefaultChecks as error messages are non-trivial
        return parameterSuppliersFromTypedData(suppliers);
    }

    private static String addErrorMessageString(boolean includeOrdinal, List<Set<DataType>> validPerPosition, List<DataType> types) {
        try {
            return typeErrorMessage(includeOrdinal, validPerPosition, types, (a, b) -> "date_nanos, datetime, numeric or dense_vector");
        } catch (IllegalStateException e) {
            // This means all the positional args were okay, so the expected error is from the combination
            return "[+] has arguments with incompatible types [" + types.get(0).typeName() + "] and [" + types.get(1).typeName() + "]";

        }
    }

    private static List<TestCaseSupplier> suppliersForDate(
        String dateString,
        TemporalAmount period,
        String zoneIdString,
        String expectedResultString
    ) {
        Instant inputDate = Instant.parse(dateString);
        long dateAsMillis = DateUtils.toLongMillis(inputDate);
        long dateAsNanos = DateUtils.toLong(inputDate);
        DataType periodType = period instanceof Period ? DataType.DATE_PERIOD : DataType.TIME_DURATION;
        ZoneId zoneId = ZoneId.of(zoneIdString);

        return List.of(
            new TestCaseSupplier(
                "millis " + dateString + ", " + period + ", " + zoneIdString,
                List.of(DataType.DATETIME, periodType),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(dateAsMillis, DataType.DATETIME, "date"),
                        new TestCaseSupplier.TypedData(period, periodType, "period").forceLiteral()
                    ),
                    "AddDatetimesEvaluator[datetime=Attribute[channel=0], temporalAmount=" + period + ", zoneId=" + zoneId + "]",
                    DataType.DATETIME,
                    matchesDateMillis(expectedResultString)
                ).withConfiguration(TEST_SOURCE, configurationForTimezone(zoneId))
            ),
            new TestCaseSupplier(
                "nanos " + dateString + ", " + period + ", " + zoneIdString,
                List.of(DataType.DATE_NANOS, periodType),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(dateAsNanos, DataType.DATE_NANOS, "date"),
                        new TestCaseSupplier.TypedData(period, periodType, "period").forceLiteral()
                    ),
                    "AddDateNanosEvaluator[dateNanos=Attribute[channel=0], temporalAmount=" + period + ", zoneId=" + zoneId + "]",
                    DataType.DATE_NANOS,
                    matchesDateNanos(expectedResultString)
                ).withConfiguration(TEST_SOURCE, configurationForTimezone(zoneId))
            )
        );
    }

    interface DateAdder {
        long add(Instant date, TemporalAmount period, ZoneId zoneId);
    }

    private static Object addDatesAndTemporalAmount(Object lhs, Object rhs, ZoneId zoneId, DateAdder adder) {
        // this weird casting dance makes the expected value lambda symmetric
        Instant date;
        TemporalAmount period;
        assert (lhs instanceof Instant) || (rhs instanceof Instant);
        if (lhs instanceof Instant) {
            date = (Instant) lhs;
            period = (TemporalAmount) rhs;
        } else {
            date = (Instant) rhs;
            period = (TemporalAmount) lhs;
        }
        return adder.add(date, period, zoneId);
    }

    private static long addMillis(Instant date, TemporalAmount period, ZoneId zoneId) {
        return asMillis(asDateTime(date, zoneId).plus(period));
    }

    private static long addNanos(Instant date, TemporalAmount period, ZoneId zoneId) {
        return DateUtils.toLong(Instant.from(asDateTime(date, zoneId).plus(period)));
    }

    @Override
    protected Expression buildWithConfiguration(Source source, List<Expression> args, Configuration configuration) {
        return new Add(source, args.get(0), args.get(1), configuration);
    }

    private static List<Float> addVectors(List<Float> left, List<Float> right) {
        List<Float> result = new ArrayList<>();
        for (int i = 0; i < left.size(); i++) {
            result.add(left.get(i) + right.get(i));
        }
        return result;
    }
}
