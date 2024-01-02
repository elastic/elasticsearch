/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.math.BigInteger;
import java.time.Duration;
import java.time.Period;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.isDateTimeOrTemporal;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.isTemporalAmount;
import static org.elasticsearch.xpack.ql.type.DataTypes.isDateTime;
import static org.elasticsearch.xpack.ql.type.DateUtils.asDateTime;
import static org.elasticsearch.xpack.ql.type.DateUtils.asMillis;
import static org.elasticsearch.xpack.ql.util.NumericUtils.asLongUnsigned;
import static org.elasticsearch.xpack.ql.util.NumericUtils.unsignedLongAsBigInteger;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class AddTests extends AbstractDateTimeArithmeticTestCase {
    public AddTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        suppliers.addAll(
            TestCaseSupplier.forBinaryWithWidening(
                new TestCaseSupplier.NumericTypeTestConfigs(
                    new TestCaseSupplier.NumericTypeTestConfig(
                        (Integer.MIN_VALUE >> 1) - 1,
                        (Integer.MAX_VALUE >> 1) - 1,
                        (l, r) -> l.intValue() + r.intValue(),
                        "AddIntsEvaluator"
                    ),
                    new TestCaseSupplier.NumericTypeTestConfig(
                        (Long.MIN_VALUE >> 1) - 1,
                        (Long.MAX_VALUE >> 1) - 1,
                        (l, r) -> l.longValue() + r.longValue(),
                        "AddLongsEvaluator"
                    ),
                    new TestCaseSupplier.NumericTypeTestConfig(
                        Double.NEGATIVE_INFINITY,
                        Double.POSITIVE_INFINITY,
                        (l, r) -> l.doubleValue() + r.doubleValue(),
                        "AddDoublesEvaluator"
                    )
                ),
                "lhs",
                "rhs",
                List.of()
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
                DataTypes.UNSIGNED_LONG,
                TestCaseSupplier.ulongCases(BigInteger.ZERO, BigInteger.valueOf(Long.MAX_VALUE)),
                TestCaseSupplier.ulongCases(BigInteger.ZERO, BigInteger.valueOf(Long.MAX_VALUE)),
                List.of(),
                false
            )
        );

        // AwaitsFix https://github.com/elastic/elasticsearch/issues/103085
        // After fixing that issue, please move this line to below where the date cases are generated
        suppliers = anyNullIsNull(true, suppliers);

        // Datetime Cases
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                // TODO: There is an evaluator for Datetime + Period, so it should be tested. Similarly below.
                "No evaluator, the tests only trigger the folding code since Period is not representable",
                "lhs",
                "rhs",
                (lhs, rhs) -> {
                    // this weird casting dance makes the expected value lambda symmetric
                    Long date;
                    Period period;
                    if (lhs instanceof Long) {
                        date = (Long) lhs;
                        period = (Period) rhs;
                    } else {
                        date = (Long) rhs;
                        period = (Period) lhs;
                    }
                    return asMillis(asDateTime(date).plus(period));
                },
                DataTypes.DATETIME,
                TestCaseSupplier.dateCases(),
                TestCaseSupplier.datePeriodCases(),
                List.of(),
                true
            )
        );
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                "No evaluator, the tests only trigger the folding code since Period is not representable",
                "lhs",
                "rhs",
                (lhs, rhs) -> ((Period) lhs).plus((Period) rhs),
                EsqlDataTypes.DATE_PERIOD,
                TestCaseSupplier.datePeriodCases(),
                TestCaseSupplier.datePeriodCases(),
                List.of(),
                false
            )
        );
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                // TODO: There is an evaluator for Datetime + Duration, so it should be tested. Similarly below.
                "No evaluator, the tests only trigger the folding code since Duration is not representable",
                "lhs",
                "rhs",
                (lhs, rhs) -> {
                    // this weird casting dance makes the expected value lambda symmetric
                    Long date;
                    Duration duration;
                    if (lhs instanceof Long) {
                        date = (Long) lhs;
                        duration = (Duration) rhs;
                    } else {
                        date = (Long) rhs;
                        duration = (Duration) lhs;
                    }
                    return asMillis(asDateTime(date).plus(duration));
                },
                DataTypes.DATETIME,
                TestCaseSupplier.dateCases(),
                TestCaseSupplier.timeDurationCases(),
                List.of(),
                true
            )
        );
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                "No evaluator, the tests only trigger the folding code since Duration is not representable",
                "lhs",
                "rhs",
                (lhs, rhs) -> ((Duration) lhs).plus((Duration) rhs),
                EsqlDataTypes.TIME_DURATION,
                TestCaseSupplier.timeDurationCases(),
                TestCaseSupplier.timeDurationCases(),
                List.of(),
                false
            )
        );

        // Cases that should generate warnings
        suppliers.addAll(List.of(new TestCaseSupplier("MV", () -> {
            // Ensure we don't have an overflow
            int rhs = randomIntBetween((Integer.MIN_VALUE >> 1) - 1, (Integer.MAX_VALUE >> 1) - 1);
            int lhs = randomIntBetween((Integer.MIN_VALUE >> 1) - 1, (Integer.MAX_VALUE >> 1) - 1);
            int lhs2 = randomIntBetween((Integer.MIN_VALUE >> 1) - 1, (Integer.MAX_VALUE >> 1) - 1);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(List.of(lhs, lhs2), DataTypes.INTEGER, "lhs"),
                    new TestCaseSupplier.TypedData(rhs, DataTypes.INTEGER, "rhs")
                ),
                "AddIntsEvaluator[lhs=Attribute[channel=0], rhs=Attribute[channel=1]]",
                DataTypes.INTEGER,
                is(nullValue())
            ).withWarning("Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.")
                .withWarning("Line -1:-1: java.lang.IllegalArgumentException: single-value function encountered multi-value");
        })));

        return parameterSuppliersFromTypedData(suppliers);
    }

    @Override
    protected boolean supportsTypes(DataType lhsType, DataType rhsType) {
        if (isDateTimeOrTemporal(lhsType) || isDateTimeOrTemporal(rhsType)) {
            return isDateTime(lhsType) && isTemporalAmount(rhsType) || isTemporalAmount(lhsType) && isDateTime(rhsType);
        }
        return super.supportsTypes(lhsType, rhsType);
    }

    @Override
    protected Add build(Source source, Expression lhs, Expression rhs) {
        return new Add(source, lhs, rhs);
    }

    @Override
    protected double expectedValue(double lhs, double rhs) {
        return lhs + rhs;
    }

    @Override
    protected int expectedValue(int lhs, int rhs) {
        return lhs + rhs;
    }

    @Override
    protected long expectedValue(long lhs, long rhs) {
        return lhs + rhs;
    }

    @Override
    protected long expectedUnsignedLongValue(long lhs, long rhs) {
        BigInteger lhsBI = unsignedLongAsBigInteger(lhs);
        BigInteger rhsBI = unsignedLongAsBigInteger(rhs);
        return asLongUnsigned(lhsBI.add(rhsBI).longValue());
    }

    @Override
    protected long expectedValue(long datetime, TemporalAmount temporalAmount) {
        return asMillis(asDateTime(datetime).plus(temporalAmount));
    }
}
