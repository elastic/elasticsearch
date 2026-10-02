/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.UnaryTestCaseHelper;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractConfigurationFunctionTestCase;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.math.BigInteger;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.test.ReadableMatchers.matchesDateMillis;
import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.TEST_SOURCE;
import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.unary;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.DEFAULT_DATE_TIME_FORMATTER;

public class ToDatetimeTests extends AbstractConfigurationFunctionTestCase {
    public ToDatetimeTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        helper().dates().expectedFromInstant(DateUtils::toLongMillis).evaluatorToString("%0").build(suppliers);
        helper().dateNanos()
            .expectedFromInstant(i -> DateUtils.toMilliSeconds(DateUtils.toLong(i)))
            .evaluatorToString("ToDatetimeFromDateNanosEvaluator[in=%0]")
            .build(suppliers);
        helper().ints().expectedFromInt(Integer::longValue).evaluatorToString("ToLongFromIntEvaluator[i=%0]").build(suppliers);
        helper().longs().expectedFromLong(l -> l).evaluatorToString("%0").build(suppliers);

        unsignedLongCase().unsignedLongs(BigInteger.ZERO, BigInteger.valueOf(Long.MAX_VALUE))
            .expectedFromBigInteger(BigInteger::longValueExact)
            .build(suppliers);
        unsignedLongCase().unsignedLongs(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.TWO), UNSIGNED_LONG_MAX)
            .expectNullAndWarningsFromBigInteger(
                bi -> List.of("Line 1:1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: [" + bi + "] out of [long] range")
            )
            .build(suppliers);

        double lowerDouble = -9.223372036854777E18; // a "convenient" value smaller than `(double) Long.MIN_VALUE` (== ...776E18)
        double upperDouble = 9.223372036854777E18; // a "convenient" value larger than `(double) Long.MAX_VALUE` (== ...776E18)
        doubleCase().doubles(lowerDouble, upperDouble).expectedFromDouble(Double::longValue);
        doubleWarningCase().doubles(Double.NEGATIVE_INFINITY, lowerDouble).build(suppliers);
        doubleWarningCase().doubles(upperDouble, Double.POSITIVE_INFINITY).build(suppliers);

        stringWarningCase().strings().build(suppliers);
        stringCase().testCases(
            new TestCaseSupplier.TypedDataSupplier(
                "<date string>",
                // millis past "0001-01-01T00:00:00.000Z" to match the default formatter
                () -> new BytesRef(randomDateString(-62135596800000L, 253402300799999L)),
                DataType.KEYWORD
            )
        ).expectedFromString(DEFAULT_DATE_TIME_FORMATTER::parseMillis).build(suppliers);
        stringWarningCase().testCases(
            new TestCaseSupplier.TypedDataSupplier(
                "<date string before -9999-12-31T23:59:59.999Z>",
                // millis before "-9999-12-31T23:59:59.999Z"
                () -> new BytesRef(randomDateString(Long.MIN_VALUE, -377736739200000L)),
                DataType.KEYWORD
            )
        ).build(suppliers);
        stringWarningCase().testCases(
            new TestCaseSupplier.TypedDataSupplier(
                "<date string after 9999-12-31T23:59:59.999Z>",
                // millis after "9999-12-31T23:59:59.999Z"
                () -> new BytesRef(randomDateString(253402300800000L, Long.MAX_VALUE)),
                DataType.KEYWORD
            )
        ).build(suppliers);
        suppliers = TestCaseSupplier.mapTestCases(
            suppliers,
            tc -> tc.withConfiguration(TEST_SOURCE, configurationForTimezone(ZoneOffset.UTC))
        );

        fixedCases("2020-05-07T02:03:04.123Z", "America/New_York", "2020-05-07T02:03:04.123Z").build(suppliers);
        fixedCases("2020-05-07T02:03:04.123", "America/New_York", "2020-05-07T02:03:04.123-04:00").build(suppliers);
        fixedCases("2010-12-31", "Z", "2010-12-31T00:00:00.000Z").build(suppliers);
        fixedCases("2010-12-31", "America/New_York", "2010-12-31T00:00:00.000-05:00").build(suppliers);

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    private static UnaryTestCaseHelper helper() {
        return unary().expectedOutputType(DataType.DATETIME);
    }

    private static UnaryTestCaseHelper unsignedLongCase() {
        return helper().evaluatorToString("ToLongFromUnsignedLongEvaluator[ul=%0]");
    }

    private static UnaryTestCaseHelper doubleCase() {
        return helper().evaluatorToString("ToLongFromDoubleEvaluator[dbl=%0]");
    }

    private static UnaryTestCaseHelper doubleWarningCase() {
        return doubleCase().expectNullAndWarningsFromDouble(
            d -> List.of("Line 1:1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: [" + d + "] out of [long] range")
        );
    }

    private static UnaryTestCaseHelper stringCase() {
        return helper().evaluatorToString("ToDatetimeFromStringEvaluator[in=%0, formatter=format[strict_date_optional_time] locale[]]");
    }

    private static UnaryTestCaseHelper stringWarningCase() {
        return stringCase().expectNullAndWarningsFromString(
            s -> List.of(
                "Line 1:1: java.lang.IllegalArgumentException: "
                    + (s.isEmpty()
                        ? "cannot parse empty datetime"
                        : ("failed to parse date field [" + s + "] with format [strict_date_optional_time]"))
            )
        );
    }

    private static UnaryTestCaseHelper fixedCases(String dateString, String zoneIdString, String expectedDate) {
        ZoneId zoneId = ZoneId.of(zoneIdString);
        return stringCase().name(s -> s.getFirst().type() + ": " + dateString + ", " + zoneIdString + ", " + expectedDate)
            .testCases(
                DataType.stringTypes().stream().map(dt -> new TestCaseSupplier.TypedDataSupplier("date", () -> dateString, dt)).toList()
            )
            .expected(o -> matchesDateMillis(expectedDate))
            .configuration(() -> configurationForTimezone(zoneId));
    }

    private static String randomDateString(long from, long to) {
        return Instant.ofEpochMilli(randomLongBetween(from, to)).toString();
    }

    @Override
    protected Expression buildWithConfiguration(Source source, List<Expression> args, Configuration configuration) {
        return new ToDatetime(source, args.getFirst(), configuration);
    }
}
