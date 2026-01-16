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
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractConfigurationFunctionTestCase;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.math.BigInteger;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static org.elasticsearch.test.ReadableMatchers.matchesDateMillis;
import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.TEST_SOURCE;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.DEFAULT_DATE_TIME_FORMATTER;

public class ToDatetimeTests extends AbstractConfigurationFunctionTestCase {
    public ToDatetimeTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        final String read = "Attribute[channel=0]";
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        TestCaseSupplier.unary(
            suppliers,
            read,
            TestCaseSupplier.dateCases(),
            DataType.DATETIME,
            v -> DateUtils.toLongMillis((Instant) v),
            emptyList()
        );
        TestCaseSupplier.unary(
            suppliers,
            "ToDatetimeFromDateNanosEvaluator[in=" + read + "]",
            TestCaseSupplier.dateNanosCases(),
            DataType.DATETIME,
            i -> DateUtils.toMilliSeconds(DateUtils.toLong((Instant) i)),
            emptyList()
        );

        TestCaseSupplier.forUnaryInt(
            suppliers,
            "ToLongFromIntEvaluator[i=" + read + "]",
            DataType.DATETIME,
            i -> ((Integer) i).longValue(),
            Integer.MIN_VALUE,
            Integer.MAX_VALUE,
            emptyList()
        );
        TestCaseSupplier.forUnaryLong(suppliers, read, DataType.DATETIME, l -> l, Long.MIN_VALUE, Long.MAX_VALUE, emptyList());
        TestCaseSupplier.forUnaryUnsignedLong(
            suppliers,
            "ToLongFromUnsignedLongEvaluator[ul=" + read + "]",
            DataType.DATETIME,
            BigInteger::longValueExact,
            BigInteger.ZERO,
            BigInteger.valueOf(Long.MAX_VALUE),
            emptyList()
        );
        TestCaseSupplier.forUnaryUnsignedLong(
            suppliers,
            "ToLongFromUnsignedLongEvaluator[ul=" + read + "]",
            DataType.DATETIME,
            bi -> null,
            BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.TWO),
            UNSIGNED_LONG_MAX,
            bi -> List.of(
                "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                "Line 1:1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: [" + bi + "] out of [long] range"
            )
        );
        TestCaseSupplier.forUnaryDouble(
            suppliers,
            "ToLongFromDoubleEvaluator[dbl=" + read + "]",
            DataType.DATETIME,
            d -> null,
            Double.NEGATIVE_INFINITY,
            -9.223372036854777E18, // a "convenient" value smaller than `(double) Long.MIN_VALUE` (== ...776E18)
            d -> List.of(
                "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                "Line 1:1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: [" + d + "] out of [long] range"
            )
        );
        TestCaseSupplier.forUnaryDouble(
            suppliers,
            "ToLongFromDoubleEvaluator[dbl=" + read + "]",
            DataType.DATETIME,
            d -> null,
            9.223372036854777E18, // a "convenient" value larger than `(double) Long.MAX_VALUE` (== ...776E18)
            Double.POSITIVE_INFINITY,
            d -> List.of(
                "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                "Line 1:1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: [" + d + "] out of [long] range"
            )
        );
        TestCaseSupplier.forUnaryStrings(
            suppliers,
            "ToDatetimeFromStringEvaluator[in=" + read + ", formatter=format[strict_date_optional_time] locale[]]",
            DataType.DATETIME,
            bytesRef -> null,
            bytesRef -> List.of(
                "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                "Line 1:1: java.lang.IllegalArgumentException: "
                    + (bytesRef.utf8ToString().isEmpty()
                        ? "cannot parse empty datetime"
                        : ("failed to parse date field [" + bytesRef.utf8ToString() + "] with format [strict_date_optional_time]"))
            )
        );
        TestCaseSupplier.unary(
            suppliers,
            "ToDatetimeFromStringEvaluator[in=" + read + ", formatter=format[strict_date_optional_time] locale[]]",
            List.of(
                new TestCaseSupplier.TypedDataSupplier(
                    "<date string>",
                    // millis past "0001-01-01T00:00:00.000Z" to match the default formatter
                    () -> new BytesRef(randomDateString(-62135596800000L, 253402300799999L)),
                    DataType.KEYWORD
                )
            ),
            DataType.DATETIME,
            bytesRef -> DEFAULT_DATE_TIME_FORMATTER.parseMillis(((BytesRef) bytesRef).utf8ToString()),
            emptyList()
        );
        TestCaseSupplier.unary(
            suppliers,
            "ToDatetimeFromStringEvaluator[in=" + read + ", formatter=format[strict_date_optional_time] locale[]]",
            List.of(
                new TestCaseSupplier.TypedDataSupplier(
                    "<date string before -9999-12-31T23:59:59.999Z>",
                    // millis before "-9999-12-31T23:59:59.999Z"
                    () -> new BytesRef(randomDateString(Long.MIN_VALUE, -377736739200000L)),
                    DataType.KEYWORD
                )
            ),
            DataType.DATETIME,
            bytesRef -> null,
            bytesRef -> List.of(
                "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                "Line 1:1: java.lang.IllegalArgumentException: failed to parse date field ["
                    + ((BytesRef) bytesRef).utf8ToString()
                    + "] with format [strict_date_optional_time]"
            )
        );
        TestCaseSupplier.unary(
            suppliers,
            "ToDatetimeFromStringEvaluator[in=" + read + ", formatter=format[strict_date_optional_time] locale[]]",
            List.of(
                new TestCaseSupplier.TypedDataSupplier(
                    "<date string after 9999-12-31T23:59:59.999Z>",
                    // millis after "9999-12-31T23:59:59.999Z"
                    () -> new BytesRef(randomDateString(253402300800000L, Long.MAX_VALUE)),
                    DataType.KEYWORD
                )
            ),
            DataType.DATETIME,
            bytesRef -> null,
            bytesRef -> List.of(
                "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                "Line 1:1: java.lang.IllegalArgumentException: failed to parse date field ["
                    + ((BytesRef) bytesRef).utf8ToString()
                    + "] with format [strict_date_optional_time]"
            )
        );
        suppliers = TestCaseSupplier.mapTestCases(
            suppliers,
            tc -> tc.withConfiguration(TEST_SOURCE, configurationForTimezone(ZoneOffset.UTC))
        );

        suppliers.addAll(casesFor("2020-05-07T02:03:04.123Z", "America/New_York", "2020-05-07T02:03:04.123Z"));
        suppliers.addAll(casesFor("2020-05-07T02:03:04.123", "America/New_York", "2020-05-07T02:03:04.123-04:00"));
        suppliers.addAll(casesFor("2010-12-31", "Z", "2010-12-31T00:00:00.000Z"));
        suppliers.addAll(casesFor("2010-12-31", "America/New_York", "2010-12-31T00:00:00.000-05:00"));

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    private static List<TestCaseSupplier> casesFor(String dateString, String zoneIdString, String expectedDate) {
        ZoneId zoneId = ZoneId.of(zoneIdString);

        return DataType.stringTypes()
            .stream()
            .map(
                inputType -> new TestCaseSupplier(
                    inputType + ": " + dateString + ", " + zoneIdString + ", " + expectedDate,
                    List.of(inputType),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(new TestCaseSupplier.TypedData(dateString, inputType, "date")),
                        "ToDatetimeFromStringEvaluator[in=Attribute[channel=0], formatter=format[strict_date_optional_time] locale[]]",
                        DataType.DATETIME,
                        matchesDateMillis(expectedDate)
                    ).withConfiguration(TEST_SOURCE, configurationForTimezone(zoneId))
                )
            )
            .toList();
    }

    private static String randomDateString(long from, long to) {
        return Instant.ofEpochMilli(randomLongBetween(from, to)).toString();
    }

    @Override
    protected Expression buildWithConfiguration(Source source, List<Expression> args, Configuration configuration) {
        return new ToDatetime(source, args.get(0), configuration);
    }
}
