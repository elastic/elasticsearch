/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
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

import static org.elasticsearch.test.ReadableMatchers.matchesDateNanos;
import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.TEST_SOURCE;

public class ToDateNanosTests extends AbstractConfigurationFunctionTestCase {
    public ToDateNanosTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        if (EsqlCapabilities.Cap.TO_DATE_NANOS.isEnabled() == false) {
            return List.of();
        }
        final String read = "Attribute[channel=0]";
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        TestCaseSupplier.unary(
            suppliers,
            read,
            TestCaseSupplier.dateNanosCases(),
            DataType.DATE_NANOS,
            v -> DateUtils.toLong((Instant) v),
            List.of()
        );
        TestCaseSupplier.unary(
            suppliers,
            "ToDateNanosFromDatetimeEvaluator[in=" + read + "]",
            TestCaseSupplier.dateCases(0, DateUtils.MAX_NANOSECOND_INSTANT.toEpochMilli()),
            DataType.DATE_NANOS,
            i -> DateUtils.toNanoSeconds(((Instant) i).toEpochMilli()),
            List.of()
        );
        TestCaseSupplier.forUnaryLong(
            suppliers,
            "ToDateNanosFromLongEvaluator[in=" + read + "]",
            DataType.DATE_NANOS,
            l -> l,
            0,
            Long.MAX_VALUE,
            List.of()
        );
        TestCaseSupplier.forUnaryLong(
            suppliers,
            "ToDateNanosFromLongEvaluator[in=" + read + "]",
            DataType.DATE_NANOS,
            l -> null,
            Long.MIN_VALUE,
            -1L,
            List.of(
                "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                "Line 1:1: java.lang.IllegalArgumentException: Nanosecond dates before 1970-01-01T00:00:00.000Z are not supported."
            )
        );
        TestCaseSupplier.forUnaryUnsignedLong(
            suppliers,
            "ToLongFromUnsignedLongEvaluator[ul=" + read + "]",
            DataType.DATE_NANOS,
            BigInteger::longValueExact,
            BigInteger.ZERO,
            BigInteger.valueOf(Long.MAX_VALUE),
            List.of()
        );
        TestCaseSupplier.forUnaryUnsignedLong(
            suppliers,
            "ToLongFromUnsignedLongEvaluator[ul=" + read + "]",
            DataType.DATE_NANOS,
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
            "ToDateNanosFromDoubleEvaluator[in=" + read + "]",
            DataType.DATE_NANOS,
            d -> null,
            Double.NEGATIVE_INFINITY,
            -Double.MIN_VALUE,
            d -> List.of(
                "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                "Line 1:1: java.lang.IllegalArgumentException: Nanosecond dates before 1970-01-01T00:00:00.000Z are not supported."
            )
        );
        TestCaseSupplier.forUnaryDouble(
            suppliers,
            "ToDateNanosFromDoubleEvaluator[in=" + read + "]",
            DataType.DATE_NANOS,
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
            "ToDateNanosFromStringEvaluator[in=" + read + ", formatter=format[strict_date_optional_time_nanos] locale[]]",
            DataType.DATE_NANOS,
            bytesRef -> null,
            bytesRef -> List.of(
                "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                "Line 1:1: java.lang.IllegalArgumentException: "
                    + (bytesRef.utf8ToString().isEmpty()
                        ? "cannot parse empty datetime"
                        : ("failed to parse date field [" + bytesRef.utf8ToString() + "] with format [strict_date_optional_time_nanos]"))
            )
        );
        suppliers = TestCaseSupplier.mapTestCases(
            suppliers,
            tc -> tc.withConfiguration(TEST_SOURCE, configurationForTimezone(ZoneOffset.UTC))
        );

        suppliers.addAll(casesFor("2020-05-07T02:03:04.123456789Z", "America/New_York", "2020-05-07T02:03:04.123456789Z"));
        suppliers.addAll(casesFor("2020-05-07T02:03:04.123456789", "America/New_York", "2020-05-07T02:03:04.123456789-04:00"));
        suppliers.addAll(casesFor("2010-12-31", "Z", "2010-12-31T00:00:00.000000000Z"));
        suppliers.addAll(casesFor("2010-12-31", "America/New_York", "2010-12-31T00:00:00.000000000-05:00"));

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
                        "ToDateNanosFromStringEvaluator[in=Attribute[channel=0], "
                            + "formatter=format[strict_date_optional_time_nanos] locale[]]",
                        DataType.DATE_NANOS,
                        matchesDateNanos(expectedDate)
                    ).withConfiguration(TEST_SOURCE, configurationForTimezone(zoneId))
                )
            )
            .toList();
    }

    @Override
    protected Expression buildWithConfiguration(Source source, List<Expression> args, Configuration configuration) {
        return new ToDateNanos(source, args.get(0), configuration);
    }
}
