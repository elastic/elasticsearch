/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.grouping;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.DocsV3Support;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class TBucketTests extends AbstractScalarFunctionTestCase {
    public TBucketTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        dateCasesWithSpan(
            suppliers,
            "fixed date with period",
            () -> DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2023-01-01T00:00:00.00Z"),
            DataType.DATE_PERIOD,
            Period.ofYears(1),
            "[YEAR_OF_CENTURY in Z][fixed to midnight]"
        );
        dateCasesWithSpan(
            suppliers,
            "fixed date with duration",
            () -> DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2023-02-17T09:00:00.00Z"),
            DataType.TIME_DURATION,
            Duration.ofDays(1L),
            "[86400000 in Z][fixed]"
        );
        dateNanosCasesWithSpan(
            suppliers,
            "fixed date nanos with period",
            () -> DateUtils.toLong(Instant.parse("2023-01-01T00:00:00.00Z")),
            DataType.DATE_PERIOD,
            Period.ofYears(1)
        );
        dateNanosCasesWithSpan(
            suppliers,
            "fixed date nanos with duration",
            () -> DateUtils.toLong(Instant.parse("2023-02-17T09:00:00.00Z")),
            DataType.TIME_DURATION,
            Duration.ofDays(1L)
        );
        return parameterSuppliersFromTypedData(suppliers);
    }

    private static void dateCasesWithSpan(
        List<TestCaseSupplier> suppliers,
        String name,
        LongSupplier date,
        DataType spanType,
        Object span,
        String spanStr
    ) {
        suppliers.add(new TestCaseSupplier(name, List.of(spanType, DataType.DATETIME), () -> {
            List<TestCaseSupplier.TypedData> args = new ArrayList<>();
            args.add(new TestCaseSupplier.TypedData(span, spanType, "buckets").forceLiteral());
            args.add(new TestCaseSupplier.TypedData(date.getAsLong(), DataType.DATETIME, "@timestamp"));

            return new TestCaseSupplier.TestCase(
                args,
                "DateTruncDatetimeEvaluator[fieldVal=Attribute[channel=0], rounding=Rounding" + spanStr + "]",
                DataType.DATETIME,
                resultsMatcher(args)
            );
        }));
    }

    private static void dateNanosCasesWithSpan(
        List<TestCaseSupplier> suppliers,
        String name,
        LongSupplier date,
        DataType spanType,
        Object span
    ) {
        suppliers.add(new TestCaseSupplier(name, List.of(spanType, DataType.DATE_NANOS), () -> {
            List<TestCaseSupplier.TypedData> args = new ArrayList<>();
            args.add(new TestCaseSupplier.TypedData(span, spanType, "buckets").forceLiteral());
            args.add(new TestCaseSupplier.TypedData(date.getAsLong(), DataType.DATE_NANOS, "@timestamp"));
            return new TestCaseSupplier.TestCase(
                args,
                Matchers.startsWith("DateTruncDateNanosEvaluator[fieldVal=Attribute[channel=0], rounding=Rounding["),
                DataType.DATE_NANOS,
                resultsMatcher(args)
            );
        }));
    }

    private static Matcher<Object> resultsMatcher(List<TestCaseSupplier.TypedData> typedData) {
        if (typedData.get(1).type() == DataType.DATE_NANOS) {
            long nanos = ((Number) typedData.get(1).data()).longValue();
            long expected = DateUtils.toNanoSeconds(
                Rounding.builder(Rounding.DateTimeUnit.DAY_OF_MONTH).build().prepareForUnknown().round(DateUtils.toMilliSeconds(nanos))
            );
            LogManager.getLogger(getTestClass()).info("Expected: " + DateUtils.toInstant(expected));
            LogManager.getLogger(getTestClass()).info("Input: " + DateUtils.toInstant(nanos));
            return equalTo(expected);
        }

        // For DATETIME, we use the millis value directly
        long millis = ((Number) typedData.get(1).data()).longValue();
        long expected = Rounding.builder(Rounding.DateTimeUnit.DAY_OF_MONTH).build().prepareForUnknown().round(millis);
        LogManager.getLogger(getTestClass()).info("Expected: " + Instant.ofEpochMilli(expected));
        LogManager.getLogger(getTestClass()).info("Input: " + Instant.ofEpochMilli(millis));
        return equalTo(expected);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new TBucket(source, args.get(0), args.get(1));
    }

    @Override
    protected boolean canSerialize() {
        return false;
    }

    public static List<DocsV3Support.Param> signatureTypes(List<DocsV3Support.Param> params) {
        assertThat(params, hasSize(2));
        assertThat(params.get(1).dataType(), anyOf(equalTo(DataType.DATE_NANOS), equalTo(DataType.DATETIME)));
        return List.of(params.get(0));
    }
}
