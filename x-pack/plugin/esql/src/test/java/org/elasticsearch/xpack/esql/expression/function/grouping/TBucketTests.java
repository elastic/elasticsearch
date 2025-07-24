/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.grouping;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DateEsField;
import org.elasticsearch.xpack.esql.expression.function.AbstractAggregationTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;

import java.time.Duration;
import java.time.Period;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class TBucketTests extends AbstractAggregationTestCase {
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

        return parameterSuppliersFromTypedData(
            anyNullIsNull(
                suppliers,
                (nullPosition, nullValueDataType, original) -> nullPosition == 0 && nullValueDataType == DataType.NULL
                    ? DataType.NULL
                    : original.expectedType(),
                (nullPosition, nullData, original) -> nullPosition == 0 ? original : equalTo("LiteralsEvaluator[lit=null]")
            )
        );
    }

    private static void dateCasesWithSpan(
        List<TestCaseSupplier> suppliers,
        String name,
        LongSupplier date,
        DataType spanType,
        Object span,
        String spanStr
    ) {
        suppliers.add(new TestCaseSupplier(name, List.of(spanType), () -> {
            List<TestCaseSupplier.TypedData> args = new ArrayList<>();
            args.add(new TestCaseSupplier.TypedData(span, spanType, "buckets").forceLiteral());
            // args.add(new TestCaseSupplier.TypedData(date.getAsLong(), DataType.DATETIME, "timestamps"));

            return new TestCaseSupplier.TestCase(
                args,
                "DateTruncDatetimeEvaluator[fieldVal=Attribute[channel=0], rounding=Rounding" + spanStr + "]",
                DataType.DATETIME,
                resultsMatcher(args)
            );
        }));
    }

    private static Matcher<Object> resultsMatcher(List<TestCaseSupplier.TypedData> typedData) {
        // long millis = ((Number) typedData.get(1).data()).longValue();
        // long expected = Rounding.builder(Rounding.DateTimeUnit.DAY_OF_MONTH).build().prepareForUnknown().round(millis);
        // LogManager.getLogger(getTestClass()).info("Expected: " + Instant.ofEpochMilli(expected));
        // LogManager.getLogger(getTestClass()).info("Input: " + Instant.ofEpochMilli(millis));
        // return equalTo(expected);
        return equalTo(null);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new TBucket(
            source,
            args.getFirst(),
            new FieldAttribute(source, "@timestamp", DateEsField.dateEsField("@timestamp", Map.of(), false))
        );
    }

    public static List<DataType> signatureTypes(List<DataType> testCaseTypes) {
        assertThat(testCaseTypes, hasSize(2));
        assertThat(testCaseTypes.get(1), equalTo(DataType.DATETIME));
        return List.of(testCaseTypes.get(0));
    }
}
