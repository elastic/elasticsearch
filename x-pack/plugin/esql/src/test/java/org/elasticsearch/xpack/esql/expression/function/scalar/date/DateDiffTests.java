/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

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

import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.TEST_SOURCE;
import static org.hamcrest.Matchers.equalTo;

public class DateDiffTests extends AbstractConfigurationFunctionTestCase {
    public DateDiffTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {

        List<TestCaseSupplier> suppliers = new ArrayList<>();
        suppliers.addAll(makeSuppliers("2023-12-04T10:15:30Z", "2023-12-05T10:45:00Z", "seconds", 88170, "Z"));
        suppliers.addAll(makeSuppliers("2023-12-12T00:01:01Z", "2024-12-12T00:01:01Z", "year", 1, "Z"));
        suppliers.addAll(makeSuppliers("2023-12-12T00:01:01.001Z", "2024-12-12T00:01:01Z", "year", 0, "Z"));

        suppliers.addAll(makeSuppliers("2023-12-04T10:15:00Z", "2023-12-04T10:15:01Z", "nanoseconds", 1000000000, "Z"));
        suppliers.addAll(makeSuppliers("2023-12-04T10:15:00Z", "2023-12-04T10:15:01Z", "ns", 1000000000, "Z"));
        suppliers.addAll(makeSuppliers("2023-12-04T10:15:00Z", "2023-12-04T10:15:01Z", "microseconds", 1000000, "Z"));
        suppliers.addAll(makeSuppliers("2023-12-04T10:15:00Z", "2023-12-04T10:15:01Z", "mcs", 1000000, "Z"));
        suppliers.addAll(makeSuppliers("2023-12-04T10:15:00Z", "2023-12-04T10:15:01Z", "milliseconds", 1000, "Z"));
        suppliers.addAll(makeSuppliers("2023-12-04T10:15:00Z", "2023-12-04T10:15:01Z", "ms", 1000, "Z"));
        suppliers.addAll(makeSuppliers("2023-12-04T10:15:00Z", "2023-12-04T10:15:01Z", "seconds", 1, "Z"));
        suppliers.addAll(makeSuppliers("2023-12-04T10:15:00Z", "2023-12-04T10:15:01Z", "ss", 1, "Z"));
        suppliers.addAll(makeSuppliers("2023-12-04T10:15:00Z", "2023-12-04T10:15:01Z", "s", 1, "Z"));

        String zdtStart = "2023-12-04T10:15:00Z";
        String zdtEnd = "2024-12-04T10:15:01Z";

        suppliers.addAll(makeSuppliers(zdtStart, zdtEnd, "minutes", 527040, "Z"));
        suppliers.addAll(makeSuppliers(zdtStart, zdtEnd, "mi", 527040, "Z"));
        suppliers.addAll(makeSuppliers(zdtStart, zdtEnd, "n", 527040, "Z"));
        suppliers.addAll(makeSuppliers(zdtStart, zdtEnd, "hours", 8784, "Z"));
        suppliers.addAll(makeSuppliers(zdtStart, zdtEnd, "hh", 8784, "Z"));

        // 2024 is a leap year, so the dates are 366 days apart
        suppliers.addAll(makeSuppliers(zdtStart, zdtEnd, "weekdays", 366, "Z"));
        suppliers.addAll(makeSuppliers(zdtStart, zdtEnd, "dw", 366, "Z"));
        suppliers.addAll(makeSuppliers(zdtStart, zdtEnd, "days", 366, "Z"));
        suppliers.addAll(makeSuppliers(zdtStart, zdtEnd, "dd", 366, "Z"));
        suppliers.addAll(makeSuppliers(zdtStart, zdtEnd, "d", 366, "Z"));
        suppliers.addAll(makeSuppliers(zdtStart, zdtEnd, "dy", 366, "Z"));
        suppliers.addAll(makeSuppliers(zdtStart, zdtEnd, "y", 366, "Z"));

        suppliers.addAll(makeSuppliers(zdtStart, zdtEnd, "weeks", 52, "Z"));
        suppliers.addAll(makeSuppliers(zdtStart, zdtEnd, "wk", 52, "Z"));
        suppliers.addAll(makeSuppliers(zdtStart, zdtEnd, "ww", 52, "Z"));
        suppliers.addAll(makeSuppliers(zdtStart, zdtEnd, "months", 12, "Z"));
        suppliers.addAll(makeSuppliers(zdtStart, zdtEnd, "mm", 12, "Z"));
        suppliers.addAll(makeSuppliers(zdtStart, zdtEnd, "m", 12, "Z"));
        suppliers.addAll(makeSuppliers(zdtStart, zdtEnd, "quarters", 4, "Z"));
        suppliers.addAll(makeSuppliers(zdtStart, zdtEnd, "qq", 4, "Z"));
        suppliers.addAll(makeSuppliers(zdtStart, zdtEnd, "q", 4, "Z"));
        suppliers.addAll(makeSuppliers(zdtStart, zdtEnd, "years", 1, "Z"));
        suppliers.addAll(makeSuppliers(zdtStart, zdtEnd, "yyyy", 1, "Z"));
        suppliers.addAll(makeSuppliers(zdtStart, zdtEnd, "yy", 1, "Z"));

        // Error cases
        String zdtStart2 = "2023-12-04T10:15:00Z";
        String zdtEnd2 = "2023-12-04T10:20:00Z";
        suppliers.addAll(
            makeSuppliersForWarning(
                zdtStart2,
                zdtEnd2,
                "nanoseconds",
                "Line 1:1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: [300000000000] out of [integer] range"
            )
        );

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    private static List<TestCaseSupplier> makeSuppliers(String startTimestampString, String endTimestampString, String unit, int expected, String timezone) {
        Instant startTimestamp = Instant.parse(startTimestampString);
        Instant endTimestamp = Instant.parse(endTimestampString);
        ZoneId zoneId = ZoneId.of(timezone);

        return Stream.of(DataType.KEYWORD, DataType.TEXT)
            .flatMap(
                unitType -> Stream.of(
                    new TestCaseSupplier(
                        "DateDiff("
                            + unit
                            + "<"
                            + unitType
                            + ">, "
                            + timezone
                            + ", "
                            + startTimestamp
                            + "<MILLIS>, "
                            + endTimestamp
                            + "<MILLIS>) == "
                            + expected,
                        List.of(unitType, DataType.DATETIME, DataType.DATETIME),
                        () -> new TestCaseSupplier.TestCase(
                            List.of(
                                new TestCaseSupplier.TypedData(new BytesRef(unit), unitType, "unit"),
                                new TestCaseSupplier.TypedData(startTimestamp.toEpochMilli(), DataType.DATETIME, "startTimestamp"),
                                new TestCaseSupplier.TypedData(endTimestamp.toEpochMilli(), DataType.DATETIME, "endTimestamp")
                            ),
                            "DateDiffMillisEvaluator[unit=Attribute[channel=0], startTimestamp=Attribute[channel=1], "
                                + "endTimestamp=Attribute[channel=2]]",
                            DataType.INTEGER,
                            equalTo(expected)
                        ).withConfiguration(TEST_SOURCE, configurationForTimezone(zoneId))
                    ),
                    new TestCaseSupplier(
                        "DateDiff("
                            + unit
                            + "<"
                            + unitType
                            + ">, "
                            + timezone
                            + ", "
                            + startTimestamp
                            + "<NANOS>, "
                            + endTimestamp
                            + "<NANOS>) == "
                            + expected,
                        List.of(unitType, DataType.DATE_NANOS, DataType.DATE_NANOS),
                        () -> new TestCaseSupplier.TestCase(
                            List.of(
                                new TestCaseSupplier.TypedData(new BytesRef(unit), unitType, "unit"),
                                new TestCaseSupplier.TypedData(DateUtils.toLong(startTimestamp), DataType.DATE_NANOS, "startTimestamp"),
                                new TestCaseSupplier.TypedData(DateUtils.toLong(endTimestamp), DataType.DATE_NANOS, "endTimestamp")
                            ),
                            "DateDiffNanosEvaluator[unit=Attribute[channel=0], startTimestamp=Attribute[channel=1], "
                                + "endTimestamp=Attribute[channel=2]]",
                            DataType.INTEGER,
                            equalTo(expected)
                        )
                    ),
                    new TestCaseSupplier(
                        "DateDiff("
                            + unit
                            + "<"
                            + unitType
                            + ">, "
                            + timezone
                            + ", "
                            + startTimestamp
                            + "<NANOS>, "
                            + endTimestamp
                            + "<MILLIS>) == "
                            + expected,
                        List.of(unitType, DataType.DATE_NANOS, DataType.DATETIME),
                        () -> new TestCaseSupplier.TestCase(
                            List.of(
                                new TestCaseSupplier.TypedData(new BytesRef(unit), unitType, "unit"),
                                new TestCaseSupplier.TypedData(DateUtils.toLong(startTimestamp), DataType.DATE_NANOS, "startTimestamp"),
                                new TestCaseSupplier.TypedData(endTimestamp.toEpochMilli(), DataType.DATETIME, "endTimestamp")
                            ),
                            "DateDiffNanosMillisEvaluator[unit=Attribute[channel=0], startTimestampNanos=Attribute[channel=1], "
                                + "endTimestampMillis=Attribute[channel=2]]",
                            DataType.INTEGER,
                            equalTo(expected)
                        )
                    ),
                    new TestCaseSupplier(
                        "DateDiff("
                            + unit
                            + "<"
                            + unitType
                            + ">, "
                            + timezone
                            + ", "
                            + startTimestamp
                            + "<MILLIS>, "
                            + endTimestamp
                            + "<NANOS>) == "
                            + expected,
                        List.of(unitType, DataType.DATETIME, DataType.DATE_NANOS),
                        () -> new TestCaseSupplier.TestCase(
                            List.of(
                                new TestCaseSupplier.TypedData(new BytesRef(unit), unitType, "unit"),
                                new TestCaseSupplier.TypedData(startTimestamp.toEpochMilli(), DataType.DATETIME, "startTimestamp"),
                                new TestCaseSupplier.TypedData(DateUtils.toLong(endTimestamp), DataType.DATE_NANOS, "endTimestamp")
                            ),
                            "DateDiffMillisNanosEvaluator[unit=Attribute[channel=0], startTimestampMillis=Attribute[channel=1], "
                                + "endTimestampNanos=Attribute[channel=2]]",
                            DataType.INTEGER,
                            equalTo(expected)
                        )
                    )
                )
            )
            .toList();
    }

    private static List<TestCaseSupplier> makeSuppliersForWarning(
        String startTimestampString,
        String endTimestampString,
        String unit,
        String warning
    ) {
        Instant startTimestamp = Instant.parse(startTimestampString);
        Instant endTimestamp = Instant.parse(endTimestampString);

        return Stream.of(DataType.KEYWORD, DataType.TEXT)
            .flatMap(
                unitType -> Stream.of(
                    new TestCaseSupplier(
                        "DateDiff(" + unit + "<KEYWORD>, " + startTimestamp + ", " + endTimestamp + ") -> warning ",
                        List.of(DataType.KEYWORD, DataType.DATETIME, DataType.DATETIME),
                        () -> new TestCaseSupplier.TestCase(
                            List.of(
                                new TestCaseSupplier.TypedData(new BytesRef(unit), DataType.KEYWORD, "unit"),
                                new TestCaseSupplier.TypedData(startTimestamp.toEpochMilli(), DataType.DATETIME, "startTimestamp"),
                                new TestCaseSupplier.TypedData(endTimestamp.toEpochMilli(), DataType.DATETIME, "endTimestamp")
                            ),
                            "DateDiffMillisEvaluator[unit=Attribute[channel=0], startTimestamp=Attribute[channel=1], "
                                + "endTimestamp=Attribute[channel=2]]",
                            DataType.INTEGER,
                            equalTo(null)
                        ).withWarning("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.")
                            .withWarning(warning)
                    ),
                    // Units as text case
                    new TestCaseSupplier(
                        "DateDiff(" + unit + "<TEXT>, " + startTimestamp + ", " + endTimestamp + ") -> warning ",
                        List.of(DataType.TEXT, DataType.DATETIME, DataType.DATETIME),
                        () -> new TestCaseSupplier.TestCase(
                            List.of(
                                new TestCaseSupplier.TypedData(new BytesRef(unit), DataType.TEXT, "unit"),
                                new TestCaseSupplier.TypedData(startTimestamp.toEpochMilli(), DataType.DATETIME, "startTimestamp"),
                                new TestCaseSupplier.TypedData(endTimestamp.toEpochMilli(), DataType.DATETIME, "endTimestamp")
                            ),
                            "DateDiffMillisEvaluator[unit=Attribute[channel=0], startTimestamp=Attribute[channel=1], "
                                + "endTimestamp=Attribute[channel=2]]",
                            DataType.INTEGER,
                            equalTo(null)
                        ).withWarning("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.")
                            .withWarning(warning)
                    )
                )
            )
            .toList();
    }

    @Override
    protected Expression buildWithConfiguration(Source source, List<Expression> args, Configuration configuration) {
        return new DateDiff(source, args.get(0), args.get(1), args.get(2), configuration); // TODO: Add config here
    }

    @Override
    protected Expression serializeDeserializeExpression(Expression expression) {
        // TODO: This function doesn't serialize the Source, and must be fixed.
        return expression;
    }
}
