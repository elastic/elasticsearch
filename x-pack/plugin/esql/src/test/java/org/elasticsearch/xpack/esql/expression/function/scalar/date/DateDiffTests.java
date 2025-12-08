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

import static org.elasticsearch.xpack.esql.ConfigurationTestUtils.randomConfiguration;
import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.TEST_SOURCE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

public class DateDiffTests extends AbstractConfigurationFunctionTestCase {
    public DateDiffTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        String zdtStart = "2023-12-04T10:15:00Z";
        String zdtEnd = "2024-12-04T10:15:01Z";

        List.of(
            ///
            /// Timezone independent
            ///
            makeSuppliers("2023-12-04T10:15:00Z", "2023-12-04T10:15:01Z", null, "nanoseconds", 1000000000),
            makeSuppliers("2023-12-04T10:15:00Z", "2023-12-04T10:15:01Z", null, "ns", 1000000000),
            makeSuppliers("2023-12-04T10:15:00Z", "2023-12-04T10:15:01Z", null, "microseconds", 1000000),
            makeSuppliers("2023-12-04T10:15:00Z", "2023-12-04T10:15:01Z", null, "mcs", 1000000),
            makeSuppliers("2023-12-04T10:15:00Z", "2023-12-04T10:15:01Z", null, "milliseconds", 1000),
            makeSuppliers("2023-12-04T10:15:00Z", "2023-12-04T10:15:01Z", null, "ms", 1000),
            makeSuppliers("2023-12-04T10:15:00Z", "2023-12-04T10:15:01Z", null, "seconds", 1),
            makeSuppliers("2023-12-04T10:15:30Z", "2023-12-05T10:45:00Z", "Z", "seconds", 88170),
            makeSuppliers("2023-12-04T10:15:00Z", "2023-12-04T10:15:01Z", null, "ss", 1),
            makeSuppliers("2023-12-04T10:15:00Z", "2023-12-04T10:15:01Z", null, "s", 1),

            makeSuppliers(zdtStart, zdtEnd, null, "minutes", 527040),
            makeSuppliers(zdtStart, zdtEnd, null, "mi", 527040),
            makeSuppliers(zdtStart, zdtEnd, null, "n", 527040),
            makeSuppliers(zdtStart, zdtEnd, null, "hours", 8784),
            makeSuppliers(zdtStart, zdtEnd, null, "hh", 8784),

            ///
            /// UTC
            ///
            // 2024 is a leap year, so the dates are 366 days apart
            makeSuppliers(zdtStart, zdtEnd, "Z", "weekdays", 366),
            makeSuppliers(zdtStart, zdtEnd, "Z", "dw", 366),
            makeSuppliers(zdtStart, zdtEnd, "Z", "days", 366),
            makeSuppliers(zdtStart, zdtEnd, "Z", "dd", 366),
            makeSuppliers(zdtStart, zdtEnd, "Z", "d", 366),
            makeSuppliers(zdtStart, zdtEnd, "Z", "dy", 366),
            makeSuppliers(zdtStart, zdtEnd, "Z", "y", 366),

            makeSuppliers(zdtStart, zdtEnd, "Z", "weeks", 52),
            makeSuppliers(zdtStart, zdtEnd, "Z", "wk", 52),
            makeSuppliers(zdtStart, zdtEnd, "Z", "ww", 52),
            makeSuppliers(zdtStart, zdtEnd, "Z", "months", 12),
            makeSuppliers(zdtStart, zdtEnd, "Z", "mm", 12),
            makeSuppliers(zdtStart, zdtEnd, "Z", "m", 12),
            makeSuppliers(zdtStart, zdtEnd, "Z", "quarters", 4),
            makeSuppliers(zdtStart, zdtEnd, "Z", "qq", 4),
            makeSuppliers(zdtStart, zdtEnd, "Z", "q", 4),
            makeSuppliers(zdtStart, zdtEnd, "Z", "years", 1),
            makeSuppliers(zdtStart, zdtEnd, "Z", "yyyy", 1),
            makeSuppliers(zdtStart, zdtEnd, "Z", "yy", 1),
            makeSuppliers("2023-12-12T00:01:01Z", "2024-12-12T00:01:01Z", "Z", "year", 1),
            makeSuppliers("2023-12-12T00:01:01.001Z", "2024-12-12T00:01:01Z", "Z", "year", 0)
        ).forEach(suppliers::addAll);

        ///
        /// DST timezones: Cases where the result doesn't change
        ///
        List.of("Z", "Europe/Paris", "America/Goose_Bay")
            .forEach(
                timezone -> List.of(
                    // Europe/Paris, 1h DST
                    // - +1 -> +2 at 2025-03-30T02:00:00+01:00
                    makeSuppliers("2025-03-29T01:00:00+01:00", "2025-03-30T03:00:00+02:00", timezone, "days", 1),
                    // - +2 -> +1 at 2025-10-26T03:00:00+02:,
                    makeSuppliers("2025-10-25T02:00:00+02:00", "2025-10-26T02:00:00+01:00", timezone, "days", 1),

                    // America/Goose_Bay, midnight DST: -3 to -4 at 2010-11-07T00:01:00-03:00)
                    makeSuppliers("2010-11-07T00:00:00-03:00", "2010-11-06T23:01:00-04:00", timezone, "minutes", 1),
                    makeSuppliers("2010-11-07T00:00:00-03:00", "2010-11-06T23:01:00-04:00", timezone, "hours", 0),
                    makeSuppliers("2010-11-07T00:00:00-03:00", "2010-11-06T23:01:00-04:00", timezone, "days", 0)
                ).forEach(suppliers::addAll)
            );

        ///
        /// DST timezones: Cases where the result changes
        ///
        List.of(
            // America/Goose_Bay, midnight DST: -3 to -4 at 2010-11-07T00:01:00-03:00)
            makeSuppliers("2010-11-06T00:00:00-03:00", "2010-11-06T23:01:00-04:00", "America/Goose_Bay", "days", 0),
            makeSuppliers("2010-11-06T00:00:00-03:00", "2010-11-06T23:01:00-04:00", "Z", "days", 1),
            makeSuppliers("2010-10-07T00:00:00-03:00", "2010-11-06T23:01:00-04:00", "America/Goose_Bay", "months", 0),
            makeSuppliers("2010-10-07T00:00:00-03:00", "2010-11-06T23:01:00-04:00", "Z", "months", 1),
            makeSuppliers("2009-10-07T00:00:00-03:00", "2010-11-06T23:01:00-04:00", "America/Goose_Bay", "months", 12),
            makeSuppliers("2009-10-07T00:00:00-03:00", "2010-11-06T23:01:00-04:00", "Z", "months", 13)
        ).forEach(suppliers::addAll);

        // Error cases
        suppliers.addAll(
            makeSuppliersForWarning(
                "2023-12-04T10:15:00Z",
                "2023-12-04T10:20:00Z",
                "nanoseconds",
                "Line 1:1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: [300000000000] out of [integer] range"
            )
        );

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    private static List<TestCaseSupplier> makeSuppliers(
        String startTimestampString,
        String endTimestampString,
        String timezone,
        String unit,
        int expected
    ) {
        Instant startTimestamp = Instant.parse(startTimestampString);
        Instant endTimestamp = Instant.parse(endTimestampString);

        Supplier<Configuration> configurationSupplier = () -> timezone == null
            ? randomConfiguration()
            : configurationForTimezone(ZoneId.of(timezone));

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
                            + startTimestampString
                            + "<MILLIS>, "
                            + endTimestampString
                            + "<MILLIS>) == "
                            + expected,
                        List.of(unitType, DataType.DATETIME, DataType.DATETIME),
                        () -> {
                            Configuration configuration = configurationSupplier.get();
                            return new TestCaseSupplier.TestCase(
                                List.of(
                                    new TestCaseSupplier.TypedData(new BytesRef(unit), unitType, "unit"),
                                    new TestCaseSupplier.TypedData(startTimestamp.toEpochMilli(), DataType.DATETIME, "startTimestamp"),
                                    new TestCaseSupplier.TypedData(endTimestamp.toEpochMilli(), DataType.DATETIME, "endTimestamp")
                                ),
                                "DateDiffMillisEvaluator[unit=Attribute[channel=0], startTimestamp=Attribute[channel=1], "
                                    + "endTimestamp=Attribute[channel=2], zoneId="
                                    + configuration.zoneId()
                                    + "]",
                                DataType.INTEGER,
                                equalTo(expected)
                            ).withConfiguration(TEST_SOURCE, configuration);
                        }
                    ),
                    new TestCaseSupplier(
                        "DateDiff("
                            + unit
                            + "<"
                            + unitType
                            + ">, "
                            + timezone
                            + ", "
                            + startTimestampString
                            + "<NANOS>, "
                            + endTimestampString
                            + "<NANOS>) == "
                            + expected,
                        List.of(unitType, DataType.DATE_NANOS, DataType.DATE_NANOS),
                        () -> {
                            Configuration configuration = configurationSupplier.get();
                            return new TestCaseSupplier.TestCase(
                                List.of(
                                    new TestCaseSupplier.TypedData(new BytesRef(unit), unitType, "unit"),
                                    new TestCaseSupplier.TypedData(DateUtils.toLong(startTimestamp), DataType.DATE_NANOS, "startTimestamp"),
                                    new TestCaseSupplier.TypedData(DateUtils.toLong(endTimestamp), DataType.DATE_NANOS, "endTimestamp")
                                ),
                                "DateDiffNanosEvaluator[unit=Attribute[channel=0], startTimestamp=Attribute[channel=1], "
                                    + "endTimestamp=Attribute[channel=2], zoneId="
                                    + configuration.zoneId()
                                    + "]",
                                DataType.INTEGER,
                                equalTo(expected)
                            ).withConfiguration(TEST_SOURCE, configuration);
                        }
                    ),
                    new TestCaseSupplier(
                        "DateDiff("
                            + unit
                            + "<"
                            + unitType
                            + ">, "
                            + timezone
                            + ", "
                            + startTimestampString
                            + "<NANOS>, "
                            + endTimestampString
                            + "<MILLIS>) == "
                            + expected,
                        List.of(unitType, DataType.DATE_NANOS, DataType.DATETIME),
                        () -> {
                            Configuration configuration = configurationSupplier.get();
                            return new TestCaseSupplier.TestCase(
                                List.of(
                                    new TestCaseSupplier.TypedData(new BytesRef(unit), unitType, "unit"),
                                    new TestCaseSupplier.TypedData(DateUtils.toLong(startTimestamp), DataType.DATE_NANOS, "startTimestamp"),
                                    new TestCaseSupplier.TypedData(endTimestamp.toEpochMilli(), DataType.DATETIME, "endTimestamp")
                                ),
                                "DateDiffNanosMillisEvaluator[unit=Attribute[channel=0], startTimestampNanos=Attribute[channel=1], "
                                    + "endTimestampMillis=Attribute[channel=2], zoneId="
                                    + configuration.zoneId()
                                    + "]",
                                DataType.INTEGER,
                                equalTo(expected)
                            ).withConfiguration(TEST_SOURCE, configuration);
                        }
                    ),
                    new TestCaseSupplier(
                        "DateDiff("
                            + unit
                            + "<"
                            + unitType
                            + ">, "
                            + timezone
                            + ", "
                            + startTimestampString
                            + "<MILLIS>, "
                            + endTimestampString
                            + "<NANOS>) == "
                            + expected,
                        List.of(unitType, DataType.DATETIME, DataType.DATE_NANOS),
                        () -> {
                            Configuration configuration = configurationSupplier.get();
                            return new TestCaseSupplier.TestCase(
                                List.of(
                                    new TestCaseSupplier.TypedData(new BytesRef(unit), unitType, "unit"),
                                    new TestCaseSupplier.TypedData(startTimestamp.toEpochMilli(), DataType.DATETIME, "startTimestamp"),
                                    new TestCaseSupplier.TypedData(DateUtils.toLong(endTimestamp), DataType.DATE_NANOS, "endTimestamp")
                                ),
                                "DateDiffMillisNanosEvaluator[unit=Attribute[channel=0], startTimestampMillis=Attribute[channel=1], "
                                    + "endTimestampNanos=Attribute[channel=2], zoneId="
                                    + configuration.zoneId()
                                    + "]",
                                DataType.INTEGER,
                                equalTo(expected)
                            ).withConfiguration(TEST_SOURCE, configuration);
                        }
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
                        "warning -> " + unit + "<KEYWORD>, " + startTimestamp + ", " + endTimestamp,
                        List.of(DataType.KEYWORD, DataType.DATETIME, DataType.DATETIME),
                        () -> new TestCaseSupplier.TestCase(
                            List.of(
                                new TestCaseSupplier.TypedData(new BytesRef(unit), DataType.KEYWORD, "unit"),
                                new TestCaseSupplier.TypedData(startTimestamp.toEpochMilli(), DataType.DATETIME, "startTimestamp"),
                                new TestCaseSupplier.TypedData(endTimestamp.toEpochMilli(), DataType.DATETIME, "endTimestamp")
                            ),
                            startsWith(
                                "DateDiffMillisEvaluator[unit=Attribute[channel=0], startTimestamp=Attribute[channel=1], "
                                    + "endTimestamp=Attribute[channel=2], zoneId="
                            ),
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
                            startsWith(
                                "DateDiffMillisEvaluator[unit=Attribute[channel=0], startTimestamp=Attribute[channel=1], "
                                    + "endTimestamp=Attribute[channel=2], zoneId="
                            ),
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
        return new DateDiff(source, args.get(0), args.get(1), args.get(2), configuration);
    }

    @Override
    protected Expression serializeDeserializeExpression(Expression expression) {
        // TODO: This function doesn't serialize the Source, and must be fixed.
        return expression;
    }
}
