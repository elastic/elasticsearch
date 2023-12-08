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
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.ql.InvalidArgumentException;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class DateDiffTests extends AbstractScalarFunctionTestCase {
    public DateDiffTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        ZonedDateTime zdtStart = ZonedDateTime.parse("2023-12-04T10:15:30Z");
        ZonedDateTime zdtEnd = ZonedDateTime.parse("2023-12-05T10:45:00Z");

        return parameterSuppliersFromTypedData(
            List.of(
                new TestCaseSupplier(
                    "Date Diff In Seconds - OK",
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(new BytesRef("seconds"), DataTypes.TEXT, "unit"),
                            new TestCaseSupplier.TypedData(zdtStart.toInstant().toEpochMilli(), DataTypes.DATETIME, "startTimestamp"),
                            new TestCaseSupplier.TypedData(zdtEnd.toInstant().toEpochMilli(), DataTypes.DATETIME, "endTimestamp")
                        ),
                        "DateDiffEvaluator[unit=Attribute[channel=0], startTimestamp=Attribute[channel=1], "
                            + "endTimestamp=Attribute[channel=2]]",
                        DataTypes.INTEGER,
                        equalTo(88170)
                    )
                ),
                new TestCaseSupplier(
                    "Date Diff Error Type unit",
                    () -> TestCaseSupplier.TestCase.typeError(
                        List.of(
                            new TestCaseSupplier.TypedData(new BytesRef("seconds"), DataTypes.INTEGER, "unit"),
                            new TestCaseSupplier.TypedData(zdtStart.toInstant().toEpochMilli(), DataTypes.DATETIME, "startTimestamp"),
                            new TestCaseSupplier.TypedData(zdtEnd.toInstant().toEpochMilli(), DataTypes.DATETIME, "endTimestamp")
                        ),
                        "first argument of [] must be [string], found value [unit] type [integer]"
                    )
                ),
                new TestCaseSupplier(
                    "Date Diff Error Type startTimestamp",
                    () -> TestCaseSupplier.TestCase.typeError(
                        List.of(
                            new TestCaseSupplier.TypedData(new BytesRef("minutes"), DataTypes.TEXT, "unit"),
                            new TestCaseSupplier.TypedData(zdtStart.toInstant().toEpochMilli(), DataTypes.INTEGER, "startTimestamp"),
                            new TestCaseSupplier.TypedData(zdtEnd.toInstant().toEpochMilli(), DataTypes.DATETIME, "endTimestamp")
                        ),
                        "second argument of [] must be [datetime], found value [startTimestamp] type [integer]"
                    )
                ),
                new TestCaseSupplier(
                    "Date Diff Error Type endTimestamp",
                    () -> TestCaseSupplier.TestCase.typeError(
                        List.of(
                            new TestCaseSupplier.TypedData(new BytesRef("minutes"), DataTypes.TEXT, "unit"),
                            new TestCaseSupplier.TypedData(zdtStart.toInstant().toEpochMilli(), DataTypes.DATETIME, "startTimestamp"),
                            new TestCaseSupplier.TypedData(zdtEnd.toInstant().toEpochMilli(), DataTypes.INTEGER, "endTimestamp")
                        ),
                        "third argument of [] must be [datetime], found value [endTimestamp] type [integer]"
                    )
                )
            )
        );
    }

    public void testDateDiffFunction() {
        ZonedDateTime zdtStart = ZonedDateTime.parse("2023-12-04T10:15:00Z");
        ZonedDateTime zdtEnd = ZonedDateTime.parse("2023-12-04T10:15:01Z");
        long startTimestamp = zdtStart.toInstant().toEpochMilli();
        long endTimestamp = zdtEnd.toInstant().toEpochMilli();

        assertEquals(1000000000, DateDiff.process(new BytesRef("nanoseconds"), startTimestamp, endTimestamp));
        assertEquals(1000000, DateDiff.process(new BytesRef("microseconds"), startTimestamp, endTimestamp));
        assertEquals(1000, DateDiff.process(new BytesRef("milliseconds"), startTimestamp, endTimestamp));
        assertEquals(1, DateDiff.process(new BytesRef("seconds"), startTimestamp, endTimestamp));

        zdtEnd = zdtEnd.plusYears(1);
        endTimestamp = zdtEnd.toInstant().toEpochMilli();

        assertEquals(527040, DateDiff.process(new BytesRef("minutes"), startTimestamp, endTimestamp));
        assertEquals(8784, DateDiff.process(new BytesRef("hours"), startTimestamp, endTimestamp));
        assertEquals(52, DateDiff.process(new BytesRef("weeks"), startTimestamp, endTimestamp));
        assertEquals(366, DateDiff.process(new BytesRef("weekdays"), startTimestamp, endTimestamp));
        assertEquals(366, DateDiff.process(new BytesRef("days"), startTimestamp, endTimestamp));
        assertEquals(366, DateDiff.process(new BytesRef("dy"), startTimestamp, endTimestamp));
        assertEquals(12, DateDiff.process(new BytesRef("months"), startTimestamp, endTimestamp));
        assertEquals(4, DateDiff.process(new BytesRef("quarters"), startTimestamp, endTimestamp));
        assertEquals(1, DateDiff.process(new BytesRef("years"), startTimestamp, endTimestamp));
    }

    public void testDateDiffFunctionErrorTooLarge() {
        ZonedDateTime zdtStart = ZonedDateTime.parse("2023-12-04T10:15:00Z");
        ZonedDateTime zdtEnd = ZonedDateTime.parse("2023-12-04T10:20:00Z");
        long startTimestamp = zdtStart.toInstant().toEpochMilli();
        long endTimestamp = zdtEnd.toInstant().toEpochMilli();

        InvalidArgumentException e = expectThrows(
            InvalidArgumentException.class,
            () -> DateDiff.process(new BytesRef("nanoseconds"), startTimestamp, endTimestamp)
        );
        assertThat(
            e.getMessage(),
            containsString(
                "The DATE_DIFF function resulted in an overflow; "
                    + "the number of units separating two date/datetime instances is too large. "
                    + "Try to use DATE_DIFF with a less precise unit."
            )
        );
    }

    public void testDateDiffFunctionErrorUnitNotValid() {
        InvalidArgumentException e = expectThrows(InvalidArgumentException.class, () -> DateDiff.process(new BytesRef("sseconds"), 0, 0));
        assertThat(
            e.getMessage(),
            containsString(
                "Received value [sseconds] is not valid date part to add; "
                    + "did you mean [seconds, second, nanoseconds, milliseconds, microseconds, nanosecond]?"
            )
        );

        e = expectThrows(InvalidArgumentException.class, () -> DateDiff.process(new BytesRef("not-valid-unit"), 0, 0));
        assertThat(
            e.getMessage(),
            containsString(
                "A value of [YEAR, QUARTER, MONTH, DAYOFYEAR, DAY, WEEK, WEEKDAY, HOUR, MINUTE, SECOND, MILLISECOND, MICROSECOND, "
                    + "NANOSECOND] or their aliases is required; received [not-valid-unit]"
            )
        );
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new DateDiff(source, args.get(0), args.get(1), args.get(2));
    }

    @Override
    protected List<ArgumentSpec> argSpec() {
        return List.of(required(strings()), required(DataTypes.DATETIME), required(DataTypes.DATETIME));
    }

    @Override
    protected DataType expectedType(List<DataType> argTypes) {
        return DataTypes.INTEGER;
    }
}
