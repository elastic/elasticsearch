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
import org.hamcrest.Matchers;

import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class DateUnitCountTests extends AbstractConfigurationFunctionTestCase {

    public DateUnitCountTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @Override
    protected Expression buildWithConfiguration(Source source, List<Expression> args, Configuration configuration) {
        return new DateUnitCount(source, args.get(0), args.get(1), args.get(2), configuration);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        suppliers.addAll(generateTest("2020-02-15T12:34:56Z", 29, ZoneId.of("Z")));
        suppliers.addAll(generateTest("2019-02-15T12:34:56Z", 28, ZoneId.of("Z")));
        suppliers.addAll(generateTest("2024-03-31T22:00:00Z", 30, ZoneId.of("Europe/Paris")));
        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    private static List<TestCaseSupplier> generateTest(String dateTime, long expectedCount, ZoneId zoneId) {
        long millis = Instant.parse(dateTime).toEpochMilli();
        long nanos = DateUtils.toNanoSeconds(millis);
        List<TestCaseSupplier> result = new ArrayList<>();
        for (DataType ut1 : List.of(DataType.KEYWORD, DataType.TEXT)) {
            for (DataType ut2 : List.of(DataType.KEYWORD, DataType.TEXT)) {
                for (DataType dateType : List.of(DataType.DATETIME, DataType.DATE_NANOS)) {
                    boolean isNanos = dateType == DataType.DATE_NANOS;
                    long dateVal = isNanos ? nanos : millis;
                    String suffix = dateTime + " " + ut1 + "/" + ut2 + "/" + dateType;
                    result.add(
                        new TestCaseSupplier(
                            expectedCount + " days/month - " + suffix,
                            List.of(ut1, ut2, dateType),
                            () -> new TestCaseSupplier.TestCase(
                                List.of(
                                    new TestCaseSupplier.TypedData(new BytesRef("day"), ut1, "unit"),
                                    new TestCaseSupplier.TypedData(new BytesRef("month"), ut2, "in_unit"),
                                    new TestCaseSupplier.TypedData(dateVal, dateType, "date")
                                ),
                                Matchers.startsWith("DateUnitCount"),
                                DataType.LONG,
                                equalTo(expectedCount)
                            ).withConfiguration(TestCaseSupplier.TEST_SOURCE, configurationForTimezone(zoneId))
                        )
                    );
                }
            }
        }
        return result;
    }

    public void testCountsAcrossSupportedContainerUnits() {
        ZoneId zoneId = ZoneId.of("Z");
        long millis = Instant.parse("2024-02-15T12:34:56Z").toEpochMilli();

        assertThat(DateUnitCount.processMillis(DateDiff.Part.HOUR, DateDiff.Part.HOUR, millis, zoneId), equalTo(1L));
        assertThat(DateUnitCount.processMillis(DateDiff.Part.HOUR, DateDiff.Part.DAY, millis, zoneId), equalTo(24L));
        assertThat(DateUnitCount.processMillis(DateDiff.Part.DAY, DateDiff.Part.WEEK, millis, zoneId), equalTo(7L));
        assertThat(DateUnitCount.processMillis(DateDiff.Part.DAY, DateDiff.Part.MONTH, millis, zoneId), equalTo(29L));
        assertThat(DateUnitCount.processMillis(DateDiff.Part.MONTH, DateDiff.Part.YEAR, millis, zoneId), equalTo(12L));
        assertThat(DateUnitCount.processMillis(DateDiff.Part.DAY, DateDiff.Part.YEAR, millis, zoneId), equalTo(366L));
    }

    public void testDayLengthRespectsDstTransitions() {
        ZoneId zoneId = ZoneId.of("Europe/Paris");
        long springForward = Instant.parse("2024-03-31T12:00:00Z").toEpochMilli();
        long fallBack = Instant.parse("2024-10-27T12:00:00Z").toEpochMilli();

        assertThat(DateUnitCount.processMillis(DateDiff.Part.HOUR, DateDiff.Part.DAY, springForward, zoneId), equalTo(23L));
        assertThat(DateUnitCount.processMillis(DateDiff.Part.HOUR, DateDiff.Part.DAY, fallBack, zoneId), equalTo(25L));
    }

    public void testAliasesAreAccepted() {
        ZoneId zoneId = ZoneId.of("Z");
        long millis = Instant.parse("2024-02-15T12:34:56Z").toEpochMilli();

        assertThat(DateUnitCount.processMillis(new BytesRef("d"), new BytesRef("m"), millis, zoneId), equalTo(29L));
        assertThat(DateUnitCount.processMillis(new BytesRef("hh"), new BytesRef("dd"), millis, zoneId), equalTo(24L));
    }

    public void testConstantCombinationsViaProcessMillis() {
        ZoneId zoneId = ZoneId.of("Z");
        long millis = Instant.parse("2024-02-15T12:34:56Z").toEpochMilli();

        assertThat(DateUnitCount.processMillis(DateDiff.Part.SECOND, DateDiff.Part.MINUTE, millis, zoneId), equalTo(60L));
        assertThat(DateUnitCount.processMillis(DateDiff.Part.MINUTE, DateDiff.Part.HOUR, millis, zoneId), equalTo(60L));
        assertThat(DateUnitCount.processMillis(DateDiff.Part.NANOSECOND, DateDiff.Part.MICROSECOND, millis, zoneId), equalTo(1000L));
    }
}
