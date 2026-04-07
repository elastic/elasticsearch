/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
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

public class DaysInMonthTests extends AbstractConfigurationFunctionTestCase {

    public DaysInMonthTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @Override
    protected Expression buildWithConfiguration(Source source, List<Expression> args, Configuration configuration) {
        return new DaysInMonth(source, args.get(0), configuration);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        suppliers.addAll(generateTest("2020-02-15T12:34:56Z", 29, ZoneId.of("Z")));
        suppliers.addAll(generateTest("2019-02-15T12:34:56Z", 28, ZoneId.of("Z")));
        suppliers.addAll(generateTest("2024-04-30T21:00:00Z", 30, ZoneId.of("Europe/Paris")));
        suppliers.addAll(generateTest("2024-03-31T22:00:00Z", 30, ZoneId.of("Europe/Paris")));
        suppliers.addAll(generateTest("2024-03-31T22:00:00Z", 31, ZoneId.of("Z")));
        suppliers.addAll(generateTest("2024-02-29T23:30:00Z", 31, ZoneId.of("Europe/Paris")));
        suppliers.add(
            new TestCaseSupplier(
                "Null",
                List.of(DataType.DATETIME),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(null, DataType.DATETIME, "date")),
                    Matchers.startsWith("DaysInMonthMillisEvaluator[val=Attribute[channel=0], zoneId="),
                    DataType.LONG,
                    equalTo(null)
                )
            )
        );
        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    private static List<TestCaseSupplier> generateTest(String dateTime, long expectedDays, ZoneId zoneId) {
        long millis = Instant.parse(dateTime).toEpochMilli();
        long nanos = DateUtils.toNanoSeconds(millis);
        return List.of(
            new TestCaseSupplier(
                expectedDays + " days - " + dateTime + " (millis) - " + zoneId,
                List.of(DataType.DATETIME),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(millis, DataType.DATETIME, "date")),
                    Matchers.startsWith("DaysInMonthMillisEvaluator[val=Attribute[channel=0], zoneId=" + zoneId + "]"),
                    DataType.LONG,
                    equalTo(expectedDays)
                ).withConfiguration(TestCaseSupplier.TEST_SOURCE, configurationForTimezone(zoneId))
            ),
            new TestCaseSupplier(
                expectedDays + " days - " + dateTime + " (nanos) - " + zoneId,
                List.of(DataType.DATE_NANOS),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(nanos, DataType.DATE_NANOS, "date")),
                    Matchers.startsWith("DaysInMonthNanosEvaluator[val=Attribute[channel=0], zoneId=" + zoneId + "]"),
                    DataType.LONG,
                    equalTo(expectedDays)
                ).withConfiguration(TestCaseSupplier.TEST_SOURCE, configurationForTimezone(zoneId))
            )
        );
    }

    public void testRandomZone() {
        long millis = Instant.parse("2024-01-15T00:00:00Z").toEpochMilli();
        Configuration cfg = configurationForTimezone(randomZone());
        long expected = Instant.ofEpochMilli(millis).atZone(cfg.zoneId()).toLocalDate().lengthOfMonth();
        DaysInMonth func = new DaysInMonth(Source.EMPTY, new Literal(Source.EMPTY, millis, DataType.DATETIME), cfg);
        assertThat(func.fold(org.elasticsearch.xpack.esql.core.expression.FoldContext.small()), equalTo(expected));
    }
}
