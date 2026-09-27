/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.ConfigurationBuilder;
import org.hamcrest.Matchers;

import java.time.Instant;
import java.time.ZoneId;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.TEST_SOURCE;
import static org.hamcrest.Matchers.equalTo;

/**
 * Shared {@link TestCaseSupplier}s for unary {@code YEAR}/{@code MONTH}/{@code DAY}/{@code HOUR}.
 * After surrogate substitution the evaluator is {@code DateExtractConstant*}.
 */
final class DatePartFunctionTestCases {

    private DatePartFunctionTestCases() {}

    static List<TestCaseSupplier> cases(String chronoDisplay, String date, String zoneIdName, long expected) {
        long dateMillis = Instant.parse(date).toEpochMilli();
        ZoneId zoneId = ZoneId.of(zoneIdName);
        return List.of(
            new TestCaseSupplier(
                chronoDisplay + " - " + date + " (millis) - " + zoneId,
                List.of(DataType.DATETIME),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(dateMillis, DataType.DATETIME, "date")),
                    "DateExtractConstantMillisEvaluator[value=Attribute[channel=0], chronoField="
                        + chronoDisplay
                        + ", zone="
                        + zoneId
                        + "]",
                    DataType.LONG,
                    equalTo(expected)
                ).withConfiguration(TEST_SOURCE, config(zoneId))
            ),
            new TestCaseSupplier(
                chronoDisplay + " - " + date + " (nanos) - " + zoneId,
                List.of(DataType.DATE_NANOS),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(DateUtils.toNanoSeconds(dateMillis), DataType.DATE_NANOS, "date")),
                    "DateExtractConstantNanosEvaluator[value=Attribute[channel=0], chronoField=" + chronoDisplay + ", zone=" + zoneId + "]",
                    DataType.LONG,
                    equalTo(expected)
                ).withConfiguration(TEST_SOURCE, config(zoneId))
            )
        );
    }

    static TestCaseSupplier nullCase(String chronoDisplay) {
        return new TestCaseSupplier(
            "Null",
            List.of(DataType.DATETIME),
            () -> new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(null, DataType.DATETIME, "date")),
                Matchers.startsWith("DateExtractConstantMillisEvaluator[value=Attribute[channel=0], chronoField=" + chronoDisplay),
                DataType.LONG,
                equalTo(null)
            )
        );
    }

    private static Configuration config(ZoneId zoneId) {
        return new ConfigurationBuilder(TEST_CFG).query(TEST_SOURCE.text()).setting(QuerySettings.TIME_ZONE, zoneId).build();
    }
}
