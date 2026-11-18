/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.test.ESTestCase;

import java.time.Duration;
import java.time.Period;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class ColumnIntervalsTests extends ESTestCase {

    public void testFormatDurations() {
        assertThat(ColumnIntervals.format(Duration.ofHours(1)), equalTo("1 hour"));
        assertThat(ColumnIntervals.format(Duration.ofHours(24)), equalTo("24 hours"));
        assertThat(ColumnIntervals.format(Duration.ofMinutes(90)), equalTo("90 minutes"));
        assertThat(ColumnIntervals.format(Duration.ofSeconds(30)), equalTo("30 seconds"));
        assertThat(ColumnIntervals.format(Duration.ofMillis(1500)), equalTo("1500 milliseconds"));
    }

    public void testFormatPeriods() {
        assertThat(ColumnIntervals.format(Period.ofDays(1)), equalTo("1 day"));
        assertThat(ColumnIntervals.format(Period.ofWeeks(2)), equalTo("2 weeks"));
        assertThat(ColumnIntervals.format(Period.ofMonths(3)), equalTo("3 months"));
        assertThat(ColumnIntervals.format(Period.ofYears(1)), equalTo("1 year"));
        assertThat(ColumnIntervals.format(Period.of(1, 2, 0)), nullValue());
    }
}
