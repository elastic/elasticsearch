/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.rollup.config;

import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.rollup.job.DateHistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.DateHistogramGroupConfig.CalendarInterval;
import org.elasticsearch.xpack.core.rollup.job.GroupConfig;
import org.elasticsearch.xpack.core.rollup.job.HistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.MetricConfig;
import org.elasticsearch.xpack.core.rollup.job.RollupJob;
import org.elasticsearch.xpack.core.rollup.job.TermsGroupConfig;
import org.joda.time.DateTimeZone;

import java.time.zone.ZoneRulesException;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.core.rollup.ConfigTestHelpers.randomHistogramGroupConfig;
import static org.elasticsearch.xpack.core.rollup.ConfigTestHelpers.randomRollupJobConfig;
import static org.elasticsearch.xpack.core.rollup.ConfigTestHelpers.randomTermsGroupConfig;
import static org.hamcrest.Matchers.equalTo;
//TODO split this into dedicated unit test classes (one for each config object)
public class ConfigTests extends ESTestCase {

    public void testEmptyField() {
        Exception e = expectThrows(IllegalArgumentException.class, () -> new MetricConfig(null, singletonList("max")));
        assertThat(e.getMessage(), equalTo("Field must be a non-null, non-empty string"));

        e = expectThrows(IllegalArgumentException.class, () -> new MetricConfig("", singletonList("max")));
        assertThat(e.getMessage(), equalTo("Field must be a non-null, non-empty string"));
    }

    public void testEmptyMetrics() {
        Exception e = expectThrows(IllegalArgumentException.class, () -> new MetricConfig("foo", emptyList()));
        assertThat(e.getMessage(), equalTo("Metrics must be a non-null, non-empty array of strings"));

        e = expectThrows(IllegalArgumentException.class, () -> new MetricConfig("foo", null));
        assertThat(e.getMessage(), equalTo("Metrics must be a non-null, non-empty array of strings"));
    }

    public void testEmptyGroup() {
        Exception e = expectThrows(IllegalArgumentException.class, () -> new GroupConfig(null, null, null));
        assertThat(e.getMessage(), equalTo("Date histogram must not be null"));
    }

    public void testNoDateHisto() {
        Exception e = expectThrows(IllegalArgumentException.class,
            () -> new GroupConfig(null, randomHistogramGroupConfig(random()), randomTermsGroupConfig(random())));
        assertThat(e.getMessage(), equalTo("Date histogram must not be null"));
    }

    public void testEmptyDateHistoField() {
        Exception e = expectThrows(IllegalArgumentException.class,
            () -> new CalendarInterval(null, DateHistogramInterval.HOUR));
        assertThat(e.getMessage(), equalTo("Field must be a non-null, non-empty string"));

        e = expectThrows(IllegalArgumentException.class, () -> new CalendarInterval("", DateHistogramInterval.HOUR));
        assertThat(e.getMessage(), equalTo("Field must be a non-null, non-empty string"));
    }

    public void testEmptyDateHistoInterval() {
        Exception e = expectThrows(IllegalArgumentException.class, () -> new CalendarInterval("foo", null));
        assertThat(e.getMessage(), equalTo("Interval must be non-null"));
    }

    public void testNullTimeZone() {
        DateHistogramGroupConfig config = new CalendarInterval("foo", DateHistogramInterval.HOUR, null, null);
        assertThat(config.getTimeZone(), equalTo(DateTimeZone.UTC.getID()));
    }

    public void testEmptyTimeZone() {
        DateHistogramGroupConfig config = new CalendarInterval("foo", DateHistogramInterval.HOUR, null, "");
        assertThat(config.getTimeZone(), equalTo(DateTimeZone.UTC.getID()));
    }

    public void testDefaultTimeZone() {
        DateHistogramGroupConfig config = new CalendarInterval("foo", DateHistogramInterval.HOUR);
        assertThat(config.getTimeZone(), equalTo(DateTimeZone.UTC.getID()));
    }

    public void testUnkownTimeZone() {
        Exception e = expectThrows(ZoneRulesException.class,
            () -> new CalendarInterval("foo", DateHistogramInterval.HOUR, null, "FOO"));
        assertThat(e.getMessage(), equalTo("Unknown time-zone ID: FOO"));
    }

    public void testObsoleteTimeZone() {
        DateHistogramGroupConfig config = new DateHistogramGroupConfig.FixedInterval(
            "foo",
            DateHistogramInterval.HOUR,
            null,
            "Canada/Mountain"
        );
        assertThat(config.getTimeZone(), equalTo("Canada/Mountain"));
    }

    public void testEmptyHistoField() {
        Exception e = expectThrows(IllegalArgumentException.class, () -> new HistogramGroupConfig(1L, (String[]) null));
        assertThat(e.getMessage(), equalTo("Fields must have at least one value"));

        e = expectThrows(IllegalArgumentException.class, () -> new HistogramGroupConfig(1L, new String[0]));
        assertThat(e.getMessage(), equalTo("Fields must have at least one value"));
    }

    public void testBadHistoIntervals() {
        Exception e = expectThrows(IllegalArgumentException.class, () -> new HistogramGroupConfig(0L, "foo", "bar"));
        assertThat(e.getMessage(), equalTo("Interval must be a positive long"));

        e = expectThrows(IllegalArgumentException.class, () -> new HistogramGroupConfig(-1L, "foo", "bar"));
        assertThat(e.getMessage(), equalTo("Interval must be a positive long"));
    }

    public void testEmptyTermsField() {
        final String[] fields = randomBoolean() ? new String[0] : null;
        Exception e = expectThrows(IllegalArgumentException.class, () -> new TermsGroupConfig(fields));
        assertThat(e.getMessage(), equalTo("Fields must have at least one value"));
    }

    public void testNoHeadersInJSON() {
        Map<String, String> headers = new HashMap<>(1);
        headers.put("es-security-runas-user", "foo");
        headers.put("_xpack_security_authentication", "bar");
        RollupJob job = new RollupJob(randomRollupJobConfig(random()), headers);
        String json = job.toString();
        assertFalse(json.contains("authentication"));
        assertFalse(json.contains("security"));
    }
}
