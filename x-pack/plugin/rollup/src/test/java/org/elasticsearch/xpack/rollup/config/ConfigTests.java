/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rollup.config;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.rollup.job.DateHistoGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.GroupConfig;
import org.elasticsearch.xpack.core.rollup.job.HistoGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.MetricConfig;
import org.elasticsearch.xpack.core.rollup.job.RollupJob;
import org.elasticsearch.xpack.core.rollup.job.RollupJobConfig;
import org.elasticsearch.xpack.core.rollup.job.TermsGroupConfig;
import org.elasticsearch.xpack.core.rollup.ConfigTestHelpers;
import org.joda.time.DateTimeZone;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class ConfigTests extends ESTestCase {

    public void testEmptyField() {
        MetricConfig.Builder config = ConfigTestHelpers.getMetricConfig();
        config.setField(null);
        Exception e = expectThrows(IllegalArgumentException.class, config::build);
        assertThat(e.getMessage(), equalTo("Parameter [field] must be a non-null, non-empty string."));

        config.setField("");
        e = expectThrows(IllegalArgumentException.class, config::build);
        assertThat(e.getMessage(), equalTo("Parameter [field] must be a non-null, non-empty string."));
    }

    public void testEmptyMetrics() {
        MetricConfig.Builder config = ConfigTestHelpers.getMetricConfig();
        config.setMetrics(null);
        Exception e = expectThrows(IllegalArgumentException.class, config::build);
        assertThat(e.getMessage(), equalTo("Parameter [metrics] must be a non-null, non-empty array of strings."));

        config.setMetrics(Collections.emptyList());
        e = expectThrows(IllegalArgumentException.class, config::build);
        assertThat(e.getMessage(), equalTo("Parameter [metrics] must be a non-null, non-empty array of strings."));
    }

    public void testEmptyGroup() {
        GroupConfig.Builder groupConfig = ConfigTestHelpers.getGroupConfig();
        groupConfig.setDateHisto(null);
        groupConfig.setTerms(null);
        groupConfig.setHisto(null);

        Exception e = expectThrows(IllegalArgumentException.class, groupConfig::build);
        assertThat(e.getMessage(), equalTo("A date_histogram group is mandatory"));
    }

    public void testNoDateHisto() {
        GroupConfig.Builder groupConfig = new GroupConfig.Builder();
        groupConfig.setTerms(ConfigTestHelpers.getTerms().build());
        groupConfig.setHisto(ConfigTestHelpers.getHisto().build());

        Exception e = expectThrows(IllegalArgumentException.class, groupConfig::build);
        assertThat(e.getMessage(), equalTo("A date_histogram group is mandatory"));
    }

    public void testEmptyGroupAndMetrics() {
        RollupJobConfig.Builder job = ConfigTestHelpers.getRollupJob("foo");
        job.setGroupConfig(null);
        job.setMetricsConfig(null);

        Exception e = expectThrows(IllegalArgumentException.class, job::build);
        assertThat(e.getMessage(), equalTo("At least one grouping or metric must be configured."));
    }

    public void testEmptyJobID() {
        RollupJobConfig.Builder job = ConfigTestHelpers.getRollupJob(null);
        Exception e = expectThrows(IllegalArgumentException.class, job::build);
        assertThat(e.getMessage(), equalTo("An ID is mandatory."));

        job = ConfigTestHelpers.getRollupJob("");
        e = expectThrows(IllegalArgumentException.class, job::build);
        assertThat(e.getMessage(), equalTo("An ID is mandatory."));

        job.setId("");
        e = expectThrows(IllegalArgumentException.class, job::build);
        assertThat(e.getMessage(), equalTo("An ID is mandatory."));

        job.setId(null);
        e = expectThrows(IllegalArgumentException.class, job::build);
        assertThat(e.getMessage(), equalTo("An ID is mandatory."));
    }

    public void testEmptyCron() {
        RollupJobConfig.Builder job = ConfigTestHelpers.getRollupJob("foo");
        job.setCron("");
        Exception e = expectThrows(IllegalArgumentException.class, job::build);
        assertThat(e.getMessage(), equalTo("A cron schedule is mandatory."));

        job.setCron(null);
        e = expectThrows(IllegalArgumentException.class, job::build);
        assertThat(e.getMessage(), equalTo("A cron schedule is mandatory."));
    }

    public void testBadCron() {
        RollupJobConfig.Builder job = ConfigTestHelpers.getRollupJob("foo");
        job.setCron("0 * * *");
        Exception e = expectThrows(IllegalArgumentException.class, job::build);
        assertThat(e.getMessage(), equalTo("invalid cron expression [0 * * *]"));
    }

    public void testEmptyIndexPattern() {
        RollupJobConfig.Builder job = ConfigTestHelpers.getRollupJob("foo");
        job.setIndexPattern("");
        Exception e = expectThrows(IllegalArgumentException.class, job::build);
        assertThat(e.getMessage(), equalTo("An index pattern is mandatory."));

        job.setIndexPattern(null);
        e = expectThrows(IllegalArgumentException.class, job::build);
        assertThat(e.getMessage(), equalTo("An index pattern is mandatory."));
    }

    public void testMatchAllIndexPattern() {
        RollupJobConfig.Builder job = ConfigTestHelpers.getRollupJob("foo");
        job.setIndexPattern("*");
        Exception e = expectThrows(IllegalArgumentException.class, job::build);
        assertThat(e.getMessage(), equalTo("Index pattern must not match all indices (as it would match it's own rollup index"));
    }

    public void testMatchOwnRollupPatternPrefix() {
        RollupJobConfig.Builder job = ConfigTestHelpers.getRollupJob("foo");
        job.setIndexPattern("foo-*");
        job.setRollupIndex("foo-rollup");
        Exception e = expectThrows(IllegalArgumentException.class, job::build);
        assertThat(e.getMessage(), equalTo("Index pattern would match rollup index name which is not allowed."));
    }

    public void testMatchOwnRollupPatternSuffix() {
        RollupJobConfig.Builder job = ConfigTestHelpers.getRollupJob("foo");
        job.setIndexPattern("*-rollup");
        job.setRollupIndex("foo-rollup");
        Exception e = expectThrows(IllegalArgumentException.class, job::build);
        assertThat(e.getMessage(), equalTo("Index pattern would match rollup index name which is not allowed."));
    }

    public void testIndexPatternIdenticalToRollup() {
        RollupJobConfig.Builder job = ConfigTestHelpers.getRollupJob("foo");
        job.setIndexPattern("foo");
        job.setRollupIndex("foo");
        Exception e = expectThrows(IllegalArgumentException.class, job::build);
        assertThat(e.getMessage(), equalTo("Rollup index may not be the same as the index pattern."));
    }

    public void testEmptyRollupIndex() {
        RollupJobConfig.Builder job = ConfigTestHelpers.getRollupJob("foo");
        job.setRollupIndex("");
        Exception e = expectThrows(IllegalArgumentException.class, job::build);
        assertThat(e.getMessage(), equalTo("A rollup index name is mandatory."));

        job.setRollupIndex(null);
        e = expectThrows(IllegalArgumentException.class, job::build);
        assertThat(e.getMessage(), equalTo("A rollup index name is mandatory."));
    }

    public void testBadSize() {
        RollupJobConfig.Builder job = ConfigTestHelpers.getRollupJob("foo");
        job.setPageSize(-1);
        Exception e = expectThrows(IllegalArgumentException.class, job::build);
        assertThat(e.getMessage(), equalTo("Parameter [page_size] is mandatory and  must be a positive long."));

        job.setPageSize(0);
        e = expectThrows(IllegalArgumentException.class, job::build);
        assertThat(e.getMessage(), equalTo("Parameter [page_size] is mandatory and  must be a positive long."));
    }

    public void testEmptyDateHistoField() {
        DateHistoGroupConfig.Builder config = ConfigTestHelpers.getDateHisto();
        config.setField(null);
        Exception e = expectThrows(IllegalArgumentException.class, config::build);
        assertThat(e.getMessage(), equalTo("Parameter [field] is mandatory."));

        config.setField("");
        e = expectThrows(IllegalArgumentException.class, config::build);
        assertThat(e.getMessage(), equalTo("Parameter [field] is mandatory."));
    }

    public void testEmptyDateHistoInterval() {
        DateHistoGroupConfig.Builder config = ConfigTestHelpers.getDateHisto();
        config.setField("foo");
        config.setInterval(null);
        Exception e = expectThrows(IllegalArgumentException.class, config::build);
        assertThat(e.getMessage(), equalTo("Parameter [interval] is mandatory."));
    }

    public void testNullTimeZone() {
        DateHistoGroupConfig.Builder config = ConfigTestHelpers.getDateHisto();
        config.setTimeZone(null);
        DateHistoGroupConfig finalConfig = config.build();
        assertThat(finalConfig.getTimeZone(), equalTo(DateTimeZone.UTC));
    }

    public void testEmptyHistoField() {
        HistoGroupConfig.Builder config = ConfigTestHelpers.getHisto();
        config.setFields(null);
        Exception e = expectThrows(IllegalArgumentException.class, config::build);
        assertThat(e.getMessage(), equalTo("Parameter [fields] must have at least one value."));

        config.setFields(Collections.emptyList());
        e = expectThrows(IllegalArgumentException.class, config::build);
        assertThat(e.getMessage(), equalTo("Parameter [fields] must have at least one value."));
    }

    public void testBadHistoIntervals() {
        HistoGroupConfig.Builder config = new HistoGroupConfig.Builder().setFields(Collections.singletonList("foo"));
        Exception e = expectThrows(IllegalArgumentException.class, config::build);
        assertThat(e.getMessage(), equalTo("Parameter [interval] must be a positive long."));

        config.setInterval(-1);
        e = expectThrows(IllegalArgumentException.class, config::build);
        assertThat(e.getMessage(), equalTo("Parameter [interval] must be a positive long."));
    }

    public void testEmptyTermsField() {
        TermsGroupConfig.Builder config = ConfigTestHelpers.getTerms();
        config.setFields(null);
        Exception e = expectThrows(IllegalArgumentException.class, config::build);
        assertThat(e.getMessage(), equalTo("Parameter [fields] must have at least one value."));

        config.setFields(Collections.emptyList());
        e = expectThrows(IllegalArgumentException.class, config::build);
        assertThat(e.getMessage(), equalTo("Parameter [fields] must have at least one value."));
    }

    public void testNoHeadersInJSON() {
        Map<String, String> headers = new HashMap<>(1);
        headers.put("es-security-runas-user", "foo");
        headers.put("_xpack_security_authentication", "bar");
        RollupJobConfig config = ConfigTestHelpers.getRollupJob(randomAlphaOfLength(5)).build();
        RollupJob job = new RollupJob(config, headers);
        String json = job.toString();
        assertFalse(json.contains("authentication"));
        assertFalse(json.contains("security"));
    }

    public void testUnsupportedMetric() {
        MetricConfig.Builder config = ConfigTestHelpers.getMetricConfig();
        config.setMetrics(Arrays.asList("max","foo"));
        Exception e = expectThrows(IllegalArgumentException.class, config::build);
        assertThat(e.getMessage(), equalTo("Unsupported metric [foo].  Supported metrics include: [max, min, sum, avg, value_count]"));
    }
}
