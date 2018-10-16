/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.rollup.job;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import static com.carrotsearch.randomizedtesting.generators.RandomStrings.randomAsciiAlphanumOfLengthBetween;
import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.core.rollup.ConfigTestHelpers.randomCron;
import static org.elasticsearch.xpack.core.rollup.ConfigTestHelpers.randomDateHistogramGroupConfig;
import static org.elasticsearch.xpack.core.rollup.ConfigTestHelpers.randomHistogramGroupConfig;
import static org.elasticsearch.xpack.core.rollup.ConfigTestHelpers.randomMetricsConfigs;
import static org.elasticsearch.xpack.core.rollup.ConfigTestHelpers.randomRollupJobConfig;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isIn;


public class RollupJobConfigTests extends AbstractSerializingTestCase<RollupJobConfig> {

    private String jobId;

    @Before
    public void setUpOptionalId() {
        jobId = randomAlphaOfLengthBetween(1, 10);
    }

    @Override
    protected RollupJobConfig createTestInstance() {
        return randomRollupJobConfig(random(), jobId);
    }

    @Override
    protected Writeable.Reader<RollupJobConfig> instanceReader() {
        return RollupJobConfig::new;
    }

    @Override
    protected RollupJobConfig doParseInstance(final XContentParser parser) throws IOException {
        if (randomBoolean()) {
            return RollupJobConfig.fromXContent(parser, jobId);
        } else {
            return RollupJobConfig.fromXContent(parser, null);
        }
    }

    public void testEmptyIndexPattern() {
        final RollupJobConfig sample = randomRollupJobConfig(random());

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
            new RollupJobConfig(sample.getId(), null, sample.getRollupIndex(), sample.getCron(), sample.getPageSize(),
                sample.getGroupConfig(), sample.getMetricsConfig(), sample.getTimeout()));
        assertThat(e.getMessage(), equalTo("Index pattern must be a non-null, non-empty string"));

        e = expectThrows(IllegalArgumentException.class, () ->
            new RollupJobConfig(sample.getId(), "", sample.getRollupIndex(), sample.getCron(), sample.getPageSize(),
                sample.getGroupConfig(), sample.getMetricsConfig(), sample.getTimeout()));
        assertThat(e.getMessage(), equalTo("Index pattern must be a non-null, non-empty string"));
    }

    public void testEmptyCron() {
        final RollupJobConfig sample = randomRollupJobConfig(random());

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
            new RollupJobConfig(sample.getId(), sample.getIndexPattern(), sample.getRollupIndex(), null, sample.getPageSize(),
                sample.getGroupConfig(), sample.getMetricsConfig(), sample.getTimeout()));
        assertThat(e.getMessage(), equalTo("Cron schedule must be a non-null, non-empty string"));

        e = expectThrows(IllegalArgumentException.class, () ->
            new RollupJobConfig(sample.getId(), sample.getIndexPattern(), sample.getRollupIndex(), "", sample.getPageSize(),
                sample.getGroupConfig(), sample.getMetricsConfig(), sample.getTimeout()));
        assertThat(e.getMessage(), equalTo("Cron schedule must be a non-null, non-empty string"));
    }

    public void testEmptyID() {
        final RollupJobConfig sample = randomRollupJobConfig(random());

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
            new RollupJobConfig(null, sample.getIndexPattern(), sample.getRollupIndex(), sample.getCron(), sample.getPageSize(),
                sample.getGroupConfig(), sample.getMetricsConfig(), sample.getTimeout()));
        assertThat(e.getMessage(), equalTo("Id must be a non-null, non-empty string"));

        e = expectThrows(IllegalArgumentException.class, () ->
            new RollupJobConfig("", sample.getIndexPattern(), sample.getRollupIndex(), sample.getCron(), sample.getPageSize(),
                sample.getGroupConfig(), sample.getMetricsConfig(), sample.getTimeout()));
        assertThat(e.getMessage(), equalTo("Id must be a non-null, non-empty string"));
    }

    public void testBadCron() {
        final RollupJobConfig sample = randomRollupJobConfig(random());

        Exception e = expectThrows(IllegalArgumentException.class, () ->
            new RollupJobConfig(sample.getId(), sample.getIndexPattern(), sample.getRollupIndex(), "0 * * *", sample.getPageSize(),
                sample.getGroupConfig(), sample.getMetricsConfig(), sample.getTimeout()));
        assertThat(e.getMessage(), equalTo("invalid cron expression [0 * * *]"));
    }

    public void testMatchAllIndexPattern() {
        final RollupJobConfig sample = randomRollupJobConfig(random());

        Exception e = expectThrows(IllegalArgumentException.class, () ->
            new RollupJobConfig(sample.getId(), "*", sample.getRollupIndex(), sample.getCron(), sample.getPageSize(),
                sample.getGroupConfig(), sample.getMetricsConfig(), sample.getTimeout()));
        assertThat(e.getMessage(), equalTo("Index pattern must not match all indices (as it would match it's own rollup index"));
    }

    public void testMatchOwnRollupPatternPrefix() {
        final RollupJobConfig sample = randomRollupJobConfig(random());

        Exception e = expectThrows(IllegalArgumentException.class, () ->
            new RollupJobConfig(sample.getId(), "foo-*", "foo-rollup", sample.getCron(), sample.getPageSize(),
                sample.getGroupConfig(), sample.getMetricsConfig(), sample.getTimeout()));
        assertThat(e.getMessage(), equalTo("Index pattern would match rollup index name which is not allowed"));
    }

    public void testMatchOwnRollupPatternSuffix() {
        final RollupJobConfig sample = randomRollupJobConfig(random());

        Exception e = expectThrows(IllegalArgumentException.class, () ->
            new RollupJobConfig(sample.getId(), "*-rollup", "foo-rollup", sample.getCron(), sample.getPageSize(),
                sample.getGroupConfig(), sample.getMetricsConfig(), sample.getTimeout()));
        assertThat(e.getMessage(), equalTo("Index pattern would match rollup index name which is not allowed"));
    }

    public void testIndexPatternIdenticalToRollup() {
        final RollupJobConfig sample = randomRollupJobConfig(random());

        Exception e = expectThrows(IllegalArgumentException.class, () ->
            new RollupJobConfig(sample.getId(), "foo", "foo", sample.getCron(), sample.getPageSize(),
                sample.getGroupConfig(), sample.getMetricsConfig(), sample.getTimeout()));
        assertThat(e.getMessage(), equalTo("Rollup index may not be the same as the index pattern"));
    }

    public void testEmptyRollupIndex() {
        final RollupJobConfig sample = randomRollupJobConfig(random());
        Exception e = expectThrows(IllegalArgumentException.class, () ->
            new RollupJobConfig(sample.getId(), sample.getIndexPattern(), "", sample.getCron(), sample.getPageSize(),
                sample.getGroupConfig(), sample.getMetricsConfig(), sample.getTimeout()));
        assertThat(e.getMessage(), equalTo("Rollup index must be a non-null, non-empty string"));

        e = expectThrows(IllegalArgumentException.class, () ->
            new RollupJobConfig(sample.getId(), sample.getIndexPattern(), null, sample.getCron(), sample.getPageSize(),
                sample.getGroupConfig(), sample.getMetricsConfig(), sample.getTimeout()));
        assertThat(e.getMessage(), equalTo("Rollup index must be a non-null, non-empty string"));
    }

    public void testBadSize() {
        final RollupJobConfig sample = randomRollupJobConfig(random());

        Exception e = expectThrows(IllegalArgumentException.class, () ->
            new RollupJobConfig(sample.getId(), sample.getIndexPattern(), sample.getRollupIndex(), sample.getCron(), -1,
                sample.getGroupConfig(), sample.getMetricsConfig(), sample.getTimeout()));
        assertThat(e.getMessage(), equalTo("Page size is mandatory and  must be a positive long"));

        e = expectThrows(IllegalArgumentException.class, () ->
            new RollupJobConfig(sample.getId(), sample.getIndexPattern(), sample.getRollupIndex(), sample.getCron(), 0,
                sample.getGroupConfig(), sample.getMetricsConfig(), sample.getTimeout()));
        assertThat(e.getMessage(), equalTo("Page size is mandatory and  must be a positive long"));
    }

    public void testEmptyGroupAndMetrics() {
        final RollupJobConfig sample = randomRollupJobConfig(random());

        Exception e = expectThrows(IllegalArgumentException.class, () ->
            new RollupJobConfig(sample.getId(), sample.getIndexPattern(), sample.getRollupIndex(), sample.getCron(), sample.getPageSize(),
                null, null, sample.getTimeout()));
        assertThat(e.getMessage(), equalTo("At least one grouping or metric must be configured"));

        e = expectThrows(IllegalArgumentException.class, () ->
            new RollupJobConfig(sample.getId(), sample.getIndexPattern(), sample.getRollupIndex(), sample.getCron(), sample.getPageSize(),
                null, emptyList(), sample.getTimeout()));
        assertThat(e.getMessage(), equalTo("At least one grouping or metric must be configured"));
    }

    public void testDefaultFieldsForDateHistograms() {
        final Random random = random();
        DateHistogramGroupConfig dateHistogramGroupConfig = randomDateHistogramGroupConfig(random);
        HistogramGroupConfig histogramGroupConfig1 = randomHistogramGroupConfig(random);
        List<MetricConfig> metrics = new ArrayList<>(randomMetricsConfigs(random));
        for (String histoField : histogramGroupConfig1.getFields()) {
            metrics.add(new MetricConfig(histoField, Arrays.asList("max")));
        }
        GroupConfig groupConfig = new GroupConfig(dateHistogramGroupConfig, histogramGroupConfig1, null);
        RollupJobConfig rollupJobConfig = new RollupJobConfig(
            randomAsciiAlphanumOfLengthBetween(random, 1, 20),
            "indexes_*",
            "rollup_" + randomAsciiAlphanumOfLengthBetween(random, 1, 20),
            randomCron(),
            randomIntBetween(1, 10),
            groupConfig,
            metrics,
            null);
        Set<String> metricFields = rollupJobConfig.getMetricsConfig().stream().map(MetricConfig::getField).collect(Collectors.toSet());
        assertThat(dateHistogramGroupConfig.getField(), isIn(metricFields));
        List<String> histoFields = Arrays.asList(histogramGroupConfig1.getFields());
        rollupJobConfig.getMetricsConfig().forEach(metricConfig -> {
            if (histoFields.contains(metricConfig.getField())) {
                // Since it is explicitly included, the defaults should not be added
                assertThat(metricConfig.getMetrics(), containsInAnyOrder("max"));
            }
            if (metricConfig.getField().equals(dateHistogramGroupConfig.getField())) {
                assertThat(metricConfig.getMetrics(), containsInAnyOrder("max", "min", "value_count"));
            }
        });
    }

    public void testDefaultFieldsForHistograms() {
        final Random random = random();
        DateHistogramGroupConfig dateHistogramGroupConfig = randomDateHistogramGroupConfig(random);
        HistogramGroupConfig histogramGroupConfig1 = randomHistogramGroupConfig(random);
        List<MetricConfig> metrics = new ArrayList<>(randomMetricsConfigs(random));
        metrics.add(new MetricConfig(dateHistogramGroupConfig.getField(), Arrays.asList("max")));
        GroupConfig groupConfig = new GroupConfig(dateHistogramGroupConfig, histogramGroupConfig1, null);
        RollupJobConfig rollupJobConfig = new RollupJobConfig(
            randomAsciiAlphanumOfLengthBetween(random, 1, 20),
            "indexes_*",
            "rollup_" + randomAsciiAlphanumOfLengthBetween(random, 1, 20),
            randomCron(),
            randomIntBetween(1, 10),
            groupConfig,
            metrics,
            null);
        Set<String> metricFields = rollupJobConfig.getMetricsConfig().stream().map(MetricConfig::getField).collect(Collectors.toSet());
        for (String histoField : histogramGroupConfig1.getFields()) {
            assertThat(histoField, isIn(metricFields));
        }
        assertThat(dateHistogramGroupConfig.getField(), isIn(metricFields));
        List<String> histoFields = Arrays.asList(histogramGroupConfig1.getFields());
        rollupJobConfig.getMetricsConfig().forEach(metricConfig -> {
            if (histoFields.contains(metricConfig.getField())) {
                // Since it is explicitly included, the defaults should not be added
                assertThat(metricConfig.getMetrics(), containsInAnyOrder("max", "min", "value_count"));
            }
            if (metricConfig.getField().equals(dateHistogramGroupConfig.getField())) {
                assertThat(metricConfig.getMetrics(), containsInAnyOrder("max"));
            }
        });
    }
}
