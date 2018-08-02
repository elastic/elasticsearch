/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.rollup;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.rollup.job.DateHistoGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.GroupConfig;
import org.elasticsearch.xpack.core.rollup.job.HistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.MetricConfig;
import org.elasticsearch.xpack.core.rollup.job.RollupJobConfig;
import org.elasticsearch.xpack.core.rollup.job.TermsGroupConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.carrotsearch.randomizedtesting.generators.RandomNumbers.randomIntBetween;
import static com.carrotsearch.randomizedtesting.generators.RandomStrings.randomAsciiAlphanumOfLengthBetween;

public class ConfigTestHelpers {

    public static RollupJobConfig.Builder getRollupJob(String jobId) {
        RollupJobConfig.Builder builder = new RollupJobConfig.Builder();
        builder.setId(jobId);
        builder.setCron(getCronString());
        builder.setTimeout(new TimeValue(ESTestCase.randomIntBetween(1,100)));
        String indexPattern = ESTestCase.randomAlphaOfLengthBetween(1,10);
        builder.setIndexPattern(indexPattern);
        builder.setRollupIndex("rollup_" + indexPattern); // to ensure the index pattern != rollup index
        builder.setGroupConfig(ConfigTestHelpers.getGroupConfig().build());
        builder.setPageSize(ESTestCase.randomIntBetween(1,10));
        if (ESTestCase.randomBoolean()) {
            List<MetricConfig> metrics = IntStream.range(1, ESTestCase.randomIntBetween(1,10))
                    .mapToObj(n -> ConfigTestHelpers.getMetricConfig().build())
                    .collect(Collectors.toList());

            builder.setMetricsConfig(metrics);
        }
        return builder;
    }

    public static GroupConfig.Builder getGroupConfig() {
        GroupConfig.Builder groupBuilder = new GroupConfig.Builder();
        groupBuilder.setDateHisto(getDateHisto().build());
        if (ESTestCase.randomBoolean()) {
            groupBuilder.setHisto(randomHistogramGroupConfig(ESTestCase.random()));
        }
        if (ESTestCase.randomBoolean()) {
            groupBuilder.setTerms(randomTermsGroupConfig(ESTestCase.random()));
        }
        return groupBuilder;
    }

    public static MetricConfig.Builder getMetricConfig() {
        MetricConfig.Builder builder = new MetricConfig.Builder();
        builder.setField(ESTestCase.randomAlphaOfLength(15));  // large names so we don't accidentally collide
        List<String> metrics = new ArrayList<>();
        if (ESTestCase.randomBoolean()) {
            metrics.add("min");
        }
        if (ESTestCase.randomBoolean()) {
            metrics.add("max");
        }
        if (ESTestCase.randomBoolean()) {
            metrics.add("sum");
        }
        if (ESTestCase.randomBoolean()) {
            metrics.add("avg");
        }
        if (ESTestCase.randomBoolean()) {
            metrics.add("value_count");
        }
        if (metrics.size() == 0) {
            metrics.add("min");
        }
        builder.setMetrics(metrics);
        return builder;
    }

    private static final String[] TIME_SUFFIXES = new String[]{"d", "h", "ms", "s", "m"};
    public static String randomPositiveTimeValue() {
        return ESTestCase.randomIntBetween(1, 1000) + ESTestCase.randomFrom(TIME_SUFFIXES);
    }

    public static DateHistoGroupConfig.Builder getDateHisto() {
        DateHistoGroupConfig.Builder dateHistoBuilder = new DateHistoGroupConfig.Builder();
        dateHistoBuilder.setInterval(new DateHistogramInterval(randomPositiveTimeValue()));
        if (ESTestCase.randomBoolean()) {
            dateHistoBuilder.setTimeZone(ESTestCase.randomDateTimeZone());
        }
        if (ESTestCase.randomBoolean()) {
            dateHistoBuilder.setDelay(new DateHistogramInterval(randomPositiveTimeValue()));
        }
        dateHistoBuilder.setField(ESTestCase.randomAlphaOfLengthBetween(5, 10));
        return dateHistoBuilder;
    }

    public static  List<String> getFields() {
        return IntStream.range(0, ESTestCase.randomIntBetween(1, 10))
                .mapToObj(n -> ESTestCase.randomAlphaOfLengthBetween(5, 10))
                .collect(Collectors.toList());
    }

    public static String getCronString() {
        return (ESTestCase.randomBoolean() ? "*" : String.valueOf(ESTestCase.randomIntBetween(0, 59)))             + //second
                " " + (ESTestCase.randomBoolean() ? "*" : String.valueOf(ESTestCase.randomIntBetween(0, 59)))      + //minute
                " " + (ESTestCase.randomBoolean() ? "*" : String.valueOf(ESTestCase.randomIntBetween(0, 23)))      + //hour
                " " + (ESTestCase.randomBoolean() ? "*" : String.valueOf(ESTestCase.randomIntBetween(1, 31)))      + //day of month
                " " + (ESTestCase.randomBoolean() ? "*" : String.valueOf(ESTestCase.randomIntBetween(1, 12)))      + //month
                " ?"                                                                         + //day of week
                " " + (ESTestCase.randomBoolean() ? "*" : String.valueOf(ESTestCase.randomIntBetween(1970, 2199)));  //year
    }

    public static HistogramGroupConfig randomHistogramGroupConfig(final Random random) {
        return new HistogramGroupConfig(randomInterval(random), randomFields(random));
    }

    public static TermsGroupConfig randomTermsGroupConfig(final Random random) {
        return new TermsGroupConfig(randomFields(random));
    }

    private static String[] randomFields(final Random random) {
        final int numFields = randomIntBetween(random, 1, 10);
        final String[] fields = new String[numFields];
        for (int i = 0; i < numFields; i++) {
            fields[i] = randomField(random);
        }
        return fields;
    }

    private static String randomField(final Random random) {
        return randomAsciiAlphanumOfLengthBetween(random, 5, 10);
    }

    private static long randomInterval(final Random random) {
        return RandomNumbers.randomLongBetween(random, 1L, Long.MAX_VALUE);
    }
}
