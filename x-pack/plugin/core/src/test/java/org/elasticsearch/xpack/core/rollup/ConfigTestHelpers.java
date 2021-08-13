/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.rollup;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.rollup.job.DateHistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.GroupConfig;
import org.elasticsearch.xpack.core.rollup.job.HistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.MetricConfig;
import org.elasticsearch.xpack.core.rollup.job.RollupJobConfig;
import org.elasticsearch.xpack.core.rollup.job.TermsGroupConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.carrotsearch.randomizedtesting.generators.RandomNumbers.randomIntBetween;
import static com.carrotsearch.randomizedtesting.generators.RandomPicks.randomFrom;
import static com.carrotsearch.randomizedtesting.generators.RandomStrings.randomAsciiAlphanumOfLengthBetween;
import static org.elasticsearch.test.ESTestCase.randomSubsetOf;
import static org.elasticsearch.test.ESTestCase.randomZone;

public class ConfigTestHelpers {

    private static final String[] TIME_SUFFIXES = new String[]{"d", "h", "ms", "s", "m"};

    private ConfigTestHelpers() {
    }

    public static RollupJobConfig randomRollupJobConfig(final Random random) {
        return randomRollupJobConfig(random, randomAsciiAlphanumOfLengthBetween(random, 5, 20));
    }
    public static RollupJobConfig randomRollupJobConfig(final Random random, final String id) {
        return randomRollupJobConfig(random, id, randomAsciiAlphanumOfLengthBetween(random, 5, 20));
    }

    public static RollupJobConfig randomRollupJobConfig(final Random random, final String id, final String indexPattern) {
        return randomRollupJobConfig(random, id, indexPattern, "rollup_" + indexPattern);
    }

    public static RollupJobConfig randomRollupJobConfig(final Random random,
                                                        final String id,
                                                        final String indexPattern,
                                                        final String rollupIndex) {
        final String cron = randomCron();
        final int pageSize = randomIntBetween(random, 1, 10);
        final TimeValue timeout = random.nextBoolean() ? null : randomTimeout(random);
        final GroupConfig groups = randomGroupConfig(random);
        final List<MetricConfig> metrics = random.nextBoolean() ? null : randomMetricsConfigs(random);
        return new RollupJobConfig(id, indexPattern, rollupIndex, cron, pageSize, groups, metrics, timeout);
    }

    public static GroupConfig randomGroupConfig(final Random random) {
        DateHistogramGroupConfig dateHistogram = randomDateHistogramGroupConfig(random);
        HistogramGroupConfig histogram = random.nextBoolean() ? randomHistogramGroupConfig(random) : null;
        TermsGroupConfig terms = random.nextBoolean() ? randomTermsGroupConfig(random) : null;
        return new GroupConfig(dateHistogram, histogram, terms);
    }

    public static RollupActionGroupConfig randomRollupActionGroupConfig(final Random random) {
        RollupActionDateHistogramGroupConfig dateHistogram = randomRollupActionDateHistogramGroupConfig(random);
        HistogramGroupConfig histogram = random.nextBoolean() ? randomHistogramGroupConfig(random) : null;
        TermsGroupConfig terms = random.nextBoolean() ? randomTermsGroupConfig(random) : null;
        return new RollupActionGroupConfig(dateHistogram, histogram, terms);
    }

    public static RollupActionDateHistogramGroupConfig randomRollupActionDateHistogramGroupConfig(final Random random) {
        final String field = randomField(random);
        final String timezone = random.nextBoolean() ? randomZone().getId() : null;
        if (random.nextBoolean()) {
            return new RollupActionDateHistogramGroupConfig.FixedInterval(field, randomInterval(), timezone);
        } else {
            List<String> units = new ArrayList<>(DateHistogramAggregationBuilder.DATE_FIELD_UNITS.keySet());
            Collections.shuffle(units, random);
            return new RollupActionDateHistogramGroupConfig.CalendarInterval(field, new DateHistogramInterval(units.get(0)), timezone);
        }
    }

    public static DateHistogramGroupConfig randomDateHistogramGroupConfig(final Random random) {
        return randomDateHistogramGroupConfigWithField(random, randomField(random));
    }

    public static DateHistogramGroupConfig randomDateHistogramGroupConfigWithField(final Random random, final String field) {
        final DateHistogramInterval delay = random.nextBoolean() ? randomInterval() : null;
        final String timezone = random.nextBoolean() ? randomZone().getId() : null;
        if (random.nextBoolean()) {
            return new DateHistogramGroupConfig.FixedInterval(field, randomInterval(), delay, timezone);
        } else {
            int i = random.nextInt(DateHistogramAggregationBuilder.DATE_FIELD_UNITS.size());
            List<String> units = new ArrayList<>(DateHistogramAggregationBuilder.DATE_FIELD_UNITS.keySet());
            Collections.shuffle(units, random);
            return new DateHistogramGroupConfig.CalendarInterval(field, new DateHistogramInterval(units.get(0)), delay, timezone);
        }
    }

    public static  List<String> getFields() {
        return IntStream.range(0, ESTestCase.randomIntBetween(1, 10))
                .mapToObj(n -> ESTestCase.randomAlphaOfLengthBetween(5, 10))
                .collect(Collectors.toList());
    }

    public static String randomCron() {
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

    public static List<MetricConfig> randomMetricsConfigs(final Random random) {
        final int numMetrics = randomIntBetween(random, 1, 10);
        final List<MetricConfig> metrics = new ArrayList<>(numMetrics);
        for (int i = 0; i < numMetrics; i++) {
            metrics.add(randomMetricConfig(random));
        }
        return Collections.unmodifiableList(metrics);
    }

    public static MetricConfig randomMetricConfig(final Random random) {
        final String field = randomAsciiAlphanumOfLengthBetween(random, 15, 25);  // large names so we don't accidentally collide
        return randomMetricConfigWithFieldAndMetrics(random, field, RollupField.SUPPORTED_METRICS);
    }

    public static MetricConfig randomMetricConfigWithFieldAndMetrics(final Random random, String field, Collection<String> metrics) {
        return new MetricConfig(field, Collections.unmodifiableList(randomSubsetOf(randomIntBetween(random, 1, metrics.size()), metrics)));
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

    public static String randomField(final Random random) {
        return randomAsciiAlphanumOfLengthBetween(random, 5, 10);
    }

    private static String randomPositiveTimeValue() {
        return ESTestCase.randomIntBetween(1, 1000) + ESTestCase.randomFrom(TIME_SUFFIXES);
    }

    public static DateHistogramInterval randomInterval() {
        return new DateHistogramInterval(randomPositiveTimeValue());
    }

    private static long randomInterval(final Random random) {
        return RandomNumbers.randomLongBetween(random, 1L, Long.MAX_VALUE);
    }

    public static TimeValue randomTimeout(final Random random) {
        return new TimeValue(randomIntBetween(random, 0, 60),
            randomFrom(random, Arrays.asList(TimeUnit.MILLISECONDS, TimeUnit.SECONDS, TimeUnit.MINUTES)));
    }

}
