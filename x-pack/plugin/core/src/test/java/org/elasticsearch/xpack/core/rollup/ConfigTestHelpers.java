/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.rollup;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.rollup.job.DateHistoGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.GroupConfig;
import org.elasticsearch.xpack.core.rollup.job.HistoGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.MetricConfig;
import org.elasticsearch.xpack.core.rollup.job.RollupJobConfig;
import org.elasticsearch.xpack.core.rollup.job.TermsGroupConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
            groupBuilder.setHisto(getHisto().build());
        }
        if (ESTestCase.randomBoolean()) {
            groupBuilder.setTerms(getTerms().build());
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
        dateHistoBuilder.setField(ESTestCase.randomAlphaOfLengthBetween(1, 10 ));
        return dateHistoBuilder;
    }

    public static HistoGroupConfig.Builder getHisto() {
        HistoGroupConfig.Builder histoBuilder = new HistoGroupConfig.Builder();
        histoBuilder.setInterval(ESTestCase.randomIntBetween(1,10000));
        histoBuilder.setFields(getFields());
        return histoBuilder;
    }

    public static TermsGroupConfig.Builder getTerms() {
        TermsGroupConfig.Builder builder = new TermsGroupConfig.Builder();
        builder.setFields(getFields());
        return builder;
    }

    public static  List<String> getFields() {
        return IntStream.range(0, ESTestCase.randomIntBetween(1,10))
                .mapToObj(n -> ESTestCase.randomAlphaOfLengthBetween(1,10))
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
}
