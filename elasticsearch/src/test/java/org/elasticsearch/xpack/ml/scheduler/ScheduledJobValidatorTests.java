/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.scheduler;

import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.job.AnalysisConfig;
import org.elasticsearch.xpack.ml.job.Detector;
import org.elasticsearch.xpack.ml.job.Job;
import org.elasticsearch.xpack.ml.job.messages.Messages;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;

public class ScheduledJobValidatorTests extends ESTestCase {

    public void testValidate_GivenNonZeroLatency() {
        String errorMessage = Messages.getMessage(Messages.SCHEDULER_DOES_NOT_SUPPORT_JOB_WITH_LATENCY);
        Job.Builder builder = buildJobBuilder("foo");
        AnalysisConfig.Builder ac = createAnalysisConfig();
        ac.setBucketSpan(1800L);
        ac.setLatency(3600L);
        builder.setAnalysisConfig(ac);
        Job job = builder.build();
        SchedulerConfig schedulerConfig = createValidSchedulerConfig().build();

        IllegalArgumentException e = ESTestCase.expectThrows(IllegalArgumentException.class,
                () -> ScheduledJobValidator.validate(schedulerConfig, job));

        assertEquals(errorMessage, e.getMessage());
    }

    public void testVerify_GivenZeroLatency() {
        Job.Builder builder = buildJobBuilder("foo");
        AnalysisConfig.Builder ac = createAnalysisConfig();
        ac.setBucketSpan(1800L);
        ac.setLatency(0L);
        builder.setAnalysisConfig(ac);
        Job job = builder.build();
        SchedulerConfig schedulerConfig = createValidSchedulerConfig().build();

        ScheduledJobValidator.validate(schedulerConfig, job);
    }

    public void testVerify_GivenNoLatency() {
        Job.Builder builder = buildJobBuilder("foo");
        AnalysisConfig.Builder ac = createAnalysisConfig();
        ac.setBatchSpan(1800L);
        ac.setBucketSpan(100L);
        builder.setAnalysisConfig(ac);
        Job job = builder.build();
        SchedulerConfig schedulerConfig = createValidSchedulerConfig().build();

        ScheduledJobValidator.validate(schedulerConfig, job);
    }

    public void testVerify_GivenAggsAndCorrectSummaryCountField() throws IOException {
        Job.Builder builder = buildJobBuilder("foo");
        AnalysisConfig.Builder ac = createAnalysisConfig();
        ac.setBucketSpan(1800L);
        ac.setSummaryCountFieldName("doc_count");
        builder.setAnalysisConfig(ac);
        Job job = builder.build();
        SchedulerConfig schedulerConfig = createValidSchedulerConfigWithAggs().build();

        ScheduledJobValidator.validate(schedulerConfig, job);
    }

    public void testVerify_GivenAggsAndNoSummaryCountField() throws IOException {
        String errorMessage = Messages.getMessage(Messages.SCHEDULER_AGGREGATIONS_REQUIRES_JOB_WITH_SUMMARY_COUNT_FIELD,
                SchedulerConfig.DOC_COUNT);
        Job.Builder builder = buildJobBuilder("foo");
        AnalysisConfig.Builder ac = createAnalysisConfig();
        ac.setBucketSpan(1800L);
        builder.setAnalysisConfig(ac);
        Job job = builder.build();
        SchedulerConfig schedulerConfig = createValidSchedulerConfigWithAggs().build();

        IllegalArgumentException e = ESTestCase.expectThrows(IllegalArgumentException.class,
                () -> ScheduledJobValidator.validate(schedulerConfig, job));

        assertEquals(errorMessage, e.getMessage());
    }

    public void testVerify_GivenAggsAndWrongSummaryCountField() throws IOException {
        String errorMessage = Messages.getMessage(
                Messages.SCHEDULER_AGGREGATIONS_REQUIRES_JOB_WITH_SUMMARY_COUNT_FIELD, SchedulerConfig.DOC_COUNT);
        Job.Builder builder = buildJobBuilder("foo");
        AnalysisConfig.Builder ac = createAnalysisConfig();
        ac.setBucketSpan(1800L);
        ac.setSummaryCountFieldName("wrong");
        builder.setAnalysisConfig(ac);
        Job job = builder.build();
        SchedulerConfig schedulerConfig = createValidSchedulerConfigWithAggs().build();

        IllegalArgumentException e = ESTestCase.expectThrows(IllegalArgumentException.class,
                () -> ScheduledJobValidator.validate(schedulerConfig, job));

        assertEquals(errorMessage, e.getMessage());
    }

    public static Job.Builder buildJobBuilder(String id) {
        Job.Builder builder = new Job.Builder(id);
        builder.setCreateTime(new Date());
        AnalysisConfig.Builder ac = createAnalysisConfig();
        builder.setAnalysisConfig(ac);
        return builder;
    }

    public static AnalysisConfig.Builder createAnalysisConfig() {
        Detector.Builder d1 = new Detector.Builder("info_content", "domain");
        d1.setOverFieldName("client");
        Detector.Builder d2 = new Detector.Builder("min", "field");
        AnalysisConfig.Builder ac = new AnalysisConfig.Builder(Arrays.asList(d1.build(), d2.build()));
        return ac;
    }

    private static SchedulerConfig.Builder createValidSchedulerConfigWithAggs() throws IOException {
        SchedulerConfig.Builder schedulerConfig = createValidSchedulerConfig();
        schedulerConfig.setAggregations(new AggregatorFactories.Builder().addAggregator(AggregationBuilders.avg("foo")));
        return schedulerConfig;
    }

    private static SchedulerConfig.Builder createValidSchedulerConfig() {
        SchedulerConfig.Builder builder = new SchedulerConfig.Builder("my-scheduler", "my-job");
        builder.setIndexes(Collections.singletonList("myIndex"));
        builder.setTypes(Collections.singletonList("myType"));
        return builder;
    }
}