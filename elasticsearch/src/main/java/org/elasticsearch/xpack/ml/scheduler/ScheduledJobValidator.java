/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.scheduler;

import org.elasticsearch.xpack.ml.job.AnalysisConfig;
import org.elasticsearch.xpack.ml.job.Job;
import org.elasticsearch.xpack.ml.job.messages.Messages;

public final class ScheduledJobValidator {

    private ScheduledJobValidator() {}

    /**
     * Validates a schedulerConfig in relation to the job it refers to
     * @param schedulerConfig the scheduler config
     * @param job the job
     */
    public static void validate(SchedulerConfig schedulerConfig, Job job) {
        AnalysisConfig analysisConfig = job.getAnalysisConfig();
        if (analysisConfig.getLatency() != null && analysisConfig.getLatency() > 0) {
            throw new IllegalArgumentException(Messages.getMessage(Messages.SCHEDULER_DOES_NOT_SUPPORT_JOB_WITH_LATENCY));
        }
        if (schedulerConfig.getAggregations() != null && !SchedulerConfig.DOC_COUNT.equals(analysisConfig.getSummaryCountFieldName())) {
            throw new IllegalArgumentException(
                    Messages.getMessage(Messages.SCHEDULER_AGGREGATIONS_REQUIRES_JOB_WITH_SUMMARY_COUNT_FIELD, SchedulerConfig.DOC_COUNT));
        }
    }
}
