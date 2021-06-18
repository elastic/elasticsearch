/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.process;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.persistence.JobDataCountsPersister;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.DataCounts;

import java.util.Arrays;
import java.util.Date;

import static org.mockito.Mockito.mock;

/**
 * Dummy DataCountsReporter for testing
 */
class DummyDataCountsReporter extends DataCountsReporter {

    int logStatusCallCount = 0;

    DummyDataCountsReporter() {
        super(createJob(), new DataCounts("DummyJobId"), mock(JobDataCountsPersister.class));
    }

    /**
     * It's difficult to use mocking to get the number of calls to {@link #logStatus(long)}
     * and Mockito.spy() doesn't work due to the lambdas used in {@link DataCountsReporter}.
     * Override the method here an count the calls
     */
    @Override
    protected boolean logStatus(long totalRecords) {
        boolean messageLogged = super.logStatus(totalRecords);
        if (messageLogged) {
            ++logStatusCallCount;
        }

        return messageLogged;
    }

    /**
     * @return Then number of times {@link #logStatus(long)} was called.
     */
    public int getLogStatusCallCount() {
        return logStatusCallCount;
    }

    private static Job createJob() {
        AnalysisConfig.Builder acBuilder = new AnalysisConfig.Builder(
                Arrays.asList(new Detector.Builder("metric", "field").build()));
        acBuilder.setBucketSpan(TimeValue.timeValueSeconds(300));
        acBuilder.setLatency(TimeValue.ZERO);
        acBuilder.setDetectors(Arrays.asList(new Detector.Builder("metric", "field").build()));

        Job.Builder builder = new Job.Builder("dummy_job_id");
        builder.setDataDescription(new DataDescription.Builder());
        builder.setAnalysisConfig(acBuilder);
        return builder.build(new Date());
    }
}
