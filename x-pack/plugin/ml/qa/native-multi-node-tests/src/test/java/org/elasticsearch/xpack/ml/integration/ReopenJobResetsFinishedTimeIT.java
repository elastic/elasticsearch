/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.junit.After;

import java.util.Collections;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.is;

public class ReopenJobResetsFinishedTimeIT extends MlNativeAutodetectIntegTestCase {

    @After
    public void cleanUpTest() {
        cleanUp();
    }

    public void test() {
        final String jobId = "reset-finished-time-test";
        Job.Builder job = createJob(jobId);

        registerJob(job);
        putJob(job);
        openJob(job.getId());

        assertThat(getSingleJob(jobId).getFinishedTime(), is(nullValue()));

        closeJob(jobId);
        assertThat(getSingleJob(jobId).getFinishedTime(), is(notNullValue()));

        openJob(jobId);
        assertThat(getSingleJob(jobId).getFinishedTime(), is(nullValue()));
    }

    private Job getSingleJob(String jobId) {
        return getJob(jobId).get(0);
    }

    private Job.Builder createJob(String id) {
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setFormat(DataDescription.DataFormat.XCONTENT);
        dataDescription.setTimeFormat(DataDescription.EPOCH_MS);

        Detector.Builder d = new Detector.Builder("count", null);
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(d.build()));

        Job.Builder builder = new Job.Builder();
        builder.setId(id);
        builder.setAnalysisConfig(analysisConfig);
        builder.setDataDescription(dataDescription);
        return builder;
    }
}
