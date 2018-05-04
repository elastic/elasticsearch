/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.core.ml.action.PersistJobAction;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.junit.After;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class PersistJobIT extends MlNativeAutodetectIntegTestCase {

    @After
    public void cleanUpJobs() {
        cleanUp();
    }

    public void testPersistJob() throws Exception {
        String jobId = "persist-job-test";
        runJob(jobId);

        PersistJobAction.Response r = persistJob(jobId);
        assertTrue(r.isPersisted());

        // Persisting the job will create a model snapshot
        assertBusy(() -> {
            List<ModelSnapshot> snapshots = getModelSnapshots(jobId);
            assertFalse(snapshots.isEmpty());
        });
    }

    private void runJob(String jobId) throws Exception {
        TimeValue bucketSpan = TimeValue.timeValueMinutes(5);
        Detector.Builder detector = new Detector.Builder("count", null);
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Arrays.asList(detector.build()));
        analysisConfig.setBucketSpan(bucketSpan);
        Job.Builder job = new Job.Builder(jobId);
        job.setAnalysisConfig(analysisConfig);
        job.setDataDescription(new DataDescription.Builder());
        registerJob(job);
        putJob(job);

        openJob(job.getId());
        List<String> data = generateData(System.currentTimeMillis(), bucketSpan, 10, bucketIndex -> randomIntBetween(10, 20));
        postData(job.getId(), data.stream().collect(Collectors.joining()));
    }
}
