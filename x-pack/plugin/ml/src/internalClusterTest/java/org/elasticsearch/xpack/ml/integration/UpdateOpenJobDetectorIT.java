/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.action.UpdateJobAction;
import org.elasticsearch.xpack.core.ml.action.UpdateProcessAction;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobUpdate;
import org.elasticsearch.xpack.core.ml.job.config.ModelPlotConfig;
import org.elasticsearch.xpack.ml.job.UpdateJobProcessNotifier;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;

import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

/**
 * Updates of detectors on an open job where the detector update carries only an index and a
 * description, so its rules are {@code null}.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class UpdateOpenJobDetectorIT extends BaseMlIntegTestCase {

    private String putAndOpenJob(String jobId) throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(1);
        ensureStableCluster(1);
        Job.Builder job = createJob(jobId, ByteSizeValue.ofMb(2));
        client().execute(PutJobAction.INSTANCE, new PutJobAction.Request(job)).actionGet();
        ensureYellow();
        client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(jobId)).actionGet();
        return awaitJobOpenedAndAssigned(jobId, null);
    }

    public void testUpdateProcessAction_givenDetectorUpdateWithoutRules() throws Exception {
        String jobId = "update-process-detector-description-only";
        putAndOpenJob(jobId);

        UpdateProcessAction.Response response = client().execute(
            UpdateProcessAction.INSTANCE,
            new UpdateProcessAction.Request(
                jobId,
                new ModelPlotConfig(true, null, false),
                null,
                Collections.singletonList(new JobUpdate.DetectorUpdate(0, "new description", null)),
                null,
                false
            )
        ).actionGet();

        assertTrue(response.isUpdated());
    }

    public void testUpdateJob_givenDetectorDescriptionOnlyOnOpenJob() throws Exception {
        String jobId = "update-job-detector-description-only";
        putAndOpenJob(jobId);

        JobUpdate update = new JobUpdate.Builder(jobId).setModelPlotConfig(new ModelPlotConfig(true, null, false))
            .setDetectorUpdates(Collections.singletonList(new JobUpdate.DetectorUpdate(0, "new description", null)))
            .build();

        Logger notifierLogger = LogManager.getLogger(UpdateJobProcessNotifier.class);
        Level oldLevel = notifierLogger.getLevel();
        Loggers.setLevel(notifierLogger, Level.DEBUG);
        try (MockLog mockLog = MockLog.capture(UpdateJobProcessNotifier.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "process update succeeded",
                    UpdateJobProcessNotifier.class.getCanonicalName(),
                    Level.DEBUG,
                    "Successfully updated remote job [" + jobId + "]"
                )
            );
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "process update failed",
                    UpdateJobProcessNotifier.class.getCanonicalName(),
                    Level.ERROR,
                    "Failed to update remote job [" + jobId + "]"
                )
            );

            Job updated = client().execute(UpdateJobAction.INSTANCE, new UpdateJobAction.Request(jobId, update)).actionGet().getResponse();

            // the config change is persisted regardless of what happens to the running process
            assertThat(updated.getAnalysisConfig().getDetectors().get(0).getDetectorDescription(), equalTo("new description"));
            assertTrue(updated.getModelPlotConfig().isEnabled());
            // and the running process must be updated too
            mockLog.awaitAllExpectationsMatched();
        } finally {
            Loggers.setLevel(notifierLogger, oldLevel);
        }
    }
}
