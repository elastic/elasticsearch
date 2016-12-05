/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.integration;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.prelert.PrelertPlugin;
import org.elasticsearch.xpack.prelert.action.OpenJobAction;
import org.elasticsearch.xpack.prelert.action.PutJobAction;
import org.elasticsearch.xpack.prelert.action.ScheduledJobsIT;
import org.elasticsearch.xpack.prelert.job.AnalysisConfig;
import org.elasticsearch.xpack.prelert.job.DataDescription;
import org.elasticsearch.xpack.prelert.job.Detector;
import org.elasticsearch.xpack.prelert.job.Job;
import org.elasticsearch.xpack.prelert.job.manager.AutodetectProcessManager;
import org.junit.After;

import java.util.Collection;
import java.util.Collections;

@ESIntegTestCase.ClusterScope(numDataNodes = 1)
public class TooManyJobsIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(PrelertPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return nodePlugins();
    }

    @After
    public void clearPrelertMetadata() throws Exception {
        ScheduledJobsIT.clearPrelertMetadata(client());
        client().admin().cluster().prepareUpdateSettings()
                .setPersistentSettings(
                        Settings.builder().put(AutodetectProcessManager.MAX_RUNNING_JOBS_PER_NODE.getKey(), (String) null)
                ).get();
    }

    public void testCannotStartTooManyAnalyticalProcesses() throws Exception {
        int maxRunningJobsPerNode = randomIntBetween(1, 16);
        logger.info("Setting [{}] to [{}]", AutodetectProcessManager.MAX_RUNNING_JOBS_PER_NODE.getKey(), maxRunningJobsPerNode);
        client().admin().cluster().prepareUpdateSettings()
                .setPersistentSettings(Settings.builder()
                        .put(AutodetectProcessManager.MAX_RUNNING_JOBS_PER_NODE.getKey(), maxRunningJobsPerNode)
                ).get();
        for (int i = 1; i <= (maxRunningJobsPerNode + 1); i++) {
            Job.Builder job = createJob(Integer.toString(i));
            PutJobAction.Request putJobRequest = new PutJobAction.Request(job.build(true));
            PutJobAction.Response putJobResponse = client().execute(PutJobAction.INSTANCE, putJobRequest).get();
            assertTrue(putJobResponse.isAcknowledged());

            try {
                OpenJobAction.Request openJobRequest = new OpenJobAction.Request(job.getId());
                openJobRequest.setOpenTimeout(TimeValue.timeValueSeconds(10));
                OpenJobAction.Response openJobResponse = client().execute(OpenJobAction.INSTANCE, openJobRequest)
                        .get();
                assertTrue(openJobResponse.isAcknowledged());
                logger.info("Opened {}th job", i);
            } catch (Exception e) {
                Throwable cause = ExceptionsHelper.unwrapCause(e.getCause());
                if (ElasticsearchStatusException.class.equals(cause.getClass()) == false) {
                    logger.warn("Unexpected cause", e);
                }
                assertEquals(ElasticsearchStatusException.class, cause.getClass());
                assertEquals(RestStatus.CONFLICT, ((ElasticsearchStatusException) cause).status());
                assertEquals("[" + (maxRunningJobsPerNode + 1) + "] expected job status [OPENED], but got [FAILED], reason " +
                        "[failed to open, max running job capacity [" + maxRunningJobsPerNode + "] reached]", cause.getMessage());
                logger.info("good news everybody --> reached maximum number of allowed opened jobs, after trying to open the {}th job", i);

                // now manually clean things up and see if we can succeed to start one new job
                clearPrelertMetadata();
                putJobResponse = client().execute(PutJobAction.INSTANCE, putJobRequest).get();
                assertTrue(putJobResponse.isAcknowledged());
                OpenJobAction.Response openJobResponse = client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(job.getId()))
                        .get();
                assertTrue(openJobResponse.isAcknowledged());
                return;
            }
        }

        fail("shouldn't be able to add more than [" + maxRunningJobsPerNode + "] jobs");
    }

    private Job.Builder createJob(String id) {
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setFormat(DataDescription.DataFormat.JSON);
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
