/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.ml.MlMetadata;
import org.elasticsearch.xpack.ml.action.CloseJobAction;
import org.elasticsearch.xpack.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.ml.action.OpenJobAction;
import org.elasticsearch.xpack.ml.action.PutJobAction;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.job.config.JobTaskStatus;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcessManager;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData;

public class TooManyJobsIT extends BaseMlIntegTestCase {

    public void testCloseFailedJob() throws Exception {
        startMlCluster(1, 1);

        // create and open first job, which succeeds:
        Job.Builder job = createJob("close-failed-job-1");
        PutJobAction.Request putJobRequest = new PutJobAction.Request(job);
        PutJobAction.Response putJobResponse = client().execute(PutJobAction.INSTANCE, putJobRequest).get();
        assertTrue(putJobResponse.isAcknowledged());
        client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(job.getId())).get();
        assertBusy(() -> {
            GetJobsStatsAction.Response statsResponse =
                    client().execute(GetJobsStatsAction.INSTANCE, new GetJobsStatsAction.Request("close-failed-job-1")).actionGet();
            assertEquals(statsResponse.getResponse().results().get(0).getState(), JobState.OPENED);
        });

        // create and try to open second job, which fails:
        job = createJob("close-failed-job-2");
        putJobRequest = new PutJobAction.Request(job);
        putJobResponse = client().execute(PutJobAction.INSTANCE, putJobRequest).get();
        assertTrue(putJobResponse.isAcknowledged());
        expectThrows(ElasticsearchStatusException.class,
                () -> client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request("close-failed-job-2")).actionGet());

        // Ensure that the second job didn't even attempt to be opened and we still have 1 job open:
        GetJobsStatsAction.Response statsResponse =
                client().execute(GetJobsStatsAction.INSTANCE, new GetJobsStatsAction.Request("close-failed-job-2")).actionGet();
        assertEquals(statsResponse.getResponse().results().get(0).getState(), JobState.CLOSED);
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        PersistentTasksCustomMetaData tasks = state.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
        assertEquals(1, tasks.taskMap().size());
        // now just double check that the first job is still opened:
        PersistentTasksCustomMetaData.PersistentTask task = tasks.getTask(MlMetadata.jobTaskId("close-failed-job-1"));
        assertEquals(JobState.OPENED, ((JobTaskStatus) task.getStatus()).getState());
    }

    public void testSingleNode() throws Exception {
        verifyMaxNumberOfJobsLimit(1, randomIntBetween(1, 32));
    }

    public void testMultipleNodes() throws Exception {
        verifyMaxNumberOfJobsLimit(3, randomIntBetween(1, 32));
    }

    private void verifyMaxNumberOfJobsLimit(int numNodes, int maxNumberOfJobsPerNode) throws Exception {
        startMlCluster(numNodes, maxNumberOfJobsPerNode);
        int clusterWideMaxNumberOfJobs = numNodes * maxNumberOfJobsPerNode;
        for (int i = 1; i <= (clusterWideMaxNumberOfJobs + 1); i++) {
            Job.Builder job = createJob("max-number-of-jobs-limit-job-" + Integer.toString(i));
            PutJobAction.Request putJobRequest = new PutJobAction.Request(job);
            PutJobAction.Response putJobResponse = client().execute(PutJobAction.INSTANCE, putJobRequest).get();
            assertTrue(putJobResponse.isAcknowledged());

            OpenJobAction.Request openJobRequest = new OpenJobAction.Request(job.getId());
            try {
                client().execute(OpenJobAction.INSTANCE, openJobRequest).actionGet();
                assertBusy(() -> {
                    GetJobsStatsAction.Response statsResponse =
                            client().execute(GetJobsStatsAction.INSTANCE, new GetJobsStatsAction.Request(job.getId())).actionGet();
                    assertEquals(statsResponse.getResponse().results().get(0).getState(), JobState.OPENED);
                });
                logger.info("Opened {}th job", i);
            } catch (ElasticsearchStatusException e) {
                assertTrue(e.getMessage(), e.getMessage().startsWith("Could not open job because no suitable nodes were found, allocation explanation"));
                assertTrue(e.getMessage(), e.getMessage().endsWith("because this node is full. Number of opened jobs [" + maxNumberOfJobsPerNode +
                        "], xpack.ml.max_open_jobs [" + maxNumberOfJobsPerNode + "]]"));
                logger.info("good news everybody --> reached maximum number of allowed opened jobs, after trying to open the {}th job", i);

                // close the first job and check if the latest job gets opened:
                CloseJobAction.Request closeRequest = new CloseJobAction.Request("max-number-of-jobs-limit-job-1");
                closeRequest.setCloseTimeout(TimeValue.timeValueSeconds(20L));
                CloseJobAction.Response closeResponse = client().execute(CloseJobAction.INSTANCE, closeRequest).actionGet();
                assertTrue(closeResponse.isClosed());
                client().execute(OpenJobAction.INSTANCE, openJobRequest).actionGet();
                assertBusy(() -> {
                    for (Client client : clients()) {
                        PersistentTasksCustomMetaData tasks = client.admin().cluster().prepareState().get().getState()
                                .getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
                        assertEquals(MlMetadata.getJobState(job.getId(), tasks), JobState.OPENED);
                    }
                });
                return;
            }
        }
        fail("shouldn't be able to add more than [" + clusterWideMaxNumberOfJobs + "] jobs");
    }

    private void startMlCluster(int numNodes, int maxNumberOfJobsPerNode) throws Exception {
        // clear all nodes, so that we can set xpack.ml.max_open_jobs setting:
        internalCluster().ensureAtMostNumDataNodes(0);
        logger.info("[{}] is [{}]", AutodetectProcessManager.MAX_OPEN_JOBS_PER_NODE.getKey(), maxNumberOfJobsPerNode);
        for (int i = 0; i < numNodes; i++) {
            internalCluster().startNode(Settings.builder()
                    .put(AutodetectProcessManager.MAX_OPEN_JOBS_PER_NODE.getKey(), maxNumberOfJobsPerNode));
        }
        logger.info("Started [{}] nodes", numNodes);
        ensureStableCluster(numNodes);
    }

}
