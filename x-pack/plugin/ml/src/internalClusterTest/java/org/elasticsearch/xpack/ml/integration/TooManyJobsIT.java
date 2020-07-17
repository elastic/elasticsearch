/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.CloseJobAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.config.JobTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;

public class TooManyJobsIT extends BaseMlIntegTestCase {

    public void testCloseFailedJob() throws Exception {
        startMlCluster(1, 1);

        // create and open first job, which succeeds:
        Job.Builder job = createJob("close-failed-job-1", new ByteSizeValue(2, ByteSizeUnit.MB));
        PutJobAction.Request putJobRequest = new PutJobAction.Request(job);
        client().execute(PutJobAction.INSTANCE, putJobRequest).get();
        client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(job.getId())).get();
        assertBusy(() -> {
            GetJobsStatsAction.Response statsResponse =
                    client().execute(GetJobsStatsAction.INSTANCE, new GetJobsStatsAction.Request("close-failed-job-1")).actionGet();
            assertEquals(statsResponse.getResponse().results().get(0).getState(), JobState.OPENED);
        });

        // create and try to open second job, which fails:
        job = createJob("close-failed-job-2", new ByteSizeValue(2, ByteSizeUnit.MB));
        putJobRequest = new PutJobAction.Request(job);
        client().execute(PutJobAction.INSTANCE, putJobRequest).get();
        expectThrows(ElasticsearchStatusException.class,
                () -> client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request("close-failed-job-2")).actionGet());

        // Ensure that the second job didn't even attempt to be opened and we still have 1 job open:
        GetJobsStatsAction.Response statsResponse =
                client().execute(GetJobsStatsAction.INSTANCE, new GetJobsStatsAction.Request("close-failed-job-2")).actionGet();
        assertEquals(statsResponse.getResponse().results().get(0).getState(), JobState.CLOSED);
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        PersistentTasksCustomMetadata tasks = state.getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
        assertEquals(1, tasks.taskMap().size());
        // now just double check that the first job is still opened:
        PersistentTasksCustomMetadata.PersistentTask<?> task = tasks.getTask(MlTasks.jobTaskId("close-failed-job-1"));
        assertEquals(JobState.OPENED, ((JobTaskState) task.getState()).getState());
    }

    public void testLazyNodeValidation() throws Exception {
        int numNodes = 1;
        int maxNumberOfJobsPerNode = 1;
        int maxNumberOfLazyNodes = 2;
        internalCluster().ensureAtMostNumDataNodes(0);
        logger.info("[{}] is [{}]", MachineLearning.MAX_OPEN_JOBS_PER_NODE.getKey(), maxNumberOfJobsPerNode);
        for (int i = 0; i < numNodes; i++) {
            internalCluster().startNode(Settings.builder()
                .put(MachineLearning.MAX_OPEN_JOBS_PER_NODE.getKey(), maxNumberOfJobsPerNode));
        }
        logger.info("Started [{}] nodes", numNodes);
        ensureStableCluster(numNodes);
        ensureTemplatesArePresent();
        logger.info("[{}] is [{}]", MachineLearning.MAX_LAZY_ML_NODES.getKey(), maxNumberOfLazyNodes);
        // Set our lazy node number
        assertTrue(client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(
                Settings.builder()
                    .put(MachineLearning.MAX_LAZY_ML_NODES.getKey(), maxNumberOfLazyNodes))
            .get()
            .isAcknowledged());
        // create and open first job, which succeeds:
        Job.Builder job = createJob("lazy-node-validation-job-1", new ByteSizeValue(2, ByteSizeUnit.MB));
        PutJobAction.Request putJobRequest = new PutJobAction.Request(job);
        client().execute(PutJobAction.INSTANCE, putJobRequest).get();
        client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(job.getId())).get();
        assertBusy(() -> {
            GetJobsStatsAction.Response statsResponse =
                client().execute(GetJobsStatsAction.INSTANCE,
                    new GetJobsStatsAction.Request("lazy-node-validation-job-1")).actionGet();
            assertEquals(statsResponse.getResponse().results().get(0).getState(), JobState.OPENED);
        });

        // create and try to open second job, which succeeds due to lazy node number:
        job = createJob("lazy-node-validation-job-2", new ByteSizeValue(2, ByteSizeUnit.MB));
        putJobRequest = new PutJobAction.Request(job);
        client().execute(PutJobAction.INSTANCE, putJobRequest).get();
        client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(job.getId())).get(); // Should return while job is opening

        assertBusy(() -> {
            GetJobsStatsAction.Response statsResponse =
                client().execute(GetJobsStatsAction.INSTANCE,
                    new GetJobsStatsAction.Request("lazy-node-validation-job-2")).actionGet();
            // Should get to opening state w/o a node
            assertEquals(JobState.OPENING, statsResponse.getResponse().results().get(0).getState());
        });

        // Add another Node so we can get allocated
        internalCluster().startNode(Settings.builder()
            .put(MachineLearning.MAX_OPEN_JOBS_PER_NODE.getKey(), maxNumberOfJobsPerNode));
        ensureStableCluster(numNodes+1);

        // We should automatically get allocated and opened to new node
        assertBusy(() -> {
            GetJobsStatsAction.Response statsResponse =
                client().execute(GetJobsStatsAction.INSTANCE,
                    new GetJobsStatsAction.Request("lazy-node-validation-job-2")).actionGet();
            assertEquals(JobState.OPENED, statsResponse.getResponse().results().get(0).getState());
        });
    }

    public void testSingleNode() throws Exception {
        verifyMaxNumberOfJobsLimit(1, randomIntBetween(1, 20), randomBoolean());
    }

    public void testMultipleNodes() throws Exception {
        verifyMaxNumberOfJobsLimit(3, randomIntBetween(1, 20), randomBoolean());
    }

    private void verifyMaxNumberOfJobsLimit(int numNodes, int maxNumberOfJobsPerNode, boolean testDynamicChange) throws Exception {
        startMlCluster(numNodes, testDynamicChange ? 1 : maxNumberOfJobsPerNode);
        long maxMlMemoryPerNode = calculateMaxMlMemory();
        ByteSizeValue jobModelMemoryLimit = new ByteSizeValue(2, ByteSizeUnit.MB);
        long memoryFootprintPerJob = jobModelMemoryLimit.getBytes() + Job.PROCESS_MEMORY_OVERHEAD.getBytes();
        long maxJobsPerNodeDueToMemoryLimit = maxMlMemoryPerNode / memoryFootprintPerJob;
        int clusterWideMaxNumberOfJobs = numNodes * maxNumberOfJobsPerNode;
        boolean expectMemoryLimitBeforeCountLimit = maxJobsPerNodeDueToMemoryLimit < maxNumberOfJobsPerNode;
        for (int i = 1; i <= (clusterWideMaxNumberOfJobs + 1); i++) {
            if (i == 2 && testDynamicChange) {
                ClusterUpdateSettingsRequest clusterUpdateSettingsRequest = new ClusterUpdateSettingsRequest().transientSettings(
                        Settings.builder().put(MachineLearning.MAX_OPEN_JOBS_PER_NODE.getKey(), maxNumberOfJobsPerNode).build());
                client().execute(ClusterUpdateSettingsAction.INSTANCE, clusterUpdateSettingsRequest).actionGet();
            }
            Job.Builder job = createJob("max-number-of-jobs-limit-job-" + Integer.toString(i), jobModelMemoryLimit);
            PutJobAction.Request putJobRequest = new PutJobAction.Request(job);
            client().execute(PutJobAction.INSTANCE, putJobRequest).get();

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
                assertEquals("Could not open job because no ML nodes with sufficient capacity were found", e.getMessage());
                IllegalStateException detail = (IllegalStateException) e.getCause();
                assertNotNull(detail);
                String detailedMessage = detail.getMessage();
                assertTrue(detailedMessage,
                    detailedMessage.startsWith("Could not open job because no suitable nodes were found, allocation explanation"));
                if (expectMemoryLimitBeforeCountLimit) {
                    int expectedJobsAlreadyOpenOnNode = (i - 1) / numNodes;
                    assertTrue(detailedMessage,
                        detailedMessage.endsWith("because this node has insufficient available memory. Available memory for ML [" +
                            maxMlMemoryPerNode + "], memory required by existing jobs [" +
                            (expectedJobsAlreadyOpenOnNode * memoryFootprintPerJob) + "], estimated memory required for this job [" +
                            memoryFootprintPerJob + "]]"));
                } else {
                    assertTrue(detailedMessage, detailedMessage.endsWith("because this node is full. Number of opened jobs [" +
                        maxNumberOfJobsPerNode + "], xpack.ml.max_open_jobs [" + maxNumberOfJobsPerNode + "]]"));
                }
                logger.info("good news everybody --> reached maximum number of allowed opened jobs, after trying to open the {}th job", i);

                // close the first job and check if the latest job gets opened:
                CloseJobAction.Request closeRequest = new CloseJobAction.Request("max-number-of-jobs-limit-job-1");
                closeRequest.setCloseTimeout(TimeValue.timeValueSeconds(20L));
                CloseJobAction.Response closeResponse = client().execute(CloseJobAction.INSTANCE, closeRequest).actionGet();
                assertTrue(closeResponse.isClosed());
                client().execute(OpenJobAction.INSTANCE, openJobRequest).actionGet();
                assertBusy(() -> {
                    for (Client client : clients()) {
                        PersistentTasksCustomMetadata tasks = client.admin().cluster().prepareState().get().getState()
                                .getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
                        assertEquals(MlTasks.getJobState(job.getId(), tasks), JobState.OPENED);
                    }
                });
                return;
            }
        }
        fail("shouldn't be able to add more than [" + clusterWideMaxNumberOfJobs + "] jobs");
    }

    private void startMlCluster(int numNodes, int maxNumberOfWorkersPerNode) throws Exception {
        // clear all nodes, so that we can set xpack.ml.max_open_jobs setting:
        internalCluster().ensureAtMostNumDataNodes(0);
        logger.info("[{}] is [{}]", MachineLearning.MAX_OPEN_JOBS_PER_NODE.getKey(), maxNumberOfWorkersPerNode);
        for (int i = 0; i < numNodes; i++) {
            internalCluster().startNode(Settings.builder()
                    .put(MachineLearning.MAX_OPEN_JOBS_PER_NODE.getKey(), maxNumberOfWorkersPerNode));
        }
        logger.info("Started [{}] nodes", numNodes);
        ensureStableCluster(numNodes);
        ensureTemplatesArePresent();
    }

    private long calculateMaxMlMemory() {
        Settings settings = internalCluster().getInstance(Settings.class);
        return Long.parseLong(internalCluster().getInstance(TransportService.class).getLocalNode().getAttributes()
                .get(MachineLearning.MACHINE_MEMORY_NODE_ATTR)) * MachineLearning.MAX_MACHINE_MEMORY_PERCENT.get(settings) / 100;
    }
}
