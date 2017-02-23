/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.action.GetDatafeedsStatsAction;
import org.elasticsearch.xpack.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.ml.action.OpenJobAction;
import org.elasticsearch.xpack.ml.action.PutDatafeedAction;
import org.elasticsearch.xpack.ml.action.PutJobAction;
import org.elasticsearch.xpack.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;
import org.elasticsearch.xpack.persistent.PersistentTasksInProgress;
import org.elasticsearch.xpack.persistent.PersistentTasksInProgress.PersistentTaskInProgress;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcessManager.MAX_RUNNING_JOBS_PER_NODE;

public class BasicDistributedJobsIT extends BaseMlIntegTestCase {

    public void testFailOverBasics() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(4);
        ensureStableCluster(4);

        Job.Builder job = createJob("job_id");
        PutJobAction.Request putJobRequest = new PutJobAction.Request(job.build());
        PutJobAction.Response putJobResponse = client().execute(PutJobAction.INSTANCE, putJobRequest).get();
        assertTrue(putJobResponse.isAcknowledged());
        ensureGreen();
        OpenJobAction.Request openJobRequest = new OpenJobAction.Request(job.getId());
        client().execute(OpenJobAction.INSTANCE, openJobRequest).get();
        assertBusy(() -> {
            GetJobsStatsAction.Response statsResponse =
                    client().execute(GetJobsStatsAction.INSTANCE, new GetJobsStatsAction.Request(job.getId())).actionGet();
            assertEquals(JobState.OPENED, statsResponse.getResponse().results().get(0).getState());
        });

        internalCluster().stopRandomDataNode();
        ensureStableCluster(3);
        ensureGreen();
        assertBusy(() -> {
            GetJobsStatsAction.Response statsResponse =
                    client().execute(GetJobsStatsAction.INSTANCE, new GetJobsStatsAction.Request(job.getId())).actionGet();
            assertEquals(JobState.OPENED, statsResponse.getResponse().results().get(0).getState());
        });

        internalCluster().stopRandomDataNode();
        ensureStableCluster(2);
        ensureGreen();
        assertBusy(() -> {
            GetJobsStatsAction.Response statsResponse =
                    client().execute(GetJobsStatsAction.INSTANCE, new GetJobsStatsAction.Request(job.getId())).actionGet();
            assertEquals(JobState.OPENED, statsResponse.getResponse().results().get(0).getState());
        });
    }

    public void testFailOverBasics_withDataFeeder() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(4);
        ensureStableCluster(4);

        Job.Builder job = createScheduledJob("job_id");
        PutJobAction.Request putJobRequest = new PutJobAction.Request(job.build());
        PutJobAction.Response putJobResponse = client().execute(PutJobAction.INSTANCE, putJobRequest).get();
        assertTrue(putJobResponse.isAcknowledged());
        DatafeedConfig config = createDatafeed("data_feed_id", job.getId(), Collections.singletonList("*"));
        PutDatafeedAction.Request putDatafeedRequest = new PutDatafeedAction.Request(config);
        PutDatafeedAction.Response putDatadeedResponse = client().execute(PutDatafeedAction.INSTANCE, putDatafeedRequest).get();
        assertTrue(putDatadeedResponse.isAcknowledged());

        ensureGreen();
        OpenJobAction.Request openJobRequest = new OpenJobAction.Request(job.getId());
        client().execute(OpenJobAction.INSTANCE, openJobRequest).get();
        assertBusy(() -> {
            GetJobsStatsAction.Response statsResponse =
                    client().execute(GetJobsStatsAction.INSTANCE, new GetJobsStatsAction.Request(job.getId())).actionGet();
            assertEquals(JobState.OPENED, statsResponse.getResponse().results().get(0).getState());
        });
        StartDatafeedAction.Request startDataFeedRequest = new StartDatafeedAction.Request(config.getId(), 0L);
        client().execute(StartDatafeedAction.INSTANCE, startDataFeedRequest);
        assertBusy(() -> {
            GetDatafeedsStatsAction.Response statsResponse =
                    client().execute(GetDatafeedsStatsAction.INSTANCE, new GetDatafeedsStatsAction.Request(config.getId())).actionGet();
            assertEquals(1, statsResponse.getResponse().results().size());
            assertEquals(DatafeedState.STARTED, statsResponse.getResponse().results().get(0).getDatafeedState());
        });

        internalCluster().stopRandomDataNode();
        ensureStableCluster(3);
        ensureGreen();
        assertBusy(() -> {
            GetJobsStatsAction.Response statsResponse =
                    client().execute(GetJobsStatsAction.INSTANCE, new GetJobsStatsAction.Request(job.getId())).actionGet();
            assertEquals(JobState.OPENED, statsResponse.getResponse().results().get(0).getState());
        });
        assertBusy(() -> {
            GetDatafeedsStatsAction.Response statsResponse =
                    client().execute(GetDatafeedsStatsAction.INSTANCE, new GetDatafeedsStatsAction.Request(config.getId())).actionGet();
            assertEquals(1, statsResponse.getResponse().results().size());
            assertEquals(DatafeedState.STARTED, statsResponse.getResponse().results().get(0).getDatafeedState());
        });

        internalCluster().stopRandomDataNode();
        ensureStableCluster(2);
        ensureGreen();
        assertBusy(() -> {
            GetJobsStatsAction.Response statsResponse =
                    client().execute(GetJobsStatsAction.INSTANCE, new GetJobsStatsAction.Request(job.getId())).actionGet();
            assertEquals(JobState.OPENED, statsResponse.getResponse().results().get(0).getState());
        });
        assertBusy(() -> {
            GetDatafeedsStatsAction.Response statsResponse =
                    client().execute(GetDatafeedsStatsAction.INSTANCE, new GetDatafeedsStatsAction.Request(config.getId())).actionGet();
            assertEquals(1, statsResponse.getResponse().results().size());
            assertEquals(DatafeedState.STARTED, statsResponse.getResponse().results().get(0).getDatafeedState());
        });
    }

    @TestLogging("org.elasticsearch.xpack.persistent:TRACE,org.elasticsearch.cluster.service:DEBUG")
    public void testDedicatedMlNode() throws Exception {
        internalCluster().ensureAtMostNumDataNodes(0);
        // start 2 non ml node that will never get a job allocated. (but ml apis are accessable from this node)
        internalCluster().startNode(Settings.builder().put(MachineLearning.ML_ENABLED.getKey(), false));
        internalCluster().startNode(Settings.builder().put(MachineLearning.ML_ENABLED.getKey(), false));
        // start ml node
        if (randomBoolean()) {
            internalCluster().startNode(Settings.builder().put(MachineLearning.ML_ENABLED.getKey(), true));
        } else {
            // the default is based on 'xpack.ml.enabled', which is enabled in base test class.
            internalCluster().startNode();
        }
        ensureStableCluster(3);

        Job.Builder job = createJob("job_id");
        PutJobAction.Request putJobRequest = new PutJobAction.Request(job.build());
        PutJobAction.Response putJobResponse = client().execute(PutJobAction.INSTANCE, putJobRequest).get();
        assertTrue(putJobResponse.isAcknowledged());

        OpenJobAction.Request openJobRequest = new OpenJobAction.Request(job.getId());
        client().execute(OpenJobAction.INSTANCE, openJobRequest).get();
        assertBusy(() -> {
            ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
            PersistentTasksInProgress tasks = clusterState.getMetaData().custom(PersistentTasksInProgress.TYPE);
            PersistentTaskInProgress task = tasks.taskMap().values().iterator().next();

            DiscoveryNode node = clusterState.nodes().resolveNode(task.getExecutorNode());
            Map<String, String> expectedNodeAttr = new HashMap<>();
            expectedNodeAttr.put(MAX_RUNNING_JOBS_PER_NODE.getKey(), "10");
            assertEquals(expectedNodeAttr, node.getAttributes());
            assertEquals(JobState.OPENED, task.getStatus());
        });

        logger.info("stop the only running ml node");
        internalCluster().stopRandomNode(settings -> settings.getAsBoolean(MachineLearning.ML_ENABLED.getKey(), true));
        ensureStableCluster(2);
        assertBusy(() -> {
            // job should get and remain in a failed state:
            ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
            PersistentTasksInProgress tasks = clusterState.getMetaData().custom(PersistentTasksInProgress.TYPE);
            PersistentTaskInProgress task = tasks.taskMap().values().iterator().next();

            assertNull(task.getExecutorNode());
            // The status remains to be opened as from ml we didn't had the chance to set the status to failed:
            assertEquals(JobState.OPENED, task.getStatus());
        });

        logger.info("start ml node");
        internalCluster().startNode(Settings.builder().put(MachineLearning.ML_ENABLED.getKey(), true));
        ensureStableCluster(3);
        assertBusy(() -> {
            // job should be re-opened:
            ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
            PersistentTasksInProgress tasks = clusterState.getMetaData().custom(PersistentTasksInProgress.TYPE);
            PersistentTaskInProgress task = tasks.taskMap().values().iterator().next();

            assertNotNull(task.getExecutorNode());
            DiscoveryNode node = clusterState.nodes().resolveNode(task.getExecutorNode());
            Map<String, String> expectedNodeAttr = new HashMap<>();
            expectedNodeAttr.put(MAX_RUNNING_JOBS_PER_NODE.getKey(), "10");
            assertEquals(expectedNodeAttr, node.getAttributes());
            assertEquals(JobState.OPENED, task.getStatus());
        });
    }

    public void testMaxConcurrentJobAllocations() throws Exception {
        int numMlNodes = 2;
        internalCluster().ensureAtMostNumDataNodes(0);
        // start non ml node, but that will hold the indices
        logger.info("Start non ml node:");
        String nonMlNode = internalCluster().startNode(Settings.builder()
                .put(MachineLearning.ML_ENABLED.getKey(), false));
        logger.info("Starting ml nodes");
        internalCluster().startNodes(numMlNodes, Settings.builder()
                .put("node.data", false)
                .put("node.master", false)
                .put(MachineLearning.ML_ENABLED.getKey(), true).build());
        ensureStableCluster(numMlNodes + 1);

        int maxConcurrentJobAllocations = randomIntBetween(1, 4);
        client().admin().cluster().prepareUpdateSettings()
                .setTransientSettings(Settings.builder()
                        .put(MachineLearning.CONCURRENT_JOB_ALLOCATIONS.getKey(), maxConcurrentJobAllocations))
                .get();

        // Sample each cs update and keep track each time a node holds more than `maxConcurrentJobAllocations` opening jobs.
        List<String> violations = new CopyOnWriteArrayList<>();
        internalCluster().clusterService(nonMlNode).addListener(event -> {
            PersistentTasksInProgress tasks = event.state().metaData().custom(PersistentTasksInProgress.TYPE);
            if (tasks == null) {
                return;
            }

            for (DiscoveryNode node : event.state().nodes()) {
                Collection<PersistentTaskInProgress<?>> foundTasks = tasks.findTasks(OpenJobAction.NAME, task -> {
                    return node.getId().equals(task.getExecutorNode()) &&
                            (task.getStatus() == null || task.getStatus() == JobState.OPENING || task.isCurrentStatus() == false);
                });
                int count = foundTasks.size();
                if (count > maxConcurrentJobAllocations) {
                    violations.add("Observed node [" + node.getName() + "] with [" + count + "] opening jobs on cluster state version [" +
                            event.state().version() + "]");
                }
            }
        });

        int numJobs = numMlNodes * 10;
        for (int i = 0; i < numJobs; i++) {
            Job.Builder job = createJob(Integer.toString(i));
            PutJobAction.Request putJobRequest = new PutJobAction.Request(job.build());
            PutJobAction.Response putJobResponse = client().execute(PutJobAction.INSTANCE, putJobRequest).get();
            assertTrue(putJobResponse.isAcknowledged());

            OpenJobAction.Request openJobRequest = new OpenJobAction.Request(job.getId());
            client().execute(OpenJobAction.INSTANCE, openJobRequest).get();
        }

        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().get().getState();
            PersistentTasksInProgress tasks = state.metaData().custom(PersistentTasksInProgress.TYPE);
            assertEquals(numJobs, tasks.taskMap().size());
            for (PersistentTaskInProgress<?> task : tasks.taskMap().values()) {
                assertNotNull(task.getExecutorNode());
                assertEquals(JobState.OPENED, task.getStatus());
            }
        });

        logger.info("stopping ml nodes");
        for (int i = 0; i < numMlNodes; i++) {
            // fork so stopping all ml nodes proceeds quicker:
            Runnable r = () -> {
                try {
                    internalCluster()
                            .stopRandomNode(settings -> settings.getAsBoolean(MachineLearning.ML_ENABLED.getKey(), false));
                } catch (IOException e) {
                    logger.error("error stopping node", e);
                }
            };
            new Thread(r).start();
        }
        ensureStableCluster(1, nonMlNode);
        assertBusy(() -> {
            ClusterState state = client(nonMlNode).admin().cluster().prepareState().get().getState();
            PersistentTasksInProgress tasks = state.metaData().custom(PersistentTasksInProgress.TYPE);
            assertEquals(numJobs, tasks.taskMap().size());
            for (PersistentTaskInProgress<?> task : tasks.taskMap().values()) {
                assertNull(task.getExecutorNode());
            }
        });

        logger.info("re-starting ml nodes");
        internalCluster().startNodes(numMlNodes, Settings.builder()
                .put("node.data", false)
                .put("node.master", false)
                .put(MachineLearning.ML_ENABLED.getKey(), true).build());

        ensureStableCluster(1 + numMlNodes);
        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().get().getState();
            PersistentTasksInProgress tasks = state.metaData().custom(PersistentTasksInProgress.TYPE);
            assertEquals(numJobs, tasks.taskMap().size());
            for (PersistentTaskInProgress<?> task : tasks.taskMap().values()) {
                assertNotNull(task.getExecutorNode());
                assertEquals(JobState.OPENED, task.getStatus());
            }
        }, 30, TimeUnit.SECONDS);

        assertEquals("Expected no violations, but got [" + violations + "]", 0, violations.size());
    }


}
