/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.PersistentTask;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.CloseJobAction;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.PostDataAction;
import org.elasticsearch.xpack.core.ml.action.PutDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.StopDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.config.JobTaskState;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.persistent.PersistentTasksClusterService.needsReassignment;
import static org.elasticsearch.test.NodeRoles.addRoles;
import static org.elasticsearch.test.NodeRoles.onlyRole;
import static org.elasticsearch.test.NodeRoles.removeRoles;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.notNullValue;

public class BasicDistributedJobsIT extends BaseMlIntegTestCase {

    public void testFailOverBasics() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(4);
        ensureStableCluster(4);

        Job.Builder job = createJob("fail-over-basics-job", ByteSizeValue.ofMb(2));
        PutJobAction.Request putJobRequest = new PutJobAction.Request(job);
        client().execute(PutJobAction.INSTANCE, putJobRequest).actionGet();
        ensureYellow(); // at least the primary shards of the indices a job uses should be started
        OpenJobAction.Request openJobRequest = new OpenJobAction.Request(job.getId());
        client().execute(OpenJobAction.INSTANCE, openJobRequest).actionGet();
        awaitJobOpenedAndAssigned(job.getId(), null);

        setMlIndicesDelayedNodeLeftTimeoutToZero();

        ensureGreen(); // replicas must be assigned, otherwise we could lose a whole index
        internalCluster().stopRandomDataNode();
        ensureStableCluster(3);
        awaitJobOpenedAndAssigned(job.getId(), null);

        ensureGreen(); // replicas must be assigned, otherwise we could lose a whole index
        internalCluster().stopRandomDataNode();
        ensureStableCluster(2);
        awaitJobOpenedAndAssigned(job.getId(), null);
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/82591")
    public void testFailOverBasics_withDataFeeder() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(4);
        ensureStableCluster(4);

        Detector.Builder d = new Detector.Builder("count", null);
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(d.build()));
        analysisConfig.setSummaryCountFieldName("doc_count");
        analysisConfig.setBucketSpan(TimeValue.timeValueHours(1));
        Job.Builder job = new Job.Builder("fail-over-basics_with-data-feeder-job");
        job.setAnalysisConfig(analysisConfig);
        job.setDataDescription(new DataDescription.Builder());

        PutJobAction.Request putJobRequest = new PutJobAction.Request(job);
        client().execute(PutJobAction.INSTANCE, putJobRequest).actionGet();
        DatafeedConfig.Builder configBuilder = createDatafeedBuilder("data_feed_id", job.getId(), Collections.singletonList("*"));

        MaxAggregationBuilder maxAggregation = AggregationBuilders.max("time").field("time");
        HistogramAggregationBuilder histogramAggregation = AggregationBuilders.histogram("time")
            .interval(60000)
            .subAggregation(maxAggregation)
            .field("time");

        configBuilder.setParsedAggregations(AggregatorFactories.builder().addAggregator(histogramAggregation));
        configBuilder.setFrequency(TimeValue.timeValueMinutes(2));
        DatafeedConfig config = configBuilder.build();
        PutDatafeedAction.Request putDatafeedRequest = new PutDatafeedAction.Request(config);
        client().execute(PutDatafeedAction.INSTANCE, putDatafeedRequest).actionGet();

        ensureYellow(); // at least the primary shards of the indices a job uses should be started
        OpenJobAction.Request openJobRequest = new OpenJobAction.Request(job.getId());
        client().execute(OpenJobAction.INSTANCE, openJobRequest).actionGet();
        awaitJobOpenedAndAssigned(job.getId(), null);

        setMlIndicesDelayedNodeLeftTimeoutToZero();

        StartDatafeedAction.Request startDataFeedRequest = new StartDatafeedAction.Request(config.getId(), 0L);
        client().execute(StartDatafeedAction.INSTANCE, startDataFeedRequest);

        assertBusy(() -> {
            GetDatafeedsStatsAction.Response statsResponse = client().execute(
                GetDatafeedsStatsAction.INSTANCE,
                new GetDatafeedsStatsAction.Request(config.getId())
            ).actionGet();
            assertEquals(1, statsResponse.getResponse().results().size());
            assertEquals(DatafeedState.STARTED, statsResponse.getResponse().results().get(0).getDatafeedState());
        });

        ensureGreen(); // replicas must be assigned, otherwise we could lose a whole index
        internalCluster().stopRandomDataNode();
        ensureStableCluster(3);
        awaitJobOpenedAndAssigned(job.getId(), null);
        assertBusy(() -> {
            GetDatafeedsStatsAction.Response statsResponse = client().execute(
                GetDatafeedsStatsAction.INSTANCE,
                new GetDatafeedsStatsAction.Request(config.getId())
            ).actionGet();
            assertEquals(1, statsResponse.getResponse().results().size());
            assertEquals(DatafeedState.STARTED, statsResponse.getResponse().results().get(0).getDatafeedState());
        });

        ensureGreen(); // replicas must be assigned, otherwise we could lose a whole index
        internalCluster().stopRandomDataNode();
        ensureStableCluster(2);
        awaitJobOpenedAndAssigned(job.getId(), null);
        assertBusy(() -> {
            GetDatafeedsStatsAction.Response statsResponse = client().execute(
                GetDatafeedsStatsAction.INSTANCE,
                new GetDatafeedsStatsAction.Request(config.getId())
            ).actionGet();
            assertEquals(1, statsResponse.getResponse().results().size());
            assertEquals(DatafeedState.STARTED, statsResponse.getResponse().results().get(0).getDatafeedState());
        });
    }

    public void testJobAutoClose() throws Exception {
        internalCluster().ensureAtMostNumDataNodes(0);
        internalCluster().startNode(removeRoles(Set.of(DiscoveryNodeRole.ML_ROLE)));
        internalCluster().startNode(addRoles(Set.of(DiscoveryNodeRole.ML_ROLE)));

        client().admin().indices().prepareCreate("data").setMapping("time", "type=date").get();

        IndexRequest indexRequest = new IndexRequest("data");
        indexRequest.source("time", 1407081600L);
        client().index(indexRequest).get();
        indexRequest = new IndexRequest("data");
        indexRequest.source("time", 1407082600L);
        client().index(indexRequest).get();
        indexRequest = new IndexRequest("data");
        indexRequest.source("time", 1407083600L);
        client().index(indexRequest).get();
        refresh("*");

        Job.Builder job = createScheduledJob("job_id");
        PutJobAction.Request putJobRequest = new PutJobAction.Request(job);
        client().execute(PutJobAction.INSTANCE, putJobRequest).actionGet();

        DatafeedConfig config = createDatafeed("data_feed_id", job.getId(), Collections.singletonList("data"));
        PutDatafeedAction.Request putDatafeedRequest = new PutDatafeedAction.Request(config);
        client().execute(PutDatafeedAction.INSTANCE, putDatafeedRequest).actionGet();

        ensureYellow(); // at least the primary shards of the indices a job uses should be started
        client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(job.getId())).get();

        StartDatafeedAction.Request startDatafeedRequest = new StartDatafeedAction.Request(config.getId(), 0L);
        startDatafeedRequest.getParams().setEndTime(1492616844L);
        client().execute(StartDatafeedAction.INSTANCE, startDatafeedRequest).get();
        assertBusy(() -> {
            GetJobsStatsAction.Response.JobStats jobStats = getJobStats(job.getId());
            assertEquals(3L, jobStats.getDataCounts().getProcessedRecordCount());
            assertEquals(JobState.CLOSED, jobStats.getState());
        });
    }

    public void testDedicatedMlNode() throws Exception {
        internalCluster().ensureAtMostNumDataNodes(0);
        // start 2 non ml node that will never get a job allocated. (but ml apis are accessible from this node)
        internalCluster().startNode(removeRoles(Set.of(DiscoveryNodeRole.ML_ROLE)));
        internalCluster().startNode(removeRoles(Set.of(DiscoveryNodeRole.ML_ROLE)));
        // start ml node
        if (randomBoolean()) {
            internalCluster().startNode(addRoles(Set.of(DiscoveryNodeRole.ML_ROLE)));
        } else {
            // the default is based on 'xpack.ml.enabled', which is enabled in base test class.
            internalCluster().startNode();
        }
        ensureStableCluster(3);

        String jobId = "dedicated-ml-node-job";
        Job.Builder job = createJob(jobId, ByteSizeValue.ofMb(2));
        PutJobAction.Request putJobRequest = new PutJobAction.Request(job);
        client().execute(PutJobAction.INSTANCE, putJobRequest).actionGet();

        ensureYellow(); // at least the primary shards of the indices a job uses should be started
        OpenJobAction.Request openJobRequest = new OpenJobAction.Request(job.getId());
        client().execute(OpenJobAction.INSTANCE, openJobRequest).actionGet();
        assertBusy(() -> {
            ClusterState clusterState = clusterAdmin().prepareState().get().getState();
            PersistentTasksCustomMetadata tasks = clusterState.getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
            PersistentTask<?> task = tasks.getTask(MlTasks.jobTaskId(jobId));

            DiscoveryNode node = clusterState.nodes().resolveNode(task.getExecutorNode());
            assertThat(node.getAttributes(), hasEntry(equalTo(MachineLearning.MACHINE_MEMORY_NODE_ATTR), notNullValue()));
            JobTaskState jobTaskState = (JobTaskState) task.getState();
            assertNotNull(jobTaskState);
            assertEquals(JobState.OPENED, jobTaskState.getState());
        });

        logger.info("stop the only running ml node");
        internalCluster().stopNode(
            internalCluster().getNodeNameThat(settings -> DiscoveryNode.hasRole(settings, DiscoveryNodeRole.ML_ROLE))
        );
        ensureStableCluster(2);
        assertBusy(() -> {
            // job should get and remain in a failed state and
            // the status remains to be opened as from ml we didn't had the chance to set the status to failed:
            assertJobTask(jobId, JobState.OPENED, false);
        });

        logger.info("start ml node");
        internalCluster().startNode(addRoles(Set.of(DiscoveryNodeRole.ML_ROLE)));
        ensureStableCluster(3);
        assertBusy(() -> {
            // job should be re-opened:
            assertJobTask(jobId, JobState.OPENED, true);
        });
    }

    public void testMaxConcurrentJobAllocations() throws Exception {
        int numMlNodes = 2;
        internalCluster().ensureAtMostNumDataNodes(0);
        // start non ml node, but that will hold the indices
        logger.info("Start non ml node:");
        String nonMlNode = internalCluster().startNode(removeRoles(Set.of(DiscoveryNodeRole.ML_ROLE)));
        logger.info("Starting ml nodes");
        internalCluster().startNodes(numMlNodes, onlyRole(DiscoveryNodeRole.ML_ROLE));
        ensureStableCluster(numMlNodes + 1);

        int maxConcurrentJobAllocations = randomIntBetween(1, 4);
        updateClusterSettings(Settings.builder().put(MachineLearning.CONCURRENT_JOB_ALLOCATIONS.getKey(), maxConcurrentJobAllocations));

        // Sample each cs update and keep track each time a node holds more than `maxConcurrentJobAllocations` opening jobs.
        List<String> violations = new CopyOnWriteArrayList<>();
        internalCluster().clusterService(nonMlNode).addListener(event -> {
            PersistentTasksCustomMetadata tasks = event.state().metadata().custom(PersistentTasksCustomMetadata.TYPE);
            if (tasks == null) {
                return;
            }

            for (DiscoveryNode node : event.state().nodes()) {
                Collection<PersistentTask<?>> foundTasks = tasks.findTasks(MlTasks.JOB_TASK_NAME, task -> {
                    JobTaskState jobTaskState = (JobTaskState) task.getState();
                    return node.getId().equals(task.getExecutorNode()) && (jobTaskState == null || jobTaskState.isStatusStale(task));
                });
                int count = foundTasks.size();
                if (count > maxConcurrentJobAllocations) {
                    violations.add(
                        "Observed node ["
                            + node.getName()
                            + "] with ["
                            + count
                            + "] opening jobs on cluster state version ["
                            + event.state().version()
                            + "]"
                    );
                }
            }
        });

        ensureYellow(); // at least the primary shards of the indices a job uses should be started
        int numJobs = numMlNodes * 10;
        for (int i = 0; i < numJobs; i++) {
            Job.Builder job = createJob(Integer.toString(i), ByteSizeValue.ofMb(2));
            PutJobAction.Request putJobRequest = new PutJobAction.Request(job);
            client().execute(PutJobAction.INSTANCE, putJobRequest).actionGet();

            OpenJobAction.Request openJobRequest = new OpenJobAction.Request(job.getId());
            client().execute(OpenJobAction.INSTANCE, openJobRequest).actionGet();
        }

        assertBusy(checkAllJobsAreAssignedAndOpened(numJobs));

        logger.info("stopping ml nodes");
        for (int i = 0; i < numMlNodes; i++) {
            try {
                internalCluster().stopNode(
                    internalCluster().getNodeNameThat(settings -> DiscoveryNode.hasRole(settings, DiscoveryNodeRole.ML_ROLE))
                );
            } catch (IOException e) {
                logger.error("error stopping node", e);
            }
        }
        ensureStableCluster(1, nonMlNode);
        assertBusy(() -> {
            ClusterState state = client(nonMlNode).admin().cluster().prepareState().get().getState();
            List<PersistentTask<?>> tasks = findTasks(state, MlTasks.JOB_TASK_NAME);
            assertEquals(numJobs, tasks.size());
            for (PersistentTask<?> task : tasks) {
                assertNull(task.getExecutorNode());
            }
        });

        logger.info("re-starting ml nodes");
        internalCluster().startNodes(numMlNodes, onlyRole(DiscoveryNodeRole.ML_ROLE));

        ensureStableCluster(1 + numMlNodes);
        assertBusy(checkAllJobsAreAssignedAndOpened(numJobs), 30, TimeUnit.SECONDS);

        assertEquals("Expected no violations, but got [" + violations + "]", 0, violations.size());
    }

    // This test is designed to check that a job will not open when the .ml-state
    // or .ml-anomalies-shared indices are not available. To do this those indices
    // must be allocated on a node which is later stopped while .ml-config is
    // allocated on a second node which remains active.
    public void testMlStateAndResultsIndicesNotAvailable() throws Exception {
        internalCluster().ensureAtMostNumDataNodes(0);
        // start non ml node that will hold the state and results indices
        logger.info("Start non ml node:");
        String nonMLNode = internalCluster().startNode(
            Settings.builder().put("node.attr.ml-indices", "state-and-results").put(removeRoles(Set.of(DiscoveryNodeRole.ML_ROLE)))
        );
        ensureStableCluster(1);
        // start an ml node for the config index
        logger.info("Starting ml node");
        String mlNode = internalCluster().startNode(
            Settings.builder()
                .put("node.attr.ml-indices", "config")
                .put(addRoles(Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.ML_ROLE)))
        );
        ensureStableCluster(2);

        // Create the indices (using installed templates) and set the routing to specific nodes
        // State and results go on the state-and-results node, config goes on the config node
        indicesAdmin().prepareCreate(".ml-anomalies-shared")
            .setSettings(
                Settings.builder()
                    .put("index.routing.allocation.include.ml-indices", "state-and-results")
                    .put("index.routing.allocation.exclude.ml-indices", "config")
                    .build()
            )
            .get();
        indicesAdmin().prepareCreate(".ml-state")
            .setSettings(
                Settings.builder()
                    .put("index.routing.allocation.include.ml-indices", "state-and-results")
                    .put("index.routing.allocation.exclude.ml-indices", "config")
                    .build()
            )
            .get();
        indicesAdmin().prepareCreate(".ml-config")
            .setSettings(
                Settings.builder()
                    .put("index.routing.allocation.exclude.ml-indices", "state-and-results")
                    .put("index.routing.allocation.include.ml-indices", "config")
                    .build()
            )
            .get();

        String jobId = "ml-indices-not-available-job";
        Job.Builder job = createFareQuoteJob(jobId);
        PutJobAction.Request putJobRequest = new PutJobAction.Request(job);
        client().execute(PutJobAction.INSTANCE, putJobRequest).actionGet();

        OpenJobAction.Request openJobRequest = new OpenJobAction.Request(job.getId());
        client().execute(OpenJobAction.INSTANCE, openJobRequest).actionGet();

        PostDataAction.Request postDataRequest = new PostDataAction.Request(jobId);
        postDataRequest.setContent(new BytesArray("""
            {"airline":"AAL","responsetime":"132.2046","sourcetype":"farequote","time":"1403481600"}
            {"airline":"JZA","responsetime":"990.4628","sourcetype":"farequote","time":"1403481700"}"""), XContentType.JSON);
        PostDataAction.Response response = client().execute(PostDataAction.INSTANCE, postDataRequest).actionGet();
        assertEquals(2, response.getDataCounts().getProcessedRecordCount());

        CloseJobAction.Request closeJobRequest = new CloseJobAction.Request(jobId);
        client().execute(CloseJobAction.INSTANCE, closeJobRequest).actionGet();
        assertBusy(() -> {
            ClusterState clusterState = clusterAdmin().prepareState().get().getState();
            List<PersistentTask<?>> tasks = findTasks(clusterState, MlTasks.JOB_TASK_NAME);
            assertEquals(0, tasks.size());
        });
        logger.info("Stop non ml node");
        Settings nonMLNodeDataPathSettings = internalCluster().dataPathSettings(nonMLNode);
        internalCluster().stopNode(nonMLNode);
        ensureStableCluster(1);

        Exception e = expectThrows(
            ElasticsearchStatusException.class,
            () -> client().execute(OpenJobAction.INSTANCE, openJobRequest).actionGet()
        );
        assertEquals("Could not open job because no ML nodes with sufficient capacity were found", e.getMessage());
        IllegalStateException detail = (IllegalStateException) e.getCause();
        assertNotNull(detail);
        String detailedMessage = detail.getMessage();
        assertTrue(
            detailedMessage,
            detailedMessage.startsWith("Could not open job because no suitable nodes were found, allocation explanation")
        );
        assertThat(detailedMessage, containsString("because not all primary shards are active for the following indices"));
        assertThat(detailedMessage, containsString(".ml-state"));
        assertThat(detailedMessage, containsString(".ml-anomalies-shared"));

        logger.info("Start data node");
        String nonMlNode = internalCluster().startNode(
            Settings.builder().put(nonMLNodeDataPathSettings).put(removeRoles(Set.of(DiscoveryNodeRole.ML_ROLE)))
        );
        ensureStableCluster(2, mlNode);
        ensureStableCluster(2, nonMlNode);
        ensureYellow(); // at least the primary shards of the indices a job uses should be started
        client().execute(OpenJobAction.INSTANCE, openJobRequest).actionGet();
        assertBusy(() -> assertJobTask(jobId, JobState.OPENED, true));
    }

    public void testCloseUnassignedLazyJobAndDatafeed() {
        internalCluster().ensureAtLeastNumDataNodes(3);
        ensureStableCluster(3);

        String jobId = "test-lazy-stop";
        String datafeedId = jobId + "-datafeed";
        // Assume the test machine won't have space to assign a 2TB job
        Job.Builder job = createJob(jobId, ByteSizeValue.ofTb(2), true);
        PutJobAction.Request putJobRequest = new PutJobAction.Request(job);
        client().execute(PutJobAction.INSTANCE, putJobRequest).actionGet();

        client().admin().indices().prepareCreate("data").setMapping("time", "type=date").get();

        DatafeedConfig config = createDatafeed(datafeedId, jobId, Collections.singletonList("data"));
        PutDatafeedAction.Request putDatafeedRequest = new PutDatafeedAction.Request(config);
        client().execute(PutDatafeedAction.INSTANCE, putDatafeedRequest).actionGet();

        ensureYellow(); // at least the primary shards of the indices a job uses should be started
        OpenJobAction.Request openJobRequest = new OpenJobAction.Request(jobId);
        client().execute(OpenJobAction.INSTANCE, openJobRequest).actionGet();

        // Job state should be opening because it won't fit anyway, but is allowed to open lazily
        GetJobsStatsAction.Request jobStatsRequest = new GetJobsStatsAction.Request(jobId);
        GetJobsStatsAction.Response jobStatsResponse = client().execute(GetJobsStatsAction.INSTANCE, jobStatsRequest).actionGet();
        assertEquals(JobState.OPENING, jobStatsResponse.getResponse().results().get(0).getState());

        StartDatafeedAction.Request startDataFeedRequest = new StartDatafeedAction.Request(config.getId(), 0L);
        client().execute(StartDatafeedAction.INSTANCE, startDataFeedRequest).actionGet();

        // Datafeed state should be starting while it waits for job assignment
        GetDatafeedsStatsAction.Request datafeedStatsRequest = new GetDatafeedsStatsAction.Request(datafeedId);
        GetDatafeedsStatsAction.Response datafeedStatsResponse = client().execute(GetDatafeedsStatsAction.INSTANCE, datafeedStatsRequest)
            .actionGet();
        assertEquals(DatafeedState.STARTING, datafeedStatsResponse.getResponse().results().get(0).getDatafeedState());

        // A starting datafeed can be stopped normally or by force
        StopDatafeedAction.Request stopDatafeedRequest = new StopDatafeedAction.Request(datafeedId);
        stopDatafeedRequest.setForce(randomBoolean());
        StopDatafeedAction.Response stopDatafeedResponse = client().execute(StopDatafeedAction.INSTANCE, stopDatafeedRequest).actionGet();
        assertTrue(stopDatafeedResponse.isStopped());

        datafeedStatsResponse = client().execute(GetDatafeedsStatsAction.INSTANCE, datafeedStatsRequest).actionGet();
        assertEquals(DatafeedState.STOPPED, datafeedStatsResponse.getResponse().results().get(0).getDatafeedState());

        // An opening job can also be stopped normally or by force
        CloseJobAction.Request closeJobRequest = new CloseJobAction.Request(jobId);
        closeJobRequest.setForce(randomBoolean());
        CloseJobAction.Response closeJobResponse = client().execute(CloseJobAction.INSTANCE, closeJobRequest).actionGet();
        assertTrue(closeJobResponse.isClosed());

        jobStatsResponse = client().execute(GetJobsStatsAction.INSTANCE, jobStatsRequest).actionGet();
        assertEquals(JobState.CLOSED, jobStatsResponse.getResponse().results().get(0).getState());
    }

    private void assertJobTask(String jobId, JobState expectedState, boolean hasExecutorNode) {
        ClusterState clusterState = clusterAdmin().prepareState().get().getState();
        List<PersistentTask<?>> tasks = findTasks(clusterState, MlTasks.JOB_TASK_NAME);
        assertEquals(1, tasks.size());
        PersistentTask<?> task = tasks.get(0);
        assertEquals(task.getId(), MlTasks.jobTaskId(jobId));

        if (hasExecutorNode) {
            assertNotNull(task.getExecutorNode());
            assertFalse(needsReassignment(task.getAssignment(), clusterState.nodes()));
            DiscoveryNode node = clusterState.nodes().resolveNode(task.getExecutorNode());
            assertThat(node.getAttributes(), hasEntry(equalTo(MachineLearning.MACHINE_MEMORY_NODE_ATTR), notNullValue()));

            JobTaskState jobTaskState = (JobTaskState) task.getState();
            assertNotNull(jobTaskState);
            assertEquals(expectedState, jobTaskState.getState());
        } else {
            assertNull(task.getExecutorNode());
        }
    }

    private CheckedRunnable<Exception> checkAllJobsAreAssignedAndOpened(int numJobs) {
        return () -> {
            ClusterState state = clusterAdmin().prepareState().get().getState();
            List<PersistentTask<?>> tasks = findTasks(state, MlTasks.JOB_TASK_NAME);
            assertEquals(numJobs, tasks.size());
            for (PersistentTask<?> task : tasks) {
                assertNotNull(task.getExecutorNode());
                JobTaskState jobTaskState = (JobTaskState) task.getState();
                assertNotNull(jobTaskState);
                assertEquals(JobState.OPENED, jobTaskState.getState());
            }
        };
    }
}
