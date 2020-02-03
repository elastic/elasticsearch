/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.CheckedRunnable;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.persistent.PersistentTaskResponse;
import org.elasticsearch.persistent.PersistentTasksClusterService;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData.PersistentTask;
import org.elasticsearch.persistent.UpdatePersistentTaskStatusAction;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.CloseJobAction;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsStatsAction.Response.DatafeedStats;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction.Response.JobStats;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.PostDataAction;
import org.elasticsearch.xpack.core.ml.action.PutDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.StopDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.job.process.autodetect.BlackHoleAutodetectProcess;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.persistent.PersistentTasksClusterService.needsReassignment;

public class MlDistributedFailureIT extends BaseMlIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal))
            .put(MachineLearning.CONCURRENT_JOB_ALLOCATIONS.getKey(), 4)
            .build();
    }

    public void testFailOver() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(3);
        ensureStableClusterOnAllNodes(3);
        run("fail-over-job", () -> {
            GetJobsStatsAction.Request  request = new GetJobsStatsAction.Request("fail-over-job");
            GetJobsStatsAction.Response response = client().execute(GetJobsStatsAction.INSTANCE, request).actionGet();
            DiscoveryNode discoveryNode = response.getResponse().results().get(0).getNode();
            internalCluster().stopRandomNode(settings -> discoveryNode.getName().equals(settings.get("node.name")));
            ensureStableClusterOnAllNodes(2);
        });
    }

    public void testLoseDedicatedMasterNode() throws Exception {
        internalCluster().ensureAtMostNumDataNodes(0);
        logger.info("Starting dedicated master node...");
        internalCluster().startNode(Settings.builder()
                .put("node.master", true)
                .put("node.data", false)
                .put("node.ml", false)
                .build());
        logger.info("Starting ml and data node...");
        String mlAndDataNode = internalCluster().startNode(Settings.builder()
                .put("node.master", false)
                .build());
        ensureStableClusterOnAllNodes(2);
        run("lose-dedicated-master-node-job", () -> {
            logger.info("Stopping dedicated master node");
            Settings masterDataPathSettings = internalCluster().dataPathSettings(internalCluster().getMasterName());
            internalCluster().stopCurrentMasterNode();
            assertBusy(() -> {
                ClusterState state = client(mlAndDataNode).admin().cluster().prepareState()
                        .setLocal(true).get().getState();
                assertNull(state.nodes().getMasterNodeId());
            });
            logger.info("Restarting dedicated master node");
            internalCluster().startNode(Settings.builder()
                    .put(masterDataPathSettings)
                    .put("node.master", true)
                    .put("node.data", false)
                    .put("node.ml", false)
                    .build());
            ensureStableClusterOnAllNodes(2);
        });
    }

    public void testFullClusterRestart() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(3);
        ensureStableClusterOnAllNodes(3);
        run("full-cluster-restart-job", () -> {
            logger.info("Restarting all nodes");
            internalCluster().fullRestart();
            logger.info("Restarted all nodes");
            ensureStableClusterOnAllNodes(3);
        });
    }

    public void testCloseUnassignedJobAndDatafeed() throws Exception {
        internalCluster().ensureAtMostNumDataNodes(0);
        logger.info("Starting dedicated master node...");
        internalCluster().startNode(Settings.builder()
                .put("node.master", true)
                .put("node.data", true)
                .put("node.ml", false)
                .build());
        logger.info("Starting ml and data node...");
        internalCluster().startNode(Settings.builder()
                .put("node.master", false)
                .build());
        ensureStableClusterOnAllNodes(2);

        // index some datafeed data
        client().admin().indices().prepareCreate("data")
                .setMapping("time", "type=date")
                .get();
        long numDocs1 = randomIntBetween(32, 2048);
        long now = System.currentTimeMillis();
        long weekAgo = now - 604800000;
        long twoWeeksAgo = weekAgo - 604800000;
        indexDocs(logger, "data", numDocs1, twoWeeksAgo, weekAgo);

        String jobId = "test-lose-ml-node";
        String datafeedId = jobId + "-datafeed";
        setupJobAndDatafeed(jobId, datafeedId, TimeValue.timeValueHours(1));
        waitForDatafeed(jobId, numDocs1);

        // stop the only ML node
        ensureGreen(); // replicas must be assigned, otherwise we could lose a whole index
        internalCluster().stopRandomNonMasterNode();
        ensureStableCluster(1);

        // Job state is opened but the job is not assigned to a node (because we just killed the only ML node)
        GetJobsStatsAction.Request jobStatsRequest = new GetJobsStatsAction.Request(jobId);
        GetJobsStatsAction.Response jobStatsResponse = client().execute(GetJobsStatsAction.INSTANCE, jobStatsRequest).actionGet();
        assertEquals(JobState.OPENED, jobStatsResponse.getResponse().results().get(0).getState());

        GetDatafeedsStatsAction.Request datafeedStatsRequest = new GetDatafeedsStatsAction.Request(datafeedId);
        GetDatafeedsStatsAction.Response datafeedStatsResponse =
                client().execute(GetDatafeedsStatsAction.INSTANCE, datafeedStatsRequest).actionGet();
        assertEquals(DatafeedState.STARTED, datafeedStatsResponse.getResponse().results().get(0).getDatafeedState());

        // An unassigned datafeed can be stopped either normally or by force
        StopDatafeedAction.Request stopDatafeedRequest = new StopDatafeedAction.Request(datafeedId);
        stopDatafeedRequest.setForce(randomBoolean());
        StopDatafeedAction.Response stopDatafeedResponse = client().execute(StopDatafeedAction.INSTANCE, stopDatafeedRequest).actionGet();
        assertTrue(stopDatafeedResponse.isStopped());

        // Since 7.5 we can also stop an unassigned job either normally or by force
        CloseJobAction.Request closeJobRequest = new CloseJobAction.Request(jobId);
        closeJobRequest.setForce(randomBoolean());
        CloseJobAction.Response closeJobResponse = client().execute(CloseJobAction.INSTANCE, closeJobRequest).actionGet();
        assertTrue(closeJobResponse.isClosed());
    }

    public void testCloseUnassignedFailedJobAndStopUnassignedStoppingDatafeed() throws Exception {
        internalCluster().ensureAtMostNumDataNodes(0);
        logger.info("Starting master/data nodes...");
        for (int count = 0; count < 3; ++count) {
            internalCluster().startNode(Settings.builder()
                .put("node.master", true)
                .put("node.data", true)
                .put("node.ml", false)
                .build());
        }
        logger.info("Starting dedicated ml node...");
        internalCluster().startNode(Settings.builder()
            .put("node.master", false)
            .put("node.data", false)
            .put("node.ml", true)
            .build());
        ensureStableClusterOnAllNodes(4);

        // index some datafeed data
        client().admin().indices().prepareCreate("data")
            .setMapping("time", "type=date")
            .get();
        long numDocs1 = randomIntBetween(32, 2048);
        long now = System.currentTimeMillis();
        long weekAgo = now - 604800000;
        long twoWeeksAgo = weekAgo - 604800000;
        indexDocs(logger, "data", numDocs1, twoWeeksAgo, weekAgo);

        String jobId = "test-stop-unassigned-datafeed-for-failed-job";
        String datafeedId = jobId + "-datafeed";
        setupJobAndDatafeed(jobId, datafeedId, TimeValue.timeValueHours(1));
        waitForDatafeed(jobId, numDocs1);

        // Job state should be opened here
        GetJobsStatsAction.Request jobStatsRequest = new GetJobsStatsAction.Request(jobId);
        GetJobsStatsAction.Response jobStatsResponse = client().execute(GetJobsStatsAction.INSTANCE, jobStatsRequest).actionGet();
        assertEquals(JobState.OPENED, jobStatsResponse.getResponse().results().get(0).getState());
        DiscoveryNode jobNode = jobStatsResponse.getResponse().results().get(0).getNode();

        // Post the job a record that will result in the job receiving a timestamp in epoch
        // seconds equal to the maximum integer - this makes the blackhole autodetect fail.
        // It's better to do this than the approach of directly updating the job state using
        // the approach used below for datafeeds, because when the job fails at the "process"
        // level it sets off a more realistic chain reaction in the layers that wrap the "process"
        // (remember it's not a real native process in these internal cluster tests).
        PostDataAction.Request postDataRequest = new PostDataAction.Request(jobId);
        postDataRequest.setContent(
            new BytesArray("{ \"time\" : \"" + BlackHoleAutodetectProcess.MAGIC_FAILURE_VALUE_AS_DATE + "\" }"), XContentType.JSON);
        PostDataAction.Response postDataResponse = client().execute(PostDataAction.INSTANCE, postDataRequest).actionGet();
        assertEquals(1L, postDataResponse.getDataCounts().getInputRecordCount());

        // Confirm the job state is now failed
        jobStatsRequest = new GetJobsStatsAction.Request(jobId);
        jobStatsResponse = client().execute(GetJobsStatsAction.INSTANCE, jobStatsRequest).actionGet();
        assertEquals(JobState.FAILED, jobStatsResponse.getResponse().results().get(0).getState());

        // It's impossible to reliably get the datafeed into a stopping state at the point when the ML node is removed from the cluster
        // using externally accessible actions.  The only way this situation could occur in reality is through extremely unfortunate
        // timing.  Therefore, to simulate this unfortunate timing we cheat and access internal classes to set the datafeed state to
        // stopping.
        PersistentTasksCustomMetaData tasks = clusterService().state().getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
        PersistentTasksCustomMetaData.PersistentTask<?> task = MlTasks.getDatafeedTask(datafeedId, tasks);
        UpdatePersistentTaskStatusAction.Request updatePersistentTaskStatusRequest =
            new UpdatePersistentTaskStatusAction.Request(task.getId(), task.getAllocationId(), DatafeedState.STOPPING);
        PersistentTaskResponse updatePersistentTaskStatusResponse =
            client().execute(UpdatePersistentTaskStatusAction.INSTANCE, updatePersistentTaskStatusRequest).actionGet();
        assertNotNull(updatePersistentTaskStatusResponse.getTask());

        // Confirm the datafeed state is now stopping
        GetDatafeedsStatsAction.Request datafeedStatsRequest = new GetDatafeedsStatsAction.Request(datafeedId);
        GetDatafeedsStatsAction.Response datafeedStatsResponse =
            client().execute(GetDatafeedsStatsAction.INSTANCE, datafeedStatsRequest).actionGet();
        assertEquals(DatafeedState.STOPPING, datafeedStatsResponse.getResponse().results().get(0).getDatafeedState());

        // Stop the node running the failed job/stopping datafeed
        ensureGreen(); // replicas must be assigned, otherwise we could lose a whole index
        internalCluster().stopRandomNode(settings -> jobNode.getName().equals(settings.get("node.name")));
        ensureStableCluster(3);

        // We should be allowed to force stop the unassigned datafeed even though it is stopping and its job has failed
        StopDatafeedAction.Request stopDatafeedRequest = new StopDatafeedAction.Request(datafeedId);
        stopDatafeedRequest.setForce(true);
        StopDatafeedAction.Response stopDatafeedResponse = client().execute(StopDatafeedAction.INSTANCE, stopDatafeedRequest).actionGet();
        assertTrue(stopDatafeedResponse.isStopped());

        // Confirm the datafeed state is now stopped
        datafeedStatsRequest = new GetDatafeedsStatsAction.Request(datafeedId);
        datafeedStatsResponse = client().execute(GetDatafeedsStatsAction.INSTANCE, datafeedStatsRequest).actionGet();
        assertEquals(DatafeedState.STOPPED, datafeedStatsResponse.getResponse().results().get(0).getDatafeedState());

        // We should be allowed to force stop the unassigned failed job
        CloseJobAction.Request closeJobRequest = new CloseJobAction.Request(jobId);
        closeJobRequest.setForce(true);
        CloseJobAction.Response closeJobResponse = client().execute(CloseJobAction.INSTANCE, closeJobRequest).actionGet();
        assertTrue(closeJobResponse.isClosed());
    }

    public void testStopAndForceStopDatafeed() throws Exception {
        internalCluster().ensureAtMostNumDataNodes(0);
        logger.info("Starting dedicated master node...");
        internalCluster().startNode(Settings.builder()
            .put("node.master", true)
            .put("node.data", true)
            .put("node.ml", false)
            .build());
        logger.info("Starting ml and data node...");
        internalCluster().startNode(Settings.builder()
            .put("node.master", false)
            .build());
        ensureStableClusterOnAllNodes(2);

        // index some datafeed data
        client().admin().indices().prepareCreate("data")
            .setMapping("time", "type=date")
            .get();
        long numDocs1 = randomIntBetween(32, 2048);
        long now = System.currentTimeMillis();
        long weekAgo = now - 604800000;
        long twoWeeksAgo = weekAgo - 604800000;
        indexDocs(logger, "data", numDocs1, twoWeeksAgo, weekAgo);

        String jobId = "test-stop-and-force-stop";
        String datafeedId = jobId + "-datafeed";
        setupJobAndDatafeed(jobId, datafeedId, TimeValue.timeValueHours(1));
        waitForDatafeed(jobId, numDocs1);

        GetDatafeedsStatsAction.Request datafeedStatsRequest = new GetDatafeedsStatsAction.Request(datafeedId);
        GetDatafeedsStatsAction.Response datafeedStatsResponse =
            client().execute(GetDatafeedsStatsAction.INSTANCE, datafeedStatsRequest).actionGet();
        assertEquals(DatafeedState.STARTED, datafeedStatsResponse.getResponse().results().get(0).getDatafeedState());

        // Stop the datafeed normally
        StopDatafeedAction.Request stopDatafeedRequest = new StopDatafeedAction.Request(datafeedId);
        ActionFuture<StopDatafeedAction.Response> normalStopActionFuture
            = client().execute(StopDatafeedAction.INSTANCE, stopDatafeedRequest);

        // Force stop the datafeed without waiting for the normal stop to return first
        stopDatafeedRequest = new StopDatafeedAction.Request(datafeedId);
        stopDatafeedRequest.setForce(true);
        StopDatafeedAction.Response stopDatafeedResponse = client().execute(StopDatafeedAction.INSTANCE, stopDatafeedRequest).actionGet();
        assertTrue(stopDatafeedResponse.isStopped());

        // Confirm that the normal stop also reports success - whichever way the datafeed
        // ends up getting stopped it's not an error to stop a stopped datafeed
        stopDatafeedResponse = normalStopActionFuture.actionGet();
        assertTrue(stopDatafeedResponse.isStopped());

        CloseJobAction.Request closeJobRequest = new CloseJobAction.Request(jobId);
        CloseJobAction.Response closeJobResponse = client().execute(CloseJobAction.INSTANCE, closeJobRequest).actionGet();
        assertTrue(closeJobResponse.isClosed());
    }

    public void testJobRelocationIsMemoryAware() throws Exception {

        internalCluster().ensureAtLeastNumDataNodes(1);
        ensureStableClusterOnAllNodes(1);

        // Open 4 small jobs.  Since there is only 1 node in the cluster they'll have to go on that node.

        setupJobWithoutDatafeed("small1", new ByteSizeValue(2, ByteSizeUnit.MB));
        setupJobWithoutDatafeed("small2", new ByteSizeValue(2, ByteSizeUnit.MB));
        setupJobWithoutDatafeed("small3", new ByteSizeValue(2, ByteSizeUnit.MB));
        setupJobWithoutDatafeed("small4", new ByteSizeValue(2, ByteSizeUnit.MB));

        // Expand the cluster to 3 nodes.  The 4 small jobs will stay on the
        // same node because we don't rebalance jobs that are happily running.

        internalCluster().ensureAtLeastNumDataNodes(3);
        ensureStableClusterOnAllNodes(3);

        // Wait for the cluster to be green - this means the indices have been replicated.

        ensureGreen();

        // Open a big job.  This should go on a different node to the 4 small ones.

        setupJobWithoutDatafeed("big1", new ByteSizeValue(500, ByteSizeUnit.MB));

        // Stop the current master node - this should be the one with the 4 small jobs on.

        internalCluster().stopCurrentMasterNode();
        ensureStableClusterOnAllNodes(2);

        // If memory requirements are used to reallocate the 4 small jobs (as we expect) then they should
        // all reallocate to the same node, that being the one that doesn't have the big job on.  If job counts
        // are used to reallocate the small jobs then this implies the fallback allocation mechanism has been
        // used in a situation we don't want it to be used in, and at least one of the small jobs will be on
        // the same node as the big job.  (This all relies on xpack.ml.node_concurrent_job_allocations being set
        // to at least 4, which we do in the nodeSettings() method.)

        assertBusy(() -> {
            GetJobsStatsAction.Response statsResponse =
                client().execute(GetJobsStatsAction.INSTANCE, new GetJobsStatsAction.Request(MetaData.ALL)).actionGet();
            QueryPage<JobStats> jobStats = statsResponse.getResponse();
            assertNotNull(jobStats);
            List<String> smallJobNodes = jobStats.results().stream().filter(s -> s.getJobId().startsWith("small") && s.getNode() != null)
                .map(s -> s.getNode().getName()).collect(Collectors.toList());
            List<String> bigJobNodes = jobStats.results().stream().filter(s -> s.getJobId().startsWith("big") && s.getNode() != null)
                .map(s -> s.getNode().getName()).collect(Collectors.toList());
            logger.info("small job nodes: " + smallJobNodes + ", big job nodes: " + bigJobNodes);
            assertEquals(5, jobStats.count());
            assertEquals(4, smallJobNodes.size());
            assertEquals(1, bigJobNodes.size());
            assertEquals(1L, smallJobNodes.stream().distinct().count());
            assertEquals(1L, bigJobNodes.stream().distinct().count());
            assertNotEquals(smallJobNodes, bigJobNodes);
        });
    }

    private void setupJobWithoutDatafeed(String jobId, ByteSizeValue modelMemoryLimit) throws Exception {
        Job.Builder job = createFareQuoteJob(jobId, modelMemoryLimit);
        PutJobAction.Request putJobRequest = new PutJobAction.Request(job);
        client().execute(PutJobAction.INSTANCE, putJobRequest).actionGet();

        client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(job.getId())).actionGet();
        assertBusy(() -> {
            GetJobsStatsAction.Response statsResponse =
                client().execute(GetJobsStatsAction.INSTANCE, new GetJobsStatsAction.Request(job.getId())).actionGet();
            assertEquals(JobState.OPENED, statsResponse.getResponse().results().get(0).getState());
        });
    }

    private void setupJobAndDatafeed(String jobId, String datafeedId, TimeValue datafeedFrequency) throws Exception {
        Job.Builder job = createScheduledJob(jobId);
        PutJobAction.Request putJobRequest = new PutJobAction.Request(job);
        client().execute(PutJobAction.INSTANCE, putJobRequest).actionGet();

        DatafeedConfig config = createDatafeed(datafeedId, job.getId(), Collections.singletonList("data"), datafeedFrequency);
        PutDatafeedAction.Request putDatafeedRequest = new PutDatafeedAction.Request(config);
        client().execute(PutDatafeedAction.INSTANCE, putDatafeedRequest).actionGet();

        client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(job.getId()));
        assertBusy(() -> {
            GetJobsStatsAction.Response statsResponse =
                    client().execute(GetJobsStatsAction.INSTANCE, new GetJobsStatsAction.Request(job.getId())).actionGet();
            assertEquals(JobState.OPENED, statsResponse.getResponse().results().get(0).getState());
        }, 20, TimeUnit.SECONDS);

        setMlIndicesDelayedNodeLeftTimeoutToZero();

        StartDatafeedAction.Request startDatafeedRequest = new StartDatafeedAction.Request(config.getId(), 0L);
        client().execute(StartDatafeedAction.INSTANCE, startDatafeedRequest).get();
    }

    private void run(String jobId, CheckedRunnable<Exception> disrupt) throws Exception {
        client().admin().indices().prepareCreate("data")
                .setMapping("time", "type=date")
                .get();
        long numDocs1 = randomIntBetween(32, 2048);
        long now = System.currentTimeMillis();
        long weekAgo = now - 604800000;
        long twoWeeksAgo = weekAgo - 604800000;
        indexDocs(logger, "data", numDocs1, twoWeeksAgo, weekAgo);

        setupJobAndDatafeed(jobId, "data_feed_id", TimeValue.timeValueSeconds(1));
        waitForDatafeed(jobId, numDocs1);

        client().admin().indices().prepareFlush().get();

        disrupt.run();

        PersistentTasksClusterService persistentTasksClusterService =
            internalCluster().getInstance(PersistentTasksClusterService.class, internalCluster().getMasterName());
        // Speed up rechecks to a rate that is quicker than what settings would allow.
        // The tests would work eventually without doing this, but the assertBusy() below
        // would need to wait 30 seconds, which would make the suite run very slowly.
        // The 200ms refresh puts a greater burden on the master node to recheck
        // persistent tasks, but it will cope in these tests as it's not doing anything
        // else.
        persistentTasksClusterService.setRecheckInterval(TimeValue.timeValueMillis(200));

        assertBusy(() -> {
            ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
            PersistentTasksCustomMetaData tasks = clusterState.metaData().custom(PersistentTasksCustomMetaData.TYPE);
            assertNotNull(tasks);
            assertEquals("Expected 2 tasks, but got [" + tasks.taskMap() + "]", 2, tasks.taskMap().size());
            for (PersistentTask<?> task : tasks.tasks()) {
                assertFalse(needsReassignment(task.getAssignment(), clusterState.nodes()));
            }

            GetJobsStatsAction.Request jobStatsRequest = new GetJobsStatsAction.Request(jobId);
            JobStats jobStats = client().execute(GetJobsStatsAction.INSTANCE, jobStatsRequest).actionGet()
                    .getResponse().results().get(0);
            assertEquals(JobState.OPENED, jobStats.getState());
            assertNotNull(jobStats.getNode());

            GetDatafeedsStatsAction.Request datafeedStatsRequest = new GetDatafeedsStatsAction.Request("data_feed_id");
            DatafeedStats datafeedStats = client().execute(GetDatafeedsStatsAction.INSTANCE, datafeedStatsRequest).actionGet()
                    .getResponse().results().get(0);
            assertEquals(DatafeedState.STARTED, datafeedStats.getDatafeedState());
            assertNotNull(datafeedStats.getNode());
        });

        long numDocs2 = randomIntBetween(2, 64);
        long now2 = System.currentTimeMillis();
        indexDocs(logger, "data", numDocs2, now2 + 5000, now2 + 6000);
        waitForDatafeed(jobId, numDocs1 + numDocs2);
    }

    // Get datacounts from index instead of via job stats api,
    // because then data counts have been persisted to an index (happens each 10s (DataCountsReporter)),
    // so when restarting job on another node the data counts
    // are what we expect them to be:
    private static DataCounts getDataCountsFromIndex(String jobId) {
        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(QueryBuilders.idsQuery().addIds(DataCounts.documentId(jobId)))
                .get();
        if (searchResponse.getHits().getTotalHits().value != 1) {
            return new DataCounts(jobId);
        }

        BytesReference source = searchResponse.getHits().getHits()[0].getSourceRef();
        try (XContentParser parser = XContentHelper.createParser(NamedXContentRegistry.EMPTY,
                        DeprecationHandler.THROW_UNSUPPORTED_OPERATION, source, XContentType.JSON)) {
            return DataCounts.PARSER.apply(parser, null);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void waitForDatafeed(String jobId, long numDocs) throws Exception {
        assertBusy(() -> {
            DataCounts dataCounts = getDataCountsFromIndex(jobId);
            assertEquals(numDocs, dataCounts.getProcessedRecordCount());
            assertEquals(0L, dataCounts.getOutOfOrderTimeStampCount());
        }, 30, TimeUnit.SECONDS);
    }

    private void ensureStableClusterOnAllNodes(int nodeCount) {
        for (String nodeName : internalCluster().getNodeNames()) {
            ensureStableCluster(nodeCount, nodeName);
        }
    }
}
