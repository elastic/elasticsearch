/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.CheckedRunnable;
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
import org.elasticsearch.persistent.PersistentTasksClusterService;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData.PersistentTask;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.action.CloseJobAction;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsStatsAction.Response.DatafeedStats;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction.Response.JobStats;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
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

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/43670")
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
                .addMapping("type", "time", "type=date")
                .get();
        long numDocs1 = randomIntBetween(32, 2048);
        long now = System.currentTimeMillis();
        long weekAgo = now - 604800000;
        long twoWeeksAgo = weekAgo - 604800000;
        indexDocs(logger, "data", numDocs1, twoWeeksAgo, weekAgo);

        String jobId = "test-lose-ml-node";
        String datafeedId = jobId + "-datafeed";
        setupJobAndDatafeed(jobId, datafeedId);
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

        // Can't normal stop an unassigned job
        CloseJobAction.Request closeJobRequest = new CloseJobAction.Request(jobId);
        ElasticsearchStatusException statusException = expectThrows(ElasticsearchStatusException.class,
                () -> client().execute(CloseJobAction.INSTANCE, closeJobRequest).actionGet());
        assertEquals("Cannot close job [" + jobId +
                        "] because the job does not have an assigned node. Use force close to close the job",
                statusException.getMessage());

        // Can only force close an unassigned job
        closeJobRequest.setForce(true);
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

    private void setupJobAndDatafeed(String jobId, String datafeedId) throws Exception {
        Job.Builder job = createScheduledJob(jobId);
        PutJobAction.Request putJobRequest = new PutJobAction.Request(job);
        client().execute(PutJobAction.INSTANCE, putJobRequest).actionGet();

        DatafeedConfig config = createDatafeed(datafeedId, job.getId(), Collections.singletonList("data"));
        PutDatafeedAction.Request putDatafeedRequest = new PutDatafeedAction.Request(config);
        client().execute(PutDatafeedAction.INSTANCE, putDatafeedRequest).actionGet();

        client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(job.getId()));
        assertBusy(() -> {
            GetJobsStatsAction.Response statsResponse =
                    client().execute(GetJobsStatsAction.INSTANCE, new GetJobsStatsAction.Request(job.getId())).actionGet();
            assertEquals(JobState.OPENED, statsResponse.getResponse().results().get(0).getState());
        });

        StartDatafeedAction.Request startDatafeedRequest = new StartDatafeedAction.Request(config.getId(), 0L);
        client().execute(StartDatafeedAction.INSTANCE, startDatafeedRequest).get();
    }

    private void run(String jobId, CheckedRunnable<Exception> disrupt) throws Exception {
        client().admin().indices().prepareCreate("data")
                .addMapping("type", "time", "type=date")
                .get();
        long numDocs1 = randomIntBetween(32, 2048);
        long now = System.currentTimeMillis();
        long weekAgo = now - 604800000;
        long twoWeeksAgo = weekAgo - 604800000;
        indexDocs(logger, "data", numDocs1, twoWeeksAgo, weekAgo);

        setupJobAndDatafeed(jobId, "data_feed_id");
        waitForDatafeed(jobId, numDocs1);

        client().admin().indices().prepareSyncedFlush().get();

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
