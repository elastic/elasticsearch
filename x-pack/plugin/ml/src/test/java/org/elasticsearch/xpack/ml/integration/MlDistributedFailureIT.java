/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.CheckedRunnable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData.PersistentTask;
import org.elasticsearch.test.junit.annotations.TestLogging;
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
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.persistent.PersistentTasksClusterService.needsReassignment;

public class MlDistributedFailureIT extends BaseMlIntegTestCase {

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

    @TestLogging("org.elasticsearch.xpack.ml.action:DEBUG,org.elasticsearch.xpack.persistent:TRACE," +
            "org.elasticsearch.xpack.ml.datafeed:TRACE")
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
            internalCluster().stopRandomNode(settings -> settings.getAsBoolean("node.master", false));
            assertBusy(() -> {
                ClusterState state = client(mlAndDataNode).admin().cluster().prepareState()
                        .setLocal(true).get().getState();
                assertNull(state.nodes().getMasterNodeId());
            });
            logger.info("Restarting dedicated master node");
            internalCluster().startNode(Settings.builder()
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
        internalCluster().stopRandomNonMasterNode();

        // Job state is opened but the job is not assigned to a node (because we just killed the only ML node)
        GetJobsStatsAction.Request jobStatsRequest = new GetJobsStatsAction.Request(jobId);
        GetJobsStatsAction.Response jobStatsResponse = client().execute(GetJobsStatsAction.INSTANCE, jobStatsRequest).actionGet();
        assertEquals(jobStatsResponse.getResponse().results().get(0).getState(), JobState.OPENED);

        GetDatafeedsStatsAction.Request datafeedStatsRequest = new GetDatafeedsStatsAction.Request(datafeedId);
        GetDatafeedsStatsAction.Response datafeedStatsResponse =
                client().execute(GetDatafeedsStatsAction.INSTANCE, datafeedStatsRequest).actionGet();
        assertEquals(datafeedStatsResponse.getResponse().results().get(0).getDatafeedState(), DatafeedState.STARTED);

        // Can't normal stop an unassigned datafeed
        StopDatafeedAction.Request stopDatafeedRequest = new StopDatafeedAction.Request(datafeedId);
        ElasticsearchStatusException statusException = expectThrows(ElasticsearchStatusException.class,
                () -> client().execute(StopDatafeedAction.INSTANCE, stopDatafeedRequest).actionGet());
        assertEquals("Cannot stop datafeed [" + datafeedId +
                        "] because the datafeed does not have an assigned node. Use force stop to stop the datafeed",
                statusException.getMessage());

        // Can only force stop an unassigned datafeed
        stopDatafeedRequest.setForce(true);
        StopDatafeedAction.Response stopDatafeedResponse = client().execute(StopDatafeedAction.INSTANCE, stopDatafeedRequest).actionGet();
        assertTrue(stopDatafeedResponse.isStopped());

        // Can't normal stop an unassigned job
        CloseJobAction.Request closeJobRequest = new CloseJobAction.Request(jobId);
        statusException = expectThrows(ElasticsearchStatusException.class,
                () -> client().execute(CloseJobAction.INSTANCE, closeJobRequest).actionGet());
        assertEquals("Cannot close job [" + jobId +
                        "] because the job does not have an assigned node. Use force close to close the job",
                statusException.getMessage());

        // Can only force close an unassigned job
        closeJobRequest.setForce(true);
        CloseJobAction.Response closeJobResponse = client().execute(CloseJobAction.INSTANCE, closeJobRequest).actionGet();
        assertTrue(closeJobResponse.isClosed());
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
            assertEquals(statsResponse.getResponse().results().get(0).getState(), JobState.OPENED);
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
