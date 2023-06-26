/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.persistent.PersistentTaskResponse;
import org.elasticsearch.persistent.PersistentTasksClusterService;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.PersistentTask;
import org.elasticsearch.persistent.UpdatePersistentTaskStatusAction;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
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
import org.elasticsearch.xpack.core.ml.action.UpdateJobAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.config.JobUpdate;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.core.ml.notifications.NotificationsIndex;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsPersister;
import org.elasticsearch.xpack.ml.job.process.autodetect.BlackHoleAutodetectProcess;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.persistent.PersistentTasksClusterService.needsReassignment;
import static org.elasticsearch.test.NodeRoles.masterOnlyNode;
import static org.elasticsearch.test.NodeRoles.onlyRole;
import static org.elasticsearch.test.NodeRoles.onlyRoles;
import static org.elasticsearch.xpack.core.ml.MlTasks.DATAFEED_TASK_NAME;
import static org.elasticsearch.xpack.core.ml.MlTasks.JOB_TASK_NAME;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class MlDistributedFailureIT extends BaseMlIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(MachineLearning.CONCURRENT_JOB_ALLOCATIONS.getKey(), 4)
            .build();
    }

    public void testFailOver() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(3);
        ensureStableCluster();
        run("fail-over-job", () -> {
            GetJobsStatsAction.Request request = new GetJobsStatsAction.Request("fail-over-job");
            GetJobsStatsAction.Response response = client().execute(GetJobsStatsAction.INSTANCE, request).actionGet();
            DiscoveryNode discoveryNode = response.getResponse().results().get(0).getNode();
            internalCluster().stopNode(discoveryNode.getName());
            ensureStableCluster();
        });
    }

    @Before
    public void setLogging() {
        updateClusterSettings(Settings.builder().put("logger.org.elasticsearch.xpack.ml.utils.persistence", "TRACE"));
    }

    @After
    public void unsetLogging() {
        updateClusterSettings(Settings.builder().putNull("logger.org.elasticsearch.xpack.ml.utils.persistence"));
    }

    public void testLoseDedicatedMasterNode() throws Exception {
        internalCluster().ensureAtMostNumDataNodes(0);
        logger.info("Starting dedicated master node...");
        internalCluster().startMasterOnlyNode();
        logger.info("Starting ml and data node...");
        String mlAndDataNode = internalCluster().startNode(onlyRoles(Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.ML_ROLE)));
        ensureStableCluster();
        run("lose-dedicated-master-node-job", () -> {
            logger.info("Stopping dedicated master node");
            Settings masterDataPathSettings = internalCluster().dataPathSettings(internalCluster().getMasterName());
            internalCluster().stopCurrentMasterNode();
            assertBusy(() -> {
                ClusterState state = client(mlAndDataNode).admin().cluster().prepareState().setLocal(true).get().getState();
                assertNull(state.nodes().getMasterNodeId());
            });
            logger.info("Restarting dedicated master node");
            internalCluster().startNode(Settings.builder().put(masterDataPathSettings).put(masterOnlyNode()).build());
            ensureStableCluster();
        });
    }

    public void testFullClusterRestart() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(3);
        ensureStableCluster();
        run("full-cluster-restart-job", () -> {
            logger.info("Restarting all nodes");
            internalCluster().fullRestart();
            logger.info("Restarted all nodes");
            ensureStableCluster();
        });
    }

    public void testCloseUnassignedJobAndDatafeed() throws Exception {
        internalCluster().ensureAtMostNumDataNodes(0);
        logger.info("Starting data and master node...");
        internalCluster().startNode(onlyRoles(Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE)));
        logger.info("Starting ml and data node...");
        internalCluster().startNode(onlyRoles(Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.ML_ROLE)));
        ensureStableCluster();

        // index some datafeed data
        client().admin().indices().prepareCreate("data").setMapping("time", "type=date").get();
        long numDocs1 = randomIntBetween(32, 2048);
        long now = System.currentTimeMillis();
        long weekAgo = now - 604800000;
        long twoWeeksAgo = weekAgo - 604800000;
        indexDocs(logger, "data", numDocs1, twoWeeksAgo, weekAgo);

        String jobId = "test-lose-ml-node";
        String datafeedId = jobId + "-datafeed";
        setupJobAndDatafeed(jobId, datafeedId, TimeValue.timeValueHours(1));
        waitForJobToHaveProcessedExactly(jobId, numDocs1);

        // stop the only ML node
        ensureGreen(); // replicas must be assigned, otherwise we could lose a whole index
        internalCluster().stopRandomNonMasterNode();
        ensureStableCluster(1);

        // Job state is opened but the job is not assigned to a node (because we just killed the only ML node)
        GetJobsStatsAction.Request jobStatsRequest = new GetJobsStatsAction.Request(jobId);
        GetJobsStatsAction.Response jobStatsResponse = client().execute(GetJobsStatsAction.INSTANCE, jobStatsRequest).actionGet();
        assertEquals(JobState.OPENED, jobStatsResponse.getResponse().results().get(0).getState());

        GetDatafeedsStatsAction.Request datafeedStatsRequest = new GetDatafeedsStatsAction.Request(datafeedId);
        GetDatafeedsStatsAction.Response datafeedStatsResponse = client().execute(GetDatafeedsStatsAction.INSTANCE, datafeedStatsRequest)
            .actionGet();
        assertEquals(DatafeedState.STARTED, datafeedStatsResponse.getResponse().results().get(0).getDatafeedState());

        // An unassigned datafeed can be stopped either normally or by force
        StopDatafeedAction.Request stopDatafeedRequest = new StopDatafeedAction.Request(datafeedId);
        stopDatafeedRequest.setForce(randomBoolean());
        StopDatafeedAction.Response stopDatafeedResponse = client().execute(StopDatafeedAction.INSTANCE, stopDatafeedRequest).actionGet();
        assertTrue(stopDatafeedResponse.isStopped());

        // Since 7.5 we can also stop an unassigned job either normally or by force
        CloseJobAction.Request closeJobRequest = new CloseJobAction.Request(jobId);
        boolean closeWithForce = randomBoolean();
        closeJobRequest.setForce(closeWithForce);
        CloseJobAction.Response closeJobResponse = client().execute(CloseJobAction.INSTANCE, closeJobRequest).actionGet();
        assertTrue(closeJobResponse.isClosed());

        // We should have an audit message indicating that the datafeed was stopped
        SearchRequest datafeedAuditSearchRequest = new SearchRequest(NotificationsIndex.NOTIFICATIONS_INDEX);
        datafeedAuditSearchRequest.source().query(new TermsQueryBuilder("message.raw", "Datafeed stopped"));
        assertBusy(() -> {
            assertTrue(indexExists(NotificationsIndex.NOTIFICATIONS_INDEX));
            SearchResponse searchResponse = client().search(datafeedAuditSearchRequest).actionGet();
            assertThat(searchResponse.getHits(), notNullValue());
            assertThat(searchResponse.getHits().getHits(), arrayWithSize(1));
            assertThat(searchResponse.getHits().getHits()[0].getSourceAsMap().get("job_id"), is(jobId));
        });

        // We should have an audit message indicating that the job was closed
        String expectedAuditMessage = closeWithForce ? "Job is closing (forced)" : "Job is closing";
        SearchRequest jobAuditSearchRequest = new SearchRequest(NotificationsIndex.NOTIFICATIONS_INDEX);
        jobAuditSearchRequest.source().query(new TermsQueryBuilder("message.raw", expectedAuditMessage));
        assertBusy(() -> {
            assertTrue(indexExists(NotificationsIndex.NOTIFICATIONS_INDEX));
            SearchResponse searchResponse = client().search(jobAuditSearchRequest).actionGet();
            assertThat(searchResponse.getHits(), notNullValue());
            assertThat(searchResponse.getHits().getHits(), arrayWithSize(1));
            assertThat(searchResponse.getHits().getHits()[0].getSourceAsMap().get("job_id"), is(jobId));
        });
    }

    public void testCloseUnassignedFailedJobAndStopUnassignedStoppingDatafeed() throws Exception {
        internalCluster().ensureAtMostNumDataNodes(0);
        logger.info("Starting master/data nodes...");
        for (int count = 0; count < 3; ++count) {
            internalCluster().startNode(onlyRoles(Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE)));
        }
        logger.info("Starting dedicated ml node...");
        internalCluster().startNode(onlyRole(DiscoveryNodeRole.ML_ROLE));
        ensureStableCluster();

        // index some datafeed data
        client().admin().indices().prepareCreate("data").setMapping("time", "type=date").get();
        long numDocs1 = randomIntBetween(32, 2048);
        long now = System.currentTimeMillis();
        long weekAgo = now - 604800000;
        long twoWeeksAgo = weekAgo - 604800000;
        indexDocs(logger, "data", numDocs1, twoWeeksAgo, weekAgo);

        String jobId = "test-stop-unassigned-datafeed-for-failed-job";
        String datafeedId = jobId + "-datafeed";
        setupJobAndDatafeed(jobId, datafeedId, TimeValue.timeValueHours(1));
        waitForJobToHaveProcessedExactly(jobId, numDocs1);

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
            new BytesArray("{ \"time\" : \"" + BlackHoleAutodetectProcess.MAGIC_FAILURE_VALUE_AS_DATE + "\" }"),
            XContentType.JSON
        );
        PostDataAction.Response postDataResponse = client().execute(PostDataAction.INSTANCE, postDataRequest).actionGet();
        assertEquals(1L, postDataResponse.getDataCounts().getInputRecordCount());

        // Confirm the job state is now failed - this may take a while to update in cluster state
        assertBusy(() -> {
            GetJobsStatsAction.Request jobStatsRequest2 = new GetJobsStatsAction.Request(jobId);
            GetJobsStatsAction.Response jobStatsResponse2 = client().execute(GetJobsStatsAction.INSTANCE, jobStatsRequest2).actionGet();
            assertEquals(JobState.FAILED, jobStatsResponse2.getResponse().results().get(0).getState());
        });

        // It's impossible to reliably get the datafeed into a stopping state at the point when the ML node is removed from the cluster
        // using externally accessible actions. The only way this situation could occur in reality is through extremely unfortunate
        // timing. Therefore, to simulate this unfortunate timing we cheat and access internal classes to set the datafeed state to
        // stopping.
        PersistentTasksCustomMetadata tasks = clusterService().state().getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
        PersistentTasksCustomMetadata.PersistentTask<?> task = MlTasks.getDatafeedTask(datafeedId, tasks);

        // It is possible that the datafeed has already detected the job failure and
        // terminated itself. In this happens there is no persistent task to stop
        if (task == null) {
            // We have to force close the job, because the standard cleanup
            // will treat a leftover failed job as a fatal error
            CloseJobAction.Request closeJobRequest = new CloseJobAction.Request(jobId);
            closeJobRequest.setForce(true);
            client().execute(CloseJobAction.INSTANCE, closeJobRequest).actionGet();
            assumeFalse(
                "The datafeed task is null most likely because the datafeed detected the job had failed. "
                    + "This is expected to happen extremely rarely but the test cannot continue in these circumstances.",
                task == null
            );
        }

        UpdatePersistentTaskStatusAction.Request updatePersistentTaskStatusRequest = new UpdatePersistentTaskStatusAction.Request(
            task.getId(),
            task.getAllocationId(),
            DatafeedState.STOPPING
        );
        PersistentTaskResponse updatePersistentTaskStatusResponse = client().execute(
            UpdatePersistentTaskStatusAction.INSTANCE,
            updatePersistentTaskStatusRequest
        ).actionGet();
        assertNotNull(updatePersistentTaskStatusResponse.getTask());

        // Confirm the datafeed state is now stopping - this may take a while to update in cluster state
        assertBusy(() -> {
            GetDatafeedsStatsAction.Request datafeedStatsRequest = new GetDatafeedsStatsAction.Request(datafeedId);
            GetDatafeedsStatsAction.Response datafeedStatsResponse = client().execute(
                GetDatafeedsStatsAction.INSTANCE,
                datafeedStatsRequest
            ).actionGet();
            assertEquals(DatafeedState.STOPPING, datafeedStatsResponse.getResponse().results().get(0).getDatafeedState());
        });

        // Stop the node running the failed job/stopping datafeed
        ensureGreen(); // replicas must be assigned, otherwise we could lose a whole index
        internalCluster().stopNode(jobNode.getName());
        ensureStableCluster(3);

        // We should be allowed to force stop the unassigned datafeed even though it is stopping and its job has failed
        StopDatafeedAction.Request stopDatafeedRequest = new StopDatafeedAction.Request(datafeedId);
        stopDatafeedRequest.setForce(true);
        StopDatafeedAction.Response stopDatafeedResponse = client().execute(StopDatafeedAction.INSTANCE, stopDatafeedRequest).actionGet();
        assertTrue(stopDatafeedResponse.isStopped());

        // Confirm the datafeed state is now stopped - shouldn't need a busy check here as
        // the stop endpoint shouldn't return until its effects are externally visible
        GetDatafeedsStatsAction.Request datafeedStatsRequest2 = new GetDatafeedsStatsAction.Request(datafeedId);
        GetDatafeedsStatsAction.Response datafeedStatsResponse2 = client().execute(GetDatafeedsStatsAction.INSTANCE, datafeedStatsRequest2)
            .actionGet();
        assertEquals(DatafeedState.STOPPED, datafeedStatsResponse2.getResponse().results().get(0).getDatafeedState());

        // We should be allowed to force stop the unassigned failed job
        CloseJobAction.Request closeJobRequest = new CloseJobAction.Request(jobId);
        closeJobRequest.setForce(true);
        CloseJobAction.Response closeJobResponse = client().execute(CloseJobAction.INSTANCE, closeJobRequest).actionGet();
        assertTrue(closeJobResponse.isClosed());
    }

    public void testStopAndForceStopDatafeed() throws Exception {
        internalCluster().ensureAtMostNumDataNodes(0);
        logger.info("Starting dedicated master node...");
        internalCluster().startMasterOnlyNode();
        logger.info("Starting ml and data node...");
        internalCluster().startNode(onlyRoles(Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.ML_ROLE)));
        ensureStableCluster();

        // index some datafeed data
        client().admin().indices().prepareCreate("data").setMapping("time", "type=date").get();
        long numDocs1 = randomIntBetween(32, 2048);
        long now = System.currentTimeMillis();
        long weekAgo = now - 604800000;
        long twoWeeksAgo = weekAgo - 604800000;
        indexDocs(logger, "data", numDocs1, twoWeeksAgo, weekAgo);

        String jobId = "test-stop-and-force-stop";
        String datafeedId = jobId + "-datafeed";
        setupJobAndDatafeed(jobId, datafeedId, TimeValue.timeValueHours(1));
        waitForJobToHaveProcessedExactly(jobId, numDocs1);

        GetDatafeedsStatsAction.Request datafeedStatsRequest = new GetDatafeedsStatsAction.Request(datafeedId);
        GetDatafeedsStatsAction.Response datafeedStatsResponse = client().execute(GetDatafeedsStatsAction.INSTANCE, datafeedStatsRequest)
            .actionGet();
        assertEquals(DatafeedState.STARTED, datafeedStatsResponse.getResponse().results().get(0).getDatafeedState());

        // Stop the datafeed normally
        StopDatafeedAction.Request stopDatafeedRequest = new StopDatafeedAction.Request(datafeedId);
        ActionFuture<StopDatafeedAction.Response> normalStopActionFuture = client().execute(
            StopDatafeedAction.INSTANCE,
            stopDatafeedRequest
        );

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
        ensureStableCluster();

        // Open 4 small jobs. Since there is only 1 node in the cluster they'll have to go on that node.

        setupJobWithoutDatafeed("small1", ByteSizeValue.ofMb(2));
        setupJobWithoutDatafeed("small2", ByteSizeValue.ofMb(2));
        setupJobWithoutDatafeed("small3", ByteSizeValue.ofMb(2));
        setupJobWithoutDatafeed("small4", ByteSizeValue.ofMb(2));

        // Expand the cluster to 3 nodes. The 4 small jobs will stay on the
        // same node because we don't rebalance jobs that are happily running.

        internalCluster().ensureAtLeastNumDataNodes(3);
        ensureStableCluster();

        // Wait for the cluster to be green - this means the indices have been replicated.

        ensureGreen();

        // Open a big job. This should go on a different node to the 4 small ones.

        setupJobWithoutDatafeed("big1", ByteSizeValue.ofMb(500));

        // Stop the current master node - this should be the one with the 4 small jobs on.

        internalCluster().stopCurrentMasterNode();
        ensureStableCluster();

        PersistentTasksClusterService persistentTasksClusterService = internalCluster().getInstance(
            PersistentTasksClusterService.class,
            internalCluster().getMasterName()
        );
        // Speed up rechecks to a rate that is quicker than what settings would allow.
        // The tests would work eventually without doing this, but the assertBusy() below
        // would need to wait 30 seconds, which would make the suite run very slowly.
        // The 200ms refresh puts a greater burden on the master node to recheck
        // persistent tasks, but it will cope in these tests as it's not doing anything
        // else.
        persistentTasksClusterService.setRecheckInterval(TimeValue.timeValueMillis(200));

        // If memory requirements are used to reallocate the 4 small jobs (as we expect) then they should
        // all reallocate to the same node, that being the one that doesn't have the big job on. If job counts
        // are used to reallocate the small jobs then this implies the fallback allocation mechanism has been
        // used in a situation we don't want it to be used in, and at least one of the small jobs will be on
        // the same node as the big job. (This all relies on xpack.ml.node_concurrent_job_allocations being set
        // to at least 4, which we do in the nodeSettings() method.)

        assertBusy(() -> {
            GetJobsStatsAction.Response statsResponse = client().execute(
                GetJobsStatsAction.INSTANCE,
                new GetJobsStatsAction.Request(Metadata.ALL)
            ).actionGet();
            QueryPage<JobStats> jobStats = statsResponse.getResponse();
            assertNotNull(jobStats);
            List<String> smallJobNodes = jobStats.results()
                .stream()
                .filter(s -> s.getJobId().startsWith("small") && s.getNode() != null)
                .map(s -> s.getNode().getName())
                .collect(Collectors.toList());
            List<String> bigJobNodes = jobStats.results()
                .stream()
                .filter(s -> s.getJobId().startsWith("big") && s.getNode() != null)
                .map(s -> s.getNode().getName())
                .collect(Collectors.toList());
            logger.info("small job nodes: " + smallJobNodes + ", big job nodes: " + bigJobNodes);
            assertEquals(5, jobStats.count());
            assertEquals(4, smallJobNodes.size());
            assertEquals(1, bigJobNodes.size());
            assertEquals(1L, smallJobNodes.stream().distinct().count());
            assertEquals(1L, bigJobNodes.stream().distinct().count());
            assertNotEquals(smallJobNodes, bigJobNodes);
        });
    }

    public void testClusterWithTwoMlNodes_RunsDatafeed_GivenOriginalNodeGoesDown() throws Exception {
        internalCluster().ensureAtMostNumDataNodes(0);
        logger.info("Starting dedicated master node...");
        internalCluster().startMasterOnlyNode();
        logger.info("Starting ml and data node...");
        internalCluster().startNode(onlyRoles(Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.ML_ROLE)));
        logger.info("Starting another ml and data node...");
        internalCluster().startNode(onlyRoles(Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.ML_ROLE)));
        ensureStableCluster();

        // index some datafeed data
        client().admin().indices().prepareCreate("data").setMapping("time", "type=date").get();
        long numDocs = 80000;
        long now = System.currentTimeMillis();
        long weekAgo = now - 604800000;
        long twoWeeksAgo = weekAgo - 604800000;
        indexDocs(logger, "data", numDocs, twoWeeksAgo, weekAgo);

        String jobId = "test-node-goes-down-while-running-job";
        String datafeedId = jobId + "-datafeed";

        Job.Builder job = createScheduledJob(jobId);
        PutJobAction.Request putJobRequest = new PutJobAction.Request(job);
        client().execute(PutJobAction.INSTANCE, putJobRequest).actionGet();

        DatafeedConfig config = createDatafeed(datafeedId, job.getId(), Collections.singletonList("data"), TimeValue.timeValueHours(1));
        PutDatafeedAction.Request putDatafeedRequest = new PutDatafeedAction.Request(config);
        client().execute(PutDatafeedAction.INSTANCE, putDatafeedRequest).actionGet();

        client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(job.getId()));

        assertBusy(() -> {
            GetJobsStatsAction.Response statsResponse = client().execute(
                GetJobsStatsAction.INSTANCE,
                new GetJobsStatsAction.Request(job.getId())
            ).actionGet();
            assertEquals(JobState.OPENED, statsResponse.getResponse().results().get(0).getState());
        }, 30, TimeUnit.SECONDS);

        DiscoveryNode nodeRunningJob = client().execute(GetJobsStatsAction.INSTANCE, new GetJobsStatsAction.Request(job.getId()))
            .actionGet()
            .getResponse()
            .results()
            .get(0)
            .getNode();

        setMlIndicesDelayedNodeLeftTimeoutToZero();

        StartDatafeedAction.Request startDatafeedRequest = new StartDatafeedAction.Request(config.getId(), 0L);
        client().execute(StartDatafeedAction.INSTANCE, startDatafeedRequest).get();

        waitForJobToHaveProcessedAtLeast(jobId, 1000);

        internalCluster().stopNode(nodeRunningJob.getName());

        // Wait for job and datafeed to get reassigned
        assertBusy(() -> {
            assertThat(getJobStats(jobId).getNode(), is(not(nullValue())));
            assertThat(getDatafeedStats(datafeedId).getNode(), is(not(nullValue())));
        }, 30, TimeUnit.SECONDS);

        assertBusy(() -> {
            DataCounts dataCounts = getJobStats(jobId).getDataCounts();
            assertThat(dataCounts.getProcessedRecordCount(), equalTo(numDocs));
            assertThat(dataCounts.getOutOfOrderTimeStampCount(), equalTo(0L));
        });
    }

    public void testClusterWithTwoMlNodes_StopsDatafeed_GivenJobFailsOnReassign() throws Exception {
        internalCluster().ensureAtMostNumDataNodes(0);
        logger.info("Starting dedicated master node...");
        internalCluster().startMasterOnlyNode();
        logger.info("Starting ml and data node...");
        internalCluster().startNode(onlyRoles(Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.ML_ROLE)));
        logger.info("Starting another ml and data node...");
        internalCluster().startNode(onlyRoles(Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.ML_ROLE)));
        ensureStableCluster();

        // index some datafeed data
        client().admin().indices().prepareCreate("data").setMapping("time", "type=date").get();
        long numDocs = 80000;
        long now = System.currentTimeMillis();
        long weekAgo = now - 604800000;
        long twoWeeksAgo = weekAgo - 604800000;
        indexDocs(logger, "data", numDocs, twoWeeksAgo, weekAgo);

        String jobId = "test-node-goes-down-while-running-job";
        String datafeedId = jobId + "-datafeed";

        Job.Builder job = createScheduledJob(jobId);
        PutJobAction.Request putJobRequest = new PutJobAction.Request(job);
        client().execute(PutJobAction.INSTANCE, putJobRequest).actionGet();

        DatafeedConfig config = createDatafeed(datafeedId, job.getId(), Collections.singletonList("data"), TimeValue.timeValueHours(1));
        PutDatafeedAction.Request putDatafeedRequest = new PutDatafeedAction.Request(config);
        client().execute(PutDatafeedAction.INSTANCE, putDatafeedRequest).actionGet();

        client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(job.getId()));

        assertBusy(() -> {
            GetJobsStatsAction.Response statsResponse = client().execute(
                GetJobsStatsAction.INSTANCE,
                new GetJobsStatsAction.Request(job.getId())
            ).actionGet();
            assertEquals(JobState.OPENED, statsResponse.getResponse().results().get(0).getState());
        }, 30, TimeUnit.SECONDS);

        DiscoveryNode nodeRunningJob = client().execute(GetJobsStatsAction.INSTANCE, new GetJobsStatsAction.Request(job.getId()))
            .actionGet()
            .getResponse()
            .results()
            .get(0)
            .getNode();

        setMlIndicesDelayedNodeLeftTimeoutToZero();

        StartDatafeedAction.Request startDatafeedRequest = new StartDatafeedAction.Request(config.getId(), 0L);
        client().execute(StartDatafeedAction.INSTANCE, startDatafeedRequest).get();

        waitForJobToHaveProcessedAtLeast(jobId, 1000);

        // The datafeed should be started
        assertBusy(() -> {
            GetDatafeedsStatsAction.Response statsResponse = client().execute(
                GetDatafeedsStatsAction.INSTANCE,
                new GetDatafeedsStatsAction.Request(config.getId())
            ).actionGet();
            assertEquals(DatafeedState.STARTED, statsResponse.getResponse().results().get(0).getDatafeedState());
        }, 30, TimeUnit.SECONDS);

        // Create a problem that will make the job fail when it restarts on a different node
        String snapshotId = "123";
        ModelSnapshot modelSnapshot = new ModelSnapshot.Builder(jobId).setSnapshotId(snapshotId).setTimestamp(new Date()).build();
        JobResultsPersister jobResultsPersister = internalCluster().getInstance(
            JobResultsPersister.class,
            internalCluster().getMasterName()
        );
        jobResultsPersister.persistModelSnapshot(modelSnapshot, WriteRequest.RefreshPolicy.IMMEDIATE, () -> true);
        UpdateJobAction.Request updateJobRequest = UpdateJobAction.Request.internal(
            jobId,
            new JobUpdate.Builder(jobId).setModelSnapshotId(snapshotId).build()
        );
        client().execute(UpdateJobAction.INSTANCE, updateJobRequest).actionGet();
        refresh(AnomalyDetectorsIndex.resultsWriteAlias(jobId));

        // Make the job move to a different node
        internalCluster().stopNode(nodeRunningJob.getName());

        // Wait for the job to fail during reassignment
        assertBusy(() -> {
            GetJobsStatsAction.Response statsResponse = client().execute(
                GetJobsStatsAction.INSTANCE,
                new GetJobsStatsAction.Request(job.getId())
            ).actionGet();
            assertEquals(JobState.FAILED, statsResponse.getResponse().results().get(0).getState());
        }, 30, TimeUnit.SECONDS);

        // The datafeed should then be stopped
        assertBusy(() -> {
            GetDatafeedsStatsAction.Response statsResponse = client().execute(
                GetDatafeedsStatsAction.INSTANCE,
                new GetDatafeedsStatsAction.Request(config.getId())
            ).actionGet();
            assertEquals(DatafeedState.STOPPED, statsResponse.getResponse().results().get(0).getDatafeedState());
        }, 30, TimeUnit.SECONDS);

        // Force close the failed job to clean up
        client().execute(CloseJobAction.INSTANCE, new CloseJobAction.Request(jobId).setForce(true)).actionGet();
    }

    private void setupJobWithoutDatafeed(String jobId, ByteSizeValue modelMemoryLimit) throws Exception {
        Job.Builder job = createFareQuoteJob(jobId, modelMemoryLimit);
        PutJobAction.Request putJobRequest = new PutJobAction.Request(job);
        client().execute(PutJobAction.INSTANCE, putJobRequest).actionGet();

        client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(job.getId())).actionGet();
        assertBusy(() -> {
            GetJobsStatsAction.Response statsResponse = client().execute(
                GetJobsStatsAction.INSTANCE,
                new GetJobsStatsAction.Request(job.getId())
            ).actionGet();
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
            GetJobsStatsAction.Response statsResponse = client().execute(
                GetJobsStatsAction.INSTANCE,
                new GetJobsStatsAction.Request(job.getId())
            ).actionGet();
            assertEquals(JobState.OPENED, statsResponse.getResponse().results().get(0).getState());
        }, 30, TimeUnit.SECONDS);

        setMlIndicesDelayedNodeLeftTimeoutToZero();

        StartDatafeedAction.Request startDatafeedRequest = new StartDatafeedAction.Request(config.getId(), 0L);
        client().execute(StartDatafeedAction.INSTANCE, startDatafeedRequest).get();
    }

    private void run(String jobId, CheckedRunnable<Exception> disrupt) throws Exception {
        client().admin().indices().prepareCreate("data").setMapping("time", "type=date").get();
        long numDocs1 = randomIntBetween(32, 2048);
        long now = System.currentTimeMillis();
        long weekAgo = now - 604800000;
        long twoWeeksAgo = weekAgo - 604800000;
        indexDocs(logger, "data", numDocs1, twoWeeksAgo, weekAgo);

        setupJobAndDatafeed(jobId, "data_feed_id", TimeValue.timeValueSeconds(1));
        waitForJobToHaveProcessedExactly(jobId, numDocs1);

        // At this point the lookback has completed and normally there would be a model snapshot persisted.
        // We manually index a model snapshot document to imitate this behaviour and to avoid the job
        // having to recover after reassignment.
        indexModelSnapshotFromCurrentJobStats(jobId);

        client().admin().indices().prepareFlush().get();

        disrupt.run();

        PersistentTasksClusterService persistentTasksClusterService = internalCluster().getInstance(
            PersistentTasksClusterService.class,
            internalCluster().getMasterName()
        );
        // Speed up rechecks to a rate that is quicker than what settings would allow.
        // The tests would work eventually without doing this, but the assertBusy() below
        // would need to wait 30 seconds, which would make the suite run very slowly.
        // The 200ms refresh puts a greater burden on the master node to recheck
        // persistent tasks, but it will cope in these tests as it's not doing anything
        // else.
        persistentTasksClusterService.setRecheckInterval(TimeValue.timeValueMillis(200));

        // The timeout here was increased from 10 seconds to 20 seconds in response to the changes in
        // https://github.com/elastic/elasticsearch/pull/50907 - now that the cluster state is stored
        // in a Lucene index it can take a while to update when there are many updates in quick
        // succession, like we see in internal cluster tests of node failure scenarios
        assertBusy(() -> {
            ClusterState clusterState = clusterAdmin().prepareState().get().getState();
            List<PersistentTask<?>> tasks = findTasks(clusterState, Set.of(DATAFEED_TASK_NAME, JOB_TASK_NAME));
            assertNotNull(tasks);
            assertEquals("Expected 2 tasks, but got [" + tasks + "]", 2, tasks.size());
            for (PersistentTask<?> task : tasks) {
                assertFalse(needsReassignment(task.getAssignment(), clusterState.nodes()));
            }

            GetJobsStatsAction.Request jobStatsRequest = new GetJobsStatsAction.Request(jobId);
            JobStats jobStats = client().execute(GetJobsStatsAction.INSTANCE, jobStatsRequest).actionGet().getResponse().results().get(0);
            assertEquals(JobState.OPENED, jobStats.getState());
            assertNotNull(jobStats.getNode());

            GetDatafeedsStatsAction.Request datafeedStatsRequest = new GetDatafeedsStatsAction.Request("data_feed_id");
            DatafeedStats datafeedStats = client().execute(GetDatafeedsStatsAction.INSTANCE, datafeedStatsRequest)
                .actionGet()
                .getResponse()
                .results()
                .get(0);
            assertEquals(DatafeedState.STARTED, datafeedStats.getDatafeedState());
            assertNotNull(datafeedStats.getNode());
        }, 20, TimeUnit.SECONDS);

        long numDocs2 = randomIntBetween(2, 64);
        long now2 = System.currentTimeMillis();
        indexDocs(logger, "data", numDocs2, now2 + 5000, now2 + 6000);
        waitForJobToHaveProcessedExactly(jobId, numDocs1 + numDocs2);
    }

    // Get datacounts from index instead of via job stats api,
    // because then data counts have been persisted to an index (happens each 10s (DataCountsReporter)),
    // so when restarting job on another node the data counts
    // are what we expect them to be:
    private static DataCounts getDataCountsFromIndex(String jobId) {
        SearchResponse searchResponse = client().prepareSearch()
            .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED_HIDDEN)
            .setQuery(QueryBuilders.idsQuery().addIds(DataCounts.documentId(jobId)))
            .get();
        if (searchResponse.getHits().getTotalHits().value != 1) {
            return new DataCounts(jobId);
        }

        BytesReference source = searchResponse.getHits().getHits()[0].getSourceRef();
        try (XContentParser parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, source, XContentType.JSON)) {
            return DataCounts.PARSER.apply(parser, null);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void waitForJobToHaveProcessedExactly(String jobId, long numDocs) throws Exception {
        assertBusy(() -> {
            DataCounts dataCounts = getDataCountsFromIndex(jobId);
            assertEquals(numDocs, dataCounts.getProcessedRecordCount());
            assertEquals(0L, dataCounts.getOutOfOrderTimeStampCount());
        }, 30, TimeUnit.SECONDS);
    }

    private void waitForJobToHaveProcessedAtLeast(String jobId, long numDocs) throws Exception {
        assertBusy(() -> {
            DataCounts dataCounts = getDataCountsFromIndex(jobId);
            assertThat(dataCounts.getProcessedRecordCount(), greaterThanOrEqualTo(numDocs));
            assertEquals(0L, dataCounts.getOutOfOrderTimeStampCount());
        }, 30, TimeUnit.SECONDS);
    }

    private void indexModelSnapshotFromCurrentJobStats(String jobId) throws IOException {
        JobStats jobStats = getJobStats(jobId);
        DataCounts dataCounts = jobStats.getDataCounts();

        ModelSnapshot modelSnapshot = new ModelSnapshot.Builder(jobId).setLatestResultTimeStamp(dataCounts.getLatestRecordTimeStamp())
            .setLatestRecordTimeStamp(dataCounts.getLatestRecordTimeStamp())
            .setMinVersion(Version.CURRENT)
            .setSnapshotId(jobId + "_mock_snapshot")
            .setTimestamp(new Date())
            .setModelSizeStats(new ModelSizeStats.Builder(jobId).build())
            .build();

        try (XContentBuilder xContentBuilder = JsonXContent.contentBuilder()) {
            modelSnapshot.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
            IndexRequest indexRequest = new IndexRequest(AnomalyDetectorsIndex.jobResultsAliasedName(jobId));
            indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            indexRequest.id(ModelSnapshot.documentId(modelSnapshot));
            indexRequest.source(xContentBuilder);
            client().index(indexRequest).actionGet();
        }

        JobUpdate jobUpdate = new JobUpdate.Builder(jobId).setModelSnapshotId(modelSnapshot.getSnapshotId()).build();
        client().execute(UpdateJobAction.INSTANCE, new UpdateJobAction.Request(jobId, jobUpdate)).actionGet();
    }
}
