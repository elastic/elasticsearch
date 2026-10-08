/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.persistent.UpdatePersistentTaskStatusAction;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.xpack.core.ml.action.CloseJobAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.PutDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.StopDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.test.NodeRoles.onlyRoles;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Integration tests for datafeed retry resilience during system-initiated reassignments.
 *
 * <p>Unit tests in {@link org.elasticsearch.xpack.ml.datafeed.DatafeedRunnerTests} cover retry
 * decision logic; these tests verify end-to-end behavior is unchanged for user starts and that
 * datafeeds recover after node failure (exercising the reassignment retry path).
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DatafeedRetryResilienceIT extends BaseMlIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), MockTransportService.TestPlugin.class);
    }

    public void testNormalDatafeedStartStop_smokeTest() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(1);
        ensureStableCluster(1);

        String jobId = "datafeed-retry-resilience-smoke-job";
        String datafeedId = jobId + "-datafeed";
        Job.Builder job = createJob(jobId, ByteSizeValue.ofMb(2));
        client().execute(PutJobAction.INSTANCE, new PutJobAction.Request(job)).actionGet();

        client().admin().indices().prepareCreate("data").setMapping("time", "type=date").get();
        DatafeedConfig config = createDatafeed(datafeedId, jobId, Collections.singletonList("data"));
        client().execute(PutDatafeedAction.INSTANCE, new PutDatafeedAction.Request(config)).actionGet();

        ensureYellow();
        client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(jobId)).actionGet();
        client().execute(StartDatafeedAction.INSTANCE, new StartDatafeedAction.Request(datafeedId, 0L)).actionGet();

        assertBusy(() -> assertThat(getDatafeedState(datafeedId), equalTo(DatafeedState.STARTED)), 30, TimeUnit.SECONDS);

        client().execute(StopDatafeedAction.INSTANCE, new StopDatafeedAction.Request(datafeedId)).actionGet();
        client().execute(CloseJobAction.INSTANCE, new CloseJobAction.Request(jobId)).actionGet();

        assertBusy(() -> {
            GetJobsStatsAction.Response stats = client().execute(GetJobsStatsAction.INSTANCE, new GetJobsStatsAction.Request(jobId))
                .actionGet();
            assertThat(stats.getResponse().results().get(0).getState(), equalTo(JobState.CLOSED));
        }, 30, TimeUnit.SECONDS);
    }

    /**
     * When the node running a datafeed stops, the datafeed persistent task is reassigned and should
     * return to {@link DatafeedState#STARTED} on another node (retrying transient failures during the
     * STARTED state write on reassignment).
     */
    public void testDatafeedReopensAfterNodeFailure() throws Exception {
        internalCluster().ensureAtMostNumDataNodes(0);
        internalCluster().startMasterOnlyNode();
        String mlNodeA = internalCluster().startNode(onlyRoles(Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.ML_ROLE)));
        String mlNodeB = internalCluster().startNode(onlyRoles(Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.ML_ROLE)));
        ensureStableCluster(3);

        String jobId = "datafeed-retry-resilience-failover-job";
        String datafeedId = jobId + "-datafeed";
        Job.Builder job = createJob(jobId, ByteSizeValue.ofMb(2));
        client().execute(PutJobAction.INSTANCE, new PutJobAction.Request(job)).actionGet();

        client().admin().indices().prepareCreate("data").setMapping("time", "type=date").get();
        DatafeedConfig config = createDatafeed(datafeedId, jobId, Collections.singletonList("data"));
        client().execute(PutDatafeedAction.INSTANCE, new PutDatafeedAction.Request(config)).actionGet();

        ensureYellow();
        client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(jobId)).actionGet();
        client().execute(StartDatafeedAction.INSTANCE, new StartDatafeedAction.Request(datafeedId, 0L)).actionGet();

        String origNode = awaitJobOpenedAndAssigned(jobId, null);
        assertNotNull(origNode);
        assertThat(origNode, anyOf(equalTo(mlNodeA), equalTo(mlNodeB)));
        String survivingNode = origNode.equals(mlNodeA) ? mlNodeB : mlNodeA;
        assertBusy(() -> assertThat(getDatafeedState(datafeedId), equalTo(DatafeedState.STARTED)), 30, TimeUnit.SECONDS);

        setMlIndicesDelayedNodeLeftTimeoutToZero();
        ensureGreen();

        String masterNode = internalCluster().getMasterName();
        MockTransportService masterTransport = MockTransportService.getInstance(masterNode);
        AtomicInteger injectedFailures = new AtomicInteger();
        masterTransport.addRequestHandlingBehavior(UpdatePersistentTaskStatusAction.INSTANCE.name(), (handler, request, channel, task) -> {
            if (injectedFailures.get() < 2) {
                injectedFailures.incrementAndGet();
                channel.sendResponse(new ConnectTransportException(masterTransport.getLocalNode(), "injected transient failure"));
                return;
            }
            handler.messageReceived(request, channel, task);
        });

        internalCluster().stopNode(origNode);
        ensureStableCluster(2, survivingNode);

        awaitJobOpenedAndAssigned(jobId, survivingNode);
        assertBusy(() -> {
            assertThat(getDatafeedState(datafeedId), equalTo(DatafeedState.STARTED));
            assertThat("retry path should have hit injected transport failures", injectedFailures.get(), greaterThan(1));
        }, 2, TimeUnit.MINUTES);

        masterTransport.clearAllRules();

        GetJobsStatsAction.Response jobStats = client().execute(GetJobsStatsAction.INSTANCE, new GetJobsStatsAction.Request(jobId))
            .actionGet();
        assertThat(jobStats.getResponse().results().get(0).getState(), equalTo(JobState.OPENED));
    }
}
