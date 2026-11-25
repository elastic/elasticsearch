/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.xpack.core.ml.MlConfigVersion;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsStatsAction;
import org.elasticsearch.xpack.core.ml.action.PutDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.StartDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.StopDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsDest;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsSource;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.OutlierDetection;
import org.elasticsearch.xpack.core.ml.utils.QueryProvider;
import org.elasticsearch.xpack.ml.action.dataframe.TransportStartDataFrameAnalyticsAction;
import org.elasticsearch.xpack.ml.dataframe.DataFrameAnalyticsTask;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class DataFrameAnalyticsFailedTaskReassignmentIT extends BaseMlIntegTestCase {

    public void testForceStopOfFailedTaskThatWasReassigned() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(3);
        ensureStableCluster();

        String sourceIndex = "dfa-source";
        indicesAdmin().prepareCreate(sourceIndex).setMapping("numeric_field", "type=double").get();
        for (int i = 0; i < 10; i++) {
            prepareIndex(sourceIndex).setSource("numeric_field", randomDouble()).get();
        }
        indicesAdmin().prepareRefresh(sourceIndex).get();

        String id = "dfa-failed-then-reassigned";
        DataFrameAnalyticsConfig config = new DataFrameAnalyticsConfig.Builder().setId(id)
            .setSource(
                new DataFrameAnalyticsSource(
                    new String[] { sourceIndex },
                    QueryProvider.fromParsedQuery(QueryBuilders.matchAllQuery()),
                    null,
                    Collections.emptyMap()
                )
            )
            .setDest(new DataFrameAnalyticsDest(id + "-dest", null))
            .setAnalysis(new OutlierDetection.Builder().build())
            .build();
        client().execute(PutDataFrameAnalyticsAction.INSTANCE, new PutDataFrameAnalyticsAction.Request(config)).actionGet();

        // The config is validated when it is created, so it can only be made to fail by removing the source index afterwards. The job then
        // fails because it cannot find its source, which gets the task into the failed state without relying on anything else.
        indicesAdmin().prepareDelete(sourceIndex).get();
        startPersistentTask(id);

        DiscoveryNode firstNode = awaitStateAndGetNode(id, DataFrameAnalyticsState.FAILED, null);
        assertThat(
            "the job must have failed because of the missing source index",
            getStats(id).getFailureReason(),
            containsString(sourceIndex)
        );

        try (var mockLog = MockLog.capture(DataFrameAnalyticsTask.class, TransportStartDataFrameAnalyticsAction.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "task adopted in failed state",
                    TransportStartDataFrameAnalyticsAction.class.getCanonicalName(),
                    Level.INFO,
                    "[" + id + "] Starting data frame analytics from state [failed]"
                )
            );
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "no error persisting progress",
                    DataFrameAnalyticsTask.class.getCanonicalName(),
                    Level.ERROR,
                    "*cannot persist progress*"
                )
            );

            // The node shuts down, so the persistent task is assigned to another node, which finds it in the failed state
            internalCluster().stopNode(firstNode.getName());
            ensureStableCluster();
            var secondNode = awaitStateAndGetNode(id, DataFrameAnalyticsState.FAILED, firstNode);
            assertThat(secondNode, not(equalTo(firstNode)));

            mockLog.awaitAllExpectationsMatched();

            // A failed task has to be force stopped, which cancels the task on the node it was reassigned to
            client().execute(StopDataFrameAnalyticsAction.INSTANCE, new StopDataFrameAnalyticsAction.Request(id).setForce(true))
                .actionGet();
            assertBusy(() -> assertThat(getStats(id).getNode(), equalTo(null)), 30, TimeUnit.SECONDS);

            mockLog.assertAllExpectationsMatched();
        }
    }

    /**
     * Does what {@code TransportStartDataFrameAnalyticsAction#masterOperation} does after validating the config, instead of calling the
     * start API. The start API estimates the memory required by running a native process, and {@link BaseMlIntegTestCase} disables
     * native processes for all its tests (the process factories are replaced with ones that return {@code null}), so that cannot be
     * used here. The nodes still run the real task executor, which is where the behavior under test is.
     */
    private void startPersistentTask(String id) {
        // The task cannot be assigned to a node until its memory requirement is known
        PlainActionFuture<Void> memoryRegistered = new PlainActionFuture<>();
        internalCluster().getInstance(MlMemoryTracker.class, internalCluster().getMasterName())
            .addDataFrameAnalyticsJobMemoryAndRefreshAllOthers(id, ByteSizeValue.ofMb(100).getBytes(), memoryRegistered);
        memoryRegistered.actionGet();

        PlainActionFuture<PersistentTasksCustomMetadata.PersistentTask<StartDataFrameAnalyticsAction.TaskParams>> started =
            new PlainActionFuture<>();
        internalCluster().getInstance(PersistentTasksService.class)
            .sendStartRequest(
                MlTasks.dataFrameAnalyticsTaskId(id),
                MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME,
                new StartDataFrameAnalyticsAction.TaskParams(id, MlConfigVersion.CURRENT, false),
                TimeValue.timeValueSeconds(30),
                started
            );
        started.actionGet();
    }

    private DiscoveryNode awaitStateAndGetNode(String id, DataFrameAnalyticsState expectedState, @Nullable DiscoveryNode notThisNode)
        throws Exception {
        DiscoveryNode[] node = new DiscoveryNode[1];
        assertBusy(() -> {
            GetDataFrameAnalyticsStatsAction.Response.Stats stats = getStats(id);
            assertThat(stats.getState(), equalTo(expectedState));
            assertThat(stats.getNode(), not(equalTo(null)));
            if (notThisNode != null) {
                assertThat(stats.getNode(), not(equalTo(notThisNode)));
            }
            node[0] = stats.getNode();
        }, 30, TimeUnit.SECONDS);
        return node[0];
    }

    private GetDataFrameAnalyticsStatsAction.Response.Stats getStats(String id) {
        return client().execute(GetDataFrameAnalyticsStatsAction.INSTANCE, new GetDataFrameAnalyticsStatsAction.Request(id))
            .actionGet()
            .getResponse()
            .results()
            .get(0);
    }
}
