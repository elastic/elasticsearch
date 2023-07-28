/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import org.apache.lucene.util.Constants;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class ClusterHealthIT extends ESIntegTestCase {

    public void testSimpleLocalHealth() {
        createIndex("test");
        ensureGreen(); // master should think it's green now.

        for (final String node : internalCluster().getNodeNames()) {
            // a very high time out, which should never fire due to the local flag
            logger.info("--> getting cluster health on [{}]", node);
            final ClusterHealthResponse health = client(node).admin()
                .cluster()
                .prepareHealth()
                .setLocal(true)
                .setWaitForEvents(Priority.LANGUID)
                .setTimeout("30s")
                .get("10s");
            logger.info("--> got cluster health on [{}]", node);
            assertFalse("timed out on " + node, health.isTimedOut());
            assertThat("health status on " + node, health.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        }
    }

    public void testHealth() {
        logger.info("--> running cluster health on an index that does not exists");
        ClusterHealthResponse healthResponse = clusterAdmin().prepareHealth("test1")
            .setWaitForYellowStatus()
            .setTimeout("1s")
            .execute()
            .actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(true));
        assertThat(healthResponse.getStatus(), equalTo(ClusterHealthStatus.RED));
        assertThat(healthResponse.getIndices().isEmpty(), equalTo(true));

        logger.info("--> running cluster wide health");
        healthResponse = clusterAdmin().prepareHealth().setWaitForGreenStatus().setTimeout("10s").execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));
        assertThat(healthResponse.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(healthResponse.getIndices().isEmpty(), equalTo(true));

        logger.info("--> Creating index test1 with zero replicas");
        createIndex("test1");

        logger.info("--> running cluster health on an index that does exists");
        healthResponse = clusterAdmin().prepareHealth("test1").setWaitForGreenStatus().setTimeout("10s").execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));
        assertThat(healthResponse.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(healthResponse.getIndices().get("test1").getStatus(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("--> running cluster health on an index that does exists and an index that doesn't exists");
        healthResponse = clusterAdmin().prepareHealth("test1", "test2").setWaitForYellowStatus().setTimeout("1s").execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(true));
        assertThat(healthResponse.getStatus(), equalTo(ClusterHealthStatus.RED));
        assertThat(healthResponse.getIndices().get("test1").getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(healthResponse.getIndices().size(), equalTo(1));
    }

    public void testHealthWithClosedIndices() {
        createIndex("index-1");
        {
            ClusterHealthResponse response = clusterAdmin().prepareHealth().setWaitForGreenStatus().get();
            assertThat(response.getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.isTimedOut(), equalTo(false));
            assertThat(response.getIndices().get("index-1").getStatus(), equalTo(ClusterHealthStatus.GREEN));
        }

        createIndex("index-2");
        assertAcked(indicesAdmin().prepareClose("index-2"));

        {
            ClusterHealthResponse response = clusterAdmin().prepareHealth().setWaitForGreenStatus().get();
            assertThat(response.getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.isTimedOut(), equalTo(false));
            assertThat(response.getIndices().size(), equalTo(2));
            assertThat(response.getIndices().get("index-1").getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.getIndices().get("index-2").getStatus(), equalTo(ClusterHealthStatus.GREEN));
        }
        {
            ClusterHealthResponse response = clusterAdmin().prepareHealth("index-1").get();
            assertThat(response.getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.isTimedOut(), equalTo(false));
            assertThat(response.getIndices().size(), equalTo(1));
            assertThat(response.getIndices().get("index-1").getStatus(), equalTo(ClusterHealthStatus.GREEN));
        }
        {
            ClusterHealthResponse response = clusterAdmin().prepareHealth("index-2").get();
            assertThat(response.getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.isTimedOut(), equalTo(false));
            assertThat(response.getIndices().size(), equalTo(1));
            assertThat(response.getIndices().get("index-2").getStatus(), equalTo(ClusterHealthStatus.GREEN));
        }
        {
            ClusterHealthResponse response = clusterAdmin().prepareHealth("index-*").get();
            assertThat(response.getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.isTimedOut(), equalTo(false));
            assertThat(response.getIndices().size(), equalTo(2));
            assertThat(response.getIndices().get("index-1").getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.getIndices().get("index-2").getStatus(), equalTo(ClusterHealthStatus.GREEN));
        }
        {
            ClusterHealthResponse response = clusterAdmin().prepareHealth("index-*")
                .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                .get();
            assertThat(response.getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.isTimedOut(), equalTo(false));
            assertThat(response.getIndices().size(), equalTo(1));
            assertThat(response.getIndices().get("index-1").getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.getIndices().get("index-2"), nullValue());
        }
        {
            ClusterHealthResponse response = clusterAdmin().prepareHealth("index-*")
                .setIndicesOptions(IndicesOptions.fromOptions(true, true, false, true))
                .get();
            assertThat(response.getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.isTimedOut(), equalTo(false));
            assertThat(response.getIndices().size(), equalTo(1));
            assertThat(response.getIndices().get("index-1"), nullValue());
            assertThat(response.getIndices().get("index-2").getStatus(), equalTo(ClusterHealthStatus.GREEN));
        }

        createIndex("index-3", Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 50).build());
        assertAcked(indicesAdmin().prepareClose("index-3"));

        {
            ClusterHealthResponse response = clusterAdmin().prepareHealth()
                .setWaitForNoRelocatingShards(true)
                .setWaitForNoInitializingShards(true)
                .setWaitForYellowStatus()
                .get();
            assertThat(response.getStatus(), equalTo(ClusterHealthStatus.YELLOW));
            assertThat(response.isTimedOut(), equalTo(false));
            assertThat(response.getIndices().size(), equalTo(3));
            assertThat(response.getIndices().get("index-1").getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.getIndices().get("index-2").getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.getIndices().get("index-3").getStatus(), equalTo(ClusterHealthStatus.YELLOW));
        }
        {
            ClusterHealthResponse response = clusterAdmin().prepareHealth("index-1").get();
            assertThat(response.getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.isTimedOut(), equalTo(false));
            assertThat(response.getIndices().size(), equalTo(1));
            assertThat(response.getIndices().get("index-1").getStatus(), equalTo(ClusterHealthStatus.GREEN));
        }
        {
            ClusterHealthResponse response = clusterAdmin().prepareHealth("index-2").get();
            assertThat(response.getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.isTimedOut(), equalTo(false));
            assertThat(response.getIndices().size(), equalTo(1));
            assertThat(response.getIndices().get("index-2").getStatus(), equalTo(ClusterHealthStatus.GREEN));
        }
        {
            ClusterHealthResponse response = clusterAdmin().prepareHealth("index-3").get();
            assertThat(response.getStatus(), equalTo(ClusterHealthStatus.YELLOW));
            assertThat(response.isTimedOut(), equalTo(false));
            assertThat(response.getIndices().size(), equalTo(1));
            assertThat(response.getIndices().get("index-3").getStatus(), equalTo(ClusterHealthStatus.YELLOW));
        }
        {
            ClusterHealthResponse response = clusterAdmin().prepareHealth("index-*").get();
            assertThat(response.getStatus(), equalTo(ClusterHealthStatus.YELLOW));
            assertThat(response.isTimedOut(), equalTo(false));
            assertThat(response.getIndices().size(), equalTo(3));
            assertThat(response.getIndices().get("index-1").getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.getIndices().get("index-2").getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.getIndices().get("index-3").getStatus(), equalTo(ClusterHealthStatus.YELLOW));
        }
        {
            ClusterHealthResponse response = clusterAdmin().prepareHealth("index-*")
                .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                .get();
            assertThat(response.getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.isTimedOut(), equalTo(false));
            assertThat(response.getIndices().size(), equalTo(1));
            assertThat(response.getIndices().get("index-1").getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.getIndices().get("index-2"), nullValue());
            assertThat(response.getIndices().get("index-3"), nullValue());
        }
        {
            ClusterHealthResponse response = clusterAdmin().prepareHealth("index-*")
                .setIndicesOptions(IndicesOptions.fromOptions(true, true, false, true))
                .get();
            assertThat(response.getStatus(), equalTo(ClusterHealthStatus.YELLOW));
            assertThat(response.isTimedOut(), equalTo(false));
            assertThat(response.getIndices().size(), equalTo(2));
            assertThat(response.getIndices().get("index-1"), nullValue());
            assertThat(response.getIndices().get("index-2").getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.getIndices().get("index-3").getStatus(), equalTo(ClusterHealthStatus.YELLOW));
        }

        setReplicaCount(numberOfReplicas(), "index-3");
        {
            ClusterHealthResponse response = clusterAdmin().prepareHealth().setWaitForGreenStatus().get();
            assertThat(response.getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.isTimedOut(), equalTo(false));
            assertThat(response.getIndices().size(), equalTo(3));
            assertThat(response.getIndices().get("index-1").getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.getIndices().get("index-2").getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.getIndices().get("index-3").getStatus(), equalTo(ClusterHealthStatus.GREEN));
        }
    }

    public void testHealthOnIndexCreation() throws Exception {
        final AtomicBoolean finished = new AtomicBoolean(false);
        Thread clusterHealthThread = new Thread() {
            @Override
            public void run() {
                while (finished.get() == false) {
                    ClusterHealthResponse health = clusterAdmin().prepareHealth().get();
                    assertThat(health.getStatus(), not(equalTo(ClusterHealthStatus.RED)));
                }
            }
        };
        clusterHealthThread.start();
        for (int i = 0; i < 10; i++) {
            createIndex("test" + i);
        }
        finished.set(true);
        clusterHealthThread.join();
    }

    public void testWaitForEventsRetriesIfOtherConditionsNotMet() {
        final ActionFuture<ClusterHealthResponse> healthResponseFuture = clusterAdmin().prepareHealth("index")
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .execute();

        final AtomicBoolean keepSubmittingTasks = new AtomicBoolean(true);
        final ClusterService clusterService = internalCluster().getInstance(ClusterService.class, internalCluster().getMasterName());
        final PlainActionFuture<Void> completionFuture = new PlainActionFuture<>();
        clusterService.submitUnbatchedStateUpdateTask("looping task", new ClusterStateUpdateTask(Priority.LOW) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return currentState;
            }

            @Override
            public void onFailure(Exception e) {
                completionFuture.onFailure(e);
                throw new AssertionError("looping task", e);
            }

            @Override
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                if (keepSubmittingTasks.get()) {
                    clusterService.submitUnbatchedStateUpdateTask("looping task", this);
                } else {
                    completionFuture.onResponse(null);
                }
            }
        });

        try {
            createIndex("index");
            assertFalse(clusterAdmin().prepareHealth("index").setWaitForGreenStatus().get().isTimedOut());

            // at this point the original health response should not have returned: there was never a point where the index was green AND
            // the master had processed all pending tasks above LANGUID priority.
            assertFalse(healthResponseFuture.isDone());
            keepSubmittingTasks.set(false);
            assertFalse(healthResponseFuture.actionGet(TimeValue.timeValueSeconds(30)).isTimedOut());
        } finally {
            keepSubmittingTasks.set(false);
            completionFuture.actionGet(TimeValue.timeValueSeconds(30));
        }
    }

    public void testHealthOnMasterFailover() throws Exception {
        final String node = internalCluster().startDataOnlyNode();
        final boolean withIndex = randomBoolean();
        if (withIndex) {
            // Create index with many shards to provoke the health request to wait (for green) while master is being shut down.
            // Notice that this is set to 0 after the test completed starting a number of health requests and master restarts.
            // This ensures that the cluster is yellow when the health request is made, making the health request wait on the observer,
            // triggering a call to observer.onClusterServiceClose when master is shutdown.
            createIndex(
                "test",
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, randomIntBetween(0, 10))
                    // avoid full recoveries of index, just wait for replica to reappear
                    .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "5m")
                    .build()
            );
        }
        final List<ActionFuture<ClusterHealthResponse>> responseFutures = new ArrayList<>();
        // Run a few health requests concurrent to master fail-overs against a data-node to make sure master failover is handled
        // without exceptions
        final int iterations = withIndex ? 10 : 20;
        // CI darwin workers are sometimes very slow, give them extra time.
        int timeoutMinutes = Constants.MAC_OS_X ? 2 : 1;
        for (int i = 0; i < iterations; ++i) {
            responseFutures.add(
                client(node).admin()
                    .cluster()
                    .prepareHealth()
                    .setWaitForEvents(Priority.LANGUID)
                    .setWaitForGreenStatus()
                    .setMasterNodeTimeout(TimeValue.timeValueMinutes(timeoutMinutes))
                    .execute()
            );
            internalCluster().restartNode(internalCluster().getMasterName(), InternalTestCluster.EMPTY_CALLBACK);
        }
        if (withIndex) {
            setReplicaCount(0, "test");
        }
        for (ActionFuture<ClusterHealthResponse> responseFuture : responseFutures) {
            assertSame(responseFuture.get().getStatus(), ClusterHealthStatus.GREEN);
        }
    }

    public void testWaitForEventsTimesOutIfMasterBusy() {
        final AtomicBoolean keepSubmittingTasks = new AtomicBoolean(true);
        final ClusterService clusterService = internalCluster().getInstance(ClusterService.class, internalCluster().getMasterName());
        final PlainActionFuture<Void> completionFuture = new PlainActionFuture<>();
        clusterService.submitUnbatchedStateUpdateTask("looping task", new ClusterStateUpdateTask(Priority.LOW) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return currentState;
            }

            @Override
            public void onFailure(Exception e) {
                completionFuture.onFailure(e);
                throw new AssertionError("looping task", e);
            }

            @Override
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                if (keepSubmittingTasks.get()) {
                    clusterService.submitUnbatchedStateUpdateTask("looping task", this);
                } else {
                    completionFuture.onResponse(null);
                }
            }
        });

        try {
            final ClusterHealthResponse clusterHealthResponse = clusterAdmin().prepareHealth()
                .setWaitForEvents(Priority.LANGUID)
                .setTimeout(TimeValue.timeValueSeconds(1))
                .get(TimeValue.timeValueSeconds(30));
            assertTrue(clusterHealthResponse.isTimedOut());
        } finally {
            keepSubmittingTasks.set(false);
            completionFuture.actionGet(TimeValue.timeValueSeconds(30));
        }
    }
}
