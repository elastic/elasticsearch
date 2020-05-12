/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
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
            final ClusterHealthResponse health = client(node).admin().cluster().prepareHealth().setLocal(true)
                .setWaitForEvents(Priority.LANGUID).setTimeout("30s").get("10s");
            logger.info("--> got cluster health on [{}]", node);
            assertFalse("timed out on " + node, health.isTimedOut());
            assertThat("health status on " + node, health.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        }
    }

    public void testHealth() {
        logger.info("--> running cluster health on an index that does not exists");
        ClusterHealthResponse healthResponse = client().admin().cluster().prepareHealth("test1")
            .setWaitForYellowStatus().setTimeout("1s").execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(true));
        assertThat(healthResponse.getStatus(), equalTo(ClusterHealthStatus.RED));
        assertThat(healthResponse.getIndices().isEmpty(), equalTo(true));

        logger.info("--> running cluster wide health");
        healthResponse = client().admin().cluster().prepareHealth().setWaitForGreenStatus().setTimeout("10s").execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));
        assertThat(healthResponse.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(healthResponse.getIndices().isEmpty(), equalTo(true));

        logger.info("--> Creating index test1 with zero replicas");
        createIndex("test1");

        logger.info("--> running cluster health on an index that does exists");
        healthResponse = client().admin().cluster().prepareHealth("test1")
            .setWaitForGreenStatus().setTimeout("10s").execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));
        assertThat(healthResponse.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(healthResponse.getIndices().get("test1").getStatus(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("--> running cluster health on an index that does exists and an index that doesn't exists");
        healthResponse = client().admin().cluster().prepareHealth("test1", "test2")
            .setWaitForYellowStatus().setTimeout("1s").execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(true));
        assertThat(healthResponse.getStatus(), equalTo(ClusterHealthStatus.RED));
        assertThat(healthResponse.getIndices().get("test1").getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(healthResponse.getIndices().size(), equalTo(1));
    }

    public void testHealthWithClosedIndices() {
        createIndex("index-1");
        {
            ClusterHealthResponse response = client().admin().cluster().prepareHealth().setWaitForGreenStatus().get();
            assertThat(response.getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.isTimedOut(), equalTo(false));
            assertThat(response.getIndices().get("index-1").getStatus(), equalTo(ClusterHealthStatus.GREEN));
        }

        createIndex("index-2");
        assertAcked(client().admin().indices().prepareClose("index-2"));

        {
            ClusterHealthResponse response = client().admin().cluster().prepareHealth()
                .setWaitForGreenStatus()
                .get();
            assertThat(response.getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.isTimedOut(), equalTo(false));
            assertThat(response.getIndices().size(), equalTo(2));
            assertThat(response.getIndices().get("index-1").getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.getIndices().get("index-2").getStatus(), equalTo(ClusterHealthStatus.GREEN));
        }
        {
            ClusterHealthResponse response = client().admin().cluster().prepareHealth("index-1").get();
            assertThat(response.getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.isTimedOut(), equalTo(false));
            assertThat(response.getIndices().size(), equalTo(1));
            assertThat(response.getIndices().get("index-1").getStatus(), equalTo(ClusterHealthStatus.GREEN));
        }
        {
            ClusterHealthResponse response = client().admin().cluster().prepareHealth("index-2").get();
            assertThat(response.getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.isTimedOut(), equalTo(false));
            assertThat(response.getIndices().size(), equalTo(1));
            assertThat(response.getIndices().get("index-2").getStatus(), equalTo(ClusterHealthStatus.GREEN));
        }
        {
            ClusterHealthResponse response = client().admin().cluster().prepareHealth("index-*").get();
            assertThat(response.getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.isTimedOut(), equalTo(false));
            assertThat(response.getIndices().size(), equalTo(2));
            assertThat(response.getIndices().get("index-1").getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.getIndices().get("index-2").getStatus(), equalTo(ClusterHealthStatus.GREEN));
        }
        {
            ClusterHealthResponse response = client().admin().cluster().prepareHealth("index-*")
                .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                .get();
            assertThat(response.getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.isTimedOut(), equalTo(false));
            assertThat(response.getIndices().size(), equalTo(1));
            assertThat(response.getIndices().get("index-1").getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.getIndices().get("index-2"), nullValue());
        }
        {
            ClusterHealthResponse response = client().admin().cluster().prepareHealth("index-*")
                .setIndicesOptions(IndicesOptions.fromOptions(true, true, false, true))
                .get();
            assertThat(response.getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.isTimedOut(), equalTo(false));
            assertThat(response.getIndices().size(), equalTo(1));
            assertThat(response.getIndices().get("index-1"), nullValue());
            assertThat(response.getIndices().get("index-2").getStatus(), equalTo(ClusterHealthStatus.GREEN));
        }

        createIndex("index-3", Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 50)
            .build());
        assertAcked(client().admin().indices().prepareClose("index-3"));

        {
            ClusterHealthResponse response = client().admin().cluster().prepareHealth()
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
            ClusterHealthResponse response = client().admin().cluster().prepareHealth("index-1").get();
            assertThat(response.getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.isTimedOut(), equalTo(false));
            assertThat(response.getIndices().size(), equalTo(1));
            assertThat(response.getIndices().get("index-1").getStatus(), equalTo(ClusterHealthStatus.GREEN));
        }
        {
            ClusterHealthResponse response = client().admin().cluster().prepareHealth("index-2").get();
            assertThat(response.getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.isTimedOut(), equalTo(false));
            assertThat(response.getIndices().size(), equalTo(1));
            assertThat(response.getIndices().get("index-2").getStatus(), equalTo(ClusterHealthStatus.GREEN));
        }
        {
            ClusterHealthResponse response = client().admin().cluster().prepareHealth("index-3").get();
            assertThat(response.getStatus(), equalTo(ClusterHealthStatus.YELLOW));
            assertThat(response.isTimedOut(), equalTo(false));
            assertThat(response.getIndices().size(), equalTo(1));
            assertThat(response.getIndices().get("index-3").getStatus(), equalTo(ClusterHealthStatus.YELLOW));
        }
        {
            ClusterHealthResponse response = client().admin().cluster().prepareHealth("index-*").get();
            assertThat(response.getStatus(), equalTo(ClusterHealthStatus.YELLOW));
            assertThat(response.isTimedOut(), equalTo(false));
            assertThat(response.getIndices().size(), equalTo(3));
            assertThat(response.getIndices().get("index-1").getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.getIndices().get("index-2").getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.getIndices().get("index-3").getStatus(), equalTo(ClusterHealthStatus.YELLOW));
        }
        {
            ClusterHealthResponse response = client().admin().cluster().prepareHealth("index-*")
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
            ClusterHealthResponse response = client().admin().cluster().prepareHealth("index-*")
                .setIndicesOptions(IndicesOptions.fromOptions(true, true, false, true))
                .get();
            assertThat(response.getStatus(), equalTo(ClusterHealthStatus.YELLOW));
            assertThat(response.isTimedOut(), equalTo(false));
            assertThat(response.getIndices().size(), equalTo(2));
            assertThat(response.getIndices().get("index-1"), nullValue());
            assertThat(response.getIndices().get("index-2").getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.getIndices().get("index-3").getStatus(), equalTo(ClusterHealthStatus.YELLOW));
        }

        assertAcked(client().admin().indices().prepareUpdateSettings("index-3")
            .setSettings(Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numberOfReplicas())
                .build()));
        {
            ClusterHealthResponse response = client().admin().cluster().prepareHealth()
                .setWaitForGreenStatus()
                .get();
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
                    ClusterHealthResponse health = client().admin().cluster().prepareHealth().get();
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

    public void testWaitForEventsRetriesIfOtherConditionsNotMet() throws Exception {
        final ActionFuture<ClusterHealthResponse> healthResponseFuture
            = client().admin().cluster().prepareHealth("index").setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute();

        final AtomicBoolean keepSubmittingTasks = new AtomicBoolean(true);
        final ClusterService clusterService = internalCluster().getInstance(ClusterService.class, internalCluster().getMasterName());
        clusterService.submitStateUpdateTask("looping task", new ClusterStateUpdateTask(Priority.LOW) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return currentState;
                }

                @Override
                public void onFailure(String source, Exception e) {
                    throw new AssertionError(source, e);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    if (keepSubmittingTasks.get()) {
                        clusterService.submitStateUpdateTask("looping task", this);
                    }
                }
            });

        createIndex("index");
        assertFalse(client().admin().cluster().prepareHealth("index").setWaitForGreenStatus().get().isTimedOut());

        // at this point the original health response should not have returned: there was never a point where the index was green AND
        // the master had processed all pending tasks above LANGUID priority.
        assertFalse(healthResponseFuture.isDone());

        keepSubmittingTasks.set(false);
        assertFalse(healthResponseFuture.get().isTimedOut());
    }

    public void testHealthOnMasterFailover() throws Exception {
        final String node = internalCluster().startDataOnlyNode();
        final List<ActionFuture<ClusterHealthResponse>> responseFutures = new ArrayList<>();
        // Run a few health requests concurrent to master fail-overs against a data-node to make sure master failover is handled
        // without exceptions
        for (int i = 0; i < 20; ++i) {
            responseFutures.add(client(node).admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID)
                .setWaitForGreenStatus().execute());
            internalCluster().restartNode(internalCluster().getMasterName(), InternalTestCluster.EMPTY_CALLBACK);
        }
        for (ActionFuture<ClusterHealthResponse> responseFuture : responseFutures) {
            assertSame(responseFuture.get().getStatus(), ClusterHealthStatus.GREEN);
        }
    }
}
