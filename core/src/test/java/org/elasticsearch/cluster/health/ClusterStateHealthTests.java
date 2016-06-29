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
package org.elasticsearch.cluster.health;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.TransportClusterHealthAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.RoutingTableGenerator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.gateway.NoopGatewayAllocator;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.HashSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class ClusterStateHealthTests extends ESTestCase {
    private final IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver(Settings.EMPTY);


    private static ThreadPool threadPool;

    private ClusterService clusterService;
    private TransportService transportService;

    @BeforeClass
    public static void beforeClass() {
        threadPool = new TestThreadPool("ClusterStateHealthTests");
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        clusterService = createClusterService(threadPool);
        transportService = new TransportService(clusterService.getSettings(), new CapturingTransport(), threadPool);
        transportService.start();
        transportService.acceptIncomingRequests();
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.close();
        transportService.close();
    }

    @AfterClass
    public static void afterClass() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
    }

    public void testClusterHealthWaitsForClusterStateApplication() throws InterruptedException, ExecutionException {
        final CountDownLatch applyLatch = new CountDownLatch(1);
        final CountDownLatch listenerCalled = new CountDownLatch(1);
        clusterService.add(event -> {
            listenerCalled.countDown();
            try {
                applyLatch.await();
            } catch (InterruptedException e) {
                logger.debug("interrupted", e);
            }
        });

        clusterService.submitStateUpdateTask("test", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                return ClusterState.builder(currentState).build();
            }

            @Override
            public void onFailure(String source, Throwable t) {
                logger.warn("unexpected failure", t);
            }
        });

        logger.info("--> waiting for listener to be called and cluster state being blocked");
        listenerCalled.await();

        TransportClusterHealthAction action = new TransportClusterHealthAction(Settings.EMPTY, transportService,
            clusterService, threadPool, new ActionFilters(new HashSet<>()), indexNameExpressionResolver, NoopGatewayAllocator.INSTANCE);
        PlainActionFuture<ClusterHealthResponse> listener = new PlainActionFuture<>();

        action.execute(new ClusterHealthRequest(), listener);

        assertFalse(listener.isDone());

        applyLatch.countDown();
        listener.get();
    }


    public void testClusterHealth() throws IOException {
        RoutingTableGenerator routingTableGenerator = new RoutingTableGenerator();
        RoutingTableGenerator.ShardCounter counter = new RoutingTableGenerator.ShardCounter();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        MetaData.Builder metaData = MetaData.builder();
        for (int i = randomInt(4); i >= 0; i--) {
            int numberOfShards = randomInt(3) + 1;
            int numberOfReplicas = randomInt(4);
            IndexMetaData indexMetaData = IndexMetaData
                    .builder("test_" + Integer.toString(i))
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(numberOfShards)
                    .numberOfReplicas(numberOfReplicas)
                    .build();
            IndexRoutingTable indexRoutingTable = routingTableGenerator.genIndexRoutingTable(indexMetaData, counter);
            metaData.put(indexMetaData, true);
            routingTable.add(indexRoutingTable);
        }
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).metaData(metaData).routingTable(routingTable.build()).build();
        String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(clusterState, IndicesOptions.strictExpand(), (String[]) null);
        ClusterStateHealth clusterStateHealth = new ClusterStateHealth(clusterState, concreteIndices);
        logger.info("cluster status: {}, expected {}", clusterStateHealth.getStatus(), counter.status());
        clusterStateHealth = maybeSerialize(clusterStateHealth);
        assertClusterHealth(clusterStateHealth, counter);
    }

    ClusterStateHealth maybeSerialize(ClusterStateHealth clusterStateHealth) throws IOException {
        if (randomBoolean()) {
            BytesStreamOutput out = new BytesStreamOutput();
            clusterStateHealth.writeTo(out);
            StreamInput in = StreamInput.wrap(out.bytes());
            clusterStateHealth = new ClusterStateHealth(in);
        }
        return clusterStateHealth;
    }

    private void assertClusterHealth(ClusterStateHealth clusterStateHealth, RoutingTableGenerator.ShardCounter counter) {
        assertThat(clusterStateHealth.getStatus(), equalTo(counter.status()));
        assertThat(clusterStateHealth.getActiveShards(), equalTo(counter.active));
        assertThat(clusterStateHealth.getActivePrimaryShards(), equalTo(counter.primaryActive));
        assertThat(clusterStateHealth.getInitializingShards(), equalTo(counter.initializing));
        assertThat(clusterStateHealth.getRelocatingShards(), equalTo(counter.relocating));
        assertThat(clusterStateHealth.getUnassignedShards(), equalTo(counter.unassigned));
        assertThat(clusterStateHealth.getActiveShardsPercent(), is(allOf(greaterThanOrEqualTo(0.0), lessThanOrEqualTo(100.0))));
    }
}
