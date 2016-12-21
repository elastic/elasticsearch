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
package org.elasticsearch.action.search;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class RemoteClusterConnectionTests extends ESIntegTestCase {

    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    public void testConnect() throws InterruptedException {
        ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().clear().setNodes(true).get();
        ImmutableOpenMap<String, DiscoveryNode> dataNodes = clusterStateResponse.getState().getNodes().getDataNodes();
        DiscoveryNode node = dataNodes.valuesIt().next();
        try (MockTransportService service = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool, null)) {
            service.start();
            service.acceptIncomingRequests();
            RemoteClusterConnection connection = new RemoteClusterConnection(Settings.EMPTY, "test-cluster", Arrays.asList(node), service);
            CountDownLatch latch = new CountDownLatch(1);
            AtomicReference<Exception> exceptionAtomicReference = new AtomicReference<>();
            ActionListener<Void> listener = ActionListener.wrap(x -> latch.countDown(), x -> {
                exceptionAtomicReference.set(x);
                latch.countDown();
            });
            connection.updateSeedNodes(Arrays.asList(node),listener);
            latch.await();
            assertTrue(service.nodeConnected(node));
            Iterable<DiscoveryNode> nodesIterable = dataNodes::valuesIt;
            for (DiscoveryNode dataNode : nodesIterable) {
                assertTrue(service.nodeConnected(dataNode));

            }
            assertNull(exceptionAtomicReference.get());
        }
    }

    public void testFetchShards() throws InterruptedException {

        ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().clear().setNodes(true).get();
        ImmutableOpenMap<String, DiscoveryNode> dataNodes = clusterStateResponse.getState().getNodes().getDataNodes();
        DiscoveryNode node = dataNodes.valuesIt().next();
        try (MockTransportService service = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool, null)) {
            service.start();
            service.acceptIncomingRequests();
            final boolean hasInitialNodes = randomBoolean();
            RemoteClusterConnection connection = new RemoteClusterConnection(Settings.EMPTY, "test-cluster",
                hasInitialNodes ? Arrays.asList(node) : Collections.emptyList(), service);
            CountDownLatch latch = new CountDownLatch(1);

            if (hasInitialNodes == false) {
                AtomicReference<Exception> exceptionAtomicReference = new AtomicReference<>();
                ActionListener<Void> listener = ActionListener.wrap(x -> latch.countDown(), x -> {
                    exceptionAtomicReference.set(x);
                    latch.countDown();
                });
                connection.updateSeedNodes(Arrays.asList(node), listener);
                latch.await();

                String newNode = internalCluster().startDataOnlyNode();
                createIndex("test-index");
                assertTrue(service.nodeConnected(node));
                Iterable<DiscoveryNode> nodesIterable = dataNodes::valuesIt;
                for (DiscoveryNode dataNode : nodesIterable) {
                    if (dataNode.getName().equals(newNode)) {
                        assertFalse(service.nodeConnected(dataNode));
                    } else {
                        assertTrue(service.nodeConnected(dataNode));
                    }
                }
                assertNull(exceptionAtomicReference.get());
            } else {
                createIndex("test-index");
            }

            SearchRequest request = new SearchRequest("test-index");
            CountDownLatch responseLatch = new CountDownLatch(1);
            AtomicReference<ClusterSearchShardsResponse> reference = new AtomicReference<>();
            AtomicReference<Exception> failReference = new AtomicReference<>();

            ActionListener<ClusterSearchShardsResponse> shardsListener = ActionListener.wrap(
                x -> {reference.set(x); responseLatch.countDown();},
                x -> {failReference.set(x); responseLatch.countDown();});
            connection.fetchSearchShards(request, Arrays.asList("test-index"), shardsListener);
            responseLatch.await();
            assertNull(failReference.get());
            assertNotNull(reference.get());
            ClusterSearchShardsResponse clusterSearchShardsResponse = reference.get();
            DiscoveryNode[] nodes = clusterSearchShardsResponse.getNodes();
            assertTrue(nodes.length != 0);
            for (DiscoveryNode dataNode : nodes) {
                assertTrue(service.nodeConnected(dataNode));
            }
        }
    }
}
