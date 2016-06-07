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
package org.elasticsearch.gateway;

import org.elasticsearch.Version;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

/**
 */
public class AsyncShardFetchTests extends ESTestCase {
    private final DiscoveryNode node1 = new DiscoveryNode("node1", DummyTransportAddress.INSTANCE, Collections.emptyMap(),
            Collections.singleton(DiscoveryNode.Role.DATA), Version.CURRENT);
    private final Response response1 = new Response(node1);
    private final Throwable failure1 = new Throwable("simulated failure 1");
    private final DiscoveryNode node2 = new DiscoveryNode("node2", DummyTransportAddress.INSTANCE, Collections.emptyMap(),
            Collections.singleton(DiscoveryNode.Role.DATA), Version.CURRENT);
    private final Response response2 = new Response(node2);
    private final Throwable failure2 = new Throwable("simulate failure 2");

    private ThreadPool threadPool;
    private TestFetch test;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.threadPool = new TestThreadPool(getTestName());
        this.test = new TestFetch(threadPool);
    }

    @After
    public void terminate() throws Exception {
        terminate(threadPool);
    }

    public void testClose() throws Exception {
        DiscoveryNodes nodes = DiscoveryNodes.builder().put(node1).build();
        test.addSimulation(node1.getId(), response1);

        // first fetch, no data, still on going
        AsyncShardFetch.FetchResult<Response> fetchData = test.fetchData(nodes, emptySet());
        assertThat(fetchData.hasData(), equalTo(false));
        assertThat(test.reroute.get(), equalTo(0));

        // fire a response, wait on reroute incrementing
        test.fireSimulationAndWait(node1.getId());
        // verify we get back the data node
        assertThat(test.reroute.get(), equalTo(1));
        test.close();
        try {
            test.fetchData(nodes, emptySet());
            fail("fetch data should fail when closed");
        } catch (IllegalStateException e) {
            // all is well
        }
    }

    public void testFullCircleSingleNodeSuccess() throws Exception {
        DiscoveryNodes nodes = DiscoveryNodes.builder().put(node1).build();
        test.addSimulation(node1.getId(), response1);

        // first fetch, no data, still on going
        AsyncShardFetch.FetchResult<Response> fetchData = test.fetchData(nodes, emptySet());
        assertThat(fetchData.hasData(), equalTo(false));
        assertThat(test.reroute.get(), equalTo(0));

        // fire a response, wait on reroute incrementing
        test.fireSimulationAndWait(node1.getId());
        // verify we get back the data node
        assertThat(test.reroute.get(), equalTo(1));
        fetchData = test.fetchData(nodes, emptySet());
        assertThat(fetchData.hasData(), equalTo(true));
        assertThat(fetchData.getData().size(), equalTo(1));
        assertThat(fetchData.getData().get(node1), sameInstance(response1));
    }

    public void testFullCircleSingleNodeFailure() throws Exception {
        DiscoveryNodes nodes = DiscoveryNodes.builder().put(node1).build();
        // add a failed response for node1
        test.addSimulation(node1.getId(), failure1);

        // first fetch, no data, still on going
        AsyncShardFetch.FetchResult<Response> fetchData = test.fetchData(nodes, emptySet());
        assertThat(fetchData.hasData(), equalTo(false));
        assertThat(test.reroute.get(), equalTo(0));

        // fire a response, wait on reroute incrementing
        test.fireSimulationAndWait(node1.getId());
        // failure, fetched data exists, but has no data
        assertThat(test.reroute.get(), equalTo(1));
        fetchData = test.fetchData(nodes, emptySet());
        assertThat(fetchData.hasData(), equalTo(true));
        assertThat(fetchData.getData().size(), equalTo(0));

        // on failure, we reset the failure on a successive call to fetchData, and try again afterwards
        test.addSimulation(node1.getId(), response1);
        fetchData = test.fetchData(nodes, emptySet());
        assertThat(fetchData.hasData(), equalTo(false));

        test.fireSimulationAndWait(node1.getId());
        // 2 reroutes, cause we have a failure that we clear
        assertThat(test.reroute.get(), equalTo(3));
        fetchData = test.fetchData(nodes, emptySet());
        assertThat(fetchData.hasData(), equalTo(true));
        assertThat(fetchData.getData().size(), equalTo(1));
        assertThat(fetchData.getData().get(node1), sameInstance(response1));
    }

    public void testTwoNodesOnSetup() throws Exception {
        DiscoveryNodes nodes = DiscoveryNodes.builder().put(node1).put(node2).build();
        test.addSimulation(node1.getId(), response1);
        test.addSimulation(node2.getId(), response2);

        // no fetched data, 2 requests still on going
        AsyncShardFetch.FetchResult<Response> fetchData = test.fetchData(nodes, emptySet());
        assertThat(fetchData.hasData(), equalTo(false));
        assertThat(test.reroute.get(), equalTo(0));

        // fire the first response, it should trigger a reroute
        test.fireSimulationAndWait(node1.getId());
        // there is still another on going request, so no data
        assertThat(test.getNumberOfInFlightFetches(), equalTo(1));
        fetchData = test.fetchData(nodes, emptySet());
        assertThat(fetchData.hasData(), equalTo(false));

        // fire the second simulation, this should allow us to get the data
        test.fireSimulationAndWait(node2.getId());
        // no more ongoing requests, we should fetch the data
        assertThat(test.reroute.get(), equalTo(2));
        fetchData = test.fetchData(nodes, emptySet());
        assertThat(fetchData.hasData(), equalTo(true));
        assertThat(fetchData.getData().size(), equalTo(2));
        assertThat(fetchData.getData().get(node1), sameInstance(response1));
        assertThat(fetchData.getData().get(node2), sameInstance(response2));
    }

    public void testTwoNodesOnSetupAndFailure() throws Exception {
        DiscoveryNodes nodes = DiscoveryNodes.builder().put(node1).put(node2).build();
        test.addSimulation(node1.getId(), response1);
        test.addSimulation(node2.getId(), failure2);

        // no fetched data, 2 requests still on going
        AsyncShardFetch.FetchResult<Response> fetchData = test.fetchData(nodes, emptySet());
        assertThat(fetchData.hasData(), equalTo(false));
        assertThat(test.reroute.get(), equalTo(0));

        // fire the first response, it should trigger a reroute
        test.fireSimulationAndWait(node1.getId());
        assertThat(test.reroute.get(), equalTo(1));
        fetchData = test.fetchData(nodes, emptySet());
        assertThat(fetchData.hasData(), equalTo(false));

        // fire the second simulation, this should allow us to get the data
        test.fireSimulationAndWait(node2.getId());
        assertThat(test.reroute.get(), equalTo(2));
        // since one of those failed, we should only have one entry
        fetchData = test.fetchData(nodes, emptySet());
        assertThat(fetchData.hasData(), equalTo(true));
        assertThat(fetchData.getData().size(), equalTo(1));
        assertThat(fetchData.getData().get(node1), sameInstance(response1));
    }

    public void testTwoNodesAddedInBetween() throws Exception {
        DiscoveryNodes nodes = DiscoveryNodes.builder().put(node1).build();
        test.addSimulation(node1.getId(), response1);

        // no fetched data, 2 requests still on going
        AsyncShardFetch.FetchResult<Response> fetchData = test.fetchData(nodes, emptySet());
        assertThat(fetchData.hasData(), equalTo(false));
        assertThat(test.reroute.get(), equalTo(0));

        // fire the first response, it should trigger a reroute
        test.fireSimulationAndWait(node1.getId());

        // now, add a second node to the nodes, it should add it to the ongoing requests
        nodes = DiscoveryNodes.builder(nodes).put(node2).build();
        test.addSimulation(node2.getId(), response2);
        // no fetch data, has a new node introduced
        fetchData = test.fetchData(nodes, emptySet());
        assertThat(fetchData.hasData(), equalTo(false));

        // fire the second simulation, this should allow us to get the data
        test.fireSimulationAndWait(node2.getId());

        // since one of those failed, we should only have one entry
        fetchData = test.fetchData(nodes, emptySet());
        assertThat(fetchData.hasData(), equalTo(true));
        assertThat(fetchData.getData().size(), equalTo(2));
        assertThat(fetchData.getData().get(node1), sameInstance(response1));
        assertThat(fetchData.getData().get(node2), sameInstance(response2));
    }

    static class TestFetch extends AsyncShardFetch<Response> {

        static class Entry {
            public final Response response;
            public final Throwable failure;
            private final CountDownLatch executeLatch = new CountDownLatch(1);
            private final CountDownLatch waitLatch = new CountDownLatch(1);

            public Entry(Response response, Throwable failure) {
                this.response = response;
                this.failure = failure;
            }
        }

        private final ThreadPool threadPool;
        private final Map<String, Entry> simulations = new ConcurrentHashMap<>();
        private AtomicInteger reroute = new AtomicInteger();

        public TestFetch(ThreadPool threadPool) {
            super(Loggers.getLogger(TestFetch.class), "test", new ShardId("test", "_na_", 1), null);
            this.threadPool = threadPool;
        }

        public void addSimulation(String nodeId, Response response) {
            simulations.put(nodeId, new Entry(response, null));
        }

        public void addSimulation(String nodeId, Throwable t) {
            simulations.put(nodeId, new Entry(null, t));
        }

        public void fireSimulationAndWait(String nodeId) throws InterruptedException {
            simulations.get(nodeId).executeLatch.countDown();
            simulations.get(nodeId).waitLatch.await();
            simulations.remove(nodeId);
        }

        @Override
        protected void reroute(ShardId shardId, String reason) {
            reroute.incrementAndGet();
        }

        @Override
        protected void asyncFetch(final ShardId shardId, String[] nodesIds) {
            for (final String nodeId : nodesIds) {
                threadPool.generic().execute(new Runnable() {
                    @Override
                    public void run() {
                        Entry entry = null;
                        try {
                            entry = simulations.get(nodeId);
                            if (entry == null) {
                                // we are simulating a master node switch, wait for it to not be null
                                awaitBusy(() ->  simulations.containsKey(nodeId));
                            }
                            assert entry != null;
                            entry.executeLatch.await();
                            if (entry.failure != null) {
                                processAsyncFetch(shardId, null, Collections.singletonList(new FailedNodeException(nodeId,
                                                                                                                   "unexpected",
                                                                                                                   entry.failure)));
                            } else {
                                processAsyncFetch(shardId, Collections.singletonList(entry.response), null);
                            }
                        } catch (Throwable e) {
                            logger.error("unexpected failure", e);
                        } finally {
                            if (entry != null) {
                                entry.waitLatch.countDown();
                            }
                        }
                    }
                });
            }
        }
    }


    static class Response extends BaseNodeResponse {

        public Response(DiscoveryNode node) {
            super(node);
        }
    }
}
