/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class RemoteClusterAwareClientTests extends ESTestCase {

    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    private MockTransportService startTransport(String id, List<DiscoveryNode> knownNodes) {
        return RemoteClusterConnectionTests.startTransport(id, knownNodes, Version.CURRENT, threadPool);
    }

    public void testSearchShards() throws Exception {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (MockTransportService seedTransport = startTransport("seed_node", knownNodes);
             MockTransportService discoverableTransport = startTransport("discoverable_node", knownNodes)) {
            knownNodes.add(seedTransport.getLocalDiscoNode());
            knownNodes.add(discoverableTransport.getLocalDiscoNode());
            Collections.shuffle(knownNodes, random());
            Settings.Builder builder = Settings.builder();
            builder.putList("cluster.remote.cluster1.seeds", seedTransport.getLocalDiscoNode().getAddress().toString());
            try (MockTransportService service = MockTransportService.createNewService(builder.build(), Version.CURRENT, threadPool, null)) {
                service.start();
                service.acceptIncomingRequests();

                try (RemoteClusterAwareClient client = new RemoteClusterAwareClient(Settings.EMPTY, threadPool, service, "cluster1")) {
                    SearchRequest request = new SearchRequest("test-index");
                    CountDownLatch responseLatch = new CountDownLatch(1);
                    AtomicReference<ClusterSearchShardsResponse> reference = new AtomicReference<>();
                    ClusterSearchShardsRequest searchShardsRequest = new ClusterSearchShardsRequest("test-index")
                        .indicesOptions(request.indicesOptions()).local(true).preference(request.preference())
                        .routing(request.routing());
                    client.admin().cluster().searchShards(searchShardsRequest,
                        new LatchedActionListener<>(ActionListener.wrap(reference::set, e -> fail("no failures expected")), responseLatch));
                    responseLatch.await();
                    assertNotNull(reference.get());
                    ClusterSearchShardsResponse clusterSearchShardsResponse = reference.get();
                    assertEquals(knownNodes, Arrays.asList(clusterSearchShardsResponse.getNodes()));
                }
            }
        }
    }

    public void testSearchShardsThreadContextHeader() {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (MockTransportService seedTransport = startTransport("seed_node", knownNodes);
             MockTransportService discoverableTransport = startTransport("discoverable_node", knownNodes)) {
            knownNodes.add(seedTransport.getLocalDiscoNode());
            knownNodes.add(discoverableTransport.getLocalDiscoNode());
            Collections.shuffle(knownNodes, random());
            Settings.Builder builder = Settings.builder();
            builder.putList("cluster.remote.cluster1.seeds", seedTransport.getLocalDiscoNode().getAddress().toString());
            try (MockTransportService service = MockTransportService.createNewService(builder.build(), Version.CURRENT, threadPool, null)) {
                service.start();
                service.acceptIncomingRequests();

                try (RemoteClusterAwareClient client = new RemoteClusterAwareClient(Settings.EMPTY, threadPool, service, "cluster1")) {
                    SearchRequest request = new SearchRequest("test-index");
                    int numThreads = 10;
                    ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
                    for (int i = 0; i < numThreads; i++) {
                        final String threadId = Integer.toString(i);
                        executorService.submit(() -> {
                            ThreadContext threadContext = seedTransport.threadPool.getThreadContext();
                            threadContext.putHeader("threadId", threadId);
                            AtomicReference<ClusterSearchShardsResponse> reference = new AtomicReference<>();
                            final ClusterSearchShardsRequest searchShardsRequest = new ClusterSearchShardsRequest("test-index")
                                .indicesOptions(request.indicesOptions()).local(true).preference(request.preference())
                                .routing(request.routing());
                            CountDownLatch responseLatch = new CountDownLatch(1);
                            client.admin().cluster().searchShards(searchShardsRequest,
                                new LatchedActionListener<>(ActionListener.wrap(
                                    resp -> {
                                        reference.set(resp);
                                        assertEquals(threadId, seedTransport.threadPool.getThreadContext().getHeader("threadId"));
                                    },
                                    e -> fail("no failures expected")), responseLatch));
                            try {
                                responseLatch.await();
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                            assertNotNull(reference.get());
                            ClusterSearchShardsResponse clusterSearchShardsResponse = reference.get();
                            assertEquals(knownNodes, Arrays.asList(clusterSearchShardsResponse.getNodes()));
                        });
                    }
                    ThreadPool.terminate(executorService, 5, TimeUnit.SECONDS);
                }
            }
        }
    }
}
