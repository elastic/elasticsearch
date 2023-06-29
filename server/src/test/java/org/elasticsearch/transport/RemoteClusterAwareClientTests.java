/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchShardsAction;
import org.elasticsearch.action.search.SearchShardsRequest;
import org.elasticsearch.action.search.SearchShardsResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;

public class RemoteClusterAwareClientTests extends ESTestCase {

    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    private MockTransportService startTransport(String id, List<DiscoveryNode> knownNodes) {
        return RemoteClusterConnectionTests.startTransport(
            id,
            knownNodes,
            VersionInformation.CURRENT,
            TransportVersion.current(),
            threadPool
        );
    }

    public void testSearchShards() throws Exception {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (
            MockTransportService seedTransport = startTransport("seed_node", knownNodes);
            MockTransportService discoverableTransport = startTransport("discoverable_node", knownNodes)
        ) {
            knownNodes.add(seedTransport.getLocalDiscoNode());
            knownNodes.add(discoverableTransport.getLocalDiscoNode());
            Collections.shuffle(knownNodes, random());
            Settings.Builder builder = Settings.builder();
            builder.putList("cluster.remote.cluster1.seeds", seedTransport.getLocalDiscoNode().getAddress().toString());
            try (
                MockTransportService service = MockTransportService.createNewService(
                    builder.build(),
                    VersionInformation.CURRENT,
                    TransportVersion.current(),
                    threadPool,
                    null
                )
            ) {
                service.start();
                service.acceptIncomingRequests();

                try (
                    RemoteClusterAwareClient client = new RemoteClusterAwareClient(
                        Settings.EMPTY,
                        threadPool,
                        service,
                        "cluster1",
                        randomBoolean()
                    )
                ) {
                    SearchShardsRequest searchShardsRequest = new SearchShardsRequest(
                        new String[] { "test-index" },
                        IndicesOptions.strictExpandOpen(),
                        new MatchAllQueryBuilder(),
                        null,
                        null,
                        randomBoolean(),
                        null
                    );
                    var searchShardsResponse = client.execute(SearchShardsAction.INSTANCE, searchShardsRequest).actionGet();
                    assertThat(searchShardsResponse.getNodes(), equalTo(knownNodes));
                }
            }
        }
    }

    public void testSearchShardsThreadContextHeader() {
        List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
        try (
            MockTransportService seedTransport = startTransport("seed_node", knownNodes);
            MockTransportService discoverableTransport = startTransport("discoverable_node", knownNodes)
        ) {
            knownNodes.add(seedTransport.getLocalDiscoNode());
            knownNodes.add(discoverableTransport.getLocalDiscoNode());
            Collections.shuffle(knownNodes, random());
            Settings.Builder builder = Settings.builder();
            builder.putList("cluster.remote.cluster1.seeds", seedTransport.getLocalDiscoNode().getAddress().toString());
            try (
                MockTransportService service = MockTransportService.createNewService(
                    builder.build(),
                    VersionInformation.CURRENT,
                    TransportVersion.current(),
                    threadPool,
                    null
                )
            ) {
                service.start();
                service.acceptIncomingRequests();

                try (
                    RemoteClusterAwareClient client = new RemoteClusterAwareClient(
                        Settings.EMPTY,
                        threadPool,
                        service,
                        "cluster1",
                        randomBoolean()
                    )
                ) {
                    int numThreads = 10;
                    ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
                    for (int i = 0; i < numThreads; i++) {
                        final String threadId = Integer.toString(i);
                        PlainActionFuture<SearchShardsResponse> future = new PlainActionFuture<>();
                        executorService.submit(() -> {
                            ThreadContext threadContext = seedTransport.threadPool.getThreadContext();
                            threadContext.putHeader("threadId", threadId);
                            var searchShardsRequest = new SearchShardsRequest(
                                new String[] { "test-index" },
                                IndicesOptions.strictExpandOpen(),
                                new MatchAllQueryBuilder(),
                                null,
                                null,
                                randomBoolean(),
                                null
                            );
                            client.execute(
                                SearchShardsAction.INSTANCE,
                                searchShardsRequest,
                                ActionListener.runBefore(
                                    future,
                                    () -> assertThat(seedTransport.threadPool.getThreadContext().getHeader("threadId"), equalTo(threadId))
                                )
                            );
                            assertThat(future.actionGet().getNodes(), equalTo(knownNodes));
                        });
                    }
                    ThreadPool.terminate(executorService, 5, TimeUnit.SECONDS);
                }
            }
        }
    }
}
