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
import org.elasticsearch.action.search.SearchShardsRequest;
import org.elasticsearch.action.search.SearchShardsResponse;
import org.elasticsearch.action.search.TransportSearchShardsAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancellationService;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.test.tasks.MockTaskManager.SPY_TASK_MANAGER_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

public class RemoteClusterAwareClientTests extends ESTestCase {

    private static final String TEST_THREAD_POOL_NAME = "test_thread_pool";

    private final ThreadPool threadPool = new TestThreadPool(
        getClass().getName(),
        new ScalingExecutorBuilder(TEST_THREAD_POOL_NAME, 1, 1, TimeValue.timeValueSeconds(60), true)
    );

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

    public void testRemoteTaskCancellationOnFailedResponse() throws Exception {
        Settings.Builder remoteTransportSettingsBuilder = Settings.builder();
        remoteTransportSettingsBuilder.put(SPY_TASK_MANAGER_SETTING.getKey(), true);
        try (
            MockTransportService remoteTransport = RemoteClusterConnectionTests.startTransport(
                "seed_node",
                new CopyOnWriteArrayList<>(),
                VersionInformation.CURRENT,
                TransportVersion.current(),
                threadPool,
                remoteTransportSettingsBuilder.build()
            )
        ) {
            remoteTransport.getTaskManager().setTaskCancellationService(new TaskCancellationService(remoteTransport));
            Settings.Builder builder = Settings.builder();
            builder.putList("cluster.remote.cluster1.seeds", remoteTransport.getLocalDiscoNode().getAddress().toString());
            try (
                MockTransportService localService = MockTransportService.createNewService(
                    builder.build(),
                    VersionInformation.CURRENT,
                    TransportVersion.current(),
                    threadPool,
                    null
                )
            ) {
                // the TaskCancellationService references the same TransportService instance
                // this is identically to how it works in the Node constructor
                localService.getTaskManager().setTaskCancellationService(new TaskCancellationService(localService));
                localService.start();
                localService.acceptIncomingRequests();

                SearchShardsRequest searchShardsRequest = new SearchShardsRequest(
                    new String[] { "test-index" },
                    IndicesOptions.strictExpandOpen(),
                    new MatchAllQueryBuilder(),
                    null,
                    "index_not_found", // this request must fail
                    randomBoolean(),
                    null
                );
                Task parentTask = localService.getTaskManager().register("test_type", "test_action", searchShardsRequest);
                TaskId parentTaskId = new TaskId("test-mock-node-id", parentTask.getId());
                searchShardsRequest.setParentTask(parentTaskId);
                var client = new RemoteClusterAwareClient(
                    localService,
                    "cluster1",
                    threadPool.executor(TEST_THREAD_POOL_NAME),
                    randomBoolean()
                );

                CountDownLatch cancelChildReceived = new CountDownLatch(1);
                remoteTransport.addRequestHandlingBehavior(
                    TaskCancellationService.CANCEL_CHILD_ACTION_NAME,
                    (handler, request, channel, task) -> {
                        handler.messageReceived(request, channel, task);
                        cancelChildReceived.countDown();
                    }
                );
                AtomicLong searchShardsRequestId = new AtomicLong(-1);
                CountDownLatch cancelChildSent = new CountDownLatch(1);
                localService.addSendBehavior(remoteTransport, (connection, requestId, action, request, options) -> {
                    connection.sendRequest(requestId, action, request, options);
                    if (action.equals("indices:admin/search/search_shards")) {
                        searchShardsRequestId.set(requestId);
                    } else if (action.equals(TaskCancellationService.CANCEL_CHILD_ACTION_NAME)) {
                        cancelChildSent.countDown();
                    }
                });

                // assert original request failed
                var future = new PlainActionFuture<SearchShardsResponse>();
                client.execute(TransportSearchShardsAction.REMOTE_TYPE, searchShardsRequest, future);
                ExecutionException e = expectThrows(ExecutionException.class, future::get);
                assertThat(e.getCause(), instanceOf(RemoteTransportException.class));

                // assert remote task is cancelled
                safeAwait(cancelChildSent);
                safeAwait(cancelChildReceived);
                verify(remoteTransport.getTaskManager()).cancelChildLocal(eq(parentTaskId), eq(searchShardsRequestId.get()), anyString());
            }
        }
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

                final var client = new RemoteClusterAwareClient(
                    service,
                    "cluster1",
                    threadPool.executor(TEST_THREAD_POOL_NAME),
                    randomBoolean()
                );
                SearchShardsRequest searchShardsRequest = new SearchShardsRequest(
                    new String[] { "test-index" },
                    IndicesOptions.strictExpandOpen(),
                    new MatchAllQueryBuilder(),
                    null,
                    null,
                    randomBoolean(),
                    null
                );
                final SearchShardsResponse searchShardsResponse = safeAwait(
                    listener -> client.execute(
                        TransportSearchShardsAction.REMOTE_TYPE,
                        searchShardsRequest,
                        ActionListener.runBefore(
                            listener,
                            () -> assertTrue(Thread.currentThread().getName().contains('[' + TEST_THREAD_POOL_NAME + ']'))
                        )
                    )
                );
                assertThat(searchShardsResponse.getNodes(), equalTo(knownNodes));
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

                final var client = new RemoteClusterAwareClient(service, "cluster1", EsExecutors.DIRECT_EXECUTOR_SERVICE, randomBoolean());

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
                            TransportSearchShardsAction.REMOTE_TYPE,
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
