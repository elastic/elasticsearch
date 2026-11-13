/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractSimpleTransportTestCase;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;

/**
 * The usage of a failed compute travels to the node that asked for it in a response header, see {@link FailedComputeUsageHeader}:
 * it arrives with an error response, whatever the exception, and the receiving side takes it off so it cannot reach the client.
 */
public class FailedComputeUsageTransportTests extends ESTestCase {
    private static final String ACTION = "internal:test/failed_compute_usage";

    static class Request extends AbstractTransportRequest {
        Request() {}

        Request(StreamInput in) throws IOException {
            super(in);
        }
    }

    private static DriverCompletionInfo usage() {
        return new DriverCompletionInfo(1, 2, 3, 4_000, 50, 6, 70, 80, List.of(), List.of(), Map.of(), false, false, Set.of());
    }

    public void testUsageArrivesFromAnotherNode() throws Exception {
        assertUsageArrives(false, false, false);
    }

    public void testUsageArrivesFromAnotherNodeWhenTheFailureIsSentFromAnotherThread() throws Exception {
        assertUsageArrives(false, true, false);
    }

    public void testUsageArrivesFromTheLocalNode() throws Exception {
        assertUsageArrives(true, false, false);
    }

    /** The coordinator handles the response on another thread, as it does on the search pool, in the context it restores. */
    public void testUsageArrivesWhenTheResponseIsHandledOnAnotherThread() throws Exception {
        assertUsageArrives(false, false, true);
        assertUsageArrives(true, false, true);
    }

    private void assertUsageArrives(boolean toLocalNode, boolean failFromAnotherThread, boolean handleResponseOnAnotherThread)
        throws Exception {
        ThreadPool threadPool = new TestThreadPool(getTestName());
        try (MockTransportService requester = newService(threadPool); MockTransportService worker = newService(threadPool)) {
            MockTransportService target = toLocalNode ? requester : worker;
            AbstractSimpleTransportTestCase.connectToNode(requester, target.getLocalNode());
            ThreadContext threadContext = threadPool.getThreadContext();
            // not an ElasticsearchException: it cannot carry metadata, which is why the usage is a header
            Exception failure = new IllegalStateException("the compute failed");

            target.registerRequestHandler(ACTION, EsExecutors.DIRECT_EXECUTOR_SERVICE, Request::new, (request, channel, task) -> {
                ActionListener<TransportResponse> channelListener = new ChannelActionListener<>(channel);
                if (failFromAnotherThread) {
                    ActionListener<TransportResponse> preserving = ContextPreservingActionListener.wrapPreservingContext(
                        channelListener,
                        threadContext
                    );
                    // as in production, the header is written on the thread that delivers the failure, just before it
                    threadPool.generic().execute(() -> {
                        FailedComputeUsageHeader.write(threadContext, usage());
                        preserving.onFailure(failure);
                    });
                } else {
                    FailedComputeUsageHeader.write(threadContext, usage());
                    channelListener.onFailure(failure);
                }
            });

            AtomicReference<DriverCompletionInfo> taken = new AtomicReference<>();
            AtomicReference<Map<String, List<String>>> headersAfterTake = new AtomicReference<>();
            PlainActionFuture<TransportResponse> future = new PlainActionFuture<>();
            ActionListener<TransportResponse> listener = ActionListener.wrap(future::onResponse, e -> {
                // where the node that asked reads what the worker did, before the failure is handled
                taken.set(FailedComputeUsageHeader.take(threadContext));
                headersAfterTake.set(threadContext.getResponseHeaders());
                future.onFailure(e);
            });
            requester.sendRequest(
                target.getLocalNode(),
                ACTION,
                new Request(),
                TransportRequestOptions.EMPTY,
                new ActionListenerResponseHandler<>(
                    listener,
                    in -> ActionResponse.Empty.INSTANCE,
                    handleResponseOnAnotherThread ? threadPool.executor(ThreadPool.Names.GENERIC) : EsExecutors.DIRECT_EXECUTOR_SERVICE
                )
            );
            expectThrows(Exception.class, () -> future.get(10, TimeUnit.SECONDS));

            assertThat(taken.get().bytesRead(), equalTo(4_000L));
            assertThat(taken.get().readNanos(), equalTo(50L));
            assertThat(taken.get().readCpuNanos(), equalTo(6L));
            assertThat(taken.get().cpuNanos(), equalTo(70L));
            assertThat(taken.get().driverNanos(), equalTo(80L));
            assertFalse(headersAfterTake.get().containsKey(FailedComputeUsageHeader.NAME));
        } finally {
            terminate(threadPool);
        }
    }

    private static MockTransportService newService(ThreadPool threadPool) {
        MockTransportService service = MockTransportService.createNewService(
            Settings.EMPTY,
            VersionInformation.CURRENT,
            TransportVersion.current(),
            threadPool
        );
        service.start();
        service.acceptIncomingRequests();
        return service;
    }
}
