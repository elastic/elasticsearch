/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;

public class TransportServiceLifecycleTests extends ESTestCase {

    public void testHandlersCompleteAtShutdown() throws Exception {
        try (var nodeA = new TestNode("node-A")) {

            final var threads = new Thread[between(1, 10)];
            final var keepGoing = new AtomicBoolean(true);
            final var requestPermits = new Semaphore(Integer.MAX_VALUE);

            try (var nodeB = new TestNode("node-B")) {

                final var connectFuture = new PlainActionFuture<Releasable>();
                nodeB.transportService.connectToNode(nodeA.transportService.getLocalNode(), connectFuture);
                connectFuture.get(10, TimeUnit.SECONDS);

                final var startBarrier = new CyclicBarrier(threads.length + 1);

                for (int i = 0; i < threads.length; i++) {
                    final var seed = randomLong();
                    threads[i] = new Thread(() -> {
                        safeAwait(startBarrier);
                        final var random = new Random(seed);
                        while (keepGoing.get() && requestPermits.tryAcquire()) {
                            nodeB.transportService.sendRequest(
                                randomFrom(random, nodeA, nodeB).transportService.getLocalNode(),
                                TestNode.ACTION_NAME_PREFIX + randomFrom(random, TestNode.EXECUTOR_NAMES),
                                TransportRequest.Empty.INSTANCE,
                                new TransportResponseHandler<TransportResponse.Empty>() {

                                    final AtomicBoolean completed = new AtomicBoolean();

                                    final String executor = randomFrom(random, TestNode.EXECUTOR_NAMES);

                                    @Override
                                    public void handleResponse(TransportResponse.Empty response) {
                                        assertTrue(completed.compareAndSet(false, true));
                                        requestPermits.release();
                                    }

                                    @Override
                                    public void handleException(TransportException exp) {
                                        assertTrue(completed.compareAndSet(false, true));
                                        requestPermits.release();
                                    }

                                    @Override
                                    public TransportResponse.Empty read(StreamInput in) {
                                        return TransportResponse.Empty.INSTANCE;
                                    }

                                    @Override
                                    public Executor executor() {
                                        return nodeB.transportService.getThreadPool().executor(executor);
                                    }
                                }
                            );
                        }
                    }, "test-thread-" + i);
                    threads[i].start();
                }

                startBarrier.await(10, TimeUnit.SECONDS);

            }

            // exiting the try-with-resources block stops node B while the background threads continue to submit requests, to verify that
            // every handler is completed even if the request or response are being handled concurrently with shutdown

            keepGoing.set(false);
            assertTrue(requestPermits.tryAcquire(Integer.MAX_VALUE, 10, TimeUnit.SECONDS));
            for (final var thread : threads) {
                thread.join();
            }
        }
    }

    public void testInternalSendExceptionForksToHandlerExecutor() {
        final var deterministicTaskQueue = new DeterministicTaskQueue();

        try (var nodeA = new TestNode("node-A")) {
            final var future = new PlainActionFuture<TransportResponse.Empty>();
            nodeA.transportService.sendRequest(
                nodeA.getThrowingConnection(),
                TestNode.ACTION_NAME_PREFIX + randomFrom(TestNode.EXECUTOR_NAMES),
                new TransportRequest.Empty(),
                TransportRequestOptions.EMPTY,
                new ActionListenerResponseHandler<>(future, unusedReader(), deterministicTaskQueue::scheduleNow)
            );

            assertFalse(future.isDone());
            assertTrue(deterministicTaskQueue.hasRunnableTasks());
            deterministicTaskQueue.runAllRunnableTasks();
            assertTrue(future.isDone());
            assertEquals("simulated exception in sendRequest", getSendRequestException(future, IOException.class).getMessage());
        }
    }

    public void testInternalSendExceptionForksToGenericIfHandlerDoesNotFork() {
        try (var nodeA = new TestNode("node-A")) {
            final var future = new PlainActionFuture<TransportResponse.Empty>();
            nodeA.transportService.sendRequest(
                nodeA.getThrowingConnection(),
                TestNode.ACTION_NAME_PREFIX + randomFrom(TestNode.EXECUTOR_NAMES),
                new TransportRequest.Empty(),
                TransportRequestOptions.EMPTY,
                new ActionListenerResponseHandler<>(future.delegateResponse((l, e) -> {
                    assertThat(Thread.currentThread().getName(), containsString("[" + ThreadPool.Names.GENERIC + "]"));
                    l.onFailure(e);
                }), unusedReader(), EsExecutors.DIRECT_EXECUTOR_SERVICE)
            );

            assertEquals("simulated exception in sendRequest", getSendRequestException(future, IOException.class).getMessage());
        }
    }

    public void testInternalSendExceptionForcesExecutionOnHandlerExecutor() {
        try (var nodeA = new TestNode("node-A")) {
            final var blockingLatch = new CountDownLatch(1);
            final var executor = nodeA.threadPool.executor(Executors.FIXED_BOUNDED_QUEUE);
            while (true) {
                try {
                    executor.execute(() -> safeAwait(blockingLatch));
                } catch (EsRejectedExecutionException e) {
                    break;
                }
            }

            final var future = new PlainActionFuture<TransportResponse.Empty>();
            try {
                nodeA.transportService.sendRequest(
                    nodeA.getThrowingConnection(),
                    TestNode.ACTION_NAME_PREFIX + randomFrom(TestNode.EXECUTOR_NAMES),
                    new TransportRequest.Empty(),
                    TransportRequestOptions.EMPTY,
                    new ActionListenerResponseHandler<>(future.delegateResponse((l, e) -> {
                        assertThat(Thread.currentThread().getName(), containsString("[" + Executors.FIXED_BOUNDED_QUEUE + "]"));
                        l.onFailure(e);
                    }), unusedReader(), executor)
                );

                assertFalse(future.isDone());
            } finally {
                blockingLatch.countDown();
            }
            assertEquals("simulated exception in sendRequest", getSendRequestException(future, IOException.class).getMessage());
        }
    }

    public void testInternalSendExceptionCompletesHandlerOnCallingThreadIfTransportServiceClosed() {
        final var nodeA = new TestNode("node-A");
        final var executor = nodeA.threadPool.executor(randomFrom(TestNode.EXECUTOR_NAMES));
        nodeA.close();

        final var testThread = Thread.currentThread();
        final var future = new PlainActionFuture<TransportResponse.Empty>();
        nodeA.transportService.sendRequest(
            nodeA.getThrowingConnection(),
            TestNode.ACTION_NAME_PREFIX + randomFrom(TestNode.EXECUTOR_NAMES),
            new TransportRequest.Empty(),
            TransportRequestOptions.EMPTY,
            new ActionListenerResponseHandler<>(future.delegateResponse((l, e) -> {
                assertSame(testThread, Thread.currentThread());
                l.onFailure(e);
            }), unusedReader(), executor)
        );

        assertTrue(future.isDone());
        assertThat(getSendRequestException(future, NodeClosedException.class).getMessage(), startsWith("node closed"));
    }

    private static <T> Writeable.Reader<T> unusedReader() {
        return in -> fail(null, "should not be used");
    }

    private static <E extends Exception> E getSendRequestException(Future<?> future, Class<E> exceptionClass) {
        return asInstanceOf(
            exceptionClass,
            expectThrows(ExecutionException.class, SendRequestTransportException.class, () -> future.get(10, TimeUnit.SECONDS)).getCause()
        );
    }

    private static class Executors {
        static final String SCALING_DROP_ON_SHUTDOWN = "scaling-drop-on-shutdown";
        static final String SCALING_REJECT_ON_SHUTDOWN = "scaling-reject-on-shutdown";
        static final String FIXED_BOUNDED_QUEUE = "fixed-bounded-queue";
        static final String FIXED_UNBOUNDED_QUEUE = "fixed-unbounded-queue";
    }

    private static class TestNode implements Releasable {

        static final String ACTION_NAME_PREFIX = "internal:test/";
        static final String[] EXECUTOR_NAMES = new String[] {
            ThreadPool.Names.SAME,
            Executors.SCALING_DROP_ON_SHUTDOWN,
            Executors.SCALING_REJECT_ON_SHUTDOWN,
            Executors.FIXED_BOUNDED_QUEUE,
            Executors.FIXED_UNBOUNDED_QUEUE };

        final ThreadPool threadPool;
        final TransportService transportService;

        TestNode(String nodeName) {
            threadPool = new TestThreadPool(
                nodeName,
                new ScalingExecutorBuilder(Executors.SCALING_DROP_ON_SHUTDOWN, 3, 3, TimeValue.timeValueSeconds(60), false),
                new ScalingExecutorBuilder(Executors.SCALING_REJECT_ON_SHUTDOWN, 3, 3, TimeValue.timeValueSeconds(60), true),
                new FixedExecutorBuilder(
                    Settings.EMPTY,
                    Executors.FIXED_BOUNDED_QUEUE,
                    2,
                    5,
                    Executors.FIXED_BOUNDED_QUEUE,
                    randomFrom(EsExecutors.TaskTrackingConfig.DO_NOT_TRACK, EsExecutors.TaskTrackingConfig.DEFAULT)
                ),
                new FixedExecutorBuilder(
                    Settings.EMPTY,
                    Executors.FIXED_UNBOUNDED_QUEUE,
                    2,
                    -1,
                    Executors.FIXED_UNBOUNDED_QUEUE,
                    randomFrom(EsExecutors.TaskTrackingConfig.DO_NOT_TRACK, EsExecutors.TaskTrackingConfig.DEFAULT)
                )
            ) {
                @Override
                public ExecutorService executor(String name) {
                    // yielding here is enough to expose the race that #85131 fixes with reasonable probability
                    Thread.yield();
                    return super.executor(name);
                }
            };
            final var tcpTransport = MockTransportService.newMockTransport(Settings.EMPTY, TransportVersion.current(), threadPool);
            transportService = new TransportService(
                Settings.EMPTY,
                tcpTransport,
                threadPool,
                TransportService.NOOP_TRANSPORT_INTERCEPTOR,
                boundTransportAddress -> DiscoveryNodeUtils.create(
                    nodeName,
                    nodeName,
                    tcpTransport.boundAddress().publishAddress(),
                    emptyMap(),
                    emptySet()
                ),
                null,
                emptySet()
            );
            for (final var executor : EXECUTOR_NAMES) {
                transportService.registerRequestHandler(
                    ACTION_NAME_PREFIX + executor,
                    threadPool.executor(executor),
                    TransportRequest.Empty::new,
                    (request, channel, task) -> {
                        if (randomBoolean()) {
                            channel.sendResponse(TransportResponse.Empty.INSTANCE);
                        } else {
                            channel.sendResponse(new ElasticsearchException("simulated"));
                        }
                    }
                );
            }
            transportService.start();
            transportService.acceptIncomingRequests();
        }

        @Override
        public void close() {
            transportService.stop();
            transportService.close();
            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        }

        Transport.Connection getThrowingConnection() {
            return new CloseableConnection() {
                @Override
                public DiscoveryNode getNode() {
                    return transportService.getLocalNode();
                }

                @Override
                public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
                    throws IOException, TransportException {
                    throw new IOException("simulated exception in sendRequest");
                }

                @Override
                public TransportVersion getTransportVersion() {
                    return TransportVersion.current();
                }
            };
        }
    }

}
