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
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Random;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

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
                        try {
                            startBarrier.await(10, TimeUnit.SECONDS);
                        } catch (Exception e) {
                            throw new AssertionError(e);
                        }
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
                                    public String executor() {
                                        return executor;
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

    private static class TestNode implements Releasable {

        static final String ACTION_NAME_PREFIX = "internal:test/";
        static final String[] EXECUTOR_NAMES = new String[] {
            ThreadPool.Names.SAME,
            ThreadPool.Names.GENERIC,
            ThreadPool.Names.CLUSTER_COORDINATION,
            ThreadPool.Names.SEARCH };

        final ThreadPool threadPool;
        final TransportService transportService;

        TestNode(String nodeName) {
            threadPool = new TestThreadPool(
                nodeName,
                // tiny search thread pool & queue to trigger non-shutdown-related rejections
                Settings.builder().put("thread_pool.search.size", 2).put("thread_pool.search.queue_size", 5).build()
            ) {
                @Override
                public ExecutorService executor(String name) {
                    // yielding here is enough to expose the race that #85131 fixes with reasonable probability
                    Thread.yield();
                    return super.executor(name);
                }
            };
            final var tcpTransport = MockTransportService.newMockTransport(Settings.EMPTY, TransportVersion.CURRENT, threadPool);
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
                    executor,
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
    }

}
