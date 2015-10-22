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

package org.elasticsearch.benchmark.transport;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;
import org.elasticsearch.transport.local.LocalTransport;
import org.elasticsearch.transport.netty.NettyTransport;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class TransportBenchmark {

    static enum Type {
        LOCAL {
            @Override
            public Transport newTransport(Settings settings, ThreadPool threadPool) {
                return new LocalTransport(settings, threadPool, Version.CURRENT, new NamedWriteableRegistry());
            }
        },
        NETTY {
            @Override
            public Transport newTransport(Settings settings, ThreadPool threadPool) {
                return new NettyTransport(settings, threadPool, new NetworkService(Settings.EMPTY), BigArrays.NON_RECYCLING_INSTANCE, Version.CURRENT, new NamedWriteableRegistry());
            }
        };

        public abstract Transport newTransport(Settings settings, ThreadPool threadPool);
    }

    public static void main(String[] args) {
        final String executor = ThreadPool.Names.GENERIC;
        final boolean waitForRequest = true;
        final ByteSizeValue payloadSize = new ByteSizeValue(100, ByteSizeUnit.BYTES);
        final int NUMBER_OF_CLIENTS = 10;
        final int NUMBER_OF_ITERATIONS = 100000;
        final byte[] payload = new byte[(int) payloadSize.bytes()];
        final AtomicLong idGenerator = new AtomicLong();
        final Type type = Type.NETTY;


        Settings settings = Settings.settingsBuilder()
                .build();

        final ThreadPool serverThreadPool = new ThreadPool("server");
        final TransportService serverTransportService = new TransportService(type.newTransport(settings, serverThreadPool), serverThreadPool).start();

        final ThreadPool clientThreadPool = new ThreadPool("client");
        final TransportService clientTransportService = new TransportService(type.newTransport(settings, clientThreadPool), clientThreadPool).start();

        final DiscoveryNode node = new DiscoveryNode("server", serverTransportService.boundAddress().publishAddress(), Version.CURRENT);

        serverTransportService.registerRequestHandler("benchmark", BenchmarkMessageRequest::new, executor, new TransportRequestHandler<BenchmarkMessageRequest>() {
            @Override
            public void messageReceived(BenchmarkMessageRequest request, TransportChannel channel) throws Exception {
                channel.sendResponse(new BenchmarkMessageResponse(request));
            }
        });

        clientTransportService.connectToNode(node);

        for (int i = 0; i < 10000; i++) {
            BenchmarkMessageRequest message = new BenchmarkMessageRequest(1, payload);
            clientTransportService.submitRequest(node, "benchmark", message, new BaseTransportResponseHandler<BenchmarkMessageResponse>() {
                @Override
                public BenchmarkMessageResponse newInstance() {
                    return new BenchmarkMessageResponse();
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }

                @Override
                public void handleResponse(BenchmarkMessageResponse response) {
                }

                @Override
                public void handleException(TransportException exp) {
                    exp.printStackTrace();
                }
            }).txGet();
        }


        Thread[] clients = new Thread[NUMBER_OF_CLIENTS];
        final CountDownLatch latch = new CountDownLatch(NUMBER_OF_CLIENTS * NUMBER_OF_ITERATIONS);
        for (int i = 0; i < NUMBER_OF_CLIENTS; i++) {
            clients[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int j = 0; j < NUMBER_OF_ITERATIONS; j++) {
                        final long id = idGenerator.incrementAndGet();
                        BenchmarkMessageRequest request = new BenchmarkMessageRequest(id, payload);
                        BaseTransportResponseHandler<BenchmarkMessageResponse> handler = new BaseTransportResponseHandler<BenchmarkMessageResponse>() {
                            @Override
                            public BenchmarkMessageResponse newInstance() {
                                return new BenchmarkMessageResponse();
                            }

                            @Override
                            public String executor() {
                                return executor;
                            }

                            @Override
                            public void handleResponse(BenchmarkMessageResponse response) {
                                if (response.id() != id) {
                                    System.out.println("NO ID MATCH [" + response.id() + "] and [" + id + "]");
                                }
                                latch.countDown();
                            }

                            @Override
                            public void handleException(TransportException exp) {
                                exp.printStackTrace();
                                latch.countDown();
                            }
                        };

                        if (waitForRequest) {
                            clientTransportService.submitRequest(node, "benchmark", request, handler).txGet();
                        } else {
                            clientTransportService.sendRequest(node, "benchmark", request, handler);
                        }
                    }
                }
            });
        }

        StopWatch stopWatch = new StopWatch().start();
        for (int i = 0; i < NUMBER_OF_CLIENTS; i++) {
            clients[i].start();
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        stopWatch.stop();

        System.out.println("Ran [" + NUMBER_OF_CLIENTS + "], each with [" + NUMBER_OF_ITERATIONS + "] iterations, payload [" + payloadSize + "]: took [" + stopWatch.totalTime() + "], TPS: " + (NUMBER_OF_CLIENTS * NUMBER_OF_ITERATIONS) / stopWatch.totalTime().secondsFrac());

        clientTransportService.close();
        clientThreadPool.shutdownNow();

        serverTransportService.close();
        serverThreadPool.shutdownNow();
    }
}