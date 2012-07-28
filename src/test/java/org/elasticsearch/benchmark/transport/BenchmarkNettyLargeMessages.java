/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Bytes;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;
import org.elasticsearch.transport.netty.NettyTransport;

import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.transport.TransportRequestOptions.options;

/**
 *
 */
public class BenchmarkNettyLargeMessages {

    public static void main(String[] args) throws InterruptedException {
        final ByteSizeValue payloadSize = new ByteSizeValue(10, ByteSizeUnit.MB);
        final int NUMBER_OF_ITERATIONS = 100000;
        final int NUMBER_OF_CLIENTS = 5;
        final byte[] payload = new byte[(int) payloadSize.bytes()];

        Settings settings = ImmutableSettings.settingsBuilder()
                .build();

        final ThreadPool threadPool = new ThreadPool();
        final TransportService transportServiceServer = new TransportService(new NettyTransport(settings, threadPool), threadPool).start();
        final TransportService transportServiceClient = new TransportService(new NettyTransport(settings, threadPool), threadPool).start();

        final DiscoveryNode bigNode = new DiscoveryNode("big", new InetSocketTransportAddress("localhost", 9300));
//        final DiscoveryNode smallNode = new DiscoveryNode("small", new InetSocketTransportAddress("localhost", 9300));
        final DiscoveryNode smallNode = bigNode;

        transportServiceClient.connectToNode(bigNode);
        transportServiceClient.connectToNode(smallNode);

        transportServiceServer.registerHandler("benchmark", new BaseTransportRequestHandler<BenchmarkMessage>() {
            @Override
            public BenchmarkMessage newInstance() {
                return new BenchmarkMessage();
            }

            @Override
            public String executor() {
                return ThreadPool.Names.GENERIC;
            }

            @Override
            public void messageReceived(BenchmarkMessage request, TransportChannel channel) throws Exception {
                channel.sendResponse(request);
            }
        });

        final CountDownLatch latch = new CountDownLatch(NUMBER_OF_CLIENTS);
        for (int i = 0; i < NUMBER_OF_CLIENTS; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int i = 0; i < NUMBER_OF_ITERATIONS; i++) {
                        BenchmarkMessage message = new BenchmarkMessage(1, payload);
                        transportServiceClient.submitRequest(bigNode, "benchmark", message, options().withLowType(), new BaseTransportResponseHandler<BenchmarkMessage>() {
                            @Override
                            public BenchmarkMessage newInstance() {
                                return new BenchmarkMessage();
                            }

                            @Override
                            public String executor() {
                                return ThreadPool.Names.SAME;
                            }

                            @Override
                            public void handleResponse(BenchmarkMessage response) {
                            }

                            @Override
                            public void handleException(TransportException exp) {
                                exp.printStackTrace();
                            }
                        }).txGet();
                    }
                    latch.countDown();
                }
            }).start();
        }

        new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < 1; i++) {
                    BenchmarkMessage message = new BenchmarkMessage(2, Bytes.EMPTY_ARRAY);
                    long start = System.currentTimeMillis();
                    transportServiceClient.submitRequest(smallNode, "benchmark", message, options().withHighType(), new BaseTransportResponseHandler<BenchmarkMessage>() {
                        @Override
                        public BenchmarkMessage newInstance() {
                            return new BenchmarkMessage();
                        }

                        @Override
                        public String executor() {
                            return ThreadPool.Names.SAME;
                        }

                        @Override
                        public void handleResponse(BenchmarkMessage response) {
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            exp.printStackTrace();
                        }
                    }).txGet();
                    long took = System.currentTimeMillis() - start;
                    System.out.println("Took " + took + "ms");
                }
            }
        }).start();

        latch.await();
    }
}
