/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.transport.netty.benchmark;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.cached.CachedThreadPool;
import org.elasticsearch.timer.TimerService;
import org.elasticsearch.transport.BaseTransportResponseHandler;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.netty.NettyTransport;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author kimchy (shay.banon)
 */
public class BenchmarkNettyClient {


    public static void main(String[] args) {
        final boolean waitForRequest = true;
        final boolean spawn = true;
        final ByteSizeValue payloadSize = new ByteSizeValue(100, ByteSizeUnit.BYTES);
        final int NUMBER_OF_CLIENTS = 1;
        final int NUMBER_OF_ITERATIONS = 100000;
        final byte[] payload = new byte[(int) payloadSize.bytes()];
        final AtomicLong idGenerator = new AtomicLong();

        Settings settings = ImmutableSettings.settingsBuilder()
                .put("network.server", false)
                .put("network.tcp.blocking", false)
                .build();

        final ThreadPool threadPool = new CachedThreadPool(settings);
//        final ThreadPool threadPool = new ScalingThreadPool(settings);
        final TimerService timerService = new TimerService(settings, threadPool);
        final TransportService transportService = new TransportService(new NettyTransport(settings, threadPool), threadPool, timerService).start();

        final DiscoveryNode node = new DiscoveryNode("server", new InetSocketTransportAddress("localhost", 9999));

        transportService.connectToNode(node);

        for (int i = 0; i < 10000; i++) {
            BenchmarkMessage message = new BenchmarkMessage(1, payload);
            transportService.submitRequest(node, "benchmark", message, new BaseTransportResponseHandler<BenchmarkMessage>() {
                @Override public BenchmarkMessage newInstance() {
                    return new BenchmarkMessage();
                }

                @Override public void handleResponse(BenchmarkMessage response) {
                }

                @Override public void handleException(TransportException exp) {
                    exp.printStackTrace();
                }
            }).txGet();
        }


        Thread[] clients = new Thread[NUMBER_OF_CLIENTS];
        final CountDownLatch latch = new CountDownLatch(NUMBER_OF_CLIENTS * NUMBER_OF_ITERATIONS);
        for (int i = 0; i < NUMBER_OF_CLIENTS; i++) {
            clients[i] = new Thread(new Runnable() {
                @Override public void run() {
                    for (int j = 0; j < NUMBER_OF_ITERATIONS; j++) {
                        final long id = idGenerator.incrementAndGet();
                        BenchmarkMessage message = new BenchmarkMessage(id, payload);
                        BaseTransportResponseHandler<BenchmarkMessage> handler = new BaseTransportResponseHandler<BenchmarkMessage>() {
                            @Override public BenchmarkMessage newInstance() {
                                return new BenchmarkMessage();
                            }

                            @Override public void handleResponse(BenchmarkMessage response) {
                                if (response.id != id) {
                                    System.out.println("NO ID MATCH [" + response.id + "] and [" + id + "]");
                                }
                                latch.countDown();
                            }

                            @Override public void handleException(TransportException exp) {
                                exp.printStackTrace();
                                latch.countDown();
                            }

                            @Override public boolean spawn() {
                                return spawn;
                            }
                        };

                        if (waitForRequest) {
                            transportService.submitRequest(node, "benchmark", message, handler).txGet();
                        } else {
                            transportService.sendRequest(node, "benchmark", message, handler);
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

        transportService.close();
        threadPool.shutdownNow();
    }
}
