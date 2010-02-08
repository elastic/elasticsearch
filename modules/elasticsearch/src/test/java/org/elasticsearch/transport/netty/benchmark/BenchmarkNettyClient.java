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

import com.google.common.collect.Lists;
import org.elasticsearch.cluster.node.Node;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.cached.CachedThreadPool;
import org.elasticsearch.transport.BaseTransportResponseHandler;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.netty.NettyTransport;
import org.elasticsearch.util.SizeUnit;
import org.elasticsearch.util.SizeValue;
import org.elasticsearch.util.StopWatch;
import org.elasticsearch.util.settings.ImmutableSettings;
import org.elasticsearch.util.settings.Settings;
import org.elasticsearch.util.transport.InetSocketTransportAddress;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author kimchy (Shay Banon)
 */
public class BenchmarkNettyClient {


    public static void main(String[] args) {
        final SizeValue payloadSize = new SizeValue(100, SizeUnit.BYTES);
        final int NUMBER_OF_CLIENTS = 1;
        final int NUMBER_OF_ITERATIONS = 500000;
        final byte[] payload = new byte[(int) payloadSize.bytes()];
        final AtomicLong idGenerator = new AtomicLong();
        final boolean waitForRequest = false;
        final boolean spawn = true;

        Settings settings = ImmutableSettings.settingsBuilder()
                .putBoolean("network.server", false)
                .putInt("transport.netty.connectionsPerNode", 5)
                .build();

        final ThreadPool threadPool = new CachedThreadPool();
        final TransportService transportService = new TransportService(new NettyTransport(settings, threadPool)).start();

        final Node node = new Node("server", new InetSocketTransportAddress("localhost", 9999));

        transportService.nodesAdded(Lists.newArrayList(node));


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

                            @Override public void handleException(RemoteTransportException exp) {
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
