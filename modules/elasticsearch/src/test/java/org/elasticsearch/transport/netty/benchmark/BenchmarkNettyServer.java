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

import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.cached.CachedThreadPool;
import org.elasticsearch.transport.BaseTransportRequestHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.netty.NettyTransport;
import org.elasticsearch.util.settings.ImmutableSettings;
import org.elasticsearch.util.settings.Settings;

/**
 * @author kimchy (Shay Banon)
 */
public class BenchmarkNettyServer {

    public static void main(String[] args) {
        final boolean spawn = true;

        Settings settings = ImmutableSettings.settingsBuilder()
                .putInt("transport.netty.port", 9999)
                .build();

        final ThreadPool threadPool = new CachedThreadPool();
        final TransportService transportService = new TransportService(new NettyTransport(settings, threadPool)).start();

        transportService.registerHandler("benchmark", new BaseTransportRequestHandler<BenchmarkMessage>() {
            @Override public BenchmarkMessage newInstance() {
                return new BenchmarkMessage();
            }

            @Override public void messageReceived(BenchmarkMessage request, TransportChannel channel) throws Exception {
                channel.sendResponse(request);
            }

            @Override public boolean spawn() {
                return spawn;
            }
        });

        final Object mutex = new Object();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override public void run() {
                transportService.close();
                threadPool.shutdownNow();
                synchronized (mutex) {
                    mutex.notifyAll();
                }
            }
        });

        synchronized (mutex) {
            try {
                mutex.wait();
            } catch (InterruptedException e) {
                // ok?
            }
        }
    }
}
