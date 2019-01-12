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

package org.elasticsearch.transport.nio;

import org.elasticsearch.Version;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.http.nio.NioHttpServerTransport;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.nio.EventHandler;
import org.elasticsearch.nio.NioGroup;
import org.elasticsearch.plugins.NetworkPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.Transport;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.common.settings.Setting.intSetting;
import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;

public class NioTransportPlugin extends Plugin implements NetworkPlugin {

    public static final String NIO_TRANSPORT_NAME = "nio-transport";
    public static final String NIO_HTTP_TRANSPORT_NAME = "nio-http-transport";

    public static final Setting<Integer> NIO_WORKER_COUNT =
        new Setting<>("transport.nio.worker_count",
            (s) -> Integer.toString(EsExecutors.numberOfProcessors(s) * 2),
            (s) -> Setting.parseInt(s, 1, "transport.nio.worker_count"), Setting.Property.NodeScope);
    public static final Setting<Integer> NIO_HTTP_ACCEPTOR_COUNT =
        intSetting("http.nio.acceptor_count", 0, 0, Setting.Property.NodeScope);
    public static final Setting<Integer> NIO_HTTP_WORKER_COUNT =
        intSetting("http.nio.worker_count", 0, 0, Setting.Property.NodeScope);;

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
            NIO_HTTP_ACCEPTOR_COUNT,
            NIO_HTTP_WORKER_COUNT,
            NIO_WORKER_COUNT
        );
    }

    @Override
    public Map<String, Supplier<Transport>> getTransports(Settings settings, ThreadPool threadPool, PageCacheRecycler pageCacheRecycler,
                                                          CircuitBreakerService circuitBreakerService,
                                                          NamedWriteableRegistry namedWriteableRegistry, NetworkService networkService) {
        return Collections.singletonMap(NIO_TRANSPORT_NAME,
            () -> new NioTransport(settings, Version.CURRENT, threadPool, networkService, pageCacheRecycler, namedWriteableRegistry,
                circuitBreakerService));
    }

    @Override
    public Map<String, Supplier<HttpServerTransport>> getHttpTransports(Settings settings, ThreadPool threadPool, BigArrays bigArrays,
                                                                        PageCacheRecycler pageCacheRecycler,
                                                                        CircuitBreakerService circuitBreakerService,
                                                                        NamedXContentRegistry xContentRegistry,
                                                                        NetworkService networkService,
                                                                        HttpServerTransport.Dispatcher dispatcher) {
        return Collections.singletonMap(NIO_HTTP_TRANSPORT_NAME,
            () -> new NioHttpServerTransport(settings, networkService, bigArrays, pageCacheRecycler, threadPool, xContentRegistry,
                dispatcher));
    }

    private static class NioGroupSupplier implements CheckedFunction<String, NioGroup, IOException> {

        private final Settings settings;
        private final int httpWorkerCount;
        private volatile NioGroup nioGroup;

        private NioGroupSupplier(Settings settings) {
            this.settings = settings;
            this.httpWorkerCount = NIO_HTTP_WORKER_COUNT.get(settings);
        }

        @Override
        public synchronized NioGroup apply(String name) throws IOException {
            if (NIO_TRANSPORT_NAME.equals(name)) {
                return getGenericGroup();
            } else if (NIO_HTTP_TRANSPORT_NAME.equals(name)) {
                if (httpWorkerCount == 0) {
                    return getGenericGroup();
                } else {
                    // TODO: Implement http group
                    return null;
                }
            } else {
                throw new IllegalArgumentException();
            }
        }

        private NioGroup getGenericGroup() throws IOException {
            if (nioGroup == null) {
                nioGroup = new NioGroup(daemonThreadFactory(this.settings, TcpTransport.TRANSPORT_WORKER_THREAD_NAME_PREFIX),
                    NioTransportPlugin.NIO_WORKER_COUNT.get(settings), (s) -> new EventHandler(null, s));
            }
            return nioGroup;
        }
    }
}
