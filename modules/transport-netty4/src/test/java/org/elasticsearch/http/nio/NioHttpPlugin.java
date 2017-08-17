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

package org.elasticsearch.http.nio;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.plugins.NetworkPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.nio.NioTransport;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class NioHttpPlugin extends Plugin implements NetworkPlugin {

    public static final String NIO_TRANSPORT_NAME = "nio";
    public static final String NIO_HTTP_TRANSPORT_NAME = "nio";

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
            NioHttpTransport.NIO_HTTP_WORKER_COUNT,
            NioHttpTransport.NIO_HTTP_ACCEPTOR_COUNT,
            NioHttpTransport.SETTING_HTTP_TCP_NO_DELAY,
            NioHttpTransport.SETTING_HTTP_TCP_KEEP_ALIVE,
            NioHttpTransport.SETTING_HTTP_TCP_REUSE_ADDRESS,
            NioHttpTransport.SETTING_HTTP_TCP_SEND_BUFFER_SIZE,
            NioHttpTransport.SETTING_HTTP_TCP_RECEIVE_BUFFER_SIZE,
            NioTransport.NIO_WORKER_COUNT,
            NioTransport.NIO_ACCEPTOR_COUNT
        );
    }

    @Override
    public Settings additionalSettings() {
        return Settings.builder()
            .put(NetworkModule.HTTP_DEFAULT_TYPE_SETTING.getKey(), NIO_HTTP_TRANSPORT_NAME)
            .put(NetworkModule.TRANSPORT_DEFAULT_TYPE_SETTING.getKey(), NIO_TRANSPORT_NAME)
            .build();
    }

    @Override
    public Map<String, Supplier<Transport>> getTransports(Settings settings, ThreadPool threadPool, BigArrays bigArrays,
                                                          CircuitBreakerService circuitBreakerService,
                                                          NamedWriteableRegistry namedWriteableRegistry,
                                                          NetworkService networkService) {
        return Collections.singletonMap(NIO_TRANSPORT_NAME, () -> new NioTransport(settings, threadPool, networkService, bigArrays,
            namedWriteableRegistry, circuitBreakerService));
    }

    @Override
    public Map<String, Supplier<HttpServerTransport>> getHttpTransports(Settings settings, ThreadPool threadPool, BigArrays bigArrays,
                                                                        CircuitBreakerService circuitBreakerService,
                                                                        NamedWriteableRegistry namedWriteableRegistry,
                                                                        NamedXContentRegistry xContentRegistry,
                                                                        NetworkService networkService,
                                                                        HttpServerTransport.Dispatcher dispatcher) {
        return Collections.singletonMap(NIO_HTTP_TRANSPORT_NAME,
            () -> new NioHttpTransport(settings, networkService, bigArrays, threadPool, xContentRegistry, dispatcher));
    }
}
