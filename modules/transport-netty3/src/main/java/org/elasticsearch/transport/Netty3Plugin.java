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

package org.elasticsearch.transport;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.http.netty3.Netty3HttpServerTransport;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.plugins.NetworkPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.netty3.Netty3Transport;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class Netty3Plugin extends Plugin implements NetworkPlugin {
    public static final String NETTY_TRANSPORT_NAME = "netty3";
    public static final String NETTY_HTTP_TRANSPORT_NAME = "netty3";

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
            Netty3HttpServerTransport.SETTING_HTTP_NETTY_MAX_CUMULATION_BUFFER_CAPACITY,
            Netty3HttpServerTransport.SETTING_HTTP_NETTY_MAX_COMPOSITE_BUFFER_COMPONENTS,
            Netty3HttpServerTransport.SETTING_HTTP_WORKER_COUNT,
            Netty3HttpServerTransport.SETTING_HTTP_TCP_NO_DELAY,
            Netty3HttpServerTransport.SETTING_HTTP_TCP_KEEP_ALIVE,
            Netty3HttpServerTransport.SETTING_HTTP_TCP_BLOCKING_SERVER,
            Netty3HttpServerTransport.SETTING_HTTP_TCP_REUSE_ADDRESS,
            Netty3HttpServerTransport.SETTING_HTTP_TCP_SEND_BUFFER_SIZE,
            Netty3HttpServerTransport.SETTING_HTTP_TCP_RECEIVE_BUFFER_SIZE,
            Netty3Transport.WORKER_COUNT,
            Netty3Transport.NETTY_MAX_CUMULATION_BUFFER_CAPACITY,
            Netty3Transport.NETTY_MAX_COMPOSITE_BUFFER_COMPONENTS,
            Netty3Transport.NETTY_RECEIVE_PREDICTOR_SIZE,
            Netty3Transport.NETTY_RECEIVE_PREDICTOR_MIN,
            Netty3Transport.NETTY_RECEIVE_PREDICTOR_MAX,
            Netty3Transport.NETTY_BOSS_COUNT
        );
    }

    @Override
    public Map<String, Supplier<Transport>> getTransports(Settings settings, ThreadPool threadPool, BigArrays bigArrays,
                                                          CircuitBreakerService circuitBreakerService,
                                                          NamedWriteableRegistry namedWriteableRegistry, NetworkService networkService) {
        return Collections.singletonMap(NETTY_TRANSPORT_NAME, () -> new Netty3Transport(settings, threadPool, networkService, bigArrays,
            namedWriteableRegistry, circuitBreakerService));
    }

    @Override
    public Map<String, Supplier<HttpServerTransport>> getHttpTransports(Settings settings, ThreadPool threadPool, BigArrays bigArrays,
                                                                        CircuitBreakerService circuitBreakerService,
                                                                        NamedWriteableRegistry namedWriteableRegistry,
                                                                        NetworkService networkService) {
        return Collections.singletonMap(NETTY_HTTP_TRANSPORT_NAME, () -> new Netty3HttpServerTransport(settings, networkService,
            bigArrays, threadPool));
    }
}
