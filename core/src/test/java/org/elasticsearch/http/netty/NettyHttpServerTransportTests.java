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

package org.elasticsearch.http.netty;

import org.elasticsearch.cache.recycler.MockPageCacheRecycler;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.http.netty.cors.CorsConfig;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.http.netty.NettyHttpServerTransport.SETTING_CORS_ALLOW_CREDENTIALS;
import static org.elasticsearch.http.netty.NettyHttpServerTransport.SETTING_CORS_ALLOW_HEADERS;
import static org.elasticsearch.http.netty.NettyHttpServerTransport.SETTING_CORS_ALLOW_METHODS;
import static org.elasticsearch.http.netty.NettyHttpServerTransport.SETTING_CORS_ALLOW_ORIGIN;
import static org.elasticsearch.http.netty.NettyHttpServerTransport.SETTING_CORS_ENABLED;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for the {@link NettyHttpServerTransport} class.
 */
public class NettyHttpServerTransportTests extends ESTestCase {
    private NetworkService networkService;
    private ThreadPool threadPool;
    private MockPageCacheRecycler mockPageCacheRecycler;
    private MockBigArrays bigArrays;

    @Before
    public void setup() throws Exception {
        networkService = new NetworkService(Settings.EMPTY);
        threadPool = new ThreadPool("test");
        mockPageCacheRecycler = new MockPageCacheRecycler(Settings.EMPTY, threadPool);
        bigArrays = new MockBigArrays(mockPageCacheRecycler, new NoneCircuitBreakerService());
    }

    @After
    public void shutdown() throws Exception {
        if (threadPool != null) {
            threadPool.shutdownNow();
        }
        threadPool = null;
        networkService = null;
        mockPageCacheRecycler = null;
        bigArrays = null;
    }

    @Test
    public void testCorsConfig() throws Exception {
        final Set<String> methods = new HashSet<>(Arrays.asList("get", "options", "post"));
        final Set<String> headers = new HashSet<>(Arrays.asList("Content-Type", "Content-Length"));
        final Settings settings = Settings.builder()
                                      .put(SETTING_CORS_ENABLED, true)
                                      .put(SETTING_CORS_ALLOW_ORIGIN, "*")
                                      .put(SETTING_CORS_ALLOW_METHODS, Strings.collectionToCommaDelimitedString(methods))
                                      .put(SETTING_CORS_ALLOW_HEADERS, Strings.collectionToCommaDelimitedString(headers))
                                      .put(SETTING_CORS_ALLOW_CREDENTIALS, true)
                                      .build();
        final NettyHttpServerTransport transport = new NettyHttpServerTransport(settings, networkService, bigArrays);
        final CorsConfig corsConfig = transport.getCorsConfig();
        assertThat(corsConfig.isAnyOriginSupported(), equalTo(true));
        assertThat(corsConfig.allowedRequestHeaders(), equalTo(headers));
        final Set<String> allowedRequestMethods = new HashSet<>();
        for (HttpMethod method : corsConfig.allowedRequestMethods()) {
            allowedRequestMethods.add(method.getName());
        }
        assertThat(allowedRequestMethods, equalTo(methods));
        transport.close();
    }
}
