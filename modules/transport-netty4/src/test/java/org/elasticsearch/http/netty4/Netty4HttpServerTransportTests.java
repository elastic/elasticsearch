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

package org.elasticsearch.http.netty4;

import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.http.netty4.cors.Netty4CorsConfig;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_CREDENTIALS;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_HEADERS;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_METHODS;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_ORIGIN;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ENABLED;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_MAX_AGE;
import static org.elasticsearch.rest.RestStatus.OK;
import static org.hamcrest.Matchers.is;

/**
 * Tests for the {@link Netty4HttpServerTransport} class.
 */
public class Netty4HttpServerTransportTests extends ESTestCase {

    private NetworkService networkService;
    private ThreadPool threadPool;
    private MockBigArrays bigArrays;

    @Before
    public void setup() throws Exception {
        networkService = new NetworkService(Settings.EMPTY, Collections.emptyList());
        threadPool = new TestThreadPool("test");
        bigArrays = new MockBigArrays(Settings.EMPTY, new NoneCircuitBreakerService());
    }

    @After
    public void shutdown() throws Exception {
        if (threadPool != null) {
            threadPool.shutdownNow();
        }
        threadPool = null;
        networkService = null;
        bigArrays = null;
    }

    public void testCorsConfig() {
        final Set<String> methods = new HashSet<>(Arrays.asList("get", "options", "post"));
        final Set<String> headers = new HashSet<>(Arrays.asList("Content-Type", "Content-Length"));
        final Settings settings = Settings.builder()
                                      .put(SETTING_CORS_ENABLED.getKey(), true)
                                      .put(SETTING_CORS_ALLOW_ORIGIN.getKey(), "*")
                                      .put(SETTING_CORS_ALLOW_METHODS.getKey(), Strings.collectionToCommaDelimitedString(methods))
                                      .put(SETTING_CORS_ALLOW_HEADERS.getKey(), Strings.collectionToCommaDelimitedString(headers))
                                      .put(SETTING_CORS_ALLOW_CREDENTIALS.getKey(), true)
                                      .build();
        final Netty4CorsConfig corsConfig = Netty4HttpServerTransport.buildCorsConfig(settings);
        assertTrue(corsConfig.isAnyOriginSupported());
        assertEquals(headers, corsConfig.allowedRequestHeaders());
        assertEquals(methods, corsConfig.allowedRequestMethods().stream().map(HttpMethod::name).collect(Collectors.toSet()));
    }

    public void testCorsConfigWithDefaults() {
        final Set<String> methods = Strings.commaDelimitedListToSet(SETTING_CORS_ALLOW_METHODS.getDefault(Settings.EMPTY));
        final Set<String> headers = Strings.commaDelimitedListToSet(SETTING_CORS_ALLOW_HEADERS.getDefault(Settings.EMPTY));
        final long maxAge = SETTING_CORS_MAX_AGE.getDefault(Settings.EMPTY);
        final Settings settings = Settings.builder().put(SETTING_CORS_ENABLED.getKey(), true).build();
        final Netty4CorsConfig corsConfig = Netty4HttpServerTransport.buildCorsConfig(settings);
        assertFalse(corsConfig.isAnyOriginSupported());
        assertEquals(Collections.emptySet(), corsConfig.origins().get());
        assertEquals(headers, corsConfig.allowedRequestHeaders());
        assertEquals(methods, corsConfig.allowedRequestMethods().stream().map(HttpMethod::name).collect(Collectors.toSet()));
        assertEquals(maxAge, corsConfig.maxAge());
        assertFalse(corsConfig.isCredentialsAllowed());
    }

    /**
     * Test that {@link Netty4HttpServerTransport} supports the "Expect: 100-continue" HTTP header
     */
    public void testExpectContinueHeader() throws Exception {
        try (Netty4HttpServerTransport transport = new Netty4HttpServerTransport(Settings.EMPTY, networkService, bigArrays, threadPool)) {
            transport.httpServerAdapter((request, channel, context) ->
                    channel.sendResponse(new BytesRestResponse(OK, BytesRestResponse.TEXT_CONTENT_TYPE, new BytesArray("done"))));
            transport.start();
            InetSocketTransportAddress remoteAddress = (InetSocketTransportAddress) randomFrom(transport.boundAddress().boundAddresses());

            try (Netty4HttpClient client = new Netty4HttpClient()) {
                FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/");
                HttpUtil.set100ContinueExpected(request, true);
                HttpUtil.setContentLength(request, 10);

                FullHttpResponse response = client.post(remoteAddress.address(), request);
                assertThat(response.status(), is(HttpResponseStatus.CONTINUE));

                request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/", Unpooled.EMPTY_BUFFER);
                response = client.post(remoteAddress.address(), request);
                assertThat(response.status(), is(HttpResponseStatus.OK));
                assertThat(new String(ByteBufUtil.getBytes(response.content()), StandardCharsets.UTF_8), is("done"));
            }
        }
    }
}
