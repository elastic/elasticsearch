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
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.http.BindHttpException;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.http.HttpTransportSettings;
import org.elasticsearch.http.NullDispatcher;
import org.elasticsearch.http.netty4.cors.Netty4CorsConfig;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.elasticsearch.common.Strings.collectionToDelimitedString;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_CREDENTIALS;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_HEADERS;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_METHODS;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_ORIGIN;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ENABLED;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_MAX_AGE;
import static org.elasticsearch.rest.RestStatus.BAD_REQUEST;
import static org.elasticsearch.rest.RestStatus.OK;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
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
        networkService = new NetworkService(Collections.emptyList());
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
        final String prefix = randomBoolean() ? " " : ""; // sometimes have a leading whitespace between comma delimited elements
        final Settings settings = Settings.builder()
                                      .put(SETTING_CORS_ENABLED.getKey(), true)
                                      .put(SETTING_CORS_ALLOW_ORIGIN.getKey(), "*")
                                      .put(SETTING_CORS_ALLOW_METHODS.getKey(), collectionToDelimitedString(methods, ",", prefix, ""))
                                      .put(SETTING_CORS_ALLOW_HEADERS.getKey(), collectionToDelimitedString(headers, ",", prefix, ""))
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
     * @throws InterruptedException if the client communication with the server is interrupted
     */
    public void testExpectContinueHeader() throws InterruptedException {
        final Settings settings = Settings.EMPTY;
        final int contentLength = randomIntBetween(1, HttpTransportSettings.SETTING_HTTP_MAX_CONTENT_LENGTH.get(settings).bytesAsInt());
        runExpectHeaderTest(settings, HttpHeaderValues.CONTINUE.toString(), contentLength, HttpResponseStatus.CONTINUE);
    }

    /**
     * Test that {@link Netty4HttpServerTransport} responds to a
     * 100-continue expectation with too large a content-length
     * with a 413 status.
     * @throws InterruptedException if the client communication with the server is interrupted
     */
    public void testExpectContinueHeaderContentLengthTooLong() throws InterruptedException {
        final String key = HttpTransportSettings.SETTING_HTTP_MAX_CONTENT_LENGTH.getKey();
        final int maxContentLength = randomIntBetween(1, 104857600);
        final Settings settings = Settings.builder().put(key, maxContentLength + "b").build();
        final int contentLength = randomIntBetween(maxContentLength + 1, Integer.MAX_VALUE);
        runExpectHeaderTest(
                settings, HttpHeaderValues.CONTINUE.toString(), contentLength, HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE);
    }

    /**
     * Test that {@link Netty4HttpServerTransport} responds to an unsupported expectation with a 417 status.
     * @throws InterruptedException if the client communication with the server is interrupted
     */
    public void testExpectUnsupportedExpectation() throws InterruptedException {
        runExpectHeaderTest(Settings.EMPTY, "chocolate=yummy", 0, HttpResponseStatus.EXPECTATION_FAILED);
    }

    private void runExpectHeaderTest(
            final Settings settings,
            final String expectation,
            final int contentLength,
            final HttpResponseStatus expectedStatus) throws InterruptedException {
        final HttpServerTransport.Dispatcher dispatcher = new HttpServerTransport.Dispatcher() {
            @Override
            public void dispatchRequest(RestRequest request, RestChannel channel, ThreadContext threadContext) {
                channel.sendResponse(new BytesRestResponse(OK, BytesRestResponse.TEXT_CONTENT_TYPE, new BytesArray("done")));
            }

            @Override
            public void dispatchBadRequest(RestRequest request, RestChannel channel, ThreadContext threadContext, Throwable cause) {
                throw new AssertionError();
            }
        };
        try (Netty4HttpServerTransport transport = new Netty4HttpServerTransport(settings, networkService, bigArrays, threadPool,
                xContentRegistry(), dispatcher)) {
            transport.start();
            final TransportAddress remoteAddress = randomFrom(transport.boundAddress().boundAddresses());
            try (Netty4HttpClient client = new Netty4HttpClient()) {
                final FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/");
                request.headers().set(HttpHeaderNames.EXPECT, expectation);
                HttpUtil.setContentLength(request, contentLength);

                final FullHttpResponse response = client.post(remoteAddress.address(), request);
                assertThat(response.status(), equalTo(expectedStatus));
                if (expectedStatus.equals(HttpResponseStatus.CONTINUE)) {
                    final FullHttpRequest continuationRequest =
                        new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/", Unpooled.EMPTY_BUFFER);
                    final FullHttpResponse continuationResponse = client.post(remoteAddress.address(), continuationRequest);

                    assertThat(continuationResponse.status(), is(HttpResponseStatus.OK));
                    assertThat(new String(ByteBufUtil.getBytes(continuationResponse.content()), StandardCharsets.UTF_8), is("done"));
                }
            }
        }
    }

    public void testBindUnavailableAddress() {
        try (Netty4HttpServerTransport transport = new Netty4HttpServerTransport(Settings.EMPTY, networkService, bigArrays, threadPool,
                xContentRegistry(), new NullDispatcher())) {
            transport.start();
            TransportAddress remoteAddress = randomFrom(transport.boundAddress().boundAddresses());
            Settings settings = Settings.builder().put("http.port", remoteAddress.getPort()).build();
            try (Netty4HttpServerTransport otherTransport = new Netty4HttpServerTransport(settings, networkService, bigArrays, threadPool,
                    xContentRegistry(), new NullDispatcher())) {
                BindHttpException bindHttpException = expectThrows(BindHttpException.class, () -> otherTransport.start());
                assertEquals("Failed to bind to [" + remoteAddress.getPort() + "]", bindHttpException.getMessage());
            }
        }
    }

    public void testBadRequest() throws InterruptedException {
        final AtomicReference<Throwable> causeReference = new AtomicReference<>();
        final HttpServerTransport.Dispatcher dispatcher = new HttpServerTransport.Dispatcher() {

            @Override
            public void dispatchRequest(final RestRequest request, final RestChannel channel, final ThreadContext threadContext) {
                throw new AssertionError();
            }

            @Override
            public void dispatchBadRequest(final RestRequest request,
                                           final RestChannel channel,
                                           final ThreadContext threadContext,
                                           final Throwable cause) {
                causeReference.set(cause);
                try {
                    final ElasticsearchException e = new ElasticsearchException("you sent a bad request and you should feel bad");
                    channel.sendResponse(new BytesRestResponse(channel, BAD_REQUEST, e));
                } catch (final IOException e) {
                    throw new AssertionError(e);
                }
            }

        };

        final Settings settings;
        final int maxInitialLineLength;
        final Setting<ByteSizeValue> httpMaxInitialLineLengthSetting = HttpTransportSettings.SETTING_HTTP_MAX_INITIAL_LINE_LENGTH;
        if (randomBoolean()) {
            maxInitialLineLength = httpMaxInitialLineLengthSetting.getDefault(Settings.EMPTY).bytesAsInt();
            settings = Settings.EMPTY;
        } else {
            maxInitialLineLength = randomIntBetween(1, 8192);
            settings = Settings.builder().put(httpMaxInitialLineLengthSetting.getKey(), maxInitialLineLength + "b").build();
        }

        try (Netty4HttpServerTransport transport =
                     new Netty4HttpServerTransport(settings, networkService, bigArrays, threadPool, xContentRegistry(), dispatcher)) {
            transport.start();
            final TransportAddress remoteAddress = randomFrom(transport.boundAddress.boundAddresses());

            try (Netty4HttpClient client = new Netty4HttpClient()) {
                final String url = "/" + new String(new byte[maxInitialLineLength], Charset.forName("UTF-8"));
                final FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, url);

                final FullHttpResponse response = client.post(remoteAddress.address(), request);
                assertThat(response.status(), equalTo(HttpResponseStatus.BAD_REQUEST));
                assertThat(
                        new String(response.content().array(), Charset.forName("UTF-8")),
                        containsString("you sent a bad request and you should feel bad"));
            }
        }

        assertNotNull(causeReference.get());
        assertThat(causeReference.get(), instanceOf(TooLongFrameException.class));
    }

    public void testDispatchDoesNotModifyThreadContext() throws InterruptedException {
        final HttpServerTransport.Dispatcher dispatcher = new HttpServerTransport.Dispatcher() {

            @Override
            public void dispatchRequest(final RestRequest request, final RestChannel channel, final ThreadContext threadContext) {
                threadContext.putHeader("foo", "bar");
                threadContext.putTransient("bar", "baz");
            }

            @Override
            public void dispatchBadRequest(final RestRequest request,
                                           final RestChannel channel,
                                           final ThreadContext threadContext,
                                           final Throwable cause) {
                threadContext.putHeader("foo_bad", "bar");
                threadContext.putTransient("bar_bad", "baz");
            }

        };

        try (Netty4HttpServerTransport transport =
                 new Netty4HttpServerTransport(Settings.EMPTY, networkService, bigArrays, threadPool, xContentRegistry(), dispatcher)) {
            transport.start();

            transport.dispatchRequest(null, null);
            assertNull(threadPool.getThreadContext().getHeader("foo"));
            assertNull(threadPool.getThreadContext().getTransient("bar"));

            transport.dispatchBadRequest(null, null, null);
            assertNull(threadPool.getThreadContext().getHeader("foo_bad"));
            assertNull(threadPool.getThreadContext().getTransient("bar_bad"));
        }
    }
}
