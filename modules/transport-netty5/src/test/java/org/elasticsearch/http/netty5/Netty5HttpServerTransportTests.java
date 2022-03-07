/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty5;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PoolArenaMetric;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocatorMetric;
import io.netty.buffer.api.BufferAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SingleThreadEventLoop;
import io.netty.channel.nio.NioHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.concurrent.Future;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.http.AbstractHttpServerTransportTestCase;
import org.elasticsearch.http.BindHttpException;
import org.elasticsearch.http.CorsHandler;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.http.HttpTransportSettings;
import org.elasticsearch.http.NullDispatcher;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.netty5.Netty5Utils;
import org.elasticsearch.transport.netty5.NettyAllocator;
import org.elasticsearch.transport.netty5.SharedGroupFactory;
import org.junit.After;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_ORIGIN;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ENABLED;
import static org.elasticsearch.rest.RestStatus.OK;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 * Tests for the {@link Netty5HttpServerTransport} class.
 */
public class Netty5HttpServerTransportTests extends AbstractHttpServerTransportTestCase {

    private NetworkService networkService;
    private ThreadPool threadPool;
    private MockBigArrays bigArrays;
    private ClusterSettings clusterSettings;

    @Before
    public void setup() throws Exception {
        networkService = new NetworkService(Collections.emptyList());
        threadPool = new TestThreadPool("test");
        bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
        clusterSettings = randomClusterSettings();
    }

    @After
    public void shutdown() throws Exception {
        if (threadPool != null) {
            threadPool.shutdownNow();
        }
        threadPool = null;
        networkService = null;
        bigArrays = null;
        clusterSettings = null;
    }

    /**
     * Test that {@link Netty5HttpServerTransport} supports the "Expect: 100-continue" HTTP header
     * @throws InterruptedException if the client communication with the server is interrupted
     */
    public void testExpectContinueHeader() throws InterruptedException {
        final Settings settings = createSettings();
        final int contentLength = randomIntBetween(1, HttpTransportSettings.SETTING_HTTP_MAX_CONTENT_LENGTH.get(settings).bytesAsInt());
        runExpectHeaderTest(settings, HttpHeaderValues.CONTINUE.toString(), contentLength, HttpResponseStatus.CONTINUE);
    }

    /**
     * Test that {@link Netty5HttpServerTransport} responds to a
     * 100-continue expectation with too large a content-length
     * with a 413 status.
     * @throws InterruptedException if the client communication with the server is interrupted
     */
    public void testExpectContinueHeaderContentLengthTooLong() throws InterruptedException {
        final String key = HttpTransportSettings.SETTING_HTTP_MAX_CONTENT_LENGTH.getKey();
        final int maxContentLength = randomIntBetween(1, 104857600);
        final Settings settings = createBuilderWithPort().put(key, maxContentLength + "b").build();
        final int contentLength = randomIntBetween(maxContentLength + 1, Integer.MAX_VALUE);
        runExpectHeaderTest(settings, HttpHeaderValues.CONTINUE.toString(), contentLength, HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE);
    }

    /**
     * Test that {@link Netty5HttpServerTransport} responds to an unsupported expectation with a 417 status.
     * @throws InterruptedException if the client communication with the server is interrupted
     */
    public void testExpectUnsupportedExpectation() throws InterruptedException {
        Settings settings = createSettings();
        runExpectHeaderTest(settings, "chocolate=yummy", 0, HttpResponseStatus.EXPECTATION_FAILED);
    }

    private void runExpectHeaderTest(
        final Settings settings,
        final String expectation,
        final int contentLength,
        final HttpResponseStatus expectedStatus
    ) throws InterruptedException {
        final HttpServerTransport.Dispatcher dispatcher = new HttpServerTransport.Dispatcher() {
            @Override
            public void dispatchRequest(RestRequest request, RestChannel channel, ThreadContext threadContext) {
                channel.sendResponse(new BytesRestResponse(OK, BytesRestResponse.TEXT_CONTENT_TYPE, new BytesArray("done")));
            }

            @Override
            public void dispatchBadRequest(RestChannel channel, ThreadContext threadContext, Throwable cause) {
                logger.error(
                    new ParameterizedMessage("--> Unexpected bad request [{}]", FakeRestRequest.requestToString(channel.request())),
                    cause
                );
                throw new AssertionError();
            }
        };
        try (
            Netty5HttpServerTransport transport = new Netty5HttpServerTransport(
                settings,
                networkService,
                bigArrays,
                threadPool,
                xContentRegistry(),
                dispatcher,
                clusterSettings,
                new SharedGroupFactory(settings)
            )
        ) {
            transport.start();
            final TransportAddress remoteAddress = randomFrom(transport.boundAddress().boundAddresses());
            try (Netty5HttpClient client = new Netty5HttpClient()) {
                final FullHttpRequest request = new DefaultFullHttpRequest(
                    HttpVersion.HTTP_1_1,
                    HttpMethod.POST,
                    "/",
                    Netty5Utils.EMPTY_BUFFER
                );
                request.headers().set(HttpHeaderNames.EXPECT, expectation);
                HttpUtil.setContentLength(request, contentLength);

                try (FullHttpResponse response = client.send(remoteAddress.address(), request)) {
                    assertThat(response.status(), equalTo(expectedStatus));
                    if (expectedStatus.equals(HttpResponseStatus.CONTINUE)) {
                        final FullHttpRequest continuationRequest = new DefaultFullHttpRequest(
                            HttpVersion.HTTP_1_1,
                            HttpMethod.POST,
                            "/",
                            Netty5Utils.EMPTY_BUFFER
                        );
                        try (FullHttpResponse continuationResponse = client.send(remoteAddress.address(), continuationRequest)) {
                            assertThat(continuationResponse.status(), is(HttpResponseStatus.OK));
                            assertThat(continuationResponse.payload().toString(StandardCharsets.UTF_8), is("done"));
                        }
                    }
                }
            }
        }
    }

    public void testBindUnavailableAddress() {
        Settings initialSettings = createSettings();
        try (
            Netty5HttpServerTransport transport = new Netty5HttpServerTransport(
                initialSettings,
                networkService,
                bigArrays,
                threadPool,
                xContentRegistry(),
                new NullDispatcher(),
                clusterSettings,
                new SharedGroupFactory(Settings.EMPTY)
            )
        ) {
            transport.start();
            TransportAddress remoteAddress = randomFrom(transport.boundAddress().boundAddresses());
            Settings settings = Settings.builder()
                .put("http.port", remoteAddress.getPort())
                .put("network.host", remoteAddress.getAddress())
                .build();
            try (
                Netty5HttpServerTransport otherTransport = new Netty5HttpServerTransport(
                    settings,
                    networkService,
                    bigArrays,
                    threadPool,
                    xContentRegistry(),
                    new NullDispatcher(),
                    clusterSettings,
                    new SharedGroupFactory(settings)
                )
            ) {
                BindHttpException bindHttpException = expectThrows(BindHttpException.class, otherTransport::start);
                assertEquals("Failed to bind to " + NetworkAddress.format(remoteAddress.address()), bindHttpException.getMessage());
            }
        }
    }

    public void testBadRequest() {
        final HttpServerTransport.Dispatcher dispatcher = new HttpServerTransport.Dispatcher() {

            @Override
            public void dispatchRequest(final RestRequest request, final RestChannel channel, final ThreadContext threadContext) {
                logger.error("--> Unexpected successful request [{}]", FakeRestRequest.requestToString(request));
                throw new AssertionError();
            }

            @Override
            public void dispatchBadRequest(final RestChannel channel, final ThreadContext threadContext, final Throwable cause) {
                logger.error("--> Unexpected bad request", cause);
                throw new AssertionError();
            }

        };

        final Settings settings;
        final int maxInitialLineLength;
        final Setting<ByteSizeValue> httpMaxInitialLineLengthSetting = HttpTransportSettings.SETTING_HTTP_MAX_INITIAL_LINE_LENGTH;
        if (randomBoolean()) {
            maxInitialLineLength = httpMaxInitialLineLengthSetting.getDefault(Settings.EMPTY).bytesAsInt();
            settings = createSettings();
        } else {
            maxInitialLineLength = randomIntBetween(1, 8192);
            settings = createBuilderWithPort().put(httpMaxInitialLineLengthSetting.getKey(), maxInitialLineLength + "b").build();
        }

        try (
            Netty5HttpServerTransport transport = new Netty5HttpServerTransport(
                settings,
                networkService,
                bigArrays,
                threadPool,
                xContentRegistry(),
                dispatcher,
                clusterSettings,
                new SharedGroupFactory(settings)
            )
        ) {
            transport.start();
            final TransportAddress remoteAddress = randomFrom(transport.boundAddress().boundAddresses());

            try (Netty5HttpClient client = new Netty5HttpClient()) {
                final String url = "/" + new String(new byte[maxInitialLineLength], StandardCharsets.UTF_8);
                final FullHttpRequest request = new DefaultFullHttpRequest(
                    HttpVersion.HTTP_1_1,
                    HttpMethod.GET,
                    url,
                    Netty5Utils.EMPTY_BUFFER
                );
                expectThrows(CompletionException.class, () -> client.send(remoteAddress.address(), request));
            }
        }
    }

    public void testLargeCompressedResponse() throws InterruptedException {
        final String responseString = randomAlphaOfLength(4 * 1024 * 1024);
        final String url = "/thing";
        final HttpServerTransport.Dispatcher dispatcher = new HttpServerTransport.Dispatcher() {

            @Override
            public void dispatchRequest(final RestRequest request, final RestChannel channel, final ThreadContext threadContext) {
                if (url.equals(request.uri())) {
                    channel.sendResponse(new BytesRestResponse(OK, responseString));
                } else {
                    logger.error("--> Unexpected successful uri [{}]", request.uri());
                    throw new AssertionError();
                }
            }

            @Override
            public void dispatchBadRequest(final RestChannel channel, final ThreadContext threadContext, final Throwable cause) {
                logger.error(
                    new ParameterizedMessage("--> Unexpected bad request [{}]", FakeRestRequest.requestToString(channel.request())),
                    cause
                );
                throw new AssertionError();
            }

        };

        try (
            Netty5HttpServerTransport transport = new Netty5HttpServerTransport(
                Settings.EMPTY,
                networkService,
                bigArrays,
                threadPool,
                xContentRegistry(),
                dispatcher,
                clusterSettings,
                new SharedGroupFactory(Settings.EMPTY)
            )
        ) {
            transport.start();
            final TransportAddress remoteAddress = randomFrom(transport.boundAddress().boundAddresses());

            try (Netty5HttpClient client = new Netty5HttpClient()) {
                DefaultFullHttpRequest request = new DefaultFullHttpRequest(
                    HttpVersion.HTTP_1_1,
                    HttpMethod.GET,
                    url,
                    Netty5Utils.EMPTY_BUFFER
                );
                request.headers().add(HttpHeaderNames.ACCEPT_ENCODING, randomFrom("deflate", "gzip"));
                long numOfHugeAllocations = getHugeAllocationCount();
                try (FullHttpResponse response = client.send(remoteAddress.address(), request)) {
                    assertThat(getHugeAllocationCount(), equalTo(numOfHugeAllocations));
                    assertThat(response.status(), equalTo(HttpResponseStatus.OK));
                    byte[] bytes = new byte[response.payload().readableBytes()];
                    response.payload().readBytes(bytes, 0, bytes.length);
                    assertThat(new String(bytes, StandardCharsets.UTF_8), equalTo(responseString));
                }
            }
        }
    }

    private long getHugeAllocationCount() {
        long numOfHugAllocations = 0;
        ByteBufAllocator allocator = NettyAllocator.getAllocator();
        assert allocator instanceof NettyAllocator.NoDirectBuffers;
        ByteBufAllocator delegate = ((NettyAllocator.NoDirectBuffers) allocator).getDelegate();
        if (delegate instanceof PooledByteBufAllocator) {
            PooledByteBufAllocatorMetric metric = ((PooledByteBufAllocator) delegate).metric();
            numOfHugAllocations = metric.heapArenas().stream().mapToLong(PoolArenaMetric::numHugeAllocations).sum();
        }
        return numOfHugAllocations;
    }

    @LuceneTestCase.AwaitsFix(bugUrl = "some encoding issue")
    public void testCorsRequest() throws InterruptedException {
        final HttpServerTransport.Dispatcher dispatcher = new HttpServerTransport.Dispatcher() {

            @Override
            public void dispatchRequest(final RestRequest request, final RestChannel channel, final ThreadContext threadContext) {
                logger.error("--> Unexpected successful request [{}]", FakeRestRequest.requestToString(request));
                throw new AssertionError();
            }

            @Override
            public void dispatchBadRequest(final RestChannel channel, final ThreadContext threadContext, final Throwable cause) {
                logger.error(
                    new ParameterizedMessage("--> Unexpected bad request [{}]", FakeRestRequest.requestToString(channel.request())),
                    cause
                );
                throw new AssertionError();
            }

        };

        final Settings settings = createBuilderWithPort().put(SETTING_CORS_ENABLED.getKey(), true)
            .put(SETTING_CORS_ALLOW_ORIGIN.getKey(), "elastic.co")
            .build();

        try (
            Netty5HttpServerTransport transport = new Netty5HttpServerTransport(
                settings,
                networkService,
                bigArrays,
                threadPool,
                xContentRegistry(),
                dispatcher,
                randomClusterSettings(),
                new SharedGroupFactory(settings)
            )
        ) {
            transport.start();
            final TransportAddress remoteAddress = randomFrom(transport.boundAddress().boundAddresses());

            // Test pre-flight request
            try (Netty5HttpClient client = new Netty5HttpClient()) {
                final FullHttpRequest request = new DefaultFullHttpRequest(
                    HttpVersion.HTTP_1_1,
                    HttpMethod.OPTIONS,
                    "/",
                    Netty5Utils.EMPTY_BUFFER
                );
                request.headers().add(CorsHandler.ORIGIN, "elastic.co");
                request.headers().add(CorsHandler.ACCESS_CONTROL_REQUEST_METHOD, "POST");

                try (FullHttpResponse response = client.send(remoteAddress.address(), request)) {
                    assertThat(response.status(), equalTo(HttpResponseStatus.OK));
                    assertThat(response.headers().get(CorsHandler.ACCESS_CONTROL_ALLOW_ORIGIN), equalTo("elastic.co"));
                    assertThat(response.headers().get(CorsHandler.VARY), equalTo(CorsHandler.ORIGIN));
                    assertTrue(response.headers().contains(CorsHandler.DATE));
                }
            }

            // Test short-circuited request
            try (Netty5HttpClient client = new Netty5HttpClient()) {
                final FullHttpRequest request = new DefaultFullHttpRequest(
                    HttpVersion.HTTP_1_1,
                    HttpMethod.GET,
                    "/",
                    BufferAllocator.offHeapPooled().allocate(0)
                );
                request.headers().add(CorsHandler.ORIGIN, "elastic2.co");

                try (FullHttpResponse response = client.send(remoteAddress.address(), request)) {
                    assertThat(response.status(), equalTo(HttpResponseStatus.FORBIDDEN));
                }
            }
        }
    }

    public void testReadTimeout() throws Exception {
        final HttpServerTransport.Dispatcher dispatcher = new HttpServerTransport.Dispatcher() {

            @Override
            public void dispatchRequest(final RestRequest request, final RestChannel channel, final ThreadContext threadContext) {
                logger.error("--> Unexpected successful request [{}]", FakeRestRequest.requestToString(request));
                throw new AssertionError("Should not have received a dispatched request");
            }

            @Override
            public void dispatchBadRequest(final RestChannel channel, final ThreadContext threadContext, final Throwable cause) {
                logger.error(
                    new ParameterizedMessage("--> Unexpected bad request [{}]", FakeRestRequest.requestToString(channel.request())),
                    cause
                );
                throw new AssertionError("Should not have received a dispatched request");
            }

        };

        Settings settings = createBuilderWithPort().put(
            HttpTransportSettings.SETTING_HTTP_READ_TIMEOUT.getKey(),
            new TimeValue(randomIntBetween(100, 300))
        ).build();

        EventLoopGroup group = new SingleThreadEventLoop(r -> { return new Thread(r); }, NioHandler.newFactory().newHandler());
        try (
            Netty5HttpServerTransport transport = new Netty5HttpServerTransport(
                settings,
                networkService,
                bigArrays,
                threadPool,
                xContentRegistry(),
                dispatcher,
                randomClusterSettings(),
                new SharedGroupFactory(settings)
            )
        ) {
            transport.start();
            final TransportAddress remoteAddress = randomFrom(transport.boundAddress().boundAddresses());

            CountDownLatch channelClosedLatch = new CountDownLatch(1);

            Bootstrap clientBootstrap = new Bootstrap().option(ChannelOption.ALLOCATOR, NettyAllocator.getAllocator())
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {

                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new ChannelHandlerAdapter() {
                        });

                    }
                })
                .group(group);
            Future<Channel> connect = clientBootstrap.connect(remoteAddress.address());
            connect.get().closeFuture().addListener(future -> channelClosedLatch.countDown());

            assertTrue("Channel should be closed due to read timeout", channelClosedLatch.await(1, TimeUnit.MINUTES));

        } finally {
            group.shutdownGracefully().await();
        }
    }

    private Settings createSettings() {
        return createBuilderWithPort().build();
    }

    private Settings.Builder createBuilderWithPort() {
        return Settings.builder().put(HttpTransportSettings.SETTING_HTTP_PORT.getKey(), getPortRange());
    }
}
