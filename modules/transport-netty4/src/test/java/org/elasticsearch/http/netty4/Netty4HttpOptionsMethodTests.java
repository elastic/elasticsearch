/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.HttpConstants;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.util.AsciiString;

import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.http.AbstractHttpServerTransportTestCase;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.tracing.Tracer;
import org.elasticsearch.transport.netty4.SharedGroupFactory;
import org.elasticsearch.transport.netty4.TLSConfig;
import org.junit.After;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.transport.Transports.TEST_MOCK_TRANSPORT_THREAD_PREFIX;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class Netty4HttpOptionsMethodTests extends AbstractHttpServerTransportTestCase {

    private NetworkService networkService;
    private ThreadPool threadPool;
    private ClusterSettings clusterSettings;

    @Before
    public void setup() {
        networkService = new NetworkService(Collections.emptyList());
        threadPool = new TestThreadPool(TEST_MOCK_TRANSPORT_THREAD_PREFIX);
        clusterSettings = randomClusterSettings();
    }

    @After
    public void shutdown() {
        if (threadPool != null) {
            threadPool.shutdownNow();
        }
        threadPool = null;
        networkService = null;
        clusterSettings = null;
    }

    private Netty4HttpServerTransport getHttpServerTransport(AtomicReference<RestRequest> dispatchedRequestReference) {
        HttpServerTransport.Dispatcher dispatcher = new HttpServerTransport.Dispatcher() {
            @Override
            public void dispatchRequest(final RestRequest request, final RestChannel channel, final ThreadContext threadContext) {
                dispatchedRequestReference.set(request);
            }

            @Override
            public void dispatchBadRequest(final RestChannel channel, final ThreadContext threadContext, final Throwable cause) {
                logger.error("--> Unexpected bad request dispatched[" + FakeRestRequest.requestToString(channel.request()) + "]", cause);
                throw new AssertionError("Unexpected bad request dispatch", cause);
            }
        };
        return new Netty4HttpServerTransport(
            Settings.EMPTY,
            networkService,
            threadPool,
            xContentRegistry(),
            dispatcher,
            clusterSettings,
            new SharedGroupFactory(Settings.EMPTY),
            Tracer.NOOP,
            TLSConfig.noTLS(),
            null,
            randomFrom((httpPreRequest, channel, listener) -> listener.onResponse(null), null)
        );
    }

    public void testRequestBodyFixedLengthSingleChunk() throws Exception {
        final AtomicReference<RestRequest> dispatchedRequestReference = new AtomicReference<>();
        try (Netty4HttpServerTransport transport = getHttpServerTransport(dispatchedRequestReference)) {
            final ChannelHandler handler = transport.configureServerChannelHandler();
            final EmbeddedChannel ch = new EmbeddedChannel(handler);
            // also test that requests following an OPTIONS are not affected
            for (String httpMethod : List.of("OPTIONS", randomFrom("PUT", "POST", "GET", "DELETE"))) {
                // OPTIONS request with fixed length content written in one chunk
                ByteBuf buf = ch.alloc().buffer();
                ByteBufUtil.copy(AsciiString.of(httpMethod + " /url/whatever/fixed-length-single-chunk HTTP/1.1"), buf);
                buf.writeByte(HttpConstants.LF);
                boolean hasHostHeader = randomBoolean();
                if (hasHostHeader) {
                    ByteBufUtil.copy(AsciiString.of("Host: localhost"), buf);
                    buf.writeByte(HttpConstants.LF);
                }
                boolean hasAcceptHeader = randomBoolean();
                if (hasAcceptHeader) {
                    ByteBufUtil.copy(AsciiString.of("Accept: */*"), buf);
                    buf.writeByte(HttpConstants.LF);
                }
                // content-encoding should be ignored for OPTIONS but it trips the test scenario for others
                if (randomBoolean() && httpMethod.equals("OPTIONS")) {
                    ByteBufUtil.copy(AsciiString.of("Content-Encoding: gzip"), buf);
                    buf.writeByte(HttpConstants.LF);
                }
                boolean hasContentTypeHeader = randomBoolean();
                if (hasContentTypeHeader) {
                    ByteBufUtil.copy(
                        AsciiString.of("Content-Type: " + randomFrom("text/plain; charset=utf-8", "application/json; charset=utf-8")),
                        buf
                    );
                    buf.writeByte(HttpConstants.LF);
                }
                String content = randomAlphaOfLengthBetween(4, 1024);
                // having a "Content-Length" request header is what makes it "fixed length"
                ByteBufUtil.copy(AsciiString.of("Content-Length: " + content.length()), buf);
                buf.writeByte(HttpConstants.LF);
                // end of headers
                buf.writeByte(HttpConstants.LF);
                ByteBufUtil.copy(AsciiString.of(content), buf);
                // write everything in one single chunk
                threadPool.generic().submit(() -> {
                    ch.writeInbound(buf);
                    ch.flushInbound();
                }).get();
                ch.runPendingTasks();
                RestRequest dispatchedRequest = dispatchedRequestReference.get();
                if ("OPTIONS".equals(httpMethod)) {
                    assertThat(dispatchedRequest.content().length(), is(0));
                    // netty adds a content length of "0" when there's no content...
                    assertThat(dispatchedRequest.header(HttpHeaderNames.CONTENT_LENGTH.toString()), is("0"));
                    assertThat(dispatchedRequest.header(HttpHeaderNames.CONTENT_ENCODING.toString()), nullValue());
                    assertThat(dispatchedRequest.header(HttpHeaderNames.CONTENT_TYPE.toString()), nullValue());
                } else {
                    assertThat(dispatchedRequest.content().utf8ToString(), is(content));
                    assertThat(dispatchedRequest.header(HttpHeaderNames.CONTENT_LENGTH.toString()), is(Integer.toString(content.length())));
                    assertThat(dispatchedRequest.header(HttpHeaderNames.CONTENT_ENCODING.toString()), nullValue());
                    if (hasContentTypeHeader) {
                        assertThat(
                            dispatchedRequest.header(HttpHeaderNames.CONTENT_TYPE.toString()),
                            anyOf(is("text/plain; charset=utf-8"), is("application/json; charset=utf-8"))
                        );
                    }
                }
                assertThat(dispatchedRequest.uri(), is("/url/whatever/fixed-length-single-chunk"));
                if (hasHostHeader) {
                    assertThat(dispatchedRequest.header(HttpHeaderNames.HOST.toString()), is("localhost"));
                }
                if (hasAcceptHeader) {
                    assertThat(dispatchedRequest.header(HttpHeaderNames.ACCEPT.toString()), is("*/*"));
                }
                dispatchedRequest.getHttpRequest().release();
            }
        }
    }

    public void testRequestBodyFixedLengthMultipleChunks() throws Exception {
        final AtomicReference<RestRequest> dispatchedRequestReference = new AtomicReference<>();
        try (Netty4HttpServerTransport transport = getHttpServerTransport(dispatchedRequestReference)) {
            final ChannelHandler handler = transport.configureServerChannelHandler();
            final EmbeddedChannel ch = new EmbeddedChannel(handler);
            ByteBuf buf = ch.alloc().buffer();
            ByteBufUtil.copy(AsciiString.of("OPTIONS /url/whatever/fixed-length?multiple-chunks=true HTTP/1.1"), buf);
            buf.writeByte(HttpConstants.LF);
            boolean hasHostHeader = randomBoolean();
            if (hasHostHeader) {
                ByteBufUtil.copy(AsciiString.of("Host: localhost"), buf);
                buf.writeByte(HttpConstants.LF);
            }
            boolean hasAcceptHeader = randomBoolean();
            if (hasAcceptHeader) {
                ByteBufUtil.copy(AsciiString.of("Accept: */*"), buf);
                buf.writeByte(HttpConstants.LF);
            }
            if (randomBoolean()) {
                ByteBufUtil.copy(AsciiString.of("Content-Encoding: gzip"), buf);
                buf.writeByte(HttpConstants.LF);
            }
            if (randomBoolean()) {
                ByteBufUtil.copy(AsciiString.of("Content-Type: text/plain; charset=UTF-8"), buf);
                buf.writeByte(HttpConstants.LF);
            }
            String[] contentParts = randomArray(2, 5, String[]::new, () -> randomAlphaOfLengthBetween(2, 20));
            // having a "Content-Length" request header is what makes it "fixed length"
            ByteBufUtil.copy(
                AsciiString.of("Content-Length: " + Arrays.stream(contentParts).map(String::length).reduce(Integer::sum).get()),
                buf
            );
            buf.writeByte(HttpConstants.LF);
            // end of headers
            buf.writeByte(HttpConstants.LF);
            // write the headers
            threadPool.generic().submit(() -> {
                ch.writeInbound(buf);
                ch.flushInbound();
            }).get();
            // write the contents in multiple parts
            for (String content : contentParts) {
                ByteBuf contentBuf = ch.alloc().buffer();
                ByteBufUtil.copy(AsciiString.of(content), contentBuf);
                threadPool.generic().submit(() -> {
                    ch.writeInbound(contentBuf);
                    ch.flushInbound();
                }).get();
            }
            RestRequest dispatchedRequest = dispatchedRequestReference.get();
            assertThat(dispatchedRequest.content().length(), is(0));
            // netty adds a content length of "0" when there's no content...
            assertThat(dispatchedRequest.header(HttpHeaderNames.CONTENT_LENGTH.toString()), is("0"));
            assertThat(dispatchedRequest.header(HttpHeaderNames.CONTENT_ENCODING.toString()), nullValue());
            assertThat(dispatchedRequest.header(HttpHeaderNames.CONTENT_TYPE.toString()), nullValue());
            assertThat(dispatchedRequest.uri(), is("/url/whatever/fixed-length?multiple-chunks=true"));
            if (hasHostHeader) {
                assertThat(dispatchedRequest.header(HttpHeaderNames.HOST.toString()), is("localhost"));
            }
            if (hasAcceptHeader) {
                assertThat(dispatchedRequest.header(HttpHeaderNames.ACCEPT.toString()), is("*/*"));
            }
            dispatchedRequest.getHttpRequest().release();
        }
    }

    public void testRequestVariableLengthChunkedTransferEncoding() throws Exception {
        final AtomicReference<RestRequest> dispatchedRequestReference = new AtomicReference<>();
        try (Netty4HttpServerTransport transport = getHttpServerTransport(dispatchedRequestReference)) {
            final ChannelHandler handler = transport.configureServerChannelHandler();
            final EmbeddedChannel ch = new EmbeddedChannel(handler);
            ByteBuf buf = ch.alloc().buffer();
            ByteBufUtil.copy(AsciiString.of("OPTIONS /url/whatever/chunked-transfer?encoding HTTP/1.1"), buf);
            buf.writeByte(HttpConstants.LF);
            boolean hasHostHeader = randomBoolean();
            if (hasHostHeader) {
                ByteBufUtil.copy(AsciiString.of("Host: localhost"), buf);
                buf.writeByte(HttpConstants.LF);
            }
            boolean hasAcceptHeader = randomBoolean();
            if (hasAcceptHeader) {
                ByteBufUtil.copy(AsciiString.of("Accept: */*"), buf);
                buf.writeByte(HttpConstants.LF);
            }
            if (randomBoolean()) {
                ByteBufUtil.copy(AsciiString.of("Content-Encoding: gzip"), buf);
                buf.writeByte(HttpConstants.LF);
            }
            if (randomBoolean()) {
                ByteBufUtil.copy(AsciiString.of("Content-Type: text/plain; charset=UTF-8"), buf);
                buf.writeByte(HttpConstants.LF);
            }
            // do not write a "Content-Length" header to make the request "variable length"
            ByteBufUtil.copy(AsciiString.of("Transfer-Encoding: " + randomFrom("chunked", "gzip, chunked")), buf);
            buf.writeByte(HttpConstants.LF);
            buf.writeByte(HttpConstants.LF);
            // maybe append some chunks as well
            String[] contentParts = randomArray(0, 4, String[]::new, () -> randomAlphaOfLengthBetween(1, 64));
            for (String content : contentParts) {
                ByteBufUtil.copy(AsciiString.of(Integer.toHexString(content.length())), buf);
                buf.writeByte(HttpConstants.CR);
                buf.writeByte(HttpConstants.LF);
                ByteBufUtil.copy(AsciiString.of(content), buf);
                buf.writeByte(HttpConstants.CR);
                buf.writeByte(HttpConstants.LF);
            }
            threadPool.generic().submit(() -> {
                ch.writeInbound(buf);
                ch.flushInbound();
            }).get();
            // append some more chunks as well
            ByteBuf buf2 = ch.alloc().buffer();
            contentParts = randomArray(1, 4, String[]::new, () -> randomAlphaOfLengthBetween(1, 64));
            for (String content : contentParts) {
                ByteBufUtil.copy(AsciiString.of(Integer.toHexString(content.length())), buf2);
                buf2.writeByte(HttpConstants.CR);
                buf2.writeByte(HttpConstants.LF);
                ByteBufUtil.copy(AsciiString.of(content), buf2);
                buf2.writeByte(HttpConstants.CR);
                buf2.writeByte(HttpConstants.LF);
            }
            // finish chunked request
            ByteBufUtil.copy(AsciiString.of("0"), buf2);
            buf2.writeByte(HttpConstants.CR);
            buf2.writeByte(HttpConstants.LF);
            buf2.writeByte(HttpConstants.CR);
            buf2.writeByte(HttpConstants.LF);
            threadPool.generic().submit(() -> {
                ch.writeInbound(buf2);
                ch.flushInbound();
            }).get();
            RestRequest dispatchedRequest = dispatchedRequestReference.get();
            assertThat(dispatchedRequest.content().length(), is(0));
            // netty adds a content length of "0" when there's no content...
            assertThat(dispatchedRequest.header(HttpHeaderNames.CONTENT_LENGTH.toString()), is("0"));
            assertThat(dispatchedRequest.header(HttpHeaderNames.CONTENT_ENCODING.toString()), nullValue());
            assertThat(dispatchedRequest.header(HttpHeaderNames.CONTENT_TYPE.toString()), nullValue());
            assertThat(dispatchedRequest.header(HttpHeaderNames.TRANSFER_ENCODING.toString()), nullValue());
            assertThat(dispatchedRequest.uri(), is("/url/whatever/chunked-transfer?encoding"));
            if (hasHostHeader) {
                assertThat(dispatchedRequest.header(HttpHeaderNames.HOST.toString()), is("localhost"));
            }
            if (hasAcceptHeader) {
                assertThat(dispatchedRequest.header(HttpHeaderNames.ACCEPT.toString()), is("*/*"));
            }
            dispatchedRequest.getHttpRequest().release();
        }
    }
}
