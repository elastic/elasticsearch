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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.transport.Transports.TEST_MOCK_TRANSPORT_THREAD_PREFIX;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class Netty4HttpOptionsMethodTests extends AbstractHttpServerTransportTestCase {

    public void testHttpOptionsMethodWithRequestBody() throws Exception {
        final AtomicReference<RestRequest> dispatchedRequestReference = new AtomicReference<>();
        final HttpServerTransport.Dispatcher dispatcher = new HttpServerTransport.Dispatcher() {
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
        final ThreadPool testThreadPool = new TestThreadPool(TEST_MOCK_TRANSPORT_THREAD_PREFIX);
        try (
            Netty4HttpServerTransport transport = new Netty4HttpServerTransport(
                Settings.EMPTY,
                new NetworkService(List.of()),
                testThreadPool,
                xContentRegistry(),
                dispatcher,
                randomClusterSettings(),
                new SharedGroupFactory(Settings.EMPTY),
                Tracer.NOOP,
                TLSConfig.noTLS(),
                null,
                randomFrom((httpPreRequest, channel, listener) -> listener.onResponse(null), null)
            )
        ) {
            final ChannelHandler handler = transport.configureServerChannelHandler();
            final EmbeddedChannel ch = new EmbeddedChannel(handler);
            // OPTIONS request with fixed length content written in one go
            {
                ByteBuf buf = ch.alloc().buffer();
                ByteBufUtil.copy(AsciiString.of("OPTIONS /url HTTP/1.1"), buf);
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
                String content = randomAlphaOfLengthBetween(4, 1024);
                ByteBufUtil.copy(AsciiString.of("Content-Length: " + content.length()), buf);
                buf.writeByte(HttpConstants.LF);
                buf.writeByte(HttpConstants.LF);
                ByteBufUtil.copy(AsciiString.of(content), buf);
                testThreadPool.generic().submit(() -> {
                    ch.writeInbound(buf);
                    ch.flushInbound();
                }).get();
                RestRequest dispatchedRequest = dispatchedRequestReference.get();
                assertThat(dispatchedRequest.content().length(), is(0));
                // netty adds a content length of "0" when there's no content...
                assertThat(dispatchedRequest.header(HttpHeaderNames.CONTENT_LENGTH.toString()), is("0"));
                assertThat(dispatchedRequest.header(HttpHeaderNames.CONTENT_ENCODING.toString()), nullValue());
                assertThat(dispatchedRequest.header(HttpHeaderNames.CONTENT_TYPE.toString()), nullValue());
                if (hasHostHeader) {
                    assertThat(dispatchedRequest.header(HttpHeaderNames.HOST.toString()), is("localhost"));
                }
                if (hasAcceptHeader) {
                    assertThat(dispatchedRequest.header(HttpHeaderNames.ACCEPT.toString()), is("*/*"));
                }
            }
            // fixed length content written in multiple parts
            {
                ByteBuf buf = ch.alloc().buffer();
                ByteBufUtil.copy(AsciiString.of("OPTIONS /url HTTP/1.1"), buf);
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
                List<String> contentParts = Arrays.asList(randomArray(2, 5, String[]::new, () -> randomAlphaOfLengthBetween(2, 20)));
                ByteBufUtil.copy(
                    AsciiString.of("Content-Length: " + contentParts.stream().map(String::length).reduce(Integer::sum).get()),
                    buf
                );
                buf.writeByte(HttpConstants.LF);
                buf.writeByte(HttpConstants.LF);
                // write the headers
                testThreadPool.generic().submit(() -> {
                    ch.writeInbound(buf);
                    ch.flushInbound();
                }).get();
                // write the contents in multiple parts
                for (String content : contentParts) {
                    ByteBuf contentBuf = ch.alloc().buffer();
                    ByteBufUtil.copy(AsciiString.of(content), contentBuf);
                    testThreadPool.generic().submit(() -> {
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
                if (hasHostHeader) {
                    assertThat(dispatchedRequest.header(HttpHeaderNames.HOST.toString()), is("localhost"));
                }
                if (hasAcceptHeader) {
                    assertThat(dispatchedRequest.header(HttpHeaderNames.ACCEPT.toString()), is("*/*"));
                }
            }
            // testThreadPool.generic().submit(() -> ch.close().get()).get();
        } finally {
            testThreadPool.shutdownNow();
        }
    }
}
