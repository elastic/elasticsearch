/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.repositories.gcs;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

import org.apache.http.HttpHost;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.DefaultProxyRoutePlanner;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.test.ESTestCase;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;

public class MockHttpProxyServerTests extends ESTestCase {

    public void testProxyServerWorks() throws Exception {
        String httpBody = randomAlphaOfLength(32);
        var proxyServer = new MockHttpProxyServer().handler(new SimpleChannelInboundHandler<>() {
            @Override
            protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) throws Exception {
                assertEquals("GET", request.method().name());
                assertEquals("http://googleapis.com/", request.uri());
                var response = new DefaultFullHttpResponse(
                    HttpVersion.HTTP_1_1,
                    HttpResponseStatus.OK,
                    Unpooled.wrappedBuffer(httpBody.getBytes(StandardCharsets.UTF_8))
                );
                response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
                ctx.writeAndFlush(response);
            }

            @Override
            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                ExceptionsHelper.maybeDieOnAnotherThread(cause);
                ctx.writeAndFlush(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR));
                ctx.close();
            }
        });
        var httpClient = HttpClients.custom()
            .setRoutePlanner(new DefaultProxyRoutePlanner(new HttpHost(InetAddress.getLoopbackAddress(), proxyServer.getPort())))
            .build();
        try (
            proxyServer;
            httpClient;
            var httpResponse = SocketAccess.doPrivilegedIOException(() -> httpClient.execute(new HttpGet("http://googleapis.com/")))
        ) {
            assertEquals(200, httpResponse.getStatusLine().getStatusCode());
            assertEquals(httpBody.length(), httpResponse.getEntity().getContentLength());
            assertEquals(httpBody, EntityUtils.toString(httpResponse.getEntity()));
        }
    }
}
