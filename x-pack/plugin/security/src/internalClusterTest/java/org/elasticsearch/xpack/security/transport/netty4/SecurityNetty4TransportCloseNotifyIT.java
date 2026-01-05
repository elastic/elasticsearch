/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.transport.netty4;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestEncoder;
import io.netty.handler.codec.http.HttpResponseDecoder;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.support.CancellableActionTestPlugin;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.SecurityIntegTestCase;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.elasticsearch.test.SecuritySettingsSource.addSSLSettingsForNodePEMFiles;
import static org.elasticsearch.test.TaskAssertions.assertAllTasksHaveFinished;
import static org.elasticsearch.test.rest.ESRestTestCase.basicAuthHeaderValue;

public class SecurityNetty4TransportCloseNotifyIT extends SecurityIntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        final Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        addSSLSettingsForNodePEMFiles(builder, "xpack.security.http.", randomBoolean());
        return builder.put("xpack.security.http.ssl.enabled", true).build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), CancellableActionTestPlugin.class);
    }

    private static Bootstrap setupNettyClient(String node, Consumer<FullHttpResponse> responseHandler) throws Exception {
        var sslCtx = SslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).build();
        var httpServer = internalCluster().getInstance(HttpServerTransport.class, node);
        var remoteAddr = randomFrom(httpServer.boundAddress().boundAddresses());
        return new Bootstrap().group(new NioEventLoopGroup(1))
            .channel(NioSocketChannel.class)
            .remoteAddress(remoteAddr.getAddress(), remoteAddr.getPort())
            .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) {
                    var p = ch.pipeline();
                    p.addLast(sslCtx.newHandler(ch.alloc()));
                    p.addLast(new HttpRequestEncoder());
                    p.addLast(new HttpResponseDecoder());
                    p.addLast(new HttpObjectAggregator(4096));
                    p.addLast(new SimpleChannelInboundHandler<FullHttpResponse>() {
                        @Override
                        protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse msg) {
                            responseHandler.accept(msg);
                        }
                    });
                }
            });
    }

    /**
     * Ensures that receiving close_notify on server will close connection.
     * Simulates normal connection flow where client and server exchange a few requests and responses.
     * After an exchange client sends close_notify and expects the server to close connection.
     */
    public void testSendCloseNotifyAfterHttpGetRequests() throws Exception {
        final var nReq = randomIntBetween(0, 10); // nothing particular about number 10
        final var responsesReceivedLatch = new CountDownLatch(nReq);
        final var client = setupNettyClient(internalCluster().getRandomNodeName(), response -> {
            assertEquals(200, response.status().code());
            responsesReceivedLatch.countDown();
        });
        try {
            var channel = client.connect().sync().channel();

            // send some HTTP GET requests before closing a channel
            for (int i = 0; i < nReq; i++) {
                channel.write(newHttpGetReq("/"));
                if (randomBoolean()) {
                    channel.flush();
                }
            }
            channel.flush();
            safeAwait(responsesReceivedLatch);

            // send close_notify alert and wait for channel closure
            var sslHandler = channel.pipeline().get(SslHandler.class);
            sslHandler.closeOutbound();
            try {
                assertTrue("server must close connection", channel.closeFuture().await(SAFE_AWAIT_TIMEOUT.millis()));
            } finally {
                channel.close().sync();
            }
        } finally {
            client.config().group().shutdownGracefully(0, 0, TimeUnit.SECONDS).sync();
        }
    }

    /**
     * Ensures that receiving close_notify will close connection and cancel running action.
     */
    public void testSendCloseNotifyCancelAction() throws Exception {
        var node = internalCluster().getRandomNodeName();
        var indexName = "close-notify-cancel";
        createIndex(indexName);
        ensureGreen(indexName);
        var gotResponse = new AtomicBoolean(false);
        var client = setupNettyClient(node, resp -> gotResponse.set(true));
        var actionName = ClusterStateAction.NAME;
        try (var capturingAction = CancellableActionTestPlugin.capturingActionOnNode(actionName, node)) {
            var channel = client.connect().sync().channel();
            var req = newHttpGetReq("/_cluster/state");
            channel.writeAndFlush(req);
            var ssl = channel.pipeline().get(SslHandler.class);
            capturingAction.captureAndCancel(ssl::closeOutbound);
            try {
                assertTrue("server must close connection", channel.closeFuture().await(SAFE_AWAIT_TIMEOUT.millis()));
                assertAllTasksHaveFinished(actionName);
                assertFalse("must cancel action before http response", gotResponse.get());
            } finally {
                channel.close().sync();
            }
        } finally {
            client.config().group().shutdownGracefully(0, 0, TimeUnit.SECONDS).sync();
        }
    }

    private DefaultFullHttpRequest newHttpGetReq(String uri) {
        var req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
        req.headers().add(HttpHeaderNames.AUTHORIZATION, basicAuthHeaderValue(nodeClientUsername(), nodeClientPassword()));
        return req;
    }

}
