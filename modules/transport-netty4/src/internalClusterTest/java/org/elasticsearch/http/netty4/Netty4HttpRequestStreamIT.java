/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

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

import org.elasticsearch.ESNetty4IntegTestCase;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class Netty4HttpRequestStreamIT extends ESNetty4IntegTestCase {

    // ensures content integrity, no loses
    public void testReceiveAllChunks() throws Exception {
        var sendBytes = randomByteArrayOfLength(1024 * 1024 * 10);
        var consumedBytes = new AtomicInteger();

        var latch = new CountDownLatch(1);
        var client = nettyClient(internalCluster().getRandomNodeName(), (resp -> {
            consumedBytes.set(Integer.parseInt(resp.content().toString(StandardCharsets.UTF_8)));
            latch.countDown();
        }));

        var req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, RequestContentStreamPlugin.ROUTE);
        req.headers().add(HttpHeaderNames.CONTENT_LENGTH, sendBytes.length);
        req.content().writeBytes(sendBytes);

        client.connect().await().channel().writeAndFlush(req).get();
        safeAwait(latch);
        assertEquals(sendBytes.length, consumedBytes.get());
        client.config().group().shutdownGracefully().get(10, TimeUnit.SECONDS);
    }

    void setChunkConsumer(Consumer<RestChannel> channelConsumer) {
        internalCluster().getInstances(RequestContentStreamPlugin.class).forEach(p -> { p.channelConsumer = channelConsumer; });
    }

    Bootstrap nettyClient(String node, Consumer<FullHttpResponse> responseHandler) throws Exception {
        var httpServer = internalCluster().getInstance(HttpServerTransport.class, node);
        var remoteAddr = randomFrom(httpServer.boundAddress().boundAddresses());
        return new Bootstrap().group(new NioEventLoopGroup(1))
            .channel(NioSocketChannel.class)
            .remoteAddress(remoteAddr.getAddress(), remoteAddr.getPort())
            .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) {
                    var p = ch.pipeline();
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

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.concatLists(List.of(RequestContentStreamPlugin.class), super.nodePlugins());
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    public static class RequestContentStreamPlugin extends Plugin implements ActionPlugin {

        static final String ROUTE = "/_test/request-stream/basic";
        Consumer<RestChannel> channelConsumer;

        @Override
        public Collection<RestHandler> getRestHandlers(
            Settings settings,
            NamedWriteableRegistry namedWriteableRegistry,
            RestController restController,
            ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings,
            SettingsFilter settingsFilter,
            IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<DiscoveryNodes> nodesInCluster,
            Predicate<NodeFeature> clusterSupportsFeature
        ) {
            return List.of(new BaseRestHandler() {
                @Override
                public String getName() {
                    return ROUTE;
                }

                @Override
                public List<Route> routes() {
                    return List.of(new Route(RestRequest.Method.POST, ROUTE));
                }

                @Override
                protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
                    return new RequestBodyChunkConsumer() {

                        int totalBytes = 0;

                        @Override
                        public void handleChunk(RestChannel channel, ReleasableBytesReference chunk, boolean isLast) {
                            try (chunk) {
                                totalBytes += chunk.length();
                                if (isLast == false) {
                                    request.contentStream().requestBytes(1024);
                                } else {
                                    channel.sendResponse(new RestResponse(RestStatus.OK, Integer.toString(totalBytes)));
                                }
                            }
                        }

                        @Override
                        public void accept(RestChannel channel) throws Exception {
                            request.contentStream().requestBytes(1024); // ask for first chunk
                        }
                    };
                }
            });
        }
    }

}
