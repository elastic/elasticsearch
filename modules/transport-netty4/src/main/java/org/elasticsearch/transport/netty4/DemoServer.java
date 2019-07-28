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

package org.elasticsearch.transport.netty4;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.CharsetUtil;
import org.elasticsearch.graphql.server.*;
import org.elasticsearch.rest.RestRequest;

import java.util.HashMap;
import java.util.Map;

public class DemoServer {
    private DemoServerRouter router;

    private static int PORT = 9100;

    public DemoServer(DemoServerRouter router) throws Exception {
        this.router = router;

        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            ServerBootstrap bootstrap = new ServerBootstrap()
                .option(ChannelOption.SO_BACKLOG, 1024)
                .group(bossGroup, workerGroup)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();

                        pipeline.addLast("http", new HttpServerCodec());
                        pipeline.addLast("continue-100", new HttpServerExpectContinueHandler());
                        pipeline.addLast("business-logic", new HttpHandler());
                    }
                })
                .channel(NioServerSocketChannel.class);

            Channel ch = bootstrap.bind(PORT).sync().channel();
            ch.closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    public static boolean compareMethods(RestRequest.Method method1, HttpMethod method2) {
        if ((method1 == RestRequest.Method.GET) && (method2 == HttpMethod.GET)) {
            return true;
        } else if ((method1 == RestRequest.Method.POST) && (method2 == HttpMethod.POST)) {
            return true;
        } else {
            return false;
        }
    }

    public class HttpHandler extends SimpleChannelInboundHandler<HttpObject> {
        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) {
            ctx.flush();
        }

        @Override
        public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
            if (msg instanceof HttpRequest) {
                HttpRequest request = (HttpRequest) msg;
                DemoServerHttpRequest req = new DemoServerHttpRequest(request);
                DemoServerHttpResponse res = new DemoServerHttpResponse(request, ctx);

                for (DemoServerRoute route : router.getRoutes()) {
                    if (compareMethods(route.method, request.method()) && route.path.equals(request.uri())) {
                        route.handler.handle(req, res);
                        return;
                    }
                }

                res.send("Path not found.");
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            ctx.close();
        }
    }

    public class DemoServerHttpRequest implements DemoServerRequest {
        private HttpRequest request;

        public DemoServerHttpRequest(HttpRequest request) {
            this.request = request;
        }

        @Override
        public String getPath() {
            return request.uri();
        }
    }

    public class DemoServerHttpResponse implements DemoServerResponse {
        private HttpRequest request;
        private ChannelHandlerContext ctx;
        private Map<String, String> headers = new HashMap<String, String>();

        public DemoServerHttpResponse(HttpRequest request, ChannelHandlerContext ctx) {
            this.request = request;
            this.ctx = ctx;
        }

        public void addHeader(String key, String value) {
            headers.put(key, value);
        }

        public void send(String contents) {
            ByteBuf content = Unpooled.copiedBuffer(contents, CharsetUtil.UTF_8);
            boolean keepAlive = HttpUtil.isKeepAlive(request);

            FullHttpResponse response = new DefaultFullHttpResponse(request.protocolVersion(), HttpResponseStatus.OK,
                Unpooled.wrappedBuffer(content));
            response.headers()
                .setInt(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());

            for (String key : headers.keySet()) {
                response.headers().set(key, headers.get(key));
            }

            if (keepAlive) {
                if (!request.protocolVersion().isKeepAliveDefault()) {
                    response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
                }
            } else {
                response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
            }

            ChannelFuture f = ctx.write(response);

            if (!keepAlive) {
                f.addListener(ChannelFutureListener.CLOSE);
            }
        }
    }
}
