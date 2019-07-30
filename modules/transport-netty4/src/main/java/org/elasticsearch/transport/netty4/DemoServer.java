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
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.codec.http.websocketx.extensions.compression.WebSocketServerCompressionHandler;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.CharsetUtil;
import org.elasticsearch.graphql.server.*;
import org.elasticsearch.rest.RestRequest;
import org.reactivestreams.Publisher;

import java.util.HashMap;
import java.util.Locale;
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

                        pipeline.addLast("request", new HttpRequestDecoder());
                        pipeline.addLast("response", new HttpResponseEncoder());
                        pipeline.addLast("aggregator", new HttpObjectAggregator(65536));

                        pipeline.addLast(new WebSocketServerCompressionHandler());
                        pipeline.addLast(new WebSocketServerProtocolHandler("/graphql", null, true));

                        pipeline.addLast("business-logic", new HttpHandler());
                        pipeline.addLast(new WebSocketFrameHandler());
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

    public class HttpHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) {
            ctx.flush();
        }

        @Override
        public void channelRead0(ChannelHandlerContext ctx, FullHttpRequest msg) {
            FullHttpRequest request = msg;
            DemoServerHttpRequest req = new DemoServerHttpRequest(request);
            DemoServerHttpResponse res = new DemoServerHttpResponse(request, ctx);

            for (DemoServerRoute route : router.getRoutes()) {
                if (compareMethods(route.method, request.method()) && route.path.equals(request.uri())) {
                    try {
                        route.handler.handle(req, res);
                    } catch (Exception e) {
                        res.sendJsonError("Internal server error.");

                        System.out.println("Handler error " + req.getPath() + " " + e);
                        e.printStackTrace();
                    }
                    return;
                }
            }

            res.send("Path not found.");
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            ctx.close();
        }
    }

    public class DemoServerHttpRequest implements DemoServerRequest {
        private FullHttpRequest request;

        public DemoServerHttpRequest(FullHttpRequest request) {
            this.request = request;
        }

        @Override
        public String getPath() {
            return request.uri();
        }

        @Override
        public String body() {
            return request.content().toString(CharsetUtil.UTF_8);
        }
    }

    public class DemoServerHttpResponse implements DemoServerResponse {
        private HttpRequest request;
        private ChannelHandlerContext ctx;
        private Map<String, String> headers = new HashMap<String, String>();
        private int status = 200;

        public DemoServerHttpResponse(HttpRequest request, ChannelHandlerContext ctx) {
            this.request = request;
            this.ctx = ctx;
        }

        @Override
        public void setStatus(int status) {
            this.status = status;
        }

        @Override
        public void setHeader(String key, String value) {
            headers.put(key, value);
        }

        @Override
        public void send(String contents) {
            ByteBuf content = Unpooled.copiedBuffer(contents, CharsetUtil.UTF_8);
            boolean keepAlive = HttpUtil.isKeepAlive(request);

            FullHttpResponse response = new DefaultFullHttpResponse(request.protocolVersion(), HttpResponseStatus.valueOf(status),
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

        @Override
        public void sendJsonError(String errorMessage) {
            setStatus(400);
            setHeader("Content-Type", "application/json");
            send("{\"error\": \"" + errorMessage + "\"}\n");
        }

        @Override
        public void sendHeadersChunk() {
            if (request.protocolVersion() != HttpVersion.HTTP_1_1) {
                send("Need to use HTTP 1.1 protocol for (Transfer-Encoding: chunked) streaming.");
                return;
            }

            boolean keepAlive = HttpUtil.isKeepAlive(request);
            HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);

            for (String key : headers.keySet()) {
                response.headers().set(key, headers.get(key));
            }

            response.headers()
                .set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8")
                .set(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);

            if (keepAlive) {
                if (!request.protocolVersion().isKeepAliveDefault()) {
                    response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
                }
            } else {
                response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
            }

            ctx.write(response);
        }


        @Override
        public void sendChunk(String chunk) {
            ByteBuf buf = Unpooled.copiedBuffer(chunk, CharsetUtil.UTF_8);
            HttpContent content = new DefaultHttpContent(buf);
            ctx.write(content);
        }

        @Override
        public void end() {
            boolean keepAlive = HttpUtil.isKeepAlive(request);
            ChannelFuture f = ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);

            if (!keepAlive) {
                f.addListener(ChannelFutureListener.CLOSE);
            }
        }
    }

    public class WebSocketFrameHandler extends SimpleChannelInboundHandler<WebSocketFrame> {
        ChannelHandlerContext ctx = null;

        SingleSink<String> incomingMessages = new SingleSink<String>();

        DemoServerSocket demoSocket = new DemoServerSocket() {
            @Override
            public Publisher<String> getIncomingMessages() {
                return incomingMessages;
            }

            @Override
            public void send(String message) {
                if (ctx == null) return;
                ctx.channel().writeAndFlush(new TextWebSocketFrame(message));
            }
        };

        @Override
        public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
            this.ctx = ctx;
            router.onSocket.next(demoSocket);
            ctx.fireChannelRegistered();
        }

        @Override
        public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
            this.ctx = null;
            incomingMessages.done();
            ctx.fireChannelUnregistered();
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, WebSocketFrame frame) throws Exception {
            if (frame instanceof TextWebSocketFrame) {
                String request = ((TextWebSocketFrame) frame).text();
                incomingMessages.next(request);
            } else {
                String message = "unsupported frame type: " + frame.getClass().getName();
                incomingMessages.error(new UnsupportedOperationException(message));
            }
        }
    }
}
