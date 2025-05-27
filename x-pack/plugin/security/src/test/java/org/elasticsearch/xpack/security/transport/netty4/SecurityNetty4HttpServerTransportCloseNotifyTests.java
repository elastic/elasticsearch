/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.transport.netty4;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestEncoder;
import io.netty.handler.codec.http.HttpResponseDecoder;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.util.concurrent.Future;

import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.http.AbstractHttpServerTransportTestCase;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.http.netty4.Netty4HttpServerTransport;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.telemetry.tracing.Tracer;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.netty4.SharedGroupFactory;
import org.elasticsearch.transport.netty4.TLSConfig;
import org.elasticsearch.xpack.core.ssl.SSLService;

import java.security.cert.CertificateException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import javax.net.ssl.SSLException;

import static org.elasticsearch.test.SecuritySettingsSource.addSSLSettingsForNodePEMFiles;

public class SecurityNetty4HttpServerTransportCloseNotifyTests extends AbstractHttpServerTransportTestCase {

    private static <T> T safePoll(BlockingQueue<T> queue) {
        try {
            var t = queue.poll(5, TimeUnit.SECONDS);
            if (t == null) {
                throw new AssertionError("queue is empty");
            } else {
                return t;
            }
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private static <T> void safeAwait(Future<T> nettyFuture) {
        try {
            nettyFuture.get(5, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Setup {@link Netty4HttpServerTransport} with SSL enabled and self-signed certificate.
     * All HTTP requests accumulate in the dispatcher reqQueue.
     * The server will not reply to request automatically, to send response poll the queue.
     */
    private HttpServer setupHttpServer(String tlsProtocols) throws CertificateException {
        var threadPool = new TestThreadPool("tls-close-notify");
        var dispatcher = new QueuedDispatcher();
        final Settings.Builder builder = Settings.builder();
        addSSLSettingsForNodePEMFiles(builder, "xpack.security.http.", randomBoolean());
        var settings = builder.put("xpack.security.http.ssl.enabled", true)
            .put("path.home", createTempDir())
            .put("xpack.security.http.ssl.supported_protocols", tlsProtocols)
            .build();
        var env = TestEnvironment.newEnvironment(settings);
        var sslService = new SSLService(env);
        var server = new Netty4HttpServerTransport(
            settings,
            new NetworkService(Collections.emptyList()),
            threadPool,
            xContentRegistry(),
            dispatcher,
            randomClusterSettings(),
            new SharedGroupFactory(settings),
            Tracer.NOOP,
            new TLSConfig(sslService.getHttpTransportSSLConfiguration(), sslService::createSSLEngine),
            null,
            randomFrom((httpPreRequest, channel, listener) -> listener.onResponse(null), null)
        );
        server.start();
        return new HttpServer(server, dispatcher, threadPool);
    }

    /**
     * Set up a Netty HTTPs client and connect to server.
     * Configured with self-signed certificate trust.
     * Server responses accumulate in the respQueue, and exceptions in the errQueue.
     */
    private HttpClient setupHttpClient(HttpServer server) throws SSLException, InterruptedException {
        var clientSslCtx = SslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).build();
        var remoteAddr = randomFrom(server.netty.boundAddress().boundAddresses());
        var respQueue = new LinkedBlockingDeque<FullHttpResponse>();
        var errQueue = new LinkedBlockingDeque<Throwable>();
        var bootstrap = new Bootstrap().group(new NioEventLoopGroup(1))
            .channel(NioSocketChannel.class)
            .remoteAddress(remoteAddr.getAddress(), remoteAddr.getPort())
            .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) {
                    var p = ch.pipeline();
                    p.addLast(clientSslCtx.newHandler(ch.alloc()));
                    p.addLast(new HttpRequestEncoder());
                    p.addLast(new HttpResponseDecoder());
                    p.addLast(new HttpObjectAggregator(server.netty.handlingSettings.maxContentLength() * 2));
                    p.addLast(new SimpleChannelInboundHandler<FullHttpResponse>() {
                        @Override
                        protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse msg) {
                            respQueue.add(msg);
                        }

                        @Override
                        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                            errQueue.add(cause);
                        }
                    });
                }
            });
        var channel = bootstrap.connect().sync().channel();
        return new HttpClient(bootstrap, channel, respQueue, errQueue);
    }

    /**
     * Setup server and client, establish ssl connection, blocks until handshake is done
     */
    private ConnectionCtx connectClientAndServer(String tlsVersion) {
        try {
            var server = setupHttpServer(tlsVersion);
            var client = setupHttpClient(server);
            var ssl = client.channel.pipeline().get(SslHandler.class);
            safeAwait(ssl.handshakeFuture());
            assertEquals(tlsVersion, ssl.engine().getSession().getProtocol());
            return new ConnectionCtx(tlsVersion, server, client);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void runForAllTlsVersions(Consumer<String> test) {
        List.of("TLSv1.2", "TLSv1.3").forEach(test);
    }

    /**
     * This test ensures that sending close_notify from the client on the idle channel trigger close connection from the server.
     */
    public void testCloseIdleConnection() {
        runForAllTlsVersions(tlsVersion -> {
            try (var ctx = connectClientAndServer(tlsVersion)) {
                var ssl = ctx.client.channel.pipeline().get(SslHandler.class);
                ssl.closeOutbound();
                safeAwait(ctx.client.channel.closeFuture());
            }
        });
    }

    /**
     * This tests ensures that sending close_notify after HTTP response close the channel immediately.
     * It should be similar to idle test, but in this test we await http request and response.
     */
    public void testSendCloseNotifyAfterHttpResponse() {
        runForAllTlsVersions(tlsVersion -> {
            try (var ctx = connectClientAndServer(tlsVersion)) {
                ctx.client.channel.writeAndFlush(new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/index"));
                var serverRequestCtx = safePoll(ctx.server.dispatcher.reqQueue);
                serverRequestCtx.restChannel.sendResponse(new RestResponse(RestStatus.OK, ""));
                safePoll(ctx.client.respQueue);
                var ssl = ctx.client.channel.pipeline().get(SslHandler.class);
                ssl.closeOutbound();
                safeAwait(ctx.client.channel.closeFuture());
            }
        });
    }

    /**
     * This test ensures that sending close_notify with outstanding requests close channel immediately.
     */
    public void testSendCloseNotifyBeforeHttpResponse() {
        runForAllTlsVersions(tlsVersion -> {
            try (var ctx = connectClientAndServer(tlsVersion)) {
                var server = ctx.server;
                var client = ctx.client;

                var nRequests = randomIntBetween(1, 5);
                for (int i = 0; i < nRequests; i++) {
                    client.channel.writeAndFlush(new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/index"));
                }
                assertBusy(() -> assertEquals(nRequests, server.dispatcher.reqQueue.size()));

                // after the server receives requests send close_notify, before server responses
                var ssl = client.channel.pipeline().get(SslHandler.class);
                ssl.closeOutbound();

                safeAwait(ctx.client.channel.closeFuture());
                assertTrue(client.errQueue.isEmpty());
                assertTrue(client.respQueue.isEmpty());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

    }

    private record HttpServer(Netty4HttpServerTransport netty, QueuedDispatcher dispatcher, ThreadPool threadPool) {}

    private record HttpClient(
        Bootstrap netty,
        Channel channel,
        BlockingDeque<FullHttpResponse> respQueue,
        BlockingDeque<Throwable> errQueue
    ) {}

    private record ConnectionCtx(String tlsProtocol, HttpServer server, HttpClient client) implements AutoCloseable {

        @Override
        public void close() {
            // need to release not consumed requests, will complain about buffer leaks after GC
            server.dispatcher.reqQueue.forEach(r -> r.request.getHttpRequest().release());
            server.netty.stop();
            server.threadPool.shutdownNow();
            safeAwait(client.netty.config().group().shutdownGracefully(0, 0, TimeUnit.SECONDS));
        }
    }

    private static class QueuedDispatcher implements HttpServerTransport.Dispatcher {
        BlockingQueue<ReqCtx> reqQueue = new LinkedBlockingDeque<>();
        BlockingDeque<ErrCtx> errQueue = new LinkedBlockingDeque<>();

        @Override
        public void dispatchRequest(RestRequest request, RestChannel channel, ThreadContext threadContext) {
            reqQueue.add(new ReqCtx(request, channel, threadContext));
        }

        @Override
        public void dispatchBadRequest(RestChannel channel, ThreadContext threadContext, Throwable cause) {
            errQueue.add(new ErrCtx(channel, threadContext, cause));
        }

        record ReqCtx(RestRequest request, RestChannel restChannel, ThreadContext threadContext) {}

        record ErrCtx(RestChannel restChannel, ThreadContext threadContext, Throwable cause) {}
    }

}
