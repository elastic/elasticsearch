/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport.netty;

import org.elasticsearch.common.netty.bootstrap.ClientBootstrap;
import org.elasticsearch.common.netty.bootstrap.ServerBootstrap;
import org.elasticsearch.common.netty.buffer.ChannelBuffer;
import org.elasticsearch.common.netty.buffer.ChannelBuffers;
import org.elasticsearch.common.netty.channel.*;
import org.elasticsearch.common.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.elasticsearch.common.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.elasticsearch.common.netty.handler.ssl.SslHandler;
import org.elasticsearch.shield.ssl.SSLService;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.*;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import java.net.InetSocketAddress;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.*;

public class HandshakeWaitingHandlerTests extends ElasticsearchTestCase {
    private static final int CONCURRENT_CLIENT_REQUESTS = 20;

    private static ServerBootstrap serverBootstrap;
    private static ClientBootstrap clientBootstrap;
    private static SSLContext sslContext;
    private static int iterations;

    private final AtomicBoolean failed = new AtomicBoolean(false);
    private volatile Throwable failureCause = null;

    @BeforeClass
    public static void setup() throws Exception {
        SSLService sslService = new SSLService(settingsBuilder()
                .put("shield.ssl.keystore.path", Paths.get(HandshakeWaitingHandlerTests.class.getResource("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode.jks").toURI()))
                .put("shield.ssl.keystore.password", "testnode")
                .build());

        sslContext = sslService.getSslContext();

        ChannelFactory factory = new NioServerSocketChannelFactory(
                Executors.newFixedThreadPool(1),
                Executors.newFixedThreadPool(1));
        serverBootstrap = new ServerBootstrap(factory);

        ChannelFactory clientFactory = new NioClientSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool());
        clientBootstrap = new ClientBootstrap(clientFactory);

        iterations = randomIntBetween(10, 100);
    }

    @After
    public void reset() {
        failed.set(false);
        failureCause = null;
    }

    @Test
    public void testWriteBeforeHandshakeFailsWithoutHandler() throws Exception {
        serverBootstrap.setPipelineFactory(getServerFactory());
        final int randomPort = randomIntBetween(49000, 65500);
        serverBootstrap.bind(new InetSocketAddress("localhost", randomPort));
        clientBootstrap.setPipelineFactory(new ChannelPipelineFactory() {

            @Override
            public ChannelPipeline getPipeline() throws Exception {
                final SSLEngine engine = sslContext.createSSLEngine();
                engine.setUseClientMode(true);
                return Channels.pipeline(
                        new SslHandler(engine));
            }
        });

        ExecutorService threadPoolExecutor = Executors.newFixedThreadPool(CONCURRENT_CLIENT_REQUESTS);
        try {
            List<Callable<ChannelFuture>> callables = new ArrayList<>(CONCURRENT_CLIENT_REQUESTS);
            for (int i = 0; i < CONCURRENT_CLIENT_REQUESTS; i++) {
                callables.add(new WriteBeforeHandshakeCompletedCallable(clientBootstrap, randomPort));
            }

            for (int i = 0; i < iterations; i++) {
                List<Future<ChannelFuture>> futures = threadPoolExecutor.invokeAll(callables);
                for (Future<ChannelFuture> future : futures) {
                    ChannelFuture handshakeFuture = future.get();
                    handshakeFuture.await();
                    handshakeFuture.getChannel().close();
                }

                if (failed.get()) {
                    assertThat(failureCause, anyOf(instanceOf(SSLException.class), instanceOf(AssertionError.class)));
                    break;
                }
            }

            if (!failed.get()) {
                fail("Expected this test to fail with an SSLException or AssertionError");
            }
        } finally {
            threadPoolExecutor.shutdown();
        }
    }

    @Test
    public void testWriteBeforeHandshakePassesWithHandshakeWaitingHandler() throws Exception {
        serverBootstrap.setPipelineFactory(getServerFactory());
        final int randomPort = randomIntBetween(49000, 65500);
        serverBootstrap.bind(new InetSocketAddress("localhost", randomPort));
        clientBootstrap.setPipelineFactory(new ChannelPipelineFactory() {

            @Override
            public ChannelPipeline getPipeline() throws Exception {
                final SSLEngine engine = sslContext.createSSLEngine();
                engine.setUseClientMode(true);
                return Channels.pipeline(
                        new SslHandler(engine),
                        new HandshakeWaitingHandler());
            }
        });

        ExecutorService threadPoolExecutor = Executors.newFixedThreadPool(CONCURRENT_CLIENT_REQUESTS);
        try {
            List<Callable<ChannelFuture>> callables = new ArrayList<>(CONCURRENT_CLIENT_REQUESTS);
            for (int i = 0; i < CONCURRENT_CLIENT_REQUESTS; i++) {
                callables.add(new WriteBeforeHandshakeCompletedCallable(clientBootstrap, randomPort));
            }

            for (int i = 0; i < iterations; i++) {
                List<Future<ChannelFuture>> futures = threadPoolExecutor.invokeAll(callables);
                for (Future<ChannelFuture> future : futures) {
                    ChannelFuture handshakeFuture = future.get();
                    handshakeFuture.await();

                    // Wait for pending writes to prevent IOExceptions
                    Channel channel = handshakeFuture.getChannel();
                    HandshakeWaitingHandler handler = channel.getPipeline().get(HandshakeWaitingHandler.class);
                    while (handler != null && handler.hasPendingWrites()) {
                        Thread.sleep(10);
                    }

                    channel.close();
                }

                if (failed.get()) {
                    failureCause.printStackTrace();
                    fail("Expected this test to always pass with the HandshakeWaitingHandler in pipeline");
                }
            }

        } finally {
            threadPoolExecutor.shutdown();
        }
    }

    private ChannelPipelineFactory getServerFactory() {
        return new ChannelPipelineFactory() {
            public ChannelPipeline getPipeline() throws Exception {
                final SSLEngine sslEngine = sslContext.createSSLEngine();
                sslEngine.setUseClientMode(false);
                return Channels.pipeline(new SslHandler(sslEngine),
                    new SimpleChannelHandler() {

                        @Override
                        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
                            // Sink the message
                        }

                        @Override
                        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
                            Throwable cause = e.getCause();
                            // Only save first cause
                            if (failed.compareAndSet(false, true)) {
                                failureCause = cause;
                            }
                            ctx.getChannel().close();
                        }
                    });
            }
        };
    }

    @AfterClass
    public static void cleanUp() {
        clientBootstrap.shutdown();
        serverBootstrap.shutdown();
        clientBootstrap.releaseExternalResources();
        serverBootstrap.releaseExternalResources();
        clientBootstrap = null;
        serverBootstrap = null;
        sslContext = null;
    }

    private static class WriteBeforeHandshakeCompletedCallable implements Callable<ChannelFuture> {

        private final ClientBootstrap bootstrap;
        private final int port;

        WriteBeforeHandshakeCompletedCallable(ClientBootstrap bootstrap, int port) {
            this.bootstrap = bootstrap;
            this.port = port;
        }

        @Override
        public ChannelFuture call() throws Exception {
            ChannelBuffer buffer = ChannelBuffers.buffer(8);
            buffer.writeLong(SecureRandom.getInstance("SHA1PRNG").nextLong());

            // Connect and wait, then immediately start writing
            ChannelFuture future = bootstrap.connect(new InetSocketAddress("localhost", port));
            future.awaitUninterruptibly();
            Channel channel = future.getChannel();

            // Do not call handshake before writing as it will most likely succeed before a write begins
            // in the test
            ChannelFuture handshakeFuture = null;
            for (int i = 0; i < 100; i++) {
                channel.write(buffer);
                handshakeFuture = channel.getPipeline().get(SslHandler.class).handshake();
            }

            return handshakeFuture;
        }
    }
}
