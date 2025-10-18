/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.transport.netty4;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleUserEventChannelHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.RunOnce;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.http.netty4.Netty4HttpServerTransport;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.MetricRecorder;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.metric.Instrument;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.telemetry.tracing.Tracer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.NodeDisconnectedException;
import org.elasticsearch.transport.netty4.Netty4Plugin;
import org.elasticsearch.transport.netty4.SharedGroupFactory;
import org.elasticsearch.transport.netty4.TLSConfig;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.hamcrest.Matchers;

import java.nio.channels.ClosedChannelException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.elasticsearch.telemetry.InstrumentType.LONG_ASYNC_COUNTER;
import static org.elasticsearch.telemetry.InstrumentType.LONG_GAUGE;
import static org.elasticsearch.test.SecuritySettingsSource.addSSLSettingsForNodePEMFiles;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class SecurityNetty4HttpServerTransportTlsHandshakeThrottleTests extends ESTestCase {

    /**
     * Represents a handshake that has passed the throttle and is in progress on the server side. It is blocked by this test fixture until
     * the {@link #unblock()} method is called.
     * <p>
     * The server transport exposes these to the tests via the {@code handshakeBlockQueue} queue.
     */
    private static class HandshakeBlock {

        private final ActionListener<Void> innerPromise;
        private final String threadName = Thread.currentThread().getName();
        private final Channel channel;

        HandshakeBlock(ActionListener<Void> innerPromise, Channel channel) {
            this.innerPromise = innerPromise;
            this.channel = channel;
        }

        void unblock() {
            // complete innerPromise on the event loop to ensure that the blocked read and readComplete events are unblocked in order:
            // if completed on the caller thread then a concurrent addListener call may invoke its listener before the waiting ones
            channel.eventLoop().execute(() -> innerPromise.onResponse(null));
        }

        @Override
        public String toString() {
            return "handshake block promise on " + threadName + " for " + channel;
        }
    }

    /**
     * Set up a {@link Netty4HttpServerTransport} with SSL enabled (using a self-signed certificate) and some extra handlers in the pipeline
     * around the {@link io.netty.handler.ssl.SslHandler} to enable testing. The first handler, just before the
     * {@link io.netty.handler.ssl.SslHandler}, watches for (and blocks) the initial messages that make up a TLS handshake after it has
     * passed the throttle mechanism. At this point it increments the count of concurrent handshakes (asserting that it is within bounds),
     * and adds an entry to the {@code handshakeBlockQueue} allowing the queue's consumer to release the blocked messages. The second
     * handler, just after the {@link io.netty.handler.ssl.SslHandler}, watches for the {@link SslHandshakeCompletionEvent} to decrement the
     * count of concurrent handshakes.
     */
    private Netty4HttpServerTransport createServerTransport(
        ThreadPool threadPool,
        SharedGroupFactory sharedGroupFactory,
        int maxConcurrentTlsHandshakes,
        int maxDelayedTlsHandshakes,
        Queue<HandshakeBlock> handshakeBlockQueue,
        MeterRegistry meterRegistry
    ) {
        final var dynamicConfiguration = randomBoolean();

        final Settings.Builder builder = Settings.builder();
        addSSLSettingsForNodePEMFiles(builder, "xpack.security.http.", randomBoolean());
        final var settings = builder.put("xpack.security.http.ssl.enabled", true)
            .put("path.home", createTempDir())
            .put(
                Netty4Plugin.SETTING_HTTP_NETTY_TLS_HANDSHAKES_MAX_IN_PROGRESS.getKey(),
                dynamicConfiguration ? between(0, 5) : maxConcurrentTlsHandshakes
            )
            .put(
                Netty4Plugin.SETTING_HTTP_NETTY_TLS_HANDSHAKES_MAX_DELAYED.getKey(),
                dynamicConfiguration ? between(0, 5) : maxDelayedTlsHandshakes
            )
            .build();
        final var env = TestEnvironment.newEnvironment(settings);
        final var sslService = new SSLService(env);
        final var tlsConfig = new TLSConfig(sslService.profile(XPackSettings.HTTP_SSL_PREFIX)::engine);

        final var inflightHandshakesByEventLoop = ConcurrentCollections.<Thread, AtomicInteger>newConcurrentMap();

        final List<Setting<?>> settingsSet = new ArrayList<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        settingsSet.add(Netty4Plugin.SETTING_HTTP_NETTY_TLS_HANDSHAKES_MAX_IN_PROGRESS);
        settingsSet.add(Netty4Plugin.SETTING_HTTP_NETTY_TLS_HANDSHAKES_MAX_DELAYED);
        final var clusterSettings = new ClusterSettings(settings, Set.copyOf(settingsSet));

        final var telemetryProvider = new TelemetryProvider() {
            @Override
            public Tracer getTracer() {
                return Tracer.NOOP;
            }

            @Override
            public MeterRegistry getMeterRegistry() {
                return meterRegistry;
            }
        };

        final var server = new Netty4HttpServerTransport(
            settings,
            new NetworkService(Collections.emptyList()),
            threadPool,
            xContentRegistry(),
            NEVER_CALLED_DISPATCHER,
            clusterSettings,
            sharedGroupFactory,
            telemetryProvider,
            tlsConfig,
            null,
            null
        ) {
            @Override
            public ChannelHandler configureServerChannelHandler() {
                return new HttpChannelHandler(this, handlingSettings, tlsConfig, null, null) {
                    @Override
                    protected void initChannel(Channel ch) throws Exception {
                        super.initChannel(ch);

                        final var workerThread = Thread.currentThread();
                        final var handshakeCounter = inflightHandshakesByEventLoop.computeIfAbsent(
                            workerThread,
                            ignored -> new AtomicInteger()
                        );
                        final var handshakeCompletePromise = new SubscribableListener<>();
                        final var handshakeBlockPromise = new SubscribableListener<Void>();

                        final var handshakeStartRecorder = new RunOnce(() -> {
                            logger.info("--> handshake start detected on [{}]", ch);
                            assertThat(handshakeCounter.incrementAndGet(), Matchers.lessThanOrEqualTo(maxConcurrentTlsHandshakes));
                            handshakeCompletePromise.addListener(ActionListener.running(handshakeCounter::decrementAndGet));
                        });

                        final var handshakeBlock = new HandshakeBlock(handshakeBlockPromise, ch);
                        final var handshakeBlockPromiseEnqueuer = new RunOnce(() -> handshakeBlockQueue.add(handshakeBlock));

                        ch.pipeline().addBefore("ssl", "handshake-start-detector", new ChannelInboundHandlerAdapter() {
                            @Override
                            public void channelRead(ChannelHandlerContext ctx, Object msg) {
                                handshakeStartRecorder.run();
                                assertSame(workerThread, Thread.currentThread());
                                // ownership transfer of msg to handshakeBlockPromise - no refcounting needed
                                handshakeBlockPromise.addListener(ActionListener.running(() -> {
                                    assertTrue(ctx.executor().inEventLoop());
                                    ctx.fireChannelRead(msg);
                                }));
                                handshakeBlockPromiseEnqueuer.run();
                            }

                            @Override
                            public void channelReadComplete(ChannelHandlerContext ctx) {
                                handshakeBlockPromise.addListener(ActionListener.running(() -> {
                                    assertTrue(ctx.executor().inEventLoop());
                                    ctx.fireChannelReadComplete();
                                }));
                            }

                            @Override
                            public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                                if (handshakeBlockPromise.isDone() == false) {
                                    handshakeBlockPromise.onFailure(new NodeDisconnectedException(null, "channel inactive", null, null));
                                }
                                super.channelInactive(ctx);
                            }
                        }).addAfter("ssl", "handshake-complete-detector", new SimpleUserEventChannelHandler<SslHandshakeCompletionEvent>() {
                            @Override
                            protected void eventReceived(ChannelHandlerContext ctx, SslHandshakeCompletionEvent evt) {
                                handshakeCompletePromise.onResponse(null);
                                ctx.fireUserEventTriggered(evt);
                            }

                            @Override
                            public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                                handshakeCompletePromise.onResponse(null);
                                super.channelInactive(ctx);
                            }
                        });
                    }
                };
            }
        };
        server.start();

        if (dynamicConfiguration) {
            clusterSettings.applySettings(
                Settings.builder()
                    .put(Netty4Plugin.SETTING_HTTP_NETTY_TLS_HANDSHAKES_MAX_IN_PROGRESS.getKey(), maxConcurrentTlsHandshakes)
                    .put(Netty4Plugin.SETTING_HTTP_NETTY_TLS_HANDSHAKES_MAX_DELAYED.getKey(), maxDelayedTlsHandshakes)
                    .build()
            );
        }

        return server;
    }

    private static SslContext newClientSslContext() {
        try {
            return SslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).build();
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private static HandshakeBlock getNextBlock(BlockingQueue<HandshakeBlock> handshakeBlockQueue) {
        try {
            return Objects.requireNonNull(
                handshakeBlockQueue.poll(SAFE_AWAIT_TIMEOUT.seconds(), TimeUnit.SECONDS),
                "timed out waiting for handshake block"
            );
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    /**
     * This test ensures that we permit up to the max number of concurrent handshakes without any throttling
     */
    public void testNoThrottling() {
        final List<Releasable> releasables = new ArrayList<>();
        try {
            // connection-to-event-loop assignment is round-robin so this will never hit the limit
            final var eventLoopCount = between(1, 5);
            final var maxConcurrentTlsHandshakes = between(1, 5);
            final var clientCount = between(1, eventLoopCount * maxConcurrentTlsHandshakes);
            final var maxDelayedTlsHandshakes = between(0, 100);

            final var threadPool = newThreadPool(releasables);
            final var sharedGroupFactory = new SharedGroupFactory(
                Settings.builder().put(Netty4Plugin.WORKER_COUNT.getKey(), eventLoopCount).build()
            );
            final var handshakeBlockQueue = ConcurrentCollections.<HandshakeBlock>newBlockingQueue();

            final var meterRegistry = new RecordingMeterRegistry();
            final var metricRecorder = meterRegistry.getRecorder();

            final var serverTransport = createServerTransport(
                threadPool,
                sharedGroupFactory,
                maxConcurrentTlsHandshakes,
                maxDelayedTlsHandshakes,
                handshakeBlockQueue,
                meterRegistry
            );
            releasables.add(serverTransport);

            final var handshakeCompletePromises = startClientsAndGetHandshakeCompletePromises(
                clientCount,
                randomFrom(serverTransport.boundAddress().boundAddresses()),
                releasables
            );

            final var handshakeBlocks = IntStream.range(0, clientCount).mapToObj(ignored -> getNextBlock(handshakeBlockQueue)).toList();

            logger.info("--> all handshakes blocked");

            metricRecorder.collect();
            assertLongMetric(metricRecorder, LONG_GAUGE, CURRENT_IN_PROGRESS_METRIC, clientCount);
            assertLongMetric(metricRecorder, LONG_GAUGE, CURRENT_DELAYED_METRIC, 0);
            assertLongMetric(metricRecorder, LONG_ASYNC_COUNTER, TOTAL_DELAYED_METRIC, 0);
            assertLongMetric(metricRecorder, LONG_ASYNC_COUNTER, TOTAL_DROPPED_METRIC, 0);
            metricRecorder.resetCalls();

            handshakeBlocks.forEach(HandshakeBlock::unblock);
            handshakeCompletePromises.forEach(ESTestCase::safeAwait);

            assertFinalStats(metricRecorder, 0, 0);
        } catch (Exception e) {
            throw new AssertionError(e);
        } finally {
            Collections.reverse(releasables);
            Releasables.close(releasables);
        }
    }

    /**
     * This test ensures that if we send more than the permitted number of TLS handshakes at once then the excess are throttled
     */
    public void testThrottleConcurrentHandshakes() {
        final List<Releasable> releasables = new ArrayList<>();
        try {
            // connection-to-event-loop assignment is round-robin so this will always use all available slots before queueing
            final var eventLoopCount = between(1, 5);
            final var maxConcurrentTlsHandshakes = between(1, 5);
            final var expectedDelayedHandshakes = between(1, 3 * eventLoopCount);
            final var clientCount = eventLoopCount * maxConcurrentTlsHandshakes + expectedDelayedHandshakes;
            final var maxDelayedTlsHandshakes = clientCount + between(0, 100);

            final var threadPool = newThreadPool(releasables);
            final var sharedGroupFactory = new SharedGroupFactory(
                Settings.builder().put(Netty4Plugin.WORKER_COUNT.getKey(), eventLoopCount).build()
            );
            final var handshakeBlockQueue = ConcurrentCollections.<HandshakeBlock>newBlockingQueue();

            final var meterRegistry = new RecordingMeterRegistry();
            final var metricRecorder = meterRegistry.getRecorder();

            final var serverTransport = createServerTransport(
                threadPool,
                sharedGroupFactory,
                maxConcurrentTlsHandshakes,
                maxDelayedTlsHandshakes,
                handshakeBlockQueue,
                meterRegistry
            );
            releasables.add(serverTransport);

            final var serverAddress = randomFrom(serverTransport.boundAddress().boundAddresses());

            final var handshakeCompletePromises = startClientsAndGetHandshakeCompletePromises(clientCount, serverAddress, releasables);

            final var handshakeBlocks = new ArrayDeque<HandshakeBlock>();
            for (int i = 0; i < eventLoopCount * maxConcurrentTlsHandshakes; i++) {
                handshakeBlocks.addLast(getNextBlock(handshakeBlockQueue));
            }
            assertNull(handshakeBlockQueue.poll()); // this is key: all the handshakes beyond the limit are delayed
            logger.info("--> max number of handshakes received & blocked");

            awaitLongMetric(metricRecorder, LONG_ASYNC_COUNTER, TOTAL_DELAYED_METRIC, expectedDelayedHandshakes);
            logger.info("--> expected number of delayed handshakes observed");

            metricRecorder.collect();
            assertLongMetric(metricRecorder, LONG_GAUGE, CURRENT_IN_PROGRESS_METRIC, eventLoopCount * maxConcurrentTlsHandshakes);
            assertLongMetric(metricRecorder, LONG_GAUGE, CURRENT_DELAYED_METRIC, expectedDelayedHandshakes);
            assertLongMetric(metricRecorder, LONG_ASYNC_COUNTER, TOTAL_DELAYED_METRIC, expectedDelayedHandshakes);
            assertLongMetric(metricRecorder, LONG_ASYNC_COUNTER, TOTAL_DROPPED_METRIC, 0);
            metricRecorder.resetCalls();

            while (handshakeBlocks.isEmpty() == false) {
                handshakeBlocks.removeFirst().unblock();
            }

            for (int i = 0; i < expectedDelayedHandshakes; i++) {
                getNextBlock(handshakeBlockQueue).unblock();
            }
            assertNull(handshakeBlockQueue.poll());

            logger.info("--> all handshakes unblocked");

            handshakeCompletePromises.forEach(ESTestCase::safeAwait);
            assertFinalStats(metricRecorder, expectedDelayedHandshakes, 0);
        } catch (Exception e) {
            throw new AssertionError(e);
        } finally {
            Collections.reverse(releasables);
            Releasables.close(releasables);
        }
    }

    public void testProcessThrottledHandshakesInLifoOrder() {
        final List<Releasable> releasables = new ArrayList<>();
        try {
            final var eventLoopCount = 1; // no concurrency
            final var expectedDelayedHandshakes = 2;

            final var threadPool = newThreadPool(releasables);
            final var sharedGroupFactory = new SharedGroupFactory(
                Settings.builder().put(Netty4Plugin.WORKER_COUNT.getKey(), eventLoopCount).build()
            );
            final var handshakeBlockQueue = ConcurrentCollections.<HandshakeBlock>newBlockingQueue();

            final var meterRegistry = new RecordingMeterRegistry();
            final var metricRecorder = meterRegistry.getRecorder();

            final var serverTransport = createServerTransport(
                threadPool,
                sharedGroupFactory,
                1,
                expectedDelayedHandshakes,
                handshakeBlockQueue,
                meterRegistry
            );
            releasables.add(serverTransport);

            final var clientEventLoop = newClientEventLoop(releasables);
            final var sslContext = newClientSslContext();
            final var serverAddress = randomFrom(serverTransport.boundAddress().boundAddresses());

            class TestClient {
                final SubscribableListener<Void> handshakeListener = new SubscribableListener<>();
                final Bootstrap bootstrap = new Bootstrap().group(clientEventLoop)
                    .channel(NioSocketChannel.class)
                    .remoteAddress(serverAddress.getAddress(), serverAddress.getPort())
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            final var sslHandler = sslContext.newHandler(ch.alloc());
                            ch.pipeline().addLast(sslHandler);
                            sslHandler.handshakeFuture().addListener(fut -> {
                                assertTrue(fut.isSuccess());
                                handshakeListener.onResponse(null);
                            });
                        }
                    });

                TestClient() {
                    final var connectFuture = bootstrap.connect();
                    releasables.add(() -> connectFuture.syncUninterruptibly().channel().close().syncUninterruptibly());
                }
            }

            final var client0 = new TestClient();
            awaitLongMetric(metricRecorder, LONG_GAUGE, CURRENT_IN_PROGRESS_METRIC, 1);

            final var client1 = new TestClient();
            awaitLongMetric(metricRecorder, LONG_GAUGE, CURRENT_DELAYED_METRIC, 1);

            final var client2 = new TestClient();
            awaitLongMetric(metricRecorder, LONG_GAUGE, CURRENT_DELAYED_METRIC, 2);

            logger.info("--> all handshakes blocked/delayed, unblocking client0");

            getNextBlock(handshakeBlockQueue).unblock();
            safeAwait(client0.handshakeListener);
            assertFalse(client1.handshakeListener.isDone());
            assertFalse(client2.handshakeListener.isDone());

            logger.info("--> client0 handshake complete, unblocking client2");

            // unblocking the client0 handshake releases the one received last, i.e. from client2
            getNextBlock(handshakeBlockQueue).unblock();
            safeAwait(client2.handshakeListener);
            assertFalse(client1.handshakeListener.isDone());

            getNextBlock(handshakeBlockQueue).unblock();
            safeAwait(client1.handshakeListener);

            assertNull(handshakeBlockQueue.poll());

            assertFinalStats(metricRecorder, 2, 0);
        } catch (Exception e) {
            throw new AssertionError(e);
        } finally {
            Collections.reverse(releasables);
            Releasables.close(releasables);
        }
    }

    public void testDiscardExcessiveConcurrentHandshakes() {
        final List<Releasable> releasables = new ArrayList<>();
        try {
            // connection-to-event-loop assignment is round-robin so this will always use all available slots before rejecting
            final var eventLoopCount = between(1, 5);
            final var maxConcurrentTlsHandshakes = between(1, 5);
            final var maxDelayedTlsHandshakes = between(1, 5);
            final var excessiveHandshakes = between(eventLoopCount, 5); // at least eventLoopCount to ensure max delayed everywhere
            final var clientCount = eventLoopCount * (maxConcurrentTlsHandshakes + maxDelayedTlsHandshakes) + excessiveHandshakes;

            final var threadPool = newThreadPool(releasables);
            final var sharedGroupFactory = new SharedGroupFactory(
                Settings.builder().put(Netty4Plugin.WORKER_COUNT.getKey(), eventLoopCount).build()
            );
            final var handshakeBlockQueue = ConcurrentCollections.<HandshakeBlock>newBlockingQueue();

            final var meterRegistry = new RecordingMeterRegistry();
            final var metricRecorder = meterRegistry.getRecorder();

            final var serverTransport = createServerTransport(
                threadPool,
                sharedGroupFactory,
                maxConcurrentTlsHandshakes,
                maxDelayedTlsHandshakes,
                handshakeBlockQueue,
                meterRegistry
            );
            releasables.add(serverTransport);

            final var handshakeCompletePromises = startClientsAndGetHandshakeCompletePromises(
                clientCount,
                randomFrom(serverTransport.boundAddress().boundAddresses()),
                releasables
            );

            final var failedHandshakesLatch = new CountDownLatch(excessiveHandshakes);
            final var exceptionCount = new AtomicInteger();
            handshakeCompletePromises.forEach(handshakeCompletePromise -> handshakeCompletePromise.addListener(new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {}

                @Override
                public void onFailure(Exception e) {
                    assertThat(exceptionCount.incrementAndGet(), lessThanOrEqualTo(excessiveHandshakes));
                    assertThat(e, Matchers.instanceOf(NodeDisconnectedException.class));
                    assertThat(e.getMessage(), equalTo("disconnected before handshake complete"));
                    failedHandshakesLatch.countDown();
                }
            }));

            safeAwait(failedHandshakesLatch);
            logger.info("--> excessive handshakes cancelled");

            metricRecorder.collect();
            assertLongMetric(metricRecorder, LONG_GAUGE, CURRENT_IN_PROGRESS_METRIC, eventLoopCount * maxConcurrentTlsHandshakes);
            assertLongMetric(metricRecorder, LONG_GAUGE, CURRENT_DELAYED_METRIC, eventLoopCount * maxDelayedTlsHandshakes);
            assertLongMetric(
                metricRecorder,
                LONG_ASYNC_COUNTER,
                TOTAL_DELAYED_METRIC,
                eventLoopCount * maxDelayedTlsHandshakes + excessiveHandshakes
            );
            assertLongMetric(metricRecorder, LONG_ASYNC_COUNTER, TOTAL_DROPPED_METRIC, excessiveHandshakes);
            metricRecorder.resetCalls();

            for (int i = 0; i < eventLoopCount * (maxConcurrentTlsHandshakes + maxDelayedTlsHandshakes); i++) {
                getNextBlock(handshakeBlockQueue).unblock();
            }
            logger.info("--> all handshakes released");

            final var completeLatch = new CountDownLatch(handshakeCompletePromises.size());
            handshakeCompletePromises.forEach(
                handshakeCompletePromise -> handshakeCompletePromise.addListener(ActionListener.running(completeLatch::countDown))
            );
            safeAwait(completeLatch);
            logger.info("--> all handshakes completed");

            assertFinalStats(metricRecorder, eventLoopCount * maxDelayedTlsHandshakes + excessiveHandshakes, excessiveHandshakes);

        } catch (Exception e) {
            throw new AssertionError(e);
        } finally {
            Collections.reverse(releasables);
            Releasables.close(releasables);
        }
    }

    public void testThrottleHandlersOmittedWhenDisabled() {
        final List<Releasable> releasables = new ArrayList<>();
        try {
            final var threadPool = newThreadPool(releasables);
            final var sharedGroupFactory = new SharedGroupFactory(Settings.builder().put(Netty4Plugin.WORKER_COUNT.getKey(), 1).build());
            final var handlersObservedQueue = ConcurrentCollections.<Boolean>newBlockingQueue();

            final Settings.Builder builder = Settings.builder();
            addSSLSettingsForNodePEMFiles(builder, "xpack.security.http.", randomBoolean());
            final var settings = builder.put("xpack.security.http.ssl.enabled", true)
                .put("path.home", createTempDir())
                .put(Netty4Plugin.SETTING_HTTP_NETTY_TLS_HANDSHAKES_MAX_IN_PROGRESS.getKey(), between(0, 1))
                .build();
            final var env = TestEnvironment.newEnvironment(settings);
            final var sslService = new SSLService(env);
            final var tlsConfig = new TLSConfig(sslService.profile(XPackSettings.HTTP_SSL_PREFIX)::engine);

            final List<Setting<?>> settingsSet = new ArrayList<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
            settingsSet.add(Netty4Plugin.SETTING_HTTP_NETTY_TLS_HANDSHAKES_MAX_IN_PROGRESS);
            settingsSet.add(Netty4Plugin.SETTING_HTTP_NETTY_TLS_HANDSHAKES_MAX_DELAYED);
            final var clusterSettings = new ClusterSettings(settings, Set.copyOf(settingsSet));

            final var serverTransport = new Netty4HttpServerTransport(
                settings,
                new NetworkService(Collections.emptyList()),
                threadPool,
                xContentRegistry(),
                NEVER_CALLED_DISPATCHER,
                clusterSettings,
                sharedGroupFactory,
                TelemetryProvider.NOOP,
                tlsConfig,
                null,
                null
            ) {
                @Override
                public ChannelHandler configureServerChannelHandler() {
                    return new HttpChannelHandler(this, handlingSettings, tlsConfig, null, null) {
                        @Override
                        protected void initChannel(Channel ch) throws Exception {
                            super.initChannel(ch);
                            final var hasInitialThrottleHandler = ch.pipeline().get("initial-tls-handshake-throttle") != null;
                            final var hasCompletionHandler = ch.pipeline().get("initial-tls-handshake-completion-watcher") != null;
                            assertEquals(hasInitialThrottleHandler, hasCompletionHandler);
                            assertTrue(handlersObservedQueue.offer(hasInitialThrottleHandler && hasCompletionHandler));
                        }
                    };
                }
            };
            serverTransport.start();
            releasables.add(serverTransport);

            final var clientEventLoop = newClientEventLoop(releasables);
            final var sslContext = newClientSslContext();
            final var serverAddress = randomFrom(serverTransport.boundAddress().boundAddresses());

            for (final var throttleConfigured : new Boolean[] { null, Boolean.TRUE, Boolean.FALSE }) {

                logger.info("--> throttleConfigured: {}", throttleConfigured);

                final boolean throttleEnabled;
                if (throttleConfigured == null) {
                    throttleEnabled = settings.getAsInt(Netty4Plugin.SETTING_HTTP_NETTY_TLS_HANDSHAKES_MAX_IN_PROGRESS.getKey(), null) > 0;
                } else {
                    final var settingsBuilder = Settings.builder();
                    settingsBuilder.put(
                        Netty4Plugin.SETTING_HTTP_NETTY_TLS_HANDSHAKES_MAX_IN_PROGRESS.getKey(),
                        throttleConfigured ? between(1, 1000) : 0
                    );
                    throttleEnabled = throttleConfigured;
                    clusterSettings.applySettings(settingsBuilder.build());
                }

                final var connectFuture = new Bootstrap().group(clientEventLoop)
                    .channel(NioSocketChannel.class)
                    .remoteAddress(serverAddress.getAddress(), serverAddress.getPort())
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            final var sslHandler = sslContext.newHandler(ch.alloc());
                            ch.pipeline().addLast(sslHandler);
                        }
                    })
                    .connect();
                releasables.add(() -> connectFuture.syncUninterruptibly().channel().close().syncUninterruptibly());

                assertEquals(throttleEnabled, handlersObservedQueue.poll(SAFE_AWAIT_TIMEOUT.millis(), TimeUnit.MILLISECONDS));
            }

        } catch (Exception e) {
            throw new AssertionError(e);
        } finally {
            Collections.reverse(releasables);
            Releasables.close(releasables);
        }
    }

    public void testIgnoreIncompleteHandshakes() {
        final List<Releasable> releasables = new ArrayList<>();
        try {
            final var eventLoopCount = between(1, 5);
            final var maxConcurrentTlsHandshakes = between(1, 5);
            final var maxDelayedTlsHandshakes = between(1, 5);
            final var clientCount = between(1, eventLoopCount * (maxConcurrentTlsHandshakes + maxDelayedTlsHandshakes) + between(1, 5));

            final var threadPool = newThreadPool(releasables);
            final var sharedGroupFactory = new SharedGroupFactory(
                Settings.builder().put(Netty4Plugin.WORKER_COUNT.getKey(), eventLoopCount).build()
            );
            final var handshakeBlockQueue = ConcurrentCollections.<HandshakeBlock>newBlockingQueue();

            final var meterRegistry = new RecordingMeterRegistry();
            final var metricRecorder = meterRegistry.getRecorder();

            final var serverTransport = createServerTransport(
                threadPool,
                sharedGroupFactory,
                maxConcurrentTlsHandshakes,
                maxDelayedTlsHandshakes,
                handshakeBlockQueue,
                meterRegistry
            );
            releasables.add(serverTransport);

            final var clientEventLoop = newClientEventLoop(releasables);
            final var sslContext = newClientSslContext();
            final var serverAddress = randomFrom(serverTransport.boundAddress().boundAddresses());
            final var completeLatch = new CountDownLatch(clientCount);

            for (int i = 0; i < clientCount; i++) {
                final var bootstrap = new Bootstrap().group(clientEventLoop)
                    .channel(NioSocketChannel.class)
                    .remoteAddress(serverAddress.getAddress(), serverAddress.getPort())
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            final var sslHandler = sslContext.newHandler(ch.alloc());
                            ch.pipeline().addLast(new ChannelOutboundHandlerAdapter() {
                                boolean truncating;

                                @Override
                                public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
                                    if (truncating) {
                                        return;
                                    }

                                    truncating = true;
                                    final var msgByteBuf = (ByteBuf) msg;
                                    final var truncatedMsg = msgByteBuf.slice(0, between(1, msgByteBuf.readableBytes() - 1));
                                    ctx.executor()
                                        .execute(
                                            () -> ctx.writeAndFlush(truncatedMsg, promise)
                                                .addListener(f -> ctx.close().addListener(ff -> completeLatch.countDown()))
                                        );
                                }
                            }).addLast(sslHandler);
                        }
                    });
                final var connectFuture = bootstrap.connect();
                releasables.add(() -> connectFuture.syncUninterruptibly().channel().close().syncUninterruptibly());
            }

            metricRecorder.collect();
            assertLongMetric(metricRecorder, LONG_GAUGE, CURRENT_IN_PROGRESS_METRIC, 0);
            assertLongMetric(metricRecorder, LONG_GAUGE, CURRENT_DELAYED_METRIC, 0);
            assertLongMetric(metricRecorder, LONG_ASYNC_COUNTER, TOTAL_DELAYED_METRIC, 0);
            assertLongMetric(metricRecorder, LONG_ASYNC_COUNTER, TOTAL_DROPPED_METRIC, 0);
            metricRecorder.resetCalls();

            safeAwait(completeLatch);

            metricRecorder.collect();
            assertLongMetric(metricRecorder, LONG_GAUGE, CURRENT_IN_PROGRESS_METRIC, 0);
            assertLongMetric(metricRecorder, LONG_GAUGE, CURRENT_DELAYED_METRIC, 0);
            assertLongMetric(metricRecorder, LONG_ASYNC_COUNTER, TOTAL_DELAYED_METRIC, 0);
            assertLongMetric(metricRecorder, LONG_ASYNC_COUNTER, TOTAL_DROPPED_METRIC, 0);
            metricRecorder.resetCalls();

            assertTrue(handshakeBlockQueue.isEmpty());

        } catch (Exception e) {
            throw new AssertionError(e);
        } finally {
            Collections.reverse(releasables);
            Releasables.close(releasables);
        }
    }

    public void testCleanUpOnEarlyClientClose() {
        final List<Releasable> releasables = new ArrayList<>();
        try {
            final var eventLoopCount = between(1, 5);
            final var maxConcurrentTlsHandshakes = between(1, 5);
            final var maxDelayedTlsHandshakes = between(1, 5);
            final var clientCount = between(1, eventLoopCount * (maxConcurrentTlsHandshakes + maxDelayedTlsHandshakes) + between(1, 5));

            final var threadPool = newThreadPool(releasables);
            final var sharedGroupFactory = new SharedGroupFactory(
                Settings.builder().put(Netty4Plugin.WORKER_COUNT.getKey(), eventLoopCount).build()
            );
            final var handshakeBlockQueue = ConcurrentCollections.<HandshakeBlock>newBlockingQueue();

            final var meterRegistry = new RecordingMeterRegistry();
            final var metricRecorder = meterRegistry.getRecorder();

            final var serverTransport = createServerTransport(
                threadPool,
                sharedGroupFactory,
                maxConcurrentTlsHandshakes,
                maxDelayedTlsHandshakes,
                handshakeBlockQueue,
                meterRegistry
            );
            releasables.add(serverTransport);

            final var clientEventLoop = newClientEventLoop(releasables);
            final var sslContext = newClientSslContext();
            final var serverAddress = randomFrom(serverTransport.boundAddress().boundAddresses());
            final var completeLatch = new CountDownLatch(clientCount);

            for (int i = 0; i < clientCount; i++) {
                final var bootstrap = new Bootstrap().group(clientEventLoop)
                    .channel(NioSocketChannel.class)
                    .remoteAddress(serverAddress.getAddress(), serverAddress.getPort())
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            final var sslHandler = sslContext.newHandler(ch.alloc());
                            ch.pipeline().addLast(new ChannelOutboundHandlerAdapter() {
                                @Override
                                public void flush(ChannelHandlerContext ctx) throws Exception {
                                    super.flush(ctx);
                                    ctx.close().addListener(ff -> completeLatch.countDown());
                                }
                            }).addLast(sslHandler);
                        }
                    });
                final var connectFuture = bootstrap.connect();
                releasables.add(() -> connectFuture.syncUninterruptibly().channel().close().syncUninterruptibly());
            }

            safeAwait(completeLatch);
            logger.info("--> completeLatch released");

            assertTrue(waitUntil(() -> serverTransport.stats().getTotalOpen() == clientCount));
            logger.info("--> all connections opened");

            assertTrue(waitUntil(() -> serverTransport.stats().getServerOpen() == 0));
            logger.info("--> all connections closed");

            getNextBlock(handshakeBlockQueue);
            logger.info("--> at least one handshake started");

            awaitLongMetric(metricRecorder, LONG_GAUGE, CURRENT_IN_PROGRESS_METRIC, 0);
            logger.info("--> CURRENT_IN_FLIGHT_METRIC reached zero");

            metricRecorder.collect();
            assertLongMetric(metricRecorder, LONG_GAUGE, CURRENT_IN_PROGRESS_METRIC, 0);
            assertLongMetric(metricRecorder, LONG_GAUGE, CURRENT_DELAYED_METRIC, 0);
            metricRecorder.resetCalls();

        } catch (Exception e) {
            throw new AssertionError(e);
        } finally {
            Collections.reverse(releasables);
            Releasables.close(releasables);
        }
    }

    private List<SubscribableListener<Void>> startClientsAndGetHandshakeCompletePromises(
        int clientCount,
        TransportAddress serverAddress,
        List<Releasable> releasables
    ) {
        final var clientEventLoop = newClientEventLoop(releasables);
        final var sslContext = newClientSslContext();
        final List<SubscribableListener<Void>> handshakeCompletePromises = new ArrayList<>(clientCount);
        for (int i = 0; i < clientCount; i++) {
            final var handshakeCompletePromise = new SubscribableListener<Void>();
            handshakeCompletePromises.add(handshakeCompletePromise);
            final var bootstrap = new Bootstrap().group(clientEventLoop)
                .channel(NioSocketChannel.class)
                .remoteAddress(serverAddress.getAddress(), serverAddress.getPort())
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        final var sslHandler = sslContext.newHandler(ch.alloc());
                        ch.pipeline().addLast(sslHandler);
                        sslHandler.handshakeFuture().addListener(future -> {
                            if (future.isSuccess()) {
                                handshakeCompletePromise.onResponse(null);
                            } else {
                                ExceptionsHelper.maybeDieOnAnotherThread(future.cause());
                                if (future.cause() instanceof ClosedChannelException closedChannelException) {
                                    handshakeCompletePromise.onFailure(
                                        new NodeDisconnectedException(
                                            null,
                                            "disconnected before handshake complete",
                                            null,
                                            closedChannelException
                                        )
                                    );
                                } else {
                                    handshakeCompletePromise.onFailure(new ElasticsearchException("handshake failed", future.cause()));
                                }
                            }
                        });

                        ch.closeFuture()
                            .addListener(
                                ignored -> handshakeCompletePromise.onFailure(
                                    new NodeDisconnectedException(null, "disconnected before handshake complete", null, null)
                                )
                            );

                        handshakeCompletePromise.addListener(ActionListener.running(ch::close));
                    }
                });
            final var connectFuture = bootstrap.connect();
            releasables.add(() -> connectFuture.syncUninterruptibly().channel().close().syncUninterruptibly());
        }
        return List.copyOf(handshakeCompletePromises);
    }

    private static EventLoopGroup newClientEventLoop(List<Releasable> releasables) {
        final var clientEventLoop = new NioEventLoopGroup(1);
        releasables.add(() -> ThreadPool.terminate(clientEventLoop, SAFE_AWAIT_TIMEOUT.millis(), TimeUnit.MILLISECONDS));
        return clientEventLoop;
    }

    private ThreadPool newThreadPool(List<Releasable> releasables) {
        final var threadPool = new TestThreadPool(getTestName());
        releasables.add(() -> ThreadPool.terminate(threadPool, SAFE_AWAIT_TIMEOUT.seconds(), TimeUnit.SECONDS));
        return threadPool;
    }

    private static void assertFinalStats(MetricRecorder<Instrument> metricRecorder, int expectedTotalDelayed, int expectedTotalDropped) {
        // clients may get handshake completion before server, so we have to busy-wait here before checking final metrics:
        awaitLongMetric(metricRecorder, LONG_GAUGE, CURRENT_IN_PROGRESS_METRIC, 0);
        metricRecorder.collect();
        assertLongMetric(metricRecorder, LONG_GAUGE, CURRENT_IN_PROGRESS_METRIC, 0);
        assertLongMetric(metricRecorder, LONG_GAUGE, CURRENT_DELAYED_METRIC, 0);
        assertLongMetric(metricRecorder, LONG_ASYNC_COUNTER, TOTAL_DELAYED_METRIC, expectedTotalDelayed);
        assertLongMetric(metricRecorder, LONG_ASYNC_COUNTER, TOTAL_DROPPED_METRIC, expectedTotalDropped);
        metricRecorder.resetCalls();
    }

    private static void assertLongMetric(
        MetricRecorder<Instrument> metricRecorder,
        InstrumentType instrumentType,
        String name,
        int expectedValue
    ) {
        assertEquals(
            name,
            List.of((long) expectedValue),
            metricRecorder.getMeasurements(instrumentType, name).stream().map(Measurement::getLong).toList()
        );
    }

    private static final Logger logger = LogManager.getLogger(SecurityNetty4HttpServerTransportTlsHandshakeThrottleTests.class);

    private static void awaitLongMetric(
        MetricRecorder<Instrument> metricRecorder,
        InstrumentType instrumentType,
        String name,
        int expectedValue
    ) {
        final var expectedMeasurements = List.of((long) expectedValue);
        assertTrue(waitUntil(() -> {
            metricRecorder.collect();
            final var measurements = metricRecorder.getMeasurements(instrumentType, name).stream().map(Measurement::getLong).toList();
            metricRecorder.resetCalls();
            logger.info("--> awaitLongMetric[{}/{}] got {}", instrumentType, name, measurements);
            assertThat(measurements, hasSize(1));
            return measurements.equals(expectedMeasurements);
        }));
    }

    private static final HttpServerTransport.Dispatcher NEVER_CALLED_DISPATCHER = new HttpServerTransport.Dispatcher() {
        @Override
        public void dispatchRequest(RestRequest request, RestChannel channel, ThreadContext threadContext) {
            fail("dispatchRequest should never be called");
        }

        @Override
        public void dispatchBadRequest(RestChannel channel, ThreadContext threadContext, Throwable cause) {
            fail("dispatchBadRequest should never be called");
        }
    };

    private static final String METRIC_PREFIX = "es.http.tls_handshakes.";
    private static final String CURRENT_IN_PROGRESS_METRIC = METRIC_PREFIX + "in_progress.current";
    private static final String CURRENT_DELAYED_METRIC = METRIC_PREFIX + "delayed.current";
    private static final String TOTAL_DELAYED_METRIC = METRIC_PREFIX + "delayed.total";
    private static final String TOTAL_DROPPED_METRIC = METRIC_PREFIX + "dropped.total";
}
