/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http.netty4;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleUserEventChannelHandler;
import io.netty.handler.ssl.SslClientHelloHandler;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.netty.util.ReferenceCounted;
import io.netty.util.concurrent.Future;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.transport.NodeDisconnectedException;
import org.elasticsearch.transport.netty4.Netty4Plugin;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

/**
 * Allows to limit the number of in-flight TLS handshakes for inbound HTTPS connections processed by each event loop, protecting against a
 * thundering herd of fresh connections.
 */
class TlsHandshakeThrottleManager extends AbstractLifecycleComponent {

    private static final Logger logger = LogManager.getLogger(TlsHandshakeThrottleManager.class);

    private final ClusterSettings clusterSettings;
    private final MeterRegistry meterRegistry;
    private final List<AutoCloseable> metricsToClose = new ArrayList<>(3);
    volatile int maxInProgressTlsHandshakes;
    volatile int maxDelayedTlsHandshakes;

    private final Map<Thread, TlsHandshakeThrottle> tlsHandshakeThrottles = ConcurrentCollections.newConcurrentMap();

    TlsHandshakeThrottleManager(ClusterSettings clusterSettings, MeterRegistry meterRegistry) {
        this.clusterSettings = clusterSettings;
        this.meterRegistry = meterRegistry;
    }

    @SuppressWarnings("unchecked")
    private static <T> Setting<T> getRegisteredInstance(ClusterSettings clusterSettings, Setting<?> setting) {
        // wtf Netty4Plugin ends up loaded twice in different classloaders, so we have to look up the setting instances by name
        return (Setting<T>) Objects.requireNonNull(clusterSettings.get(setting.getKey()));
    }

    @Override
    protected void doStart() {
        clusterSettings.<Integer>initializeAndWatch(
            getRegisteredInstance(clusterSettings, Netty4Plugin.SETTING_HTTP_NETTY_TLS_HANDSHAKES_MAX_IN_PROGRESS),
            maxInProgressTlsHandshakes -> this.maxInProgressTlsHandshakes = maxInProgressTlsHandshakes
        );
        clusterSettings.<Integer>initializeAndWatch(
            getRegisteredInstance(clusterSettings, Netty4Plugin.SETTING_HTTP_NETTY_TLS_HANDSHAKES_MAX_DELAYED),
            maxDelayedTlsHandshakes -> this.maxDelayedTlsHandshakes = maxDelayedTlsHandshakes
        );

        metricsToClose.add(
            meterRegistry.registerLongGauge(
                "es.http.tls_handshakes.in_progress.current",
                "current number of in-progress TLS handshakes for HTTP connections",
                "count",
                getMetric(TlsHandshakeThrottle::getInProgressHandshakesCount)
            )
        );
        metricsToClose.add(
            meterRegistry.registerLongGauge(
                "es.http.tls_handshakes.delayed.current",
                "current number of delayed TLS handshakes for HTTP connections",
                "count",
                getMetric(TlsHandshakeThrottle::getCurrentDelayedHandshakesCount)
            )
        );
        metricsToClose.add(
            meterRegistry.registerLongAsyncCounter(
                "es.http.tls_handshakes.delayed.total",
                "total number of TLS handshakes for HTTP connections that were delayed due to throttling",
                "count",
                getMetric(TlsHandshakeThrottle::getTotalDelayedHandshakesCount)
            )
        );
        metricsToClose.add(
            meterRegistry.registerLongAsyncCounter(
                "es.http.tls_handshakes.dropped.total",
                "number of TLS handshakes for HTTP connections dropped due to throttling",
                "count",
                getMetric(TlsHandshakeThrottle::getDroppedHandshakesCount)
            )
        );
    }

    @Override
    protected void doStop() {
        tlsHandshakeThrottles.values().forEach(TlsHandshakeThrottle::close);
        for (var metricToClose : metricsToClose) {
            try {
                metricToClose.close();
            } catch (Exception e) {
                assert false : e;
                logger.error(Strings.format("exception closing metric [%s]", metricToClose), e);
            }
        }
    }

    @Override
    protected void doClose() {}

    private Supplier<LongWithAttributes> getMetric(ToLongFunction<TlsHandshakeThrottle> metricFunction) {
        return () -> {
            long result = 0L;
            for (var tlsHandshakeThrottle : tlsHandshakeThrottles.values()) {
                result += metricFunction.applyAsLong(tlsHandshakeThrottle);
            }
            return new LongWithAttributes(result);
        };
    }

    @Nullable // if throttling disabled
    TlsHandshakeThrottle getThrottleForCurrentThread() {
        synchronized (lifecycle) {
            if (lifecycle.stoppedOrClosed()) {
                throw new IllegalStateException("HTTP transport is already stopped");
            }
            if (maxInProgressTlsHandshakes == 0) {
                return null;
            }
            return tlsHandshakeThrottles.computeIfAbsent(Thread.currentThread(), ignored -> new TlsHandshakeThrottle());
        }
    }

    /**
     * A throttle on TLS handshakes for incoming HTTP connections for a single event loop thread.
     */
    class TlsHandshakeThrottle {

        // volatile for metrics
        private volatile int inProgressHandshakesCount = 0;

        // actions to run (or fail) to release a throttled handshake
        private final ArrayDeque<AbstractRunnable> delayedHandshakes = new ArrayDeque<>();

        // delayedHandshakes.size() but tracked separately for metrics
        private volatile int delayedHandshakesCount = 0;

        // for metrics
        private volatile long totalDelayedHandshakesCount = 0;
        private volatile long droppedHandshakesCount = 0;

        private AbstractRunnable takeFirstDelayedHandshake() {
            final var result = delayedHandshakes.removeFirst();
            delayedHandshakesCount = delayedHandshakes.size();
            return result;
        }

        private AbstractRunnable takeLastDelayedHandshake() {
            final var result = delayedHandshakes.removeLast();
            delayedHandshakesCount = delayedHandshakes.size();
            return result;
        }

        private void addDelayedHandshake(AbstractRunnable abstractRunnable) {
            delayedHandshakes.addFirst(abstractRunnable);
            // noinspection NonAtomicOperationOnVolatileField all writes are on this thread
            totalDelayedHandshakesCount += 1;
            delayedHandshakesCount = delayedHandshakes.size();
        }

        void close() {
            while (delayedHandshakes.isEmpty() == false) {
                takeFirstDelayedHandshake().onFailure(new NodeClosedException((DiscoveryNode) null));
            }
        }

        ChannelHandler newHandshakeThrottleHandler(SubscribableListener<Void> handshakeCompletePromise) {
            return new HandshakeThrottleHandler(handshakeCompletePromise);
        }

        ChannelHandler newHandshakeCompletionWatcher(SubscribableListener<Void> handshakeCompletePromise) {
            return new HandshakeCompletionWatcher(handshakeCompletePromise);
        }

        void handleHandshakeCompletion() {
            if (delayedHandshakes.isEmpty()) {
                // noinspection NonAtomicOperationOnVolatileField all writes are on this thread
                inProgressHandshakesCount -= 1;
            } else {
                takeFirstDelayedHandshake().run();
            }
        }

        public int getInProgressHandshakesCount() {
            return inProgressHandshakesCount;
        }

        public int getCurrentDelayedHandshakesCount() {
            return delayedHandshakesCount;
        }

        public long getTotalDelayedHandshakesCount() {
            return totalDelayedHandshakesCount;
        }

        public long getDroppedHandshakesCount() {
            return droppedHandshakesCount;
        }

        /**
         * A Netty pipeline handler that aggregates inbound messages until it receives a full TLS {@code ClientHello} and then either
         * passes all the received messages on down the pipeline (if not throttled) or else delays that work until another TLS handshake
         * completes (if too many such handshakes are already in flight).
         */
        private class HandshakeThrottleHandler extends SslClientHelloHandler<Void> {

            /**
             * Promise which accumulates the messages received until we receive a full handshake. Completed when we receive a full
             * handshake, at which point all the delayed messages are pushed down the pipeline for actual processing.
             */
            private final SubscribableListener<Void> handshakeStartedPromise = new SubscribableListener<>();

            /**
             * Promise which will be completed by the channel's matching {@link HandshakeCompletionWatcher} when the handshake we sent down
             * the pipeline has completed.
             */
            private final SubscribableListener<Void> handshakeCompletePromise;

            HandshakeThrottleHandler(SubscribableListener<Void> handshakeCompletePromise) {
                this.handshakeCompletePromise = handshakeCompletePromise;
            }

            @Override
            public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                if (msg instanceof ReferenceCounted referenceCounted) {
                    referenceCounted.retain();
                }
                handshakeStartedPromise.addListener(new ActionListener<>() {
                    @Override
                    public void onResponse(Void unused) {
                        ctx.fireChannelRead(msg);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        if (msg instanceof ReferenceCounted referenceCounted) {
                            referenceCounted.release();
                        }
                    }
                });
                super.channelRead(ctx, msg);
            }

            @Override
            public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
                handshakeStartedPromise.addListener(new ActionListener<>() {
                    @Override
                    public void onResponse(Void unused) {
                        ctx.fireChannelReadComplete();
                    }

                    @Override
                    public void onFailure(Exception e) {}
                });
                super.channelReadComplete(ctx);
            }

            @Override
            public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                handshakeStartedPromise.onFailure(
                    new NodeDisconnectedException(null, "connection closed before handshake started", null, null)
                );
                super.channelInactive(ctx);
            }

            @Override
            protected Future<Void> lookup(ChannelHandlerContext ctx, ByteBuf clientHello) {
                if (clientHello == null) {
                    logger.debug("lookup with no ClientHello, closing [{}]", ctx.channel());
                    ctx.channel().close();
                    final var exception = new IllegalArgumentException(
                        "did not receive initial ClientHello on channel [" + ctx.channel() + "]"
                    );
                    handshakeStartedPromise.onFailure(exception);
                    return ctx.executor().newFailedFuture(exception);
                }

                if (ctx.channel().isActive() == false) {
                    logger.debug("lookup after channel inactive, ignoring [{}]", ctx.channel());
                    final var exception = new NodeDisconnectedException(
                        null,
                        "lookup after channel inactive [" + ctx.channel() + "]",
                        null,
                        null
                    );
                    handshakeStartedPromise.onFailure(exception);
                    return ctx.executor().newFailedFuture(exception);
                }

                final var maxInProgressTlsHandshakes = TlsHandshakeThrottleManager.this.maxInProgressTlsHandshakes; // single volatile read
                if (maxInProgressTlsHandshakes == 0 || inProgressHandshakesCount < maxInProgressTlsHandshakes) {
                    // noinspection NonAtomicOperationOnVolatileField all writes are on this thread
                    inProgressHandshakesCount += 1;
                    handshakeCompletePromise.addListener(ActionListener.running(TlsHandshakeThrottle.this::handleHandshakeCompletion));
                    ctx.channel().pipeline().remove(HandshakeThrottleHandler.this);
                    handshakeStartedPromise.onResponse(null);
                } else {
                    logger.debug(
                        "[{}] in-progress TLS handshakes already, enqueueing new handshake on [{}]",
                        inProgressHandshakesCount,
                        ctx.channel()
                    );
                    addDelayedHandshake(new AbstractRunnable() {
                        @Override
                        public void onFailure(Exception e) {
                            logger.debug(
                                "[{}] in-progress and [{}] delayed TLS handshakes, cancelling handshake on [{}]: {}",
                                inProgressHandshakesCount,
                                delayedHandshakes.size(),
                                ctx.channel(),
                                e.getMessage()
                            );
                            ctx.channel().close();
                        }

                        @Override
                        protected void doRun() {
                            logger.debug(
                                "[{}] in flight and [{}] delayed TLS handshakes, processing delayed handshake on [{}]",
                                inProgressHandshakesCount,
                                delayedHandshakes.size(),
                                ctx.channel()
                            );
                            handshakeCompletePromise.addListener(
                                ActionListener.running(TlsHandshakeThrottle.this::handleHandshakeCompletion)
                            );
                            ctx.pipeline().remove(HandshakeThrottleHandler.this);
                            handshakeStartedPromise.onResponse(null);
                        }

                        @Override
                        public String toString() {
                            return "delayed handshake on [" + ctx.channel() + "]";
                        }
                    });

                    final var maxDelayedTlsHandshakes = TlsHandshakeThrottleManager.this.maxDelayedTlsHandshakes; // single volatile read
                    while (delayedHandshakes.size() > maxDelayedTlsHandshakes) {
                        final var lastDelayedHandshake = takeLastDelayedHandshake();
                        // noinspection NonAtomicOperationOnVolatileField all writes are on this thread
                        droppedHandshakesCount += 1;
                        lastDelayedHandshake.onFailure(new ElasticsearchException("too many in-flight TLS handshakes"));
                    }
                }

                ctx.read(); // auto-read is disabled but we must watch for client-close
                return ctx.executor().newSucceededFuture(null);
            }

            @Override
            protected void onLookupComplete(ChannelHandlerContext ctx, Future<Void> future) {}
        }

        /**
         * A Netty pipeline handler that watches for a user event indicating a TLS handshake completed (or else the channel closed). On
         * completion of a handshake, this handler removes itself from the pipeline and completes a promise which will in turn call
         * {@link #handleHandshakeCompletion} to trigger either the processing of another handshake or a decrement of
         * {@link #inProgressHandshakesCount}.
         */
        private static class HandshakeCompletionWatcher extends SimpleUserEventChannelHandler<SslHandshakeCompletionEvent> {
            private final SubscribableListener<Void> handshakeCompletePromise;

            HandshakeCompletionWatcher(SubscribableListener<Void> handshakeCompletePromise) {
                this.handshakeCompletePromise = handshakeCompletePromise;
            }

            @Override
            protected void eventReceived(ChannelHandlerContext ctx, SslHandshakeCompletionEvent evt) {
                ctx.pipeline().remove(HandshakeCompletionWatcher.this);
                if (evt.isSuccess()) {
                    handshakeCompletePromise.onResponse(null);
                } else {
                    ExceptionsHelper.maybeDieOnAnotherThread(evt.cause());
                    handshakeCompletePromise.onFailure(
                        evt.cause() instanceof Exception exception
                            ? exception
                            : new ElasticsearchException("TLS handshake failed", evt.cause())
                    );
                }
            }

            @Override
            public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                if (handshakeCompletePromise.isDone() == false) {
                    handshakeCompletePromise.onFailure(new ElasticsearchException("channel closed before TLS handshake completed"));
                }
                super.channelInactive(ctx);
            }
        }
    }
}
