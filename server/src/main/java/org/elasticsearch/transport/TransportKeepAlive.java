/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.AsyncBiFunction;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Implements the scheduling and sending of keep alive pings. Client channels send keep alive pings to the
 * server and server channels respond. Pings are only sent at the scheduled time if the channel did not send
 * and receive a message since the last ping.
 */
final class TransportKeepAlive implements Closeable {

    static final int PING_DATA_SIZE = -1;

    private static final BytesReference PING_MESSAGE;

    static {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeByte((byte) 'E');
            out.writeByte((byte) 'S');
            out.writeInt(PING_DATA_SIZE);
            PING_MESSAGE = out.copyBytes();
        } catch (IOException e) {
            throw new AssertionError(e.getMessage(), e); // won't happen
        }
    }

    private static final Logger logger = LogManager.getLogger(TransportKeepAlive.class);
    private final CounterMetric successfulPings = new CounterMetric();
    private final CounterMetric failedPings = new CounterMetric();
    private final ConcurrentMap<TimeValue, ScheduledPing> pingIntervals = ConcurrentCollections.newConcurrentMap();
    private final ThreadPool threadPool;
    private final AsyncBiFunction<TcpChannel, BytesReference, Void> pingSender;
    private volatile boolean isClosed;

    TransportKeepAlive(ThreadPool threadPool, AsyncBiFunction<TcpChannel, BytesReference, Void> pingSender) {
        this.threadPool = threadPool;
        this.pingSender = pingSender;
    }

    void registerNodeConnection(List<TcpChannel> nodeChannels, ConnectionProfile connectionProfile) {
        TimeValue pingInterval = connectionProfile.getPingInterval();
        if (pingInterval.millis() < 0) {
            return;
        }

        final ScheduledPing scheduledPing = pingIntervals.computeIfAbsent(pingInterval, ScheduledPing::new);
        scheduledPing.ensureStarted();

        for (TcpChannel channel : nodeChannels) {
            scheduledPing.addChannel(channel);
            channel.addCloseListener(ActionListener.running(() -> scheduledPing.removeChannel(channel)));
        }
    }

    /**
     * Called when a keep alive ping is received. If the channel that received the keep alive ping is a
     * server channel, a ping is sent back. If the channel that received the keep alive is a client channel,
     * this method does nothing as the client initiated the ping in the first place.
     *
     * @param channel that received the keep alive ping
     */
    void receiveKeepAlive(TcpChannel channel) {
        // The client-side initiates pings and the server-side responds. So if this is a client channel, this
        // method is a no-op.
        if (channel.isServerChannel()) {
            sendPing(channel);
        }
    }

    long successfulPingCount() {
        return successfulPings.count();
    }

    long failedPingCount() {
        return failedPings.count();
    }

    private void sendPing(TcpChannel channel) {
        pingSender.apply(channel, PING_MESSAGE, new ActionListener<Void>() {

            @Override
            public void onResponse(Void v) {
                successfulPings.inc();
            }

            @Override
            public void onFailure(Exception e) {
                if (channel.isOpen()) {
                    logger.debug(() -> "[" + channel + "] failed to send transport ping", e);
                    failedPings.inc();
                } else {
                    logger.trace(() -> "[" + channel + "] failed to send transport ping (channel closed)", e);
                }
            }
        });
    }

    @Override
    public void close() {
        isClosed = true;
    }

    private class ScheduledPing extends AbstractRunnable {

        private final TimeValue pingInterval;

        private final Set<TcpChannel> channels = ConcurrentCollections.newConcurrentSet();

        private final AtomicBoolean isStarted = new AtomicBoolean(false);
        private volatile long lastPingRelativeMillis;

        private ScheduledPing(TimeValue pingInterval) {
            this.pingInterval = pingInterval;
            this.lastPingRelativeMillis = threadPool.relativeTimeInMillis();
        }

        void ensureStarted() {
            if (isStarted.get() == false && isStarted.compareAndSet(false, true)) {
                threadPool.schedule(this, pingInterval, threadPool.generic());
            }
        }

        void addChannel(TcpChannel channel) {
            channels.add(channel);
        }

        void removeChannel(TcpChannel channel) {
            channels.remove(channel);
        }

        @Override
        protected void doRun() throws Exception {
            if (isClosed) {
                return;
            }

            for (TcpChannel channel : channels) {
                // In the future it is possible that we may want to kill a channel if we have not read from
                // the channel since the last ping. However, this will need to be backwards compatible with
                // pre-6.6 nodes that DO NOT respond to pings
                if (needsKeepAlivePing(channel)) {
                    sendPing(channel);
                }
            }
            this.lastPingRelativeMillis = threadPool.relativeTimeInMillis();
        }

        @Override
        public void onAfter() {
            if (isClosed) {
                return;
            }

            threadPool.scheduleUnlessShuttingDown(pingInterval, threadPool.generic(), this);
        }

        @Override
        public void onFailure(Exception e) {
            logger.warn("failed to send ping transport message", e);
        }

        private boolean needsKeepAlivePing(TcpChannel channel) {
            TcpChannel.ChannelStats stats = channel.getChannelStats();
            long accessedDelta = stats.lastAccessedTime() - lastPingRelativeMillis;
            return accessedDelta <= 0;
        }
    }
}
