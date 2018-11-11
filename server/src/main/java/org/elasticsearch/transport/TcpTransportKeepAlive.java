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
package org.elasticsearch.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractLifecycleRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class TcpTransportKeepAlive implements Closeable {

    private static final int PING_DATA_SIZE = -1;

    private final Logger logger = LogManager.getLogger(TcpTransportKeepAlive.class);
    private final CounterMetric successfulPings = new CounterMetric();
    private final CounterMetric failedPings = new CounterMetric();
    private final ConcurrentMap<TimeValue, ScheduledPing> pingIntervals = ConcurrentCollections.newConcurrentMap();
    private final ConcurrentMap<TcpChannel, KeepAliveStats> channelStats = ConcurrentCollections.newConcurrentMap();
    private final Lifecycle lifecycle = new Lifecycle();
    private final ThreadPool threadPool;
    private final PingSender pingSender;
    private final BytesReference pingMessage;

    public TcpTransportKeepAlive(ThreadPool threadPool, PingSender pingSender) {
        this.threadPool = threadPool;
        this.pingSender = pingSender;

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeByte((byte) 'E');
            out.writeByte((byte) 'S');
            out.writeInt(PING_DATA_SIZE);
            pingMessage = out.bytes();
        } catch (IOException e) {
            throw new AssertionError(e.getMessage(), e); // won't happen
        }
    }

    // TODO: NEED BACKWARDS COMPATIBILITY. AT LEAST IF WE ADD TIMEOUTS

    public void registerNodeConnection(List<TcpChannel> nodeChannels, ConnectionProfile connectionProfile) {
        TimeValue pingInterval = connectionProfile.getPingInterval();
        if (pingInterval.millis() < 0) {
            return;
        }

        final ScheduledPing scheduledPing = pingIntervals.computeIfAbsent(pingInterval, ScheduledPing::new);
        scheduledPing.ensureStarted();


        long currentTime = System.nanoTime();
        for (TcpChannel channel : nodeChannels) {
            scheduledPing.addChannel(channel);
            channelStats.put(channel, new KeepAliveStats(currentTime));

            channel.addCloseListener(ActionListener.wrap(() -> {
                scheduledPing.removeChannel(channel);
                channelStats.remove(channel);
            }));
        }
    }

    public void receiveKeepAlive(TcpChannel channel) {
        boolean isClient = channel.isClient();
        KeepAliveStats keepAliveStats = channelStats.get(channel);

        if (isClient) {
            if (keepAliveStats != null) {
                keepAliveStats.lastReadTime = System.nanoTime();
            }

        } else {
            sendPing(channel);
            if (keepAliveStats != null) {
                keepAliveStats.lastWriteTime = System.nanoTime();
            }
        }
    }

    public void receiveNonKeepAlive(TcpChannel channel) {
        KeepAliveStats keepAliveStats = channelStats.get(channel);
        if (keepAliveStats != null) {
            keepAliveStats.lastReadTime = System.nanoTime();
        }
    }

    private void sendPing(TcpChannel channel) {
        pingSender.send(channel, pingMessage, new ActionListener<Void>() {

            @Override
            public void onResponse(Void v) {
                successfulPings.inc();
            }

            @Override
            public void onFailure(Exception e) {
                if (channel.isOpen()) {
                    logger.debug(() -> new ParameterizedMessage("[{}] failed to send transport ping", channel), e);
                    failedPings.inc();
                } else {
                    logger.trace(() -> new ParameterizedMessage("[{}] failed to send transport ping (channel closed)", channel), e);
                }
            }
        });
    }

    private void updateKeepAlive(TcpChannel channel, boolean read, boolean write) {
        KeepAliveStats keepAliveStats = channelStats.get(channel);
        if (keepAliveStats != null) {
            if (read) {
                keepAliveStats.lastReadTime = System.nanoTime();
            }
            if (write) {
                keepAliveStats.lastWriteTime = System.nanoTime();
            }
        }
    }

    @Override
    public void close() {
        lifecycle.moveToStopped();
        lifecycle.moveToClosed();
    }

    private class KeepAliveStats {

        private long lastReadTime;
        private long lastWriteTime;

        private KeepAliveStats(long currentTime) {
            lastReadTime = currentTime;
            lastWriteTime = currentTime;
        }
    }

    private class ScheduledPing extends AbstractLifecycleRunnable {

        private final TimeValue pingInterval;
        private final Set<TcpChannel> channels = ConcurrentCollections.newConcurrentSet();
        private final AtomicBoolean isStarted = new AtomicBoolean(false);
        private volatile long lastPingTime;

        private ScheduledPing(TimeValue pingInterval) {
            super(lifecycle, logger);
            this.pingInterval = pingInterval;
            // Set lastPingTime to a pingInterval in the past to avoid timing out channels on the first run
            this.lastPingTime = System.nanoTime() - pingInterval.getNanos();
        }

        void ensureStarted() {
            if (isStarted.get() == false && isStarted.compareAndSet(false, true)) {
                threadPool.schedule(pingInterval, ThreadPool.Names.GENERIC, this);
            }
        }

        void addChannel(TcpChannel channel) {
            channels.add(channel);
        }

        void removeChannel(TcpChannel channel) {
            channels.remove(channel);
        }

        @Override
        protected void doRunInLifecycle() {
            long now = System.nanoTime();
            for (TcpChannel channel : channels) {
                KeepAliveStats keepAliveStats = channelStats.get(channel);
                if (keepAliveStats != null && keepAliveStats.lastWriteTime > lastPingTime) {
                    sendPing(channel);
                    keepAliveStats.lastWriteTime = now;
                }

                assert keepAliveStats != null || channel.isOpen() == false : "If keepalive stats are null the channel must be closed";
            }
            this.lastPingTime = System.nanoTime();
        }

        @Override
        protected void onAfterInLifecycle() {
            try {
                threadPool.schedule(pingInterval, ThreadPool.Names.GENERIC, this);
            } catch (EsRejectedExecutionException ex) {
                if (ex.isExecutorShutdown()) {
                    logger.debug("couldn't schedule new ping execution, executor is shutting down", ex);
                } else {
                    throw ex;
                }
            }
        }

        @Override
        public void onFailure(Exception e) {
            if (lifecycle.stoppedOrClosed()) {
                logger.trace("failed to send ping transport message", e);
            } else {
                logger.warn("failed to send ping transport message", e);
            }
        }
    }

    interface PingSender {

        void send(TcpChannel channel, BytesReference message, ActionListener<Void> listener);
    }
}
