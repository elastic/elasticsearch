/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport.netty4;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPromise;
import io.netty.util.internal.PlatformDependent;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.transport.TcpChannel;

import java.net.InetSocketAddress;
import java.util.Queue;

import static org.elasticsearch.transport.netty4.Netty4Utils.addListener;

public class Netty4TcpChannel implements TcpChannel {

    private final Channel channel;
    private final boolean isServer;
    private final String profile;
    private final ListenableFuture<Void> connectContext;
    private final ListenableFuture<Void> closeContext = new ListenableFuture<>();
    private final ChannelStats stats = new ChannelStats();
    private final boolean rstOnClose;
    private final Runnable flushTask;
    // queue of objects that will be written by the next execution of #flushTask and the promise to resolve on their completion
    private final Queue<Tuple<Object, ChannelPromise>> sendQueue = PlatformDependent.newFixedMpscUnpaddedQueue(128);

    Netty4TcpChannel(Channel channel, boolean isServer, String profile, boolean rstOnClose, ChannelFuture connectFuture) {
        this.channel = channel;
        this.isServer = isServer;
        this.profile = profile;
        this.connectContext = new ListenableFuture<>();
        this.rstOnClose = rstOnClose;
        addListener(this.channel.closeFuture(), closeContext);
        addListener(connectFuture, connectContext);
        flushTask = () -> {
            if (flushQueue()) {
                channel.flush();
            }
        };
    }

    private boolean flushQueue() {
        boolean wroteMessage = false;
        Tuple<Object, ChannelPromise> toWrite;
        while ((toWrite = sendQueue.poll()) != null) {
            channel.write(toWrite.v1(), toWrite.v2());
            wroteMessage = true;
        }
        return wroteMessage;
    }

    @Override
    public void close() {
        if (rstOnClose) {
            rstAndClose();
        } else {
            channel.close();
        }
    }

    private void rstAndClose() {
        Releasables.close(() -> {
            if (channel.isOpen()) {
                try {
                    channel.config().setOption(ChannelOption.SO_LINGER, 0);
                } catch (Exception e) {
                    if (IOUtils.MAC_OS_X) {
                        // Just ignore on OSX for now, there is no reliably way of determining if the socket is still in a state that
                        // accepts the setting because it could have already been reset from the other end which quietly does nothing on
                        // Linux but throws OSX.
                        // TODO: handle this cleaner?
                        return;
                    }
                    if (channel.isOpen()) {
                        throw e;
                    }
                }
            }
        }, channel::close);
    }

    @Override
    public boolean isServerChannel() {
        return isServer;
    }

    @Override
    public String getProfile() {
        return profile;
    }

    @Override
    public void addCloseListener(ActionListener<Void> listener) {
        closeContext.addListener(listener);
    }

    @Override
    public void addConnectListener(ActionListener<Void> listener) {
        connectContext.addListener(listener);
    }

    @Override
    public ChannelStats getChannelStats() {
        return stats;
    }

    @Override
    public boolean isOpen() {
        return channel.isOpen();
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return (InetSocketAddress) channel.localAddress();
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return (InetSocketAddress) channel.remoteAddress();
    }

    @Override
    public void sendMessage(BytesReference reference, ActionListener<Void> listener) {
        // Use ImmediateEventExecutor.INSTANCE since we want to be able to complete this promise, and any waiting listeners, even if the
        // channel's event loop has shut down. Normally this completion will happen on the channel's event loop anyway because the write op
        // can only be completed by some network event from this point on. However...
        final Channel channel = this.channel;
        final var promise = Netty4Utils.asChannelPromise(listener, channel);
        var eventLoop = channel.eventLoop();
        if (eventLoop.inEventLoop()) {
            // from the eventloop we minimize allocations and latency by forwarding the current queue and the new message and triggering
            // one flush after wards
            flushQueue();
            channel.writeAndFlush(reference, promise);
        } else {
            // We are not on the channel's transport thread, queue the write and trigger a flush task
            if (sendQueue.offer(new Tuple<>(reference, promise)) == false) {
                channel.writeAndFlush(reference, promise);
            } else {
                eventLoop.execute(flushTask);
            }
        }
        Netty4Utils.handleTerminatedEventLoop(eventLoop, promise);
    }

    public Channel getNettyChannel() {
        return channel;
    }

    @Override
    public String toString() {
        return "Netty4TcpChannel{"
            + "localAddress="
            + getLocalAddress()
            + ", remoteAddress="
            + channel.remoteAddress()
            + ", profile="
            + profile
            + '}';
    }
}
