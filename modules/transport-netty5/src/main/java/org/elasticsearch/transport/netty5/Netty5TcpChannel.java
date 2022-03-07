/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport.netty5;

import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.util.concurrent.Future;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.CompletableContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.transport.TcpChannel;
import org.elasticsearch.transport.TransportException;

import java.net.InetSocketAddress;

public class Netty5TcpChannel implements TcpChannel {

    private volatile Channel channel;
    private final boolean isServer;
    private final String profile;
    private final CompletableContext<Void> connectContext;
    private final CompletableContext<Void> closeContext = new CompletableContext<>();
    private final ChannelStats stats = new ChannelStats();
    private final boolean rstOnClose;

    Netty5TcpChannel(boolean isServer, String profile, boolean rstOnClose, Future<Channel> connectFuture) {
        this.isServer = isServer;
        this.profile = profile;
        this.connectContext = new CompletableContext<>();
        connectFuture.addListener(f -> {
            if (f.isSuccess()) {
                channel = f.getNow();
                channel.attr(Netty5Transport.CHANNEL_KEY).set(this);
            }
        });
        this.rstOnClose = rstOnClose;
        addListener(connectFuture, connectContext);
        connectFuture.addListener(future -> {
            if (future.isSuccess()) {
                addListener(future.getNow().closeFuture(), closeContext);
            }
        });
    }

    public static void addListener(Future<?> channelFuture, CompletableContext<Void> context) {
        channelFuture.addListener(f -> {
            if (f.isSuccess()) {
                context.complete(null);
            } else {
                Throwable cause = f.cause();
                if (cause instanceof Error) {
                    ExceptionsHelper.maybeDieOnAnotherThread(cause);
                    context.completeExceptionally(new Exception(cause));
                } else {
                    context.completeExceptionally((Exception) cause);
                }
            }
        });
    }

    public static void addPromise(ActionListener<Void> listener, Future<Void> future) {
        future.addListener(f -> {
            if (f.isSuccess()) {
                listener.onResponse(null);
            } else {
                final Throwable cause = f.cause();
                ExceptionsHelper.maybeDieOnAnotherThread(cause);
                if (cause instanceof Error) {
                    listener.onFailure(new Exception(cause));
                } else {
                    listener.onFailure((Exception) cause);
                }
            }
        });
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
        closeContext.addListener(ActionListener.toBiConsumer(listener));
    }

    @Override
    public void addConnectListener(ActionListener<Void> listener) {
        connectContext.addListener(ActionListener.toBiConsumer(listener));
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
        addPromise(listener, channel.writeAndFlush(Netty5Utils.toByteBuf(reference)));

        if (channel.executor().isShutdown()) {
            listener.onFailure(new TransportException("Cannot send message, event loop is shutting down."));
        }
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
