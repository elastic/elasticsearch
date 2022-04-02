/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport.netty4;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPromise;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.CompletableContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.transport.TcpChannel;
import org.elasticsearch.transport.TransportException;

import java.net.InetSocketAddress;

public class Netty4TcpChannel implements TcpChannel {

    private final Channel channel;
    private final boolean isServer;
    private final String profile;
    private final CompletableContext<Void> connectContext;
    private final CompletableContext<Void> closeContext = new CompletableContext<>();
    private final ChannelStats stats = new ChannelStats();
    private final boolean rstOnClose;

    Netty4TcpChannel(Channel channel, boolean isServer, String profile, boolean rstOnClose, @Nullable ChannelFuture connectFuture) {
        this.channel = channel;
        this.isServer = isServer;
        this.profile = profile;
        this.connectContext = new CompletableContext<>();
        this.rstOnClose = rstOnClose;
        addListener(this.channel.closeFuture(), closeContext);
        addListener(connectFuture, connectContext);
    }

    /**
     * Adds a listener that completes the given {@link CompletableContext} to the given {@link ChannelFuture}.
     * @param channelFuture Channel future
     * @param context Context to complete
     */
    public static void addListener(ChannelFuture channelFuture, CompletableContext<Void> context) {
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

    /**
     * Creates a {@link ChannelPromise} for the given {@link Channel} and adds a listener that invokes the given {@link ActionListener}
     * on its completion.
     * @param listener lister to invoke
     * @param channel channel
     * @return write promise
     */
    public static ChannelPromise addPromise(ActionListener<Void> listener, Channel channel) {
        ChannelPromise writePromise = channel.newPromise();
        writePromise.addListener(f -> {
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
        return writePromise;
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
        channel.writeAndFlush(Netty4Utils.toByteBuf(reference), addPromise(listener, channel));

        if (channel.eventLoop().isShutdown()) {
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
