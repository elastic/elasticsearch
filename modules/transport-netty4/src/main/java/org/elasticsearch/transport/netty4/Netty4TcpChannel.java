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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.transport.TcpChannel;

import java.net.InetSocketAddress;

import static org.elasticsearch.transport.netty4.Netty4Utils.addListener;
import static org.elasticsearch.transport.netty4.Netty4Utils.safeWriteAndFlush;

public class Netty4TcpChannel implements TcpChannel {

    private final Channel channel;
    private final boolean isServer;
    private final String profile;
    private final SubscribableListener<Void> connectContext = new SubscribableListener<>();
    private final SubscribableListener<Void> closeContext = new SubscribableListener<>();
    private final ChannelStats stats = new ChannelStats();
    private final boolean rstOnClose;
    /**
     * Exception causing a close, reported to the {@link #closeContext} listener
     */
    private volatile Exception closeException = null;

    Netty4TcpChannel(Channel channel, boolean isServer, String profile, boolean rstOnClose, ChannelFuture connectFuture) {
        this.channel = channel;
        this.isServer = isServer;
        this.profile = profile;
        this.rstOnClose = rstOnClose;
        addListener(connectFuture, connectContext);
        addListener(this.channel.closeFuture(), new ActionListener<>() {
            @Override
            public void onResponse(Void ignored) {
                if (closeException != null) {
                    closeContext.onFailure(closeException);
                } else {
                    closeContext.onResponse(null);
                }
            }

            @Override
            public void onFailure(Exception e) {
                assert false : new AssertionError("netty channel closeFuture should never report a failure");
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
        closeContext.addListener(listener);
    }

    @Override
    public void addConnectListener(ActionListener<Void> listener) {
        connectContext.addListener(listener);
    }

    @Override
    public void setCloseException(Exception e) {
        closeException = e;
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
        safeWriteAndFlush(channel, reference, listener);
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
