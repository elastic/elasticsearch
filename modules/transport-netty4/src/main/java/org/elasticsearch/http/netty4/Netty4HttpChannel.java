/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

import io.netty.channel.Channel;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.http.HttpResponse;
import org.elasticsearch.transport.netty4.Netty4TcpChannel;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

public class Netty4HttpChannel implements HttpChannel {

    private final Channel channel;
    private final ListenableFuture<Void> closeContext = new ListenableFuture<>();

    Netty4HttpChannel(Channel channel) {
        this.channel = channel;
        Netty4TcpChannel.addListener(this.channel.closeFuture(), closeContext);
    }

    @Override
    public void sendResponse(HttpResponse response, ActionListener<Void> listener) {
        channel.writeAndFlush(response, Netty4TcpChannel.addPromise(listener, channel));
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return castAddressOrNull(channel.localAddress());
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return castAddressOrNull(channel.remoteAddress());
    }

    private static InetSocketAddress castAddressOrNull(SocketAddress socketAddress) {
        if (socketAddress instanceof InetSocketAddress) {
            return (InetSocketAddress) socketAddress;
        } else {
            return null;
        }
    }

    @Override
    public void addCloseListener(ActionListener<Void> listener) {
        closeContext.addListener(listener);
    }

    @Override
    public boolean isOpen() {
        return channel.isOpen();
    }

    @Override
    public void close() {
        channel.close();
    }

    public Channel getNettyChannel() {
        return channel;
    }

    @Override
    public String toString() {
        return "Netty4HttpChannel{" + "localAddress=" + getLocalAddress() + ", remoteAddress=" + getRemoteAddress() + '}';
    }
}
