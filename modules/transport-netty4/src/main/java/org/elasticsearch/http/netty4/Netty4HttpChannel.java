/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http.netty4;

import io.netty.channel.Channel;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.http.HttpResponse;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import static org.elasticsearch.transport.netty4.Netty4Utils.addListener;
import static org.elasticsearch.transport.netty4.Netty4Utils.safeWriteAndFlush;

public class Netty4HttpChannel implements HttpChannel {

    private final Channel channel;
    private final SubscribableListener<Void> closeContext = new SubscribableListener<>();

    Netty4HttpChannel(Channel channel) {
        this.channel = channel;
        addListener(this.channel.closeFuture(), closeContext);
    }

    @Override
    public void sendResponse(HttpResponse response, ActionListener<Void> listener) {
        safeWriteAndFlush(channel, response, listener);
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
