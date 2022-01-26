/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport.nio;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.nio.NioSocketChannel;
import org.elasticsearch.transport.TcpChannel;

import java.nio.channels.SocketChannel;

public class NioTcpChannel extends NioSocketChannel implements TcpChannel {

    private final boolean isServer;
    private final String profile;
    private final ChannelStats stats = new ChannelStats();

    public NioTcpChannel(boolean isServer, String profile, SocketChannel socketChannel) {
        super(socketChannel);
        this.isServer = isServer;
        this.profile = profile;
    }

    public void sendMessage(BytesReference reference, ActionListener<Void> listener) {
        getContext().sendMessage(BytesReference.toByteBuffers(reference), ActionListener.toBiConsumer(listener));
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
        addCloseListener(ActionListener.toBiConsumer(listener));
    }

    @Override
    public void addConnectListener(ActionListener<Void> listener) {
        addConnectListener(ActionListener.toBiConsumer(listener));
    }

    @Override
    public ChannelStats getChannelStats() {
        return stats;
    }

    @Override
    public void close() {
        getContext().closeChannel();
    }

    @Override
    public String toString() {
        return "TcpNioSocketChannel{"
            + "localAddress="
            + getLocalAddress()
            + ", remoteAddress="
            + getRemoteAddress()
            + ", profile="
            + profile
            + '}';
    }
}
