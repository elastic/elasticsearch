/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.transport;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.CompletableContext;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicReference;

public class FakeTcpChannel implements TcpChannel {

    private final boolean isServer;
    private final InetSocketAddress localAddress;
    private final InetSocketAddress remoteAddress;
    private final String profile;
    private final ChannelStats stats = new ChannelStats();
    private final CompletableContext<Void> closeContext = new CompletableContext<>();
    private final AtomicReference<BytesReference> messageCaptor;
    private final AtomicReference<ActionListener<Void>> listenerCaptor;

    public FakeTcpChannel() {
        this(false, "profile", new AtomicReference<>());
    }

    public FakeTcpChannel(boolean isServer) {
        this(isServer, "profile", new AtomicReference<>());
    }

    public FakeTcpChannel(boolean isServer, InetSocketAddress localAddress, InetSocketAddress remoteAddress) {
        this(isServer, localAddress, remoteAddress, "profile", new AtomicReference<>());
    }

    public FakeTcpChannel(boolean isServer, AtomicReference<BytesReference> messageCaptor) {
        this(isServer, "profile", messageCaptor);
    }

    public FakeTcpChannel(boolean isServer, String profile, AtomicReference<BytesReference> messageCaptor) {
        this(isServer, null, null, profile, messageCaptor);
    }

    public FakeTcpChannel(
        boolean isServer,
        InetSocketAddress localAddress,
        InetSocketAddress remoteAddress,
        String profile,
        AtomicReference<BytesReference> messageCaptor
    ) {
        this.isServer = isServer;
        this.localAddress = localAddress;
        this.remoteAddress = remoteAddress;
        this.profile = profile;
        this.messageCaptor = messageCaptor;
        this.listenerCaptor = new AtomicReference<>();
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
    public InetSocketAddress getLocalAddress() {
        return localAddress;
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return remoteAddress;
    }

    @Override
    public void sendMessage(BytesReference reference, ActionListener<Void> listener) {
        messageCaptor.set(reference);
        listenerCaptor.set(listener);
    }

    @Override
    public void addConnectListener(ActionListener<Void> listener) {

    }

    @Override
    public void close() {
        closeContext.complete(null);
    }

    @Override
    public void addCloseListener(ActionListener<Void> listener) {
        closeContext.addListener(ActionListener.toBiConsumer(listener));
    }

    @Override
    public boolean isOpen() {
        return closeContext.isDone() == false;
    }

    @Override
    public ChannelStats getChannelStats() {
        return stats;
    }

    public AtomicReference<BytesReference> getMessageCaptor() {
        return messageCaptor;
    }

    public AtomicReference<ActionListener<Void>> getListenerCaptor() {
        return listenerCaptor;
    }
}
