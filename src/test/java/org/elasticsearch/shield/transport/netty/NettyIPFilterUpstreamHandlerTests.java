/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport.netty;

import com.google.common.net.InetAddresses;
import org.elasticsearch.common.netty.channel.*;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.audit.AuditTrail;
import org.elasticsearch.shield.transport.filter.IPFilter;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.is;

/**
 *
 */
public class NettyIPFilterUpstreamHandlerTests extends ElasticsearchTestCase {

    private NettyIPFilterUpstreamHandler nettyUpstreamHandler;

    @Before
    public void init() throws Exception {
        Settings settings = settingsBuilder()
                .put("shield.transport.filter.allow", "127.0.0.1")
                .put("shield.transport.filter.deny", "10.0.0.0/8")
                .build();

        IPFilter ipFilter = new IPFilter(settings, AuditTrail.NOOP);

        nettyUpstreamHandler = new NettyIPFilterUpstreamHandler(ipFilter, "default");
    }

    @Test
    public void testThatFilteringWorksByIp() throws Exception {
        InetSocketAddress localhostAddr = new InetSocketAddress(InetAddresses.forString("127.0.0.1"), 12345);
        assertThat(nettyUpstreamHandler.accept(new NullChannelHandlerContext(), new UpstreamMessageEvent(new NullChannel(), "my message", localhostAddr), localhostAddr), is(true));

        InetSocketAddress remoteAddr = new InetSocketAddress(InetAddresses.forString("10.0.0.8"), 12345);
        assertThat(nettyUpstreamHandler.accept(new NullChannelHandlerContext(), new UpstreamMessageEvent(new NullChannel(), "my message", remoteAddr), remoteAddr), is(false));
    }


    private static class NullChannelHandlerContext implements ChannelHandlerContext {
        public boolean canHandleDownstream() {
            return false;
        }

        public boolean canHandleUpstream() {
            return false;
        }

        public Object getAttachment() {
            return null;
        }

        public Channel getChannel() {
            return null;
        }

        public ChannelHandler getHandler() {
            return null;
        }

        public String getName() {
            return null;
        }

        public ChannelPipeline getPipeline() {
            return null;
        }

        public void sendDownstream(ChannelEvent e) {
            // NOOP
        }

        public void sendUpstream(ChannelEvent e) {
            // NOOP
        }

        public void setAttachment(Object attachment) {
            // NOOP
        }
    }

    private static class NullChannel implements Channel {
        public ChannelFuture bind(SocketAddress localAddress) {
            return null;
        }

        public ChannelFuture close() {
            return null;
        }

        public ChannelFuture connect(SocketAddress remoteAddress) {
            return null;
        }

        public ChannelFuture disconnect() {
            return null;
        }

        public ChannelFuture getCloseFuture() {
            return null;
        }

        public ChannelConfig getConfig() {
            return null;
        }

        public ChannelFactory getFactory() {
            return null;
        }

        public Integer getId() {
            return null;
        }

        public int getInterestOps() {
            return 0;
        }

        public SocketAddress getLocalAddress() {
            return null;
        }

        public Channel getParent() {
            return null;
        }

        public ChannelPipeline getPipeline() {
            return null;
        }

        public SocketAddress getRemoteAddress() {
            return null;
        }

        public boolean isBound() {
            return false;
        }

        public boolean isConnected() {
            return false;
        }

        public boolean isOpen() {
            return false;
        }

        public boolean isReadable() {
            return false;
        }

        public boolean isWritable() {
            return false;
        }

        public ChannelFuture setInterestOps(int interestOps) {
            return null;
        }

        public ChannelFuture setReadable(boolean readable) {
            return null;
        }

        public ChannelFuture unbind() {
            return null;
        }

        public ChannelFuture write(Object message) {
            return null;
        }

        public ChannelFuture write(Object message, SocketAddress remoteAddress) {
            return null;
        }

        public int compareTo(Channel o) {
            return 0;
        }

        public int hashCode() {
            return 0;
        }

        public boolean equals(Object o) {
            return this == o;
        }

        public Object getAttachment() {
            return null;
        }

        public void setAttachment(Object attachment) {
            // NOOP
        }
    }
}
