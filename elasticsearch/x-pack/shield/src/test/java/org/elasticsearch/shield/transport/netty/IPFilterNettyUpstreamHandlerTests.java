/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport.netty;

import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.shield.audit.AuditTrail;
import org.elasticsearch.shield.SecurityLicenseState;
import org.elasticsearch.shield.transport.filter.IPFilter;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportSettings;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelConfig;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.UpstreamMessageEvent;
import org.junit.Before;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.HashSet;


import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 */
public class IPFilterNettyUpstreamHandlerTests extends ESTestCase {
    private IPFilterNettyUpstreamHandler nettyUpstreamHandler;

    @Before
    public void init() throws Exception {
        Settings settings = Settings.builder()
                .put("xpack.security.transport.filter.allow", "127.0.0.1")
                .put("xpack.security.transport.filter.deny", "10.0.0.0/8")
                .build();

        boolean isHttpEnabled = randomBoolean();

        Transport transport = mock(Transport.class);
        InetSocketTransportAddress address = new InetSocketTransportAddress(InetAddress.getLoopbackAddress(), 9300);
        when(transport.boundAddress()).thenReturn(new BoundTransportAddress(new TransportAddress[] { address }, address));
        when(transport.lifecycleState()).thenReturn(Lifecycle.State.STARTED);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, new HashSet<>(Arrays.asList(
                IPFilter.HTTP_FILTER_ALLOW_SETTING,
                IPFilter.HTTP_FILTER_DENY_SETTING,
                IPFilter.IP_FILTER_ENABLED_HTTP_SETTING,
                IPFilter.IP_FILTER_ENABLED_SETTING,
                IPFilter.TRANSPORT_FILTER_ALLOW_SETTING,
                IPFilter.TRANSPORT_FILTER_DENY_SETTING,
                TransportSettings.TRANSPORT_PROFILES_SETTING)));
        SecurityLicenseState licenseState = mock(SecurityLicenseState.class);
        when(licenseState.ipFilteringEnabled()).thenReturn(true);
        IPFilter ipFilter = new IPFilter(settings, AuditTrail.NOOP, clusterSettings, licenseState);
        ipFilter.setBoundTransportAddress(transport.boundAddress(), transport.profileBoundAddresses());
        if (isHttpEnabled) {
            HttpServerTransport httpTransport = mock(HttpServerTransport.class);
            InetSocketTransportAddress httpAddress = new InetSocketTransportAddress(InetAddress.getLoopbackAddress(), 9200);
            when(httpTransport.boundAddress()).thenReturn(new BoundTransportAddress(new TransportAddress[] { httpAddress }, httpAddress));
            when(httpTransport.lifecycleState()).thenReturn(Lifecycle.State.STARTED);
            ipFilter.setBoundHttpTransportAddress(httpTransport.boundAddress());
        }

        if (isHttpEnabled) {
            nettyUpstreamHandler = new IPFilterNettyUpstreamHandler(ipFilter, IPFilter.HTTP_PROFILE_NAME);
        } else {
            nettyUpstreamHandler = new IPFilterNettyUpstreamHandler(ipFilter, "default");
        }
    }

    public void testThatFilteringWorksByIp() throws Exception {
        InetSocketAddress localhostAddr = new InetSocketAddress(InetAddresses.forString("127.0.0.1"), 12345);
        assertThat(nettyUpstreamHandler.accept(new NullChannelHandlerContext(), new UpstreamMessageEvent(
                new NullChannel(), "my message", localhostAddr), localhostAddr), is(true));

        InetSocketAddress remoteAddr = new InetSocketAddress(InetAddresses.forString("10.0.0.8"), 12345);
        assertThat(nettyUpstreamHandler.accept(new NullChannelHandlerContext(), new UpstreamMessageEvent(
                new NullChannel(), "my message", remoteAddr), remoteAddr), is(false));
    }

    private static class NullChannelHandlerContext implements ChannelHandlerContext {
        @Override
        public boolean canHandleDownstream() {
            return false;
        }

        @Override
        public boolean canHandleUpstream() {
            return false;
        }

        @Override
        public Object getAttachment() {
            return null;
        }

        @Override
        public Channel getChannel() {
            return null;
        }

        @Override
        public ChannelHandler getHandler() {
            return null;
        }

        @Override
        public String getName() {
            return null;
        }

        @Override
        public ChannelPipeline getPipeline() {
            return null;
        }

        @Override
        public void sendDownstream(ChannelEvent e) {
            // NOOP
        }

        @Override
        public void sendUpstream(ChannelEvent e) {
            // NOOP
        }

        @Override
        public void setAttachment(Object attachment) {
            // NOOP
        }
    }

    private static class NullChannel implements Channel {
        @Override
        public ChannelFuture bind(SocketAddress localAddress) {
            return null;
        }

        @Override
        public ChannelFuture close() {
            return null;
        }

        @Override
        public ChannelFuture connect(SocketAddress remoteAddress) {
            return null;
        }

        @Override
        public ChannelFuture disconnect() {
            return null;
        }

        @Override
        public ChannelFuture getCloseFuture() {
            return null;
        }

        @Override
        public ChannelConfig getConfig() {
            return null;
        }

        @Override
        public ChannelFactory getFactory() {
            return null;
        }

        @Override
        public Integer getId() {
            return null;
        }

        @Override
        public int getInterestOps() {
            return 0;
        }

        @Override
        public SocketAddress getLocalAddress() {
            return null;
        }

        @Override
        public Channel getParent() {
            return null;
        }

        @Override
        public ChannelPipeline getPipeline() {
            return null;
        }

        @Override
        public SocketAddress getRemoteAddress() {
            return null;
        }

        @Override
        public boolean isBound() {
            return false;
        }

        @Override
        public boolean isConnected() {
            return false;
        }

        @Override
        public boolean isOpen() {
            return false;
        }

        @Override
        public boolean isReadable() {
            return false;
        }

        @Override
        public boolean isWritable() {
            return false;
        }

        @Override
        public ChannelFuture setInterestOps(int interestOps) {
            return null;
        }

        @Override
        public ChannelFuture setReadable(boolean readable) {
            return null;
        }

        @Override
        public boolean getUserDefinedWritability(int i) {
            return false;
        }

        @Override
        public void setUserDefinedWritability(int i, boolean b) {
        }

        @Override
        public ChannelFuture unbind() {
            return null;
        }

        @Override
        public ChannelFuture write(Object message) {
            return null;
        }

        @Override
        public ChannelFuture write(Object message, SocketAddress remoteAddress) {
            return null;
        }

        @Override
        public int compareTo(Channel o) {
            return 0;
        }

        @Override
        public int hashCode() {
            return 0;
        }

        @Override
        public boolean equals(Object o) {
            return this == o;
        }

        @Override
        public Object getAttachment() {
            return null;
        }

        @Override
        public void setAttachment(Object attachment) {
            // NOOP
        }
    }
}
