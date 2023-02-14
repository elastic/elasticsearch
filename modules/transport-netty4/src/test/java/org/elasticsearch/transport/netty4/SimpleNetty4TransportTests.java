/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport.netty4;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.test.transport.StubbableTransport;
import org.elasticsearch.transport.AbstractSimpleTransportTestCase;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.RemoteClusterPortSettings;
import org.elasticsearch.transport.TcpChannel;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.TestProfiles;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportSettings;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.channels.SocketChannel;
import java.util.Collections;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class SimpleNetty4TransportTests extends AbstractSimpleTransportTestCase {

    @Override
    protected Transport build(Settings settings, TransportVersion version, ClusterSettings clusterSettings, boolean doHandshake) {
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(Collections.emptyList());
        return new Netty4Transport(
            settings,
            version,
            threadPool,
            new NetworkService(Collections.emptyList()),
            PageCacheRecycler.NON_RECYCLING_INSTANCE,
            namedWriteableRegistry,
            new NoneCircuitBreakerService(),
            new SharedGroupFactory(settings)
        ) {

            @Override
            public void executeHandshake(
                DiscoveryNode node,
                TcpChannel channel,
                ConnectionProfile profile,
                ActionListener<TransportVersion> listener
            ) {
                if (doHandshake) {
                    super.executeHandshake(node, channel, profile, listener);
                } else {
                    listener.onResponse(version.calculateMinimumCompatVersion());
                }
            }
        };
    }

    public void testConnectException() throws UnknownHostException {
        try {
            connectToNode(
                serviceA,
                new DiscoveryNode(
                    "C",
                    new TransportAddress(InetAddress.getByName("localhost"), 9876),
                    emptyMap(),
                    emptySet(),
                    Version.CURRENT
                )
            );
            fail("Expected ConnectTransportException");
        } catch (ConnectTransportException e) {
            assertThat(e.getMessage(), containsString("connect_exception"));
            assertThat(e.getMessage(), containsString("[127.0.0.1:9876]"));
        }
    }

    public void testDefaultKeepAliveSettings() throws IOException {
        assumeTrue("setting default keepalive options not supported on this platform", (IOUtils.LINUX || IOUtils.MAC_OS_X));
        try (
            MockTransportService serviceC = buildService("TS_C", Version.CURRENT, Settings.EMPTY);
            MockTransportService serviceD = buildService("TS_D", Version.CURRENT, Settings.EMPTY)
        ) {

            try (Transport.Connection connection = openConnection(serviceC, serviceD.getLocalDiscoNode(), TestProfiles.LIGHT_PROFILE)) {
                assertThat(connection, instanceOf(StubbableTransport.WrappedConnection.class));
                Transport.Connection conn = ((StubbableTransport.WrappedConnection) connection).getConnection();
                assertThat(conn, instanceOf(TcpTransport.NodeChannels.class));
                TcpTransport.NodeChannels nodeChannels = (TcpTransport.NodeChannels) conn;
                for (TcpChannel channel : nodeChannels.getChannels()) {
                    assertFalse(channel.isServerChannel());
                    checkDefaultKeepAliveOptions(channel);
                }

                assertThat(serviceD.getOriginalTransport(), instanceOf(TcpTransport.class));
                for (TcpChannel channel : getAcceptedChannels((TcpTransport) serviceD.getOriginalTransport())) {
                    assertTrue(channel.isServerChannel());
                    checkDefaultKeepAliveOptions(channel);
                }
            }
        }
    }

    public void testTransportProfile() {
        final String transportProfile = randomFrom(
            TransportSettings.DEFAULT_PROFILE,
            RemoteClusterPortSettings.REMOTE_CLUSTER_PROFILE,
            randomAlphaOfLengthBetween(5, 12)
        );
        final ConnectionProfile connectionProfile = ConnectionProfile.resolveConnectionProfile(
            new ConnectionProfile.Builder().setTransportProfile(transportProfile)
                .addConnections(
                    1,
                    TransportRequestOptions.Type.BULK,
                    TransportRequestOptions.Type.PING,
                    TransportRequestOptions.Type.RECOVERY,
                    TransportRequestOptions.Type.REG,
                    TransportRequestOptions.Type.STATE
                )
                .build(),
            TestProfiles.LIGHT_PROFILE
        );

        try (
            MockTransportService serviceC = buildService("TS_C", Version.CURRENT, Settings.EMPTY);
            MockTransportService serviceD = buildService("TS_D", Version.CURRENT, Settings.EMPTY)
        ) {

            try (Transport.Connection connection = openConnection(serviceC, serviceD.getLocalDiscoNode(), connectionProfile)) {
                assertThat(connection, instanceOf(StubbableTransport.WrappedConnection.class));
                Transport.Connection conn = ((StubbableTransport.WrappedConnection) connection).getConnection();
                assertThat(conn, instanceOf(TcpTransport.NodeChannels.class));
                TcpTransport.NodeChannels nodeChannels = (TcpTransport.NodeChannels) conn;
                for (TcpChannel channel : nodeChannels.getChannels()) {
                    assertFalse(channel.isServerChannel());
                    assertThat(channel.getProfile(), equalTo(transportProfile));
                }

                assertThat(serviceD.getOriginalTransport(), instanceOf(TcpTransport.class));
                for (TcpChannel channel : getAcceptedChannels((TcpTransport) serviceD.getOriginalTransport())) {
                    assertTrue(channel.isServerChannel());
                    assertThat(channel.getProfile(), equalTo(TransportSettings.DEFAULT_PROFILE));
                }
            }
        }
    }

    private void checkDefaultKeepAliveOptions(TcpChannel channel) throws IOException {
        assertThat(channel, instanceOf(Netty4TcpChannel.class));
        Netty4TcpChannel nettyChannel = (Netty4TcpChannel) channel;
        assertThat(nettyChannel.getNettyChannel(), instanceOf(Netty4NioSocketChannel.class));
        Netty4NioSocketChannel netty4NioSocketChannel = (Netty4NioSocketChannel) nettyChannel.getNettyChannel();
        SocketChannel socketChannel = netty4NioSocketChannel.javaChannel();
        assertThat(socketChannel.supportedOptions(), hasItem(NetUtils.getTcpKeepIdleSocketOption()));
        Integer keepIdle = socketChannel.getOption(NetUtils.getTcpKeepIdleSocketOption());
        assertNotNull(keepIdle);
        assertThat(keepIdle, lessThanOrEqualTo(500));
        assertThat(socketChannel.supportedOptions(), hasItem(NetUtils.getTcpKeepIntervalSocketOption()));
        Integer keepInterval = socketChannel.getOption(NetUtils.getTcpKeepIntervalSocketOption());
        assertNotNull(keepInterval);
        assertThat(keepInterval, lessThanOrEqualTo(500));
    }
}
