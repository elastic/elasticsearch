/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport.netty4;

import org.apache.lucene.util.Constants;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.mocksocket.MockServerSocket;
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
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.TransportSettings;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
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
            networkService,
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
                    assert version.equals(TransportVersion.current());
                    listener.onResponse(TransportVersions.MINIMUM_COMPATIBLE);
                }
            }
        };
    }

    public void testConnectException() throws UnknownHostException {
        final var e = connectToNodeExpectFailure(
            serviceA,
            DiscoveryNodeUtils.create("C", new TransportAddress(InetAddress.getByName("localhost"), 9876), emptyMap(), emptySet()),
            null
        );
        assertThat(e.getMessage(), containsString("connect_exception"));
        assertThat(e.getMessage(), containsString("[127.0.0.1:9876]"));
    }

    public void testDefaultKeepAliveSettings() throws IOException {
        assumeTrue("setting default keepalive options not supported on this platform", (IOUtils.LINUX || IOUtils.MAC_OS_X));
        try (
            MockTransportService serviceC = buildService("TS_C", VersionInformation.CURRENT, TransportVersion.current(), Settings.EMPTY);
            MockTransportService serviceD = buildService("TS_D", VersionInformation.CURRENT, TransportVersion.current(), Settings.EMPTY)
        ) {

            try (Transport.Connection connection = openConnection(serviceC, serviceD.getLocalNode(), TestProfiles.LIGHT_PROFILE)) {
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
            MockTransportService serviceC = buildService("TS_C", VersionInformation.CURRENT, TransportVersion.current(), Settings.EMPTY);
            MockTransportService serviceD = buildService("TS_D", VersionInformation.CURRENT, TransportVersion.current(), Settings.EMPTY)
        ) {

            try (Transport.Connection connection = openConnection(serviceC, serviceD.getLocalNode(), connectionProfile)) {
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

    public void testTimeoutPerConnection() throws IOException {
        assumeTrue("Works only on BSD network stacks", Constants.MAC_OS_X || Constants.FREE_BSD);
        try (ServerSocket socket = new MockServerSocket()) {

            // note - this test uses backlog=1 which is implementation specific ie. it might not work on some TCP/IP stacks
            // on linux (at least newer ones) the listen(addr, backlog=1) should just ignore new connections if the queue is full which
            // means that once we received an ACK from the client we just drop the packet on the floor (which is what we want) and we run
            // into a connection timeout quickly. Yet other implementations can for instance can terminate the connection within the 3 way
            // handshake which I haven't tested yet.

            // note - this test doesn't work with security enabled because it relies on connecting to the MockServerSocket and we are not
            // set up to accept a TLS handshake on this socket

            socket.bind(getLocalEphemeral(), 1);
            socket.setReuseAddress(true);

            DiscoveryNode first = DiscoveryNodeUtils.builder("TEST")
                .address(new TransportAddress(socket.getInetAddress(), socket.getLocalPort()))
                .roles(emptySet())
                .version(version0)
                .build();
            DiscoveryNode second = DiscoveryNodeUtils.builder("TEST")
                .address(new TransportAddress(socket.getInetAddress(), socket.getLocalPort()))
                .roles(emptySet())
                .version(version0)
                .build();
            ConnectionProfile.Builder builder = new ConnectionProfile.Builder();
            builder.addConnections(
                1,
                TransportRequestOptions.Type.BULK,
                TransportRequestOptions.Type.PING,
                TransportRequestOptions.Type.RECOVERY,
                TransportRequestOptions.Type.REG,
                TransportRequestOptions.Type.STATE
            );
            // connection with one connection and a large timeout -- should consume the one spot in the backlog queue
            try (
                TransportService service = buildService(
                    "TS_TPC",
                    VersionInformation.CURRENT,
                    TransportVersion.current(),
                    null,
                    Settings.EMPTY,
                    true,
                    false
                )
            ) {
                IOUtils.close(openConnection(service, first, builder.build()));
                builder.setConnectTimeout(TimeValue.timeValueMillis(1));
                final ConnectionProfile profile = builder.build();
                // now with the 1ms timeout we got and test that is it's applied
                long startTime = System.nanoTime();
                ConnectTransportException ex = openConnectionExpectFailure(service, second, profile);
                final long now = System.nanoTime();
                final long timeTaken = TimeValue.nsecToMSec(now - startTime);
                assertTrue(
                    "test didn't timeout quick enough, time taken: [" + timeTaken + "]",
                    timeTaken < TimeValue.timeValueSeconds(5).millis()
                );
                assertEquals(ex.getMessage(), "[][" + second.getAddress() + "] connect_timeout[1ms]");
            }
        }
    }

}
