/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.transport.filter;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.xpack.core.common.socket.SocketAccess;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.is;

// no client nodes as they all get rejected on network connections
@ClusterScope(scope = Scope.SUITE, numDataNodes = 0, numClientNodes = 0)
public class IpFilteringIntegrationTests extends SecurityIntegTestCase {
    private static int randomClientPort;

    @BeforeClass
    public static void getRandomPort() {
        randomClientPort = randomIntBetween(49000, 65500); // ephemeral port
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        String randomClientPortRange = randomClientPort + "-" + (randomClientPort+100);
        return Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings))
                .put("transport.profiles.client.port", randomClientPortRange)
                // make sure this is "localhost", no matter if ipv4 or ipv6, but be consistent
                .put("transport.profiles.client.bind_host", "localhost")
                .put("transport.profiles.client.xpack.security.filter.deny", "_all")
                .put(IPFilter.TRANSPORT_FILTER_DENY_SETTING.getKey(), "_all")
                .build();
    }

    public void testThatIpFilteringIsIntegratedIntoNettyPipelineViaHttp() throws Exception {
        TransportAddress transportAddress =
                randomFrom(internalCluster().getDataNodeInstance(HttpServerTransport.class).boundAddress().boundAddresses());
        try (Socket socket = new Socket()){
            trySocketConnection(socket, transportAddress.address());
            assertThat(socket.isClosed(), is(true));
        }
    }

    public void testThatIpFilteringIsAppliedForProfile() throws Exception {
        try (Socket socket = new Socket()){
            trySocketConnection(socket, getProfileAddress("client"));
            assertThat(socket.isClosed(), is(true));
        }
    }

    @SuppressForbidden(reason = "Allow opening socket for test")
    private void trySocketConnection(Socket socket, InetSocketAddress address) throws IOException {
        logger.info("connecting to {}", address);
        SocketAccess.doPrivileged(() -> socket.connect(address, 5000));

        assertThat(socket.isConnected(), is(true));
        try (OutputStream os = socket.getOutputStream()) {
            os.write("fooooo".getBytes(StandardCharsets.UTF_8));
            os.flush();
        }
    }

    private static InetSocketAddress getProfileAddress(String profile) {
        TransportAddress transportAddress =
                randomFrom(internalCluster().getInstance(Transport.class).profileBoundAddresses().get(profile).boundAddresses());
        return transportAddress.address();
    }
}
