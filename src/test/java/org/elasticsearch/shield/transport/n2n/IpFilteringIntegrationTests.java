/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport.n2n;

import com.google.common.base.Charsets;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.shield.test.ShieldIntegrationTest;
import org.elasticsearch.transport.Transport;
import org.junit.Test;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

// no client nodes, no transport nodes, as they all get rejected on network connections
@ClusterScope(scope = Scope.SUITE, numDataNodes = 1, numClientNodes = 0, transportClientRatio = 0.0)
public class IpFilteringIntegrationTests extends ShieldIntegrationTest {

    private static final String CONFIG_IPFILTER_DENY_ALL = "deny: all\n";

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        ImmutableSettings.Builder builder = settingsBuilder().put(super.nodeSettings(nodeOrdinal));
        builder.put(InternalNode.HTTP_ENABLED, true);

        File folder = newFolder();
        builder.put("shield.transport.n2n.ip_filter.file", writeFile(folder, "ip_filter.yml", CONFIG_IPFILTER_DENY_ALL));

        return builder.build();
    }

    @Test(expected = SocketException.class)
    public void testThatIpFilteringIsIntegratedIntoNettyPipelineViaHttp() throws Exception {
        TransportAddress transportAddress = internalCluster().getDataNodeInstance(HttpServerTransport.class).boundAddress().boundAddress();
        assertThat(transportAddress, is(instanceOf(InetSocketTransportAddress.class)));
        InetSocketTransportAddress inetSocketTransportAddress = (InetSocketTransportAddress) transportAddress;

        trySocketConnection(inetSocketTransportAddress.address());
    }

    @Test(expected = SocketException.class)
    public void testThatIpFilteringIsIntegratedIntoNettyPipelineViaTransportClient() throws Exception {
        TransportAddress transportAddress = internalCluster().getDataNodeInstance(Transport.class).boundAddress().boundAddress();
        assertThat(transportAddress, is(instanceOf(InetSocketTransportAddress.class)));
        InetSocketTransportAddress inetSocketTransportAddress = (InetSocketTransportAddress) transportAddress;
        trySocketConnection(inetSocketTransportAddress.address());
    }

    private void trySocketConnection(InetSocketAddress address) throws Exception {
        try (Socket socket = new Socket()) {
            logger.info("Connecting to {}", address);
            socket.connect(address, 500);

            assertThat(socket.isConnected(), is(true));
            try (OutputStream os = socket.getOutputStream()) {
                os.write("foo".getBytes(Charsets.UTF_8));
                os.flush();
            }
            try (InputStream is = socket.getInputStream()) {
                is.read();
            }
        }
    }
}
