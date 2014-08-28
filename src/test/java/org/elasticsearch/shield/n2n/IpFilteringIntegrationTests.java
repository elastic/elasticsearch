/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.n2n;

import com.google.common.base.Charsets;
import com.google.common.net.InetAddresses;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.shield.test.ShieldIntegrationTest;
import org.elasticsearch.transport.Transport;
import org.junit.Test;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.Socket;
import java.net.SocketException;
import java.net.URL;
import java.util.Locale;

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
        File folder = newFolder();

        return settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("shield.n2n.file", writeFile(folder, "ip_filter.yml", CONFIG_IPFILTER_DENY_ALL))
                .build();
    }

    @Test(expected = SocketException.class)
    public void testThatIpFilteringIsIntegratedIntoNettyPipelineViaHttp() throws Exception {
        TransportAddress transportAddress = internalCluster().getDataNodeInstance(HttpServerTransport.class).boundAddress().boundAddress();
        assertThat(transportAddress, is(instanceOf(InetSocketTransportAddress.class)));
        InetSocketTransportAddress inetSocketTransportAddress = (InetSocketTransportAddress) transportAddress;
        String url = String.format(Locale.ROOT, "http://%s:%s/", InetAddresses.toUriString(inetSocketTransportAddress.address().getAddress()), inetSocketTransportAddress.address().getPort());

        logger.info("Opening connection to {}", url);
        HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
        connection.connect();
        logger.info("HTTP connection response code [{}]", connection.getResponseCode());
    }

    @Test(expected = SocketException.class)
    public void testThatIpFilteringIsIntegratedIntoNettyPipelineViaTransportClient() throws Exception {
        InetSocketTransportAddress transportAddress = (InetSocketTransportAddress) internalCluster().getDataNodeInstance(Transport.class).boundAddress().boundAddress();

        try (Socket socket = new Socket()) {
            logger.info("Connecting to {}", transportAddress.address());
            socket.connect(transportAddress.address(), 500);

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
