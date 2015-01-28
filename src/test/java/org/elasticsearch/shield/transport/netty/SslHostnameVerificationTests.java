/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport.netty;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import org.elasticsearch.test.ShieldIntegrationTest;
import org.elasticsearch.transport.Transport;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;

@ClusterScope(scope = Scope.SUITE)
public class SslHostnameVerificationTests extends ShieldIntegrationTest {

    static Path keystore;

    @Override
    protected boolean sslTransportEnabled() {
        return true;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        ImmutableSettings.Builder settingsBuilder = settingsBuilder().put(super.nodeSettings(nodeOrdinal));

        try {
            /*
             * This keystore uses a cert without any subject alternative names and a CN of "Elasticsearch Test Node No SAN"
             * that will not resolve to a DNS name and will always cause hostname verification failures
             */
            keystore = Paths.get(getClass().getResource("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode-no-subjaltname.jks").toURI());
            assertThat(Files.exists(keystore), is(true));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return settingsBuilder.put("shield.ssl.keystore.path", keystore.toAbsolutePath()) // settings for client truststore
                .put("shield.ssl.keystore.password", "testnode-no-subjaltname")
                .put("shield.ssl.truststore.path", keystore.toAbsolutePath()) // settings for client truststore
                .put("shield.ssl.truststore.password", "testnode-no-subjaltname")
                .put(ShieldNettyTransport.HOSTNAME_VERIFICATION_SETTING, false) // disable hostname verification as this test uses non-localhost addresses
                .build();
    }

    @Override
    protected Settings transportClientSettings() {
        return ImmutableSettings.builder().put(super.transportClientSettings())
                .put(ShieldNettyTransport.HOSTNAME_VERIFICATION_SETTING, false)
                .put("shield.ssl.truststore.path", keystore.toAbsolutePath()) // settings for client truststore
                .put("shield.ssl.truststore.password", "testnode-no-subjaltname")
                .build();
    }

    @Test(expected = NoNodeAvailableException.class)
    public void testThatHostnameMismatchDeniesTransportClientConnection() throws Exception {
        Transport transport = internalCluster().getDataNodeInstance(Transport.class);
        TransportAddress transportAddress = transport.boundAddress().publishAddress();
        assertThat(transportAddress, instanceOf(InetSocketTransportAddress.class));
        InetSocketAddress inetSocketAddress = ((InetSocketTransportAddress) transportAddress).address();

        Settings settings = ImmutableSettings.builder().put(transportClientSettings())
                .put(ShieldNettyTransport.HOSTNAME_VERIFICATION_SETTING, true)
                .build();

        try (TransportClient client = new TransportClient(settings, false)) {
            client.addTransportAddress(new InetSocketTransportAddress(inetSocketAddress.getHostName(), inetSocketAddress.getPort()));
            client.admin().cluster().prepareHealth().get();
            fail("Expected a NoNodeAvailableException due to hostname verification failures");
        }
    }

    @Test
    public void testTransportClientConnectionIgnoringHostnameVerification() throws Exception {
        Client client = internalCluster().transportClient();
        assertGreenClusterState(client);
    }
}
