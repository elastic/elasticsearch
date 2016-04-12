/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport.netty;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.shield.Security;
import org.elasticsearch.test.ShieldIntegTestCase;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.xpack.XPackPlugin;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map.Entry;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsString;

public class SslHostnameVerificationTests extends ShieldIntegTestCase {

    @Override
    protected boolean sslTransportEnabled() {
        return true;
    }

    @Override
    protected boolean autoSSLEnabled() {
        return false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings settings = super.nodeSettings(nodeOrdinal);
        Settings.Builder settingsBuilder = Settings.builder();
        for (Entry<String, String> entry : settings.getAsMap().entrySet()) {
            if (entry.getKey().startsWith("xpack.security.ssl.") == false) {
                settingsBuilder.put(entry.getKey(), entry.getValue());
            }
        }
        Path keystore;
        try {
            /*
             * This keystore uses a cert without any subject alternative names and a CN of "Elasticsearch Test Node No SAN"
             * that will not resolve to a DNS name and will always cause hostname verification failures
             */
            keystore = getDataPath("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode-no-subjaltname.jks");
            assert keystore != null;
            assertThat(Files.exists(keystore), is(true));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return settingsBuilder.put("xpack.security.ssl.keystore.path", keystore.toAbsolutePath())
                .put("xpack.security.ssl.keystore.password", "testnode-no-subjaltname")
                .put("xpack.security.ssl.truststore.path", keystore.toAbsolutePath())
                .put("xpack.security.ssl.truststore.password", "testnode-no-subjaltname")
                // disable hostname verification as this test uses non-localhost addresses
                .put(ShieldNettyTransport.HOSTNAME_VERIFICATION_SETTING.getKey(), false)
                .build();
    }

    @Override
    protected Settings transportClientSettings() {
        Path keystore = getDataPath("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode-no-subjaltname.jks");
        assert keystore != null;
        Settings settings = super.transportClientSettings();
        // remove all ssl settings
        Settings.Builder builder = Settings.builder();
        for (Entry<String, String> entry : settings.getAsMap().entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(Security.setting("ssl.")) == false) {
                builder.put(key, entry.getValue());
            }
        }

        builder.put(ShieldNettyTransport.HOSTNAME_VERIFICATION_SETTING.getKey(), false)
                .put("xpack.security.ssl.keystore.path", keystore.toAbsolutePath()) // settings for client keystore
                .put("xpack.security.ssl.keystore.password", "testnode-no-subjaltname");

        if (randomBoolean()) {
            // randomly set the truststore, if not set the keystore should be used
            builder.put("xpack.security.ssl.truststore.path", keystore.toAbsolutePath())
                    .put("xpack.security.ssl.truststore.password", "testnode-no-subjaltname");
        }
        return builder.build();
    }

    public void testThatHostnameMismatchDeniesTransportClientConnection() throws Exception {
        Transport transport = internalCluster().getDataNodeInstance(Transport.class);
        TransportAddress transportAddress = transport.boundAddress().publishAddress();
        assertThat(transportAddress, instanceOf(InetSocketTransportAddress.class));
        InetSocketAddress inetSocketAddress = ((InetSocketTransportAddress) transportAddress).address();

        Settings settings = Settings.builder().put(transportClientSettings())
                .put(ShieldNettyTransport.HOSTNAME_VERIFICATION_SETTING.getKey(), true)
                .build();

        try (TransportClient client = TransportClient.builder().addPlugin(XPackPlugin.class).settings(settings).build()) {
            client.addTransportAddress(new InetSocketTransportAddress(inetSocketAddress.getAddress(), inetSocketAddress.getPort()));
            client.admin().cluster().prepareHealth().get();
            fail("Expected a NoNodeAvailableException due to hostname verification failures");
        } catch (NoNodeAvailableException e) {
            assertThat(e.getMessage(), containsString("None of the configured nodes are available: [{#transport#"));
        }
    }

    public void testTransportClientConnectionIgnoringHostnameVerification() throws Exception {
        Client client = internalCluster().transportClient();
        assertGreenClusterState(client);
    }
}
