/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport.netty;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ShieldIntegTestCase;
import org.elasticsearch.transport.TransportSettings;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map.Entry;

import static org.hamcrest.CoreMatchers.is;

public class IPHostnameVerificationTests extends ShieldIntegTestCase {
    Path keystore;

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
        Settings.Builder builder = Settings.builder();
        for (Entry<String, String> entry : settings.getAsMap().entrySet()) {
            if (entry.getKey().startsWith("xpack.security.ssl.") == false) {
                builder.put(entry.getKey(), entry.getValue());
            }
        }
        settings = builder.build();

        // The default Unicast test behavior is to use 'localhost' with the port number. For this test we need to use IP
        String[] unicastAddresses = settings.getAsArray("discovery.zen.ping.unicast.hosts");
        for (int i = 0; i < unicastAddresses.length; i++) {
            String address = unicastAddresses[i];
            unicastAddresses[i] = address.replace("localhost", "127.0.0.1");
        }

        Settings.Builder settingsBuilder = Settings.builder()
                .put(settings)
                .putArray("discovery.zen.ping.unicast.hosts", unicastAddresses);

        try {
            //This keystore uses a cert with a CN of "Elasticsearch Test Node" and IPv4+IPv6 ip addresses as SubjectAlternativeNames
            keystore = getDataPath("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode-ip-only.jks");
            assertThat(Files.exists(keystore), is(true));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return settingsBuilder.put("xpack.security.ssl.keystore.path", keystore.toAbsolutePath()) // settings for client truststore
                .put("xpack.security.ssl.keystore.password", "testnode-ip-only")
                .put("xpack.security.ssl.truststore.path", keystore.toAbsolutePath()) // settings for client truststore
                .put("xpack.security.ssl.truststore.password", "testnode-ip-only")
                .put(TransportSettings.BIND_HOST.getKey(), "127.0.0.1")
                .put("network.host", "127.0.0.1")
                .put("xpack.security.ssl.client.auth", "false")
                .put(ShieldNettyTransport.HOSTNAME_VERIFICATION_SETTING.getKey(), true)
                .put(ShieldNettyTransport.HOSTNAME_VERIFICATION_RESOLVE_NAME_SETTING.getKey(), false)
                .build();
    }

    @Override
    protected Settings transportClientSettings() {
        Settings clientSettings = super.transportClientSettings();
        Settings.Builder builder = Settings.builder();
        for (Entry<String, String> entry : clientSettings.getAsMap().entrySet()) {
            if (entry.getKey().startsWith("xpack.security.ssl.") == false) {
                builder.put(entry.getKey(), entry.getValue());
            }
        }
        clientSettings = builder.build();

        return Settings.builder().put(clientSettings)
                .put(ShieldNettyTransport.HOSTNAME_VERIFICATION_SETTING.getKey(), true)
                .put(ShieldNettyTransport.HOSTNAME_VERIFICATION_RESOLVE_NAME_SETTING.getKey(), false)
                .put("xpack.security.ssl.keystore.path", keystore.toAbsolutePath())
                .put("xpack.security.ssl.keystore.password", "testnode-ip-only")
                .put("xpack.security.ssl.truststore.path", keystore.toAbsolutePath())
                .put("xpack.security.ssl.truststore.password", "testnode-ip-only")
                .build();
    }

    public void testTransportClientConnectionWorksWithIPOnlyHostnameVerification() throws Exception {
        Client client = internalCluster().transportClient();
        assertGreenClusterState(client);
    }
}
