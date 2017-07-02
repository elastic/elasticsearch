/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport.netty4;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.xpack.ssl.SSLClientAuth;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map.Entry;

import static org.hamcrest.CoreMatchers.is;

// TODO delete this test?
public class IPHostnameVerificationTests extends SecurityIntegTestCase {
    Path keystore;

    @Override
    protected boolean useGeneratedSSLConfig() {
        return false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings settings = super.nodeSettings(nodeOrdinal);
        Settings.Builder builder = Settings.builder()
                .put(settings.filter((s) -> s.startsWith("xpack.ssl.") == false).getAsMap());
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
            keystore = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode-ip-only.jks");
            assertThat(Files.exists(keystore), is(true));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        SecuritySettingsSource.addSecureSettings(settingsBuilder, secureSettings -> {
            secureSettings.setString("xpack.ssl.keystore.secure_password", "testnode-ip-only");
            secureSettings.setString("xpack.ssl.truststore.secure_password", "testnode-ip-only");
        });
        return settingsBuilder.put("xpack.ssl.keystore.path", keystore.toAbsolutePath()) // settings for client truststore
                .put("xpack.ssl.truststore.path", keystore.toAbsolutePath()) // settings for client truststore
                .put(TcpTransport.BIND_HOST.getKey(), "127.0.0.1")
                .put("network.host", "127.0.0.1")
                .put("xpack.ssl.client_authentication", SSLClientAuth.NONE)
                .put("xpack.ssl.verification_mode", "full")
                .build();
    }

    @Override
    protected Settings transportClientSettings() {
        Settings clientSettings = super.transportClientSettings();
        Settings.Builder builder = Settings.builder();
        for (Entry<String, String> entry : clientSettings.getAsMap().entrySet()) {
            if (entry.getKey().startsWith("xpack.ssl.") == false) {
                builder.put(entry.getKey(), entry.getValue());
            }
        }
        clientSettings = builder.build();

        return Settings.builder().put(clientSettings)
                .put("xpack.ssl.verification_mode", "certificate")
                .put("xpack.ssl.keystore.path", keystore.toAbsolutePath())
                .put("xpack.ssl.keystore.password", "testnode-ip-only")
                .put("xpack.ssl.truststore.path", keystore.toAbsolutePath())
                .put("xpack.ssl.truststore.password", "testnode-ip-only")
                .build();
    }

    public void testTransportClientConnectionWorksWithIPOnlyHostnameVerification() throws Exception {
        Client client = internalCluster().transportClient();
        assertGreenClusterState(client);
    }
}
