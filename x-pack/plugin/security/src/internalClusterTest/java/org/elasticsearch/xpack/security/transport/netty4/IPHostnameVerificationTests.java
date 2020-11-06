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
import org.elasticsearch.transport.TransportSettings;
import org.elasticsearch.xpack.core.ssl.SSLClientAuth;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.discovery.SettingsBasedSeedHostsProvider.DISCOVERY_SEED_HOSTS_SETTING;
import static org.hamcrest.CoreMatchers.is;

// TODO delete this test?
public class IPHostnameVerificationTests extends SecurityIntegTestCase {
    private Path certPath;
    private Path keyPath;

    @Override
    protected boolean transportSSLEnabled() {
        return true;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings settings = super.nodeSettings(nodeOrdinal);
        Settings.Builder builder = Settings.builder()
                .put(settings.filter((s) -> s.startsWith("xpack.security.transport.ssl.") == false), false);
        settings = builder.build();

        // The default Unicast test behavior is to use 'localhost' with the port number. For this test we need to use IP
         List<String> newUnicastAddresses = new ArrayList<>();
         for (String address : settings.getAsList(DISCOVERY_SEED_HOSTS_SETTING.getKey())) {
             newUnicastAddresses.add(address.replace("localhost", "127.0.0.1"));
         }

        Settings.Builder settingsBuilder = Settings.builder()
                .put(settings)
                .putList(DISCOVERY_SEED_HOSTS_SETTING.getKey(), newUnicastAddresses);

        try {
            //Use a cert with a CN of "Elasticsearch Test Node" and IPv4+IPv6 ip addresses as SubjectAlternativeNames
            certPath = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode-ip-only.crt");
            keyPath = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode-ip-only.pem");
            assertThat(Files.exists(certPath), is(true));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        SecuritySettingsSource.addSecureSettings(settingsBuilder, secureSettings -> {
            secureSettings.setString("xpack.security.transport.ssl.secure_key_passphrase", "testnode-ip-only");
        });
        return settingsBuilder.put("xpack.security.transport.ssl.key", keyPath.toAbsolutePath())
            .put("xpack.security.transport.ssl.certificate", certPath.toAbsolutePath())
            .put("xpack.security.transport.ssl.certificate_authorities", certPath.toAbsolutePath())
            .put(TransportSettings.BIND_HOST.getKey(), "127.0.0.1")
            .put("network.host", "127.0.0.1")
            .put("xpack.security.transport.ssl.client_authentication", SSLClientAuth.NONE)
            .put("xpack.security.transport.ssl.verification_mode", "full")
            .build();
    }

    @Override
    protected Settings transportClientSettings() {
        Settings clientSettings = super.transportClientSettings();
        return Settings.builder().put(clientSettings.filter(k -> k.startsWith("xpack.security.transport.ssl.") == false))
            .put("xpack.security.transport.ssl.verification_mode", "certificate")
            .put("xpack.security.transport.ssl.key", keyPath.toAbsolutePath())
            .put("xpack.security.transport.ssl.certificate", certPath.toAbsolutePath())
            .put("xpack.security.transport.ssl.key_passphrase", "testnode-ip-only")
            .put("xpack.security.transport.ssl.certificate_authorities", certPath)
            .build();
    }

    public void testTransportClientConnectionWorksWithIPOnlyHostnameVerification() throws Exception {
        Client client = internalCluster().transportClient();
        assertGreenClusterState(client);
    }
}
