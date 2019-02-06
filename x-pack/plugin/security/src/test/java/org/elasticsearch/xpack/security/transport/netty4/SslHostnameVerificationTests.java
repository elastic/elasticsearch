/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport.netty4;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.xpack.core.TestXPackTransportClient;
import org.elasticsearch.xpack.security.LocalStateSecurity;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsString;

public class SslHostnameVerificationTests extends SecurityIntegTestCase {

    @Override
    protected boolean transportSSLEnabled() {
        return true;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings settings = super.nodeSettings(nodeOrdinal);
        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(settings.filter(
            k -> k.startsWith("xpack.security.transport.ssl.") == false || k.equals("xpack.security.transport.ssl.enabled")), false);
        Path keyPath;
        Path certPath;
        Path nodeCertPath;
        try {
            /*
             * This keystore uses a cert without any subject alternative names and a CN of "Elasticsearch Test Node No SAN"
             * that will not resolve to a DNS name and will always cause hostname verification failures
             */
            keyPath = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode-no-subjaltname.pem");
            certPath = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode-no-subjaltname.crt");
            nodeCertPath = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt");
            assert keyPath != null;
            assert certPath != null;
            assert nodeCertPath != null;
            assertThat(Files.exists(certPath), is(true));
            assertThat(Files.exists(nodeCertPath), is(true));
            assertThat(Files.exists(keyPath), is(true));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        SecuritySettingsSource.addSecureSettings(settingsBuilder, secureSettings -> {
            secureSettings.setString("xpack.ssl.secure_key_passphrase", "testnode-no-subjaltname");
        });
        return settingsBuilder.put("xpack.ssl.key", keyPath.toAbsolutePath())
            .put("xpack.ssl.certificate", certPath.toAbsolutePath())
            .putList("xpack.ssl.certificate_authorities", Arrays.asList(certPath.toString(), nodeCertPath.toString()))
                // disable hostname verification as this test uses certs without a valid SAN or DNS in the CN
                .put("xpack.ssl.verification_mode", "certificate")
                .build();
    }

    @Override
    protected Settings transportClientSettings() {
        Path keyPath;
        Path certPath;
        Path nodeCertPath;
        try {
            keyPath = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode-no-subjaltname.pem");
            certPath = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode-no-subjaltname.crt");
            nodeCertPath = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt");
            assert keyPath != null;
            assert certPath != null;
            assert nodeCertPath != null;
            assertThat(Files.exists(certPath), is(true));
            assertThat(Files.exists(nodeCertPath), is(true));
            assertThat(Files.exists(keyPath), is(true));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        Settings settings = super.transportClientSettings();
        // remove all ssl settings
        Settings.Builder builder = Settings.builder();
        builder.put(settings.filter(
            k -> k.startsWith("xpack.security.transport.ssl.") == false || k.equals("xpack.security.transport.ssl.enabled")), false);

        builder.put("xpack.ssl.verification_mode", "certificate")
            .put("xpack.ssl.key", keyPath.toAbsolutePath())
            .put("xpack.ssl.key_passphrase", "testnode-no-subjaltname")
            .put("xpack.ssl.certificate", certPath.toAbsolutePath())
            .putList("xpack.ssl.certificate_authorities", Arrays.asList(certPath.toString(), nodeCertPath.toString()));
        return builder.build();
    }

    public void testThatHostnameMismatchDeniesTransportClientConnection() throws Exception {
        Transport transport = internalCluster().getDataNodeInstance(Transport.class);
        TransportAddress transportAddress = transport.boundAddress().publishAddress();
        InetSocketAddress inetSocketAddress = transportAddress.address();

        Settings settings = Settings.builder().put(transportClientSettings())
                .put("xpack.ssl.verification_mode", "full")
                .build();

        try (TransportClient client = new TestXPackTransportClient(settings, LocalStateSecurity.class)) {
            client.addTransportAddress(new TransportAddress(inetSocketAddress.getAddress(), inetSocketAddress.getPort()));
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
