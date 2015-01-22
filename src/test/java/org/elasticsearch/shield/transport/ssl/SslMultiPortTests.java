/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport.ssl;

import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.ShieldIntegrationTest;
import org.elasticsearch.test.ShieldSettingsSource;
import org.elasticsearch.transport.Transport;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.shield.transport.support.TransportProfileUtil.getProfilePort;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import static org.elasticsearch.test.ShieldSettingsSource.DEFAULT_USER_NAME;
import static org.elasticsearch.test.ShieldSettingsSource.DEFAULT_PASSWORD;
import static org.hamcrest.CoreMatchers.is;

@ClusterScope(scope = Scope.SUITE)
public class SslMultiPortTests extends ShieldIntegrationTest {

    private static int randomClientPort;
    private static int randomNonSslPort;
    private static int randomNoClientAuthPort;

    @BeforeClass
    public static void getRandomPort() {
        randomClientPort = randomIntBetween(49000, 65500); // ephemeral port
        randomNonSslPort = randomIntBetween(49000, 65500);
        randomNoClientAuthPort = randomIntBetween(49000, 65500);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        String randomClientPortRange = randomClientPort + "-" + (randomClientPort+100);
        String randomNonSslPortRange = randomNonSslPort + "-" + (randomNonSslPort+100);
        String randomNoClientAuthPortRange = randomNoClientAuthPort + "-" + (randomNoClientAuthPort+100);

        Path store;
        try {
            store = Paths.get(getClass().getResource("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode-client-profile.jks").toURI());
            assertThat(Files.exists(store), is(true));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                // client set up here
                .put("transport.profiles.client.port", randomClientPortRange)
                .put("transport.profiles.client.bind_host", "localhost") // make sure this is "localhost", no matter if ipv4 or ipv6, but be consistent
                .put("transport.profiles.client.shield.truststore.path", store.toAbsolutePath()) // settings for client truststore
                .put("transport.profiles.client.shield.truststore.password", "testnode-client-profile")
                .put("transport.profiles.no_ssl.port", randomNonSslPortRange)
                .put("transport.profiles.no_ssl.bind_host", "localhost")
                .put("transport.profiles.no_ssl.shield.ssl", "false")
                .put("transport.profiles.no_client_auth.port", randomNoClientAuthPortRange)
                .put("transport.profiles.no_client_auth.bind_host", "localhost")
                .put("transport.profiles.no_client_auth.shield.ssl.client.auth", false)
                .build();
    }

    @Override
    protected boolean sslTransportEnabled() {
        return true;
    }

    private TransportClient createTransportClient(Settings additionalSettings) {
        Settings settings = ImmutableSettings.builder().put(transportClientSettings())
                .put("name", "programmatic_transport_client")
                .put("cluster.name", internalCluster().getClusterName())
                .put(additionalSettings)
                .build();
        return new TransportClient(settings, false);
    }

    @Test
    public void testThatStandardTransportClientCanConnectToDefaultProfile() throws Exception {
        assertGreenClusterState(internalCluster().transportClient());
    }

    @Test
    public void testThatStandardTransportClientCanConnectToNoClientAuthProfile() throws Exception {
        try(TransportClient transportClient = createTransportClient(ImmutableSettings.EMPTY)) {
            transportClient.addTransportAddress(new InetSocketTransportAddress("localhost", getProfilePort("no_client_auth", internalCluster())));
            assertGreenClusterState(transportClient);
        }
    }

    @Test(expected = NoNodeAvailableException.class)
    public void testThatStandardTransportClientCannotConnectToClientProfile() throws Exception {
        try(TransportClient transportClient = createTransportClient(ImmutableSettings.EMPTY)) {
            transportClient.addTransportAddress(new InetSocketTransportAddress("localhost", getProfilePort("client", internalCluster())));
            transportClient.admin().cluster().prepareHealth().get();
        }
    }

    @Test(expected = NoNodeAvailableException.class)
    public void testThatStandardTransportClientCannotConnectToNoSslProfile() throws Exception {
        try (TransportClient transportClient = createTransportClient(ImmutableSettings.EMPTY)) {
            transportClient.addTransportAddress(new InetSocketTransportAddress("localhost", getProfilePort("no_ssl", internalCluster())));
            assertGreenClusterState(transportClient);
        }
    }

    @Test
    public void testThatProfileTransportClientCanConnectToClientProfile() throws Exception {
        Settings settings = ShieldSettingsSource.getSSLSettingsForStore("/org/elasticsearch/shield/transport/ssl/certs/simple/testclient-client-profile.jks", "testclient-client-profile");
        try (TransportClient transportClient = createTransportClient(settings)) {
            transportClient.addTransportAddress(new InetSocketTransportAddress("localhost", getProfilePort("client", internalCluster())));
            assertGreenClusterState(transportClient);
        }
    }

    @Test
    public void testThatProfileTransportClientCanConnectToNoClientAuthProfile() throws Exception {
        Settings settings = ShieldSettingsSource.getSSLSettingsForStore("/org/elasticsearch/shield/transport/ssl/certs/simple/testclient-client-profile.jks", "testclient-client-profile");
        try (TransportClient transportClient = createTransportClient(settings)) {
            transportClient.addTransportAddress(new InetSocketTransportAddress("localhost", getProfilePort("no_client_auth", internalCluster())));
            assertGreenClusterState(transportClient);
        }
    }

    @Test(expected = NoNodeAvailableException.class)
    public void testThatProfileTransportClientCannotConnectToDefaultProfile() throws Exception {
        Settings settings = ShieldSettingsSource.getSSLSettingsForStore("/org/elasticsearch/shield/transport/ssl/certs/simple/testclient-client-profile.jks", "testclient-client-profile");
        try (TransportClient transportClient = createTransportClient(settings)) {
            TransportAddress transportAddress = internalCluster().getInstance(Transport.class).boundAddress().boundAddress();
            transportClient.addTransportAddress(transportAddress);
            transportClient.admin().cluster().prepareHealth().get();
        }
    }

    @Test(expected = NoNodeAvailableException.class)
    public void testThatProfileTransportClientCannotConnectToNoSslProfile() throws Exception {
        Settings settings = ShieldSettingsSource.getSSLSettingsForStore("/org/elasticsearch/shield/transport/ssl/certs/simple/testclient-client-profile.jks", "testclient-client-profile");
        try (TransportClient transportClient = createTransportClient(settings)) {
            transportClient.addTransportAddress(new InetSocketTransportAddress("localhost", getProfilePort("no_ssl", internalCluster())));
            transportClient.admin().cluster().prepareHealth().get();
        }
    }

    @Test
    public void testThatTransportClientCanConnectToNoSslProfile() throws Exception {
        Settings settings = ImmutableSettings.builder()
                .put("shield.user", DEFAULT_USER_NAME + ":" + DEFAULT_PASSWORD)
                .put("cluster.name", internalCluster().getClusterName())
                .build();
        try (TransportClient transportClient = new TransportClient(settings, false)) {
            transportClient.addTransportAddress(new InetSocketTransportAddress("localhost", getProfilePort("no_ssl", internalCluster())));
            assertGreenClusterState(transportClient);
        }
    }

    @Test(expected = NoNodeAvailableException.class)
    public void testThatTransportClientCannotConnectToDefaultProfile() throws Exception {
        Settings settings = ImmutableSettings.builder()
                .put("shield.user", DEFAULT_USER_NAME + ":" + DEFAULT_PASSWORD)
                .put("cluster.name", internalCluster().getClusterName())
                .build();
        try (TransportClient transportClient = new TransportClient(settings, false)) {
            transportClient.addTransportAddress(new InetSocketTransportAddress("localhost", getProfilePort("default", internalCluster())));
            assertGreenClusterState(transportClient);
        }
    }

    @Test(expected = NoNodeAvailableException.class)
    public void testThatTransportClientCannotConnectToClientProfile() throws Exception {
        Settings settings = ImmutableSettings.builder()
                .put("shield.user", DEFAULT_USER_NAME + ":" + DEFAULT_PASSWORD)
                .put("cluster.name", internalCluster().getClusterName())
                .build();
        try (TransportClient transportClient = new TransportClient(settings, false)) {
            transportClient.addTransportAddress(new InetSocketTransportAddress("localhost", getProfilePort("client", internalCluster())));
            assertGreenClusterState(transportClient);
        }
    }

    @Test(expected = NoNodeAvailableException.class)
    public void testThatTransportClientCannotConnectToNoClientAuthProfile() throws Exception {
        Settings settings = ImmutableSettings.builder()
                .put("shield.user", DEFAULT_USER_NAME + ":" + DEFAULT_PASSWORD)
                .put("cluster.name", internalCluster().getClusterName())
                .build();
        try (TransportClient transportClient = new TransportClient(settings, false)) {
            transportClient.addTransportAddress(new InetSocketTransportAddress("localhost", getProfilePort("no_client_auth", internalCluster())));
            assertGreenClusterState(transportClient);
        }
    }

    @Test
    public void testThatTransportClientWithOnlyTruststoreCanConnectToNoClientAuthProfile() throws Exception {
        Settings settings = ImmutableSettings.builder()
                .put("shield.user", DEFAULT_USER_NAME + ":" + DEFAULT_PASSWORD)
                .put("cluster.name", internalCluster().getClusterName())
                .put("shield.transport.ssl", true)
                .put("shield.ssl.truststore.path", Paths.get(getClass().getResource("/org/elasticsearch/shield/transport/ssl/certs/simple/truststore-testnode-only.jks").toURI()))
                .put("shield.ssl.truststore.password", "truststore-testnode-only")
                .build();
        try (TransportClient transportClient = new TransportClient(settings, false)) {
            transportClient.addTransportAddress(new InetSocketTransportAddress("localhost", getProfilePort("no_client_auth", internalCluster())));
            assertGreenClusterState(transportClient);
        }
    }

    @Test(expected = NoNodeAvailableException.class)
    public void testThatTransportClientWithOnlyTruststoreCannotConnectToClientProfile() throws Exception {
        Settings settings = ImmutableSettings.builder()
                .put("shield.user", DEFAULT_USER_NAME + ":" + DEFAULT_PASSWORD)
                .put("cluster.name", internalCluster().getClusterName())
                .put("shield.transport.ssl", true)
                .put("shield.ssl.truststore.path", Paths.get(getClass().getResource("/org/elasticsearch/shield/transport/ssl/certs/simple/truststore-testnode-only.jks").toURI()))
                .put("shield.ssl.truststore.password", "truststore-testnode-only")
                .build();
        try (TransportClient transportClient = new TransportClient(settings, false)) {
            transportClient.addTransportAddress(new InetSocketTransportAddress("localhost", getProfilePort("client", internalCluster())));
            assertGreenClusterState(transportClient);
        }
    }

    @Test(expected = NoNodeAvailableException.class)
    public void testThatTransportClientWithOnlyTruststoreCannotConnectToDefaultProfile() throws Exception {
        Settings settings = ImmutableSettings.builder()
                .put("shield.user", DEFAULT_USER_NAME + ":" + DEFAULT_PASSWORD)
                .put("cluster.name", internalCluster().getClusterName())
                .put("shield.transport.ssl", true)
                .put("shield.ssl.truststore.path", Paths.get(getClass().getResource("/org/elasticsearch/shield/transport/ssl/certs/simple/truststore-testnode-only.jks").toURI()))
                .put("shield.ssl.truststore.password", "truststore-testnode-only")
                .build();
        try (TransportClient transportClient = new TransportClient(settings, false)) {
            transportClient.addTransportAddress(new InetSocketTransportAddress("localhost", getProfilePort("default", internalCluster())));
            assertGreenClusterState(transportClient);
        }
    }

    @Test(expected = NoNodeAvailableException.class)
    public void testThatTransportClientWithOnlyTruststoreCannotConnectToNoSslProfile() throws Exception {
        Settings settings = ImmutableSettings.builder()
                .put("shield.user", DEFAULT_USER_NAME + ":" + DEFAULT_PASSWORD)
                .put("cluster.name", internalCluster().getClusterName())
                .put("shield.transport.ssl", true)
                .put("shield.ssl.truststore.path", Paths.get(getClass().getResource("/org/elasticsearch/shield/transport/ssl/certs/simple/truststore-testnode-only.jks").toURI()))
                .put("shield.ssl.truststore.password", "truststore-testnode-only")
                .build();
        try (TransportClient transportClient = new TransportClient(settings, false)) {
            transportClient.addTransportAddress(new InetSocketTransportAddress("localhost", getProfilePort("no_ssl", internalCluster())));
            assertGreenClusterState(transportClient);
        }
    }
}
