/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport.ssl;

import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.shield.Security;
import org.elasticsearch.shield.transport.netty.ShieldNettyTransport;
import org.elasticsearch.test.ShieldIntegTestCase;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.xpack.XPackPlugin;
import org.junit.BeforeClass;

import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map.Entry;

import static org.elasticsearch.test.ShieldSettingsSource.DEFAULT_PASSWORD;
import static org.elasticsearch.test.ShieldSettingsSource.DEFAULT_USER_NAME;
import static org.elasticsearch.test.ShieldSettingsSource.getSSLSettingsForStore;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsString;

public class SslMultiPortTests extends ShieldIntegTestCase {

    private static int randomClientPort;
    private static int randomNonSslPort;
    private static int randomNoClientAuthPort;

    @BeforeClass
    public static void getRandomPort() {
        randomClientPort = randomIntBetween(49000, 65500); // ephemeral port
        randomNonSslPort = randomIntBetween(49000, 65500);
        randomNoClientAuthPort = randomIntBetween(49000, 65500);
    }

    /**
     * On each node sets up the following profiles:
     * <ul>
     *     <li>default: testnode keystore. Requires client auth</li>
     *     <li>client: testnode-client-profile keystore that only trusts the testclient cert. Requires client auth</li>
     *     <li>no_client_auth: testnode keystore. Does not require client auth</li>
     *     <li>no_ssl: plaintext transport profile</li>
     * </ul>
     */
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        String randomClientPortRange = randomClientPort + "-" + (randomClientPort+100);
        String randomNonSslPortRange = randomNonSslPort + "-" + (randomNonSslPort+100);
        String randomNoClientAuthPortRange = randomNoClientAuthPort + "-" + (randomNoClientAuthPort+100);

        Path store;
        try {
            store = getDataPath("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode-client-profile.jks");
            assertThat(Files.exists(store), is(true));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                // client set up here
                .put("transport.profiles.client.port", randomClientPortRange)
                // make sure this is "localhost", no matter if ipv4 or ipv6, but be consistent
                .put("transport.profiles.client.bind_host", "localhost")
                .put("transport.profiles.client.xpack.security.truststore.path", store.toAbsolutePath()) // settings for client truststore
                .put("transport.profiles.client.xpack.security.truststore.password", "testnode-client-profile")
                .put("transport.profiles.no_ssl.port", randomNonSslPortRange)
                .put("transport.profiles.no_ssl.bind_host", "localhost")
                .put(randomFrom(
                        "transport.profiles.no_ssl.xpack.security.ssl.enabled", "transport.profiles.no_ssl.xpack.security.ssl"), "false")
                .put("transport.profiles.no_client_auth.port", randomNoClientAuthPortRange)
                .put("transport.profiles.no_client_auth.bind_host", "localhost")
                .put("transport.profiles.no_client_auth.xpack.security.ssl.client.auth", false)
                .build();
    }

    @Override
    protected boolean sslTransportEnabled() {
        return true;
    }

    @Override
    protected boolean autoSSLEnabled() {
        return false;
    }

    private TransportClient createTransportClient(Settings additionalSettings) {
        Settings clientSettings = transportClientSettings();
        if (additionalSettings.getByPrefix("xpack.security.ssl.").isEmpty() == false) {
            Settings.Builder builder = Settings.builder();
            for (Entry<String, String> entry : clientSettings.getAsMap().entrySet()) {
                if (entry.getKey().startsWith("xpack.security.ssl.") == false) {
                    builder.put(entry.getKey(), entry.getValue());
                }
            }
            clientSettings = builder.build();
        }

        Settings settings = Settings.builder().put(clientSettings)
                .put("node.name", "programmatic_transport_client")
                .put("cluster.name", internalCluster().getClusterName())
                .put(additionalSettings)
                .build();
        return TransportClient.builder().settings(settings)
                .addPlugin(XPackPlugin.class)
                .build();
    }

    /**
     * Uses the internal cluster's transport client to test connection to the default profile. The internal transport
     * client uses the same SSL settings as the default profile so a connection should always succeed
     */
    public void testThatStandardTransportClientCanConnectToDefaultProfile() throws Exception {
        assertGreenClusterState(internalCluster().transportClient());
    }

    /**
     * Uses a transport client with the same settings as the internal cluster transport client to test connection to the
     * no_client_auth profile. The internal transport client is not used here since we are connecting to a different
     * profile. Since the no_client_auth profile does not require client authentication, the standard transport client
     * connection should always succeed as the settings are the same as the default profile except for the port and
     * disabling the client auth requirement
     */
    public void testThatStandardTransportClientCanConnectToNoClientAuthProfile() throws Exception {
        try(TransportClient transportClient = createTransportClient(Settings.EMPTY)) {
            transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getLoopbackAddress(),
                    getProfilePort("no_client_auth")));
            assertGreenClusterState(transportClient);
        }
    }

    /**
     * Uses a transport client with the same settings as the internal cluster transport client to test connection to the
     * client profile. The internal transport client is not used here since we are connecting to a different
     * profile. The client profile requires client auth and only trusts the certificate in the testclient-client-profile
     * keystore so this connection will fail as the certificate presented by the standard transport client is not trusted
     * by this profile
     */
    public void testThatStandardTransportClientCannotConnectToClientProfile() throws Exception {
        try (TransportClient transportClient = createTransportClient(Settings.EMPTY)) {
            transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getLoopbackAddress(), getProfilePort("client")));
            transportClient.admin().cluster().prepareHealth().get();
            fail("Expected NoNodeAvailableException");
        } catch (NoNodeAvailableException e) {
            assertThat(e.getMessage(), containsString("None of the configured nodes are available: [{#transport#-"));
        }
    }

    /**
     * Uses a transport client with the same settings as the internal cluster transport client to test connection to the
     * no_ssl profile. The internal transport client is not used here since we are connecting to a different
     * profile. The no_ssl profile is plain text and the standard transport client uses SSL, so a connection will never work
     */
    public void testThatStandardTransportClientCannotConnectToNoSslProfile() throws Exception {
        try (TransportClient transportClient = createTransportClient(Settings.EMPTY)) {
            transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getLoopbackAddress(), getProfilePort("no_ssl")));
            assertGreenClusterState(transportClient);
            fail("Expected NoNodeAvailableException");
        } catch (NoNodeAvailableException e) {
            assertThat(e.getMessage(), containsString("None of the configured nodes are available: [{#transport#-"));
        }
    }

    /**
     * Uses a transport client with a custom keystore; this keystore testclient-client-profile.jks trusts the testnode
     * certificate and had its own self signed certificate. This test connects to the client profile, which is only
     * set to trust the testclient-client-profile certificate so the connection should always succeed
     */
    public void testThatProfileTransportClientCanConnectToClientProfile() throws Exception {
        Settings settings = getSSLSettingsForStore("/org/elasticsearch/shield/transport/ssl/certs/simple/testclient-client-profile.jks",
                "testclient-client-profile");
        try (TransportClient transportClient = createTransportClient(settings)) {
            transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getLoopbackAddress(), getProfilePort("client")));
            assertGreenClusterState(transportClient);
        }
    }

    /**
     * Uses a transport client with a custom keystore; this keystore testclient-client-profile.jks trusts the testnode
     * certificate and had its own self signed certificate. This test connects to the no_client_auth profile, which
     * uses a truststore that does not trust the testclient-client-profile certificate but does not require client
     * authentication
     */
    public void testThatProfileTransportClientCanConnectToNoClientAuthProfile() throws Exception {
        Settings settings = getSSLSettingsForStore("/org/elasticsearch/shield/transport/ssl/certs/simple/testclient-client-profile.jks",
                "testclient-client-profile");
        try (TransportClient transportClient = createTransportClient(settings)) {
            transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getLoopbackAddress(),
                    getProfilePort("no_client_auth")));
            assertGreenClusterState(transportClient);
        }
    }

    /**
     * Uses a transport client with a custom keystore; this keystore testclient-client-profile.jks trusts the testnode
     * certificate and had its own self signed certificate. This test connects to the default profile, which
     * uses a truststore that does not trust the testclient-client-profile certificate and requires client authentication
     * so the connection should always fail
     */
    public void testThatProfileTransportClientCannotConnectToDefaultProfile() throws Exception {
        Settings settings = getSSLSettingsForStore("/org/elasticsearch/shield/transport/ssl/certs/simple/testclient-client-profile.jks",
                "testclient-client-profile");
        try (TransportClient transportClient = createTransportClient(settings)) {
            TransportAddress transportAddress = randomFrom(internalCluster().getInstance(Transport.class).boundAddress().boundAddresses());
            transportClient.addTransportAddress(transportAddress);
            transportClient.admin().cluster().prepareHealth().get();
            fail("Expected NoNodeAvailableException");
        } catch (NoNodeAvailableException e) {
            assertThat(e.getMessage(), containsString("None of the configured nodes are available: [{#transport#-"));
        }
    }

    /**
     * Uses a transport client with a custom keystore; this keystore testclient-client-profile.jks trusts the testnode
     * certificate and had its own self signed certificate. This test connects to the no_ssl profile, which does not
     * use SSL so the connection will never work
     */
    public void testThatProfileTransportClientCannotConnectToNoSslProfile() throws Exception {
        Settings settings = getSSLSettingsForStore("/org/elasticsearch/shield/transport/ssl/certs/simple/testclient-client-profile.jks",
                "testclient-client-profile");
        try (TransportClient transportClient = createTransportClient(settings)) {
            transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getLoopbackAddress(), getProfilePort("no_ssl")));
            transportClient.admin().cluster().prepareHealth().get();
            fail("Expected NoNodeAvailableException");
        } catch (NoNodeAvailableException e) {
            assertThat(e.getMessage(), containsString("None of the configured nodes are available: [{#transport#-"));
        }
    }

    /**
     * Uses a transport client with SSL disabled. This test connects to the no_ssl profile, which should always succeed
     */
    public void testThatTransportClientCanConnectToNoSslProfile() throws Exception {
        Settings settings = Settings.builder()
                .put(Security.USER_SETTING.getKey(), DEFAULT_USER_NAME + ":" + DEFAULT_PASSWORD)
                .put(ShieldNettyTransport.SSL_SETTING.getKey(), false)
                .put("cluster.name", internalCluster().getClusterName())
                .build();
        try (TransportClient transportClient = TransportClient.builder().settings(settings).addPlugin(XPackPlugin.class).build()) {
            transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getLoopbackAddress(), getProfilePort("no_ssl")));
            assertGreenClusterState(transportClient);
        }
    }

    /**
     * Uses a transport client with SSL disabled. This test connects to the default profile, which should always fail
     * as a non-ssl transport client cannot connect to a ssl profile
     */
    public void testThatTransportClientCannotConnectToDefaultProfile() throws Exception {
        Settings settings = Settings.builder()
                .put(Security.USER_SETTING.getKey(), DEFAULT_USER_NAME + ":" + DEFAULT_PASSWORD)
                .put("cluster.name", internalCluster().getClusterName())
                .build();
        try (TransportClient transportClient = TransportClient.builder().addPlugin(XPackPlugin.class).settings(settings).build()) {
            transportClient.addTransportAddress(randomFrom(internalCluster().getInstance(Transport.class).boundAddress().boundAddresses()));
            assertGreenClusterState(transportClient);
            fail("Expected NoNodeAvailableException");
        } catch (NoNodeAvailableException e) {
            assertThat(e.getMessage(), containsString("None of the configured nodes are available: [{#transport#-"));
        }
    }

    /**
     * Uses a transport client with SSL disabled. This test connects to the client profile, which should always fail
     * as a non-ssl transport client cannot connect to a ssl profile
     */
    public void testThatTransportClientCannotConnectToClientProfile() throws Exception {
        Settings settings = Settings.builder()
                .put(Security.USER_SETTING.getKey(), DEFAULT_USER_NAME + ":" + DEFAULT_PASSWORD)
                .put("cluster.name", internalCluster().getClusterName())
                .build();
        try (TransportClient transportClient = TransportClient.builder().addPlugin(XPackPlugin.class).settings(settings).build()) {
            transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getLoopbackAddress(), getProfilePort("client")));
            assertGreenClusterState(transportClient);
            fail("Expected NoNodeAvailableException");
        } catch (NoNodeAvailableException e) {
            assertThat(e.getMessage(), containsString("None of the configured nodes are available: [{#transport#-"));
        }
    }

    /**
     * Uses a transport client with SSL disabled. This test connects to the no_client_auth profile, which should always fail
     * as a non-ssl transport client cannot connect to a ssl profile
     */
    public void testThatTransportClientCannotConnectToNoClientAuthProfile() throws Exception {
        Settings settings = Settings.builder()
                .put(Security.USER_SETTING.getKey(), DEFAULT_USER_NAME + ":" + DEFAULT_PASSWORD)
                .put("cluster.name", internalCluster().getClusterName())
                .build();
        try (TransportClient transportClient = TransportClient.builder().addPlugin(XPackPlugin.class).settings(settings).build()) {
            transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getLoopbackAddress(),
                    getProfilePort("no_client_auth")));
            assertGreenClusterState(transportClient);
            fail("Expected NoNodeAvailableException");
        } catch (NoNodeAvailableException e) {
            assertThat(e.getMessage(), containsString("None of the configured nodes are available: [{#transport#-"));
        }
    }

    /**
     * Uses a transport client with a custom truststore; this truststore truststore-testnode-only only trusts the testnode
     * certificate and contains no other certification. This test connects to the no_client_auth profile, which uses
     * the testnode certificate and does not require to present a certificate, so this connection should always succeed
     */
    public void testThatTransportClientWithOnlyTruststoreCanConnectToNoClientAuthProfile() throws Exception {
        Settings settings = Settings.builder()
                .put(Security.USER_SETTING.getKey(), DEFAULT_USER_NAME + ":" + DEFAULT_PASSWORD)
                .put("cluster.name", internalCluster().getClusterName())
                .put(ShieldNettyTransport.SSL_SETTING.getKey(), true)
                .put("xpack.security.ssl.truststore.path",
                        getDataPath("/org/elasticsearch/shield/transport/ssl/certs/simple/truststore-testnode-only.jks"))
                .put("xpack.security.ssl.truststore.password", "truststore-testnode-only")
                .build();
        try (TransportClient transportClient = TransportClient.builder().settings(settings).addPlugin(XPackPlugin.class).build()) {
            transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getLoopbackAddress(),
                    getProfilePort("no_client_auth")));
            assertGreenClusterState(transportClient);
        }
    }

    /**
     * Uses a transport client with a custom truststore; this truststore truststore-testnode-only only trusts the testnode
     * certificate and contains no other certification. This test connects to the client profile, which uses
     * the testnode certificate and requires the client to present a certificate, so this connection will never work as
     * the client has no certificate to present
     */
    public void testThatTransportClientWithOnlyTruststoreCannotConnectToClientProfile() throws Exception {
        Settings settings = Settings.builder()
                .put(Security.USER_SETTING.getKey(), DEFAULT_USER_NAME + ":" + DEFAULT_PASSWORD)
                .put("cluster.name", internalCluster().getClusterName())
                .put(ShieldNettyTransport.SSL_SETTING.getKey(), true)
                .put("xpack.security.ssl.truststore.path",
                        getDataPath("/org/elasticsearch/shield/transport/ssl/certs/simple/truststore-testnode-only.jks"))
                .put("xpack.security.ssl.truststore.password", "truststore-testnode-only")
                .build();
        try (TransportClient transportClient = TransportClient.builder().addPlugin(XPackPlugin.class).settings(settings).build()) {
            transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getLoopbackAddress(), getProfilePort("client")));
            assertGreenClusterState(transportClient);
            fail("Expected NoNodeAvailableException");
        } catch (NoNodeAvailableException e) {
            assertThat(e.getMessage(), containsString("None of the configured nodes are available: [{#transport#-"));
        }
    }

    /**
     * Uses a transport client with a custom truststore; this truststore truststore-testnode-only only trusts the testnode
     * certificate and contains no other certification. This test connects to the default profile, which uses
     * the testnode certificate and requires the client to present a certificate, so this connection will never work as
     * the client has no certificate to present
     */
    public void testThatTransportClientWithOnlyTruststoreCannotConnectToDefaultProfile() throws Exception {
        Settings settings = Settings.builder()
                .put(Security.USER_SETTING.getKey(), DEFAULT_USER_NAME + ":" + DEFAULT_PASSWORD)
                .put("cluster.name", internalCluster().getClusterName())
                .put(ShieldNettyTransport.SSL_SETTING.getKey(), true)
                .put("xpack.security.ssl.truststore.path",
                        getDataPath("/org/elasticsearch/shield/transport/ssl/certs/simple/truststore-testnode-only.jks"))
                .put("xpack.security.ssl.truststore.password", "truststore-testnode-only")
                .build();
        try (TransportClient transportClient = TransportClient.builder().addPlugin(XPackPlugin.class).settings(settings).build()) {
            transportClient.addTransportAddress(randomFrom(internalCluster().getInstance(Transport.class).boundAddress().boundAddresses()));
                    assertGreenClusterState(transportClient);
            fail("Expected NoNodeAvailableException");
        } catch (NoNodeAvailableException e) {
            assertThat(e.getMessage(), containsString("None of the configured nodes are available: [{#transport#-"));
        }
    }

    /**
     * Uses a transport client with a custom truststore; this truststore truststore-testnode-only only trusts the testnode
     * certificate and contains no other certification. This test connects to the no_ssl profile, which does not use
     * SSL so the connection should never succeed
     */
    public void testThatTransportClientWithOnlyTruststoreCannotConnectToNoSslProfile() throws Exception {
        Settings settings = Settings.builder()
                .put(Security.USER_SETTING.getKey(), DEFAULT_USER_NAME + ":" + DEFAULT_PASSWORD)
                .put("cluster.name", internalCluster().getClusterName())
                .put(ShieldNettyTransport.SSL_SETTING.getKey(), true)
                .put("xpack.security.ssl.truststore.path",
                        getDataPath("/org/elasticsearch/shield/transport/ssl/certs/simple/truststore-testnode-only.jks"))
                .put("xpack.security.ssl.truststore.password", "truststore-testnode-only")
                .build();
        try (TransportClient transportClient = TransportClient.builder().addPlugin(XPackPlugin.class).settings(settings).build()) {
            transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getLoopbackAddress(), getProfilePort("no_ssl")));
            assertGreenClusterState(transportClient);
            fail("Expected NoNodeAvailableException");
        } catch (NoNodeAvailableException e) {
            assertThat(e.getMessage(), containsString("None of the configured nodes are available: [{#transport#-"));
        }
    }

    /**
     * Uses a transport client with the default JDK truststore; this truststore only trusts the known good public
     * certificate authorities. This test connects to the default profile, which uses a self-signed certificate that
     * will never be trusted by the default truststore so the connection should always fail
     */
    public void testThatSSLTransportClientWithNoTruststoreCannotConnectToDefaultProfile() throws Exception {
        Settings settings = Settings.builder()
                .put(Security.USER_SETTING.getKey(), DEFAULT_USER_NAME + ":" + DEFAULT_PASSWORD)
                .put("cluster.name", internalCluster().getClusterName())
                .put(ShieldNettyTransport.SSL_SETTING.getKey(), true)
                .build();
        try (TransportClient transportClient = TransportClient.builder().addPlugin(XPackPlugin.class).settings(settings).build()) {
            transportClient.addTransportAddress(randomFrom(internalCluster().getInstance(Transport.class).boundAddress().boundAddresses()));
            assertGreenClusterState(transportClient);
            fail("Expected NoNodeAvailableException");
        } catch (NoNodeAvailableException e) {
            assertThat(e.getMessage(), containsString("None of the configured nodes are available: [{#transport#-"));
        }
    }

    /**
     * Uses a transport client with the default JDK truststore; this truststore only trusts the known good public
     * certificate authorities. This test connects to the client profile, which uses a self-signed certificate that
     * will never be trusted by the default truststore so the connection should always fail
     */
    public void testThatSSLTransportClientWithNoTruststoreCannotConnectToClientProfile() throws Exception {
        Settings settings = Settings.builder()
                .put(Security.USER_SETTING.getKey(), DEFAULT_USER_NAME + ":" + DEFAULT_PASSWORD)
                .put("cluster.name", internalCluster().getClusterName())
                .put(ShieldNettyTransport.SSL_SETTING.getKey(), true)
                .build();
        try (TransportClient transportClient = TransportClient.builder().addPlugin(XPackPlugin.class).settings(settings).build()) {
            transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getLoopbackAddress(), getProfilePort("client")));
            assertGreenClusterState(transportClient);
            fail("Expected NoNodeAvailableException");
        } catch (NoNodeAvailableException e) {
            assertThat(e.getMessage(), containsString("None of the configured nodes are available: [{#transport#-"));
        }
    }

    /**
     * Uses a transport client with the default JDK truststore; this truststore only trusts the known good public
     * certificate authorities. This test connects to the no_client_auth profile, which uses a self-signed certificate that
     * will never be trusted by the default truststore so the connection should always fail
     */
    public void testThatSSLTransportClientWithNoTruststoreCannotConnectToNoClientAuthProfile() throws Exception {
        Settings settings = Settings.builder()
                .put(Security.USER_SETTING.getKey(), DEFAULT_USER_NAME + ":" + DEFAULT_PASSWORD)
                .put("cluster.name", internalCluster().getClusterName())
                .put(ShieldNettyTransport.SSL_SETTING.getKey(), true)
                .build();
        try (TransportClient transportClient = TransportClient.builder().addPlugin(XPackPlugin.class).settings(settings).build()) {
            transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getLoopbackAddress(),
                    getProfilePort("no_client_auth")));
            assertGreenClusterState(transportClient);
            fail("Expected NoNodeAvailableException");
        } catch (NoNodeAvailableException e) {
            assertThat(e.getMessage(), containsString("None of the configured nodes are available: [{#transport#-"));
        }
    }

    /**
     * Uses a transport client with the default JDK truststore; this truststore only trusts the known good public
     * certificate authorities. This test connects to the no_ssl profile, which does not use SSL so the connection
     * will not work
     */
    public void testThatSSLTransportClientWithNoTruststoreCannotConnectToNoSslProfile() throws Exception {
        Settings settings = Settings.builder()
                .put(Security.USER_SETTING.getKey(), DEFAULT_USER_NAME + ":" + DEFAULT_PASSWORD)
                .put("cluster.name", internalCluster().getClusterName())
                .put(ShieldNettyTransport.SSL_SETTING.getKey(), true)
                .build();
        try (TransportClient transportClient = TransportClient.builder().addPlugin(XPackPlugin.class).settings(settings).build()) {
            transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getLoopbackAddress(), getProfilePort("no_ssl")));
            assertGreenClusterState(transportClient);
            fail("Expected NoNodeAvailableException");
        } catch (NoNodeAvailableException e) {
            assertThat(e.getMessage(), containsString("None of the configured nodes are available: [{#transport#-"));
        }
    }

    private static int getProfilePort(String profile) {
        TransportAddress transportAddress =
                randomFrom(internalCluster().getInstance(Transport.class).profileBoundAddresses().get(profile).boundAddresses());
        assert transportAddress instanceof InetSocketTransportAddress;
        return ((InetSocketTransportAddress)transportAddress).address().getPort();
    }
}
