/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport.ssl;

import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.xpack.core.TestXPackTransportClient;
import org.elasticsearch.xpack.core.security.SecurityField;
import org.elasticsearch.xpack.core.ssl.SSLClientAuth;
import org.elasticsearch.xpack.security.LocalStateSecurity;
import org.junit.BeforeClass;

import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;

import static org.elasticsearch.test.SecuritySettingsSource.TEST_USER_NAME;
import static org.elasticsearch.test.SecuritySettingsSource.addSSLSettingsForPEMFiles;
import static org.elasticsearch.test.SecuritySettingsSourceField.TEST_PASSWORD;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsString;

public class SslMultiPortTests extends SecurityIntegTestCase {

    private static int randomClientPort;
    private static int randomNoClientAuthPort;

    @BeforeClass
    public static void getRandomPort() {
        randomClientPort = randomIntBetween(49000, 65500); // ephemeral port
        randomNoClientAuthPort = randomIntBetween(49000, 65500);
    }

    /**
     * On each node sets up the following profiles:
     * <ul>
     *     <li>default: testnode keypair. Requires client auth</li>
     *     <li>client: testnode-client-profile profile  that only trusts the testclient cert. Requires client auth</li>
     *     <li>no_client_auth: testnode keypair. Does not require client auth</li>
     * </ul>
     */
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        String randomClientPortRange = randomClientPort + "-" + (randomClientPort+100);
        String randomNoClientAuthPortRange = randomNoClientAuthPort + "-" + (randomNoClientAuthPort+100);

        Path trustCert;
        try {
            trustCert = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient-client-profile.crt");
            assertThat(Files.exists(trustCert), is(true));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        Settings settings = Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            // client set up here
            .put("transport.profiles.client.port", randomClientPortRange)
            // make sure this is "localhost", no matter if ipv4 or ipv6, but be consistent
            .put("transport.profiles.client.bind_host", "localhost")
            .put("transport.profiles.client.xpack.security.ssl.certificate_authorities", trustCert.toAbsolutePath())
            .put("transport.profiles.no_client_auth.port", randomNoClientAuthPortRange)
            .put("transport.profiles.no_client_auth.bind_host", "localhost")
            .put("transport.profiles.no_client_auth.xpack.security.ssl.client_authentication", SSLClientAuth.NONE)
            .build();
        logger.info("node {} settings:\n{}", nodeOrdinal, settings);
        return settings;
    }

    @Override
    protected boolean transportSSLEnabled() {
        return true;
    }

    private TransportClient createTransportClient(Settings additionalSettings) {
        Settings settings = Settings.builder()
                .put(transportClientSettings().filter(s -> s.startsWith("xpack.ssl") == false))
                .put("node.name", "programmatic_transport_client")
                .put("cluster.name", internalCluster().getClusterName())
                .put("xpack.security.transport.ssl.enabled", true)
                .put(additionalSettings)
                .build();
        //return new TestXPackTransportClient(settings, LocalStateSecurity.class);
        logger.info("transport client settings:\n{}", settings);
        return new TestXPackTransportClient(settings, LocalStateSecurity.class);
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
        try(TransportClient transportClient = new TestXPackTransportClient(Settings.builder()
                .put(transportClientSettings())
                .put("xpack.security.transport.ssl.enabled", true)
                .put("node.name", "programmatic_transport_client")
                .put("cluster.name", internalCluster().getClusterName())
                .build(), LocalStateSecurity.class)) {
            transportClient.addTransportAddress(new TransportAddress(InetAddress.getLoopbackAddress(),
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
            transportClient.addTransportAddress(new TransportAddress(InetAddress.getLoopbackAddress(), getProfilePort("client")));
            transportClient.admin().cluster().prepareHealth().get();
            fail("Expected NoNodeAvailableException");
        } catch (NoNodeAvailableException e) {
            assertThat(e.getMessage(), containsString("None of the configured nodes are available: [{#transport#-"));
        }
    }

    /**
     * Uses a transport client with a custom key pair; TransportClient only trusts the testnode
     * certificate and had its own self signed certificate. This test connects to the client profile, which is only
     * set to trust the testclient-client-profile certificate so the connection should always succeed
     */
    public void testThatProfileTransportClientCanConnectToClientProfile() throws Exception {
        Settings.Builder builder = Settings.builder();
        addSSLSettingsForPEMFiles(
            builder,
            "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient-client-profile.pem",
            "testclient-client-profile",
            "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient-client-profile.crt",
            Arrays.asList("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"));
        try (TransportClient transportClient = createTransportClient(builder.build())) {
            transportClient.addTransportAddress(new TransportAddress(InetAddress.getLoopbackAddress(), getProfilePort("client")));
            assertGreenClusterState(transportClient);
        }
    }

    /**
     * Uses a transport client with a custom key pair; TransportClient only trusts the testnode
     * certificate and had its own self signed certificate. This test connects to the no_client_auth profile, which
     * uses a truststore that does not trust the testclient-client-profile certificate but does not require client
     * authentication
     */
    public void testThatProfileTransportClientCanConnectToNoClientAuthProfile() throws Exception {
        Settings.Builder builder = Settings.builder();
        addSSLSettingsForPEMFiles(
            builder,
            "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient-client-profile.pem",
            "testclient-client-profile",
            "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient-client-profile.crt",
            Arrays.asList("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"));
        try (TransportClient transportClient = createTransportClient(builder.build())) {
            transportClient.addTransportAddress(new TransportAddress(InetAddress.getLoopbackAddress(),
                    getProfilePort("no_client_auth")));
            assertGreenClusterState(transportClient);
        }
    }

    /**
     * Uses a transport client with a custom key pair; TransportClient only trusts the testnode
     * certificate and had its own self signed certificate. This test connects to the default profile, which
     * uses a truststore that does not trust the testclient-client-profile certificate and requires client authentication
     * so the connection should always fail
     */
    public void testThatProfileTransportClientCannotConnectToDefaultProfile() throws Exception {
        Settings.Builder builder = Settings.builder();
        addSSLSettingsForPEMFiles(
            builder,
            "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient-client-profile.pem",
            "testclient-client-profile",
            "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient-client-profile.crt",
            Arrays.asList("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"));
        try (TransportClient transportClient = createTransportClient(builder.build())) {
            TransportAddress transportAddress = randomFrom(internalCluster().getInstance(Transport.class).boundAddress().boundAddresses());
            transportClient.addTransportAddress(transportAddress);
            transportClient.admin().cluster().prepareHealth().get();
            fail("Expected NoNodeAvailableException");
        } catch (NoNodeAvailableException e) {
            assertThat(e.getMessage(), containsString("None of the configured nodes are available: [{#transport#-"));
        }
    }

    /**
     * Uses a transport client with SSL disabled. This test connects to the default profile, which should always fail
     * as a non-ssl transport client cannot connect to a ssl profile
     */
    public void testThatTransportClientCannotConnectToDefaultProfile() throws Exception {
        Settings settings = Settings.builder()
                .put(SecurityField.USER_SETTING.getKey(), TEST_USER_NAME + ":" + TEST_PASSWORD)
                .put("cluster.name", internalCluster().getClusterName())
                .build();
        try (TransportClient transportClient = new TestXPackTransportClient(settings,
                                                                            Collections.singletonList(LocalStateSecurity.class))) {
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
                .put(SecurityField.USER_SETTING.getKey(), TEST_USER_NAME + ":" + TEST_PASSWORD)
                .put("cluster.name", internalCluster().getClusterName())
                .build();
        try (TransportClient transportClient = new TestXPackTransportClient(settings,
                                                                            Collections.singletonList(LocalStateSecurity.class))) {
            transportClient.addTransportAddress(new TransportAddress(InetAddress.getLoopbackAddress(), getProfilePort("client")));
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
                .put(SecurityField.USER_SETTING.getKey(), TEST_USER_NAME + ":" + TEST_PASSWORD)
                .put("cluster.name", internalCluster().getClusterName())
                .build();
        try (TransportClient transportClient = new TestXPackTransportClient(settings,
                                                                            Collections.singletonList(LocalStateSecurity.class))) {
            transportClient.addTransportAddress(new TransportAddress(InetAddress.getLoopbackAddress(),
                    getProfilePort("no_client_auth")));
            assertGreenClusterState(transportClient);
            fail("Expected NoNodeAvailableException");
        } catch (NoNodeAvailableException e) {
            assertThat(e.getMessage(), containsString("None of the configured nodes are available: [{#transport#-"));
        }
    }

    /**
     * Uses a transport client that only trusts the testnode certificate. This test connects to the no_client_auth profile,
     * which uses  the testnode certificate and does not require to present a certificate, so this connection should always succeed
     */
    public void testThatTransportClientWithOnlyTruststoreCanConnectToNoClientAuthProfile() throws Exception {
        Settings settings = Settings.builder()
            .put(SecurityField.USER_SETTING.getKey(), TEST_USER_NAME + ":" + TEST_PASSWORD)
            .put("cluster.name", internalCluster().getClusterName())
            .put("xpack.security.transport.ssl.enabled", true)
            .put("xpack.ssl.certificate_authorities",
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"))
            .build();
        try (TransportClient transportClient = new TestXPackTransportClient(settings,
                                                                            Collections.singletonList(LocalStateSecurity.class))) {
            transportClient.addTransportAddress(new TransportAddress(InetAddress.getLoopbackAddress(),
                    getProfilePort("no_client_auth")));
        }
    }

    /**
     * Uses a transport client that only trusts the testnode certificate. This test connects to the client profile, which uses
     * the testnode certificate and requires the client to present a certificate, so this connection will never work as
     * the client has no certificate to present
     */
    public void testThatTransportClientWithOnlyTruststoreCannotConnectToClientProfile() throws Exception {
        Settings settings = Settings.builder()
            .put(SecurityField.USER_SETTING.getKey(), TEST_USER_NAME + ":" + TEST_PASSWORD)
            .put("cluster.name", internalCluster().getClusterName())
            .put("xpack.security.transport.ssl.enabled", true)
            .put("xpack.ssl.client_authentication", SSLClientAuth.REQUIRED)
            .put("xpack.ssl.certificate_authorities",
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"))
            .build();
        try (TransportClient transportClient = new TestXPackTransportClient(settings,
                                                                            Collections.singletonList(LocalStateSecurity.class))) {
            transportClient.addTransportAddress(new TransportAddress(InetAddress.getLoopbackAddress(), getProfilePort("client")));
            assertGreenClusterState(transportClient);
            fail("Expected NoNodeAvailableException");
        } catch (NoNodeAvailableException e) {
            assertThat(e.getMessage(), containsString("None of the configured nodes are available: [{#transport#-"));
        }
    }

    /**
     * Uses a transport client that only trusts the testnode certificate. This test connects to the default profile, which uses
     * the testnode certificate and requires the client to present a certificate, so this connection will never work as
     * the client has no certificate to present
     */
    public void testThatTransportClientWithOnlyTruststoreCannotConnectToDefaultProfile() throws Exception {
        Settings settings = Settings.builder()
            .put(SecurityField.USER_SETTING.getKey(), TEST_USER_NAME + ":" + TEST_PASSWORD)
            .put("cluster.name", internalCluster().getClusterName())
            .put("xpack.security.transport.ssl.enabled", true)
            .put("xpack.ssl.client_authentication", SSLClientAuth.REQUIRED)
            .put("xpack.ssl.certificate_authorities",
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"))
            .build();
        try (TransportClient transportClient = new TestXPackTransportClient(settings,
                                                                            Collections.singletonList(LocalStateSecurity.class))) {
            transportClient.addTransportAddress(randomFrom(internalCluster().getInstance(Transport.class).boundAddress().boundAddresses()));
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
            .put(SecurityField.USER_SETTING.getKey(), TEST_USER_NAME + ":" + TEST_PASSWORD)
            .put("cluster.name", internalCluster().getClusterName())
            .put("xpack.ssl.client_authentication", SSLClientAuth.REQUIRED)
            .put("xpack.security.transport.ssl.enabled", true)
            .build();
        try (TransportClient transportClient = new TestXPackTransportClient(settings,
                                                                            Collections.singletonList(LocalStateSecurity.class))) {
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
            .put(SecurityField.USER_SETTING.getKey(), TEST_USER_NAME + ":" + TEST_PASSWORD)
            .put("cluster.name", internalCluster().getClusterName())
            .put("xpack.ssl.client_authentication", SSLClientAuth.REQUIRED)
            .put("xpack.security.transport.ssl.enabled", true)
            .build();
        try (TransportClient transportClient = new TestXPackTransportClient(settings,
                                                                            Collections.singletonList(LocalStateSecurity.class))) {
            transportClient.addTransportAddress(new TransportAddress(InetAddress.getLoopbackAddress(), getProfilePort("client")));
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
            .put(SecurityField.USER_SETTING.getKey(), TEST_USER_NAME + ":" + TEST_PASSWORD)
            .put("cluster.name", internalCluster().getClusterName())
            .put("xpack.ssl.client_authentication", SSLClientAuth.REQUIRED)
            .put("xpack.security.transport.ssl.enabled", true)
            .build();
        try (TransportClient transportClient = new TestXPackTransportClient(settings,
                                                                            Collections.singletonList(LocalStateSecurity.class))) {
            transportClient.addTransportAddress(new TransportAddress(InetAddress.getLoopbackAddress(),
                    getProfilePort("no_client_auth")));
            assertGreenClusterState(transportClient);
            fail("Expected NoNodeAvailableException");
        } catch (NoNodeAvailableException e) {
            assertThat(e.getMessage(), containsString("None of the configured nodes are available: [{#transport#-"));
        }
    }

    private static int getProfilePort(String profile) {
        TransportAddress transportAddress =
                randomFrom(internalCluster().getInstance(Transport.class).profileBoundAddresses().get(profile).boundAddresses());
        return transportAddress.address().getPort();
    }
}
