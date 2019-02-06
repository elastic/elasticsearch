/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport.ssl;

import org.apache.http.Header;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.xpack.core.TestXPackTransportClient;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.common.socket.SocketAccess;
import org.elasticsearch.xpack.core.ssl.SSLConfiguration;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.LocalStateSecurity;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.TrustManagerFactory;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.elasticsearch.test.SecuritySettingsSource.addSSLSettingsForNodePEMFiles;
import static org.elasticsearch.test.SecuritySettingsSource.addSSLSettingsForPEMFiles;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

public class SslIntegrationTests extends SecurityIntegTestCase {
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        final Settings superSettings = super.nodeSettings(nodeOrdinal);
        if (superSettings.keySet().stream().anyMatch(k -> k.contains("supported_protocols"))) {
            throw new IllegalStateException("Node configured supported_protocols");
        }
        Settings.Builder builder = Settings.builder().put(superSettings);
        addSSLSettingsForNodePEMFiles(builder, "xpack.security.http.", true);
        return builder.put(NetworkModule.HTTP_ENABLED.getKey(), true)
            .put("xpack.security.http.ssl.enabled", true)
            .build();
    }

    @Override
    protected boolean transportSSLEnabled() {
        return true;
    }

    // no SSL exception as this is the exception is returned when connecting
    public void testThatUnconfiguredCiphersAreRejected() throws Exception {
        Set<String> supportedCiphers = Sets.newHashSet(SSLContext.getDefault().getSupportedSSLParameters().getCipherSuites());
        Set<String> defaultXPackCiphers = Sets.newHashSet(XPackSettings.DEFAULT_CIPHERS);
        final List<String> unconfiguredCiphers = new ArrayList<>(Sets.difference(supportedCiphers, defaultXPackCiphers));
        Collections.shuffle(unconfiguredCiphers, random());
        assumeFalse("the unconfigured ciphers list is empty", unconfiguredCiphers.isEmpty());

        try (TransportClient transportClient = new TestXPackTransportClient(Settings.builder()
            .put(transportClientSettings())
            .put("node.name", "programmatic_transport_client")
            .put("cluster.name", internalCluster().getClusterName())
            .putList("xpack.ssl.cipher_suites", unconfiguredCiphers)
            .build(), LocalStateSecurity.class)) {

            TransportAddress transportAddress = randomFrom(internalCluster().getInstance(Transport.class).boundAddress().boundAddresses());
            transportClient.addTransportAddress(transportAddress);

            transportClient.admin().cluster().prepareHealth().get();
            fail("Expected NoNodeAvailableException");
        } catch (NoNodeAvailableException e) {
            assertThat(e.getMessage(), containsString("None of the configured nodes are available: [{#transport#"));
        }
    }

    public void testThatTransportClientUsingDefaultSettingsIsAccepted() {
        final String clusterName = internalCluster().getClusterName();
        try (TransportClient transportClient = new TestXPackTransportClient(Settings.builder()
            .put(transportClientSettings())
            .put("node.name", "programmatic_transport_client")
            .put("cluster.name", clusterName)
            .build(), LocalStateSecurity.class)) {

            TransportAddress transportAddress = randomFrom(internalCluster().getInstance(Transport.class).boundAddress().boundAddresses());
            transportClient.addTransportAddress(transportAddress);

            final ClusterHealthResponse response = transportClient.admin().cluster().prepareHealth().get();
            assertThat(response.getClusterName(), is(clusterName));
        }
    }

    public void testThatTransportClientUsingSSLv3ProtocolIsRejected() {
        assumeFalse("Can't run in a FIPS JVM as SSLv3 SSLContext not available", inFipsJvm());
        try (TransportClient transportClient = new TestXPackTransportClient(Settings.builder()
            .put(transportClientSettings())
            .put("node.name", "programmatic_transport_client")
            .put("cluster.name", internalCluster().getClusterName())
            .putList("xpack.security.transport.ssl.supported_protocols", new String[]{"SSLv3"})
            .build(), LocalStateSecurity.class)) {

            TransportAddress transportAddress = randomFrom(internalCluster().getInstance(Transport.class).boundAddress().boundAddresses());
            transportClient.addTransportAddress(transportAddress);

            transportClient.admin().cluster().prepareHealth().get();
            fail("Expected NoNodeAvailableException");
        } catch (NoNodeAvailableException e) {
            assertThat(e.getMessage(), containsString("None of the configured nodes are available: [{#transport#"));
        }
    }

    public void testThatConnectionToHTTPWorks() throws Exception {
        Settings.Builder builder = Settings.builder();
        addSSLSettingsForPEMFiles(
            builder, "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.pem",
            "testclient",
            "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.crt",
            Collections.singletonList("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"));
        SSLService service = new SSLService(builder.build(), null);

        try (CloseableHttpClient client = buildHttpClient(service);
             CloseableHttpResponse response = SocketAccess.doPrivileged(() -> client.execute(new HttpGet(getNodeUrl())))) {
            assertThat(response.getStatusLine().getStatusCode(), is(200));
            String data = Streams.copyToString(new InputStreamReader(response.getEntity().getContent(), StandardCharsets.UTF_8));
            assertThat(data, containsString("You Know, for Search"));
        }
    }

    public void testThatHttpUsingSSLv3IsRejected() throws Exception {
        assumeFalse("Can't run in a FIPS JVM as we can't even get an instance of SSL SSL Context", inFipsJvm());
        SSLContext sslContext = SSLContext.getInstance("SSL");
        TrustManagerFactory factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        factory.init((KeyStore) null);

        sslContext.init(null, factory.getTrustManagers(), new SecureRandom());
        SSLConnectionSocketFactory sf = new SSLConnectionSocketFactory(sslContext, new String[]{"SSLv3"}, null,
            NoopHostnameVerifier.INSTANCE);
        try (CloseableHttpClient client = HttpClients.custom().setSSLSocketFactory(sf).build()) {
            CloseableHttpResponse result = SocketAccess.doPrivileged(() -> client.execute(new HttpGet(getNodeUrl())));
            fail("Expected a connection error due to SSLv3 not being supported by default");
        } catch (Exception e) {
            assertThat(e, is(instanceOf(SSLHandshakeException.class)));
        }
    }

    public void testServerLogsDeprecationWarningWhenTransportClientConnectsWithTLS1() throws Exception {
        assumeFalse("Can't run in a FIPS JVM with verification mode 'none'", inFipsJvm());
        Logger logger =
            LogManager.getLogger("org.elasticsearch.deprecation.xpack.core.security.transport.netty4.SecurityNetty4Transport");
        MockLogAppender appender = expectTls1DeprecationWarnings(logger, "xpack.security.transport.ssl", "transport profile: default");
        try {
            final String clusterName = internalCluster().getClusterName();
            try (TransportClient transportClient = new TestXPackTransportClient(Settings.builder()
                .put(transportClientSettings())
                .put("node.name", "programmatic_transport_client")
                .put("cluster.name", clusterName)
                .putList("xpack.security.transport.ssl.supported_protocols", "TLSv1")
                .put("xpack.security.transport.ssl.verification_mode", "none")
                // For some unknown reason, verification mode 'none' is needed with TLSv1, but only for some test seeds.
                .build(), LocalStateSecurity.class)) {

                final Transport transport = internalCluster().getInstance(Transport.class);
                TransportAddress transportAddress = randomFrom(transport.boundAddress().boundAddresses());
                transportClient.addTransportAddress(transportAddress);

                final ClusterHealthResponse response = transportClient.admin().cluster().prepareHealth().get();
                assertThat(response.getClusterName(), is(clusterName));
            }
            appender.assertAllExpectationsMatched();
        } finally {
            Loggers.removeAppender(logger, appender);
            appender.stop();
        }
    }

    public void testDeprecationWarningWhenHttpClientConnectsWithTLS1() throws Exception {
        Logger logger =
            LogManager.getLogger("org.elasticsearch.deprecation.xpack.security.Security");
        MockLogAppender appender = expectTls1DeprecationWarnings(logger, "xpack.security.http.ssl", "HTTP connection from *");

        try {
            Settings.Builder builder = Settings.builder().putList("xpack.security.transport.ssl.supported_protocols", "TLSv1");
            addSSLSettingsForPEMFiles(
                builder, "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.pem",
                "testclient",
                "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.crt",
                Collections.singletonList("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"));
            SSLService service = new SSLService(builder.build(), null);

            try (CloseableHttpClient client = buildHttpClient(service);
                 CloseableHttpResponse response = SocketAccess.doPrivileged(() -> client.execute(new HttpGet(getNodeUrl())))) {
                assertThat(response.getStatusLine().getStatusCode(), is(200));
                final Header warningHeader = response.getFirstHeader("Warning");
                assertThat(warningHeader, notNullValue());
                assertThat(warningHeader.getValue(), containsString("a TLS v1.0 session was used for [HTTP connection"));
                assertThat(warningHeader.getValue(),
                    containsString("The [xpack.security.http.ssl.supported_protocols] setting can be used to control this."));
            }
            appender.assertAllExpectationsMatched();
        } finally {
            Loggers.removeAppender(logger, appender);
            appender.stop();
        }
    }

    private CloseableHttpClient buildHttpClient(SSLService service) {
        CredentialsProvider provider = new BasicCredentialsProvider();
        provider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(nodeClientUsername(),
            new String(nodeClientPassword().getChars())));
        SSLConfiguration sslConfiguration = service.getSSLConfiguration("xpack.security.transport.ssl");
        final SSLConnectionSocketFactory socketFactory = new SSLConnectionSocketFactory(
            service.sslSocketFactory(sslConfiguration),
            SSLConnectionSocketFactory.getDefaultHostnameVerifier());

        return HttpClients.custom()
            .setSSLSocketFactory(socketFactory)
            .setDefaultCredentialsProvider(provider)
            .build();
    }

    private String getNodeUrl() {
        TransportAddress transportAddress =
            randomFrom(internalCluster().getInstance(HttpServerTransport.class).boundAddress().boundAddresses());
        final InetSocketAddress inetSocketAddress = transportAddress.address();
        return String.format(Locale.ROOT, "https://%s/", NetworkAddress.format(inetSocketAddress));
    }

    private MockLogAppender expectTls1DeprecationWarnings(Logger logger, String settingPrefix, String description)
        throws IllegalAccessException {

        MockLogAppender appender = new MockLogAppender();
        Loggers.addAppender(logger, appender);
        appender.addExpectation(new MockLogAppender.SeenEventExpectation("TLS v1.0 deprecation warning",
            logger.getName(), Level.WARN,
            "a TLS v1.0 session was used for [" + description + "]," +
                " this protocol will be disabled by default in a future version." +
                " The [" + settingPrefix + ".supported_protocols] setting can be used to control this."
        ));
        appender.start();
        return appender;
    }

}
