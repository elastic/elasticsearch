/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.reindex;

import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsExchange;
import com.sun.net.httpserver.HttpsParameters;
import com.sun.net.httpserver.HttpsServer;
import org.elasticsearch.jdk.JavaVersion;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.PemKeyConfig;
import org.elasticsearch.common.ssl.PemTrustConfig;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.mockito.Mockito.mock;

/**
 * Because core ES doesn't have SSL available, this test uses a mock webserver
 * as the remote endpoint.
 * This makes it hard to test actual reindex functionality, but does allow us to test that the correct connections are made with the
 * right SSL keys + trust settings.
 */
@SuppressForbidden(reason = "use http server")
public class ReindexRestClientSslTests extends ESTestCase {

    private static HttpsServer server;
    private static Consumer<HttpsExchange> handler = ignore -> {
    };

    @BeforeClass
    public static void setupHttpServer() throws Exception {
        InetSocketAddress address = new InetSocketAddress(InetAddress.getLoopbackAddress().getHostAddress(), 0);
        SSLContext sslContext = buildServerSslContext();
        server = MockHttpServer.createHttps(address, 0);
        server.setHttpsConfigurator(new ClientAuthHttpsConfigurator(sslContext));
        server.start();
        server.createContext("/", http -> {
            assert http instanceof HttpsExchange;
            HttpsExchange https = (HttpsExchange) http;
            handler.accept(https);
            // Always respond with 200
            //  * If the reindex sees the 200, it means the SSL connection was established correctly.
            //  * We can check client certs in the handler.
            https.sendResponseHeaders(200, 0);
            https.close();
        });
    }

    @AfterClass
    public static void shutdownHttpServer() {
        server.stop(0);
        server = null;
        handler = null;
    }

    private static SSLContext buildServerSslContext() throws Exception {
        final SSLContext sslContext = SSLContext.getInstance(isHttpsServerBrokenWithTLSv13() ? "TLSv1.2" : "TLS");
        final char[] password = "http-password".toCharArray();

        final Path cert = PathUtils.get(ReindexRestClientSslTests.class.getResource("http/http.crt").toURI());
        final Path key = PathUtils.get(ReindexRestClientSslTests.class.getResource("http/http.key").toURI());
        final Path configPath = cert.getParent().getParent();
        final PemKeyConfig keyConfig = new PemKeyConfig(cert.toString(), key.toString(), password, configPath);
        final X509ExtendedKeyManager keyManager = keyConfig.createKeyManager();

        final Path ca = PathUtils.get(ReindexRestClientSslTests.class.getResource("ca.pem").toURI());
        final List<String> caList = Collections.singletonList(ca.toString());
        final X509ExtendedTrustManager trustManager = new PemTrustConfig(caList, configPath).createTrustManager();

        sslContext.init(new KeyManager[] { keyManager }, new TrustManager[] { trustManager }, null);
        return sslContext;
    }

    public void testClientFailsWithUntrustedCertificate() throws IOException {
        assumeFalse("https://github.com/elastic/elasticsearch/issues/49094", inFipsJvm());
        final List<Thread> threads = new ArrayList<>();
        final Settings.Builder builder = Settings.builder()
            .put("path.home", createTempDir());
        if (isHttpsServerBrokenWithTLSv13()) {
            builder.put("reindex.ssl.supported_protocols", "TLSv1.2");
        }
        final Settings settings = builder.build();
        final Environment environment = TestEnvironment.newEnvironment(settings);
        final ReindexSslConfig ssl = new ReindexSslConfig(settings, environment, mock(ResourceWatcherService.class));
        try (RestClient client = Reindexer.buildRestClient(getRemoteInfo(), ssl, 1L, threads)) {
            expectThrows(SSLHandshakeException.class, () -> client.performRequest(new Request("GET", "/")));
        }
    }

    public void testClientSucceedsWithCertificateAuthorities() throws IOException {
        final List<Thread> threads = new ArrayList<>();
        final Path ca = getDataPath("ca.pem");
        final Settings.Builder builder = Settings.builder()
            .put("path.home", createTempDir())
            .putList("reindex.ssl.certificate_authorities", ca.toString());
        if (isHttpsServerBrokenWithTLSv13()) {
            builder.put("reindex.ssl.supported_protocols", "TLSv1.2");
        }
        final Settings settings = builder.build();
        final Environment environment = TestEnvironment.newEnvironment(settings);
        final ReindexSslConfig ssl = new ReindexSslConfig(settings, environment, mock(ResourceWatcherService.class));
        try (RestClient client = Reindexer.buildRestClient(getRemoteInfo(), ssl, 1L, threads)) {
            final Response response = client.performRequest(new Request("GET", "/"));
            assertThat(response.getStatusLine().getStatusCode(), Matchers.is(200));
        }
    }

    public void testClientSucceedsWithVerificationDisabled() throws IOException {
        assumeFalse("Cannot disable verification in FIPS JVM", inFipsJvm());
        final List<Thread> threads = new ArrayList<>();
        final Settings.Builder builder = Settings.builder()
            .put("path.home", createTempDir())
            .put("reindex.ssl.verification_mode", "NONE");
        if (isHttpsServerBrokenWithTLSv13()) {
            builder.put("reindex.ssl.supported_protocols", "TLSv1.2");
        }
        final Settings settings = builder.build();
        final Environment environment = TestEnvironment.newEnvironment(settings);
        final ReindexSslConfig ssl = new ReindexSslConfig(settings, environment, mock(ResourceWatcherService.class));
        try (RestClient client = Reindexer.buildRestClient(getRemoteInfo(), ssl, 1L, threads)) {
            final Response response = client.performRequest(new Request("GET", "/"));
            assertThat(response.getStatusLine().getStatusCode(), Matchers.is(200));
        }
    }

    public void testClientPassesClientCertificate() throws IOException {
        final List<Thread> threads = new ArrayList<>();
        final Path ca = getDataPath("ca.pem");
        final Path cert = getDataPath("client/client.crt");
        final Path key = getDataPath("client/client.key");
        final Settings.Builder builder = Settings.builder()
            .put("path.home", createTempDir())
            .putList("reindex.ssl.certificate_authorities", ca.toString())
            .put("reindex.ssl.certificate", cert)
            .put("reindex.ssl.key", key)
            .put("reindex.ssl.key_passphrase", "client-password");
        if (isHttpsServerBrokenWithTLSv13()) {
            builder.put("reindex.ssl.supported_protocols", "TLSv1.2");
        }
        final Settings settings = builder.build();
        AtomicReference<Certificate[]> clientCertificates = new AtomicReference<>();
        handler = https -> {
            try {
                clientCertificates.set(https.getSSLSession().getPeerCertificates());
            } catch (SSLPeerUnverifiedException e) {
                logger.warn("Client did not provide certificates", e);
                clientCertificates.set(null);
            }
        };
        final Environment environment = TestEnvironment.newEnvironment(settings);
        final ReindexSslConfig ssl = new ReindexSslConfig(settings, environment, mock(ResourceWatcherService.class));
        try (RestClient client = Reindexer.buildRestClient(getRemoteInfo(), ssl, 1L, threads)) {
            final Response response = client.performRequest(new Request("GET", "/"));
            assertThat(response.getStatusLine().getStatusCode(), Matchers.is(200));
            final Certificate[] certs = clientCertificates.get();
            assertThat(certs, Matchers.notNullValue());
            assertThat(certs, Matchers.arrayWithSize(1));
            assertThat(certs[0], Matchers.instanceOf(X509Certificate.class));
            final X509Certificate clientCert = (X509Certificate) certs[0];
            assertThat(clientCert.getSubjectDN().getName(), Matchers.is("CN=client"));
            assertThat(clientCert.getIssuerDN().getName(), Matchers.is("CN=Elastic Certificate Tool Autogenerated CA"));
        }
    }

    private RemoteInfo getRemoteInfo() {
        return new RemoteInfo("https", server.getAddress().getHostName(), server.getAddress().getPort(), "/",
            new BytesArray("{\"match_all\":{}}"), "user", "password", Collections.emptyMap(), RemoteInfo.DEFAULT_SOCKET_TIMEOUT,
            RemoteInfo.DEFAULT_CONNECT_TIMEOUT);
    }

    @SuppressForbidden(reason = "use http server")
    private static class ClientAuthHttpsConfigurator extends HttpsConfigurator {
        ClientAuthHttpsConfigurator(SSLContext sslContext) {
            super(sslContext);
        }

        @Override
        public void configure(HttpsParameters params) {
            params.setWantClientAuth(true);
        }
    }

    /**
     * Checks whether the JVM this test is run under is affected by JDK-8254967, which causes these
     * tests to fail if a TLSv1.3 SSLContext is used.
     */
    private static boolean isHttpsServerBrokenWithTLSv13() {
        return JavaVersion.current().compareTo(JavaVersion.parse("16.0.0")) < 0;
    }
}
