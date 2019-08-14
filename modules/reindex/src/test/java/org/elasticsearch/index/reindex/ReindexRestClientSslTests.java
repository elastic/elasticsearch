/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.reindex;

import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsExchange;
import com.sun.net.httpserver.HttpsParameters;
import com.sun.net.httpserver.HttpsServer;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.PathUtils;
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
        final SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
        final char[] password = "http-password".toCharArray();

        final Path cert = PathUtils.get(ReindexRestClientSslTests.class.getResource("http/http.crt").toURI());
        final Path key = PathUtils.get(ReindexRestClientSslTests.class.getResource("http/http.key").toURI());
        final X509ExtendedKeyManager keyManager = new PemKeyConfig(cert, key, password).createKeyManager();

        final Path ca = PathUtils.get(ReindexRestClientSslTests.class.getResource("ca.pem").toURI());
        final X509ExtendedTrustManager trustManager = new PemTrustConfig(Collections.singletonList(ca)).createTrustManager();

        sslContext.init(new KeyManager[] { keyManager }, new TrustManager[] { trustManager }, null);
        return sslContext;
    }

    public void testClientFailsWithUntrustedCertificate() throws IOException {
        final List<Thread> threads = new ArrayList<>();
        final Settings settings = Settings.builder()
            .put("path.home", createTempDir())
            .put("reindex.ssl.supported_protocols", "TLSv1.2")
            .build();
        final Environment environment = TestEnvironment.newEnvironment(settings);
        final ReindexSslConfig ssl = new ReindexSslConfig(settings, environment, mock(ResourceWatcherService.class));
        try (RestClient client = Reindexer.buildRestClient(getRemoteInfo(), ssl, 1L, threads)) {
            expectThrows(SSLHandshakeException.class, () -> client.performRequest(new Request("GET", "/")));
        }
    }

    public void testClientSucceedsWithCertificateAuthorities() throws IOException {
        final List<Thread> threads = new ArrayList<>();
        final Path ca = getDataPath("ca.pem");
        final Settings settings = Settings.builder()
            .put("path.home", createTempDir())
            .putList("reindex.ssl.certificate_authorities", ca.toString())
            .put("reindex.ssl.supported_protocols", "TLSv1.2")
            .build();
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
        final Settings settings = Settings.builder()
            .put("path.home", createTempDir())
            .put("reindex.ssl.verification_mode", "NONE")
            .put("reindex.ssl.supported_protocols", "TLSv1.2")
            .build();
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
        final Settings settings = Settings.builder()
            .put("path.home", createTempDir())
            .putList("reindex.ssl.certificate_authorities", ca.toString())
            .put("reindex.ssl.certificate", cert)
            .put("reindex.ssl.key", key)
            .put("reindex.ssl.key_passphrase", "client-password")
            .put("reindex.ssl.supported_protocols", "TLSv1.2")
            .build();
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
}
