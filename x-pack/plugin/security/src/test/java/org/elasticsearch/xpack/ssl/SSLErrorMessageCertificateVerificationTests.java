/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ssl;

import org.apache.http.HttpHost;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.DiagnosticTrustManager;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.xpack.core.common.socket.SocketAccess;
import org.elasticsearch.xpack.core.ssl.SSLClientAuth;
import org.elasticsearch.xpack.core.ssl.SSLConfiguration;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.core.ssl.VerificationMode;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Pattern;

import static org.elasticsearch.test.TestMatchers.throwableWithMessage;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.containsStringIgnoringCase;
import static org.hamcrest.Matchers.is;

public class SSLErrorMessageCertificateVerificationTests extends ESTestCase {

    private static final String HTTP_SERVER_SSL = "xpack.security.http.ssl";
    private static final String HTTP_CLIENT_SSL = "xpack.http.ssl";

    public void testMessageForHttpClientHostnameVerificationFailure() throws IOException, URISyntaxException {
        final Settings sslSetup = getPemSSLSettings(HTTP_SERVER_SSL, "not-this-host.crt", "not-this-host.key",
            SSLClientAuth.NONE, VerificationMode.FULL, null)
            .putList("xpack.http.ssl.certificate_authorities", getPath("ca1.crt"))
            .build();
        final SSLService sslService = new SSLService(TestEnvironment.newEnvironment(buildEnvSettings(sslSetup)));
        try (MockWebServer webServer = initWebServer(sslService);
             CloseableHttpClient client = buildHttpClient(sslService)) {
            final HttpGet request = new HttpGet(webServer.getUri("/"));
            try (CloseableHttpResponse ignore = SocketAccess.doPrivileged(() -> client.execute(request))) {
                fail("Expected hostname verification exception");
            } catch (Exception e) {
                assertThat(e, throwableWithMessage(containsStringIgnoringCase("Certificate")));
                assertThat(e, throwableWithMessage(containsString(request.getURI().getHost())));
                assertThat(e, throwableWithMessage(containsStringIgnoringCase("subject alternative names")));
                assertThat(e, throwableWithMessage(containsString("not.this.host")));
            }
        }
    }

    public void testMessageForRestClientHostnameVerificationFailure() throws IOException, URISyntaxException {
        final Settings sslSetup = getPemSSLSettings(HTTP_SERVER_SSL, "not-this-host.crt", "not-this-host.key",
            SSLClientAuth.NONE, VerificationMode.FULL, null)
            // Client
            .putList("xpack.http.ssl.certificate_authorities", getPath("ca1.crt"))
            .build();
        final SSLService sslService = new SSLService(TestEnvironment.newEnvironment(buildEnvSettings(sslSetup)));
        try (MockWebServer webServer = initWebServer(sslService)) {
            try (RestClient restClient = buildRestClient(sslService, webServer)) {
                restClient.performRequest(new Request("GET", "/"));
                fail("Expected hostname verification exception");
            } catch (Exception e) {
                assertThat(e, throwableWithMessage(containsStringIgnoringCase("certificate")));
                assertThat(e, throwableWithMessage(containsString(webServer.getHostName())));
                assertThat(e, throwableWithMessage(containsStringIgnoringCase("subject alternative names")));
                assertThat(e, throwableWithMessage(containsString("not.this.host")));
            }
        }
    }

    public void testDiagnosticTrustManagerForHostnameVerificationFailure() throws Exception {
        final Settings settings = getPemSSLSettings(HTTP_SERVER_SSL, "not-this-host.crt", "not-this-host.key",
            SSLClientAuth.NONE, VerificationMode.FULL, null)
            .putList("xpack.http.ssl.certificate_authorities", getPath("ca1.crt"))
            .build();
        final SSLService sslService = new SSLService(TestEnvironment.newEnvironment(buildEnvSettings(settings)));
        final SSLConfiguration clientSslConfig = sslService.getSSLConfiguration(HTTP_CLIENT_SSL);
        final SSLSocketFactory clientSocketFactory = sslService.sslSocketFactory(clientSslConfig);

        final Logger diagnosticLogger = LogManager.getLogger(DiagnosticTrustManager.class);
        final MockLogAppender mockAppender = new MockLogAppender();
        mockAppender.start();

        // Apache clients implement their own hostname checking, but we don't want that.
        // We use a raw socket so we get the builtin JDK checking (which is what we use for transport protocol SSL checks)
        try (MockWebServer webServer = initWebServer(sslService);
             SSLSocket clientSocket = (SSLSocket) clientSocketFactory.createSocket()) {
            Loggers.addAppender(diagnosticLogger, mockAppender);

            mockAppender.addExpectation(new MockLogAppender.PatternSeenEventExpectation(
                "ssl diagnostic",
                DiagnosticTrustManager.class.getName(),
                Level.WARN,
                "failed to establish trust with server at \\[" + Pattern.quote(webServer.getHostName()) + "\\];" +
                    " the server provided a certificate with subject name \\[CN=not-this-host\\]" +
                    " and fingerprint \\[[0-9a-f]{40}\\];" +
                    " the certificate has subject alternative names \\[DNS:not\\.this\\.host\\];" +
                    " the certificate is issued by \\[CN=Certificate Authority 1,OU=ssl-error-message-test,DC=elastic,DC=co\\]" +
                    " but the server did not provide a copy of the issuing certificate in the certificate chain;" +
                    " the issuing certificate with fingerprint \\[[0-9a-f]{40}\\]" +
                    " is trusted in this ssl context " + Pattern.quote("([" + HTTP_CLIENT_SSL + "])")));
            enableHttpsHostnameChecking(clientSocket);
            connect(clientSocket, webServer);
            assertThat(clientSocket.isConnected(), is(true));
            final SSLHandshakeException handshakeException = expectThrows(SSLHandshakeException.class,
                () -> clientSocket.getInputStream().read());
            assertThat(handshakeException, throwableWithMessage(containsStringIgnoringCase("subject alternative names")));
            assertThat(handshakeException, throwableWithMessage(containsString(webServer.getHostName())));

            // Logging message failures are tricky to debug because you just get a "didn't find match" assertion failure.
            // You should be able to check the log output for the text that was logged and compare to the regex above.
            mockAppender.assertAllExpectationsMatched();
        } finally {
            Loggers.removeAppender(diagnosticLogger, mockAppender);
            mockAppender.stop();
        }
    }

    @SuppressForbidden(reason = "Allow opening socket for test")
    private void connect(SSLSocket clientSocket, MockWebServer webServer) throws IOException {
        SocketAccess.doPrivileged(() -> clientSocket.connect(webServer.getAddress()));
    }

    private CloseableHttpClient buildHttpClient(SSLService sslService) {
        final SSLConfiguration sslConfiguration = sslService.getSSLConfiguration(HTTP_CLIENT_SSL);
        final HostnameVerifier verifier = SSLService.getHostnameVerifier(sslConfiguration);
        final SSLSocketFactory socketFactory = sslService.sslSocketFactory(sslConfiguration);
        final SSLConnectionSocketFactory connectionSocketFactory = new SSLConnectionSocketFactory(socketFactory, verifier);
        return HttpClientBuilder.create().setSSLSocketFactory(connectionSocketFactory).build();
    }

    private RestClient buildRestClient(SSLService sslService, MockWebServer webServer) {
        final SSLConfiguration sslConfiguration = sslService.getSSLConfiguration(HTTP_CLIENT_SSL);
        final HttpHost httpHost = new HttpHost(webServer.getHostName(), webServer.getPort(), "https");
        return RestClient.builder(httpHost)
            .setHttpClientConfigCallback(client -> client.setSSLStrategy(sslService.sslIOSessionStrategy(sslConfiguration)))
            .build();
    }

    /**
     * By default, JSSE doesn't actually do hostname checking as part of certificate verifications.
     * It's possible to implement it yourself, or opt-in to have the TrustManager do it for you.
     * However, just to make things difficult (ha!) the HTTP RFC and LDAP RFC have different rules for wildcard expansion in Certificate
     * DNS SANs, which is why we need to enable "https" checking.
     */
    private void enableHttpsHostnameChecking(SSLSocket clientSocket) {
        final SSLParameters params = new SSLParameters();
        params.setEndpointIdentificationAlgorithm("HTTPS");
        clientSocket.setSSLParameters(params);
    }

    private Settings.Builder getPemSSLSettings(String prefix, String certificatePath, String keyPath, SSLClientAuth clientAuth,
                                               VerificationMode verificationMode, String caPath) throws FileNotFoundException {
        final Settings.Builder builder = Settings.builder()
            .put(prefix + ".enabled", true)
            .put(prefix + ".certificate", getPath(certificatePath))
            .put(prefix + ".key", getPath(keyPath))
            .put(prefix + ".client_authentication", clientAuth.name())
            .put(prefix + ".verification_mode", verificationMode.name());
        if (caPath != null) {
            builder.putList(prefix + ".certificate_authorities", getPath(caPath));
        }
        return builder;
    }

    private MockWebServer initWebServer(SSLService sslService) throws IOException {
        final SSLConfiguration httpSslConfig = sslService.getSSLConfiguration(HTTP_SERVER_SSL);
        final MockWebServer webServer = new MockWebServer(sslService.sslContext(httpSslConfig), false);

        webServer.enqueue(new MockResponse().setBody("{}").setResponseCode(200));
        webServer.start();
        return webServer;
    }

    private String getPath(String fileName) throws FileNotFoundException {
        final Path path = getDataPath("/org/elasticsearch/xpack/ssl/SSLErrorMessageTests/" + fileName);
        if (Files.exists(path)) {
            return path.toString();
        } else {
            throw new FileNotFoundException("File " + path + " does not exist");
        }
    }

}
