/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ssl;

import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.client5.http.impl.async.HttpAsyncClients;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.apache.hc.core5.http.Method;
import org.apache.hc.core5.http2.impl.nio.ProtocolNegotiationException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.SslConfiguration;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLPeerUnverifiedException;

import static org.elasticsearch.test.TestMatchers.throwableWithMessage;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

@ESTestCase.EntitledTestPackages({ "org.apache.hc.core5.concurrent", "org.apache.hc.core5.reactor" })
public class HttpClient5SslTests extends ESTestCase {

    private MockWebServer server;
    private Path caCertPath;
    private String responseBody;

    @Before
    public void setup() throws Exception {
        this.caCertPath = getDataPath("/org/elasticsearch/xpack/core/ssl/ca/ca.crt");
        this.responseBody = randomAlphaOfLengthBetween(5, 25);
    }

    @After
    public void stopServer() throws Exception {
        if (server != null) {
            server.close();
        }
    }

    public void testTlsStrategyCustomServerCertificate() throws Exception {
        final CloseableHttpAsyncClient httpClient = buildClient(
            Settings.builder().put("xpack.http.ssl.certificate_authorities", caCertPath)
        );
        performRequest(httpClient);
    }

    public void testTlsStrategyFailsWithoutConfiguredCA() throws Exception {
        final CloseableHttpAsyncClient httpClient = buildClient(Settings.builder());
        final SSLException exception = expectThrows(SSLException.class, () -> performRequest(httpClient));
        assertThat(
            exception,
            // Different error messages on JDK21 vs JDK24
            throwableWithMessage(either(containsString("certificate_unknown")).or(containsString("PKIX path building failed")))
        );
    }

    public void testTlsStrategyWithVerificationOff() throws Exception {
        final CloseableHttpAsyncClient httpClient = buildClient(Settings.builder().put("xpack.http.ssl.verification_mode", "none"));
        performRequest(httpClient);
    }

    public void testTlsStrategyWithoutHostnameVerification() throws Exception {
        startServer("server-no-san", false, null, null);

        // Works with verification_mode "certificate" or "none"
        for (var mode : List.of("certificate", "none")) {
            final CloseableHttpAsyncClient httpClient = buildClient(
                Settings.builder() //
                    .put("xpack.http.ssl.certificate_authorities", caCertPath) //
                    .put("xpack.http.ssl.verification_mode", mode)
            );
            performRequest(httpClient);
        }

        // Fails with verification mode "full" or not-set (default to "full")
        for (var mode : Arrays.asList("full", null)) {
            final Settings.Builder settings = Settings.builder().put("xpack.http.ssl.certificate_authorities", caCertPath);
            if (mode != null) {
                settings.put("xpack.http.ssl.verification_mode", mode);
            }
            final CloseableHttpAsyncClient httpClient = buildClient(settings);
            final SSLPeerUnverifiedException exception = expectThrows(SSLPeerUnverifiedException.class, () -> performRequest(httpClient));
            assertThat(exception, throwableWithMessage(containsString("subject alternative names")));
        }
    }

    public void testTlsStrategyWithClientCertificate() throws Exception {
        var jdkVersion = Runtime.version().feature();
        assumeFalse(
            "A bug in JDK 22 and earlier prevents the mock server from requiring certificates."
                + " See https://bugs.openjdk.org/browse/JDK-8326233",
            jdkVersion <= 22
        );

        startServer(null, true, null, null);

        final Settings.Builder settings = Settings.builder();
        // Either of these configurations should work to trust the server (covered in other test methods)
        if (randomBoolean()) {
            settings.put("xpack.http.ssl.certificate_authorities", caCertPath);
        } else {
            settings.put("xpack.http.ssl.verification_mode", "none");
        }

        // Fail without certificates
        {
            final CloseableHttpAsyncClient httpClient = buildClient(settings);
            final SSLException exception = expectThrows(SSLException.class, () -> performRequest(httpClient));
            assertThat(exception, throwableWithMessage(containsString("certificate_required")));
        }

        // Succeed if we provide certificates
        {
            settings.put("xpack.http.ssl.certificate", getDataPath("/org/elasticsearch/xpack/core/ssl/client/client.crt"))
                .put("xpack.http.ssl.key", getDataPath("/org/elasticsearch/xpack/core/ssl/client/client.key"));
            final CloseableHttpAsyncClient httpClient = buildClient(settings);
            performRequest(httpClient);
        }
    }

    public void testTlsStrategyWithSpecifiedProtocols() throws Exception {
        final String activeProtocol;
        final String inactiveProtocol;
        if (randomBoolean()) {
            activeProtocol = "TLSv1.3";
            inactiveProtocol = "TLSv1.2";
        } else {
            activeProtocol = "TLSv1.2";
            inactiveProtocol = "TLSv1.3";
        }
        assertThat(activeProtocol, not(equalTo(inactiveProtocol)));

        startServer(null, false, List.of(activeProtocol), null);

        final Settings.Builder settings = Settings.builder();
        // Either of these configurations should work to trust the server (covered in other test methods)
        if (randomBoolean()) {
            settings.put("xpack.http.ssl.certificate_authorities", caCertPath);
        } else {
            settings.put("xpack.http.ssl.verification_mode", "none");
        }

        // Work with default protocols
        {
            final CloseableHttpAsyncClient httpClient = buildClient(settings);
            performRequest(httpClient);
        }

        // Fail with incorrect protocol
        {
            settings.put("xpack.http.ssl.supported_protocols", inactiveProtocol);
            final CloseableHttpAsyncClient httpClient = buildClient(settings);
            // In JDK 21 something messes up and HTTP client's SSL engine fails badly if there's no negotiated cipher
            // and ends up with a `ProtocolNegotiationException` instead of a normal handshake failure
            final IOException exception = expectThrows(IOException.class, () -> performRequest(httpClient));
            if (exception instanceof SSLException) {
                assertThat(exception, throwableWithMessage(containsString("protocol_version")));
            } else {
                assertThat(exception, instanceOf(ProtocolNegotiationException.class));
            }
        }

        // Work with specified protocols
        {
            settings.put("xpack.http.ssl.supported_protocols", activeProtocol);
            final CloseableHttpAsyncClient httpClient = buildClient(settings);
            performRequest(httpClient);
        }

        // Fail with unavailable protocol
        // TLSv1 is only disabled when using the SUN SSL provider, not (at this time) in BC-TLS
        if (inFipsJvm() == false) {
            settings.put("xpack.http.ssl.supported_protocols", "TLSv1");
            final CloseableHttpAsyncClient httpClient = buildClient(settings);
            final SSLException exception = expectThrows(SSLException.class, () -> performRequest(httpClient));
            assertThat(exception, throwableWithMessage(containsString("protocol is disabled")));
        }
    }

    public void testTlsStrategyWithSpecifiedCipherSuites() throws Exception {
        final String protocol;
        List<String> supportedCiphers;

        // Things get weird if one party only has TLSv1.3 ciphers enabled and the other party only has TLSv1.2 ciphers enabled
        // To avoid that, we force ourselves to only use ciphers from a single protocol
        if (randomBoolean()) {
            protocol = "TLSv1.2";
            supportedCiphers = List.of(
                "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA",
                "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256",
                "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
                "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA",
                "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384",
                "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
                "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256"
            );
        } else {
            protocol = "TLSv1.3";
            supportedCiphers = List.of("TLS_AES_256_GCM_SHA384", "TLS_AES_128_GCM_SHA256", "TLS_CHACHA20_POLY1305_SHA256");
        }
        if (inFipsJvm()) {
            supportedCiphers = supportedCiphers.stream().filter(name -> name.contains("CHACHA") == false).toList();
        }

        List<String> serverEnabledCiphers = shuffledList(
            randomSubsetOf(randomIntBetween(supportedCiphers.size() / 4, supportedCiphers.size() * 3 / 4), supportedCiphers)
        );
        if (serverEnabledCiphers.isEmpty()) {
            serverEnabledCiphers = List.of(randomFrom(supportedCiphers));
        }

        final List<String> serverDisabledCiphers = new ArrayList<>(supportedCiphers);
        serverDisabledCiphers.removeAll(serverEnabledCiphers);

        logger.info(
            "Server TLS CipherSuites\n\tSupported: {}\n\tEnabled: {}\n\tDisabled {}",
            supportedCiphers,
            serverEnabledCiphers,
            serverDisabledCiphers
        );

        startServer(null, false, List.of(protocol), serverEnabledCiphers);

        final Settings.Builder settings = Settings.builder();
        // Either of these configurations should work to trust the server (covered in other test methods)
        if (randomBoolean()) {
            settings.put("xpack.http.ssl.certificate_authorities", caCertPath);
        } else {
            settings.put("xpack.http.ssl.verification_mode", "none");
        }

        // Work with default cipher suites
        {
            final CloseableHttpAsyncClient httpClient = buildClient(settings);
            performRequest(httpClient);
        }

        // Fail with no matching cipher
        {
            settings.putList("xpack.http.ssl.cipher_suites", serverDisabledCiphers);
            final CloseableHttpAsyncClient httpClient = buildClient(settings);
            // In JDK 21 something messes up and HTTP client's SSL engine fails badly if there's no negotiated cipher
            // and ends up with a `ProtocolNegotiationException` instead of a normal handshake failure
            final IOException exception = expectThrows(IOException.class, () -> performRequest(httpClient));
            if (exception instanceof SSLException) {
                assertThat(exception, throwableWithMessage(containsString("handshake_failure")));
            } else {
                assertThat(exception, instanceOf(ProtocolNegotiationException.class));
            }
        }

        // Work with specified cipher
        {
            settings.putList("xpack.http.ssl.cipher_suites", shuffledList(serverEnabledCiphers));
            final CloseableHttpAsyncClient httpClient = buildClient(settings);
            performRequest(httpClient);
        }

        // Work with a subset of the server's ciphers
        {
            List<String> clientCiphers = new ArrayList<>(
                randomSubsetOf(randomIntBetween(1, serverEnabledCiphers.size()), serverEnabledCiphers)
            );
            settings.putList("xpack.http.ssl.cipher_suites", clientCiphers);
            final CloseableHttpAsyncClient httpClient = buildClient(settings);
            performRequest(httpClient);
        }

        // Work with a mix of supported and unsupported ciphers
        {
            List<String> clientCiphers = new ArrayList<>();
            clientCiphers.addAll(randomSubsetOf(randomIntBetween(1, 1 + serverEnabledCiphers.size() / 3), serverEnabledCiphers));
            clientCiphers.addAll(randomSubsetOf(randomIntBetween(1, 1 + serverDisabledCiphers.size() / 3), serverDisabledCiphers));
            clientCiphers = shuffledList(clientCiphers);
            settings.putList("xpack.http.ssl.cipher_suites", clientCiphers);
            final CloseableHttpAsyncClient httpClient = buildClient(settings);
            logger.info("Server cipher suites: {}", serverEnabledCiphers);
            logger.info("Client cipher suites: {}", clientCiphers);
            performRequest(httpClient);
        }
    }

    private void startServer() throws Exception {
        startServer("server", false, null, null);
    }

    private void startServer(String serverCert, boolean requireClientCert, List<String> protocols, List<String> cipherSuites)
        throws Exception {
        if (serverCert == null) {
            serverCert = "server";
        }
        final Path certPath = getDataPath("/org/elasticsearch/xpack/core/ssl/" + serverCert + "/" + serverCert + ".crt");
        final Path keyPath = getDataPath("/org/elasticsearch/xpack/core/ssl/" + serverCert + "/" + serverCert + ".key");

        final Settings.Builder settings = Settings.builder().put("ssl.certificate", certPath).put("ssl.key", keyPath);
        settings.put("ssl.client_authentication", requireClientCert ? "required" : "none");
        if (requireClientCert) {
            settings.putList("ssl.certificate_authorities", caCertPath.toString());
        }
        if (protocols != null) {
            settings.putList("ssl.supported_protocols", protocols);
        }
        if (cipherSuites != null) {
            settings.putList("ssl.cipher_suites", cipherSuites);
        }

        final SslConfiguration sslConfiguration = SslSettingsLoader.load(settings.build(), "ssl.", newEnvironment());
        final SSLContext context = sslConfiguration.createSslContext();

        final MockWebServer.TlsConfig tlsConfig = new MockWebServer.TlsConfig(sslConfiguration);
        assertThat(requireClientCert, equalTo(tlsConfig.needClientAuth()));

        server = new MockWebServer(context, tlsConfig);
        server.start();
        logger.info("Server is listening at: {}", server.getUri("/"));
    }

    private CloseableHttpAsyncClient buildClient(final Settings.Builder environmentSettings) {
        Environment env = newEnvironment(environmentSettings.build());
        final TestsSSLService sslService = new TestsSSLService(env);
        final SslProfile profile = sslService.profile("xpack.http.ssl");

        PoolingAsyncClientConnectionManager connectionManager = PoolingAsyncClientConnectionManagerBuilder.create()
            .setTlsStrategy(profile.clientTlsStrategy())
            .build();

        final CloseableHttpAsyncClient httpClient = HttpAsyncClients.custom().setConnectionManager(connectionManager).build();
        httpClient.start();
        return httpClient;
    }

    private void performRequest(CloseableHttpAsyncClient httpClient) throws Exception {
        if (server == null) {
            startServer();
        }
        server.enqueue(new MockResponse().setResponseCode(200).setBody(responseBody));
        try {
            final SimpleHttpRequest req = SimpleHttpRequest.create(Method.GET, server.getUri("/"));
            final PlainActionFuture<SimpleHttpResponse> future = new PlainActionFuture<>();
            httpClient.execute(req, new ListenerCallbackAdapter<>(future));
            assertThat(future.get().getBody().getBodyText(), equalTo(responseBody));
        } catch (ExecutionException ex) {
            // Log a summary of the exception. Often this is expected, so we don't want fill the logs with noise
            // but if it leads to a failed test then we want a bit of info
            logger.info(
                "Failed to perform HTTP request: {}\n at {}",
                ex.toString(),
                Stream.of(ex.getStackTrace())
                    .filter(e -> e.getClassName().equals(this.getClass().getName()))
                    .map(e -> e.getClassName() + "." + e.getMethodName() + " (" + e.getFileName() + ":" + e.getLineNumber() + ")")
                    .collect(Collectors.joining("\n\t"))
            );
            if (ex.getCause() instanceof Exception cause) {
                throw cause;
            } else {
                throw ex;
            }
        } finally {
            httpClient.close();
        }
    }

    private static class ListenerCallbackAdapter<T> implements FutureCallback<T> {
        private final ActionListener<T> listener;

        ListenerCallbackAdapter(ActionListener<T> listener) {
            this.listener = listener;
        }

        @Override
        public void completed(T result) {
            listener.onResponse(result);
        }

        @Override
        public void failed(Exception ex) {
            listener.onFailure(ex);
        }

        @Override
        public void cancelled() {
            failed(new RuntimeException("cancelled"));
        }
    }
}
