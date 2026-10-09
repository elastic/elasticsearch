/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.SslConfiguration;
import org.elasticsearch.common.ssl.SslVerificationMode;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.core.ssl.SslSettingsLoader;
import org.elasticsearch.xpack.monitoring.exporter.Exporter.Config;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.util.Objects;

import javax.net.ssl.SSLPeerUnverifiedException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;

/**
 * Sends requests over HTTPS through the {@link RestClient} built by {@link HttpExporter#createRestClient}, using a real {@link SSLService}.
 */
public class HttpExporterSslTests extends ESTestCase {

    private static final String SSL_PREFIX = "xpack.monitoring.exporters._http.ssl.";

    private Path sslResourceDir;
    private MockWebServer server;
    private MockSecureSettings secureSettings;

    @Before
    public void createResourceDir() {
        sslResourceDir = createTempDir();
    }

    @After
    public void stopServer() {
        if (server != null) {
            server.close();
        }
    }

    public void testRequestOverHttps() throws Exception {
        startServer("server");
        final Settings.Builder settings = Settings.builder()
            .putList(SSL_PREFIX + "certificate_authorities", sslResource("ca.crt").toString());
        if (randomBoolean()) {
            settings.put(SSL_PREFIX + "verification_mode", SslVerificationMode.FULL.name());
        }
        assertRequestSucceeds(settings);
    }

    public void testRequestOverHttpsWithSecureSslSettings() throws Exception {
        assumeFalse("PKCS12 truststores are not supported in FIPS mode", inFipsJvm());
        startServer("server");
        secureSettings = new MockSecureSettings();
        secureSettings.setString(SSL_PREFIX + "truststore.secure_password", "truststore-password");
        assertRequestSucceeds(
            Settings.builder()
                .put(SSL_PREFIX + "truststore.path", createTruststore("truststore-password"))
                .put(SSL_PREFIX + "truststore.type", "PKCS12")
                .setSecureSettings(secureSettings)
        );
    }

    public void testHostnameVerificationFailure() throws Exception {
        startServer("server-no-san");
        final Settings.Builder settings = Settings.builder()
            .putList(SSL_PREFIX + "certificate_authorities", sslResource("ca.crt").toString());
        try (RestClient client = createRestClient(settings)) {
            final Exception exception = expectThrows(Exception.class, () -> client.performRequest(new Request("GET", "/")));
            final Throwable unverified = ExceptionsHelper.unwrap(exception, SSLPeerUnverifiedException.class);
            assertThat(unverified, notNullValue());
            assertThat(unverified.getMessage(), containsString("[" + server.getHostName() + "]"));
            assertThat(unverified.getMessage(), containsString("subject alternative names"));
            assertThat(unverified.getMessage(), containsString("CN=server-no-san"));
        }
        assertThat(server.requests(), hasSize(0));
        assertDeprecationWarnings();
    }

    public void testHostnameVerificationSkippedWithCertificateMode() throws Exception {
        startServer("server-no-san");
        assertRequestSucceeds(
            Settings.builder()
                .putList(SSL_PREFIX + "certificate_authorities", sslResource("ca.crt").toString())
                .put(SSL_PREFIX + "verification_mode", SslVerificationMode.CERTIFICATE.name())
        );
    }

    private void assertDeprecationWarnings() {
        assertWarnings(
            "[xpack.monitoring.exporters._http.host] setting was deprecated in Elasticsearch and will be removed in a future release. "
                + "See the breaking changes documentation for the next major version.",
            "[xpack.monitoring.exporters._http.type] setting was deprecated in Elasticsearch and will be removed in a future release. "
                + "See the breaking changes documentation for the next major version."
        );
    }

    private void assertRequestSucceeds(Settings.Builder sslSettings) throws Exception {
        final String body = randomAlphaOfLengthBetween(2, 100);
        server.enqueue(new MockResponse().setResponseCode(200).setBody(body));
        try (RestClient client = createRestClient(sslSettings)) {
            final var response = client.performRequest(new Request("GET", "/"));
            assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
            assertThat(new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8), equalTo(body));
        }
        assertThat(server.requests(), hasSize(1));
        assertDeprecationWarnings();
    }

    private RestClient createRestClient(Settings.Builder sslSettings) throws IOException {
        final Settings settings = sslSettings.put("xpack.monitoring.exporters._http.type", HttpExporter.TYPE)
            .put("xpack.monitoring.exporters._http.host", "https://" + server.getHostName() + ":" + server.getPort())
            .put("path.home", createTempDir())
            .build();
        final SSLService sslService = new SSLService(newEnvironment(settings));
        // Secure settings are closed after node startup, so the exporter must reuse the profile loaded by the SSL service
        if (secureSettings != null) {
            secureSettings.close();
        }
        final Config config = new Config("_http", HttpExporter.TYPE, settings, mock(ClusterService.class), mock(XPackLicenseState.class));
        return HttpExporter.createRestClient(config, sslService, mock(NodeFailureListener.class));
    }

    private void startServer(String certName) throws Exception {
        final Settings settings = Settings.builder()
            .put("ssl.certificate", sslResource(certName + ".crt"))
            .put("ssl.key", sslResource(certName + ".key"))
            .put("ssl.client_authentication", "none")
            .build();
        final SslConfiguration sslConfiguration = SslSettingsLoader.load(settings, "ssl.", newEnvironment());
        server = new MockWebServer(sslConfiguration.createSslContext(), new MockWebServer.TlsConfig(sslConfiguration));
        // The test certificates only have SANs for 127.0.0.1 and localhost
        server.start(InetAddress.getByName("127.0.0.1"));
    }

    private Path createTruststore(String password) throws Exception {
        final KeyStore truststore = KeyStore.getInstance("PKCS12");
        truststore.load(null, null);
        try (InputStream in = Files.newInputStream(sslResource("ca.crt"))) {
            truststore.setCertificateEntry("ca", CertificateFactory.getInstance("X.509").generateCertificate(in));
        }
        final Path path = sslResourceDir.resolve("truststore.p12");
        try (OutputStream out = Files.newOutputStream(path)) {
            truststore.store(out, password.toCharArray());
        }
        return path;
    }

    /**
     * Copies a certificate file from the x-pack core test artifact, which is a jar on this classpath, to a temp dir.
     */
    private Path sslResource(String fileName) throws IOException {
        final Path file = sslResourceDir.resolve(fileName);
        if (Files.exists(file) == false) {
            final String dir = fileName.substring(0, fileName.lastIndexOf('.'));
            try (
                InputStream in = HttpExporterSslTests.class.getResourceAsStream("/org/elasticsearch/xpack/core/ssl/" + dir + "/" + fileName)
            ) {
                Files.copy(Objects.requireNonNull(in), file);
            }
        }
        return file;
    }
}
