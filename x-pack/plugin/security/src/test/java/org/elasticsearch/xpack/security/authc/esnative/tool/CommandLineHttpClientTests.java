/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.esnative.tool;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.PemUtils;
import org.elasticsearch.common.ssl.SslUtil;
import org.elasticsearch.common.ssl.SslVerificationMode;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.xpack.core.security.CommandLineHttpClient;
import org.elasticsearch.xpack.core.security.HttpResponse;
import org.elasticsearch.xpack.core.security.HttpResponse.HttpResponseBuilder;
import org.elasticsearch.xpack.core.ssl.CertParsingUtils;
import org.elasticsearch.xpack.core.ssl.SSLConfigurationSettingsTests;
import org.elasticsearch.xpack.core.ssl.TestsSSLService;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateExpiredException;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.List;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.X509ExtendedKeyManager;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * This class tests {@link CommandLineHttpClient} For extensive tests related to
 * ssl settings can be found {@link SSLConfigurationSettingsTests}
 */
public class CommandLineHttpClientTests extends ESTestCase {

    private MockWebServer webServer;
    private Path certPath;
    private Path keyPath;
    private Path caCertPath;

    @Before
    public void setup() throws Exception {
        certPath = getDataPath("/org/elasticsearch/xpack/security/authc/esnative/tool/http.crt");
        keyPath = getDataPath("/org/elasticsearch/xpack/security/authc/esnative/tool/http.key");
        caCertPath = getDataPath("/org/elasticsearch/xpack/security/authc/esnative/tool/ca.crt");

        webServer = createMockWebServer();
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody("{\"test\": \"complete\"}"));
        webServer.start();
    }

    @After
    public void shutdown() {
        webServer.close();
    }

    public void testCommandLineHttpClientCanExecuteAndReturnCorrectResultUsingSSLSettings() throws Exception {
        Settings settings = getHttpSslSettings().put("xpack.security.http.ssl.certificate_authorities", caCertPath.toString())
            .put("xpack.security.http.ssl.verification_mode", SslVerificationMode.CERTIFICATE)
            .build();
        CommandLineHttpClient client = new CommandLineHttpClient(TestEnvironment.newEnvironment(settings));
        HttpResponse httpResponse = client.execute(
            "GET",
            new URL("https://localhost:" + webServer.getPort() + "/test"),
            "u1",
            new SecureString(new char[] { 'p' }),
            () -> null,
            is -> responseBuilder(is)
        );

        assertNotNull("Should have http response", httpResponse);
        assertEquals("Http status code does not match", 200, httpResponse.getHttpStatus());
        assertEquals("Http response body does not match", "complete", httpResponse.getResponseBody().get("test"));
    }

    public void testCommandLineClientCanTrustPinnedCaCertificateFingerprint() throws Exception {
        X509Certificate caCert = CertParsingUtils.readX509Certificate(caCertPath);
        CommandLineHttpClient client = new CommandLineHttpClient(
            (TestEnvironment.newEnvironment(Settings.builder().put("path.home", createTempDir()).build())),
            SslUtil.calculateFingerprint(caCert, "SHA-256")
        );
        HttpResponse httpResponse = client.execute(
            "GET",
            new URL("https://localhost:" + webServer.getPort() + "/test"),
            "u1",
            new SecureString(new char[] { 'p' }),
            () -> null,
            is -> responseBuilder(is)
        );

        assertNotNull("Should have http response", httpResponse);
        assertEquals("Http status code does not match", 200, httpResponse.getHttpStatus());
        assertEquals("Http response body does not match", "complete", httpResponse.getResponseBody().get("test"));
    }

    /**
     * Checks certificate-chain validation when connecting with a pinned CA fingerprint.
     */
    public void testPinnedFingerprintValidatesCertificateChain() throws Exception {
        final List<Certificate> certificateChain = PemUtils.readCertificates(List.of(certPath));
        final X509Certificate leafCertificate = (X509Certificate) certificateChain.get(0);
        final PrivateKey leafKey = PemUtils.readPrivateKey(keyPath, () -> "testnode".toCharArray());
        // Use a separate CA certificate as the client's trust anchor.
        final X509Certificate pinnedCa = CertParsingUtils.readX509Certificate(
            getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/active-directory-ca.crt")
        );

        try (MockWebServer testServer = createMockWebServerPresentingChain(leafKey, leafCertificate, pinnedCa)) {
            testServer.enqueue(new MockResponse().setResponseCode(200).setBody("{\"test\": \"complete\"}"));
            testServer.start();

            final CommandLineHttpClient client = new CommandLineHttpClient(
                TestEnvironment.newEnvironment(Settings.builder().put("path.home", createTempDir()).build()),
                SslUtil.calculateFingerprint(pinnedCa, "SHA-256")
            );
            final URL url = new URL("https://localhost:" + testServer.getPort() + "/test");
            // SunJSSE reports the handshake failure as an SSLException. The FIPS JSSE provider throws
            // org.bouncycastle.tls.TlsFatalAlert, which is only present on the FIPS runtime classpath.
            final Exception thrown = expectThrows(
                Exception.class,
                () -> client.execute("GET", url, "u1", new SecureString(new char[] { 'p' }), () -> null, this::responseBuilder)
            );
            if (inFipsJvm()) {
                assertThat(thrown.getClass().getName(), equalTo("org.bouncycastle.tls.TlsFatalAlert"));
                Throwable cause = ExceptionsHelper.unwrap(thrown, CertificateException.class);
                assertThat(cause, instanceOf(CertificateException.class));
                assertThat(cause.getMessage(), containsString("Unable to construct a valid chain"));
            } else {
                assertThat(thrown, instanceOf(SSLException.class));
            }
        }
    }

    /**
     * An HTTP leaf signed by the pinned CA but outside its validity period is rejected.
     * {@link #testPinnedFingerprintValidatesCertificateChain} only covers a leaf that does not chain to the pinned CA.
     */
    public void testPinnedFingerprintRejectsExpiredLeaf() throws Exception {
        final X509Certificate leafCertificate = CertParsingUtils.readX509Certificate(
            getDataPath("/org/elasticsearch/xpack/security/authc/esnative/tool/expired-http.crt")
        );
        assertThat(leafCertificate.getNotAfter().before(new Date()), equalTo(true));
        // Same key as the valid fixture leaf. This certificate is that key, signed by ca.crt, expired in 2021.
        final PrivateKey leafKey = PemUtils.readPrivateKey(keyPath, () -> "testnode".toCharArray());
        final X509Certificate pinnedCa = CertParsingUtils.readX509Certificate(caCertPath);

        try (MockWebServer testServer = createMockWebServerPresentingChain(leafKey, leafCertificate, pinnedCa)) {
            testServer.enqueue(new MockResponse().setResponseCode(200).setBody("{\"test\": \"complete\"}"));
            testServer.start();

            final CommandLineHttpClient client = new CommandLineHttpClient(
                TestEnvironment.newEnvironment(Settings.builder().put("path.home", createTempDir()).build()),
                SslUtil.calculateFingerprint(pinnedCa, "SHA-256")
            );
            final URL url = new URL("https://localhost:" + testServer.getPort() + "/test");
            // SunJSSE reports the handshake failure as an SSLException. The FIPS JSSE provider throws
            // org.bouncycastle.tls.TlsFatalAlert, which is only present on the FIPS runtime classpath.
            final Exception thrown = expectThrows(
                Exception.class,
                () -> client.execute("GET", url, "u1", new SecureString(new char[] { 'p' }), () -> null, this::responseBuilder)
            );
            if (inFipsJvm()) {
                assertThat(thrown.getClass().getName(), equalTo("org.bouncycastle.tls.TlsFatalAlert"));
            } else {
                assertThat(thrown, instanceOf(SSLException.class));
            }
            // SunJSSE formats notAfter with Date.toString(), which uses the JVM default zone, so the
            // calendar year is not stable (Jan 1 2021 GMT is still 2020 in US zones). BouncyCastle on a
            // FIPS JVM reports the certificate's ASN.1 time instead
            // ("certificate expired on 20210101000000GMT+00:00"). Match this leaf's notAfter either way
            // to distinguish it from the fixture CA, which is also expired.
            Throwable cause = ExceptionsHelper.unwrap(thrown, CertificateExpiredException.class);
            assertThat(exceptionChain(thrown), cause, instanceOf(CertificateExpiredException.class));
            final String expectedExpiry = inFipsJvm()
                ? "certificate expired on 20210101000000GMT+00:00"
                : leafCertificate.getNotAfter().toString();
            assertThat(cause.getMessage(), containsString(expectedExpiry));
        }
    }

    public void testGetDefaultURLFailsWithHelpfulMessage() {
        Settings settings = Settings.builder().put("path.home", createTempDir()).put("network.host", "_ec2:privateIpv4_").build();
        CommandLineHttpClient client = new CommandLineHttpClient(TestEnvironment.newEnvironment(settings));
        assertThat(
            expectThrows(IllegalStateException.class, () -> client.getDefaultURL()).getMessage(),
            containsString("unable to determine default URL from settings, please use the -u option to explicitly provide the url")
        );
    }

    private MockWebServer createMockWebServer() {
        Settings settings = getHttpSslSettings().build();
        TestsSSLService sslService = new TestsSSLService(TestEnvironment.newEnvironment(settings));
        return new MockWebServer(sslService.sslContext("xpack.security.http.ssl."), false);
    }

    /**
     * Builds a mock HTTPS server with the supplied private key and certificate chain. A custom key manager allows tests
     * to supply certificate chains independently of key-store validation.
     */
    private MockWebServer createMockWebServerPresentingChain(
        PrivateKey leafKey,
        X509Certificate leaf,
        X509Certificate... additionalChainCerts
    ) throws Exception {
        final X509Certificate[] chain = new X509Certificate[additionalChainCerts.length + 1];
        chain[0] = leaf;
        System.arraycopy(additionalChainCerts, 0, chain, 1, additionalChainCerts.length);

        final X509ExtendedKeyManager keyManager = new X509ExtendedKeyManager() {
            @Override
            public String[] getServerAliases(String keyType, Principal[] issuers) {
                return new String[] { "server" };
            }

            @Override
            public String chooseServerAlias(String keyType, Principal[] issuers, Socket socket) {
                return "server";
            }

            @Override
            public String chooseEngineServerAlias(String keyType, Principal[] issuers, SSLEngine engine) {
                return "server";
            }

            @Override
            public String[] getClientAliases(String keyType, Principal[] issuers) {
                return null;
            }

            @Override
            public String chooseClientAlias(String[] keyType, Principal[] issuers, Socket socket) {
                return null;
            }

            @Override
            public String chooseEngineClientAlias(String[] keyType, Principal[] issuers, SSLEngine engine) {
                return null;
            }

            @Override
            public X509Certificate[] getCertificateChain(String alias) {
                return chain.clone();
            }

            @Override
            public PrivateKey getPrivateKey(String alias) {
                return leafKey;
            }
        };

        final SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(new KeyManager[] { keyManager }, null, null);
        return new MockWebServer(sslContext, false);
    }

    private Settings.Builder getHttpSslSettings() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.security.http.ssl.secure_key_passphrase", "testnode");
        return Settings.builder()
            .put("path.home", createTempDir())
            .put("xpack.security.http.ssl.enabled", true)
            .put("xpack.security.http.ssl.key", keyPath.toString())
            .put("xpack.security.http.ssl.certificate", certPath.toString())
            .setSecureSettings(secureSettings);
    }

    private static String exceptionChain(Throwable thrown) {
        StringBuilder chain = new StringBuilder();
        for (Throwable current = thrown; current != null; current = current.getCause()) {
            if (chain.length() > 0) {
                chain.append(" -> ");
            }
            chain.append(current.getClass().getName()).append(": ").append(current.getMessage());
        }
        return chain.toString();
    }

    private HttpResponseBuilder responseBuilder(final InputStream is) throws IOException {
        final HttpResponseBuilder httpResponseBuilder = new HttpResponseBuilder();
        if (is != null) {
            byte[] bytes = toByteArray(is);
            httpResponseBuilder.withResponseBody(new String(bytes, StandardCharsets.UTF_8));
        }
        return httpResponseBuilder;
    }

    private byte[] toByteArray(InputStream is) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] internalBuffer = new byte[1024];
        int read = is.read(internalBuffer);
        while (read != -1) {
            baos.write(internalBuffer, 0, read);
            read = is.read(internalBuffer);
        }
        return baos.toByteArray();
    }
}
