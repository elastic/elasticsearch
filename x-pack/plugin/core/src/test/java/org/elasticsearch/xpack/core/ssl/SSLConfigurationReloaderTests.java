/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ssl;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.openssl.jcajce.JcePEMEncryptorBuilder;
import org.elasticsearch.common.CheckedRunnable;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.junit.After;
import org.junit.Before;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import javax.security.auth.x500.X500Principal;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.AccessController;
import java.security.KeyManagementException;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Unit tests for the reloading of SSL configuration
 */
public class SSLConfigurationReloaderTests extends ESTestCase {

    private ThreadPool threadPool;
    private ResourceWatcherService resourceWatcherService;

    @Before
    public void setup() {
        threadPool = new TestThreadPool("reload tests");
        resourceWatcherService =
                new ResourceWatcherService(Settings.builder().put("resource.reload.interval.high", "1s").build(), threadPool);
        resourceWatcherService.start();
    }

    @After
    public void cleanup() throws Exception {
        if (threadPool != null) {
            terminate(threadPool);
        }
    }

    /**
     * Tests reloading a keystore that is used in the KeyManager of SSLContext
     */
    public void testReloadingKeyStore() throws Exception {
        final Path tempDir = createTempDir();
        final Path keystorePath = tempDir.resolve("testnode.jks");
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks"), keystorePath);
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.ssl.keystore.secure_password", "testnode");
        final Settings settings = Settings.builder()
                .put("path.home", createTempDir())
                .put("xpack.ssl.keystore.path", keystorePath)
                .setSecureSettings(secureSettings)
                .build();
        final Environment env = randomBoolean() ? null : TestEnvironment.newEnvironment(settings);
        //Load HTTPClient only once. Client uses the same store as a truststore
        try (CloseableHttpClient client = getSSLClient(keystorePath, "testnode")) {
            final Consumer<SSLContext> keyMaterialPreChecks = (context) -> {
                try (MockWebServer server = new MockWebServer(context, true)) {
                    server.enqueue(new MockResponse().setResponseCode(200).setBody("body"));
                    server.start();
                    privilegedConnect(() -> client.execute(new HttpGet("https://localhost:" + server.getPort())).close());
                } catch (Exception e) {
                    throw new RuntimeException("Exception starting or connecting to the mock server", e);
                }
            };

            final Runnable modifier = () -> {
                try {
                    // modify the keystore that the KeyManager uses
                    KeyStore keyStore = KeyStore.getInstance("jks");
                    keyStore.load(null, null);
                    final KeyPair keyPair = CertUtils.generateKeyPair(512);
                    X509Certificate cert = CertUtils.generateSignedCertificate(new X500Principal("CN=localhost"), null, keyPair,
                        null, null, 365);
                    keyStore.setKeyEntry("key", keyPair.getPrivate(), "testnode".toCharArray(), new X509Certificate[]{cert});
                    Path updated = tempDir.resolve("updated.jks");
                    try (OutputStream out = Files.newOutputStream(updated)) {
                        keyStore.store(out, "testnode".toCharArray());
                    }
                    atomicMoveIfPossible(updated, keystorePath);
                } catch (Exception e) {
                    throw new RuntimeException("modification failed", e);
                }
            };
            // The new server certificate is not in the client's truststore so SSLHandshake should fail
            final Consumer<SSLContext> keyMaterialPostChecks = (updatedContext) -> {
                try (MockWebServer server = new MockWebServer(updatedContext, true)) {
                    server.enqueue(new MockResponse().setResponseCode(200).setBody("body"));
                    server.start();
                    SSLHandshakeException sslException = expectThrows(SSLHandshakeException.class, () ->
                        privilegedConnect(() -> client.execute(new HttpGet("https://localhost:" + server.getPort())).close()));
                    assertThat(sslException.getCause().getMessage(), containsString("PKIX path building failed"));
                } catch (Exception e) {
                    throw new RuntimeException("Exception starting or connecting to the mock server", e);
                }
            };
            validateSSLConfigurationIsReloaded(settings, env, keyMaterialPreChecks, modifier, keyMaterialPostChecks);
        }
    }

    /**
     * Tests the reloading of SSLContext when a PEM key and certificate are used.
     */
    public void testPEMKeyCertConfigReloading() throws Exception {
        final Path tempDir = createTempDir();
        final Path keyPath = tempDir.resolve("testnode.pem");
        final Path certPath = tempDir.resolve("testnode.crt");
        final Path clientTruststorePath = tempDir.resolve("testnode.jks");
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks"), clientTruststorePath);
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem"), keyPath);
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"), certPath);
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.ssl.secure_key_passphrase", "testnode");
        final Settings settings = Settings.builder()
            .put("path.home", createTempDir())
            .put("xpack.ssl.key", keyPath)
            .put("xpack.ssl.certificate", certPath)
            .setSecureSettings(secureSettings)
            .build();
        final Environment env = randomBoolean() ? null :
            TestEnvironment.newEnvironment(Settings.builder().put("path.home", createTempDir()).build());
        // Load HTTPClient once. Client uses a keystore containing testnode key/cert as a truststore
        try (CloseableHttpClient client = getSSLClient(clientTruststorePath, "testnode")) {
            final Consumer<SSLContext> keyMaterialPreChecks = (context) -> {
                try (MockWebServer server = new MockWebServer(context, false)) {
                    server.enqueue(new MockResponse().setResponseCode(200).setBody("body"));
                    server.start();
                    privilegedConnect(() -> client.execute(new HttpGet("https://localhost:" + server.getPort())).close());
                } catch (Exception e) {
                    throw new RuntimeException("Exception starting or connecting to the mock server", e);
                }
            };
            final Runnable modifier = () -> {
                try {
                    final KeyPair keyPair = CertUtils.generateKeyPair(512);
                    X509Certificate cert = CertUtils.generateSignedCertificate(new X500Principal("CN=localhost"), null, keyPair,
                        null, null, 365);
                    Path updatedKeyPath = tempDir.resolve("updated.pem");
                    Path updatedCertPath = tempDir.resolve("updated.crt");
                    try (OutputStream os = Files.newOutputStream(updatedKeyPath);
                         OutputStreamWriter osWriter = new OutputStreamWriter(os, StandardCharsets.UTF_8);
                         JcaPEMWriter writer = new JcaPEMWriter(osWriter)) {
                        writer.writeObject(keyPair,
                                new JcePEMEncryptorBuilder("DES-EDE3-CBC").setProvider(CertUtils.BC_PROV).build("testnode".toCharArray()));
                    }
                    try (BufferedWriter out = Files.newBufferedWriter(updatedCertPath);
                         JcaPEMWriter pemWriter = new JcaPEMWriter(out)) {
                        pemWriter.writeObject(cert);
                    }
                    atomicMoveIfPossible(updatedKeyPath, keyPath);
                    atomicMoveIfPossible(updatedCertPath, certPath);
                } catch (Exception e) {
                    throw new RuntimeException("failed to modify file", e);
                }
            };
            // The new server certificate is not in the client's truststore so SSLHandshake should fail
            final Consumer<SSLContext> keyMaterialPostChecks = (updatedContext) -> {
                try (MockWebServer server = new MockWebServer(updatedContext, false)) {
                    server.enqueue(new MockResponse().setResponseCode(200).setBody("body"));
                    server.start();
                    SSLHandshakeException sslException = expectThrows(SSLHandshakeException.class, () ->
                        privilegedConnect(() -> client.execute(new HttpGet("https://localhost:" + server.getPort())).close()));
                    assertThat(sslException.getCause().getMessage(), containsString("PKIX path building failed"));
                } catch (Exception e) {
                    throw new RuntimeException("Exception starting or connecting to the mock server", e);
                }
            };

            validateSSLConfigurationIsReloaded(settings, env, keyMaterialPreChecks, modifier, keyMaterialPostChecks);
        }
    }

    /**
     * Tests the reloading of SSLContext when the trust store is modified. The same store is used as a TrustStore (for the
     * reloadable SSLContext used in the HTTPClient) and as a KeyStore for the MockWebServer
     */
    public void testReloadingTrustStore() throws Exception {
        Path tempDir = createTempDir();
        Path trustStorePath = tempDir.resolve("testnode.jks");
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks"), trustStorePath);
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.ssl.truststore.secure_password", "testnode");
        Settings settings = Settings.builder()
            .put("xpack.ssl.truststore.path", trustStorePath)
            .put("path.home", createTempDir())
            .setSecureSettings(secureSettings)
            .build();
        Environment env = randomBoolean() ? null : TestEnvironment.newEnvironment(settings);
        // Create the MockWebServer once for both pre and post checks
        try(MockWebServer server = getSslServer(trustStorePath, "testnode")){
            final Consumer<SSLContext> trustMaterialPreChecks = (context) -> {
                try (CloseableHttpClient client = HttpClients.custom().setSSLContext(context).build()){
                    privilegedConnect(() -> client.execute(new HttpGet("https://localhost:" + server.getPort())).close());
                } catch (Exception e) {
                    throw new RuntimeException("Error connecting to the mock server", e);
                }
            };

            final Runnable modifier = () -> {
                try {
                    Path updatedTrustStore = tempDir.resolve("updated.jks");
                    KeyStore keyStore = KeyStore.getInstance("jks");
                    keyStore.load(null, null);
                    final KeyPair keyPair = CertUtils.generateKeyPair(512);
                    X509Certificate cert = CertUtils.generateSignedCertificate(new X500Principal("CN=localhost"), null, keyPair,
                        null, null, 365);
                    keyStore.setKeyEntry("newKey", keyPair.getPrivate(), "testnode".toCharArray(), new Certificate[]{cert});
                    try (OutputStream out = Files.newOutputStream(updatedTrustStore)) {
                        keyStore.store(out, "testnode".toCharArray());
                    }
                    atomicMoveIfPossible(updatedTrustStore, trustStorePath);
                } catch (Exception e) {
                    throw new RuntimeException("failed to modify file", e);
                }
            };

            // Client's truststore doesn't contain the server's certificate anymore so SSLHandshake should fail
            final Consumer<SSLContext> trustMaterialPostChecks = (updatedContext) -> {
                try (CloseableHttpClient client = HttpClients.custom().setSSLContext(updatedContext).build()){
                    SSLHandshakeException sslException = expectThrows(SSLHandshakeException.class, () ->
                        privilegedConnect(() -> client.execute(new HttpGet("https://localhost:" + server.getPort())).close()));
                    assertThat(sslException.getCause().getMessage(), containsString("PKIX path building failed"));
                } catch (Exception e) {
                    throw new RuntimeException("Error closing CloseableHttpClient", e);
                }
            };

            validateSSLConfigurationIsReloaded(settings, env, trustMaterialPreChecks, modifier, trustMaterialPostChecks);
        }
    }

    /**
     * Test the reloading of SSLContext whose trust config is backed by PEM certificate files.
     */
    public void testReloadingPEMTrustConfig() throws Exception {
        Path tempDir = createTempDir();
        Path clientCertPath = tempDir.resolve("testnode.crt");
        Path keyStorePath = tempDir.resolve("testnode.jks");
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks"), keyStorePath);
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"), clientCertPath);
        Settings settings = Settings.builder()
                .putList("xpack.ssl.certificate_authorities", clientCertPath.toString())
                .put("path.home", createTempDir())
                .build();
        Environment env = randomBoolean() ? null : TestEnvironment.newEnvironment(settings);
        // Create the MockWebServer once for both pre and post checks
        try(MockWebServer server = getSslServer(keyStorePath, "testnode")){
            final Consumer<SSLContext> trustMaterialPreChecks = (context) -> {
                try (CloseableHttpClient client = HttpClients.custom().setSSLContext(context).build()){
                    privilegedConnect(() -> client.execute(new HttpGet("https://localhost:" + server.getPort())).close());
                } catch (Exception e) {
                    throw new RuntimeException("Exception connecting to the mock server", e);
                }
            };

            final Runnable modifier = () -> {
                try {
                    final KeyPair keyPair = CertUtils.generateKeyPair(512);
                    X509Certificate cert = CertUtils.generateSignedCertificate(new X500Principal("CN=localhost"), null, keyPair,
                        null, null, 365);
                    Path updatedCertPath = tempDir.resolve("updated.crt");
                    try (BufferedWriter out = Files.newBufferedWriter(updatedCertPath);
                         JcaPEMWriter pemWriter = new JcaPEMWriter(out)) {
                        pemWriter.writeObject(cert);
                    }
                    atomicMoveIfPossible(updatedCertPath, clientCertPath);
                } catch (Exception e) {
                    throw new RuntimeException("failed to modify file", e);
                }
            };
            // Client doesn't trust the Server certificate anymore so SSLHandshake should fail
            final Consumer<SSLContext> trustMaterialPostChecks = (updatedContext) -> {
                try (CloseableHttpClient client = HttpClients.custom().setSSLContext(updatedContext).build()){
                    SSLHandshakeException sslException = expectThrows(SSLHandshakeException.class, () ->
                        privilegedConnect(() -> client.execute(new HttpGet("https://localhost:" + server.getPort())).close()));
                    assertThat(sslException.getCause().getMessage(), containsString("PKIX path building failed"));
                } catch (Exception e) {
                    throw new RuntimeException("Error closing CloseableHttpClient", e);
                }
            };

            validateSSLConfigurationIsReloaded(settings, env, trustMaterialPreChecks, modifier, trustMaterialPostChecks);
        }
    }

    /**
     * Tests the reloading of a keystore when there is an exception during reloading. An exception is caused by truncating the keystore
     * that is being monitored
     */
    public void testReloadingKeyStoreException() throws Exception {
        Path tempDir = createTempDir();
        Path keystorePath = tempDir.resolve("testnode.jks");
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks"), keystorePath);
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.ssl.keystore.secure_password", "testnode");
        Settings settings = Settings.builder()
                .put("xpack.ssl.keystore.path", keystorePath)
                .setSecureSettings(secureSettings)
                .put("path.home", createTempDir())
                .build();
        Environment env = randomBoolean() ? null : TestEnvironment.newEnvironment(settings);
        final SSLService sslService = new SSLService(settings, env);
        final SSLConfiguration config = sslService.sslConfiguration(Settings.EMPTY);
        new SSLConfigurationReloader(settings, env, sslService, resourceWatcherService) {
            @Override
            void reloadSSLContext(SSLConfiguration configuration) {
                fail("reload should not be called! [keystore reload exception]");
            }
        };

        final SSLContext context = sslService.sslContextHolder(config).sslContext();

        // truncate the keystore
        try (OutputStream out = Files.newOutputStream(keystorePath, StandardOpenOption.TRUNCATE_EXISTING)) {
        }

        // we intentionally don't wait here as we rely on concurrency to catch a failure
        assertThat(sslService.sslContextHolder(config).sslContext(), sameInstance(context));
    }

    /**
     * Tests the reloading of a key config backed by pem files when there is an exception during reloading. An exception is caused by
     * truncating the key file that is being monitored
     */
    public void testReloadingPEMKeyConfigException() throws Exception {
        Path tempDir = createTempDir();
        Path keyPath = tempDir.resolve("testnode.pem");
        Path certPath = tempDir.resolve("testnode.crt");
        Path clientCertPath = tempDir.resolve("testclient.crt");
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem"), keyPath);
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"), certPath);
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.crt"), clientCertPath);
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.ssl.secure_key_passphrase", "testnode");
        Settings settings = Settings.builder()
                .put("xpack.ssl.key", keyPath)
                .put("xpack.ssl.certificate", certPath)
                .putList("xpack.ssl.certificate_authorities", certPath.toString(), clientCertPath.toString())
                .put("path.home", createTempDir())
                .setSecureSettings(secureSettings)
                .build();
        Environment env = randomBoolean() ? null : TestEnvironment.newEnvironment(settings);
        final SSLService sslService = new SSLService(settings, env);
        final SSLConfiguration config = sslService.sslConfiguration(Settings.EMPTY);
        new SSLConfigurationReloader(settings, env, sslService, resourceWatcherService) {
            @Override
            void reloadSSLContext(SSLConfiguration configuration) {
                fail("reload should not be called! [pem key reload exception]");
            }
        };

        final SSLContext context = sslService.sslContextHolder(config).sslContext();

        // truncate the file
        try (OutputStream os = Files.newOutputStream(keyPath, StandardOpenOption.TRUNCATE_EXISTING)) {
        }

        // we intentionally don't wait here as we rely on concurrency to catch a failure
        assertThat(sslService.sslContextHolder(config).sslContext(), sameInstance(context));
    }

    /**
     * Tests the reloading of a truststore when there is an exception during reloading. An exception is caused by truncating the truststore
     * that is being monitored
     */
    public void testTrustStoreReloadException() throws Exception {
        Path tempDir = createTempDir();
        Path trustStorePath = tempDir.resolve("testnode.jks");
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks"), trustStorePath);
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.ssl.truststore.secure_password", "testnode");
        Settings settings = Settings.builder()
                .put("xpack.ssl.truststore.path", trustStorePath)
                .put("path.home", createTempDir())
                .setSecureSettings(secureSettings)
                .build();
        Environment env = randomBoolean() ? null : TestEnvironment.newEnvironment(settings);
        final SSLService sslService = new SSLService(settings, env);
        final SSLConfiguration config = sslService.sslConfiguration(Settings.EMPTY);
        new SSLConfigurationReloader(settings, env, sslService, resourceWatcherService) {
            @Override
            void reloadSSLContext(SSLConfiguration configuration) {
                fail("reload should not be called! [truststore reload exception]");
            }
        };

        final SSLContext context = sslService.sslContextHolder(config).sslContext();

        // truncate the truststore
        try (OutputStream os = Files.newOutputStream(trustStorePath, StandardOpenOption.TRUNCATE_EXISTING)) {
        }

        // we intentionally don't wait here as we rely on concurrency to catch a failure
        assertThat(sslService.sslContextHolder(config).sslContext(), sameInstance(context));
    }

    /**
     * Tests the reloading of a trust config backed by pem files when there is an exception during reloading. An exception is caused by
     * truncating the certificate file that is being monitored
     */
    public void testPEMTrustReloadException() throws Exception {
        Path tempDir = createTempDir();
        Path clientCertPath = tempDir.resolve("testclient.crt");
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.crt"), clientCertPath);
        Settings settings = Settings.builder()
                .putList("xpack.ssl.certificate_authorities", clientCertPath.toString())
                .put("path.home", createTempDir())
                .build();
        Environment env = randomBoolean() ? null : TestEnvironment.newEnvironment(settings);
        final SSLService sslService = new SSLService(settings, env);
        final SSLConfiguration config = sslService.sslConfiguration(Settings.EMPTY);
        new SSLConfigurationReloader(settings, env, sslService, resourceWatcherService) {
            @Override
            void reloadSSLContext(SSLConfiguration configuration) {
                fail("reload should not be called! [pem trust reload exception]");
            }
        };

        final SSLContext context = sslService.sslContextHolder(config).sslContext();

        // write bad file
        Path updatedCert = tempDir.resolve("updated.crt");
        try (OutputStream os = Files.newOutputStream(updatedCert)) {
            os.write(randomByte());
        }
        atomicMoveIfPossible(updatedCert, clientCertPath);

        // we intentionally don't wait here as we rely on concurrency to catch a failure
        assertThat(sslService.sslContextHolder(config).sslContext(), sameInstance(context));
    }

    private void validateSSLConfigurationIsReloaded(Settings settings, Environment env,
                                                    Consumer<SSLContext> preChecks,
                                                    Runnable modificationFunction,
                                                    Consumer<SSLContext> postChecks)
                                                    throws Exception {

        final CountDownLatch reloadLatch = new CountDownLatch(1);
        final SSLService sslService = new SSLService(settings, env);
        final SSLConfiguration config = sslService.sslConfiguration(Settings.EMPTY);
        new SSLConfigurationReloader(settings, env, sslService, resourceWatcherService) {
            @Override
            void reloadSSLContext(SSLConfiguration configuration) {
                super.reloadSSLContext(configuration);
                reloadLatch.countDown();
            }
        };
        // Baseline checks
        preChecks.accept(sslService.sslContextHolder(config).sslContext());

        assertEquals("nothing should have called reload", 1, reloadLatch.getCount());

        // modify
        modificationFunction.run();
        reloadLatch.await();
        // checks after reload
        postChecks.accept(sslService.sslContextHolder(config).sslContext());
    }

    private static void atomicMoveIfPossible(Path source, Path target) throws IOException {
        try {
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static MockWebServer getSslServer(Path keyStorePath, String keyStorePass) throws KeyStoreException, CertificateException,
        NoSuchAlgorithmException, IOException, KeyManagementException, UnrecoverableKeyException {
        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        try(InputStream is = Files.newInputStream(keyStorePath)) {
            keyStore.load(is, keyStorePass.toCharArray());
        }
        final SSLContext sslContext = new SSLContextBuilder().loadKeyMaterial(keyStore, keyStorePass.toCharArray())
            .build();
        MockWebServer server = new MockWebServer(sslContext, false);
        server.enqueue(new MockResponse().setResponseCode(200).setBody("body"));
        server.start();
        return server;
    }

    private static CloseableHttpClient getSSLClient(Path trustStorePath, String trustStorePass) throws KeyStoreException,
        NoSuchAlgorithmException,
        KeyManagementException, IOException, CertificateException {
        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        try(InputStream is = Files.newInputStream(trustStorePath)) {
            trustStore.load(is, trustStorePass.toCharArray());
        }
        final SSLContext sslContext = new SSLContextBuilder().loadTrustMaterial(trustStore, null).build();
        return HttpClients.custom().setSSLContext(sslContext).build();
    }

    private static void privilegedConnect(CheckedRunnable<Exception> runnable) throws Exception {
        try {
            AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                runnable.run();
                return null;
            });
        } catch (PrivilegedActionException e) {
            throw (Exception) e.getCause();
        }
    }

}
