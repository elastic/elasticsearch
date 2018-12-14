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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.AccessController;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.util.Collections;
import java.util.List;
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
        assumeFalse("Can't run in a FIPS JVM", inFipsJvm());
        final Path tempDir = createTempDir();
        final Path keystorePath = tempDir.resolve("testnode.jks");
        final Path updatedKeystorePath = tempDir.resolve("testnode_updated.jks");
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks"), keystorePath);
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode_updated.jks"), updatedKeystorePath);
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
                    atomicMoveIfPossible(updatedKeystorePath, keystorePath);
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
                    assertThat(sslException.getCause().getMessage(), containsString("PKIX path validation failed"));
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
    public void testPEMKeyConfigReloading() throws Exception {
        Path tempDir = createTempDir();
        Path keyPath = tempDir.resolve("testnode.pem");
        Path updatedKeyPath = tempDir.resolve("testnode_updated.pem");
        Path certPath = tempDir.resolve("testnode.crt");
        Path updatedCertPath = tempDir.resolve("testnode_updated.crt");
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem"), keyPath);
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode_updated.pem"), updatedKeyPath);
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode_updated.crt"), updatedCertPath);
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
        try (CloseableHttpClient client = getSSLClient(Collections.singletonList(certPath))) {
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
                    assertThat(sslException.getCause().getMessage(), containsString("PKIX path validation failed"));
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
        assumeFalse("Can't run in a FIPS JVM", inFipsJvm());
        Path tempDir = createTempDir();
        Path trustStorePath = tempDir.resolve("testnode.jks");
        Path updatedTruststorePath = tempDir.resolve("testnode_updated.jks");
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks"), trustStorePath);
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode_updated.jks"),
            updatedTruststorePath);
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.ssl.truststore.secure_password", "testnode");
        Settings settings = Settings.builder()
            .put("xpack.ssl.truststore.path", trustStorePath)
            .put("path.home", createTempDir())
            .setSecureSettings(secureSettings)
            .build();
        Environment env = randomBoolean() ? null : TestEnvironment.newEnvironment(settings);
        // Create the MockWebServer once for both pre and post checks
        try (MockWebServer server = getSslServer(trustStorePath, "testnode")) {
            final Consumer<SSLContext> trustMaterialPreChecks = (context) -> {
                try (CloseableHttpClient client = HttpClients.custom().setSSLContext(context).build()) {
                    privilegedConnect(() -> client.execute(new HttpGet("https://localhost:" + server.getPort())).close());
                } catch (Exception e) {
                    throw new RuntimeException("Error connecting to the mock server", e);
                }
            };

            final Runnable modifier = () -> {
                try {
                    atomicMoveIfPossible(updatedTruststorePath, trustStorePath);
                } catch (Exception e) {
                    throw new RuntimeException("failed to modify file", e);
                }
            };

            // Client's truststore doesn't contain the server's certificate anymore so SSLHandshake should fail
            final Consumer<SSLContext> trustMaterialPostChecks = (updatedContext) -> {
                try (CloseableHttpClient client = HttpClients.custom().setSSLContext(updatedContext).build()) {
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
        Path serverCertPath = tempDir.resolve("testnode.crt");
        Path serverKeyPath = tempDir.resolve("testnode.pem");
        Path updatedCert = tempDir.resolve("updated.crt");
        //Our keystore contains two Certificates it can present. One build from the RSA keypair and one build from the EC keypair. EC is
        // used since it keyManager presents the first one in alias alphabetical order (and testnode_ec comes before testnode_rsa)
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"), serverCertPath);
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem"), serverKeyPath);
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode_updated.crt"), updatedCert);
        Settings settings = Settings.builder()
            .put("xpack.ssl.certificate_authorities", serverCertPath)
            .put("path.home", createTempDir())
            .build();
        Environment env = randomBoolean() ? null : TestEnvironment.newEnvironment(settings);
        // Create the MockWebServer once for both pre and post checks
        try (MockWebServer server = getSslServer(serverKeyPath, serverCertPath, "testnode")) {
            final Consumer<SSLContext> trustMaterialPreChecks = (context) -> {
                try (CloseableHttpClient client = HttpClients.custom().setSSLContext(context).build()) {
                    privilegedConnect(() -> client.execute(new HttpGet("https://localhost:" + server.getPort())).close());
                } catch (Exception e) {
                    throw new RuntimeException("Exception connecting to the mock server", e);
                }
            };

            final Runnable modifier = () -> {
                try {
                    atomicMoveIfPossible(updatedCert, serverCertPath);
                } catch (Exception e) {
                    throw new RuntimeException("failed to modify file", e);
                }
            };

            // Client doesn't trust the Server certificate anymore so SSLHandshake should fail
            final Consumer<SSLContext> trustMaterialPostChecks = (updatedContext) -> {
                try (CloseableHttpClient client = HttpClients.custom().setSSLContext(updatedContext).build()) {
                    SSLHandshakeException sslException = expectThrows(SSLHandshakeException.class, () ->
                        privilegedConnect(() -> client.execute(new HttpGet("https://localhost:" + server.getPort())).close()));
                    assertThat(sslException.getCause().getMessage(), containsString("PKIX path validation failed"));
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
        assumeFalse("Can't run in a FIPS JVM", inFipsJvm());
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
        final SSLConfiguration config = sslService.getSSLConfiguration("xpack.ssl");
        new SSLConfigurationReloader(env, sslService, resourceWatcherService) {
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
        final SSLConfiguration config = sslService.getSSLConfiguration("xpack.ssl");
        new SSLConfigurationReloader(env, sslService, resourceWatcherService) {
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
        final SSLConfiguration config = sslService.getSSLConfiguration("xpack.ssl");
        new SSLConfigurationReloader(env, sslService, resourceWatcherService) {
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
        final SSLConfiguration config = sslService.getSSLConfiguration("xpack.ssl");
        new SSLConfigurationReloader(env, sslService, resourceWatcherService) {
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
        final SSLConfiguration config = sslService.getSSLConfiguration("xpack.ssl");
        new SSLConfigurationReloader(env, sslService, resourceWatcherService) {
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
        try (InputStream is = Files.newInputStream(keyStorePath)) {
            keyStore.load(is, keyStorePass.toCharArray());
        }
        final SSLContext sslContext = new SSLContextBuilder().loadKeyMaterial(keyStore, keyStorePass.toCharArray())
            .build();
        MockWebServer server = new MockWebServer(sslContext, false);
        server.enqueue(new MockResponse().setResponseCode(200).setBody("body"));
        server.start();
        return server;
    }

    private static MockWebServer getSslServer(Path keyPath, Path certPath, String password) throws KeyStoreException, CertificateException,
        NoSuchAlgorithmException, IOException, KeyManagementException, UnrecoverableKeyException {
        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        keyStore.load(null, password.toCharArray());
        keyStore.setKeyEntry("testnode_ec", PemUtils.readPrivateKey(keyPath, password::toCharArray), password.toCharArray(),
            CertParsingUtils.readCertificates(Collections.singletonList(certPath)));
        final SSLContext sslContext = new SSLContextBuilder().loadKeyMaterial(keyStore, password.toCharArray())
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
        try (InputStream is = Files.newInputStream(trustStorePath)) {
            trustStore.load(is, trustStorePass.toCharArray());
        }
        final SSLContext sslContext = new SSLContextBuilder().loadTrustMaterial(trustStore, null).build();
        return HttpClients.custom().setSSLContext(sslContext).build();
    }

    /**
     * Creates a {@link CloseableHttpClient} that only trusts the given certificate(s)
     *
     * @param trustedCertificatePaths The certificates this client trusts
     **/
    private static CloseableHttpClient getSSLClient(List<Path> trustedCertificatePaths) throws KeyStoreException,
        NoSuchAlgorithmException,
        KeyManagementException, IOException, CertificateException {
        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null, null);
        for (Certificate cert : CertParsingUtils.readCertificates(trustedCertificatePaths)) {
            trustStore.setCertificateEntry(cert.toString(), cert);
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
