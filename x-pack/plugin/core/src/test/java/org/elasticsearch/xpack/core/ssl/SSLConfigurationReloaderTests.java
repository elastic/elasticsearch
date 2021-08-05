/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ssl;

import org.apache.http.HttpConnectionMetrics;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.HttpConnectionFactory;
import org.apache.http.conn.ManagedHttpClientConnection;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.ManagedHttpClientConnectionFactory;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContextBuilder;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.PemUtils;
import org.elasticsearch.common.ssl.SslConfiguration;
import org.elasticsearch.core.CheckedRunnable;
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
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.AccessControlException;
import java.security.AccessController;
import java.security.GeneralSecurityException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;

import static org.elasticsearch.test.TestMatchers.throwableWithMessage;
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
    }

    @After
    public void cleanup() {
        if (resourceWatcherService != null) {
            resourceWatcherService.close();
        }
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
        secureSettings.setString("xpack.security.transport.ssl.keystore.secure_password", "testnode");
        final Settings settings = Settings.builder()
            .put("path.home", createTempDir())
            .put("xpack.security.transport.ssl.enabled", true)
            .put("xpack.security.transport.ssl.keystore.path", keystorePath)
            .setSecureSettings(secureSettings)
            .build();
        final Environment env = TestEnvironment.newEnvironment(settings);
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
            validateSSLConfigurationIsReloaded(env, keyMaterialPreChecks, modifier, keyMaterialPostChecks);
        }
    }
    /**
     * Tests the reloading of SSLContext when a PEM key and certificate are used.
     */
    public void testPEMKeyConfigReloading() throws Exception {
        assumeFalse("https://github.com/elastic/elasticsearch/issues/49094", inFipsJvm());
        Path tempDir = createTempDir();
        Path keyPath = tempDir.resolve("testnode.pem");
        Path certPath = tempDir.resolve("testnode.crt");
        Path updatedKeyPath = tempDir.resolve("testnode_updated.pem");
        Path updatedCertPath = tempDir.resolve("testnode_updated.crt");
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem"), keyPath);
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"), certPath);
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode_updated.pem"), updatedKeyPath);
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode_updated.crt"), updatedCertPath);
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.security.transport.ssl.secure_key_passphrase", "testnode");
        final Settings settings = Settings.builder()
            .put("path.home", createTempDir())
            .put("xpack.security.transport.ssl.enabled", true)
            .put("xpack.security.transport.ssl.key", keyPath)
            .put("xpack.security.transport.ssl.certificate", certPath)
            .putList("xpack.security.transport.ssl.certificate_authorities", certPath.toString())
            .setSecureSettings(secureSettings)
            .build();
        final Environment env = TestEnvironment.newEnvironment(settings);
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
            validateSSLConfigurationIsReloaded(env, keyMaterialPreChecks, modifier, keyMaterialPostChecks);
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
        secureSettings.setString("xpack.security.transport.ssl.truststore.secure_password", "testnode");
        final Settings settings = baseKeystoreSettings(tempDir, secureSettings)
            .put("xpack.security.transport.ssl.enabled", true)
            .put("xpack.security.transport.ssl.truststore.path", trustStorePath)
            .put("path.home", createTempDir())
            .build();
        Environment env = TestEnvironment.newEnvironment(settings);
        // Create the MockWebServer once for both pre and post checks
        try (MockWebServer server = getSslServer(trustStorePath, "testnode")) {
            final Consumer<SSLContext> trustMaterialPreChecks = (context) -> {
                try (CloseableHttpClient client = createHttpClient(context)) {
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
                try (CloseableHttpClient client = createHttpClient(updatedContext)) {
                    SSLHandshakeException sslException = expectThrows(SSLHandshakeException.class, () ->
                        privilegedConnect(() -> client.execute(new HttpGet("https://localhost:" + server.getPort())).close()));
                    assertThat(sslException.getCause().getMessage(), containsString("PKIX path building failed"));
                } catch (Exception e) {
                    throw new RuntimeException("Error closing CloseableHttpClient", e);
                }
            };
            validateSSLConfigurationIsReloaded(env, trustMaterialPreChecks, modifier, trustMaterialPostChecks);
        }
    }

    /**
     * Test the reloading of SSLContext whose trust config is backed by PEM certificate files.
     */
    public void testReloadingPEMTrustConfig() throws Exception {
        assumeFalse("https://github.com/elastic/elasticsearch/issues/49094", inFipsJvm());
        Path tempDir = createTempDir();
        Path serverCertPath = tempDir.resolve("testnode.crt");
        Path serverKeyPath = tempDir.resolve("testnode.pem");
        Path updatedCertPath = tempDir.resolve("updated.crt");
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"), serverCertPath);
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem"), serverKeyPath);
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode_updated.crt"), updatedCertPath);
        Settings settings = baseKeystoreSettings(tempDir, null)
            .put("xpack.security.transport.ssl.enabled", true)
            .putList("xpack.security.transport.ssl.certificate_authorities", serverCertPath.toString())
            .put("path.home", createTempDir())
            .build();
        Environment env = TestEnvironment.newEnvironment(settings);
        // Create the MockWebServer once for both pre and post checks
        try (MockWebServer server = getSslServer(serverKeyPath, serverCertPath, "testnode")) {
            final Consumer<SSLContext> trustMaterialPreChecks = (context) -> {
                try (CloseableHttpClient client = createHttpClient(context)) {
                    privilegedConnect(() -> client.execute(new HttpGet("https://localhost:" + server.getPort())));//.close());
                } catch (Exception e) {
                    throw new RuntimeException("Exception connecting to the mock server", e);
                }
            };

            final Runnable modifier = () -> {
                try {
                    atomicMoveIfPossible(updatedCertPath, serverCertPath);
                } catch (Exception e) {
                    throw new RuntimeException("failed to modify file", e);
                }
            };

            // Client doesn't trust the Server certificate anymore so SSLHandshake should fail
            final Consumer<SSLContext> trustMaterialPostChecks = (updatedContext) -> {
                try (CloseableHttpClient client = createHttpClient(updatedContext)) {
                    SSLHandshakeException sslException = expectThrows(SSLHandshakeException.class, () ->
                        privilegedConnect(() -> client.execute(new HttpGet("https://localhost:" + server.getPort())).close()));
                    assertThat(sslException.getCause().getMessage(), containsString("PKIX path validation failed"));
                } catch (Exception e) {
                    throw new RuntimeException("Error closing CloseableHttpClient", e);
                }
            };
            validateSSLConfigurationIsReloaded(env, trustMaterialPreChecks, modifier, trustMaterialPostChecks);
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
        secureSettings.setString("xpack.security.transport.ssl.keystore.secure_password", "testnode");
        Settings settings = Settings.builder()
            .put("xpack.security.transport.ssl.enabled", true)
            .put("xpack.security.transport.ssl.keystore.path", keystorePath)
            .setSecureSettings(secureSettings)
            .put("path.home", createTempDir())
            .build();
        Environment env = TestEnvironment.newEnvironment(settings);
        final SSLService sslService = new SSLService(env);
        final SslConfiguration config = sslService.getSSLConfiguration("xpack.security.transport.ssl.");
        final AtomicReference<Exception> exceptionRef = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        final Consumer<SslConfiguration> reloadConsumer = sslConfiguration -> {
            try {
                sslService.reloadSSLContext(sslConfiguration);
            } catch (Exception e) {
                exceptionRef.set(e);
                throw e;
            } finally {
                latch.countDown();
            }
        };
        new SSLConfigurationReloader(reloadConsumer, resourceWatcherService, SSLService.getSSLConfigurations(env).values());

        final SSLContext context = sslService.sslContextHolder(config).sslContext();

        // truncate the keystore
        try (OutputStream ignore = Files.newOutputStream(keystorePath, StandardOpenOption.TRUNCATE_EXISTING)) {
            // do nothing
        }

        latch.await();
        assertNotNull(exceptionRef.get());
        assertThat(exceptionRef.get(), throwableWithMessage(containsString("cannot read configured [jks] keystore")));
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
        secureSettings.setString("xpack.security.transport.ssl.secure_key_passphrase", "testnode");
        Settings settings = Settings.builder()
            .put("xpack.security.transport.ssl.enabled", true)
            .put("xpack.security.transport.ssl.key", keyPath)
            .put("xpack.security.transport.ssl.certificate", certPath)
            .putList("xpack.security.transport.ssl.certificate_authorities", certPath.toString(), clientCertPath.toString())
            .put("path.home", createTempDir())
            .setSecureSettings(secureSettings)
            .build();
        Environment env = TestEnvironment.newEnvironment(settings);
        final SSLService sslService = new SSLService(env);
        final SslConfiguration config = sslService.getSSLConfiguration("xpack.security.transport.ssl.");
        final AtomicReference<Exception> exceptionRef = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        final Consumer<SslConfiguration> reloadConsumer = sslConfiguration -> {
            try {
                sslService.reloadSSLContext(sslConfiguration);
            } catch (Exception e) {
                exceptionRef.set(e);
                throw e;
            } finally {
                latch.countDown();
            }
        };
        new SSLConfigurationReloader(reloadConsumer, resourceWatcherService, SSLService.getSSLConfigurations(env).values());

        final SSLContext context = sslService.sslContextHolder(config).sslContext();

        // truncate the file
        try (OutputStream ignore = Files.newOutputStream(keyPath, StandardOpenOption.TRUNCATE_EXISTING)) {
        }

        latch.await();
        assertNotNull(exceptionRef.get());
        assertThat(exceptionRef.get().getMessage(), containsString("Error parsing Private Key"));
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
        secureSettings.setString("xpack.security.transport.ssl.truststore.secure_password", "testnode");
        Settings settings = baseKeystoreSettings(tempDir, secureSettings)
            .put("xpack.security.transport.ssl.enabled", true)
            .put("xpack.security.transport.ssl.truststore.path", trustStorePath)
            .put("path.home", createTempDir())
            .build();
        Environment env = TestEnvironment.newEnvironment(settings);
        final SSLService sslService = new SSLService(env);
        final SslConfiguration config = sslService.getSSLConfiguration("xpack.security.transport.ssl.");
        final AtomicReference<Exception> exceptionRef = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        final Consumer<SslConfiguration> reloadConsumer = sslConfiguration -> {
            try {
                sslService.reloadSSLContext(sslConfiguration);
            } catch (Exception e) {
                exceptionRef.set(e);
                throw e;
            } finally {
                latch.countDown();
            }
        };
        new SSLConfigurationReloader(reloadConsumer, resourceWatcherService, SSLService.getSSLConfigurations(env).values());

        final SSLContext context = sslService.sslContextHolder(config).sslContext();

        // truncate the truststore
        try (OutputStream ignore = Files.newOutputStream(trustStorePath, StandardOpenOption.TRUNCATE_EXISTING)) {
        }

        latch.await();
        assertNotNull(exceptionRef.get());
        assertThat(exceptionRef.get(), throwableWithMessage(containsString("cannot read configured [jks] keystore (as a truststore)")));
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
        Settings settings = baseKeystoreSettings(tempDir, null)
            .put("xpack.security.transport.ssl.enabled", true)
            .putList("xpack.security.transport.ssl.certificate_authorities", clientCertPath.toString())
            .put("path.home", createTempDir())
            .build();
        Environment env = TestEnvironment.newEnvironment(settings);
        final SSLService sslService = new SSLService(env);
        final SslConfiguration config = sslService.sslConfiguration(settings.getByPrefix("xpack.security.transport.ssl."));
        final AtomicReference<Exception> exceptionRef = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        final Consumer<SslConfiguration> reloadConsumer = sslConfiguration -> {
            try {
                sslService.reloadSSLContext(sslConfiguration);
            } catch (Exception e) {
                exceptionRef.set(e);
                throw e;
            } finally {
                latch.countDown();
            }
        };
        new SSLConfigurationReloader(reloadConsumer, resourceWatcherService, SSLService.getSSLConfigurations(env).values());

        final SSLContext context = sslService.sslContextHolder(config).sslContext();

        // write bad file
        Path updatedCert = tempDir.resolve("updated.crt");
        try (OutputStream os = Files.newOutputStream(updatedCert)) {
            os.write(randomByte());
        }
        atomicMoveIfPossible(updatedCert, clientCertPath);

        latch.await();
        assertNotNull(exceptionRef.get());
        assertThat(exceptionRef.get(), throwableWithMessage(containsString("cannot load PEM certificate_authorities")));
        assertThat(sslService.sslContextHolder(config).sslContext(), sameInstance(context));
    }

    /**
     * Tests that the reloader doesn't throw an exception if a file is unreadable or configured to be outside of the config/ directory.
     * These errors are handled correctly by the relevant {@link org.elasticsearch.common.ssl.SslKeyConfig} and
     * {@link org.elasticsearch.common.ssl.SslTrustConfig} classes, so the reloader should simply log and continue.
     */
    public void testFailureToReadFileDoesntFail() throws Exception {
        Path tempDir = createTempDir();
        Path clientCertPath = tempDir.resolve("testclient.crt");
        Settings settings = baseKeystoreSettings(tempDir, null)
            .putList("xpack.security.transport.ssl.certificate_authorities", clientCertPath.toString())
            .put("path.home", createTempDir())
            .build();
        Environment env = TestEnvironment.newEnvironment(settings);

        final ResourceWatcherService mockResourceWatcher = Mockito.mock(ResourceWatcherService.class);
        Mockito.when(mockResourceWatcher.add(Mockito.any(), Mockito.any()))
            .thenThrow(randomBoolean() ? new AccessControlException("access denied in test") : new IOException("file error for testing"));
        final Collection<SslConfiguration> configurations = SSLService.getSSLConfigurations(env).values();
        try {
            new SSLConfigurationReloader(ignore -> {}, mockResourceWatcher, configurations);
        } catch (Exception e) {
            fail("SSLConfigurationReloader threw exception, but is expected to catch and log file access errors instead:" + e);
        }
    }

    private Settings.Builder baseKeystoreSettings(Path tempDir, MockSecureSettings secureSettings) throws IOException {
        final Path keyPath = tempDir.resolve("testclient.pem");
        final Path certPath = tempDir.resolve("testclientcert.crt"); // testclient.crt filename already used in #testPEMTrustReloadException
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem"), keyPath);
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"), certPath);

        if (secureSettings == null) {
            secureSettings = new MockSecureSettings();
        }
        secureSettings.setString("xpack.security.transport.ssl.secure_key_passphrase", "testnode");

        return Settings.builder()
            .put("xpack.security.transport.ssl.key", keyPath.toString())
            .put("xpack.security.transport.ssl.certificate", certPath.toString())
            .setSecureSettings(secureSettings);
    }

    private void validateSSLConfigurationIsReloaded(Environment env, Consumer<SSLContext> preChecks,
                                                    Runnable modificationFunction, Consumer<SSLContext> postChecks) throws Exception {
        final CountDownLatch reloadLatch = new CountDownLatch(1);
        final SSLService sslService = new SSLService(env);
        final SslConfiguration config = sslService.getSSLConfiguration("xpack.security.transport.ssl");
        final Consumer<SslConfiguration> reloadConsumer = sslConfiguration -> {
            try {
                sslService.reloadSSLContext(sslConfiguration);
            } finally {
                reloadLatch.countDown();
            }
        };
        new SSLConfigurationReloader(reloadConsumer, resourceWatcherService, SSLService.getSSLConfigurations(env).values());
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
        final SSLContext sslContext = new SSLContextBuilder()
            .loadKeyMaterial(keyStore, keyStorePass.toCharArray())
            .build();
        MockWebServer server = new MockWebServer(sslContext, false);
        server.enqueue(new MockResponse().setResponseCode(200).setBody("body"));
        server.start();
        return server;
    }

    private static MockWebServer getSslServer(Path keyPath, Path certPath, String password) throws GeneralSecurityException, IOException {
        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        keyStore.load(null, password.toCharArray());
        keyStore.setKeyEntry("testnode_ec", PemUtils.readPrivateKey(keyPath, password::toCharArray), password.toCharArray(),
            PemUtils.readCertificates(Collections.singletonList(certPath)).toArray(Certificate[]::new));
        final SSLContext sslContext = new SSLContextBuilder()
            .loadKeyMaterial(keyStore, password.toCharArray())
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
        final SSLContext sslContext = new SSLContextBuilder()
            .loadTrustMaterial(trustStore, null)
            .build();
        return createHttpClient(sslContext);
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
        for (Certificate cert : PemUtils.readCertificates(trustedCertificatePaths)) {
            trustStore.setCertificateEntry(cert.toString(), cert);
        }
        final SSLContext sslContext = new SSLContextBuilder()
            .loadTrustMaterial(trustStore, null)
            .build();
        return createHttpClient(sslContext);
    }

    private static CloseableHttpClient createHttpClient(SSLContext sslContext) {
        return HttpClients.custom()
            .setConnectionManager(new PoolingHttpClientConnectionManager(
                RegistryBuilder.<ConnectionSocketFactory>create()
                    .register("http", PlainConnectionSocketFactory.getSocketFactory())
                    .register("https", new SSLConnectionSocketFactory(sslContext, null, null, new DefaultHostnameVerifier()))
                    .build(), getHttpClientConnectionFactory(), null, null, -1, TimeUnit.MILLISECONDS))
            .build();
    }

    /**
     * Creates our own HttpConnectionFactory that changes how the connection is closed to prevent issues with
     * the MockWebServer going into an endless loop based on the way that HttpClient closes its connection.
     */
    private static HttpConnectionFactory<HttpRoute, ManagedHttpClientConnection> getHttpClientConnectionFactory() {
        return (route, config) -> {
            ManagedHttpClientConnection delegate = ManagedHttpClientConnectionFactory.INSTANCE.create(route, config);
            return new ManagedHttpClientConnection() {
                @Override
                public String getId() {
                    return delegate.getId();
                }

                @Override
                public void bind(Socket socket) throws IOException {
                    delegate.bind(socket);
                }

                @Override
                public Socket getSocket() {
                    return delegate.getSocket();
                }

                @Override
                public SSLSession getSSLSession() {
                    return delegate.getSSLSession();
                }

                @Override
                public boolean isResponseAvailable(int timeout) throws IOException {
                    return delegate.isResponseAvailable(timeout);
                }

                @Override
                public void sendRequestHeader(HttpRequest request) throws HttpException, IOException {
                    delegate.sendRequestHeader(request);
                }

                @Override
                public void sendRequestEntity(HttpEntityEnclosingRequest request) throws HttpException, IOException {
                    delegate.sendRequestEntity(request);
                }

                @Override
                public HttpResponse receiveResponseHeader() throws HttpException, IOException {
                    return delegate.receiveResponseHeader();
                }

                @Override
                public void receiveResponseEntity(HttpResponse response) throws HttpException, IOException {
                    delegate.receiveResponseEntity(response);
                }

                @Override
                public void flush() throws IOException {
                    delegate.flush();
                }

                @Override
                public InetAddress getLocalAddress() {
                    return delegate.getLocalAddress();
                }

                @Override
                public int getLocalPort() {
                    return delegate.getLocalPort();
                }

                @Override
                public InetAddress getRemoteAddress() {
                    return delegate.getRemoteAddress();
                }

                @Override
                public int getRemotePort() {
                    return delegate.getRemotePort();
                }

                @Override
                public void close() throws IOException {
                    if (delegate.getSocket() instanceof SSLSocket) {
                        try (SSLSocket socket = (SSLSocket) delegate.getSocket()) {
                        }
                    }
                    delegate.close();
                }

                @Override
                public boolean isOpen() {
                    return delegate.isOpen();
                }

                @Override
                public boolean isStale() {
                    return delegate.isStale();
                }

                @Override
                public void setSocketTimeout(int timeout) {
                    delegate.setSocketTimeout(timeout);
                }

                @Override
                public int getSocketTimeout() {
                    return delegate.getSocketTimeout();
                }

                @Override
                public void shutdown() throws IOException {
                    delegate.shutdown();
                }

                @Override
                public HttpConnectionMetrics getMetrics() {
                    return delegate.getMetrics();
                }
            };
        };
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
