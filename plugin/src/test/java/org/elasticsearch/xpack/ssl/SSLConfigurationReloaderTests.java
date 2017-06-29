/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ssl;

import org.apache.lucene.util.SetOnce;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.openssl.jcajce.JcePEMEncryptorBuilder;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.junit.After;
import org.junit.Before;

import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.security.auth.x500.X500Principal;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiConsumer;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.core.Is.is;

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
     * Tests reloading a keystore. The contents of the keystore is used for both keystore and truststore material, so both key
     * config and trust config is checked.
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
        final Environment env = randomBoolean() ? null : new Environment(settings);

        final BiConsumer<X509ExtendedKeyManager, SSLConfiguration> keyManagerPreChecks = (keyManager, config) -> {
            // key manager checks
            String[] aliases = keyManager.getServerAliases("RSA", null);
            assertNotNull(aliases);
            assertThat(aliases.length, is(1));
            assertThat(aliases[0], is("testnode"));
        };

        final SetOnce<Integer> trustedCount = new SetOnce<>();
        final BiConsumer<X509ExtendedTrustManager, SSLConfiguration> trustManagerPreChecks = (trustManager, config) -> {
            // trust manager checks
            Certificate[] certificates = trustManager.getAcceptedIssuers();
            trustedCount.set(certificates.length);
        };

        final Runnable modifier = () -> {
            try {
                // modify it
                KeyStore keyStore = KeyStore.getInstance("jks");
                keyStore.load(null, null);
                final KeyPair keyPair = CertUtils.generateKeyPair(512);
                X509Certificate cert = CertUtils.generateSignedCertificate(new X500Principal("CN=testReloadingKeyStore"), null, keyPair,
                        null, null, 365);
                keyStore.setKeyEntry("key", keyPair.getPrivate(), "testnode".toCharArray(), new X509Certificate[] { cert });
                Path updated = tempDir.resolve("updated.jks");
                try (OutputStream out = Files.newOutputStream(updated)) {
                    keyStore.store(out, "testnode".toCharArray());
                }
                atomicMoveIfPossible(updated, keystorePath);
            } catch (Exception e) {
                throw new RuntimeException("modification failed", e);
            }
        };

        final BiConsumer<X509ExtendedKeyManager, SSLConfiguration> keyManagerPostChecks = (updatedKeyManager, config) -> {
            String[] aliases = updatedKeyManager.getServerAliases("RSA", null);
            assertNotNull(aliases);
            assertThat(aliases.length, is(1));
            assertThat(aliases[0], is("key"));
        };
        final BiConsumer<X509ExtendedTrustManager, SSLConfiguration> trustManagerPostChecks = (updatedTrustManager, config) -> {
            assertThat(trustedCount.get() - updatedTrustManager.getAcceptedIssuers().length, is(4));
        };
        validateSSLConfigurationIsReloaded(settings, env, keyManagerPreChecks, trustManagerPreChecks, modifier, keyManagerPostChecks,
                trustManagerPostChecks);
    }

    /**
     * Tests the reloading of a PEM key config when the key is overwritten. The trust portion is not tested as it is not modified by this
     * test.
     */
    public void testPEMKeyConfigReloading() throws Exception {
        Path tempDir = createTempDir();
        Path keyPath = tempDir.resolve("testnode.pem");
        Path certPath = tempDir.resolve("testnode.crt");
        Path clientCertPath = tempDir.resolve("testclient.crt");
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem"), keyPath);
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"), certPath);
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.crt"), clientCertPath);
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.ssl.secure_key_passphrase", "testnode");
        final Settings settings = Settings.builder()
                .put("path.home", createTempDir())
                .put("xpack.ssl.key", keyPath)
                .put("xpack.ssl.certificate", certPath)
                .putArray("xpack.ssl.certificate_authorities", certPath.toString(), clientCertPath.toString())
                .setSecureSettings(secureSettings)
                .build();
        final Environment env = randomBoolean() ? null :
                new Environment(Settings.builder().put("path.home", createTempDir()).build());

        final SetOnce<PrivateKey> privateKey = new SetOnce<>();
        final BiConsumer<X509ExtendedKeyManager, SSLConfiguration> keyManagerPreChecks = (keyManager, config) -> {
            String[] aliases = keyManager.getServerAliases("RSA", null);
            assertNotNull(aliases);
            assertThat(aliases.length, is(1));
            assertThat(aliases[0], is("key"));
            privateKey.set(keyManager.getPrivateKey("key"));
            assertNotNull(privateKey.get());
        };

        final KeyPair keyPair = CertUtils.generateKeyPair(randomFrom(1024, 2048));
        final Runnable modifier = () -> {
            try {
                // make sure we wait long enough to see a change. if time is within a second the file may not be seen as modified since the
                // size is the same!
                assertTrue(awaitBusy(() -> {
                    try {
                        BasicFileAttributes attributes = Files.readAttributes(keyPath, BasicFileAttributes.class);
                        return System.currentTimeMillis() - attributes.lastModifiedTime().toMillis() >= 1000L;
                    } catch (IOException e) {
                        throw new RuntimeException("io exception while checking time", e);
                    }
                }));
                Path updatedKeyPath = tempDir.resolve("updated.pem");
                try (OutputStream os = Files.newOutputStream(updatedKeyPath);
                     OutputStreamWriter osWriter = new OutputStreamWriter(os, StandardCharsets.UTF_8);
                     JcaPEMWriter writer = new JcaPEMWriter(osWriter)) {
                    writer.writeObject(keyPair,
                            new JcePEMEncryptorBuilder("DES-EDE3-CBC").setProvider(CertUtils.BC_PROV).build("testnode".toCharArray()));
                }
                atomicMoveIfPossible(updatedKeyPath, keyPath);
            } catch (Exception e) {
                throw new RuntimeException("failed to modify file", e);
            }
        };

        final BiConsumer<X509ExtendedKeyManager, SSLConfiguration> keyManagerPostChecks = (keyManager, config) -> {
            String[] aliases = keyManager.getServerAliases("RSA", null);
            assertNotNull(aliases);
            assertThat(aliases.length, is(1));
            assertThat(aliases[0], is("key"));
            assertThat(keyManager.getPrivateKey(aliases[0]), not(equalTo(privateKey)));
            assertThat(keyManager.getPrivateKey(aliases[0]), is(equalTo(keyPair.getPrivate())));
        };
        validateKeyConfigurationIsReloaded(settings, env, keyManagerPreChecks, modifier, keyManagerPostChecks);
    }

    /**
     * Tests the reloading of the trust config when the trust store is modified. The key config is not tested as part of this test.
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
        Environment env = randomBoolean() ? null : new Environment(settings);
        final X500Principal expectedPrincipal = new X500Principal("CN=xpack public development ca");

        final SetOnce<Integer> trustedCount = new SetOnce<>();
        final BiConsumer<X509ExtendedTrustManager, SSLConfiguration> trustManagerPreChecks = (trustManager, config) -> {
            // trust manager checks
            Certificate[] certificates = trustManager.getAcceptedIssuers();
            trustedCount.set(certificates.length);
            assertTrue(Arrays.stream(trustManager.getAcceptedIssuers())
                    .anyMatch((cert) -> expectedPrincipal.equals(cert.getSubjectX500Principal())));
        };


        final Runnable modifier = () -> {
            try {
                Path updatedTruststore = tempDir.resolve("updated.jks");
                KeyStore keyStore = KeyStore.getInstance("jks");
                keyStore.load(null, null);
                try (OutputStream out = Files.newOutputStream(updatedTruststore)) {
                    keyStore.store(out, "testnode".toCharArray());
                }
                atomicMoveIfPossible(updatedTruststore, trustStorePath);
            } catch (Exception e) {
                throw new RuntimeException("failed to modify file", e);
            }
        };

        final BiConsumer<X509ExtendedTrustManager, SSLConfiguration> trustManagerPostChecks = (updatedTrustManager, config) -> {
            assertThat(trustedCount.get() - updatedTrustManager.getAcceptedIssuers().length, is(5));
            assertTrue(Arrays.stream(updatedTrustManager.getAcceptedIssuers())
                    .anyMatch((cert) -> expectedPrincipal.equals(cert.getSubjectX500Principal())));
        };

        validateTrustConfigurationIsReloaded(settings, env, trustManagerPreChecks, modifier, trustManagerPostChecks);
    }

    /**
     * Test the reloading of a trust config that is backed by PEM certificate files. The key config is not tested as we only care about the
     * trust config in this test.
     */
    public void testReloadingPEMTrustConfig() throws Exception {
        Path tempDir = createTempDir();
        Path clientCertPath = tempDir.resolve("testclient.crt");
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.crt"), clientCertPath);
        Settings settings = Settings.builder()
                .putArray("xpack.ssl.certificate_authorities", clientCertPath.toString())
                .put("path.home", createTempDir())
                .build();
        Environment env = randomBoolean() ? null : new Environment(settings);
        final X500Principal expectedPrincipal = new X500Principal("CN=xpack public development ca");

        final BiConsumer<X509ExtendedTrustManager, SSLConfiguration> trustManagerPreChecks = (trustManager, config) -> {
            // trust manager checks
            Certificate[] certificates = trustManager.getAcceptedIssuers();
            assertThat(certificates.length, is(2));
            assertThat(((X509Certificate)certificates[0]).getSubjectX500Principal().getName(), containsString("Test Client"));
            assertTrue(Arrays.stream(trustManager.getAcceptedIssuers())
                    .anyMatch((cert) -> expectedPrincipal.equals(cert.getSubjectX500Principal())));
        };

        final Runnable modifier = () -> {
            try {
                Path updatedCert = tempDir.resolve("updated.crt");
                Files.copy(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"), updatedCert,
                        StandardCopyOption.REPLACE_EXISTING);
                atomicMoveIfPossible(updatedCert, clientCertPath);
            } catch (Exception e) {
                throw new RuntimeException("failed to modify file", e);
            }
        };

        final BiConsumer<X509ExtendedTrustManager, SSLConfiguration> trustManagerPostChecks = (updatedTrustManager, config) -> {
            Certificate[] updatedCerts = updatedTrustManager.getAcceptedIssuers();
            assertThat(updatedCerts.length, is(2));
            assertThat(((X509Certificate)updatedCerts[0]).getSubjectX500Principal().getName(), containsString("Test Node"));
            assertTrue(Arrays.stream(updatedTrustManager.getAcceptedIssuers())
                    .anyMatch((cert) -> expectedPrincipal.equals(cert.getSubjectX500Principal())));
        };

        validateTrustConfigurationIsReloaded(settings, env, trustManagerPreChecks, modifier, trustManagerPostChecks);
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
        Environment env = randomBoolean() ? null : new Environment(settings);
        final SSLService sslService = new SSLService(settings, env);
        final SSLConfiguration config = sslService.sslConfiguration(Settings.EMPTY);
        new SSLConfigurationReloader(settings, env, sslService, resourceWatcherService) {
            @Override
            void reloadSSLContext(SSLConfiguration configuration) {
                fail("reload should not be called! [keystore reload exception]");
            }
        };

        // key manager checks
        final X509ExtendedKeyManager keyManager = sslService.sslContextHolder(config).keyManager().getKeyManager();

        // truncate the keystore
        try (OutputStream out = Files.newOutputStream(keystorePath, StandardOpenOption.TRUNCATE_EXISTING)) {
        }

        // we intentionally don't wait here as we rely on concurrency to catch a failure
        assertThat(sslService.sslContextHolder(config).keyManager().getKeyManager(), sameInstance(keyManager));
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
                .putArray("xpack.ssl.certificate_authorities", certPath.toString(), clientCertPath.toString())
                .put("path.home", createTempDir())
                .setSecureSettings(secureSettings)
                .build();
        Environment env = randomBoolean() ? null : new Environment(settings);
        final SSLService sslService = new SSLService(settings, env);
        final SSLConfiguration config = sslService.sslConfiguration(Settings.EMPTY);
        new SSLConfigurationReloader(settings, env, sslService, resourceWatcherService) {
            @Override
            void reloadSSLContext(SSLConfiguration configuration) {
                fail("reload should not be called! [pem key reload exception]");
            }
        };

        final X509ExtendedKeyManager keyManager = sslService.sslContextHolder(config).keyManager().getKeyManager();

        // truncate the file
        try (OutputStream os = Files.newOutputStream(keyPath, StandardOpenOption.TRUNCATE_EXISTING)) {
        }

        // we intentionally don't wait here as we rely on concurrency to catch a failure
        assertThat(sslService.sslContextHolder(config).keyManager().getKeyManager(), sameInstance(keyManager));
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
        Environment env = randomBoolean() ? null : new Environment(settings);
        final SSLService sslService = new SSLService(settings, env);
        final SSLConfiguration config = sslService.sslConfiguration(Settings.EMPTY);
        new SSLConfigurationReloader(settings, env, sslService, resourceWatcherService) {
            @Override
            void reloadSSLContext(SSLConfiguration configuration) {
                fail("reload should not be called! [truststore reload exception]");
            }
        };

        final X509ExtendedTrustManager trustManager = sslService.sslContextHolder(config).trustManager().getTrustManager();

        // truncate the truststore
        try (OutputStream os = Files.newOutputStream(trustStorePath, StandardOpenOption.TRUNCATE_EXISTING)) {
        }

        // we intentionally don't wait here as we rely on concurrency to catch a failure
        assertThat(sslService.sslContextHolder(config).trustManager().getTrustManager(), sameInstance(trustManager));
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
                .putArray("xpack.ssl.certificate_authorities", clientCertPath.toString())
                .put("path.home", createTempDir())
                .build();
        Environment env = randomBoolean() ? null : new Environment(settings);
        final SSLService sslService = new SSLService(settings, env);
        final SSLConfiguration config = sslService.sslConfiguration(Settings.EMPTY);
        new SSLConfigurationReloader(settings, env, sslService, resourceWatcherService) {
            @Override
            void reloadSSLContext(SSLConfiguration configuration) {
                fail("reload should not be called! [pem trust reload exception]");
            }
        };

        final X509ExtendedTrustManager trustManager = sslService.sslContextHolder(config).trustManager().getTrustManager();

        // write bad file
        Path updatedCert = tempDir.resolve("updated.crt");
        try (OutputStream os = Files.newOutputStream(updatedCert)) {
            os.write(randomByte());
        }
        atomicMoveIfPossible(updatedCert, clientCertPath);

        // we intentionally don't wait here as we rely on concurrency to catch a failure
        assertThat(sslService.sslContextHolder(config).trustManager().getTrustManager(), sameInstance(trustManager));
    }

    /**
     * Validates the trust configuration aspect of the SSLConfiguration is reloaded
     */
    private void validateTrustConfigurationIsReloaded(Settings settings, Environment env,
                                                      BiConsumer<X509ExtendedTrustManager, SSLConfiguration> trustManagerPreChecks,
                                                      Runnable modificationFunction,
                                                      BiConsumer<X509ExtendedTrustManager, SSLConfiguration> trustManagerPostChecks)
                                                      throws Exception {
        validateSSLConfigurationIsReloaded(settings, env, false, true, null, trustManagerPreChecks, modificationFunction, null,
                trustManagerPostChecks);
    }

    /**
     * Validates the trust configuration aspect of the SSLConfiguration is reloaded
     */
    private void validateKeyConfigurationIsReloaded(Settings settings, Environment env,
                                                    BiConsumer<X509ExtendedKeyManager, SSLConfiguration> keyManagerPreChecks,
                                                    Runnable modificationFunction,
                                                    BiConsumer<X509ExtendedKeyManager, SSLConfiguration> keyManagerPostChecks)
                                                    throws Exception {
        validateSSLConfigurationIsReloaded(settings, env, true, false, keyManagerPreChecks, null, modificationFunction,
                keyManagerPostChecks, null);
    }

    /**
     * Validates that both the key and trust configuration aspects of the SSLConfiguration are reloaded
     */
    private void validateSSLConfigurationIsReloaded(Settings settings, Environment env,
                                                    BiConsumer<X509ExtendedKeyManager, SSLConfiguration> keyManagerPreChecks,
                                                    BiConsumer<X509ExtendedTrustManager, SSLConfiguration> trustManagerPreChecks,
                                                    Runnable modificationFunction,
                                                    BiConsumer<X509ExtendedKeyManager, SSLConfiguration> keyManagerPostChecks,
                                                    BiConsumer<X509ExtendedTrustManager, SSLConfiguration> trustManagerPostChecks)
                                                    throws Exception {
        validateSSLConfigurationIsReloaded(settings, env, true, true, keyManagerPreChecks, trustManagerPreChecks, modificationFunction,
                keyManagerPostChecks, trustManagerPostChecks);
    }

    private void validateSSLConfigurationIsReloaded(Settings settings, Environment env, boolean checkKeys, boolean checkTrust,
                                                    BiConsumer<X509ExtendedKeyManager, SSLConfiguration> keyManagerPreChecks,
                                                    BiConsumer<X509ExtendedTrustManager, SSLConfiguration> trustManagerPreChecks,
                                                    Runnable modificationFunction,
                                                    BiConsumer<X509ExtendedKeyManager, SSLConfiguration> keyManagerPostChecks,
                                                    BiConsumer<X509ExtendedTrustManager, SSLConfiguration> trustManagerPostChecks)
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

        final X509ExtendedKeyManager keyManager;
        if (checkKeys) {
            keyManager = sslService.sslContextHolder(config).keyManager().getKeyManager();
        } else {
            keyManager = null;
        }

        final X509ExtendedTrustManager trustManager;
        if (checkTrust) {
            trustManager = sslService.sslContextHolder(config).trustManager().getTrustManager();
        } else {
            trustManager = null;
        }

        // key manager checks
        if (checkKeys) {
            keyManagerPreChecks.accept(keyManager, config);
        }

        // trust manager checks
        if (checkTrust) {
            trustManagerPreChecks.accept(trustManager, config);
        }

        assertEquals("nothing should have called reload", 1, reloadLatch.getCount());

        // modify
        modificationFunction.run();
        reloadLatch.await();

        // check key manager
        if (checkKeys) {
            final X509ExtendedKeyManager updatedKeyManager = sslService.sslContextHolder(config).keyManager().getKeyManager();
            assertThat(updatedKeyManager, not(sameInstance(keyManager)));
            keyManagerPostChecks.accept(updatedKeyManager, config);
        }

        // check trust manager
        if (checkTrust) {
            final X509ExtendedTrustManager updatedTrustManager = sslService.sslContextHolder(config).trustManager().getTrustManager();
            assertThat(updatedTrustManager, not(sameInstance(trustManager)));
            trustManagerPostChecks.accept(updatedTrustManager, config);
        }
    }

    private static void atomicMoveIfPossible(Path source, Path target) throws IOException {
        try {
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }
}
