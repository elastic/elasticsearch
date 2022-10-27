/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ssl;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.InternalTestCluster.RestartCallback;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.junit.BeforeClass;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.CountDownLatch;

/**
 * Start a cluster. Restart a node with a bad Transport keystore to it can't rejoin.
 * Update the Transport keystore, so ES can reload it at runtime. Verify reload was OK
 * by verifying if the cluster goes back to normal.
 */
public class SSLReloadDuringStartupIntegTests extends SecurityIntegTestCase {
    private final Logger LOGGER = LogManager.getLogger(SSLReloadDuringStartupIntegTests.class);

    private final String goodKeyStoreFilePath = "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks";
    private final String badKeyStoreFilePath = "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode_updated.jks";

    @BeforeClass
    public static void skipInFips() {
        assumeFalse("Can't use JKS keystores in FIPS JVM", inFipsJvm());
    }

    /**
     * Called for each node. Copy the original testnode.jks file into each node's config directory.
     *
     * @param nodeOrdinal   Number of the node in the cluster.
     * @param otherSettings Pass through settings for this test.
     * @return Node settings with overrides for Transport SSL, so the test can update the Transport keystore file twice.
     */
    @Override
    public Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings settings = super.nodeSettings(nodeOrdinal, otherSettings);
        Environment tmpEnv = TestEnvironment.newEnvironment(settings);

        // For each node, copy the original testnode.jks into each node's config directory.
        Path nodeKeystorePath = tmpEnv.configFile().resolve("testnode.jks");
        try {
            Path goodKeystorePath = getDataPath(goodKeyStoreFilePath);
            Files.copy(goodKeystorePath, nodeKeystorePath, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        Settings.Builder builder = Settings.builder()
            .put(settings.filter((s) -> s.startsWith("xpack.security.transport.ssl.") == false), false);
        MockSecureSettings secureSettings = new MockSecureSettings();
        builder.setSecureSettings(secureSettings);
        builder.put("xpack.security.transport.ssl.keystore.path", nodeKeystorePath)
            .put("resource.reload.interval.high", "1s")
            .put("xpack.security.transport.ssl.enabled", true);
        secureSettings.setString("xpack.security.transport.ssl.keystore.secure_password", "testnode");
        return builder.build();
    }

    @Override
    protected boolean transportSSLEnabled() {
        return true;
    }

    /**
     * This class start a cluster. For this test, restart a random node.
     * While stopped, replace the Transport keystore with a bad one, so the node cannot rejoin the cluster.
     * While restarting, replace the keystore with a good one, and verify if ES reloaded it by checking it if rejoined the cluster.
     *
     * @throws Exception Compare ES startup logs to diagnostic and timing logs for the test, to narrow down why ES startup failed.
     */
    // @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/77490")
    // Unmuted by https://github.com/elastic/elasticsearch/pull/91147
    // Related to https://github.com/elastic/elasticsearch/pull/91166
    public void testReloadDuringStartup() throws Exception {
        final String nodeName = randomFrom(internalCluster().getNodeNames());
        final Environment env = internalCluster().getInstance(Environment.class, nodeName);
        final CountDownLatch beforeKeystoreFix = new CountDownLatch(2); // SYNC: Cert update & ES restart
        final CountDownLatch afterKeystoreFix = new CountDownLatch(1); // SYNC: Verify cluster after cert update
        final Path nodeKeystorePath = env.configFile().resolve("testnode.jks"); // all nodes have good keystore
        final Path badKeystorePath = getDataPath(badKeyStoreFilePath); // stop a node, and apply this bad keystore
        final Path goodKeystorePath = getDataPath(goodKeyStoreFilePath); // start the node, and apply this good keystore
        assertTrue(Files.exists(nodeKeystorePath));
        LOGGER.info("Stopping node [{}] in cluster {}...", nodeName, internalCluster().getNodeNames());
        final long stopNanos = System.nanoTime();
        internalCluster().restartNode(nodeName, new RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                LOGGER.info("Node [{}] stopped in {}ms.", nodeName, TimeValue.timeValueNanos(System.nanoTime() - stopNanos).millisFrac());
                atomicCopyIfPossible(badKeystorePath, nodeKeystorePath);
                final Thread fixKeystoreThread = new Thread(() -> {
                    waitUntilNodeStartupIsReadyToBegin(beforeKeystoreFix); // SYNC: Cert update & ES restart
                    try {
                        atomicCopyIfPossible(goodKeystorePath, nodeKeystorePath);
                        afterKeystoreFix.countDown(); // SYNC: Cert update & ES restart
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
                fixKeystoreThread.setName("Fix Keystore");
                fixKeystoreThread.start();
                waitUntilFixKeystoreIsReadyToBegin(beforeKeystoreFix); // SYNC: Cert update & ES restart
                return super.onNodeStopped(nodeName); // ASSUME: RestartCallback will do ES start next
            }
        });
        try (Timed t = new Timed(LOGGER, Level.INFO, "Awaited {}ms. Verifying the cluster...")) {
            afterKeystoreFix.await(); // SYNC: Verify cluster after cert update
        }
        try (Timed t = new Timed(LOGGER, Level.INFO, "Ensure cluster size consistency took {}ms.")) {
            ensureClusterSizeConsistency();
        }
        try (Timed t = new Timed(LOGGER, Level.INFO, "Ensure fully connected cluster took {}ms.")) {
            ensureFullyConnectedCluster();
        }
    }

    private void waitUntilNodeStartupIsReadyToBegin(final CountDownLatch beforeKeystoreFix) {
        beforeKeystoreFix.countDown(); // SYNC: Cert update & ES restart
        try {
            final long sleepMillis = randomLongBetween(1L, 2000L); // intended sleepMillis
            try (Timed t = new Timed(LOGGER, Level.INFO, "Awaited {}ms. Sleeping " + sleepMillis + "ms before fixing...")) {
                beforeKeystoreFix.await(); // SYNC: Cert update & ES restart
            }
            try (Timed t = new Timed(LOGGER, Level.INFO, "Slept {}ms, intended " + sleepMillis + "ms. Fixing can start now...")) {
                Thread.sleep(sleepMillis); // Simulate cert update delay relative to ES start
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void waitUntilFixKeystoreIsReadyToBegin(final CountDownLatch beforeKeystoreFix) {
        beforeKeystoreFix.countDown(); // SYNC: Cert update & ES restart
        try (Timed t = new Timed(LOGGER, Level.INFO, "Awaited {}ms. Node can start now...")) {
            beforeKeystoreFix.await(); // SYNC: Cert update & ES restart
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Copy a source file to a destination file, overwriting if necessary.
     * Use an intermediate temporary file, to attempt an atomic move.
     * If atomic move fails, fall back to non-atomic move.
     * @param source File to be copied.
     * @param target File to be created or overwritten.
     * @throws IOException Cannot create temp file, or copy source file to temp file, or non-atomic move temp file to target file.
     */
    private void atomicCopyIfPossible(Path source, Path target) throws IOException {
        LOGGER.info("Copying [{}] to [{}]", source, target);
        Path tmp = createTempFile();
        // LOGGER.info("Temporary file [{}] created", tmp);
        Files.copy(source, tmp, StandardCopyOption.REPLACE_EXISTING);
        // LOGGER.info("Copy succeeded from [{}] to [{}]", source, tmp);
        try {
            Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            LOGGER.info("Atomic move succeeded from [{}] to [{}]", tmp, target);
        } catch (AtomicMoveNotSupportedException e) {
            LOGGER.warn("Atomic move failed from [{}] to [{}]", tmp, target, e);
            Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
            LOGGER.info("Non-atomic move succeeded from [{}] to [{}]", tmp, target);
        }
    }

    public static class Timed implements Closeable {
        private final long n = System.nanoTime();
        private final Logger log;
        private final Level lvl;
        private final String msg;

        Timed(final Logger log, final Level lvl, final String msg) {
            this.log = log;
            this.lvl = lvl;
            this.msg = msg;
        }

        @Override
        public void close() {
            this.log.log(this.lvl, this.msg, TimeValue.timeValueNanos(System.nanoTime() - this.n).millisFrac());
        }
    }
}
