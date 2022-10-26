/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ssl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.InternalTestCluster.RestartCallback;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.CountDownLatch;

/**
 * Start a 3 node cluster, with one node having a bad Transport keystore setting.
 * Fix the Transport keystore setting, and verify the node successfully reloads it.
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
     * @param nodeOrdinal Number of nodes in the cluster.
     * @param otherSettings Other settings injected into this test.
     * @return Node settings with overridden Transport keystore file path.
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
     * Update Transport keystore while node is starting, and verify the node reloads it successfully.
     * This is implemented by starting a cluster, stopping a random node, using a bad keystore, and starting it again.
     * While the node is starting, a Thread will replace the keystore with the original working one.
     * The test will verify that the node recovers and rejoins the cluster, and that the cluster is healthy.
     * @throws Exception Unexpected error during node startup.
     */
//    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/77490")
    public void testReloadDuringStartup() throws Exception {
        final String nodeName = randomFrom(internalCluster().getNodeNames());
        final Environment env = internalCluster().getInstance(Environment.class, nodeName);
        final CountDownLatch beforeKeystoreFix = new CountDownLatch(2); // Begin Node startup and Keystore Fix at the same time
        final CountDownLatch afterKeystoreFix = new CountDownLatch(1); // Begin cluster verify after Keystore Fix is complete
        final Path nodeKeystorePath = env.configFile().resolve("testnode.jks"); // all nodes have this initial keystore
        final Path badKeystorePath = getDataPath(badKeyStoreFilePath); // apply after node stop, before start
        final Path goodKeystorePath = getDataPath(goodKeyStoreFilePath); // apply after node restart begins
        assertTrue(Files.exists(nodeKeystorePath));
        LOGGER.info("Stopping node [{}] in cluster {}...", nodeName, internalCluster().getNodeNames());
        final long stopNanos = System.nanoTime();
        internalCluster().restartNode(nodeName, new RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                LOGGER.info("Node [{}] stopped in {}ms.", nodeName, (System.nanoTime() - stopNanos)/1000000F);
                atomicCopyIfPossible(badKeystorePath, nodeKeystorePath);
                final Thread fixKeystoreThread = new Thread(() -> {
                    waitUntilNodeStartupIsReadyToBegin(beforeKeystoreFix); // SYNCHRONIZED TASK
                    try {
                        atomicCopyIfPossible(goodKeystorePath, nodeKeystorePath);
                        afterKeystoreFix.countDown(); // Indicate keystore is fixed
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
                fixKeystoreThread.setName("Fix Keystore");
                fixKeystoreThread.start();
                waitUntilFixKeystoreIsReadyToBegin(beforeKeystoreFix); // SYNCHRONIZED TASK
                return super.onNodeStopped(nodeName); // Return control to RestartCallback to do startup next
            }
        });
        final long awaitNanos = System.nanoTime(); // actual await nanos
        try {
            afterKeystoreFix.await(); // Wait until keystore is fixed
        } finally {
            LOGGER.info("Awaited {}ms. Verifying the cluster...", (System.nanoTime() - awaitNanos)/1000000F);
        }
        final long ensureClusterSizeConsistencyNanos = System.nanoTime(); // actual verify nanos
        try {
            ensureClusterSizeConsistency();
        } finally {
            LOGGER.info("Ensure cluster size consistency took {}ms.", (System.nanoTime() - ensureClusterSizeConsistencyNanos)/1000000F);
        }
        final long ensureFullyConnectedClusterNanos = System.nanoTime(); // actual verify nanos
        try {
            ensureFullyConnectedCluster();
        } finally {
            LOGGER.info("Ensure fully connected cluster took {}ms.", (System.nanoTime() - ensureFullyConnectedClusterNanos)/1000000F);
        }
    }

    private void waitUntilNodeStartupIsReadyToBegin(final CountDownLatch beforeKeystoreFix) {
        beforeKeystoreFix.countDown(); // Fixer thread ready
        try {
            final long sleepMillis = randomLongBetween(200L, 600L); // intended sleepMillis
            final long awaitNanos = System.nanoTime(); // actual await nanos
            try {
                beforeKeystoreFix.await(); // Wait for Main thread to be ready for concurrent start
            } finally {
                LOGGER.info("Awaited {}ms. Sleeping {}ms before fixing...", (System.nanoTime() - awaitNanos)/1000000F, sleepMillis);
            }
            long sleepNanos = System.nanoTime(); // actual sleep nanos
            try {
                Thread.sleep(sleepMillis); // Note: nodeSettings() => `resource.reload.interval.high: 1s`
            } finally {
                LOGGER.info("Slept {}ms, intended {}ms. Fixing can start now...",(System.nanoTime() - sleepNanos)/1000000F, sleepMillis);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void waitUntilFixKeystoreIsReadyToBegin(final CountDownLatch beforeKeystoreFix) {
        beforeKeystoreFix.countDown(); // Main thread ready
        long awaitNanos = -1; // actual await nanos
        try {
            awaitNanos = System.nanoTime();
            try {
                beforeKeystoreFix.await(); // Wait for Fixer thread to be ready for concurrent start
            } finally {
                awaitNanos = System.nanoTime() - awaitNanos;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        LOGGER.info("Awaited {}ms. Node can start now...", awaitNanos/1000000F);
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
//        LOGGER.info("Temporary file [{}] created", tmp);
        Files.copy(source, tmp, StandardCopyOption.REPLACE_EXISTING);
//        LOGGER.info("Copy succeeded from [{}] to [{}]", source, tmp);
        try {
            Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            LOGGER.info("Atomic move succeeded from [{}] to [{}]", tmp, target);
        } catch (AtomicMoveNotSupportedException e) {
            LOGGER.warn("Atomic move failed from [{}] to [{}]", tmp, target, e);
            Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
            LOGGER.info("Non-atomic move succeeded from [{}] to [{}]", tmp, target);
        }
    }
}
