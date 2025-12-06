/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ssl;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.util.Strings;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.InternalTestCluster.RestartCallback;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.CountDownLatch;

/**
 * Start a cluster. Restart a node with a bad Transport keystore so it can't rejoin.
 * Update the Transport keystore, so ES can reload it at runtime. Verify reload was OK
 * by verifying if the cluster goes back to normal.
 */
public class SSLReloadDuringStartupIntegTests extends SecurityIntegTestCase {

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
        Path nodeKeystorePath = tmpEnv.configDir().resolve("testnode.jks");
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
    @TestLogging(value = "org.elasticsearch.xpack.ssl.SSLReloadDuringStartupIntegTests:TRACE", reason = "See Keystore update vs ES restart")
    public void testReloadDuringStartup() throws Exception {
        final String[] nodeNames = internalCluster().getNodeNames();
        final String nodeName = randomFrom(nodeNames);
        final Environment env = internalCluster().getInstance(Environment.class, nodeName);
        final CountDownLatch beforeKeystoreFix = new CountDownLatch(2); // SYNC: Cert update & ES restart
        final CountDownLatch afterKeystoreFix = new CountDownLatch(1); // SYNC: Verify cluster after cert update
        final Path nodeKeystorePath = env.configDir().resolve("testnode.jks"); // all nodes have good keystore
        final Path badKeystorePath = getDataPath(badKeyStoreFilePath); // stop a node, and apply this bad keystore
        final Path goodKeystorePath = getDataPath(goodKeyStoreFilePath); // start the node, and apply this good keystore
        assertTrue(Files.exists(nodeKeystorePath));
        logger.trace("Stopping node [{}] in {}-node cluster {}...", nodeName, nodeNames.length, nodeNames);
        final long stopNanos = System.nanoTime();
        internalCluster().restartNode(nodeName, new RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                logger.debug("Node [{}] stopped in {}ms.", nodeName, TimeValue.timeValueNanos(System.nanoTime() - stopNanos).millisFrac());
                atomicCopyIfPossible(badKeystorePath, nodeKeystorePath);
                final Thread fixKeystoreThread = new Thread(() -> {
                    waitUntilNodeStartupIsReadyToBegin(beforeKeystoreFix); // SYNC: Cert update & ES restart
                    try {
                        atomicCopyIfPossible(goodKeystorePath, nodeKeystorePath);
                        logger.trace("Waiting for ES restart...");
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
        logger.trace("Waiting for keystore fix...");
        timed(Level.DEBUG, "Awaited {}ms. Verifying the cluster...", () -> {
            try {
                afterKeystoreFix.await(); // SYNC: Verify cluster after cert update
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        timed(Level.TRACE, "Ensure cluster size consistency took {}ms.", this::ensureClusterSizeConsistency);
        timed(Level.TRACE, "Ensure fully connected cluster took {}ms.", this::ensureFullyConnectedCluster);
    }

    private void waitUntilNodeStartupIsReadyToBegin(final CountDownLatch beforeKeystoreFix) {
        logger.trace("Waiting for ES start to begin...");
        beforeKeystoreFix.countDown(); // SYNC: Cert update & ES restart
        final long sleepMillis = randomLongBetween(1L, 2000L); // intended sleepMillis
        timed(Level.DEBUG, "Awaited {}ms. Sleeping " + sleepMillis + "ms before fixing...", () -> {
            try {
                beforeKeystoreFix.await(); // SYNC: Cert update & ES restart
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        timed(Level.DEBUG, "Slept {}ms, intended " + sleepMillis + "ms. Fixing can start now...", () -> {
            try {
                Thread.sleep(sleepMillis); // Simulate cert update delay relative to ES start
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    private void waitUntilFixKeystoreIsReadyToBegin(final CountDownLatch beforeKeystoreFix) {
        logger.trace("Waiting for keystore fix to begin...");
        beforeKeystoreFix.countDown(); // SYNC: Cert update & ES restart
        timed(Level.DEBUG, "Awaited {}ms. Node can start now...", () -> {
            try {
                beforeKeystoreFix.await(); // SYNC: Cert update & ES restart
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    private void timed(final Level level, final String message, final Runnable runnable) {
        assert level != null;
        assert Strings.isEmpty(message) == false;
        assert message.contains("{}ms") : "Message must contain {}ms";
        assert message.replace("{}", "").contains("{}") == false : "Message can only contain one binding.";
        assert runnable != null;
        final long startNanos = System.nanoTime();
        try {
            runnable.run();
        } finally {
            logger.log(level, message, TimeValue.timeValueNanos(System.nanoTime() - startNanos).millisFrac());
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
        logger.trace("Copying [{}] to [{}]", source, target);
        Path tmp = createTempFile();
        logger.trace("Created temporary file [{}]", tmp);
        Files.copy(source, tmp, StandardCopyOption.REPLACE_EXISTING);
        try {
            Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            logger.debug("Atomic move succeeded from [{}] to [{}]", tmp, target);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
            logger.debug("Non-atomic move succeeded from [{}] to [{}]", tmp, target);
        }
    }
}
