/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ssl;

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

public class SSLReloadDuringStartupIntegTests extends SecurityIntegTestCase {

    @BeforeClass
    public static void skipInFips() {
        assumeFalse("Can't use JKS keystores in FIPS JVM", inFipsJvm());
    }

    @Override
    public Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings settings = super.nodeSettings(nodeOrdinal, otherSettings);
        Environment tmpEnv = TestEnvironment.newEnvironment(settings);
        // each node gets its own keystore under its home/config dir
        Path origKeystorePath = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks");
        Path nodeSpecificPath = tmpEnv.configFile().resolve("testnode.jks");
        try {
            Files.copy(origKeystorePath, nodeSpecificPath, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        Settings.Builder builder = Settings.builder()
            .put(settings.filter((s) -> s.startsWith("xpack.security.transport.ssl.") == false), false);
        MockSecureSettings secureSettings = new MockSecureSettings();
        builder.setSecureSettings(secureSettings);
        builder.put("xpack.security.transport.ssl.keystore.path", nodeSpecificPath)
            .put("resource.reload.interval.high", "1s")
            .put("xpack.security.transport.ssl.enabled", true);
        secureSettings.setString("xpack.security.transport.ssl.keystore.secure_password", "testnode");
        return builder.build();
    }

    @Override
    protected boolean transportSSLEnabled() {
        return true;
    }

    public void testReloadDuringStartup() throws Exception {
        final String node = randomFrom(internalCluster().getNodeNames());
        final Environment env = internalCluster().getInstance(Environment.class, node);
        // this latch is to synchronize the start of the thread that updates the TLS config
        // and the continuation of the node restart
        final CountDownLatch latch = new CountDownLatch(2);
        // this latch is used by the test to signal when the updated TLS configuration has been
        // written so that the test should proceed with checks to ensure the node rejoins the cluster
        final CountDownLatch writtenLatch = new CountDownLatch(1);

        // restart the node
        internalCluster().restartNode(node, new RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                Path origKeystorePath = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks");
                Path keystorePath = env.configFile().resolve("testnode.jks");
                Path updatedKeystorePath = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode_updated.jks");
                assertTrue(Files.exists(keystorePath));
                // replace the keystore with one that will fail during handshaking
                copyAndAtomicMoveIfPossible(updatedKeystorePath, keystorePath);
                new Thread(() -> {
                    latch.countDown();
                    try {
                        latch.await();
                        Thread.sleep(randomLongBetween(1L, 2000L));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }

                    // replace the bad keystore with a new copy of the good one
                    try {
                        copyAndAtomicMoveIfPossible(origKeystorePath, keystorePath);
                        writtenLatch.countDown();
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }).start();

                latch.countDown();
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return super.onNodeStopped(nodeName);
            }
        });
        writtenLatch.await();
        ensureClusterSizeConsistency();
        ensureFullyConnectedCluster();
    }

    private void copyAndAtomicMoveIfPossible(Path source, Path target) throws IOException {
        Path tmp = createTempFile();
        Files.copy(source, tmp, StandardCopyOption.REPLACE_EXISTING);
        try {
            Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }
}
