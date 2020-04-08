/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.xpack.ssl;

import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.InternalTestCluster.RestartCallback;
import org.elasticsearch.test.SecurityIntegTestCase;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.CountDownLatch;

public class SSLReloadDuringStartupIntegTests extends SecurityIntegTestCase {

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        Settings settings = super.nodeSettings(nodeOrdinal);
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
        final CountDownLatch latch = new CountDownLatch(2);
        final CountDownLatch writtenLatch = new CountDownLatch(1);

        // restart the node
        internalCluster().restartNode(node, new RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                Path origKeystorePath = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks");
                Path keystorePath = env.configFile().resolve("testnode.jks");
                Path updatedKeystorePath = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode_updated.jks");
                assertTrue(Files.exists(keystorePath));
                // replace the keystore
                copyAndAtomicMoveIfPossible(updatedKeystorePath, keystorePath);
                new Thread(() -> {
                    latch.countDown();
                    try {
                        latch.await();
                        Thread.sleep(randomLongBetween(1L, 2000L));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }

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
