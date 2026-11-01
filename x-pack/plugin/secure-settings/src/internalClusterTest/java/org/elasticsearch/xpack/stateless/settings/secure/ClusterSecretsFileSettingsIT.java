/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.settings.secure;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.ReservedStateHandlerMetadata;
import org.elasticsearch.cluster.metadata.ReservedStateMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSecrets;
import org.elasticsearch.core.Strings;
import org.elasticsearch.reservedstate.service.FileSettingsService;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.GeneralSecurityException;
import java.util.Base64;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.test.NodeRoles.dataOnlyNode;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
@LuceneTestCase.SuppressFileSystems("*")
public class ClusterSecretsFileSettingsIT extends ESIntegTestCase {

    private final AtomicLong versionCounter = new AtomicLong(1);

    private static String secretsJSON(long version, String stringValue, String base64FileValue) {
        return Strings.format("""
            {
                "metadata": {
                    "version": "%s",
                    "compatibility": "8.4.0"
                },
                "state": {
                    "cluster_secrets": {
                        "string_secrets": {"secure.str": "%s"},
                        "file_secrets": {"secure.file": "%s"}
                    }
                }
            }""", version, stringValue, base64FileValue);
    }

    private static String emptyStateJSON(long version) {
        return Strings.format("""
            {
                "metadata": {
                    "version": "%s",
                    "compatibility": "8.4.0"
                },
                "state": {}
            }""", version);
    }

    public void testClusterSecretsApplied() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        internalCluster().startNode(dataOnlyNode());
        final String masterNode = internalCluster().startMasterOnlyNode();

        FileSettingsService fss = internalCluster().getInstance(FileSettingsService.class, masterNode);
        assertBusy(() -> assertTrue(fss.watching()));

        String strValue = randomAlphaOfLength(12);
        byte[] fileValue = randomByteArrayOfLength(16);
        String fileValueB64 = Base64.getEncoder().encodeToString(fileValue);

        long version = versionCounter.incrementAndGet();
        CountDownLatch latch = clusterSecretsLatch(masterNode, version);
        writeJSONFile(masterNode, secretsJSON(version, strValue, fileValueB64));
        assertTrue(latch.await(20, TimeUnit.SECONDS));

        assertClusterSecrets(strValue, fileValue);
        assertClusterSecretsHandlerKeys("secure.str", "secure.file");
    }

    public void testClusterSecretsUpdate() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        final String masterNode = internalCluster().startMasterOnlyNode();

        FileSettingsService fss = internalCluster().getInstance(FileSettingsService.class, masterNode);
        assertBusy(() -> assertTrue(fss.watching()));

        // Apply initial secrets
        String str1 = randomAlphaOfLength(10);
        byte[] file1 = randomByteArrayOfLength(10);
        long v1 = versionCounter.incrementAndGet();
        CountDownLatch latch1 = clusterSecretsLatch(masterNode, v1);
        writeJSONFile(masterNode, secretsJSON(v1, str1, Base64.getEncoder().encodeToString(file1)));
        assertTrue(latch1.await(20, TimeUnit.SECONDS));

        assertClusterSecrets(str1, file1);
        ClusterSecrets firstSecrets = currentClusterSecrets();
        assertThat(firstSecrets.getVersion(), equalTo(1L));

        // Update with different secrets
        String str2 = randomAlphaOfLength(10);
        byte[] file2 = randomByteArrayOfLength(10);
        long v2 = versionCounter.incrementAndGet();
        CountDownLatch latch2 = clusterSecretsLatch(masterNode, v2);
        writeJSONFile(masterNode, Strings.format("""
            {
                "metadata": {
                    "version": "%s",
                    "compatibility": "8.4.0"
                },
                "state": {
                    "cluster_secrets": {
                        "string_secrets": {"secure.new_str": "%s"},
                        "file_secrets": {"secure.new_file": "%s"}
                    }
                }
            }""", v2, str2, Base64.getEncoder().encodeToString(file2)));
        assertTrue(latch2.await(20, TimeUnit.SECONDS));

        ClusterSecrets updated = currentClusterSecrets();
        assertThat(updated.getVersion(), equalTo(2L));
        assertThat(updated.getSettings().getString("secure.new_str").toString(), equalTo(str2));
        assertArrayEquals(file2, updated.getSettings().getFile("secure.new_file").readAllBytes());
        assertNull(updated.getSettings().getString("secure.str"));

        assertClusterSecretsHandlerKeys("secure.new_str", "secure.new_file");
    }

    public void testClusterSecretsSurviveRestart() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        final String masterNode = internalCluster().startMasterOnlyNode();

        FileSettingsService fss = internalCluster().getInstance(FileSettingsService.class, masterNode);
        assertBusy(() -> assertTrue(fss.watching()));

        String strValue = randomAlphaOfLength(12);
        byte[] fileValue = randomByteArrayOfLength(16);
        String fileValueB64 = Base64.getEncoder().encodeToString(fileValue);

        long version = versionCounter.incrementAndGet();
        CountDownLatch latch = clusterSecretsLatch(masterNode, version);
        writeJSONFile(masterNode, secretsJSON(version, strValue, fileValueB64));
        assertTrue(latch.await(20, TimeUnit.SECONDS));

        assertClusterSecrets(strValue, fileValue);

        internalCluster().restartNode(masterNode);
        ensureGreen();

        assertBusy(() -> assertClusterSecrets(strValue, fileValue));
    }

    public void testClusterSecretsRemoved() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        final String masterNode = internalCluster().startMasterOnlyNode();

        FileSettingsService fss = internalCluster().getInstance(FileSettingsService.class, masterNode);
        assertBusy(() -> assertTrue(fss.watching()));

        // Apply secrets
        String strValue = randomAlphaOfLength(12);
        byte[] fileValue = randomByteArrayOfLength(16);
        long v1 = versionCounter.incrementAndGet();
        CountDownLatch latch1 = clusterSecretsLatch(masterNode, v1);
        writeJSONFile(masterNode, secretsJSON(v1, strValue, Base64.getEncoder().encodeToString(fileValue)));
        assertTrue(latch1.await(20, TimeUnit.SECONDS));

        assertNotNull(currentClusterSecrets());

        // Write file settings without cluster_secrets key
        long v2 = versionCounter.incrementAndGet();
        CountDownLatch latch2 = clusterSecretsRemovedLatch(masterNode, v2);
        writeJSONFile(masterNode, emptyStateJSON(v2));
        assertTrue(latch2.await(20, TimeUnit.SECONDS));

        assertNull(masterClusterState().custom(ClusterSecrets.TYPE));
    }

    // --- helpers ---

    /**
     * Waits for reserved state metadata at the given file settings version that contains
     * the cluster_secrets handler.
     */
    private CountDownLatch clusterSecretsLatch(String node, long expectedVersion) {
        ClusterService cs = internalCluster().clusterService(node);
        CountDownLatch latch = new CountDownLatch(1);
        cs.addListener(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                ReservedStateMetadata rsm = event.state().metadata().reservedStateMetadata().get(FileSettingsService.NAMESPACE);
                if (rsm != null && rsm.version() == expectedVersion) {
                    ReservedStateHandlerMetadata hm = rsm.handlers().get(ClusterSecrets.NAME);
                    if (hm != null) {
                        cs.removeListener(this);
                        latch.countDown();
                    }
                }
            }
        });
        return latch;
    }

    /**
     * Waits for a reserved state version update where the cluster_secrets handler is absent,
     * meaning the remove() path was executed.
     */
    private CountDownLatch clusterSecretsRemovedLatch(String node, long expectedVersion) {
        ClusterService cs = internalCluster().clusterService(node);
        CountDownLatch latch = new CountDownLatch(1);
        cs.addListener(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                ReservedStateMetadata rsm = event.state().metadata().reservedStateMetadata().get(FileSettingsService.NAMESPACE);
                if (rsm != null && rsm.version() == expectedVersion && rsm.handlers().containsKey(ClusterSecrets.NAME) == false) {
                    cs.removeListener(this);
                    latch.countDown();
                }
            }
        });
        return latch;
    }

    private void assertClusterSecrets(String expectedStr, byte[] expectedFile) throws GeneralSecurityException, IOException {
        ClusterSecrets secrets = currentClusterSecrets();
        assertThat(secrets, notNullValue());
        assertThat(secrets.getSettings().getString("secure.str").toString(), equalTo(expectedStr));
        assertArrayEquals(expectedFile, secrets.getSettings().getFile("secure.file").readAllBytes());
    }

    private ClusterSecrets currentClusterSecrets() {
        return masterClusterState().custom(ClusterSecrets.TYPE);
    }

    /**
     * Returns the cluster state from the master's ClusterService directly.
     * We cannot use the cluster state API because ClusterSecrets is a private
     * custom and is stripped from transport responses.
     */
    private ClusterState masterClusterState() {
        return internalCluster().getCurrentMasterNodeInstance(ClusterService.class).state();
    }

    private void assertClusterSecretsHandlerKeys(String... expectedKeys) {
        ReservedStateMetadata rsm = masterClusterState().metadata().reservedStateMetadata().get(FileSettingsService.NAMESPACE);
        assertThat(rsm, notNullValue());
        ReservedStateHandlerMetadata hm = rsm.handlers().get(ClusterSecrets.NAME);
        assertThat(hm, notNullValue());
        assertThat(hm.keys(), containsInAnyOrder(expectedKeys));
    }

    private static void writeJSONFile(String node, String json) throws Exception {
        FileSettingsService fileSettingsService = internalCluster().getInstance(FileSettingsService.class, node);
        Files.createDirectories(fileSettingsService.watchedFileDir());
        Path tempFilePath = createTempFile();
        Files.writeString(tempFilePath, json);
        Files.move(tempFilePath, fileSettingsService.watchedFile(), StandardCopyOption.ATOMIC_MOVE);
    }
}
