/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ReservedStateMetadata;
import org.elasticsearch.common.Priority;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reservedstate.service.FileSettingsService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.settings.secure.SecureSettingsPlugin;
import org.junit.Before;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;

import static org.elasticsearch.xpack.encryption.ProjectEncryptionKeyPasswordSettings.ACTIVE_PASSWORD_ID_KEY;
import static org.elasticsearch.xpack.encryption.ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX;
import static org.hamcrest.Matchers.equalTo;

/**
 * On stateless only master-eligible nodes persist cluster state, so only they must be able to wrap the cluster state encryption key.
 * When the password comes from cluster secrets every node gets it from the cluster state, so nodes are not checked for it; otherwise each
 * node uses the password in its own keystore, which may differ between nodes.
 *
 * <p>Nodes start without a password.
 */
@LuceneTestCase.SuppressFileSystems("*")
// not auto-managed, so that starting a master-eligible node that is rejected at join doesn't wait for it to join
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0, autoManageMasterNodes = false)
public class StatelessEncryptionPasswordIT extends AbstractStatelessPluginIntegTestCase {

    @Before
    public void bootstrapFirstMaster() {
        internalCluster().setBootstrapMasterNodeIndex(0);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        Collection<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(EncryptionPlugin.class);
        plugins.add(SecureSettingsPlugin.class);
        return plugins;
    }

    /** A master-eligible node without a password at startup joins, since it gets the password from cluster secrets once it has joined. */
    public void testNodeWithoutPasswordJoinsWhenPasswordIsInClusterSecrets() throws Exception {
        startMasterOnlyNode();
        ensureStableCluster(1);
        applyClusterSecrets(1, ACTIVE_PASSWORD_ID_KEY, "v1", PASSWORD_PREFIX + "v1", "test-password-v1");
        assertBusy(() -> assertTrue(hasUsableKey(masterState())));

        String joining = startMasterOnlyNode();
        ensureStableCluster(2);
        assertBusy(() -> assertTrue(pekEncryption(joining).canWrapForGateway()));
        assertThat(pekEncryption(joining).activePasswordId(), equalTo("v1"));
    }

    /** Writes file settings holding the given cluster secrets, as alternating names and values, and waits for every node to apply them. */
    private static void applyClusterSecrets(long version, String... namesAndValues) throws Exception {
        FileSettingsService fileSettings = internalCluster().getInstance(FileSettingsService.class, internalCluster().getMasterName());
        assertBusy(() -> assertTrue(fileSettings.watching()));
        Files.createDirectories(fileSettings.watchedFileDir());
        Path tempFile = createTempFile();
        Files.writeString(tempFile, EncryptionIntegTestUtils.clusterSecretsJson(version, namesAndValues));
        Files.move(tempFile, fileSettings.watchedFile(), StandardCopyOption.ATOMIC_MOVE);

        assertBusy(() -> {
            ReservedStateMetadata reserved = masterState().metadata().reservedStateMetadata().get(FileSettingsService.NAMESPACE);
            assertNotNull(reserved);
            assertEquals(version, reserved.version().longValue());
        });
        // every node has applied the cluster secrets, and so reloaded them, once the master has no pending tasks
        clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT).setWaitForEvents(Priority.LANGUID).get();
    }

    private static ClusterState masterState() {
        return internalCluster().clusterService(internalCluster().getMasterName()).state();
    }

    private static ProjectEncryptionKeyMetadata pek(ClusterState state) {
        return state.metadata().getSingleProjectCustom(ProjectEncryptionKeyMetadata.TYPE);
    }

    private static boolean hasUsableKey(ClusterState state) {
        ProjectEncryptionKeyMetadata metadata = pek(state);
        return metadata != null && metadata.isUnwrapFailed() == false;
    }

    private static PasswordPekEncryption pekEncryption(String node) {
        return internalCluster().getInstance(PasswordPekEncryption.class, node);
    }
}
