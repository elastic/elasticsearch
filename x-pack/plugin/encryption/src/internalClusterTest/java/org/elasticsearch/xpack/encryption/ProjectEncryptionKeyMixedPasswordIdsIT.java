/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;
import org.elasticsearch.xpack.encryption.spi.EncryptionService;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;

import static org.hamcrest.Matchers.notNullValue;

/**
 * Verifies that the PEK survives a full cluster restart when each node uses a different active password ID. Node 0 uses {@code "id-0"},
 * node 1 uses {@code "id-1"}, etc. Every node also carries all other nodes' passwords so each disk file can be unwrapped after restart
 * regardless of which node originally wrote it.
 *
 * <p>The per-node password configuration is supplied directly via {@link #nodeSettings(int, Settings)} (rather than via a runtime
 * {@code reload}) so that the same secure settings are present both before and after {@link #internalCluster()}'s
 * {@code fullRestart()}: at restart the framework reuses the settings captured at node construction time, so any in-memory reload-only
 * configuration would be lost.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 3, supportsDedicatedMasters = false)
public class ProjectEncryptionKeyMixedPasswordIdsIT extends ESIntegTestCase {

    private static final int NUM_NODES = 3;
    private static final String BASE_PASSWORD = "encryption-test-password";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        Collection<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(EncryptionPlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        MockSecureSettings secure = new MockSecureSettings();
        secure.setString(ProjectEncryptionKeyPasswordSettings.ACTIVE_PASSWORD_ID_KEY, "id-" + (nodeOrdinal % NUM_NODES));
        for (int j = 0; j < NUM_NODES; j++) {
            secure.setString(ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX + "id-" + j, BASE_PASSWORD + "-id-" + j);
        }
        builder.setSecureSettings(secure);
        return builder.build();
    }

    public void testKeyPersistsAfterFullRestartWithDifferentPasswordIdsPerNode() throws Exception {
        ensureGreen();
        assertBusy(() -> assertThat(getKeyFromMaster(), notNullValue()));
        String originalKeyId = getKeyFromMaster().getActiveKeyId();

        internalCluster().fullRestart();
        ensureGreen();

        assertBusy(() -> {
            ProjectEncryptionKeyMetadata pek = getKeyFromMaster();
            assertThat("PEK must survive a full restart with per-node password IDs", pek, notNullValue());
            assertEquals("active key ID must be unchanged after restart", originalKeyId, pek.getActiveKeyId());
        });

        EncryptionService masterSvc = internalCluster().getInstance(EncryptionService.class, internalCluster().getMasterName());
        byte[] payload = "post-restart-mixed-id-payload".getBytes(StandardCharsets.UTF_8);
        EncryptedData encrypted = masterSvc.encrypt(payload);

        for (String nodeName : internalCluster().getNodeNames()) {
            EncryptionService svc = internalCluster().getInstance(EncryptionService.class, nodeName);
            assertArrayEquals(
                "node [" + nodeName + "] must decrypt post-restart data despite different active password ID",
                payload,
                svc.decrypt(encrypted)
            );
        }
    }

    private ProjectEncryptionKeyMetadata getKeyFromMaster() {
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, internalCluster().getMasterName());
        return clusterService.state().metadata().getSingleProjectCustom(ProjectEncryptionKeyMetadata.TYPE);
    }
}
