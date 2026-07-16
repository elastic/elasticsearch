/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;
import org.elasticsearch.xpack.encryption.spi.EncryptionService;

import java.util.ArrayList;
import java.util.Collection;

import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 3, supportsDedicatedMasters = false)
public class EncryptionServiceIT extends SecurityIntegTestCase {

    private static final String PASSWORD_ID = "v1";
    private static final String PASSWORD = "encryption-test-password";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        Collection<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(EncryptionPlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        SecuritySettingsSource.addSecureSettings(builder, secure -> {
            secure.setString(ProjectEncryptionKeyPasswordSettings.ACTIVE_PASSWORD_ID_KEY, PASSWORD_ID);
            secure.setString(ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX + PASSWORD_ID, PASSWORD);
        });
        return builder.build();
    }

    private void waitForKey() throws Exception {
        assertBusy(() -> {
            ClusterService clusterService = internalCluster().getInstance(ClusterService.class, internalCluster().getMasterName());
            ProjectEncryptionKeyMetadata pek = clusterService.state().metadata().getSingleProjectCustom(ProjectEncryptionKeyMetadata.TYPE);
            assertThat(pek, notNullValue());
        });
    }

    public void testEncryptDecryptRoundTrip() throws Exception {
        ensureGreen();
        waitForKey();

        EncryptionService service = internalCluster().getInstance(EncryptionService.class, internalCluster().getMasterName());
        byte[] plaintext = randomByteArrayOfLength(randomIntBetween(1, 4096));

        EncryptedData encrypted = service.encrypt(plaintext);
        byte[] decrypted = service.decrypt(encrypted);
        assertArrayEquals(plaintext, decrypted);
    }

    public void testEncryptOnOneNodeDecryptOnAnother() throws Exception {
        ensureGreen();
        waitForKey();

        String[] nodes = internalCluster().getNodeNames();
        assertTrue("need at least 2 nodes", nodes.length >= 2);

        EncryptionService serviceA = internalCluster().getInstance(EncryptionService.class, nodes[0]);
        EncryptionService serviceB = internalCluster().getInstance(EncryptionService.class, nodes[1]);

        byte[] plaintext = randomByteArrayOfLength(randomIntBetween(1, 4096));
        EncryptedData encrypted = serviceA.encrypt(plaintext);
        byte[] decrypted = serviceB.decrypt(encrypted);
        assertArrayEquals(plaintext, decrypted);
    }

    public void testDecryptSurvivesMasterFailover() throws Exception {
        ensureGreen();
        waitForKey();

        String nonMasterNode = randomValueOtherThan(internalCluster().getMasterName(), () -> randomFrom(internalCluster().getNodeNames()));

        EncryptionService serviceBefore = internalCluster().getInstance(EncryptionService.class, nonMasterNode);
        byte[] plaintext = randomByteArrayOfLength(randomIntBetween(1, 256));
        EncryptedData encrypted = serviceBefore.encrypt(plaintext);

        internalCluster().stopCurrentMasterNode();
        ensureGreen();

        EncryptionService serviceAfter = internalCluster().getInstance(EncryptionService.class, nonMasterNode);
        byte[] decrypted = serviceAfter.decrypt(encrypted);
        assertArrayEquals(plaintext, decrypted);
    }
}
