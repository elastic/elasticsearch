/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.crypto;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.xpack.core.crypto.EncryptedData;
import org.elasticsearch.xpack.core.crypto.EncryptionService;
import org.elasticsearch.xpack.core.crypto.PrimaryEncryptionKeyMetadata;
import org.junit.Before;

import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 3, supportsDedicatedMasters = false)
public class EncryptionServiceIT extends SecurityIntegTestCase {

    @Before
    public void checkFeatureFlag() {
        assumeTrue(
            "primary encryption key feature flag must be enabled",
            PrimaryEncryptionKeyService.PRIMARY_ENCRYPTION_KEY_FEATURE_FLAG.isEnabled()
        );
    }

    private void waitForKey() throws Exception {
        assertBusy(() -> {
            ClusterService clusterService = internalCluster().getInstance(ClusterService.class, internalCluster().getMasterName());
            PrimaryEncryptionKeyMetadata pek = clusterService.state().metadata().getSingleProjectCustom(PrimaryEncryptionKeyMetadata.TYPE);
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
