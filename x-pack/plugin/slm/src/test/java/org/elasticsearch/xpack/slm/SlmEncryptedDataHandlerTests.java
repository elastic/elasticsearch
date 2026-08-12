/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm;

import org.apache.logging.log4j.Level;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.xpack.core.encryption.EncryptedData;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyMetadata;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class SlmEncryptedDataHandlerTests extends ESTestCase {

    private final SlmEncryptedDataHandler handler = new SlmEncryptedDataHandler();

    public void testCustomName() {
        assertEquals(SnapshotLifecycleMetadata.TYPE, handler.customName());
    }

    public void testReEncryptRewrapsOnlyPasswordedPolicies() {
        EncryptedData original = new EncryptedData("old-key", randomByteArrayOfLength(16));
        SnapshotLifecycleMetadata metadata = metadataOf(policyMetadata("with-password", original), policyMetadata("plain", null));
        EncryptedData rewrapped = new EncryptedData("new-key", randomByteArrayOfLength(16));

        SnapshotLifecycleMetadata result = handler.reEncrypt(metadata, existing -> rewrapped);

        assertNotSame(metadata, result);
        assertThat(result.getSnapshotConfigurations().get("with-password").getPolicy().getEncryptedPassword(), sameInstance(rewrapped));
        assertThat(
            "the policy without a password must be forwarded by reference",
            result.getSnapshotConfigurations().get("plain"),
            sameInstance(metadata.getSnapshotConfigurations().get("plain"))
        );
    }

    public void testReEncryptReturnsSameInstanceWhenNothingChanges() {
        SnapshotLifecycleMetadata noPasswords = metadataOf(policyMetadata("plain", null));
        assertThat(handler.reEncrypt(noPasswords, existing -> existing), sameInstance(noPasswords));

        SnapshotLifecycleMetadata withPassword = metadataOf(
            policyMetadata("with-password", new EncryptedData("key", randomByteArrayOfLength(8)))
        );
        assertThat(handler.reEncrypt(withPassword, existing -> existing), sameInstance(withPassword));
    }

    public void testReEncryptClearsPasswordOnNullResult() {
        SnapshotLifecycleMetadata metadata = metadataOf(
            policyMetadata("with-password", new EncryptedData("key", randomByteArrayOfLength(8)))
        );

        SnapshotLifecycleMetadata result = handler.reEncrypt(metadata, existing -> null);

        SnapshotLifecyclePolicy cleared = result.getSnapshotConfigurations().get("with-password").getPolicy();
        assertThat(cleared.getEncryptedPassword(), nullValue());
        assertThat(cleared.getConfig(), equalTo(metadata.getSnapshotConfigurations().get("with-password").getPolicy().getConfig()));
    }

    public void testOnDestructiveResetClearsPasswordsPreservesPoliciesAndWarns() {
        SnapshotLifecycleMetadata metadata = metadataOf(
            policyMetadata("bravo", new EncryptedData("key", randomByteArrayOfLength(8))),
            policyMetadata("alpha", new EncryptedData("key", randomByteArrayOfLength(8))),
            policyMetadata("plain", null)
        );

        var resultHolder = new SnapshotLifecycleMetadata[1];
        MockLog.assertThatLogger(
            () -> resultHolder[0] = handler.onDestructiveReset(metadata),
            SlmEncryptedDataHandler.class,
            new MockLog.SeenEventExpectation(
                "reset warning names the cleared policies in order",
                SlmEncryptedDataHandler.class.getCanonicalName(),
                Level.WARN,
                "*destroyed the snapshot encryption passwords of SLM policies [alpha, bravo]*"
            )
        );

        SnapshotLifecycleMetadata result = resultHolder[0];
        assertThat(result, notNullValue());
        assertEquals(3, result.getSnapshotConfigurations().size());
        assertThat(result.getSnapshotConfigurations().get("alpha").getPolicy().getEncryptedPassword(), nullValue());
        assertThat(result.getSnapshotConfigurations().get("bravo").getPolicy().getEncryptedPassword(), nullValue());
        assertThat(result.getSnapshotConfigurations().get("plain"), sameInstance(metadata.getSnapshotConfigurations().get("plain")));
    }

    public void testOnDestructiveResetIsQuietWhenNothingToClear() {
        assertThat(handler.onDestructiveReset(null), nullValue());

        SnapshotLifecycleMetadata noPasswords = metadataOf(policyMetadata("plain", null));
        MockLog.assertThatLogger(
            () -> assertThat(handler.onDestructiveReset(noPasswords), sameInstance(noPasswords)),
            SlmEncryptedDataHandler.class,
            new MockLog.UnseenEventExpectation(
                "no warning when no passwords were cleared",
                SlmEncryptedDataHandler.class.getCanonicalName(),
                Level.WARN,
                "*"
            )
        );
    }

    private static SnapshotLifecycleMetadata metadataOf(SnapshotLifecyclePolicyMetadata... policies) {
        Map<String, SnapshotLifecyclePolicyMetadata> byId = new java.util.HashMap<>();
        for (SnapshotLifecyclePolicyMetadata policy : policies) {
            byId.put(policy.getId(), policy);
        }
        return new SnapshotLifecycleMetadata(byId, randomFrom(OperationMode.values()), null);
    }

    private static SnapshotLifecyclePolicyMetadata policyMetadata(String id, EncryptedData encryptedPassword) {
        SnapshotLifecyclePolicy policy = new SnapshotLifecyclePolicy(id, "snap", "0 30 1 * * ?", "repo", Map.of("k", "v"), null, null)
            .withEncryptedPassword(encryptedPassword);
        return SnapshotLifecyclePolicyMetadata.builder()
            .setPolicy(policy)
            .setVersion(randomNonNegativeLong())
            .setModifiedDate(randomNonNegativeLong())
            .build();
    }
}
